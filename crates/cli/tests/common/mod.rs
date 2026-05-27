// Shared helpers extracted from the previous monolithic crates/cli/tests/cli.rs.
// Each integration test binary in this directory imports this module via
// `mod common;` and `use common::*;`. The integration tests spawn `dm` and
// `dmd` subprocesses, so they live under `tests/` rather than `src/`. The
// per-binary compile cost of duplicating this module is offset by the
// parallelism nextest gains from splitting the CLI test surface into
// smaller, area-focused binaries.

#![allow(dead_code, unused_imports)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, Mutex, OnceLock, mpsc as std_mpsc};
use std::thread::JoinHandle;

use nostr_relay_builder::MockRelay;
use tokio::sync::oneshot;
use transport_quic_broker::{DEFAULT_SUBSCRIBER_QUEUE_DEPTH, QuicBrokerConfig, QuicBrokerServer};

// Re-exports for downstream test binaries. Each `tests/cli_*.rs` does
// `use common::*;` and inherits the names that test bodies reference directly.
pub use serde_json::Value;
pub use std::env;
pub use std::net::TcpStream;
#[cfg(unix)]
pub use std::os::unix::fs::PermissionsExt;
pub use std::process::{Child, Command, Output, Stdio};
pub use std::time::{Duration, Instant};

pub const POLL_TIMEOUT: Duration = Duration::from_secs(8);
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);

pub struct TestRelay {
    _runtime: tokio::runtime::Runtime,
    _relay: MockRelay,
    url: String,
}

impl TestRelay {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("test relay runtime");
        let mut last_error = None;
        let relay = (0..8)
            .find_map(|attempt| match runtime.block_on(MockRelay::run()) {
                Ok(relay) => Some(relay),
                Err(err) => {
                    eprintln!("mock relay startup attempt {} failed: {err}", attempt + 1);
                    last_error = Some(err);
                    std::thread::sleep(Duration::from_millis(25));
                    None
                }
            })
            .unwrap_or_else(|| panic!("mock relay should start: {last_error:?}"));
        let url = runtime.block_on(relay.url()).to_string();
        Self {
            _runtime: runtime,
            _relay: relay,
            url,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

pub struct TestBlossom {
    url: String,
    blobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    shutdown: Option<std_mpsc::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl TestBlossom {
    pub fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind blossom");
        listener
            .set_nonblocking(true)
            .expect("nonblocking blossom listener");
        let addr = listener.local_addr().expect("blossom addr");
        let url = format!("http://{addr}");
        let blobs = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
        let server_blobs = blobs.clone();
        let server_url = url.clone();
        let (shutdown_tx, shutdown_rx) = std_mpsc::channel();
        let handle = std::thread::spawn(move || {
            loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((stream, _peer)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("blocking blossom stream");
                        handle_blossom_connection(stream, &server_url, &server_blobs)
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            url,
            blobs,
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn blob(&self, hash_hex: &str) -> Option<Vec<u8>> {
        self.blobs
            .lock()
            .expect("blossom blobs")
            .get(hash_hex)
            .cloned()
    }
}

impl Drop for TestBlossom {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub fn handle_blossom_connection(
    mut stream: TcpStream,
    server_url: &str,
    blobs: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
) {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).expect("read blossom request");
        if read == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(offset) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break offset + 4;
        }
    };
    let headers = String::from_utf8_lossy(&request[..header_end]).to_string();
    let mut lines = headers.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();
    let mut content_length = 0_usize;
    let mut x_sha256 = None;
    let mut authorization = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name.to_ascii_lowercase().as_str() {
            "content-length" => content_length = value.trim().parse().unwrap_or_default(),
            "x-sha-256" => x_sha256 = Some(value.trim().to_owned()),
            "authorization" => authorization = Some(value.trim().to_owned()),
            _ => {}
        }
    }
    while request.len() < header_end + content_length {
        let read = stream.read(&mut buffer).expect("read blossom body");
        if read == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let body = request[header_end..header_end + content_length].to_vec();
    match (method.as_str(), path.as_str()) {
        ("PUT", "/upload") => {
            assert!(
                authorization
                    .as_deref()
                    .is_some_and(|value| value.starts_with("Nostr "))
            );
            let encrypted_hash = x_sha256.expect("upload should include X-SHA-256");
            blobs
                .lock()
                .expect("blossom blobs")
                .insert(encrypted_hash.clone(), body.clone());
            let descriptor = serde_json::json!({
                "url": format!("{server_url}/{encrypted_hash}.bin"),
                "sha256": encrypted_hash,
                "size": body.len(),
                "type": "application/octet-stream",
                "uploaded": 1_u64,
            })
            .to_string();
            write_blossom_response(&mut stream, 201, "application/json", descriptor.as_bytes());
        }
        ("GET", blob_path) => {
            let hash = blob_path
                .trim_start_matches('/')
                .split_once('.')
                .map(|(hash, _)| hash)
                .unwrap_or_else(|| blob_path.trim_start_matches('/'));
            let blob = blobs.lock().expect("blossom blobs").get(hash).cloned();
            if let Some(blob) = blob {
                write_blossom_response(&mut stream, 200, "application/octet-stream", &blob);
            } else {
                write_blossom_response(&mut stream, 404, "text/plain", b"not found");
            }
        }
        _ => write_blossom_response(&mut stream, 404, "text/plain", b"not found"),
    }
}

pub fn write_blossom_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        404 => "Not Found",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .expect("write response head");
    stream.write_all(body).expect("write response body");
}

pub fn test_relay_url() -> &'static str {
    static RELAY: OnceLock<TestRelay> = OnceLock::new();
    RELAY.get_or_init(TestRelay::new).url()
}

pub fn two_default_relays() -> (TestRelay, TestRelay, String) {
    let first = TestRelay::new();
    let second = TestRelay::new();
    let relays = format!("{},{}", first.url(), second.url());
    (first, second, relays)
}

pub fn relay_pair_json(first: &TestRelay, second: &TestRelay) -> Value {
    serde_json::json!([first.url(), second.url()])
}

pub fn dm(home: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dm"));
    command.arg("--home").arg(home).arg("--json");
    command.env("DM_SECRET_STORE", "file");
    command.env("DM_RELAY", test_relay_url());
    command
}

pub fn dm_without_relay(home: &std::path::Path) -> Command {
    let mut command = dm(home);
    command.env_remove("DM_RELAY");
    command
}

pub fn dm_with_relay(home: &std::path::Path, relay: &str) -> Command {
    let mut command = dm(home);
    command.arg("--relay").arg(relay);
    command
}

pub fn command_output_summary(output: &Output) -> String {
    format!(
        "status={}\nstdout_len={}\nstderr_len={}\nstdout={}\nstderr={}",
        output.status,
        output.stdout.len(),
        output.stderr.len(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

pub fn json_value_summary(label: &str, value: &Value) -> String {
    format!("{label}_json_len={}", value.to_string().len())
}

pub fn assert_two_word_pseudonym(value: &str) {
    let words = value.split(' ').collect::<Vec<_>>();
    assert_eq!(words.len(), 2, "expected two words: {value}");
    for word in words {
        let mut chars = word.chars();
        assert!(
            chars.next().is_some_and(|ch| ch.is_ascii_uppercase()),
            "word should start uppercase: {word}"
        );
        assert!(
            chars.all(|ch| ch.is_ascii_lowercase()),
            "word should be title-cased ASCII: {word}"
        );
    }
}

pub fn run_json(home: &std::path::Path, args: &[&str]) -> Value {
    try_run_json(home, args).unwrap_or_else(|failure| panic!("dm failed\n{failure}"))
}

pub fn run_json_with_stdin(home: &std::path::Path, args: &[&str], stdin: &str) -> Value {
    let mut child = dm(home)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dm command should start");
    child
        .stdin
        .take()
        .expect("stdin should be piped")
        .write_all(stdin.as_bytes())
        .expect("stdin should accept nsec input");
    let output = child.wait_with_output().expect("dm command should finish");
    assert!(
        output.status.success(),
        "dm failed\nargs={args:?}\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], true);
    value["result"].clone()
}

pub fn run_json_without_relay(home: &std::path::Path, args: &[&str]) -> Value {
    try_run_json_without_relay(home, args).unwrap_or_else(|failure| panic!("dm failed\n{failure}"))
}

pub fn try_run_json(home: &std::path::Path, args: &[&str]) -> Result<Value, String> {
    let output = dm(home)
        .args(args)
        .output()
        .expect("dm command should start");
    if !output.status.success() {
        return Err(format!(
            "dm failed\nargs={args:?}\n{}",
            command_output_summary(&output)
        ));
    }
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    if value["ok"] != true {
        return Err(format!("unexpected json response: {value}"));
    }
    Ok(value["result"].clone())
}

pub fn try_run_json_without_relay(home: &std::path::Path, args: &[&str]) -> Result<Value, String> {
    let output = dm_without_relay(home)
        .args(args)
        .output()
        .expect("dm command should start");
    if !output.status.success() {
        return Err(format!(
            "dm failed\nargs={args:?}\n{}",
            command_output_summary(&output)
        ));
    }
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    if value["ok"] != true {
        return Err(format!("unexpected json response: {value}"));
    }
    Ok(value["result"].clone())
}

pub fn run_json_with_relay(home: &std::path::Path, relay: &str, args: &[&str]) -> Value {
    let output = dm_with_relay(home, relay)
        .args(args)
        .output()
        .expect("dm command should start");
    assert!(
        output.status.success(),
        "dm failed\nrelay=<REDACTED_RELAY>\nargs={args:?}\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], true);
    value["result"].clone()
}

pub fn run_json_error(home: &std::path::Path, args: &[&str]) -> Value {
    let output = dm(home)
        .args(args)
        .output()
        .expect("dm command should start");
    assert!(
        !output.status.success(),
        "dm unexpectedly succeeded\nargs={args:?}\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], false);
    value["error"].clone()
}

pub fn run_json_error_with_stdin(home: &std::path::Path, args: &[&str], stdin: &str) -> Value {
    let mut child = dm(home)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dm command should start");
    child
        .stdin
        .take()
        .expect("stdin should be piped")
        .write_all(stdin.as_bytes())
        .expect("stdin should accept nsec input");
    let output = child.wait_with_output().expect("dm command should finish");
    assert!(
        !output.status.success(),
        "dm unexpectedly succeeded\nargs={args:?}\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], false);
    value["error"].clone()
}

pub fn run_json_with_env(home: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> Value {
    let mut command = dm(home);
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command.output().expect("dm command should start");
    assert!(
        output.status.success(),
        "dm failed\nargs={args:?}\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], true);
    value["result"].clone()
}

pub fn create_account(home: &std::path::Path) -> String {
    run_json(home, &["account", "create"])["account_id"]
        .as_str()
        .expect("account id")
        .to_owned()
}

pub fn create_account_with_relays(
    home: &std::path::Path,
    default_relays: &str,
    bootstrap_relays: &str,
) -> Value {
    run_json(
        home,
        &[
            "account",
            "create",
            "--default-relays",
            default_relays,
            "--bootstrap-relays",
            bootstrap_relays,
        ],
    )
}

pub fn member_accounts(value: &Value) -> Vec<String> {
    let mut accounts = value["members"]
        .as_array()
        .expect("members array")
        .iter()
        .filter_map(|member| member["member_id"].as_str().map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    accounts.sort();
    accounts
}

pub fn admin_accounts(value: &Value) -> Vec<String> {
    let mut accounts = value["admins"]
        .as_array()
        .expect("admins array")
        .iter()
        .filter_map(|admin| admin["admin_id"].as_str().map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    accounts.sort();
    accounts
}

pub fn sorted_accounts<const N: usize>(accounts: [&str; N]) -> Vec<String> {
    let mut accounts = accounts
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    accounts.sort();
    accounts
}

pub fn message_plaintexts(value: &Value) -> Vec<String> {
    value["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .map(|message| {
            message["plaintext"]
                .as_str()
                .expect("message plaintext")
                .to_owned()
        })
        .collect()
}

pub fn assert_message_plaintexts(value: &Value, expected: &[&str]) {
    let actual = message_plaintexts(value);
    for expected in expected {
        assert!(
            actual.iter().any(|plaintext| plaintext == expected),
            "expected message {expected:?} in {actual:?}"
        );
    }
}

pub fn assert_no_message_plaintext(value: &Value, unexpected: &str) {
    let actual = message_plaintexts(value);
    assert!(
        actual.iter().all(|plaintext| plaintext != unexpected),
        "did not expect message {unexpected:?} in {actual:?}"
    );
}

pub fn free_udp_addr() -> String {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind free udp socket");
    socket.local_addr().expect("local udp addr").to_string()
}

pub fn wait_for_udp_listener(addr: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match UdpSocket::bind(addr) {
            Ok(socket) => drop(socket),
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => return,
            Err(err) => panic!("failed to probe udp listener {addr}: {err}"),
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("udp listener {addr} did not become ready");
}

pub fn run_json_until_child_exits(
    home: &std::path::Path,
    mut child: Child,
    timeout: Duration,
    mut run_command: impl FnMut(&std::path::Path) -> Result<Value, String>,
) -> (Value, Output) {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    let mut command_value = None;
    while Instant::now() < deadline {
        if command_value.is_none() {
            match run_command(home) {
                Ok(value) => command_value = Some(value),
                Err(error) => last_error = Some(error),
            }
        }
        if let Some(value) = command_value.take() {
            if child.try_wait().expect("child status").is_some() {
                let output = child.wait_with_output().expect("child output");
                return (value, output);
            }
            command_value = Some(value);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let output = child.wait_with_output().expect("killed child output");
    panic!(
        "child did not finish after retried command\n{}\nlast_command_error={}",
        command_output_summary(&output),
        last_error.as_deref().unwrap_or("<none>")
    );
}

pub fn run_json_until_success(home: &std::path::Path, args: &[&str], timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        match try_run_json(home, args) {
            Ok(value) => return value,
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "dm did not succeed after retries\nlast_command_error={}",
        last_error.as_deref().unwrap_or("<none>")
    );
}

pub fn poll_json_until(
    home: &std::path::Path,
    args: &[&str],
    timeout: Duration,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last_value = None;
    let mut last_error = None;
    while Instant::now() < deadline {
        match try_run_json(home, args) {
            Ok(value) if predicate(&value) => return value,
            Ok(value) => last_value = Some(value),
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "dm did not reach expected JSON state\nlast_value={}\nlast_error={}",
        last_value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "<none>".to_owned()),
        last_error.as_deref().unwrap_or("<none>")
    );
}

pub fn poll_json_without_relay_until(
    home: &std::path::Path,
    args: &[&str],
    timeout: Duration,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last_value = None;
    let mut last_error = None;
    while Instant::now() < deadline {
        match try_run_json_without_relay(home, args) {
            Ok(value) if predicate(&value) => return value,
            Ok(value) => last_value = Some(value),
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "dm did not reach expected JSON state\nlast_value={}\nlast_error={}",
        last_value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "<none>".to_owned()),
        last_error.as_deref().unwrap_or("<none>")
    );
}

pub fn wait_child_output_or_panic(child: Child, timeout: Duration, context: &str) -> Output {
    let output = wait_child_output(child, timeout);
    assert!(
        output.status.success(),
        "{context}\n{}",
        command_output_summary(&output)
    );
    output
}

pub struct BrokerHandle {
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for BrokerHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn spawn_quic_broker() -> BrokerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = std_mpsc::channel();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("broker runtime");
        runtime.block_on(async {
            let server = QuicBrokerServer::bind(QuicBrokerConfig {
                bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                per_subscriber_queue: DEFAULT_SUBSCRIBER_QUEUE_DEPTH,
                ..QuicBrokerConfig::default()
            })
            .expect("broker bind");
            let addr = server.local_addr().expect("broker addr");
            ready_tx.send(addr).expect("broker ready signal");
            server
                .run_until(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("broker should stop cleanly");
        });
    });
    let addr = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("broker should become ready");
    BrokerHandle {
        addr,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    }
}

pub fn wait_child_output(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if child.try_wait().expect("child status").is_some() {
            return child.wait_with_output().expect("child output");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    let output = child.wait_with_output().expect("killed child output");
    panic!("child timed out\n{}", command_output_summary(&output));
}

pub fn real_relay_urls() -> Vec<String> {
    env::var("DARKMATTER_E2E_RELAYS")
        .ok()
        .map(|relays| {
            relays
                .split(',')
                .map(str::trim)
                .filter(|relay| !relay.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|relays| !relays.is_empty())
        .unwrap_or_else(|| vec!["ws://127.0.0.1:27777".to_owned()])
}

pub fn require_real_relays() -> bool {
    env::var("DARKMATTER_E2E_REQUIRE_RELAYS")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

pub fn local_relay_available(relay: &str) -> bool {
    let Some(address) = relay
        .strip_prefix("wss://")
        .or_else(|| relay.strip_prefix("ws://"))
    else {
        return false;
    };
    let address = address.split('/').next().expect("relay authority");
    let Ok(addresses) = address.to_socket_addrs() else {
        return false;
    };
    addresses.into_iter().any(|socket_address| {
        TcpStream::connect_timeout(&socket_address, Duration::from_millis(200)).is_ok()
    })
}

pub fn create_account_with_real_relay(home: &std::path::Path, relay: &str) -> String {
    run_json_with_relay(
        home,
        relay,
        &[
            "account",
            "create",
            "--default-relays",
            relay,
            "--bootstrap-relays",
            relay,
        ],
    )["account_id"]
        .as_str()
        .expect("account id")
        .to_owned()
}

pub fn sync_until_joined(
    home: &std::path::Path,
    relay: &str,
    account: &str,
    group_id: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let mut sync = run_json_with_relay(home, relay, &["--account", account, "sync"]);
        if sync["joined_groups"]
            .as_array()
            .is_some_and(|groups| groups.iter().any(|group| group == group_id))
        {
            return sync;
        }
        let chats = run_json_with_relay(home, relay, &["--account", account, "chats", "list"]);
        if chats["chats"]
            .as_array()
            .is_some_and(|chats| chats.iter().any(|chat| chat["group_id"] == group_id))
        {
            sync["joined_groups"] = serde_json::json!([group_id]);
            return sync;
        }
        last = sync;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not join <REDACTED_GROUP> via <REDACTED_RELAY>; {}",
        json_value_summary("last_sync", &last)
    );
}

pub fn sync_until_message(
    home: &std::path::Path,
    relay: &str,
    account: &str,
    plaintext: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let sync = run_json_with_relay(home, relay, &["--account", account, "sync"]);
        if message_plaintexts(&sync)
            .iter()
            .any(|message| message == plaintext)
        {
            return sync;
        }
        let messages = run_json_with_relay(home, relay, &["--account", account, "message", "list"]);
        if message_plaintexts(&messages)
            .iter()
            .any(|message| message == plaintext)
        {
            if let Some(message) = messages["messages"].as_array().and_then(|messages| {
                messages
                    .iter()
                    .find(|message| message["plaintext"] == plaintext)
            }) {
                let mut projected = messages.clone();
                projected["messages"] = serde_json::json!([message.clone()]);
                return projected;
            }
            return messages;
        }
        last = messages;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not receive <REDACTED_MESSAGE> via <REDACTED_RELAY>; {}",
        json_value_summary("last_sync", &last)
    );
}

/// Poll `sync`/`message list` until a projected message of `kind` referencing
/// `target` via an `e` tag arrives (used for reactions/deletes whose content is
/// empty or just an emoji, so plaintext matching doesn't apply).
pub fn sync_until_message_with_kind(
    home: &std::path::Path,
    relay: &str,
    account: &str,
    kind: u64,
    target: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let sync = run_json_with_relay(home, relay, &["--account", account, "sync"]);
        if first_message_with_kind_and_target(&sync, kind, target).is_some() {
            return sync;
        }
        let messages = run_json_with_relay(home, relay, &["--account", account, "message", "list"]);
        if first_message_with_kind_and_target(&messages, kind, target).is_some() {
            return messages;
        }
        last = messages;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not receive a kind-{kind} message; {}",
        json_value_summary("last_sync", &last)
    );
}

pub fn first_message_with_kind(value: &Value, kind: u64) -> Option<&Value> {
    value["messages"]
        .as_array()?
        .iter()
        .find(|message| message["kind"].as_u64() == Some(kind))
}

pub fn first_message_with_kind_and_target<'a>(
    value: &'a Value,
    kind: u64,
    target: &str,
) -> Option<&'a Value> {
    value["messages"].as_array()?.iter().find(|message| {
        message["kind"].as_u64() == Some(kind) && message_e_tag(message) == Some(target)
    })
}

/// First `e` tag value on a projected message's `tags` array.
pub fn message_e_tag(message: &Value) -> Option<&str> {
    message["tags"].as_array()?.iter().find_map(|tag| {
        let tag = tag.as_array()?;
        if tag.first()?.as_str()? == "e" {
            tag.get(1)?.as_str()
        } else {
            None
        }
    })
}

pub fn sync_until_member(
    home: &std::path::Path,
    account: &str,
    group_id: &str,
    member: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let _ = run_json(home, &["--account", account, "sync"]);
        let members = run_json(home, &["--account", account, "group", "members", group_id]);
        if member_accounts(&members)
            .iter()
            .any(|candidate| candidate == member)
        {
            return members;
        }
        last = members;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not see expected member in <REDACTED_GROUP>; {}",
        json_value_summary("last_members", &last)
    );
}

pub fn sync_until_admins<const N: usize>(
    home: &std::path::Path,
    account: &str,
    group_id: &str,
    expected: [&str; N],
) -> Value {
    let expected = sorted_accounts(expected);
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let _ = run_json(home, &["--account", account, "sync"]);
        let admins = run_json(home, &["--account", account, "groups", "admins", group_id]);
        if admin_accounts(&admins) == expected {
            return admins;
        }
        last = admins;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not see expected admins in <REDACTED_GROUP>; {}",
        json_value_summary("last_admins", &last)
    );
}

pub fn wait_until_chat_visible(
    home: &std::path::Path,
    relay: &str,
    account: &str,
    group_id: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let chats = run_json_with_relay(home, relay, &["--account", account, "chats", "list"]);
        if chats["chats"]
            .as_array()
            .is_some_and(|chats| chats.iter().any(|chat| chat["group_id"] == group_id))
        {
            return chats;
        }
        last = chats;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not project <REDACTED_GROUP> via daemon; {}",
        json_value_summary("last_chats", &last)
    );
}

pub fn wait_until_projected_message(
    home: &std::path::Path,
    relay: &str,
    account: &str,
    group_id: &str,
    plaintext: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let messages = run_json_with_relay(
            home,
            relay,
            &["--account", account, "message", "list", "--group", group_id],
        );
        if message_plaintexts(&messages)
            .iter()
            .any(|message| message == plaintext)
        {
            return messages;
        }
        last = messages;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not project <REDACTED_MESSAGE> via daemon; {}",
        json_value_summary("last_messages", &last)
    );
}

pub fn wait_until_projected_agent_stream_message(
    home: &std::path::Path,
    relay: &str,
    account: &str,
    group_id: &str,
    stream_id: &str,
    kind: &str,
) -> Value {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let messages = run_json_with_relay(
            home,
            relay,
            &["--account", account, "message", "list", "--group", group_id],
        );
        if let Some(message) = messages["messages"].as_array().and_then(|messages| {
            messages.iter().find(|message| {
                message["agent_text_stream"]["kind"] == kind
                    && message["agent_text_stream"]["stream_id"] == stream_id
            })
        }) {
            return message.clone();
        }
        last = messages;
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "account <REDACTED_ACCOUNT> did not project <REDACTED_STREAM> via daemon; {}",
        json_value_summary("last_messages", &last)
    );
}

pub fn wait_for_daemon(socket: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let output = Command::new(env!("CARGO_BIN_EXE_dm"))
            .arg("--socket")
            .arg(socket)
            .arg("--json")
            .args(["daemon", "status"])
            .output()
            .expect("dm daemon status should start");
        if output.status.success() {
            let value: Value =
                serde_json::from_slice(&output.stdout).expect("status stdout should be JSON");
            if value["result"]["running"].as_bool() == Some(true) {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("daemon did not become ready at {}", socket.display());
}

pub fn stop_daemon(socket: &std::path::Path, child: &mut Child) {
    let _ = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(socket)
        .arg("--json")
        .args(["daemon", "stop"])
        .output();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(Some(_)) = child.try_wait() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
}

pub struct JsonLineSubscription {
    child: Child,
    lines: std_mpsc::Receiver<Value>,
    reader: Option<JoinHandle<()>>,
}

impl JsonLineSubscription {
    #[track_caller]
    pub fn wait_for(&self, timeout: Duration, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + timeout;
        let mut last = None;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(Duration::from_millis(100));
            match self.lines.recv_timeout(wait) {
                Ok(value) if predicate(&value) => return value,
                Ok(value) => last = Some(value),
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        panic!(
            "subscription did not emit expected line\nlast_line={}",
            last.map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_owned())
        );
    }

    #[track_caller]
    pub fn wait_until(&self, timeout: Duration, mut complete: impl FnMut(&Value) -> bool) {
        let deadline = Instant::now() + timeout;
        let mut last = None;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(Duration::from_millis(100));
            match self.lines.recv_timeout(wait) {
                Ok(value) => {
                    if complete(&value) {
                        return;
                    }
                    last = Some(value);
                }
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        panic!(
            "subscription did not emit expected lines\nlast_line={}",
            last.map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_owned())
        );
    }
}

impl Drop for JsonLineSubscription {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

pub fn spawn_json_subscription(home: &std::path::Path, args: &[&str]) -> JsonLineSubscription {
    spawn_json_subscription_with_command(dm(home), args)
}

pub fn spawn_json_subscription_without_relay(
    home: &std::path::Path,
    args: &[&str],
) -> JsonLineSubscription {
    spawn_json_subscription_with_command(dm_without_relay(home), args)
}

pub fn spawn_json_subscription_with_command(
    mut command: Command,
    args: &[&str],
) -> JsonLineSubscription {
    let mut child = command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("subscription should start");
    let stdout = child.stdout.take().expect("subscription stdout");
    let (tx, rx) = std_mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            let value = serde_json::from_str::<Value>(&line)
                .unwrap_or_else(|err| panic!("subscription line should be JSON: {err}; {line}"));
            if tx.send(value).is_err() {
                break;
            }
        }
    });
    JsonLineSubscription {
        child,
        lines: rx,
        reader: Some(reader),
    }
}
