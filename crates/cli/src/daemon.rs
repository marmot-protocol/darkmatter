use std::ffi::OsString;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::{fs::PermissionsExt, process::CommandExt};

use clap::Parser;
#[cfg(test)]
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};

use crate::{
    Cli, CliOutput, DaemonCommand, SecretStoreKind, create_private_dir_all,
    open_private_append_file, resolve_home, write_private_file,
};
#[cfg(test)]
use crate::{is_timeline_messages_subscribe, timeline_messages_subscribe_args};

const DAEMON_SOCKET_DIR_MODE: u32 = 0o700;
const DAEMON_SOCKET_MODE: u32 = 0o600;

#[derive(Parser, Debug)]
#[command(
    name = "dmd",
    about = "Darkmatter background runtime daemon for live subscriptions and stream previews"
)]
struct DaemonArgs {
    #[arg(long, value_name = "PATH", help = "Use this Darkmatter data directory")]
    home: Option<PathBuf>,
    #[arg(long, value_name = "PATH", help = "Alias for --home")]
    data_dir: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Write daemon logs in this directory"
    )]
    logs_dir: Option<PathBuf>,
    #[arg(long, value_name = "PATH", help = "Listen on this Unix socket")]
    socket: Option<PathBuf>,
    #[arg(long, value_name = "URL", hide = true)]
    relay: Option<String>,
    #[arg(
        long,
        value_name = "URLS",
        value_delimiter = ',',
        help = "Comma-separated discovery relays for profiles, relay lists, and KeyPackages"
    )]
    discovery_relays: Vec<String>,
    #[arg(
        long,
        value_name = "URLS",
        value_delimiter = ',',
        help = "Comma-separated default account relays used when creating identities"
    )]
    default_account_relays: Vec<String>,
    #[arg(
        long,
        value_enum,
        value_name = "STORE",
        help = "Store account secrets in the OS keychain or local files"
    )]
    secret_store: Option<SecretStoreKind>,
    #[arg(
        long,
        value_name = "SERVICE",
        help = "Use this OS keychain service name for local secret storage"
    )]
    keychain_service: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct DaemonDefaults {
    home: PathBuf,
    socket: PathBuf,
    pid_path: PathBuf,
    log_path: PathBuf,
    relay: Option<String>,
    discovery_relays: Vec<String>,
    default_account_relays: Vec<String>,
    secret_store: Option<SecretStoreKind>,
    keychain_service: Option<String>,
}

mod client;
mod dispatch;
mod paths;
mod runtime_bridge;
mod state;
mod stream_compose;
mod subscriptions;
mod wire;

#[allow(unused_imports)]
use dispatch::{
    MAX_DAEMON_REQUEST_BYTES, blocked_daemon_execute_command, blocked_daemon_execute_output,
    handle_connection, read_daemon_request, write_daemon_output,
};

#[allow(unused_imports)]
use runtime_bridge::{
    AppRuntimeRefresh, app_runtime_account_setup_request, app_runtime_enabled,
    app_runtime_refresh_after_execute, apply_default_account_relays, apply_defaults,
    auto_watch_agent_stream_starts, empty_runtime_activity_report,
    handle_app_runtime_account_setup_request, handle_app_runtime_command_request,
    handle_app_runtime_event, is_hosted_runtime_command, open_app_runtime, reconcile_app_runtime,
    record_runtime_activity_error, record_runtime_activity_report, refresh_app_runtime,
    resolve_app_runtime_account_id, runtime_activity_report_from_summary, runtime_message_json,
    spawn_app_runtime_bridge,
};

#[allow(unused_imports)]
use stream_compose::{
    append_stream_compose, append_stream_compose_text, cancel_stream_compose,
    finish_stream_compose, finish_stream_compose_report, flush_pending_live_text,
    handle_stream_compose_request, open_stream_compose, run_hosted_stream_marker_cli_json,
    run_stream_compose_session, short_id, stream_compose_key, validate_stream_chunk_bytes,
};

#[cfg(test)]
#[allow(unused_imports)]
use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_RECORD_TEXT_DELTA, AgentTextStreamTranscriptV1,
};
#[cfg(test)]
#[allow(unused_imports)]
use state::StreamComposeCommand;
#[cfg(test)]
#[allow(unused_imports)]
use tokio::sync::{mpsc, oneshot};
#[cfg(test)]
#[allow(unused_imports)]
use transport_quic_broker::{BrokerTextPublisher, OpenBrokerTextPublisher};

#[allow(unused_imports)]
use subscriptions::{
    cli_output_result, handle_chats_subscription, handle_group_state_subscription,
    handle_messages_subscription, message_stream_response, messages_subscribe_args,
    spawn_stream_watch, start_stream_watch, stream_response_matches_subscription,
};

use state::{DaemonEventHub, DaemonState, DaemonWorkers, StreamWatchWorkers};

#[cfg(test)]
use client::stream_result_plain;
pub use client::{DaemonClient, DaemonClientError};
pub(crate) use client::{
    send_chats_subscribe, send_execute, send_group_state_subscribe, send_messages_subscribe,
    send_stream_watch,
};
pub use paths::{default_log_path, default_pid_path, default_socket_path};
pub(crate) use wire::DaemonRequest;
pub use wire::{
    DaemonOutgoingStreamReport, DaemonRuntimeActivityReport, DaemonStatus, DaemonStreamError,
    DaemonStreamResponse, DaemonStreamWatchReport,
};

pub async fn run_server_from<I, T>(args: I) -> CliOutput
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let argv = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let args = match DaemonArgs::try_parse_from(argv) {
        Ok(args) => args,
        Err(err) => {
            return CliOutput {
                code: err.exit_code(),
                stdout: String::new(),
                stderr: err.to_string(),
            };
        }
    };

    server_output("dmd", run_server(args).await)
}

fn server_output(
    label: &str,
    result: Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> CliOutput {
    match result {
        Ok(()) => CliOutput {
            code: 0,
            stdout: String::new(),
            stderr: String::new(),
        },
        Err(err) => CliOutput {
            code: 1,
            stdout: String::new(),
            stderr: format!("{label}: {err}\n"),
        },
    }
}

pub(crate) async fn run_daemon_command(cli: Cli, command: DaemonCommand) -> CliOutput {
    match command {
        DaemonCommand::Start {
            discovery_relays,
            default_account_relays,
        } => {
            let home = resolve_home(cli.home.clone());
            let socket = cli
                .socket
                .clone()
                .or_else(|| std::env::var_os("DM_SOCKET").map(PathBuf::from))
                .unwrap_or_else(|| default_socket_path(&home));
            start_daemon(
                &cli,
                &home,
                &socket,
                discovery_relays,
                default_account_relays,
            )
            .await
        }
        DaemonCommand::Stop => {
            let home = resolve_home(cli.home.clone());
            let socket = cli
                .socket
                .clone()
                .or_else(|| std::env::var_os("DM_SOCKET").map(PathBuf::from))
                .unwrap_or_else(|| default_socket_path(&home));
            stop_daemon(cli.json, &socket).await
        }
        DaemonCommand::Status => {
            let home = resolve_home(cli.home.clone());
            let socket = cli
                .socket
                .clone()
                .or_else(|| std::env::var_os("DM_SOCKET").map(PathBuf::from))
                .unwrap_or_else(|| default_socket_path(&home));
            status_daemon(cli.json, &socket).await
        }
    }
}

async fn run_server(args: DaemonArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let home = resolve_home(args.home.or(args.data_dir));
    let socket = args
        .socket
        .clone()
        .unwrap_or_else(|| default_socket_path(&home));
    let pid_path = default_pid_path(&home);
    let log_path = args
        .logs_dir
        .clone()
        .map(|logs_dir| logs_dir.join("dmd.log"))
        .unwrap_or_else(|| default_log_path(&home));
    if let Some(parent) = socket.parent() {
        prepare_socket_dir(parent, &home)?;
    }
    if let Some(parent) = pid_path.parent() {
        create_private_dir_all(parent)?;
    }
    remove_stale_socket(&socket).await?;
    remove_stale_pid(&pid_path).await?;

    let listener = UnixListener::bind(&socket)?;
    harden_socket_permissions(&socket)?;
    write_pid_file(&pid_path)?;
    let hidden_relay = crate::resolve_relay(args.relay)?;
    let mut discovery_relays = normalize_relay_list(args.discovery_relays)?;
    let mut default_account_relays = normalize_relay_list(args.default_account_relays)?;
    if discovery_relays.is_empty() {
        if let Some(relay) = hidden_relay.clone() {
            discovery_relays.push(relay);
        } else if !default_account_relays.is_empty() {
            discovery_relays = default_account_relays.clone();
        }
    }
    if default_account_relays.is_empty() {
        if !discovery_relays.is_empty() {
            default_account_relays = discovery_relays.clone();
        } else if let Some(relay) = hidden_relay.clone() {
            default_account_relays.push(relay);
        }
    }
    let relay = hidden_relay
        .or_else(|| discovery_relays.first().cloned())
        .or_else(|| default_account_relays.first().cloned())
        .ok_or(crate::DmError::MissingRelay)?;
    let defaults = DaemonDefaults {
        home,
        socket: socket.clone(),
        pid_path: pid_path.clone(),
        log_path,
        relay: Some(relay),
        discovery_relays,
        default_account_relays,
        secret_store: args.secret_store,
        keychain_service: args.keychain_service,
    };
    let state = Arc::new(Mutex::new(DaemonState {
        pid: std::process::id(),
        started_at: unix_now(),
        last_runtime_activity: None,
    }));
    let events = DaemonEventHub::new();
    let mut workers = DaemonWorkers::default();
    reconcile_app_runtime(
        &defaults,
        state.clone(),
        events.clone(),
        &mut workers.runtime,
    )
    .await;
    let shutdown_result = loop {
        let (mut stream, _) = listener.accept().await?;
        if let Err(err) = authorize_daemon_peer(&stream) {
            write_daemon_output(
                &mut stream,
                &CliOutput {
                    code: 1,
                    stdout: String::new(),
                    stderr: format!("error: {err}\n"),
                },
            )
            .await;
            continue;
        }
        let request = read_daemon_request(&mut stream).await?;
        match request {
            DaemonRequest::MessagesSubscribe { mut cli } => {
                apply_defaults(&mut cli, &defaults);
                reconcile_app_runtime(
                    &defaults,
                    state.clone(),
                    events.clone(),
                    &mut workers.runtime,
                )
                .await;
                let defaults = defaults.clone();
                let state = state.clone();
                let events = events.clone();
                let runtime = workers.runtime.runtime.clone();
                tokio::spawn(async move {
                    let _ = handle_messages_subscription(
                        &mut stream,
                        &defaults,
                        state,
                        events,
                        runtime,
                        *cli,
                    )
                    .await;
                });
            }
            DaemonRequest::ChatsSubscribe { mut cli } => {
                apply_defaults(&mut cli, &defaults);
                reconcile_app_runtime(
                    &defaults,
                    state.clone(),
                    events.clone(),
                    &mut workers.runtime,
                )
                .await;
                let defaults = defaults.clone();
                let runtime = workers.runtime.runtime.clone();
                tokio::spawn(async move {
                    let _ = handle_chats_subscription(&mut stream, &defaults, runtime, *cli).await;
                });
            }
            DaemonRequest::GroupStateSubscribe { mut cli } => {
                apply_defaults(&mut cli, &defaults);
                reconcile_app_runtime(
                    &defaults,
                    state.clone(),
                    events.clone(),
                    &mut workers.runtime,
                )
                .await;
                let defaults = defaults.clone();
                let runtime = workers.runtime.runtime.clone();
                tokio::spawn(async move {
                    let _ = handle_group_state_subscription(&mut stream, &defaults, runtime, *cli)
                        .await;
                });
            }
            request => {
                let should_shutdown = handle_connection(
                    request,
                    &mut stream,
                    &defaults,
                    state.clone(),
                    events.clone(),
                    &mut workers,
                )
                .await?;
                if should_shutdown {
                    break Ok(());
                }
            }
        }
    };

    workers.abort_all().await;
    let _ = std::fs::remove_file(&socket);
    let _ = std::fs::remove_file(&pid_path);
    shutdown_result
}

fn prepare_socket_dir(parent: &Path, home: &Path) -> std::io::Result<()> {
    let existed = parent.try_exists()?;
    std::fs::create_dir_all(parent)?;
    if !existed || is_daemon_owned_socket_dir(parent, home) {
        std::fs::set_permissions(
            parent,
            std::fs::Permissions::from_mode(DAEMON_SOCKET_DIR_MODE),
        )?;
    }
    Ok(())
}

fn is_daemon_owned_socket_dir(parent: &Path, home: &Path) -> bool {
    let dev_dir = home.join("dev");
    parent == dev_dir || parent.starts_with(dev_dir)
}

fn harden_socket_permissions(socket: &Path) -> std::io::Result<()> {
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(DAEMON_SOCKET_MODE))
}

fn authorize_daemon_peer(stream: &UnixStream) -> std::io::Result<()> {
    let peer_uid = stream.peer_cred()?.uid();
    let server_uid = current_effective_uid();
    if daemon_peer_uid_authorized(peer_uid, server_uid) {
        return Ok(());
    }
    Err(std::io::Error::new(
        ErrorKind::PermissionDenied,
        "daemon peer UID does not match server UID",
    ))
}

fn current_effective_uid() -> libc::uid_t {
    unsafe { libc::geteuid() }
}

fn daemon_peer_uid_authorized(peer_uid: libc::uid_t, server_uid: libc::uid_t) -> bool {
    peer_uid == server_uid
}

async fn start_daemon(
    cli: &Cli,
    home: &Path,
    socket: &Path,
    mut discovery_relays: Vec<String>,
    mut default_account_relays: Vec<String>,
) -> CliOutput {
    if let Ok(status) = DaemonClient::new(socket).status().await {
        return daemon_output(
            cli.json,
            "daemon already running",
            daemon_status_json(status),
            0,
        );
    }
    discovery_relays = match normalize_relay_list(discovery_relays) {
        Ok(relays) => relays,
        Err(err) => return daemon_error(cli.json, relay_error_code(&err), err.to_string()),
    };
    default_account_relays = match normalize_relay_list(default_account_relays) {
        Ok(relays) => relays,
        Err(err) => return daemon_error(cli.json, relay_error_code(&err), err.to_string()),
    };
    let hidden_relay = match crate::resolve_relay(cli.relay.clone()) {
        Ok(relay) => relay,
        Err(err) => return daemon_error(cli.json, relay_error_code(&err), err.to_string()),
    };
    if discovery_relays.is_empty()
        && default_account_relays.is_empty()
        && let Some(relay) = hidden_relay.clone()
    {
        discovery_relays.push(relay.clone());
        default_account_relays.push(relay);
    }
    if discovery_relays.is_empty() && !default_account_relays.is_empty() {
        discovery_relays = default_account_relays.clone();
    }
    if default_account_relays.is_empty() && !discovery_relays.is_empty() {
        default_account_relays = discovery_relays.clone();
    }
    if discovery_relays.is_empty() && default_account_relays.is_empty() {
        return daemon_error(
            cli.json,
            "missing_relay_url",
            crate::DmError::MissingRelay.to_string(),
        );
    }

    let executable = match daemon_executable() {
        Ok(path) => path,
        Err(err) => {
            return daemon_error(cli.json, "daemon_start_failed", err.to_string());
        }
    };

    let mut command = Command::new(executable);
    command.arg("--home").arg(home);
    command.arg("--socket").arg(socket);
    if !discovery_relays.is_empty() {
        command
            .arg("--discovery-relays")
            .arg(discovery_relays.join(","));
    }
    if !default_account_relays.is_empty() {
        command
            .arg("--default-account-relays")
            .arg(default_account_relays.join(","));
    }
    if let Some(secret_store) = cli.secret_store {
        command.arg("--secret-store").arg(secret_store.as_str());
    }
    if let Some(keychain_service) = &cli.keychain_service {
        command.arg("--keychain-service").arg(keychain_service);
    }
    detach_daemon_command(&mut command);
    let log_path = default_log_path(home);
    let log = match open_daemon_log(&log_path) {
        Ok(log) => log,
        Err(err) => return daemon_error(cli.json, "daemon_start_failed", err.to_string()),
    };
    let stderr = match log.try_clone() {
        Ok(stderr) => stderr,
        Err(err) => return daemon_error(cli.json, "daemon_start_failed", err.to_string()),
    };
    command.stdout(Stdio::from(log)).stderr(Stdio::from(stderr));

    if let Err(err) = command.spawn() {
        return daemon_error(cli.json, "daemon_start_failed", err.to_string());
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(status) = DaemonClient::new(socket).status().await {
            return daemon_output(cli.json, "daemon started", daemon_status_json(status), 0);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    daemon_error(
        cli.json,
        "daemon_start_failed",
        format!(
            "daemon did not become ready at {}; log: {}{}",
            socket.display(),
            log_path.display(),
            daemon_log_hint(&log_path)
        ),
    )
}

async fn stop_daemon(json: bool, socket: &Path) -> CliOutput {
    match DaemonClient::new(socket).shutdown().await {
        Ok(_) => daemon_output(
            json,
            "daemon stopped",
            serde_json::json!({"running": false, "socket": socket}),
            0,
        ),
        Err(err) => daemon_error(json, "daemon_unavailable", err.to_string()),
    }
}

async fn status_daemon(json: bool, socket: &Path) -> CliOutput {
    let status = DaemonClient::new(socket)
        .status()
        .await
        .ok()
        .unwrap_or_else(|| {
            let home = socket
                .parent()
                .and_then(Path::parent)
                .map(Path::to_path_buf);
            let stale_pid = home
                .as_deref()
                .and_then(|home| read_pid_file(&default_pid_path(home)).ok().flatten());
            DaemonStatus {
                running: false,
                socket: socket.to_path_buf(),
                pid: None,
                pid_file: home.as_deref().map(default_pid_path),
                stale_pid,
                started_at: None,
                log: home.as_deref().map(default_log_path),
                home,
                last_runtime_activity: None,
                relay_health: None,
                stream_watches: Vec::new(),
            }
        });
    let plain = if status.running {
        format!("daemon running\nsocket: {}", socket.display())
    } else {
        "daemon not running".to_owned()
    };
    daemon_output(json, &plain, daemon_status_json(status), 0)
}

pub(super) fn daemon_output(
    json: bool,
    plain: &str,
    result: serde_json::Value,
    code: i32,
) -> CliOutput {
    if json {
        return CliOutput {
            code,
            stdout: format!(
                "{}\n",
                serde_json::to_string(&serde_json::json!({
                    "ok": code == 0,
                    "result": result,
                }))
                .expect("JSON response serialization cannot fail")
            ),
            stderr: String::new(),
        };
    }
    CliOutput {
        code,
        stdout: format!("{plain}\n"),
        stderr: String::new(),
    }
}

fn daemon_status_json(status: DaemonStatus) -> serde_json::Value {
    serde_json::json!({
        "running": status.running,
        "socket": status.socket,
        "pid": status.pid,
        "pid_file": status.pid_file,
        "stale_pid": status.stale_pid,
        "started_at": status.started_at,
        "home": status.home,
        "log": status.log,
        "last_runtime_activity": status.last_runtime_activity,
        "relay_health": status.relay_health,
        "stream_watches": status.stream_watches,
    })
}

async fn server_status(
    defaults: &DaemonDefaults,
    state: &Arc<Mutex<DaemonState>>,
    runtime: Option<&marmot_app::MarmotAppRuntime>,
    stream_workers: &StreamWatchWorkers,
) -> DaemonStatus {
    stream_workers.reap_finished();
    let state = state.lock().ok();
    let relay_health = if let Some(runtime) = runtime {
        let shared = runtime.shared_services();
        Some(shared.relay_plane().relay_health().await)
    } else {
        None
    };
    let stream_watches = runtime
        .map(|runtime| runtime.shared_services().agent_streams().reports())
        .unwrap_or_default();
    DaemonStatus {
        running: true,
        socket: defaults.socket.clone(),
        pid: state.as_ref().map(|state| state.pid),
        pid_file: Some(defaults.pid_path.clone()),
        stale_pid: None,
        started_at: state.as_ref().map(|state| state.started_at),
        home: Some(defaults.home.clone()),
        log: Some(defaults.log_path.clone()),
        last_runtime_activity: state
            .as_ref()
            .and_then(|state| state.last_runtime_activity.clone()),
        relay_health,
        stream_watches,
    }
}

pub(super) fn daemon_error(json: bool, code: &str, message: String) -> CliOutput {
    if json {
        return CliOutput {
            code: 1,
            stdout: format!(
                "{}\n",
                serde_json::to_string(&serde_json::json!({
                    "ok": false,
                    "error": {
                        "code": code,
                        "message": message,
                    }
                }))
                .expect("JSON response serialization cannot fail")
            ),
            stderr: String::new(),
        };
    }
    CliOutput {
        code: 1,
        stdout: String::new(),
        stderr: format!("error: {message}\n"),
    }
}

fn normalize_relay_list(relays: Vec<String>) -> Result<Vec<String>, crate::DmError> {
    relays
        .into_iter()
        .map(crate::validate_relay_url)
        .collect::<Result<Vec<_>, _>>()
}

fn write_pid_file(pid_path: &Path) -> std::io::Result<()> {
    write_private_file(pid_path, format!("{}\n", std::process::id()))
}

fn read_pid_file(pid_path: &Path) -> std::io::Result<Option<u32>> {
    match std::fs::read_to_string(pid_path) {
        Ok(contents) => Ok(contents.trim().parse::<u32>().ok()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

async fn remove_stale_pid(pid_path: &Path) -> std::io::Result<()> {
    if read_pid_file(pid_path)?.is_some() {
        match std::fs::remove_file(pid_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    } else {
        Ok(())
    }
}

fn open_daemon_log(log_path: &Path) -> std::io::Result<std::fs::File> {
    let mut log = open_private_append_file(log_path)?;
    writeln!(log, "dmd start requested at {}", unix_now())?;
    Ok(log)
}

fn daemon_log_hint(log_path: &Path) -> String {
    match std::fs::read_to_string(log_path) {
        Ok(contents) if !contents.trim().is_empty() => {
            let tail = contents
                .lines()
                .rev()
                .take(5)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join(" | ");
            format!("; recent log: {tail}")
        }
        _ => String::new(),
    }
}

pub(super) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn unix_now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

async fn remove_stale_socket(
    socket: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !socket.exists() {
        return Ok(());
    }

    match client::send_request(socket, &DaemonRequest::Ping).await {
        Ok(_) => Err(std::io::Error::new(
            ErrorKind::AddrInUse,
            format!("daemon already running at {}", socket.display()),
        )
        .into()),
        Err(DaemonClientError::Connect { source, .. })
            if matches!(
                source.kind(),
                ErrorKind::ConnectionRefused | ErrorKind::NotFound
            ) =>
        {
            match std::fs::remove_file(socket) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err.into()),
            }
        }
        Err(DaemonClientError::Connect { source, .. }) => Err(source.into()),
        Err(err) => Err(std::io::Error::new(
            ErrorKind::AddrInUse,
            format!(
                "socket already exists at {} but did not respond as dmd: {err}",
                socket.display()
            ),
        )
        .into()),
    }
}

fn relay_error_code(err: &crate::DmError) -> &'static str {
    match err {
        crate::DmError::EmptyRelayUrl => "empty_relay_url",
        crate::DmError::InvalidRelayUrl(_) => "invalid_relay_url",
        _ => "relay_url_error",
    }
}

#[cfg(unix)]
fn detach_daemon_command(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn detach_daemon_command(_command: &mut Command) {}

fn daemon_executable() -> Result<PathBuf, String> {
    if let Ok(current) = std::env::current_exe()
        && let Some(parent) = current.parent()
    {
        let sibling = parent.join("dmd");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }

    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths).find_map(|dir| {
                let candidate = dir.join("dmd");
                candidate.is_file().then_some(candidate)
            })
        })
        .ok_or_else(|| "dmd not found; ensure it is built and on PATH".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cgka_traits::GroupId;
    use cgka_traits::MessageId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    #[cfg(unix)]
    fn daemon_pid_and_log_writers_create_private_files() {
        let home = tempfile::tempdir().expect("tempdir");
        let pid_path = home.path().join("dev").join("dmd.pid");
        let log_path = home.path().join("logs").join("dmd.log");

        write_pid_file(&pid_path).expect("write pid file");
        drop(open_daemon_log(&log_path).expect("open daemon log"));

        assert_eq!(
            pid_path
                .parent()
                .expect("pid parent")
                .metadata()
                .expect("pid parent metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            pid_path
                .metadata()
                .expect("pid metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            log_path
                .parent()
                .expect("log parent")
                .metadata()
                .expect("log parent metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            log_path
                .metadata()
                .expect("log metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn apply_defaults_overwrites_forwarded_cli_relay_with_daemon_relay() {
        let defaults = DaemonDefaults {
            home: PathBuf::from("/tmp/dm-daemon-home"),
            socket: PathBuf::from("/tmp/dm-daemon.sock"),
            pid_path: PathBuf::from("/tmp/dm-daemon.pid"),
            log_path: PathBuf::from("/tmp/dm-daemon.log"),
            relay: Some("wss://daemon.example".to_owned()),
            discovery_relays: vec!["wss://discovery.example".to_owned()],
            default_account_relays: vec!["wss://account.example".to_owned()],
            secret_store: Some(crate::SecretStoreKind::File),
            keychain_service: Some("daemon-keychain".to_owned()),
        };
        let mut cli = Cli {
            home: None,
            socket: Some(PathBuf::from("/tmp/forwarded.sock")),
            relay: Some("wss://client.example".to_owned()),
            daemon_discovery_relays: Vec::new(),
            daemon_default_account_relays: Vec::new(),
            secret_store: None,
            keychain_service: None,
            account: None,
            json: true,
            command: crate::Command::Sync,
        };

        apply_defaults(&mut cli, &defaults);

        assert_eq!(cli.relay.as_deref(), Some("wss://daemon.example"));
        assert_eq!(cli.socket, None);
    }

    #[test]
    fn apply_defaults_overwrites_client_storage_scope_with_daemon_defaults() {
        let defaults = DaemonDefaults {
            home: PathBuf::from("/tmp/dm-daemon-home"),
            socket: PathBuf::from("/tmp/dm-daemon.sock"),
            pid_path: PathBuf::from("/tmp/dm-daemon.pid"),
            log_path: PathBuf::from("/tmp/dm-daemon.log"),
            relay: Some("wss://daemon.example".to_owned()),
            discovery_relays: Vec::new(),
            default_account_relays: Vec::new(),
            secret_store: Some(crate::SecretStoreKind::File),
            keychain_service: Some("daemon-keychain".to_owned()),
        };
        let mut cli = Cli {
            home: Some(PathBuf::from("/tmp/client-selected-home")),
            socket: Some(PathBuf::from("/tmp/forwarded.sock")),
            relay: None,
            daemon_discovery_relays: Vec::new(),
            daemon_default_account_relays: Vec::new(),
            secret_store: Some(crate::SecretStoreKind::Keychain),
            keychain_service: Some("client-keychain".to_owned()),
            account: None,
            json: true,
            command: crate::Command::Sync,
        };

        apply_defaults(&mut cli, &defaults);

        assert_eq!(cli.home.as_deref(), Some(defaults.home.as_path()));
        assert_eq!(cli.secret_store, Some(crate::SecretStoreKind::File));
        assert_eq!(cli.keychain_service.as_deref(), Some("daemon-keychain"));
    }

    #[test]
    fn apply_defaults_adds_daemon_account_relays_to_account_create() {
        let defaults = DaemonDefaults {
            home: PathBuf::from("/tmp/dm-daemon-home"),
            socket: PathBuf::from("/tmp/dm-daemon.sock"),
            pid_path: PathBuf::from("/tmp/dm-daemon.pid"),
            log_path: PathBuf::from("/tmp/dm-daemon.log"),
            relay: Some("wss://daemon.example".to_owned()),
            discovery_relays: vec!["wss://discovery.example".to_owned()],
            default_account_relays: vec!["wss://account.example".to_owned()],
            secret_store: Some(crate::SecretStoreKind::File),
            keychain_service: Some("daemon-keychain".to_owned()),
        };
        let mut cli = Cli {
            home: None,
            socket: Some(PathBuf::from("/tmp/forwarded.sock")),
            relay: None,
            daemon_discovery_relays: Vec::new(),
            daemon_default_account_relays: Vec::new(),
            secret_store: None,
            keychain_service: None,
            account: None,
            json: true,
            command: crate::Command::Account {
                command: crate::AccountCommand::Create {
                    identity: None,
                    nsec_stdin: false,
                    default_relays: Vec::new(),
                    bootstrap_relays: Vec::new(),
                    publish_missing_relay_lists: false,
                },
            },
        };

        apply_defaults(&mut cli, &defaults);

        let crate::Command::Account {
            command:
                crate::AccountCommand::Create {
                    default_relays,
                    bootstrap_relays,
                    ..
                },
        } = cli.command
        else {
            panic!("expected account create command");
        };
        assert_eq!(default_relays, vec!["wss://account.example"]);
        assert_eq!(bootstrap_relays, vec!["wss://discovery.example"]);
    }

    fn test_stream_compose_open(
        stream_id: Vec<u8>,
        start_event_id: MessageId,
    ) -> OpenBrokerTextPublisher {
        OpenBrokerTextPublisher {
            broker_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
            server_name: "localhost".to_owned(),
            trust: transport_quic_broker::BrokerServerTrust::InsecureLocal,
            stream_id,
            start_event_id,
            crypto: None,
        }
    }

    fn test_stream_compose_report(stream_id: &[u8]) -> DaemonOutgoingStreamReport {
        DaemonOutgoingStreamReport {
            account: Some("account".to_owned()),
            group_id: hex::encode([0x11; 32]),
            stream_id: hex::encode(stream_id),
            start_message_id: hex::encode([0x22; 32]),
            candidate: "quic://127.0.0.1:9".to_owned(),
            status: "streaming".to_owned(),
            text: String::new(),
            transcript_hash: None,
            chunk_count: 0,
            error: None,
        }
    }

    fn expected_stream_transcript_hash(
        stream_id: &[u8],
        start_event_id: &MessageId,
        text: &str,
        chunk_bytes: usize,
    ) -> String {
        expected_stream_transcript_hash_for_appends(stream_id, start_event_id, &[text], chunk_bytes)
    }

    fn expected_stream_transcript_hash_for_appends(
        stream_id: &[u8],
        start_event_id: &MessageId,
        appends: &[&str],
        chunk_bytes: usize,
    ) -> String {
        let mut transcript =
            AgentTextStreamTranscriptV1::new(stream_id.to_vec(), start_event_id.clone());
        let mut seq = 1_u64;
        for text in appends {
            for chunk in transport_quic_stream::split_text_deltas(text, chunk_bytes) {
                transcript.append(seq, AGENT_TEXT_STREAM_RECORD_TEXT_DELTA, &chunk);
                seq += 1;
            }
        }
        hex::encode(transcript.hash())
    }

    #[tokio::test]
    async fn stream_compose_returns_local_transcript_when_broker_connect_is_pending() {
        let stream_id = vec![0xaa; 32];
        let start_event_id = MessageId::new(vec![0xbb; 32]);
        let open = test_stream_compose_open(stream_id.clone(), start_event_id.clone());
        let report = test_stream_compose_report(&stream_id);
        let (tx, rx) = mpsc::channel(4);
        let session = tokio::spawn(run_stream_compose_session(open, 8, rx, report));

        let (append_tx, append_rx) = oneshot::channel();
        tx.send(StreamComposeCommand::Append {
            text: "hello ".to_owned(),
            respond: append_tx,
        })
        .await
        .unwrap();
        let appended = tokio::time::timeout(Duration::from_millis(250), append_rx)
            .await
            .expect("append should not wait for broker connect")
            .unwrap()
            .unwrap();
        assert_eq!(appended.text, "hello ");
        assert_eq!(appended.chunk_count, 1);

        let (finish_tx, finish_rx) = oneshot::channel();
        tx.send(StreamComposeCommand::Finish { respond: finish_tx })
            .await
            .unwrap();
        let finished = tokio::time::timeout(Duration::from_millis(250), finish_rx)
            .await
            .expect("finish should use local transcript fallback")
            .unwrap()
            .unwrap();

        assert_eq!(finished.status, "finished");
        assert_eq!(finished.text, "hello ");
        assert_eq!(finished.chunk_count, 1);
        assert_eq!(
            finished.transcript_hash.as_deref(),
            Some(
                expected_stream_transcript_hash(&stream_id, &start_event_id, "hello ", 8).as_str()
            )
        );

        session.await.unwrap();
    }

    #[tokio::test]
    async fn stream_compose_final_report_contains_full_transcript_text() {
        let stream_id = vec![0xcc; 32];
        let start_event_id = MessageId::new(vec![0xdd; 32]);
        let open = test_stream_compose_open(stream_id.clone(), start_event_id.clone());
        let report = test_stream_compose_report(&stream_id);
        let (tx, rx) = mpsc::channel(4);
        let session = tokio::spawn(run_stream_compose_session(open, 5, rx, report));

        for text in ["hello ", "world"] {
            let (respond, response) = oneshot::channel();
            tx.send(StreamComposeCommand::Append {
                text: text.to_owned(),
                respond,
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_millis(250), response)
                .await
                .expect("append should complete")
                .unwrap()
                .unwrap();
        }

        let (respond, response) = oneshot::channel();
        tx.send(StreamComposeCommand::Finish { respond })
            .await
            .unwrap();
        let finished = tokio::time::timeout(Duration::from_millis(250), response)
            .await
            .expect("finish should complete")
            .unwrap()
            .unwrap();

        assert_eq!(finished.text, "hello world");
        assert_eq!(finished.chunk_count, 3);
        assert_eq!(
            finished.transcript_hash.as_deref(),
            Some(
                expected_stream_transcript_hash_for_appends(
                    &stream_id,
                    &start_event_id,
                    &["hello ", "world"],
                    5,
                )
                .as_str()
            )
        );

        session.await.unwrap();
    }

    #[test]
    fn destructive_execute_commands_are_refused_over_daemon() {
        let reset = blocked_daemon_execute_output(&daemon_test_cli(crate::Command::Reset {
            confirm: true,
        }))
        .expect("reset should be blocked");
        let reset_json: serde_json::Value =
            serde_json::from_str(reset.stdout.trim()).expect("reset error JSON");
        assert_eq!(reset.code, 1);
        assert_eq!(reset_json["error"]["code"], "daemon_forbidden");
        assert_eq!(reset_json["error"]["command"], "reset");

        let logout = blocked_daemon_execute_output(&daemon_test_cli(crate::Command::Logout {
            pubkey: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        }))
        .expect("logout should be blocked");
        let logout_json: serde_json::Value =
            serde_json::from_str(logout.stdout.trim()).expect("logout error JSON");
        assert_eq!(logout.code, 1);
        assert_eq!(logout_json["error"]["code"], "daemon_forbidden");
        assert_eq!(logout_json["error"]["command"], "logout");
    }

    #[tokio::test]
    async fn daemon_peer_authorization_accepts_current_uid() {
        let (stream, _peer) = UnixStream::pair().expect("unix stream pair");

        authorize_daemon_peer(&stream).expect("same-uid peer should be authorized");
    }

    #[test]
    fn daemon_peer_authorization_rejects_mismatched_uid_value() {
        let current_uid = current_effective_uid();
        let other_uid = current_uid.checked_add(1).unwrap_or(current_uid - 1);

        assert!(!daemon_peer_uid_authorized(other_uid, current_uid));
    }

    #[tokio::test]
    async fn daemon_request_reader_rejects_oversized_requests() {
        let (mut server, mut client) = UnixStream::pair().expect("unix stream pair");
        let writer = tokio::spawn(async move {
            let oversized = vec![b'{'; MAX_DAEMON_REQUEST_BYTES + 1];
            client
                .write_all(&oversized)
                .await
                .expect("write oversized request");
            client.shutdown().await.expect("shutdown client");
        });

        let err = read_daemon_request(&mut server)
            .await
            .expect_err("oversized request should fail");

        assert!(
            err.to_string().contains("daemon request exceeds"),
            "unexpected error: {err}"
        );
        writer.await.expect("writer task");
    }

    fn daemon_test_cli(command: crate::Command) -> Cli {
        Cli {
            home: None,
            socket: None,
            relay: None,
            daemon_discovery_relays: Vec::new(),
            daemon_default_account_relays: Vec::new(),
            secret_store: None,
            keychain_service: None,
            account: None,
            json: true,
            command,
        }
    }

    #[test]
    fn runtime_message_json_marks_account_label_sender_as_me() {
        let message = marmot_app::ReceivedMessage {
            message_id_hex: "01".to_owned(),
            source_message_id_hex: "source-01".to_owned(),
            sender: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            sender_display_name: Some("Alice Example".to_owned()),
            group_id: GroupId::new(vec![0xab; 32]),
            plaintext: "hello".to_owned(),
            kind: cgka_traits::MARMOT_APP_EVENT_KIND_CHAT,
            tags: Vec::new(),
        };

        let value = runtime_message_json(
            &message,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Alice Example",
        );

        assert_eq!(value["direction"], "sent");
        assert_eq!(
            value["from"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            value["account_id"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(value["from_display_name"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn stream_watch_workers_reap_finished_handles_on_replace() {
        let workers = StreamWatchWorkers::default();
        workers.replace("finished".to_owned(), tokio::spawn(async {}));
        for _ in 0..10 {
            tokio::task::yield_now().await;
            if workers
                .handles
                .lock()
                .map(|handles| handles["finished"].is_finished())
                .unwrap_or(false)
            {
                break;
            }
        }

        workers.replace(
            "running".to_owned(),
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }),
        );

        let handles = workers.handles.lock().expect("worker lock");
        assert!(!handles.contains_key("finished"));
        assert!(handles.contains_key("running"));
        handles["running"].abort();
    }

    #[test]
    fn runtime_message_json_carries_named_peer_display_name() {
        let message = marmot_app::ReceivedMessage {
            message_id_hex: "02".to_owned(),
            source_message_id_hex: "source-02".to_owned(),
            sender: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            sender_display_name: Some("Bob Example".to_owned()),
            group_id: GroupId::new(vec![0xcd; 32]),
            plaintext: "hello back".to_owned(),
            kind: cgka_traits::MARMOT_APP_EVENT_KIND_CHAT,
            tags: Vec::new(),
        };

        let value = runtime_message_json(
            &message,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Alice Example",
        );

        assert_eq!(value["direction"], "received");
        assert_eq!(
            value["from"],
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        assert_eq!(
            value["account_id"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(value["from_display_name"], "Bob Example");
    }

    #[test]
    fn message_subscription_filters_group_events_by_account() {
        let response = DaemonStreamResponse::ok(serde_json::json!({
            "type": "message",
            "message": {
                "account_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "group_id": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "message_id": "01",
                "plaintext": "wrong account copy"
            }
        }));

        assert!(!stream_response_matches_subscription(
            &response,
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        assert!(stream_response_matches_subscription(
            &response,
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
        assert!(stream_response_matches_subscription(
            &response,
            None,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
    }

    #[test]
    fn messages_subscribe_args_allow_all_groups() {
        let cli = Cli {
            home: None,
            socket: None,
            relay: None,
            daemon_discovery_relays: Vec::new(),
            daemon_default_account_relays: Vec::new(),
            secret_store: None,
            keychain_service: None,
            account: None,
            json: true,
            command: crate::Command::Messages {
                command: crate::MessageCommand::Subscribe {
                    group: None,
                    limit: Some(250),
                },
            },
        };

        assert_eq!(messages_subscribe_args(&cli), Ok((None, Some(200))));
    }

    #[test]
    fn timeline_messages_subscribe_is_routed_by_command_shape() {
        let cli = Cli {
            home: None,
            socket: None,
            relay: None,
            daemon_discovery_relays: Vec::new(),
            daemon_default_account_relays: Vec::new(),
            secret_store: None,
            keychain_service: None,
            account: None,
            json: true,
            command: crate::Command::Messages {
                command: crate::MessageCommand::Timeline {
                    command: crate::MessageTimelineCommand::Subscribe {
                        group: Some("not-hex".to_owned()),
                        limit: Some(25),
                    },
                },
            },
        };

        assert!(is_timeline_messages_subscribe(&cli));
        assert!(timeline_messages_subscribe_args(&cli).is_err());
    }

    #[test]
    fn timeline_stream_plain_output_is_human_readable() {
        let ready = serde_json::json!({
            "type": "timeline_subscription_ready",
            "group_id": "aa"
        });
        assert_eq!(
            stream_result_plain(&ready),
            "timeline subscription ready group=aa"
        );

        let page = serde_json::json!({
            "type": "initial_timeline_page",
            "has_more_before": true,
            "has_more_after": false,
            "messages": [
                {
                    "group_id": "aa",
                    "from": "alice",
                    "plaintext": "hello",
                    "deleted": false
                }
            ]
        });

        assert_eq!(
            stream_result_plain(&page),
            "initial timeline page has_more_before=true has_more_after=false\ngroup=aa from=alice: hello"
        );
    }

    #[test]
    fn message_subscription_filters_stream_updates_by_account_when_present() {
        let scoped_delta = DaemonStreamResponse::ok(serde_json::json!({
            "type": "agent_stream_delta",
            "agent_stream_delta": {
                "account": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "group_id": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "stream_id": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "text": "hello"
            }
        }));
        let accountless_preview = DaemonStreamResponse::ok(serde_json::json!({
            "type": "stream_preview",
            "stream_preview": {
                "group_id": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "stream_id": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "status": "running",
                "text": "hello"
            }
        }));

        assert!(!stream_response_matches_subscription(
            &scoped_delta,
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        assert!(stream_response_matches_subscription(
            &scoped_delta,
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
        assert!(stream_response_matches_subscription(
            &accountless_preview,
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
    }
}
