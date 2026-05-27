use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::{fs::PermissionsExt, process::CommandExt};

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use transport_quic_broker::{BrokerTextPublisher, OpenBrokerTextPublisher};

use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_MAX_PLAINTEXT_FRAME_LEN, AGENT_TEXT_STREAM_RECORD_TEXT_DELTA,
    AgentTextStreamTranscriptV1,
};

use crate::{
    Cli, CliOutput, DaemonCommand, SecretStoreKind, create_private_dir_all,
    open_private_append_file, resolve_home, write_private_file,
};

const MAX_DAEMON_REQUEST_BYTES: usize = 1024 * 1024;
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
struct DaemonDefaults {
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
mod paths;
mod state;
mod subscriptions;
mod wire;

#[allow(unused_imports)]
use subscriptions::{
    cli_output_result, handle_chats_subscription, handle_group_state_subscription,
    handle_messages_subscription, message_stream_response, messages_subscribe_args,
    spawn_stream_watch, start_stream_watch, stream_response_matches_subscription,
};

use state::{
    AppRuntimeHost, DaemonEventHub, DaemonState, DaemonWorkers, StreamComposeCommand,
    StreamComposeSession, StreamComposeWorkers, StreamWatchWorkers,
};

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

async fn handle_connection(
    request: DaemonRequest,
    stream: &mut UnixStream,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    workers: &mut DaemonWorkers,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let (shutdown, output) = match request {
        DaemonRequest::Ping => (
            false,
            CliOutput {
                code: 0,
                stdout: String::new(),
                stderr: String::new(),
            },
        ),
        DaemonRequest::Status => {
            let status = server_status(
                defaults,
                &state,
                workers.runtime.runtime.as_ref(),
                &workers.runtime.stream_watch,
            )
            .await;
            (
                false,
                CliOutput {
                    code: 0,
                    stdout: serde_json::to_string(&status)?,
                    stderr: String::new(),
                },
            )
        }
        DaemonRequest::Shutdown => (
            true,
            CliOutput {
                code: 0,
                stdout: String::new(),
                stderr: String::new(),
            },
        ),
        DaemonRequest::StreamWatch { mut cli } => {
            apply_defaults(&mut cli, defaults);
            reconcile_app_runtime(
                defaults,
                state.clone(),
                events.clone(),
                &mut workers.runtime,
            )
            .await;
            let output = start_stream_watch(
                *cli,
                defaults,
                workers.runtime.runtime.as_ref(),
                &workers.runtime.stream_watch,
            )
            .await;
            (false, output)
        }
        DaemonRequest::MessagesSubscribe { .. } => (
            false,
            daemon_error(
                false,
                "invalid_daemon_request",
                "messages subscribe must use the streaming daemon path".to_owned(),
            ),
        ),
        DaemonRequest::ChatsSubscribe { .. } => (
            false,
            daemon_error(
                false,
                "invalid_daemon_request",
                "chats subscribe must use the streaming daemon path".to_owned(),
            ),
        ),
        DaemonRequest::GroupStateSubscribe { .. } => (
            false,
            daemon_error(
                false,
                "invalid_daemon_request",
                "groups subscribe-state must use the streaming daemon path".to_owned(),
            ),
        ),
        DaemonRequest::Execute { mut cli } => {
            apply_defaults(&mut cli, defaults);
            if let Some(output) = blocked_daemon_execute_output(cli.as_ref()) {
                write_daemon_output(stream, &output).await;
                return Ok(false);
            }
            if let Some(output) = handle_stream_compose_request(
                &cli,
                defaults,
                state.clone(),
                events.clone(),
                &mut workers.runtime,
                &mut workers.stream_compose,
            )
            .await
            {
                write_daemon_output(stream, &output).await;
                return Ok(false);
            }
            let refresh = app_runtime_refresh_after_execute(&cli);
            if let Some(output) = handle_app_runtime_account_setup_request(
                &cli,
                defaults,
                state.clone(),
                events.clone(),
                &mut workers.runtime,
            )
            .await
            {
                write_daemon_output(stream, &output).await;
                return Ok(false);
            }
            if let Some(output) = handle_app_runtime_command_request(
                &cli,
                defaults,
                state.clone(),
                events.clone(),
                &mut workers.runtime,
            )
            .await
            {
                write_daemon_output(stream, &output).await;
                return Ok(false);
            }
            let output = crate::run_cli_local(*cli).await;
            if output.code == 0 {
                refresh_app_runtime(
                    defaults,
                    state.clone(),
                    events.clone(),
                    &mut workers.runtime,
                    refresh,
                )
                .await;
            }
            (false, output)
        }
    };

    write_daemon_output(stream, &output).await;
    Ok(shutdown)
}

fn blocked_daemon_execute_output(cli: &Cli) -> Option<CliOutput> {
    let (command, reason) = blocked_daemon_execute_command(&cli.command)?;
    let message = format!("{command} cannot be run through dmd: {reason}");
    if cli.json {
        return Some(CliOutput {
            code: 1,
            stdout: format!(
                "{}\n",
                serde_json::to_string(&serde_json::json!({
                    "ok": false,
                    "error": {
                        "code": "daemon_forbidden",
                        "message": message,
                        "command": command,
                        "reason": reason,
                    },
                }))
                .expect("JSON response serialization cannot fail")
            ),
            stderr: String::new(),
        });
    }
    Some(CliOutput {
        code: 1,
        stdout: String::new(),
        stderr: format!("error: {message}\n"),
    })
}

fn blocked_daemon_execute_command(
    command: &crate::Command,
) -> Option<(&'static str, &'static str)> {
    match command {
        crate::Command::Reset { .. } => Some((
            "reset",
            "it deletes the daemon home; run dm reset directly after stopping the daemon",
        )),
        crate::Command::Logout { .. } => Some((
            "logout",
            "it removes a local account; run dm logout directly without --socket",
        )),
        _ => None,
    }
}

async fn write_daemon_output(stream: &mut UnixStream, output: &CliOutput) {
    let Ok(mut response) = serde_json::to_vec(output) else {
        return;
    };
    response.push(b'\n');
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

async fn read_daemon_request(
    stream: &mut UnixStream,
) -> Result<DaemonRequest, Box<dyn std::error::Error + Send + Sync>> {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = stream.read(&mut byte).await?;
        if read == 0 {
            if request.is_empty() {
                return Err(DaemonClientError::EmptyResponse.into());
            }
            break;
        }
        if byte[0] == b'\n' {
            break;
        }
        if request.len() == MAX_DAEMON_REQUEST_BYTES {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("daemon request exceeds {MAX_DAEMON_REQUEST_BYTES} bytes"),
            )
            .into());
        }
        request.push(byte[0]);
    }
    Ok(serde_json::from_slice(&request)?)
}

async fn handle_stream_compose_request(
    cli: &Cli,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    runtime_host: &mut AppRuntimeHost,
    workers: &mut StreamComposeWorkers,
) -> Option<CliOutput> {
    let crate::Command::Stream { command } = &cli.command else {
        return None;
    };
    match command {
        crate::StreamCommand::ComposeOpen {
            group,
            stream_id,
            quic_candidates,
            insecure_local,
            chunk_bytes,
        } => Some(
            open_stream_compose(
                cli,
                defaults,
                state,
                events,
                runtime_host,
                workers,
                group,
                stream_id.clone(),
                quic_candidates.clone(),
                *insecure_local,
                *chunk_bytes,
            )
            .await,
        ),
        crate::StreamCommand::ComposeAppend { stream_id, text } => {
            Some(append_stream_compose(cli, workers, stream_id, text.join(" ")).await)
        }
        crate::StreamCommand::ComposeFinish { stream_id } => Some(
            finish_stream_compose(
                cli,
                defaults,
                state,
                events,
                runtime_host,
                workers,
                stream_id,
            )
            .await,
        ),
        crate::StreamCommand::ComposeCancel { stream_id } => {
            Some(cancel_stream_compose(cli, workers, stream_id))
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn open_stream_compose(
    cli: &Cli,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    runtime_host: &mut AppRuntimeHost,
    workers: &mut StreamComposeWorkers,
    group: &str,
    stream_id: Option<String>,
    quic_candidates: Vec<String>,
    insecure_local: bool,
    chunk_bytes: usize,
) -> CliOutput {
    let account = cli.account.clone();
    let group_id = match crate::normalize_group_id_hex(group) {
        Ok(group_id) => group_id,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let stream_id = match stream_id
        .map(|stream_id| crate::normalize_hex(&stream_id))
        .transpose()
    {
        Ok(Some(stream_id)) => stream_id,
        Ok(None) => hex::encode(transport_quic_stream::random_stream_id()),
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let Some(candidate) = quic_candidates
        .iter()
        .find(|candidate| candidate.trim().starts_with("quic://"))
        .cloned()
    else {
        return daemon_error(
            cli.json,
            "stream_compose_failed",
            "stream compose requires a quic:// candidate".to_owned(),
        );
    };
    let parsed_candidate = match crate::parse_quic_candidate(&candidate) {
        Ok(candidate) => candidate,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let candidate_addr = match crate::resolve_quic_candidate_addr(&parsed_candidate).await {
        Ok(addr) => addr,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let trust = match crate::broker_trust(candidate_addr, None, insecure_local) {
        Ok(trust) => trust,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };

    let mut start_cli = cli.clone();
    start_cli.json = true;
    start_cli.command = crate::Command::Stream {
        command: crate::StreamCommand::Start {
            group: group_id.clone(),
            stream_id: Some(stream_id.clone()),
            quic_candidates: quic_candidates.clone(),
        },
    };
    let start =
        match run_hosted_stream_marker_cli_json(&start_cli, defaults, state, events, runtime_host)
            .await
        {
            Ok(result) => result,
            Err(err) => return daemon_error(cli.json, "stream_compose_failed", err),
        };
    let Some(start_message_id) = start
        .get("message_ids")
        .and_then(serde_json::Value::as_array)
        .and_then(|ids| ids.first())
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    else {
        return daemon_error(
            cli.json,
            "stream_compose_failed",
            "stream start did not return a start message id".to_owned(),
        );
    };
    let start_event_id = match hex::decode(&start_message_id) {
        Ok(bytes) => cgka_traits::MessageId::new(bytes),
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let stream_id_bytes = match hex::decode(&stream_id) {
        Ok(bytes) => bytes,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let crypto = {
        let Some(runtime) = runtime_host.runtime.as_ref() else {
            return daemon_error(
                cli.json,
                "stream_compose_failed",
                "app runtime is not available for stream crypto".to_owned(),
            );
        };
        let secret_store = match crate::resolve_secret_store(defaults.secret_store) {
            Ok(secret_store) => secret_store,
            Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
        };
        let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
        let account_home =
            match crate::open_account_home(&defaults.home, secret_store, &keychain_service) {
                Ok(account_home) => account_home,
                Err(err) => {
                    return daemon_error(cli.json, "stream_compose_failed", err.to_string());
                }
            };
        let app = crate::app_for(
            defaults.home.clone(),
            defaults.relay.clone(),
            account_home.clone(),
        );
        match crate::stream_crypto_for_start_event(
            &account_home,
            &app,
            runtime,
            account.as_deref(),
            Some(&group_id),
            Some(&stream_id),
            &start_message_id,
        )
        .await
        {
            Ok((_, crypto)) => Some(crypto),
            Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
        }
    };

    let key = stream_compose_key(account.as_deref(), &stream_id);
    let (tx, rx) = mpsc::channel(32);
    let report = DaemonOutgoingStreamReport {
        account,
        group_id,
        stream_id: stream_id.clone(),
        start_message_id,
        candidate: candidate.clone(),
        status: "streaming".to_owned(),
        text: String::new(),
        transcript_hash: None,
        chunk_count: 0,
        error: None,
    };
    let task_report = report.clone();
    let handle = tokio::spawn(async move {
        run_stream_compose_session(
            OpenBrokerTextPublisher {
                broker_addr: candidate_addr,
                server_name: parsed_candidate.server_name,
                trust,
                stream_id: stream_id_bytes,
                start_event_id,
                crypto,
            },
            chunk_bytes,
            rx,
            task_report,
        )
        .await;
    });
    workers.insert(key, StreamComposeSession { tx, handle });
    daemon_output(
        cli.json,
        &format!("streaming {}", short_id(&report.stream_id)),
        serde_json::json!(report),
        0,
    )
}

async fn append_stream_compose(
    cli: &Cli,
    workers: &StreamComposeWorkers,
    stream_id: &str,
    text: String,
) -> CliOutput {
    let stream_id = match crate::normalize_hex(stream_id) {
        Ok(stream_id) => stream_id,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let key = stream_compose_key(cli.account.as_deref(), &stream_id);
    let Some(session) = workers.get(&key) else {
        return daemon_error(
            cli.json,
            "stream_compose_not_found",
            format!("no active stream compose session for {stream_id}"),
        );
    };
    let (respond, response) = oneshot::channel();
    if session
        .tx
        .send(StreamComposeCommand::Append { text, respond })
        .await
        .is_err()
    {
        return daemon_error(
            cli.json,
            "stream_compose_failed",
            "stream compose session is closed".to_owned(),
        );
    }
    match response.await {
        Ok(Ok(report)) => daemon_output(
            cli.json,
            &format!("streaming {}", short_id(&report.stream_id)),
            serde_json::json!(report),
            0,
        ),
        Ok(Err(err)) => daemon_error(cli.json, "stream_compose_failed", err),
        Err(err) => daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    }
}

async fn finish_stream_compose(
    cli: &Cli,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    runtime_host: &mut AppRuntimeHost,
    workers: &mut StreamComposeWorkers,
    stream_id: &str,
) -> CliOutput {
    let stream_id = match crate::normalize_hex(stream_id) {
        Ok(stream_id) => stream_id,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let key = stream_compose_key(cli.account.as_deref(), &stream_id);
    let Some(session) = workers.remove(&key) else {
        return daemon_error(
            cli.json,
            "stream_compose_not_found",
            format!("no active stream compose session for {stream_id}"),
        );
    };
    let (respond, response) = oneshot::channel();
    if session
        .tx
        .send(StreamComposeCommand::Finish { respond })
        .await
        .is_err()
    {
        return daemon_error(
            cli.json,
            "stream_compose_failed",
            "stream compose session is closed".to_owned(),
        );
    }
    let report = match response.await {
        Ok(Ok(report)) => report,
        Ok(Err(err)) => return daemon_error(cli.json, "stream_compose_failed", err),
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    if report.text.is_empty() {
        return daemon_error(
            cli.json,
            "stream_compose_failed",
            "stream compose text is empty".to_owned(),
        );
    }
    let Some(transcript_hash) = report.transcript_hash.clone() else {
        return daemon_error(
            cli.json,
            "stream_compose_failed",
            "stream compose did not return a transcript hash".to_owned(),
        );
    };

    let mut finish_cli = cli.clone();
    finish_cli.json = true;
    finish_cli.command = crate::Command::Stream {
        command: crate::StreamCommand::Finish {
            group: report.group_id.clone(),
            stream_id: report.stream_id.clone(),
            start_event_id: report.start_message_id.clone(),
            transcript_hash,
            chunk_count: report.chunk_count,
            text: vec![report.text.clone()],
        },
    };
    if let Err(err) =
        run_hosted_stream_marker_cli_json(&finish_cli, defaults, state, events, runtime_host).await
    {
        return daemon_error(cli.json, "stream_compose_failed", err);
    }
    daemon_output(
        cli.json,
        &format!("finished stream {}", short_id(&report.stream_id)),
        serde_json::json!(report),
        0,
    )
}

fn cancel_stream_compose(
    cli: &Cli,
    workers: &mut StreamComposeWorkers,
    stream_id: &str,
) -> CliOutput {
    let stream_id = match crate::normalize_hex(stream_id) {
        Ok(stream_id) => stream_id,
        Err(err) => return daemon_error(cli.json, "stream_compose_failed", err.to_string()),
    };
    let key = stream_compose_key(cli.account.as_deref(), &stream_id);
    if let Some(session) = workers.remove(&key) {
        let _ = session.tx.try_send(StreamComposeCommand::Cancel);
        session.handle.abort();
        return daemon_output(
            cli.json,
            &format!("cancelled stream {}", short_id(&stream_id)),
            serde_json::json!({
                "stream_id": stream_id,
                "cancelled": true,
            }),
            0,
        );
    }
    daemon_error(
        cli.json,
        "stream_compose_not_found",
        format!("no active stream compose session for {stream_id}"),
    )
}

async fn run_hosted_stream_marker_cli_json(
    cli: &Cli,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    runtime_host: &mut AppRuntimeHost,
) -> Result<serde_json::Value, String> {
    let Some(output) =
        handle_app_runtime_command_request(cli, defaults, state, events, runtime_host).await
    else {
        return Err("stream marker command did not use the daemon runtime".to_owned());
    };
    cli_output_result(output)
}

async fn run_stream_compose_session(
    open: OpenBrokerTextPublisher,
    chunk_bytes: usize,
    mut rx: mpsc::Receiver<StreamComposeCommand>,
    mut report: DaemonOutgoingStreamReport,
) {
    let mut transcript = LocalComposeTranscript::new(&open);
    let mut pending_live_text = VecDeque::new();
    let mut publisher = None;
    let mut connect_task = Some(tokio::spawn(BrokerTextPublisher::connect(open)));
    let mut live_error = None;

    loop {
        let command = if let Some(task) = connect_task.as_mut() {
            tokio::select! {
                connect_result = task => {
                    connect_task = None;
                    match connect_result {
                        Ok(Ok(mut connected)) => {
                            if let Err(err) = flush_pending_live_text(
                                &mut connected,
                                &mut pending_live_text,
                                chunk_bytes,
                            )
                            .await
                            {
                                live_error = Some(err);
                            } else {
                                publisher = Some(connected);
                            }
                        }
                        Ok(Err(err)) => {
                            live_error = Some(err.to_string());
                            pending_live_text.clear();
                        }
                        Err(err) => {
                            live_error = Some(err.to_string());
                            pending_live_text.clear();
                        }
                    }
                    continue;
                }
                command = rx.recv() => command,
            }
        } else {
            rx.recv().await
        };
        let Some(command) = command else {
            if let Some(task) = connect_task {
                task.abort();
            }
            return;
        };

        match command {
            StreamComposeCommand::Append { text, respond } => {
                let result = append_stream_compose_text(
                    &mut report,
                    &mut transcript,
                    &mut publisher,
                    &mut pending_live_text,
                    &mut live_error,
                    text,
                    chunk_bytes,
                )
                .await;
                let _ = respond.send(result);
            }
            StreamComposeCommand::Finish { respond } => {
                if let Some(task) = connect_task.take() {
                    task.abort();
                }
                let result = finish_stream_compose_report(
                    &mut report,
                    &transcript,
                    &mut publisher,
                    &mut pending_live_text,
                    &mut live_error,
                    chunk_bytes,
                )
                .await;
                let _ = respond.send(result);
                return;
            }
            StreamComposeCommand::Cancel => {
                if let Some(task) = connect_task {
                    task.abort();
                }
                return;
            }
        }
    }
}

struct LocalComposeTranscript {
    transcript: AgentTextStreamTranscriptV1,
    next_seq: u64,
}

impl LocalComposeTranscript {
    fn new(open: &OpenBrokerTextPublisher) -> Self {
        Self {
            transcript: AgentTextStreamTranscriptV1::new(
                open.stream_id.clone(),
                open.start_event_id.clone(),
            ),
            next_seq: 1,
        }
    }

    fn append_text(&mut self, text: &str, chunk_bytes: usize) -> Result<u64, String> {
        validate_stream_chunk_bytes(chunk_bytes)?;
        let mut appended = 0_u64;
        for chunk in transport_quic_stream::split_text_deltas(text, chunk_bytes) {
            self.transcript
                .append(self.next_seq, AGENT_TEXT_STREAM_RECORD_TEXT_DELTA, &chunk);
            self.next_seq += 1;
            appended += 1;
        }
        Ok(appended)
    }

    fn transcript_hash(&self) -> String {
        hex::encode(self.transcript.hash())
    }

    fn chunk_count(&self) -> u64 {
        self.transcript.chunk_count()
    }
}

fn validate_stream_chunk_bytes(chunk_bytes: usize) -> Result<(), String> {
    if chunk_bytes == 0 {
        return Err("agent text stream chunk size cannot be zero".to_owned());
    }
    if chunk_bytes > AGENT_TEXT_STREAM_MAX_PLAINTEXT_FRAME_LEN as usize {
        return Err(format!(
            "agent text stream chunk size exceeds app profile max: {chunk_bytes}"
        ));
    }
    Ok(())
}

async fn append_stream_compose_text(
    report: &mut DaemonOutgoingStreamReport,
    transcript: &mut LocalComposeTranscript,
    publisher: &mut Option<BrokerTextPublisher>,
    pending_live_text: &mut VecDeque<String>,
    live_error: &mut Option<String>,
    text: String,
    chunk_bytes: usize,
) -> Result<DaemonOutgoingStreamReport, String> {
    transcript.append_text(&text, chunk_bytes)?;
    report.text.push_str(&text);
    report.chunk_count = transcript.chunk_count();
    report.transcript_hash = Some(transcript.transcript_hash());

    if live_error.is_none() {
        if let Some(publisher) = publisher.as_mut() {
            if let Err(err) = publisher
                .append_text(&text, chunk_bytes, Duration::ZERO)
                .await
                .map_err(|err| err.to_string())
            {
                *live_error = Some(err);
            }
        } else {
            pending_live_text.push_back(text);
        }
    }
    if let Some(err) = live_error {
        report.error = Some(format!("live stream failed: {err}"));
    }

    Ok(report.clone())
}

async fn finish_stream_compose_report(
    report: &mut DaemonOutgoingStreamReport,
    transcript: &LocalComposeTranscript,
    publisher: &mut Option<BrokerTextPublisher>,
    pending_live_text: &mut VecDeque<String>,
    live_error: &mut Option<String>,
    chunk_bytes: usize,
) -> Result<DaemonOutgoingStreamReport, String> {
    if live_error.is_none()
        && let Some(publisher) = publisher.as_mut()
        && let Err(err) = flush_pending_live_text(publisher, pending_live_text, chunk_bytes).await
    {
        *live_error = Some(err);
    }

    if live_error.is_none()
        && let Some(publisher) = publisher.take()
        && let Err(err) = publisher.finish().await.map_err(|err| err.to_string())
    {
        *live_error = Some(err);
    }

    report.status = "finished".to_owned();
    report.transcript_hash = Some(transcript.transcript_hash());
    report.chunk_count = transcript.chunk_count();
    if let Some(err) = live_error {
        report.error = Some(format!("live stream failed: {err}"));
    }
    Ok(report.clone())
}

async fn flush_pending_live_text(
    publisher: &mut BrokerTextPublisher,
    pending_live_text: &mut VecDeque<String>,
    chunk_bytes: usize,
) -> Result<(), String> {
    while let Some(text) = pending_live_text.pop_front() {
        if let Err(err) = publisher
            .append_text(&text, chunk_bytes, Duration::ZERO)
            .await
            .map_err(|err| err.to_string())
        {
            pending_live_text.clear();
            return Err(err);
        }
    }
    Ok(())
}

fn short_id(value: &str) -> String {
    value.chars().take(12).collect()
}

fn stream_compose_key(account: Option<&str>, stream_id: &str) -> String {
    format!("{}:{stream_id}", account.unwrap_or(""))
}

#[derive(Clone, Debug)]
enum AppRuntimeRefresh {
    None,
    Reconcile,
    RestartSelected(Option<String>),
    CatchUpAll,
}

fn app_runtime_enabled(defaults: &DaemonDefaults) -> bool {
    defaults.relay.is_some()
}

async fn handle_app_runtime_account_setup_request(
    cli: &Cli,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    host: &mut AppRuntimeHost,
) -> Option<CliOutput> {
    let request = match app_runtime_account_setup_request(cli) {
        Ok(Some(request)) => request,
        Ok(None) => return None,
        Err(err) => return Some(crate::command_output_result(cli.json, Err(err))),
    };
    if !app_runtime_enabled(defaults) {
        return None;
    }
    reconcile_app_runtime(defaults, state.clone(), events, host).await;
    let Some(runtime) = &host.runtime else {
        return Some(crate::command_output_result(
            cli.json,
            Err(crate::DmError::MissingRelay),
        ));
    };
    let output = runtime
        .create_or_import_account(request)
        .await
        .map_err(crate::commands::account::map_setup_error)
        .and_then(crate::commands::account::setup_command_output);
    Some(crate::command_output_result(cli.json, output))
}

async fn handle_app_runtime_command_request(
    cli: &Cli,
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    host: &mut AppRuntimeHost,
) -> Option<CliOutput> {
    if !app_runtime_enabled(defaults) || !is_hosted_runtime_command(cli) {
        return None;
    }
    reconcile_app_runtime(defaults, state.clone(), events, host).await;
    let Some(runtime) = &host.runtime else {
        return Some(crate::command_output_result(
            cli.json,
            Err(crate::DmError::MissingRelay),
        ));
    };

    let secret_store = match crate::resolve_secret_store(defaults.secret_store) {
        Ok(secret_store) => secret_store,
        Err(err) => return Some(crate::command_output_result(cli.json, Err(err))),
    };
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home =
        match crate::open_account_home(&defaults.home, secret_store, &keychain_service) {
            Ok(account_home) => account_home,
            Err(err) => return Some(crate::command_output_result(cli.json, Err(err))),
        };
    let app = crate::app_for(
        defaults.home.clone(),
        defaults.relay.clone(),
        account_home.clone(),
    );

    let output = match cli.command.clone() {
        crate::Command::Group { command } => {
            crate::commands::group::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
            )
            .await
        }
        crate::Command::Groups { command } => {
            crate::commands::groups::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
            )
            .await
        }
        crate::Command::Message { command } | crate::Command::Messages { command } => {
            crate::commands::message::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
            )
            .await
        }
        crate::Command::Stream { command } => {
            crate::commands::stream::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
            )
            .await
        }
        crate::Command::Keys { command } => {
            crate::commands::key_package::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
            )
            .await
        }
        crate::Command::Follows { command } => {
            crate::commands::follows::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
                cli.relay.clone(),
            )
            .await
        }
        crate::Command::Profile { command } => {
            crate::commands::profile::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
                cli.relay.clone(),
            )
            .await
        }
        crate::Command::Relays { command } => {
            crate::commands::relays::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
                cli.relay.clone(),
            )
            .await
        }
        crate::Command::Media { command } => {
            crate::commands::media::with_runtime(
                &account_home,
                &app,
                runtime,
                command,
                cli.account.clone(),
            )
            .await
        }
        _ => return None,
    };
    Some(crate::command_output_result(cli.json, output))
}

fn is_hosted_runtime_command(cli: &Cli) -> bool {
    match &cli.command {
        crate::Command::Group { .. } | crate::Command::Groups { .. } => true,
        crate::Command::Message { command } | crate::Command::Messages { command } => {
            !matches!(command, crate::MessageCommand::Subscribe { .. })
        }
        crate::Command::Stream { command } => matches!(
            command,
            crate::StreamCommand::Start { .. }
                | crate::StreamCommand::Finish { .. }
                | crate::StreamCommand::Watch { .. }
                | crate::StreamCommand::Send {
                    start_event_id: Some(_),
                    ..
                }
        ),
        crate::Command::Keys { .. }
        | crate::Command::Follows { .. }
        | crate::Command::Profile { .. }
        | crate::Command::Relays { .. }
        | crate::Command::Media { .. } => true,
        _ => false,
    }
}

fn app_runtime_account_setup_request(
    cli: &Cli,
) -> Result<Option<marmot_app::AccountSetupRequest>, crate::DmError> {
    match &cli.command {
        crate::Command::CreateIdentity => {
            if cli.daemon_default_account_relays.is_empty() {
                return Err(crate::DmError::MissingRelay);
            }
            Ok(Some(marmot_app::AccountSetupRequest {
                identity: None,
                default_relays: crate::relay_endpoints(cli.daemon_default_account_relays.clone())?,
                bootstrap_relays: crate::relay_endpoints(cli.daemon_discovery_relays.clone())?,
                publish_missing_relay_lists: false,
                publish_initial_key_package: true,
            }))
        }
        crate::Command::Login {
            identity,
            nsec_stdin,
            ..
        } => {
            crate::validate_materialized_secret_identity("login", identity, *nsec_stdin)?;
            let Some(identity) = identity.clone() else {
                return Err(crate::DmError::MissingLoginIdentity);
            };
            if crate::is_nostr_secret(&identity) && cli.daemon_default_account_relays.is_empty() {
                return Err(crate::DmError::MissingRelay);
            }
            Ok(Some(marmot_app::AccountSetupRequest {
                identity: Some(identity),
                default_relays: crate::relay_endpoints(cli.daemon_default_account_relays.clone())?,
                bootstrap_relays: crate::relay_endpoints(cli.daemon_discovery_relays.clone())?,
                publish_missing_relay_lists: true,
                publish_initial_key_package: true,
            }))
        }
        crate::Command::Account {
            command:
                crate::AccountCommand::Create {
                    identity,
                    nsec_stdin,
                    default_relays,
                    bootstrap_relays,
                    publish_missing_relay_lists,
                },
        }
        | crate::Command::Accounts {
            command:
                crate::AccountCommand::Create {
                    identity,
                    nsec_stdin,
                    default_relays,
                    bootstrap_relays,
                    publish_missing_relay_lists,
                },
        } => {
            crate::validate_materialized_secret_identity("account create", identity, *nsec_stdin)?;
            Ok(Some(marmot_app::AccountSetupRequest {
                identity: identity.clone(),
                default_relays: crate::relay_endpoints(default_relays.clone())?,
                bootstrap_relays: crate::relay_endpoints(bootstrap_relays.clone())?,
                publish_missing_relay_lists: *publish_missing_relay_lists,
                publish_initial_key_package: false,
            }))
        }
        _ => Ok(None),
    }
}

fn app_runtime_refresh_after_execute(cli: &Cli) -> AppRuntimeRefresh {
    match &cli.command {
        crate::Command::CreateIdentity | crate::Command::Login { .. } => {
            AppRuntimeRefresh::Reconcile
        }
        crate::Command::Account {
            command: crate::AccountCommand::Create { .. },
        } => AppRuntimeRefresh::Reconcile,
        crate::Command::Group { .. } | crate::Command::Groups { .. } => {
            AppRuntimeRefresh::CatchUpAll
        }
        crate::Command::Message { .. }
        | crate::Command::Messages { .. }
        | crate::Command::Stream { .. } => AppRuntimeRefresh::CatchUpAll,
        crate::Command::Sync => AppRuntimeRefresh::RestartSelected(cli.account.clone()),
        _ => AppRuntimeRefresh::None,
    }
}

async fn refresh_app_runtime(
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    host: &mut AppRuntimeHost,
    refresh: AppRuntimeRefresh,
) {
    if !app_runtime_enabled(defaults) {
        return;
    }
    match refresh {
        AppRuntimeRefresh::None => {}
        AppRuntimeRefresh::Reconcile => {
            reconcile_app_runtime(defaults, state, events, host).await;
        }
        AppRuntimeRefresh::RestartSelected(selector) => {
            if host.runtime.is_none() {
                reconcile_app_runtime(defaults, state, events, host).await;
                return;
            }
            if let Some(account_id) = resolve_app_runtime_account_id(defaults, selector).await {
                if let Some(runtime) = &host.runtime
                    && let Err(err) = runtime.restart_account(&account_id).await
                {
                    record_runtime_activity_error(&state, err.to_string());
                }
            } else {
                reconcile_app_runtime(defaults, state, events, host).await;
            }
        }
        AppRuntimeRefresh::CatchUpAll => {
            reconcile_app_runtime(defaults, state.clone(), events, host).await;
            if let Some(runtime) = &host.runtime
                && let Err(err) = runtime.catch_up_accounts().await
            {
                record_runtime_activity_error(&state, err.to_string());
            }
        }
    }
}

async fn resolve_app_runtime_account_id(
    defaults: &DaemonDefaults,
    selector: Option<String>,
) -> Option<String> {
    let secret_store = crate::resolve_secret_store(defaults.secret_store).ok()?;
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home =
        crate::open_account_home(&defaults.home, secret_store, &keychain_service).ok()?;
    crate::resolve_account(&account_home, selector)
        .ok()
        .map(|account| account.account_id_hex)
}

async fn reconcile_app_runtime(
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    host: &mut AppRuntimeHost,
) {
    if !app_runtime_enabled(defaults) {
        return;
    }

    if host.runtime.is_none() {
        let runtime = match open_app_runtime(defaults) {
            Ok(runtime) => runtime,
            Err(err) => {
                record_runtime_activity_error(&state, err.to_string());
                return;
            }
        };
        let receiver = runtime.subscribe();
        if let Err(err) = runtime.start().await {
            record_runtime_activity_error(&state, err.to_string());
            return;
        }
        host.bridge = Some(spawn_app_runtime_bridge(
            defaults.clone(),
            state.clone(),
            events.clone(),
            host.stream_watch.clone(),
            runtime.clone(),
            runtime.shared_services().agent_streams(),
            receiver,
        ));
        host.runtime = Some(runtime);
        return;
    }

    if let Some(runtime) = &host.runtime {
        if let Err(err) = runtime.reconcile_accounts().await {
            record_runtime_activity_error(&state, err.to_string());
        }
        if host
            .bridge
            .as_ref()
            .is_none_or(|handle| handle.is_finished())
        {
            host.bridge = Some(spawn_app_runtime_bridge(
                defaults.clone(),
                state,
                events,
                host.stream_watch.clone(),
                runtime.clone(),
                runtime.shared_services().agent_streams(),
                runtime.subscribe(),
            ));
        }
    }
}

fn open_app_runtime(
    defaults: &DaemonDefaults,
) -> Result<marmot_app::MarmotAppRuntime, crate::DmError> {
    let secret_store = crate::resolve_secret_store(defaults.secret_store)?;
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home = crate::open_account_home(&defaults.home, secret_store, &keychain_service)?;
    let app = crate::app_for(defaults.home.clone(), defaults.relay.clone(), account_home);
    Ok(app.runtime())
}

fn spawn_app_runtime_bridge(
    defaults: DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    stream_workers: StreamWatchWorkers,
    runtime: marmot_app::MarmotAppRuntime,
    stream_manager: marmot_app::AgentStreamWatchManager,
    mut receiver: broadcast::Receiver<marmot_app::MarmotAppEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    handle_app_runtime_event(
                        &defaults,
                        state.clone(),
                        events.clone(),
                        stream_workers.clone(),
                        runtime.clone(),
                        stream_manager.clone(),
                        event,
                    )
                    .await;
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    record_runtime_activity_error(
                        &state,
                        format!("app runtime event stream lagged: {count} updates dropped"),
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}

async fn handle_app_runtime_event(
    defaults: &DaemonDefaults,
    state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    stream_workers: StreamWatchWorkers,
    runtime: marmot_app::MarmotAppRuntime,
    stream_manager: marmot_app::AgentStreamWatchManager,
    event: marmot_app::MarmotAppEvent,
) {
    let started_at = unix_now();
    match event {
        marmot_app::MarmotAppEvent::GroupJoined { group_id, .. } => {
            let summary = marmot_app::SyncSummary {
                joined_groups: vec![group_id],
                ..marmot_app::SyncSummary::default()
            };
            record_runtime_activity_report(
                &state,
                runtime_activity_report_from_summary(started_at, 1, &summary),
            );
        }
        marmot_app::MarmotAppEvent::GroupStateUpdated { .. } => {}
        marmot_app::MarmotAppEvent::MessageReceived(message) => {
            // Every delivered timeline message (including a kind-9 stream-final)
            // surfaces here; kind-1200 starts arrive as `AgentStreamStarted`.
            events.publish_message(message_stream_response(
                runtime_message_json(
                    &message.message,
                    &message.account_id_hex,
                    &message.account_label,
                ),
                "MessageReceived",
            ));
            let summary = marmot_app::SyncSummary {
                messages: vec![message.message],
                ..marmot_app::SyncSummary::default()
            };
            record_runtime_activity_report(
                &state,
                runtime_activity_report_from_summary(started_at, 1, &summary),
            );
        }
        marmot_app::MarmotAppEvent::AgentStreamStarted(message) => {
            events.publish_message(message_stream_response(
                runtime_message_json(
                    &message.message,
                    &message.account_id_hex,
                    &message.account_label,
                ),
                "AgentStreamStarted",
            ));
            let summary = marmot_app::SyncSummary {
                messages: vec![message.message],
                ..marmot_app::SyncSummary::default()
            };
            auto_watch_agent_stream_starts(
                defaults,
                &message.account_id_hex,
                &summary,
                stream_workers,
                runtime,
                stream_manager,
            )
            .await;
            record_runtime_activity_report(
                &state,
                runtime_activity_report_from_summary(started_at, 1, &summary),
            );
        }
        marmot_app::MarmotAppEvent::GroupEvent(group_event) => {
            let summary = marmot_app::SyncSummary {
                events: vec![group_event.event],
                ..marmot_app::SyncSummary::default()
            };
            record_runtime_activity_report(
                &state,
                runtime_activity_report_from_summary(started_at, 1, &summary),
            );
        }
        marmot_app::MarmotAppEvent::AccountError(error) => {
            record_runtime_activity_error(
                &state,
                format!(
                    "app runtime account {} failed: {}",
                    error.account_id_hex, error.message
                ),
            );
        }
    }
}

pub(super) fn runtime_message_json(
    message: &marmot_app::ReceivedMessage,
    account_id_hex: &str,
    account_label: &str,
) -> serde_json::Value {
    let now = unix_now();
    let is_own_sender = message.sender == account_id_hex || message.sender == account_label;
    let from_display_name = if is_own_sender {
        None
    } else {
        message.sender_display_name.clone()
    };
    let mut value = serde_json::json!({
        "account_id": account_id_hex,
        "message_id": message.message_id_hex,
        "direction": if is_own_sender { "sent" } else { "received" },
        "from": message.sender,
        "from_display_name": from_display_name,
        "group_id": hex::encode(message.group_id.as_slice()),
        "plaintext": message.plaintext,
        "kind": message.kind,
        "tags": message.tags,
        "recorded_at": now,
        "received_at": now,
    });
    if let Some(agent_text_stream) =
        crate::agent_text_stream_payload_value(message.kind, &message.tags, &message.plaintext)
    {
        value["agent_text_stream"] = agent_text_stream;
    }
    value
}

async fn auto_watch_agent_stream_starts(
    defaults: &DaemonDefaults,
    account_id: &str,
    summary: &marmot_app::SyncSummary,
    stream_workers: StreamWatchWorkers,
    runtime: marmot_app::MarmotAppRuntime,
    stream_manager: marmot_app::AgentStreamWatchManager,
) {
    let secret_store = match crate::resolve_secret_store(defaults.secret_store) {
        Ok(secret_store) => secret_store,
        Err(_) => return,
    };
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home =
        match crate::open_account_home(&defaults.home, secret_store, &keychain_service) {
            Ok(account_home) => account_home,
            Err(_) => return,
        };
    let app = crate::app_for(
        defaults.home.clone(),
        defaults.relay.clone(),
        account_home.clone(),
    );
    for message in &summary.messages {
        let Some(start) = marmot_app::StreamStartView::from_event(message.kind, &message.tags)
        else {
            continue;
        };
        if start.route != "quic" {
            continue;
        }
        let group_id = hex::encode(message.group_id.as_slice());
        let insecure_local = crate::first_quic_candidate_is_loopback(&start.quic_candidates);
        let stream_id = start.stream_id_hex;
        if stream_manager.watch_exists(Some(account_id), &group_id, Some(stream_id.as_str())) {
            continue;
        }

        let cli = Cli {
            home: Some(defaults.home.clone()),
            socket: None,
            relay: defaults.relay.clone(),
            daemon_discovery_relays: defaults.discovery_relays.clone(),
            daemon_default_account_relays: defaults.default_account_relays.clone(),
            secret_store: defaults.secret_store,
            keychain_service: defaults.keychain_service.clone(),
            account: Some(account_id.to_owned()),
            json: true,
            command: crate::Command::Stream {
                command: crate::StreamCommand::Watch {
                    group: group_id,
                    stream_id: Some(stream_id),
                    server_cert_der_hex: None,
                    insecure_local,
                    background: false,
                },
            },
        };
        if let Ok((report, handle)) = spawn_stream_watch(
            cli,
            account_home.clone(),
            app.clone(),
            runtime.clone(),
            stream_manager.clone(),
        ) {
            stream_workers.replace(report.watch_id, handle);
        }
    }
}

fn empty_runtime_activity_report(started_at: u64) -> DaemonRuntimeActivityReport {
    DaemonRuntimeActivityReport {
        started_at,
        finished_at: started_at,
        accounts: 0,
        events: 0,
        joined_groups: 0,
        messages: 0,
        directory_accounts: 0,
        directory_follows: 0,
        directory_profiles: 0,
        errors: Vec::new(),
    }
}

fn runtime_activity_report_from_summary(
    started_at: u64,
    accounts: usize,
    summary: &marmot_app::SyncSummary,
) -> DaemonRuntimeActivityReport {
    let mut report = empty_runtime_activity_report(started_at);
    report.finished_at = unix_now();
    report.accounts = accounts;
    report.events = summary.events.len();
    report.joined_groups = summary.joined_groups.len();
    report.messages = summary.messages.len();
    report
}

fn record_runtime_activity_error(state: &Arc<Mutex<DaemonState>>, error: String) {
    let started_at = unix_now();
    let mut report = empty_runtime_activity_report(started_at);
    report.finished_at = unix_now();
    report.errors.push(error);
    record_runtime_activity_report(state, report);
}

fn record_runtime_activity_report(
    state: &Arc<Mutex<DaemonState>>,
    report: DaemonRuntimeActivityReport,
) {
    if let Ok(mut state) = state.lock() {
        state.last_runtime_activity = Some(report);
    }
}

fn apply_defaults(cli: &mut Cli, defaults: &DaemonDefaults) {
    cli.home = Some(defaults.home.clone());
    cli.relay = defaults.relay.clone();
    cli.daemon_discovery_relays = defaults.discovery_relays.clone();
    cli.daemon_default_account_relays = defaults.default_account_relays.clone();
    apply_default_account_relays(cli, defaults);
    cli.secret_store = defaults.secret_store;
    cli.keychain_service = defaults.keychain_service.clone();
    cli.socket = None;
}

fn apply_default_account_relays(cli: &mut Cli, defaults: &DaemonDefaults) {
    let default_relays = defaults.default_account_relays.clone();
    let bootstrap_relays = if defaults.discovery_relays.is_empty() {
        default_relays.clone()
    } else {
        defaults.discovery_relays.clone()
    };
    match &mut cli.command {
        crate::Command::Account {
            command:
                crate::AccountCommand::Create {
                    default_relays: command_default_relays,
                    bootstrap_relays: command_bootstrap_relays,
                    ..
                },
        }
        | crate::Command::Accounts {
            command:
                crate::AccountCommand::Create {
                    default_relays: command_default_relays,
                    bootstrap_relays: command_bootstrap_relays,
                    ..
                },
        } => {
            if command_default_relays.is_empty() {
                *command_default_relays = default_relays;
            }
            if command_bootstrap_relays.is_empty() {
                *command_bootstrap_relays = bootstrap_relays;
            }
        }
        _ => {}
    }
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

fn daemon_output(json: bool, plain: &str, result: serde_json::Value, code: i32) -> CliOutput {
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
