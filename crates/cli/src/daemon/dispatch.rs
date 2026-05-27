//! Daemon connection dispatcher: reads one `DaemonRequest` per Unix socket
//! connection and routes it to the right handler (subscriptions, stream
//! compose, runtime bridge), or returns a `daemon_forbidden` error for
//! requests we refuse to run over the socket.

#![allow(dead_code, unused_imports)]

use std::io::ErrorKind;
use std::sync::{Arc, Mutex};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use super::client::DaemonClientError;
use super::runtime_bridge::{
    app_runtime_refresh_after_execute, apply_defaults, handle_app_runtime_account_setup_request,
    handle_app_runtime_command_request, is_hosted_runtime_command, reconcile_app_runtime,
    refresh_app_runtime,
};
use super::state::{DaemonEventHub, DaemonState, DaemonWorkers};
use super::stream_compose::handle_stream_compose_request;
use super::subscriptions::{
    handle_chats_subscription, handle_group_state_subscription, handle_messages_subscription,
    spawn_stream_watch, start_stream_watch,
};
use super::wire::DaemonRequest;
use super::{DaemonDefaults, daemon_error, server_status};
use crate::{Cli, CliOutput};

pub(super) const MAX_DAEMON_REQUEST_BYTES: usize = 1024 * 1024;

pub(super) async fn handle_connection(
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

pub(super) fn blocked_daemon_execute_output(cli: &Cli) -> Option<CliOutput> {
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

pub(super) fn blocked_daemon_execute_command(
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

pub(super) async fn write_daemon_output(stream: &mut UnixStream, output: &CliOutput) {
    let Ok(mut response) = serde_json::to_vec(output) else {
        return;
    };
    response.push(b'\n');
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

pub(super) async fn read_daemon_request(
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
