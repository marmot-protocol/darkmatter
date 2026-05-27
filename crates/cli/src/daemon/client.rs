//! Client side of the `dmd` socket protocol: the `dm` binary uses this to
//! forward commands and subscribe to streamed updates over the Unix socket.

use std::io::Write;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::wire::{DaemonRequest, DaemonStatus, DaemonStreamResponse};
use crate::{Cli, CliOutput};

#[derive(Debug, thiserror::Error)]
pub enum DaemonClientError {
    #[error("daemon not running at {socket}: {source}")]
    Connect {
        socket: PathBuf,
        source: std::io::Error,
    },
    #[error("daemon request failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("daemon protocol failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("daemon closed the connection without responding")]
    EmptyResponse,
}

#[derive(Clone, Debug)]
pub struct DaemonClient {
    socket: PathBuf,
}

impl DaemonClient {
    pub fn new(socket: impl AsRef<Path>) -> Self {
        Self {
            socket: socket.as_ref().to_path_buf(),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn status(&self) -> Result<DaemonStatus, DaemonClientError> {
        let output = send_request(&self.socket, &DaemonRequest::Status).await?;
        if output.code != 0 {
            return Err(DaemonClientError::EmptyResponse);
        }
        serde_json::from_str(output.stdout.trim()).map_err(DaemonClientError::Json)
    }

    pub async fn shutdown(&self) -> Result<CliOutput, DaemonClientError> {
        send_request(&self.socket, &DaemonRequest::Shutdown).await
    }

    pub(crate) async fn execute(&self, cli: Cli) -> Result<CliOutput, DaemonClientError> {
        send_request(&self.socket, &DaemonRequest::Execute { cli: Box::new(cli) }).await
    }

    pub(crate) async fn stream_watch(&self, cli: Cli) -> Result<CliOutput, DaemonClientError> {
        send_request(
            &self.socket,
            &DaemonRequest::StreamWatch { cli: Box::new(cli) },
        )
        .await
    }

    pub(crate) async fn messages_subscribe(
        &self,
        cli: Cli,
    ) -> Result<CliOutput, DaemonClientError> {
        let json = cli.json;
        stream_request(
            &self.socket,
            &DaemonRequest::MessagesSubscribe { cli: Box::new(cli) },
            json,
        )
        .await
    }

    pub(crate) async fn chats_subscribe(&self, cli: Cli) -> Result<CliOutput, DaemonClientError> {
        let json = cli.json;
        stream_request(
            &self.socket,
            &DaemonRequest::ChatsSubscribe { cli: Box::new(cli) },
            json,
        )
        .await
    }

    pub(crate) async fn group_state_subscribe(
        &self,
        cli: Cli,
    ) -> Result<CliOutput, DaemonClientError> {
        let json = cli.json;
        stream_request(
            &self.socket,
            &DaemonRequest::GroupStateSubscribe { cli: Box::new(cli) },
            json,
        )
        .await
    }
}

pub(crate) async fn send_execute(socket: &Path, cli: Cli) -> Result<CliOutput, DaemonClientError> {
    DaemonClient::new(socket).execute(cli).await
}

pub(crate) async fn send_stream_watch(
    socket: &Path,
    cli: Cli,
) -> Result<CliOutput, DaemonClientError> {
    DaemonClient::new(socket).stream_watch(cli).await
}

pub(crate) async fn send_messages_subscribe(
    socket: &Path,
    cli: Cli,
) -> Result<CliOutput, DaemonClientError> {
    DaemonClient::new(socket).messages_subscribe(cli).await
}

pub(crate) async fn send_chats_subscribe(
    socket: &Path,
    cli: Cli,
) -> Result<CliOutput, DaemonClientError> {
    DaemonClient::new(socket).chats_subscribe(cli).await
}

pub(crate) async fn send_group_state_subscribe(
    socket: &Path,
    cli: Cli,
) -> Result<CliOutput, DaemonClientError> {
    DaemonClient::new(socket).group_state_subscribe(cli).await
}

pub(super) async fn send_request(
    socket: &Path,
    request: &DaemonRequest,
) -> Result<CliOutput, DaemonClientError> {
    let mut stream =
        UnixStream::connect(socket)
            .await
            .map_err(|source| DaemonClientError::Connect {
                socket: socket.to_owned(),
                source,
            })?;
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    if response.is_empty() {
        return Err(DaemonClientError::EmptyResponse);
    }
    Ok(serde_json::from_slice(&response)?)
}

async fn stream_request(
    socket: &Path,
    request: &DaemonRequest,
    json_output: bool,
) -> Result<CliOutput, DaemonClientError> {
    let mut stream =
        UnixStream::connect(socket)
            .await
            .map_err(|source| DaemonClientError::Connect {
                socket: socket.to_owned(),
                source,
            })?;
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let mut had_error = false;
    loop {
        line.clear();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            break;
        }
        let response: DaemonStreamResponse = serde_json::from_str(line.trim_end())?;
        if response.stream_end {
            break;
        }
        if response.error.is_some() {
            had_error = true;
        }
        write_client_stream_response(json_output, &response)?;
    }

    Ok(CliOutput {
        code: if had_error { 1 } else { 0 },
        stdout: String::new(),
        stderr: String::new(),
    })
}

fn write_client_stream_response(
    json_output: bool,
    response: &DaemonStreamResponse,
) -> std::io::Result<()> {
    if json_output {
        let mut stdout = std::io::stdout().lock();
        serde_json::to_writer(&mut stdout, response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
        return Ok(());
    }

    if let Some(error) = &response.error {
        let mut stderr = std::io::stderr().lock();
        writeln!(stderr, "error: {}", error.message)?;
        stderr.flush()?;
        return Ok(());
    }

    if let Some(result) = &response.result {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{}", stream_result_plain(result))?;
        stdout.flush()?;
    }
    Ok(())
}

pub(super) fn stream_result_plain(result: &serde_json::Value) -> String {
    match result.get("type").and_then(serde_json::Value::as_str) {
        Some("message")
        | Some("reaction")
        | Some("message_delete")
        | Some("media")
        | Some("agent_stream_start")
        | Some("agent_stream_final") => {
            let message = result.get("message").unwrap_or(&serde_json::Value::Null);
            let label = match result.get("type").and_then(serde_json::Value::as_str) {
                Some("agent_stream_start") => "agent stream start",
                Some("agent_stream_final") => "agent stream final",
                Some("reaction") => "reaction",
                Some("message_delete") => "message delete",
                Some("media") => "media",
                _ => "message",
            };
            format!(
                "{label} group={} from={}: {}",
                message
                    .get("group_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unknown>"),
                message
                    .get("from")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unknown>"),
                message
                    .get("plaintext")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
            )
        }
        Some("stream_preview") => {
            let preview = result
                .get("stream_preview")
                .unwrap_or(&serde_json::Value::Null);
            format!(
                "stream preview {} [{}]: {}",
                preview
                    .get("stream_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<latest>"),
                preview
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown"),
                preview
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
            )
        }
        Some("agent_stream_delta") => {
            let delta = result
                .get("agent_stream_delta")
                .unwrap_or(&serde_json::Value::Null);
            format!(
                "agent stream delta {} #{}: {}",
                delta
                    .get("stream_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unknown>"),
                delta
                    .get("seq")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                delta
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
            )
        }
        _ => result.to_string(),
    }
}
