//! Wire types shared between the `dmd` server and the `dm` daemon client.
//!
//! Newline-delimited JSON is the serialization framing. The server reads one
//! `DaemonRequest` per connection; for streaming subscriptions it emits zero
//! or more `DaemonStreamResponse` frames terminated by `stream_end: true`.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::Cli;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DaemonRuntimeActivityReport {
    pub started_at: u64,
    pub finished_at: u64,
    pub accounts: usize,
    pub events: usize,
    pub joined_groups: usize,
    pub messages: usize,
    pub directory_accounts: usize,
    pub directory_follows: usize,
    pub directory_profiles: usize,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub running: bool,
    pub socket: PathBuf,
    pub pid: Option<u32>,
    pub pid_file: Option<PathBuf>,
    pub stale_pid: Option<u32>,
    pub started_at: Option<u64>,
    pub home: Option<PathBuf>,
    pub log: Option<PathBuf>,
    pub last_runtime_activity: Option<DaemonRuntimeActivityReport>,
    pub relay_health: Option<marmot_app::RelayPlaneHealth>,
    pub stream_watches: Vec<DaemonStreamWatchReport>,
}

pub type DaemonStreamWatchReport = marmot_app::AgentStreamWatchReport;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonOutgoingStreamReport {
    pub account: Option<String>,
    pub group_id: String,
    pub stream_id: String,
    pub start_message_id: String,
    pub candidate: String,
    pub status: String,
    pub text: String,
    pub transcript_hash: Option<String>,
    pub chunk_count: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum DaemonRequest {
    Ping,
    Status,
    Shutdown,
    StreamWatch { cli: Box<Cli> },
    MessagesSubscribe { cli: Box<Cli> },
    ChatsSubscribe { cli: Box<Cli> },
    GroupStateSubscribe { cli: Box<Cli> },
    Execute { cli: Box<Cli> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonStreamResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<DaemonStreamError>,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub stream_end: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonStreamError {
    pub message: String,
}

impl DaemonStreamResponse {
    pub(crate) fn ok(result: serde_json::Value) -> Self {
        Self {
            result: Some(result),
            error: None,
            stream_end: false,
        }
    }

    pub(crate) fn err(message: impl Into<String>) -> Self {
        Self {
            result: None,
            error: Some(DaemonStreamError {
                message: message.into(),
            }),
            stream_end: false,
        }
    }
}
