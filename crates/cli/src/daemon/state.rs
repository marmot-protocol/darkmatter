//! Shared mutable state used across the daemon's connection dispatcher,
//! subscription workers, and runtime bridge.
//!
//! Everything here is `pub(super)` — only `crate::daemon::*` callers should
//! touch this state. The wire-level types (`DaemonStreamResponse`,
//! `DaemonRuntimeActivityReport`, `DaemonOutgoingStreamReport`) come from
//! `super::wire`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::wire::{DaemonOutgoingStreamReport, DaemonRuntimeActivityReport, DaemonStreamResponse};

pub(super) const DAEMON_EVENT_REPLAY_LIMIT: usize = 256;

pub(super) struct DaemonState {
    pub(super) pid: u32,
    pub(super) started_at: u64,
    pub(super) last_runtime_activity: Option<DaemonRuntimeActivityReport>,
}

#[derive(Default)]
pub(super) struct AppRuntimeHost {
    pub(super) runtime: Option<marmot_app::MarmotAppRuntime>,
    pub(super) bridge: Option<JoinHandle<()>>,
    pub(super) stream_watch: StreamWatchWorkers,
}

impl AppRuntimeHost {
    pub(super) async fn abort_all(&mut self) {
        if let Some(runtime) = &self.runtime {
            runtime.shutdown().await;
        }
        if let Some(handle) = self.bridge.take() {
            handle.abort();
        }
        self.stream_watch.abort_all();
        self.runtime = None;
    }
}

#[derive(Clone, Default)]
pub(super) struct StreamWatchWorkers {
    pub(super) handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

impl StreamWatchWorkers {
    pub(super) fn replace(&self, watch_id: String, handle: JoinHandle<()>) {
        match self.handles.lock() {
            Ok(mut handles) => {
                Self::reap_finished_locked(&mut handles);
                if let Some(previous) = handles.insert(watch_id, handle) {
                    previous.abort();
                }
            }
            Err(_) => handle.abort(),
        }
    }

    pub(super) fn reap_finished(&self) {
        if let Ok(mut handles) = self.handles.lock() {
            Self::reap_finished_locked(&mut handles);
        }
    }

    fn reap_finished_locked(handles: &mut HashMap<String, JoinHandle<()>>) {
        handles.retain(|_, handle| !handle.is_finished());
    }

    pub(super) fn abort_all(&self) {
        if let Ok(mut handles) = self.handles.lock() {
            for (_, handle) in handles.drain() {
                handle.abort();
            }
        }
    }
}

#[derive(Default)]
pub(super) struct StreamComposeWorkers {
    pub(super) sessions: HashMap<String, StreamComposeSession>,
}

impl StreamComposeWorkers {
    pub(super) fn insert(&mut self, key: String, session: StreamComposeSession) {
        if let Some(previous) = self.sessions.insert(key, session) {
            let _ = previous.tx.try_send(StreamComposeCommand::Cancel);
            previous.handle.abort();
        }
    }

    pub(super) fn remove(&mut self, key: &str) -> Option<StreamComposeSession> {
        self.sessions.remove(key)
    }

    pub(super) fn get(&self, key: &str) -> Option<&StreamComposeSession> {
        self.sessions.get(key)
    }

    pub(super) fn abort_all(&mut self) {
        for (_, session) in self.sessions.drain() {
            let _ = session.tx.try_send(StreamComposeCommand::Cancel);
            session.handle.abort();
        }
    }
}

#[derive(Default)]
pub(super) struct DaemonWorkers {
    pub(super) runtime: AppRuntimeHost,
    pub(super) stream_compose: StreamComposeWorkers,
}

impl DaemonWorkers {
    pub(super) async fn abort_all(&mut self) {
        self.runtime.abort_all().await;
        self.stream_compose.abort_all();
    }
}

pub(super) struct StreamComposeSession {
    pub(super) tx: mpsc::Sender<StreamComposeCommand>,
    pub(super) handle: JoinHandle<()>,
}

pub(super) enum StreamComposeCommand {
    Append {
        text: String,
        respond: oneshot::Sender<Result<DaemonOutgoingStreamReport, String>>,
    },
    Finish {
        respond: oneshot::Sender<Result<DaemonOutgoingStreamReport, String>>,
    },
    Cancel,
}

#[derive(Clone)]
pub(super) struct DaemonEventHub {
    messages: broadcast::Sender<DaemonStreamResponse>,
    recent_messages: Arc<Mutex<VecDeque<DaemonStreamResponse>>>,
}

impl DaemonEventHub {
    pub(super) fn new() -> Self {
        let (messages, _) = broadcast::channel(1024);
        Self {
            messages,
            recent_messages: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub(super) fn subscribe_messages(&self) -> broadcast::Receiver<DaemonStreamResponse> {
        self.messages.subscribe()
    }

    pub(super) fn publish_message(&self, response: DaemonStreamResponse) {
        if let Ok(mut recent) = self.recent_messages.lock() {
            recent.push_back(response.clone());
            while recent.len() > DAEMON_EVENT_REPLAY_LIMIT {
                recent.pop_front();
            }
        }
        let _ = self.messages.send(response);
    }

    pub(super) fn recent_messages(&self) -> Vec<DaemonStreamResponse> {
        self.recent_messages
            .lock()
            .map(|recent| recent.iter().cloned().collect())
            .unwrap_or_default()
    }
}
