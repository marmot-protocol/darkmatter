//! Daemon-side stream compose session: open, append, finish, cancel — plus
//! the `LocalComposeTranscript` helper and the brokered text-publisher
//! coordination that lets clients drive a streaming text payload through
//! the daemon's runtime.

#![allow(dead_code, unused_imports)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cgka_traits::GroupId;
use cgka_traits::agent_text_stream::{
    AGENT_TEXT_STREAM_MAX_PLAINTEXT_FRAME_LEN, AGENT_TEXT_STREAM_RECORD_TEXT_DELTA,
    AgentTextStreamKeyContextV1, AgentTextStreamTranscriptV1,
};
use marmot_app::{AgentTextStreamFinishRequest, MarmotApp};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use transport_quic_broker::{BrokerTextPublisher, OpenBrokerTextPublisher};

use super::state::{
    AppRuntimeHost, DaemonEventHub, DaemonState, DaemonWorkers, StreamComposeCommand,
    StreamComposeSession, StreamComposeWorkers,
};
use super::wire::{DaemonOutgoingStreamReport, DaemonStreamResponse};
use super::{
    DaemonDefaults, cli_output_result, daemon_error, daemon_output,
    handle_app_runtime_command_request, unix_now, unix_now_millis,
};
use crate::{Cli, CliOutput};

pub(super) async fn handle_stream_compose_request(
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
pub(super) async fn open_stream_compose(
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

pub(super) async fn append_stream_compose(
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

pub(super) async fn finish_stream_compose(
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

pub(super) fn cancel_stream_compose(
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

pub(super) async fn run_hosted_stream_marker_cli_json(
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

pub(super) async fn run_stream_compose_session(
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

pub(super) struct LocalComposeTranscript {
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

pub(super) fn validate_stream_chunk_bytes(chunk_bytes: usize) -> Result<(), String> {
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

pub(super) async fn append_stream_compose_text(
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

pub(super) async fn finish_stream_compose_report(
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

pub(super) async fn flush_pending_live_text(
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

pub(super) fn short_id(value: &str) -> String {
    value.chars().take(12).collect()
}

pub(super) fn stream_compose_key(account: Option<&str>, stream_id: &str) -> String {
    format!("{}:{stream_id}", account.unwrap_or(""))
}
