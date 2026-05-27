//! Daemon subscription handlers (messages / chats / group state) plus the
//! supporting helpers that frame stream responses, fingerprint previews,
//! filter events per subscription, and manage stream watches on behalf of
//! the dispatcher.

#![allow(dead_code)]

use cgka_traits::GroupId;
use cgka_traits::app_event::{
    MARMOT_APP_EVENT_KIND_CHAT, MARMOT_APP_EVENT_KIND_DELETE, MARMOT_APP_EVENT_KIND_REACTION,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use super::DaemonDefaults;
use super::state::{DaemonEventHub, DaemonState, StreamWatchWorkers};
use super::wire::{DaemonStreamResponse, DaemonStreamWatchReport};
use super::{daemon_error, runtime_message_json, unix_now, unix_now_millis};
use crate::{Cli, CliOutput};

pub(super) async fn handle_messages_subscription(
    stream: &mut UnixStream,
    defaults: &DaemonDefaults,
    _state: Arc<Mutex<DaemonState>>,
    events: DaemonEventHub,
    runtime: Option<marmot_app::MarmotAppRuntime>,
    cli: Cli,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (group_id, limit) = match messages_subscribe_args(&cli) {
        Ok(args) => args,
        Err(message) => {
            let _ = write_stream_response(stream, &DaemonStreamResponse::err(message)).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let account_ref = match daemon_account_ref(defaults, &cli) {
        Ok(account_ref) => account_ref,
        Err(message) => {
            let _ = write_stream_response(stream, &DaemonStreamResponse::err(message)).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let Some(runtime) = runtime else {
        let _ = write_stream_response(
            stream,
            &DaemonStreamResponse::err("app runtime is not running".to_owned()),
        )
        .await;
        let _ = write_stream_end(stream).await;
        return Ok(());
    };
    let stream_manager = runtime.shared_services().agent_streams();
    let mut runtime_subscription = match runtime.subscribe_messages(
        &account_ref,
        marmot_app::AppMessageQuery {
            group_id_hex: group_id.clone(),
            limit,
        },
    ) {
        Ok(subscription) => subscription,
        Err(err) => {
            let _ =
                write_stream_response(stream, &DaemonStreamResponse::err(err.to_string())).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let mut seen_messages = HashSet::new();
    let mut seen_stream_previews = HashSet::new();
    let mut event_rx = events.subscribe_messages();
    let mut stream_rx = stream_manager.subscribe();
    if !write_stream_response(
        stream,
        &DaemonStreamResponse::ok(serde_json::json!({
            "trigger": "SubscriptionReady",
            "type": "subscription_ready",
            "group_id": group_id.clone(),
        })),
    )
    .await
    {
        return Ok(());
    }

    let mut display_names_by_sender: HashMap<String, Option<String>> = HashMap::new();
    for message in runtime_subscription.snapshot.drain(..) {
        if !message.message_id_hex.is_empty() {
            seen_messages.insert(message.message_id_hex.clone());
        }
        let display_name = display_names_by_sender
            .entry(message.sender.clone())
            .or_insert_with(|| runtime.display_name_for_account_id(&message.sender))
            .clone();
        let response = message_stream_response(
            app_message_record_json(message, display_name),
            "InitialMessage",
        );
        if !write_stream_response(stream, &response).await {
            return Ok(());
        }
    }

    for response in events.recent_messages() {
        if !write_message_subscription_event(
            stream,
            response,
            group_id.as_deref(),
            &account_ref,
            &mut seen_messages,
            &mut seen_stream_previews,
        )
        .await
        {
            return Ok(());
        }
    }

    for update in stream_manager.recent_updates() {
        let response = agent_stream_update_response(update, false);
        if !write_message_subscription_event(
            stream,
            response,
            group_id.as_deref(),
            &account_ref,
            &mut seen_messages,
            &mut seen_stream_previews,
        )
        .await
        {
            return Ok(());
        }
    }

    if let Some(group_id) = group_id.as_deref() {
        for preview in stream_manager.previews_for_group(Some(&account_ref), group_id) {
            let preview =
                serde_json::to_value(preview).expect("stream preview serialization cannot fail");
            let fingerprint = stream_preview_fingerprint(&preview);
            if !seen_stream_previews.insert(fingerprint) {
                continue;
            }
            let response = stream_preview_response(preview, true);
            if !write_stream_response(stream, &response).await {
                return Ok(());
            }
        }
    }

    loop {
        tokio::select! {
            // Stream-start messages are published before their preview updates; keep that
            // ordering stable when both broadcast channels are ready in the same poll.
            biased;

            update = runtime_subscription.recv() => {
                let Some(update) = update else {
                    return Ok(());
                };
                let response = runtime_message_update_stream_response(update);
                if !write_message_subscription_event(
                    stream,
                    response,
                    group_id.as_deref(),
                    &account_ref,
                    &mut seen_messages,
                    &mut seen_stream_previews,
                )
                .await
                {
                    return Ok(());
                }
            }
            event = event_rx.recv() => {
                match event {
                    Ok(response) => {
                        if !write_message_subscription_event(
                            stream,
                            response,
                            group_id.as_deref(),
                            &account_ref,
                            &mut seen_messages,
                            &mut seen_stream_previews,
                        )
                        .await
                        {
                            return Ok(());
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        let response = DaemonStreamResponse::err(format!(
                            "message stream lagged: {count} updates dropped"
                        ));
                        if !write_stream_response(stream, &response).await {
                            return Ok(());
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            stream_update = stream_rx.recv() => {
                match stream_update {
                    Ok(update) => {
                        let response = agent_stream_update_response(update, false);
                        if !write_message_subscription_event(
                            stream,
                            response,
                            group_id.as_deref(),
                            &account_ref,
                            &mut seen_messages,
                            &mut seen_stream_previews,
                        )
                        .await
                        {
                            return Ok(());
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        let response = DaemonStreamResponse::err(format!(
                            "agent stream update stream lagged: {count} updates dropped"
                        ));
                        if !write_stream_response(stream, &response).await {
                            return Ok(());
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

pub(super) fn daemon_account_ref(defaults: &DaemonDefaults, cli: &Cli) -> Result<String, String> {
    let secret_store =
        crate::resolve_secret_store(defaults.secret_store).map_err(|err| err.to_string())?;
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home = crate::open_account_home(&defaults.home, secret_store, &keychain_service)
        .map_err(|err| err.to_string())?;
    let account = crate::resolve_account(&account_home, cli.account.clone())
        .map_err(|err| err.to_string())?;
    if !account.local_signing {
        return Err(format!(
            "account {} is not a local signing account",
            account.account_id_hex
        ));
    }
    Ok(account.account_id_hex)
}

pub(super) fn app_message_record_json(
    message: marmot_app::AppMessageRecord,
    from_display_name: Option<String>,
) -> serde_json::Value {
    crate::message_record_json(message, from_display_name)
}

pub(super) fn runtime_message_update_stream_response(
    update: marmot_app::RuntimeMessageUpdate,
) -> DaemonStreamResponse {
    match update {
        marmot_app::RuntimeMessageUpdate::Message(message) => message_stream_response(
            runtime_message_json(
                &message.message,
                &message.account_id_hex,
                &message.account_label,
            ),
            "MessageReceived",
        ),
        marmot_app::RuntimeMessageUpdate::AgentStreamStarted(message) => message_stream_response(
            runtime_message_json(
                &message.message,
                &message.account_id_hex,
                &message.account_label,
            ),
            "AgentStreamStarted",
        ),
    }
}

pub(super) fn chat_stream_response(
    group: marmot_app::AppGroupRecord,
    trigger: &str,
) -> DaemonStreamResponse {
    let group_id = group.group_id_hex.clone();
    DaemonStreamResponse::ok(serde_json::json!({
        "trigger": trigger,
        "type": "chat",
        "chat": crate::group_json(group),
        "group_id": group_id,
    }))
}

pub(super) fn group_state_stream_response(
    group: marmot_app::AppGroupRecord,
    trigger: &str,
    mls: Option<serde_json::Value>,
) -> DaemonStreamResponse {
    let group_id = group.group_id_hex.clone();
    let mut result = serde_json::json!({
        "trigger": trigger,
        "type": "group_state",
        "group": crate::group_json(group),
        "group_id": group_id,
    });
    if let Some(mls) = mls {
        result["mls"] = mls;
    }
    DaemonStreamResponse::ok(result)
}

pub(super) async fn write_message_subscription_event(
    stream: &mut UnixStream,
    response: DaemonStreamResponse,
    group_id: Option<&str>,
    account_id: &str,
    seen_messages: &mut HashSet<String>,
    seen_stream_previews: &mut HashSet<String>,
) -> bool {
    if !stream_response_matches_subscription(&response, group_id, account_id) {
        return true;
    }
    if mark_stream_response_seen(&response, seen_messages, seen_stream_previews) {
        write_stream_response(stream, &response).await
    } else {
        true
    }
}

pub(super) async fn handle_chats_subscription(
    stream: &mut UnixStream,
    defaults: &DaemonDefaults,
    runtime: Option<marmot_app::MarmotAppRuntime>,
    cli: Cli,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let include_archived = match chats_subscribe_args(&cli) {
        Ok(include_archived) => include_archived,
        Err(message) => {
            let _ = write_stream_response(stream, &DaemonStreamResponse::err(message)).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let account_ref = match daemon_account_ref(defaults, &cli) {
        Ok(account_ref) => account_ref,
        Err(message) => {
            let _ = write_stream_response(stream, &DaemonStreamResponse::err(message)).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let Some(runtime) = runtime else {
        let _ = write_stream_response(
            stream,
            &DaemonStreamResponse::err("app runtime is not running".to_owned()),
        )
        .await;
        let _ = write_stream_end(stream).await;
        return Ok(());
    };
    let mut subscription = match runtime.subscribe_chats(&account_ref, include_archived) {
        Ok(subscription) => subscription,
        Err(err) => {
            let _ =
                write_stream_response(stream, &DaemonStreamResponse::err(err.to_string())).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    for chat in subscription.snapshot.drain(..) {
        if !write_stream_response(stream, &chat_stream_response(chat, "InitialChat")).await {
            return Ok(());
        }
    }
    while let Some(chat) = subscription.recv().await {
        if !write_stream_response(stream, &chat_stream_response(chat, "ChatUpdated")).await {
            return Ok(());
        }
    }
    Ok(())
}

pub(super) async fn handle_group_state_subscription(
    stream: &mut UnixStream,
    defaults: &DaemonDefaults,
    runtime: Option<marmot_app::MarmotAppRuntime>,
    cli: Cli,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let group_id = match group_state_subscribe_args(&cli) {
        Ok(group_id) => group_id,
        Err(message) => {
            let _ = write_stream_response(stream, &DaemonStreamResponse::err(message)).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let account_ref = match daemon_account_ref(defaults, &cli) {
        Ok(account_ref) => account_ref,
        Err(message) => {
            let _ = write_stream_response(stream, &DaemonStreamResponse::err(message)).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let Some(runtime) = runtime else {
        let _ = write_stream_response(
            stream,
            &DaemonStreamResponse::err("app runtime is not running".to_owned()),
        )
        .await;
        let _ = write_stream_end(stream).await;
        return Ok(());
    };
    let mut subscription = match runtime.subscribe_group_state(&account_ref, &group_id) {
        Ok(subscription) => subscription,
        Err(err) => {
            let _ =
                write_stream_response(stream, &DaemonStreamResponse::err(err.to_string())).await;
            let _ = write_stream_end(stream).await;
            return Ok(());
        }
    };
    let group_id_value = GroupId::new(hex::decode(&group_id)?);
    let initial_mls = runtime
        .group_mls_state(&account_ref, &group_id_value)
        .await
        .ok()
        .map(crate::commands::groups::group_mls_state_json);
    if !write_stream_response(
        stream,
        &group_state_stream_response(
            subscription.snapshot.clone(),
            "InitialGroupState",
            initial_mls,
        ),
    )
    .await
    {
        return Ok(());
    }
    while let Some(group) = subscription.recv().await {
        let mls = runtime
            .group_mls_state(&account_ref, &group_id_value)
            .await
            .ok()
            .map(crate::commands::groups::group_mls_state_json);
        if !write_stream_response(
            stream,
            &group_state_stream_response(group, "GroupStateUpdated", mls),
        )
        .await
        {
            return Ok(());
        }
    }
    Ok(())
}

pub(super) fn group_state_subscribe_args(cli: &Cli) -> Result<String, String> {
    match &cli.command {
        crate::Command::Groups {
            command: crate::GroupsCommand::SubscribeState { group_id },
        } => crate::normalize_group_id_hex(group_id).map_err(|err| err.to_string()),
        _ => Err("groups subscribe-state requires dm groups subscribe-state".to_owned()),
    }
}

pub(super) fn chats_subscribe_args(cli: &Cli) -> Result<bool, String> {
    match &cli.command {
        crate::Command::Chats {
            command: crate::ChatsCommand::Subscribe,
        } => Ok(false),
        crate::Command::Chats {
            command: crate::ChatsCommand::SubscribeArchived,
        } => Ok(true),
        _ => Err("chats subscribe requires dm chats subscribe".to_owned()),
    }
}

pub(super) fn messages_subscribe_args(
    cli: &Cli,
) -> Result<(Option<String>, Option<usize>), String> {
    let (group, limit) = match &cli.command {
        crate::Command::Message {
            command: crate::MessageCommand::Subscribe { group, limit },
        }
        | crate::Command::Messages {
            command: crate::MessageCommand::Subscribe { group, limit },
        } => (group, *limit),
        _ => return Err("messages subscribe requires dm messages subscribe".to_owned()),
    };
    let group_id = group
        .as_deref()
        .map(crate::normalize_group_id_hex)
        .transpose()
        .map_err(|err| err.to_string())?;
    Ok((group_id, Some(limit.unwrap_or(50).min(200))))
}

pub(super) fn cli_output_result(output: CliOutput) -> Result<serde_json::Value, String> {
    let value = serde_json::from_str::<serde_json::Value>(output.stdout.trim())
        .map_err(|err| format!("daemon command returned invalid JSON: {err}"))?;
    if output.code != 0 || value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let message = value
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                if output.stderr.trim().is_empty() {
                    None
                } else {
                    Some(output.stderr.trim())
                }
            })
            .unwrap_or("daemon command failed");
        return Err(message.to_owned());
    }
    Ok(value
        .get("result")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

pub(super) fn stream_preview_fingerprint(preview: &serde_json::Value) -> String {
    let watch_id = preview
        .get("watch_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let status = preview
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let text = preview
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let transcript_hash = preview
        .get("transcript_hash")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let error = preview
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    format!("{watch_id}:{status}:{text}:{transcript_hash}:{error}")
}

pub(super) fn stream_preview_response(
    preview: serde_json::Value,
    initial: bool,
) -> DaemonStreamResponse {
    let trigger = if initial {
        "InitialStreamPreview"
    } else {
        match preview
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
        {
            "completed" => "StreamPreviewCompleted",
            "failed" => "StreamPreviewFailed",
            _ => "StreamPreviewUpdated",
        }
    };
    DaemonStreamResponse::ok(serde_json::json!({
        "trigger": trigger,
        "type": "stream_preview",
        "stream_preview": preview,
    }))
}

pub(super) fn agent_stream_delta_response(delta: crate::AgentStreamDelta) -> DaemonStreamResponse {
    DaemonStreamResponse::ok(serde_json::json!({
        "trigger": "AgentStreamDelta",
        "type": "agent_stream_delta",
        "agent_stream_delta": delta,
    }))
}

pub(super) fn agent_stream_update_response(
    update: marmot_app::AgentStreamUpdate,
    initial: bool,
) -> DaemonStreamResponse {
    match update {
        marmot_app::AgentStreamUpdate::WatchUpdated(report) => {
            let preview =
                serde_json::to_value(report).expect("stream preview serialization cannot fail");
            stream_preview_response(preview, initial)
        }
        marmot_app::AgentStreamUpdate::Delta(delta) => agent_stream_delta_response(delta),
    }
}

pub(super) fn message_stream_response(
    message: serde_json::Value,
    trigger: &str,
) -> DaemonStreamResponse {
    DaemonStreamResponse::ok(serde_json::json!({
        "trigger": trigger,
        "type": message_stream_type(&message),
        "message": message,
    }))
}

pub(super) fn message_stream_type(message: &serde_json::Value) -> &'static str {
    // Agent text stream classification is derived from the inner-event tags and
    // exposed under `agent_text_stream`; prefer it so stream-final chats surface
    // as `agent_stream_final` rather than a bare `message`.
    if let Some(stream_kind) = message
        .get("agent_text_stream")
        .and_then(|stream| stream.get("kind"))
        .and_then(serde_json::Value::as_str)
    {
        return match stream_kind {
            "start" => "agent_stream_start",
            "final" => "agent_stream_final",
            _ => "message",
        };
    }
    let kind = message.get("kind").and_then(serde_json::Value::as_u64);
    let has_imeta = message
        .get("tags")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|tags| {
            tags.iter().any(|tag| {
                tag.as_array()
                    .and_then(|values| values.first())
                    .and_then(serde_json::Value::as_str)
                    == Some("imeta")
            })
        });
    match kind {
        Some(MARMOT_APP_EVENT_KIND_REACTION) => "reaction",
        Some(MARMOT_APP_EVENT_KIND_DELETE) => "message_delete",
        Some(MARMOT_APP_EVENT_KIND_CHAT) if has_imeta => "media",
        _ => "message",
    }
}

pub(super) fn stream_response_matches_subscription(
    response: &DaemonStreamResponse,
    group_id: Option<&str>,
    account_id: &str,
) -> bool {
    let Some(result) = &response.result else {
        return true;
    };
    match result.get("type").and_then(serde_json::Value::as_str) {
        Some("message")
        | Some("reaction")
        | Some("message_delete")
        | Some("media")
        | Some("agent_stream_start")
        | Some("agent_stream_final") => {
            let Some(message) = result.get("message") else {
                return false;
            };
            value_matches_group_and_account(message, group_id, account_id)
        }
        Some("stream_preview") => {
            let Some(preview) = result.get("stream_preview") else {
                return false;
            };
            value_matches_group_and_account(preview, group_id, account_id)
        }
        Some("agent_stream_delta") => {
            let Some(delta) = result.get("agent_stream_delta") else {
                return false;
            };
            value_matches_group_and_account(delta, group_id, account_id)
        }
        _ => false,
    }
}

pub(super) fn value_matches_group_and_account(
    value: &serde_json::Value,
    group_id: Option<&str>,
    account_id: &str,
) -> bool {
    group_id.is_none_or(|group_id| {
        value.get("group_id").and_then(serde_json::Value::as_str) == Some(group_id)
    }) && value
        .get("account")
        .or_else(|| value.get("account_id"))
        .and_then(serde_json::Value::as_str)
        .is_none_or(|event_account| event_account == account_id)
}

pub(super) fn mark_stream_response_seen(
    response: &DaemonStreamResponse,
    seen_messages: &mut HashSet<String>,
    seen_stream_previews: &mut HashSet<String>,
) -> bool {
    let Some(result) = &response.result else {
        return true;
    };
    match result.get("type").and_then(serde_json::Value::as_str) {
        Some("message")
        | Some("reaction")
        | Some("message_delete")
        | Some("media")
        | Some("agent_stream_start")
        | Some("agent_stream_final") => result
            .get("message")
            .and_then(|message| message.get("message_id"))
            .and_then(serde_json::Value::as_str)
            .is_none_or(|message_id| seen_messages.insert(message_id.to_owned())),
        Some("stream_preview") => result
            .get("stream_preview")
            .map(stream_preview_fingerprint)
            .is_none_or(|fingerprint| seen_stream_previews.insert(fingerprint)),
        Some("agent_stream_delta") => true,
        _ => true,
    }
}

pub(super) async fn write_stream_response(
    stream: &mut UnixStream,
    response: &DaemonStreamResponse,
) -> bool {
    let Ok(mut bytes) = serde_json::to_vec(response) else {
        return false;
    };
    bytes.push(b'\n');
    stream.write_all(&bytes).await.is_ok()
}

pub(super) async fn write_stream_end(stream: &mut UnixStream) -> bool {
    write_stream_response(
        stream,
        &DaemonStreamResponse {
            result: None,
            error: None,
            stream_end: true,
        },
    )
    .await
}

pub(super) async fn start_stream_watch(
    cli: Cli,
    defaults: &DaemonDefaults,
    runtime: Option<&marmot_app::MarmotAppRuntime>,
    workers: &StreamWatchWorkers,
) -> CliOutput {
    let json = cli.json;
    let Some(runtime) = runtime else {
        return daemon_error(
            json,
            "stream_watch_failed",
            "app runtime is not running".to_owned(),
        );
    };
    let stream_manager = runtime.shared_services().agent_streams();
    let secret_store = match crate::resolve_secret_store(defaults.secret_store) {
        Ok(secret_store) => secret_store,
        Err(err) => return daemon_error(json, "stream_watch_failed", err.to_string()),
    };
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home =
        match crate::open_account_home(&defaults.home, secret_store, &keychain_service) {
            Ok(account_home) => account_home,
            Err(err) => return daemon_error(json, "stream_watch_failed", err.to_string()),
        };
    let app = crate::app_for(
        defaults.home.clone(),
        defaults.relay.clone(),
        account_home.clone(),
    );
    let (report, handle) =
        match spawn_stream_watch(cli, account_home, app, runtime.clone(), stream_manager) {
            Ok(spawned) => spawned,
            Err(message) => return daemon_error(json, "stream_watch_failed", message),
        };
    let watch_id = report.watch_id.clone();
    workers.replace(watch_id, handle);

    stream_watch_output(json, &report)
}

pub(super) fn spawn_stream_watch(
    mut cli: Cli,
    account_home: marmot_account::AccountHome,
    app: marmot_app::MarmotApp,
    runtime: marmot_app::MarmotAppRuntime,
    stream_manager: marmot_app::AgentStreamWatchManager,
) -> Result<(DaemonStreamWatchReport, JoinHandle<()>), String> {
    let report = stream_manager.start_watch(new_stream_watch_start(&cli)?);
    let watch_id = report.watch_id.clone();

    cli.json = true;
    if let crate::Command::Stream {
        command: crate::StreamCommand::Watch { background, .. },
    } = &mut cli.command
    {
        *background = false;
    }

    let worker_watch_id = watch_id;
    let worker_stream_manager = stream_manager.clone();
    let handle = tokio::spawn(async move {
        let json = cli.json;
        let account_flag = cli.account.clone();
        let command = match cli.command.clone() {
            crate::Command::Stream { command } => command,
            _ => return,
        };
        let output = crate::command_output_result(
            json,
            crate::commands::stream::watch_with_runtime(
                &account_home,
                &app,
                &runtime,
                command,
                account_flag,
                move |delta| {
                    worker_stream_manager.record_delta(delta.clone());
                },
            )
            .await,
        );
        finish_stream_watch(stream_manager, worker_watch_id, output);
    });

    Ok((report, handle))
}

pub(super) fn new_stream_watch_start(
    cli: &Cli,
) -> Result<marmot_app::AgentStreamWatchStart, String> {
    let crate::Command::Stream {
        command: crate::StreamCommand::Watch {
            group, stream_id, ..
        },
    } = &cli.command
    else {
        return Err("background stream watch requires dm stream watch".to_owned());
    };
    let group_id = crate::normalize_group_id_hex(group).map_err(|err| err.to_string())?;
    let stream_id = stream_id
        .as_deref()
        .map(crate::normalize_hex)
        .transpose()
        .map_err(|err| err.to_string())?;
    let started_at = unix_now();
    Ok(marmot_app::AgentStreamWatchStart {
        account: cli.account.clone(),
        group_id,
        stream_id,
        started_at,
        started_at_millis: unix_now_millis(),
    })
}

pub(super) fn finish_stream_watch(
    stream_manager: marmot_app::AgentStreamWatchManager,
    watch_id: String,
    output: CliOutput,
) {
    let mut status = "failed".to_owned();
    let mut text = None;
    let mut transcript_hash = None;
    let mut chunk_count = None;
    let mut error = None;
    let mut result = None;
    let mut stream_id = None;

    if output.code == 0 {
        match serde_json::from_str::<serde_json::Value>(output.stdout.trim()) {
            Ok(value) if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) => {
                let body = value
                    .get("result")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                status = "completed".to_owned();
                text = body
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                transcript_hash = body
                    .get("transcript_hash")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                chunk_count = body.get("chunk_count").and_then(serde_json::Value::as_u64);
                stream_id = body
                    .get("stream_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                result = Some(body);
            }
            Ok(value) => {
                error = Some(
                    value
                        .get("error")
                        .and_then(|error| error.get("message"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("stream watch failed")
                        .to_owned(),
                );
            }
            Err(err) => {
                error = Some(format!("stream watch returned invalid JSON: {err}"));
            }
        }
    } else if !output.stderr.trim().is_empty() {
        error = Some(output.stderr.trim().to_owned());
    } else if !output.stdout.trim().is_empty() {
        error = Some(output.stdout.trim().to_owned());
    } else {
        error = Some("stream watch failed".to_owned());
    }

    let _ = stream_manager.finish_watch(
        &watch_id,
        marmot_app::AgentStreamWatchCompletion {
            finished_at: unix_now(),
            status,
            stream_id,
            text,
            transcript_hash,
            chunk_count,
            error,
            result,
        },
    );
}

pub(super) fn stream_watch_output(json: bool, report: &DaemonStreamWatchReport) -> CliOutput {
    if json {
        return CliOutput {
            code: 0,
            stdout: format!(
                "{}\n",
                serde_json::to_string(&serde_json::json!({
                    "ok": true,
                    "result": report,
                }))
                .expect("JSON response serialization cannot fail")
            ),
            stderr: String::new(),
        };
    }
    CliOutput {
        code: 0,
        stdout: format!("watching stream {}\n", report.watch_id),
        stderr: String::new(),
    }
}
