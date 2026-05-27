//! Bridge between the daemon's connection dispatcher and the live `MarmotApp`
//! runtime: account-setup forwarding, command execution forwarding, event
//! consumption, the per-request runtime-refresh policy, and helpers that
//! inject daemon defaults into incoming `Cli` requests.

#![allow(dead_code, unused_imports)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use marmot_app::{AccountSetupRequest, MarmotApp, MarmotAppRuntime};
use serde_json::json;
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;

use super::state::{
    AppRuntimeHost, DaemonEventHub, DaemonState, DaemonWorkers, StreamWatchWorkers,
};
use super::subscriptions::{
    finish_stream_watch, message_stream_response, new_stream_watch_start, spawn_stream_watch,
};
use super::wire::{DaemonRuntimeActivityReport, DaemonStreamResponse};
use super::{DaemonDefaults, daemon_error, daemon_output, unix_now};
use crate::{Cli, CliOutput};

pub(super) enum AppRuntimeRefresh {
    None,
    Reconcile,
    RestartSelected(Option<String>),
    CatchUpAll,
}

pub(super) fn app_runtime_enabled(defaults: &DaemonDefaults) -> bool {
    defaults.relay.is_some()
}

pub(super) async fn handle_app_runtime_account_setup_request(
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

pub(super) async fn handle_app_runtime_command_request(
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

pub(super) fn is_hosted_runtime_command(cli: &Cli) -> bool {
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

pub(super) fn app_runtime_account_setup_request(
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

pub(super) fn app_runtime_refresh_after_execute(cli: &Cli) -> AppRuntimeRefresh {
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

pub(super) async fn refresh_app_runtime(
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

pub(super) async fn resolve_app_runtime_account_id(
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

pub(super) async fn reconcile_app_runtime(
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

pub(super) fn open_app_runtime(
    defaults: &DaemonDefaults,
) -> Result<marmot_app::MarmotAppRuntime, crate::DmError> {
    let secret_store = crate::resolve_secret_store(defaults.secret_store)?;
    let keychain_service = crate::resolve_keychain_service(defaults.keychain_service.clone());
    let account_home = crate::open_account_home(&defaults.home, secret_store, &keychain_service)?;
    let app = crate::app_for(defaults.home.clone(), defaults.relay.clone(), account_home);
    Ok(app.runtime())
}

pub(super) fn spawn_app_runtime_bridge(
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

pub(super) async fn handle_app_runtime_event(
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
        marmot_app::MarmotAppEvent::ProjectionUpdated(_) => {}
        marmot_app::MarmotAppEvent::MessageReceived(message) => {
            // Raw message updates keep kind-1200 starts separate as
            // `AgentStreamStarted`; materialized timeline subscriptions include
            // those starts as timeline rows.
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

pub(super) async fn auto_watch_agent_stream_starts(
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

pub(super) fn empty_runtime_activity_report(started_at: u64) -> DaemonRuntimeActivityReport {
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

pub(super) fn runtime_activity_report_from_summary(
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

pub(super) fn record_runtime_activity_error(state: &Arc<Mutex<DaemonState>>, error: String) {
    let started_at = unix_now();
    let mut report = empty_runtime_activity_report(started_at);
    report.finished_at = unix_now();
    report.errors.push(error);
    record_runtime_activity_report(state, report);
}

pub(super) fn record_runtime_activity_report(
    state: &Arc<Mutex<DaemonState>>,
    report: DaemonRuntimeActivityReport,
) {
    if let Ok(mut state) = state.lock() {
        state.last_runtime_activity = Some(report);
    }
}

pub(super) fn apply_defaults(cli: &mut Cli, defaults: &DaemonDefaults) {
    cli.home = Some(defaults.home.clone());
    cli.relay = defaults.relay.clone();
    cli.daemon_discovery_relays = defaults.discovery_relays.clone();
    cli.daemon_default_account_relays = defaults.default_account_relays.clone();
    apply_default_account_relays(cli, defaults);
    cli.secret_store = defaults.secret_store;
    cli.keychain_service = defaults.keychain_service.clone();
    cli.socket = None;
}

pub(super) fn apply_default_account_relays(cli: &mut Cli, defaults: &DaemonDefaults) {
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
