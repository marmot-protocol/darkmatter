//! Local Marmot agent connector daemon.

mod allowlist;
mod bootstrap;
mod error;
mod event_projection;
mod quic;
mod socket;
mod stream_session;
mod validation;

#[cfg(test)]
mod tests;

pub use bootstrap::{
    BootstrapError, BootstrapOptions, BootstrapResult, DEFAULT_BOOTSTRAP_LABEL,
    DEFAULT_QUIC_CANDIDATE, DEFAULT_RELAYS, default_bootstrap_home, read_bootstrap_auth_token,
    resolve_bootstrap_home, resolve_bootstrap_quic_candidates, resolve_bootstrap_relays,
    resolve_bootstrap_socket, run_bootstrap,
};
pub use error::ConnectorError;
pub use socket::{bind_connector_socket, bind_connector_socket_with_mode, default_socket_path};

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use agent_control::{
    AgentControlAccount, AgentControlDebugFinalSend, AgentControlEnvelope, AgentControlError,
    AgentControlEvent, AgentControlRequest, AgentControlResponse, read_envelope, write_frame,
};
use agent_stream_compose::{StreamComposeCommand, StreamComposeReport, run_stream_compose_session};
use cgka_traits::{GroupId, MemberId, MessageId, engine::GroupEvent};
use marmot_account::{AccountHome, AccountHomeError, AccountSummary};
use marmot_app::{
    AccountRelayListBootstrap, AgentOperationEventRequest, AgentTextStreamFinishRequest,
    AppMessageQuery, MarmotApp, MarmotAppEvent, MarmotAppRuntime, UserProfileMetadata,
};
use tokio::io::{AsyncBufRead, AsyncWrite, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use transport_quic_broker::OpenBrokerTextPublisher;

use crate::allowlist::AllowlistStore;
use crate::event_projection::{
    DeliveredInboundCursor, InboundCatchUpDriver, InboundCatchUpEvent,
    control_event_from_debug_event, control_event_from_runtime_event,
    inbound_message_event_from_record, resync_required_event,
};
use crate::quic::{
    broker_trust_for_addr, first_quic_candidate, parse_quic_candidate, resolve_quic_candidate_addr,
};
use crate::socket::current_effective_uid;
use crate::stream_session::{ActiveStreamSession, DebugFinalSendStore, StreamSessionStore};
use crate::validation::{
    InvitePolicyKey, InvitePolicyRetryState, PendingInvitePolicyCandidate,
    agent_control_request_type, auth_token_matches, endpoint, normalize_hex,
    transcript_hash_from_hex, unix_now_seconds, unsupported_request_message,
    validate_control_plane_config, validate_profile_name,
};

pub(crate) const AGENT_SOCKET_DIR_MODE: u32 = 0o700;
pub(crate) const AGENT_SOCKET_MODE: u32 = 0o600;
pub(crate) const ALLOWLIST_DIR: &str = "agent-allowlist";
const STREAM_COMPOSE_CHANNEL_DEPTH: usize = 32;
const STREAM_COMPOSE_CHUNK_BYTES: usize = 1024;
pub(crate) const INBOUND_CATCH_UP_INTERVAL: Duration = Duration::from_secs(5);
const INVITE_POLICY_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const INVITE_POLICY_RETRY_BASE: Duration = Duration::from_secs(5);
pub(crate) const INVITE_POLICY_RETRY_MAX: Duration = Duration::from_secs(300);
/// Maximum time a stream compose session may sit without an append/status/progress
/// command before the sweeper aborts it. Bounds the lifetime of sessions abandoned by
/// a crashed or restarted gateway (fresh ids on restart leave no cleanup path).
const STREAM_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// How often the background sweeper scans for idle stream compose sessions.
const STREAM_SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// Capacity of the per-subscription delivered-inbound-id cursor used to dedup storage-backed
/// replay after broadcast lag. Comfortably larger than the runtime broadcast channel depth
/// (1024) so every message that could be re-queried after a single overflow is still tracked.
pub(crate) const DELIVERED_INBOUND_CURSOR_CAPACITY: usize = 4096;
pub(crate) const MAX_PROFILE_NAME_CHARS: usize = 80;

#[derive(Clone, Debug)]
pub struct AgentConnectorConfig {
    pub home: PathBuf,
    pub socket: PathBuf,
    pub socket_dir_mode: u32,
    pub socket_mode: u32,
    pub relays: Vec<String>,
    pub allow_any: bool,
    pub debug_controls: bool,
    pub auth_token: Option<String>,
}

impl AgentConnectorConfig {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        let socket = default_socket_path(&home);
        Self {
            home,
            socket,
            socket_dir_mode: AGENT_SOCKET_DIR_MODE,
            socket_mode: AGENT_SOCKET_MODE,
            relays: Vec::new(),
            allow_any: false,
            debug_controls: false,
            auth_token: None,
        }
    }
}

#[derive(Clone)]
pub struct AgentConnector {
    account_home: AccountHome,
    allowlists: AllowlistStore,
    allow_any: bool,
    debug_controls: bool,
    auth_token: Option<String>,
    debug_events: broadcast::Sender<AgentControlEvent>,
    debug_final_sends: DebugFinalSendStore,
    streams: StreamSessionStore,
    app: MarmotApp,
    runtime: MarmotAppRuntime,
    inbound_catch_up: InboundCatchUpDriver,
    relays: Vec<String>,
    connection_errors: Arc<AtomicU64>,
}

impl AgentConnector {
    pub fn open(config: AgentConnectorConfig) -> Result<Self, ConnectorError> {
        let account_home = AccountHome::open(&config.home);
        let relays = config.relays;
        let app = MarmotApp::with_relays_and_account_home(
            &config.home,
            relays.clone(),
            account_home.clone(),
        );
        let runtime = MarmotAppRuntime::new(app.clone());
        let inbound_catch_up = InboundCatchUpDriver::new(runtime.clone());
        let allowlists = AllowlistStore::new(&config.home);
        let (debug_events, _) = broadcast::channel(1024);
        Ok(Self {
            account_home,
            allowlists,
            allow_any: config.allow_any,
            debug_controls: config.debug_controls,
            auth_token: config.auth_token,
            debug_events,
            debug_final_sends: DebugFinalSendStore::default(),
            streams: StreamSessionStore::default(),
            app,
            runtime,
            inbound_catch_up,
            relays,
            connection_errors: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn serve_once(&self, listener: &UnixListener) -> Result<(), ConnectorError> {
        let (stream, _peer_addr) = listener.accept().await?;
        self.handle_connection(stream).await
    }

    pub async fn start(&self) -> Result<(), ConnectorError> {
        self.runtime.start().await?;
        self.spawn_invite_policy_worker();
        self.spawn_stream_session_sweeper();
        self.ensure_agent_accounts_ready().await?;
        Ok(())
    }

    async fn ensure_agent_accounts_ready(&self) -> Result<(), ConnectorError> {
        let accounts = self.account_home.accounts()?;
        for account in accounts.into_iter().filter(|account| account.local_signing) {
            self.ensure_agent_account_relay_lists(&account.label)
                .await?;
            let has_key_package = !self
                .runtime
                .account_key_packages(&account.label, Vec::new())
                .await?
                .is_empty();
            if !has_key_package {
                self.runtime.publish_key_package(&account.label).await?;
            }
        }
        Ok(())
    }

    async fn ensure_agent_account_relay_lists(
        &self,
        account_ref: &str,
    ) -> Result<(), ConnectorError> {
        let missing_nip65 = self.runtime.account_nip65_relays(account_ref)?.is_empty();
        let missing_inbox = self.runtime.account_inbox_relays(account_ref)?.is_empty();
        if self.relays.is_empty() || (!missing_nip65 && !missing_inbox) {
            return Ok(());
        }

        let relays = self.configured_relay_endpoints();
        if missing_nip65 {
            self.runtime
                .set_account_nip65_relays(account_ref, relays.clone(), relays.clone())
                .await?;
        }
        if missing_inbox {
            self.runtime
                .set_account_inbox_relays(account_ref, relays.clone(), relays)
                .await?;
        }
        Ok(())
    }

    fn configured_relay_endpoints(&self) -> Vec<cgka_traits::TransportEndpoint> {
        self.relays.iter().map(|relay| endpoint(relay)).collect()
    }

    pub async fn handle_connection(&self, stream: UnixStream) -> Result<(), ConnectorError> {
        let peer_uid = stream.peer_cred()?.uid();
        let peer_authorized_by_uid = peer_uid == current_effective_uid();
        let (read_half, mut write_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(read_half);
        let Some(request): Option<AgentControlEnvelope<AgentControlRequest>> =
            read_envelope(&mut reader).await?
        else {
            return Ok(());
        };
        if let Err(err) =
            self.authorize_control_request(peer_authorized_by_uid, request.auth_token.as_deref())
        {
            let response = AgentControlEnvelope::new(
                request.id,
                self.error_response("authorize_control_request", &err),
            );
            write_frame(&mut write_half, &response).await?;
            return Ok(());
        }
        if let AgentControlRequest::SubscribeInbound {
            account_id_hex,
            group_id_hex,
        } = request.payload
        {
            return self
                .stream_inbound_events(
                    request.id,
                    account_id_hex,
                    group_id_hex,
                    &mut reader,
                    &mut write_half,
                )
                .await;
        }
        let response = match self.handle_request(request.payload).await {
            Ok(response) => response,
            Err(err) => self.error_response("handle_connection", &err),
        };
        let response = AgentControlEnvelope::new(request.id, response);
        write_frame(&mut write_half, &response).await?;
        Ok(())
    }

    fn error_response(&self, method: &'static str, err: &ConnectorError) -> AgentControlResponse {
        tracing::warn!(
            target: "agent_connector",
            method = method,
            error_code = err.privacy_safe_code(),
            "control request failed"
        );
        AgentControlResponse::Error {
            code: err.code().to_owned(),
            message: err.client_message().to_owned(),
        }
    }

    fn authorize_control_request(
        &self,
        peer_authorized_by_uid: bool,
        auth_token: Option<&str>,
    ) -> Result<(), ConnectorError> {
        if let Some(expected) = self.auth_token.as_deref() {
            if auth_token_matches(expected, auth_token) {
                return Ok(());
            }
            return Err(ConnectorError::Unauthorized);
        }

        if peer_authorized_by_uid {
            Ok(())
        } else {
            Err(ConnectorError::Unauthorized)
        }
    }

    async fn handle_request(
        &self,
        request: AgentControlRequest,
    ) -> Result<AgentControlResponse, ConnectorError> {
        match request {
            AgentControlRequest::AccountList => self.account_list_response(),
            AgentControlRequest::AllowlistList { account_id_hex } => {
                self.allowlist_response(&account_id_hex)
            }
            AgentControlRequest::AllowlistAdd {
                account_id_hex,
                welcomer_account_id_hex,
            } => self.allowlist_add_response(&account_id_hex, &welcomer_account_id_hex),
            AgentControlRequest::AllowlistRemove {
                account_id_hex,
                welcomer_account_id_hex,
            } => self.allowlist_remove_response(&account_id_hex, &welcomer_account_id_hex),
            AgentControlRequest::DebugInjectInbound {
                account_id_hex,
                group_id_hex,
                message_id_hex,
                sender_account_id_hex,
                text,
            } => self.debug_inject_inbound_response(
                &account_id_hex,
                &group_id_hex,
                &message_id_hex,
                &sender_account_id_hex,
                text,
            ),
            AgentControlRequest::DebugRecordedFinals => self.debug_recorded_finals_response(),
            AgentControlRequest::SendFinal {
                account_id_hex,
                group_id_hex,
                text,
                reply_to_message_id_hex,
            } => {
                self.send_final_response(
                    &account_id_hex,
                    &group_id_hex,
                    text,
                    reply_to_message_id_hex,
                )
                .await
            }
            AgentControlRequest::StreamBegin {
                account_id_hex,
                group_id_hex,
                stream_id_hex,
                quic_candidates,
            } => {
                self.stream_begin_response(
                    &account_id_hex,
                    &group_id_hex,
                    stream_id_hex,
                    quic_candidates,
                )
                .await
            }
            AgentControlRequest::StreamAppend {
                stream_id_hex,
                append_text,
            } => {
                self.stream_append_response(&stream_id_hex, append_text)
                    .await
            }
            AgentControlRequest::StreamStatus {
                stream_id_hex,
                status,
            } => self.stream_status_response(&stream_id_hex, status).await,
            AgentControlRequest::StreamProgress {
                stream_id_hex,
                text,
            } => self.stream_progress_response(&stream_id_hex, text).await,
            AgentControlRequest::StreamFinalize {
                stream_id_hex,
                final_text,
                transcript_hash_hex,
                chunk_count,
            } => {
                self.stream_finalize_response(
                    &stream_id_hex,
                    final_text,
                    &transcript_hash_hex,
                    chunk_count,
                )
                .await
            }
            AgentControlRequest::StreamCancel { stream_id_hex, .. } => {
                self.stream_cancel_response(&stream_id_hex)
            }
            AgentControlRequest::AccountCreate {
                label,
                publish_key_package,
            } => {
                self.create_account_response(label, publish_key_package)
                    .await
            }
            AgentControlRequest::AccountPublishKeyPackage { account_id_hex } => {
                let account = self.local_account_for_account_id(&account_id_hex)?;
                let key_package_bytes = self.runtime.publish_key_package(&account.label).await?;
                Ok(AgentControlResponse::KeyPackagePublished {
                    account_id_hex,
                    key_package_bytes,
                })
            }
            AgentControlRequest::AccountPublishProfile {
                account_id_hex,
                name,
                display_name,
            } => {
                self.publish_profile_response(&account_id_hex, name, display_name)
                    .await
            }
            AgentControlRequest::SendAgentActivity {
                account_id_hex,
                group_id_hex,
                status,
                text,
                reply_to_message_id_hex,
                extra,
            } => {
                self.send_agent_activity_response(
                    &account_id_hex,
                    &group_id_hex,
                    status,
                    text,
                    reply_to_message_id_hex,
                    extra,
                )
                .await
            }
            AgentControlRequest::SendAgentOperationEvent {
                account_id_hex,
                group_id_hex,
                event_type,
                status,
                operation_id,
                run_id,
                turn_id,
                name,
                text,
                preview,
                details,
                sequence,
                ok,
                duration_ms,
                reply_to_message_id_hex,
            } => {
                self.send_agent_operation_event_response(
                    &account_id_hex,
                    &group_id_hex,
                    event_type,
                    status,
                    operation_id,
                    run_id,
                    turn_id,
                    name,
                    text,
                    preview,
                    details,
                    sequence,
                    ok,
                    duration_ms,
                    reply_to_message_id_hex,
                )
                .await
            }
            AgentControlRequest::SendGroupSystemEvent {
                account_id_hex,
                group_id_hex,
                system_type,
                text,
                data,
            } => {
                self.send_group_system_event_response(
                    &account_id_hex,
                    &group_id_hex,
                    system_type,
                    text,
                    data,
                )
                .await
            }
            other => Ok(AgentControlResponse::Error {
                code: "unsupported_request".to_owned(),
                message: unsupported_request_message(&other).to_owned(),
            }),
        }
    }

    fn account_list_response(&self) -> Result<AgentControlResponse, ConnectorError> {
        let accounts = self
            .account_home
            .accounts()?
            .into_iter()
            .map(|account| AgentControlAccount {
                account_id_hex: account.account_id_hex,
                label: account.label,
                local_signing: account.local_signing,
            })
            .collect();
        Ok(AgentControlResponse::AccountList { accounts })
    }

    async fn create_account_response(
        &self,
        label: Option<String>,
        publish_key_package: bool,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = match label {
            Some(label) => self.account_home.create_account(&label)?,
            None => self.account_home.create_nostr_account()?,
        };
        if publish_key_package {
            self.runtime.publish_key_package(&account.label).await?;
        }
        Ok(AgentControlResponse::AccountCreated {
            account: AgentControlAccount {
                account_id_hex: account.account_id_hex,
                label: account.label,
                local_signing: account.local_signing,
            },
        })
    }

    async fn publish_profile_response(
        &self,
        account_id_hex: &str,
        name: String,
        display_name: Option<String>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let name = validate_profile_name(name)?;
        let display_name = display_name
            .map(validate_profile_name)
            .transpose()?
            .unwrap_or_else(|| name.clone());
        let bootstrap_relays = self.configured_relay_endpoints();
        let profile = UserProfileMetadata {
            name: Some(name.clone()),
            display_name: Some(display_name.clone()),
            created_at: unix_now_seconds(),
            ..UserProfileMetadata::default()
        };
        self.runtime
            .publish_user_profile(
                &account.label,
                profile,
                AccountRelayListBootstrap::new(bootstrap_relays.clone(), bootstrap_relays),
            )
            .await?;
        Ok(AgentControlResponse::ProfilePublished {
            account_id_hex: account.account_id_hex,
            name,
            display_name: Some(display_name),
        })
    }

    fn local_account_for_account_id(
        &self,
        account_id_hex: &str,
    ) -> Result<AccountSummary, ConnectorError> {
        self.account_home
            .accounts()?
            .into_iter()
            .find(|account| account.account_id_hex == account_id_hex)
            .ok_or_else(|| AccountHomeError::UnknownAccount(account_id_hex.to_owned()).into())
    }

    fn allowlist_response(
        &self,
        account_id_hex: &str,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        Ok(AgentControlResponse::Allowlist {
            account_id_hex: account.account_id_hex.clone(),
            welcomer_account_ids_hex: self.allowlists.list(&account.account_id_hex)?,
        })
    }

    fn allowlist_add_response(
        &self,
        account_id_hex: &str,
        welcomer_account_id_hex: &str,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let welcomer_account_id_hex =
            AccountHome::account_id_for_public_key(welcomer_account_id_hex)?;
        Ok(AgentControlResponse::Allowlist {
            account_id_hex: account.account_id_hex.clone(),
            welcomer_account_ids_hex: self
                .allowlists
                .add(&account.account_id_hex, &welcomer_account_id_hex)?,
        })
    }

    fn allowlist_remove_response(
        &self,
        account_id_hex: &str,
        welcomer_account_id_hex: &str,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let welcomer_account_id_hex =
            AccountHome::account_id_for_public_key(welcomer_account_id_hex)?;
        Ok(AgentControlResponse::Allowlist {
            account_id_hex: account.account_id_hex.clone(),
            welcomer_account_ids_hex: self
                .allowlists
                .remove(&account.account_id_hex, &welcomer_account_id_hex)?,
        })
    }

    async fn send_final_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        text: String,
        reply_to_message_id_hex: Option<String>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        if self.debug_controls {
            return self.debug_record_final_send_response(
                account_id_hex,
                group_id_hex,
                text,
                reply_to_message_id_hex,
            );
        }

        let account = self.local_account_for_account_id(account_id_hex)?;
        let group_id = GroupId::new(hex::decode(group_id_hex)?);
        let summary = if let Some(target_message_id) = reply_to_message_id_hex {
            self.runtime
                .reply_to_message(&account.label, &group_id, &target_message_id, &text)
                .await?
        } else {
            self.runtime
                .send_message(&account.label, &group_id, text.into_bytes())
                .await?
        };
        Ok(AgentControlResponse::FinalSent {
            message_ids_hex: summary.message_ids,
        })
    }

    fn debug_inject_inbound_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        message_id_hex: &str,
        sender_account_id_hex: &str,
        text: String,
    ) -> Result<AgentControlResponse, ConnectorError> {
        self.ensure_debug_controls()?;
        let event = AgentControlEvent::InboundMessage {
            account_id_hex: normalize_hex(account_id_hex)?,
            group_id_hex: normalize_hex(group_id_hex)?,
            message_id_hex: normalize_hex(message_id_hex)?,
            sender_account_id_hex: normalize_hex(sender_account_id_hex)?,
            text,
        };
        let _ = self.debug_events.send(event);
        Ok(AgentControlResponse::Ack)
    }

    fn debug_recorded_finals_response(&self) -> Result<AgentControlResponse, ConnectorError> {
        self.ensure_debug_controls()?;
        Ok(AgentControlResponse::DebugRecordedFinals {
            sends: self.debug_final_sends.list(),
        })
    }

    fn debug_record_final_send_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        text: String,
        reply_to_message_id_hex: Option<String>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        self.ensure_debug_controls()?;
        let record = self.debug_final_sends.record(AgentControlDebugFinalSend {
            account_id_hex: normalize_hex(account_id_hex)?,
            group_id_hex: normalize_hex(group_id_hex)?,
            text,
            reply_to_message_id_hex: reply_to_message_id_hex
                .map(|value| normalize_hex(&value))
                .transpose()?,
            message_ids_hex: Vec::new(),
        });
        Ok(AgentControlResponse::FinalSent {
            message_ids_hex: record.message_ids_hex,
        })
    }

    fn ensure_debug_controls(&self) -> Result<(), ConnectorError> {
        if self.debug_controls {
            Ok(())
        } else {
            Err(ConnectorError::DebugControlsDisabled)
        }
    }

    async fn stream_begin_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        stream_id_hex: Option<String>,
        quic_candidates: Vec<String>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let group_id_hex = normalize_hex(group_id_hex)?;
        let group_id = GroupId::new(hex::decode(&group_id_hex)?);
        let stream_id = stream_id_hex
            .map(|stream_id_hex| -> Result<Vec<u8>, ConnectorError> {
                Ok(hex::decode(normalize_hex(&stream_id_hex)?)?)
            })
            .transpose()?
            .unwrap_or_else(transport_quic_stream::random_stream_id);
        let stream_id_hex = hex::encode(&stream_id);
        let candidate = first_quic_candidate(&quic_candidates)?;
        let parsed_candidate = parse_quic_candidate(&candidate)?;
        let broker_addr = resolve_quic_candidate_addr(&parsed_candidate).await?;
        let trust = broker_trust_for_addr(broker_addr);
        let (_payload, summary) = self
            .runtime
            .start_agent_text_stream(
                &account.label,
                &group_id,
                &stream_id,
                unix_now_seconds(),
                quic_candidates.clone(),
            )
            .await?;
        let start_message_id_hex =
            summary.message_ids.first().cloned().ok_or_else(|| {
                ConnectorError::Stream("stream start returned no message id".into())
            })?;
        let crypto = self
            .runtime
            .agent_text_stream_crypto_for_start_event(
                Some(&account.label),
                Some(&group_id_hex),
                Some(&stream_id_hex),
                &start_message_id_hex,
            )
            .await?;

        let policy_max_plaintext_frame_len = crypto.policy_max_plaintext_frame_len;

        let (tx, rx) = mpsc::channel(STREAM_COMPOSE_CHANNEL_DEPTH);
        // Dedicated cancel signal: a separate, bounded channel that cannot be
        // starved behind queued append/status/progress commands, so an explicit
        // cancel always reaches the session and a live `Abort` is emitted.
        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let report = StreamComposeReport {
            account: Some(account.account_id_hex.clone()),
            group_id: group_id_hex.clone(),
            stream_id: stream_id_hex.clone(),
            start_message_id: start_message_id_hex.clone(),
            candidate: candidate.clone(),
            status: "streaming".to_owned(),
            text: String::new(),
            transcript_hash: None,
            chunk_count: 0,
            error: None,
        };
        let handle = tokio::spawn(run_stream_compose_session(
            OpenBrokerTextPublisher {
                broker_addr,
                server_name: parsed_candidate.server_name,
                trust,
                stream_id: stream_id.clone(),
                start_event_id: MessageId::new(hex::decode(&start_message_id_hex)?),
                crypto: Some(crypto.crypto),
                max_plaintext_frame_len: policy_max_plaintext_frame_len,
            },
            STREAM_COMPOSE_CHUNK_BYTES,
            rx,
            cancel_rx,
            report,
        ));
        self.streams.insert(
            stream_id_hex.clone(),
            ActiveStreamSession {
                account_label: account.label,
                group_id,
                stream_id,
                start_message_id_hex: start_message_id_hex.clone(),
                tx,
                cancel_tx,
                abort: handle.abort_handle(),
                last_activity: Instant::now(),
            },
        );
        Ok(AgentControlResponse::StreamBegun {
            stream_id_hex,
            start_message_id_hex,
            quic_candidates,
        })
    }

    async fn stream_append_response(
        &self,
        stream_id_hex: &str,
        append_text: String,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let session = self.streams.get(stream_id_hex)?;
        let (respond, response) = oneshot::channel();
        session
            .tx
            .send(StreamComposeCommand::Append {
                text: append_text,
                respond,
            })
            .await
            .map_err(|_| ConnectorError::Stream("stream compose session is closed".into()))?;
        response
            .await
            .map_err(|err| ConnectorError::Stream(err.to_string()))?
            .map_err(ConnectorError::Stream)?;
        Ok(AgentControlResponse::Ack)
    }

    async fn stream_status_response(
        &self,
        stream_id_hex: &str,
        status: String,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let session = self.streams.get(stream_id_hex)?;
        let (respond, response) = oneshot::channel();
        session
            .tx
            .send(StreamComposeCommand::Status { status, respond })
            .await
            .map_err(|_| ConnectorError::Stream("stream compose session is closed".into()))?;
        response
            .await
            .map_err(|err| ConnectorError::Stream(err.to_string()))?
            .map_err(ConnectorError::Stream)?;
        Ok(AgentControlResponse::Ack)
    }

    async fn stream_progress_response(
        &self,
        stream_id_hex: &str,
        text: String,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let session = self.streams.get(stream_id_hex)?;
        let (respond, response) = oneshot::channel();
        session
            .tx
            .send(StreamComposeCommand::Progress { text, respond })
            .await
            .map_err(|_| ConnectorError::Stream("stream compose session is closed".into()))?;
        response
            .await
            .map_err(|err| ConnectorError::Stream(err.to_string()))?
            .map_err(ConnectorError::Stream)?;
        Ok(AgentControlResponse::Ack)
    }

    async fn send_agent_activity_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        status: String,
        text: String,
        reply_to_message_id_hex: Option<String>,
        extra: Option<serde_json::Value>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let group_id_hex = normalize_hex(group_id_hex)?;
        let group_id = GroupId::new(hex::decode(&group_id_hex)?);
        let reply_to_message_id_hex = reply_to_message_id_hex
            .map(|value| normalize_hex(&value))
            .transpose()?;
        let summary = self
            .runtime
            .send_agent_activity(
                &account.label,
                &group_id,
                status,
                text,
                reply_to_message_id_hex,
                extra,
            )
            .await?;
        Ok(AgentControlResponse::AppEventSent {
            message_ids_hex: summary.message_ids,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_agent_operation_event_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        event_type: String,
        status: String,
        operation_id: Option<String>,
        run_id: Option<String>,
        turn_id: Option<String>,
        name: Option<String>,
        text: String,
        preview: Option<String>,
        details: Option<serde_json::Value>,
        sequence: Option<u64>,
        ok: Option<bool>,
        duration_ms: Option<u64>,
        reply_to_message_id_hex: Option<String>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let group_id_hex = normalize_hex(group_id_hex)?;
        let group_id = GroupId::new(hex::decode(&group_id_hex)?);
        let reply_to_message_id_hex = reply_to_message_id_hex
            .map(|value| normalize_hex(&value))
            .transpose()?;
        let summary = self
            .runtime
            .send_agent_operation_event(
                &account.label,
                &group_id,
                AgentOperationEventRequest {
                    event_type,
                    status,
                    operation_id,
                    run_id,
                    turn_id,
                    name,
                    text,
                    preview,
                    details,
                    sequence,
                    ok,
                    duration_ms,
                    reply_to_message_id: reply_to_message_id_hex,
                },
            )
            .await?;
        Ok(AgentControlResponse::AppEventSent {
            message_ids_hex: summary.message_ids,
        })
    }

    async fn send_group_system_event_response(
        &self,
        account_id_hex: &str,
        group_id_hex: &str,
        system_type: String,
        text: String,
        data: Option<serde_json::Value>,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let group_id_hex = normalize_hex(group_id_hex)?;
        let group_id = GroupId::new(hex::decode(&group_id_hex)?);
        let summary = self
            .runtime
            .send_group_system_event(&account.label, &group_id, system_type, text, data)
            .await?;
        Ok(AgentControlResponse::AppEventSent {
            message_ids_hex: summary.message_ids,
        })
    }

    async fn stream_finalize_response(
        &self,
        stream_id_hex: &str,
        final_text: String,
        transcript_hash_hex: &str,
        chunk_count: u64,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let stream_id_hex = normalize_hex(stream_id_hex)?;
        let session = self.streams.remove(&stream_id_hex)?;
        let (respond, response) = oneshot::channel();
        if session
            .tx
            .send(StreamComposeCommand::Finish { respond })
            .await
            .is_err()
        {
            session.abort.abort();
            return Err(ConnectorError::Stream(
                "stream compose session is closed".into(),
            ));
        }
        let report = response
            .await
            .map_err(|err| ConnectorError::Stream(err.to_string()))?
            .map_err(ConnectorError::Stream)?;
        if report.text != final_text {
            return Err(ConnectorError::Stream(
                "stream final text does not match appended transcript".into(),
            ));
        }
        let transcript_hash = transcript_hash_from_hex(transcript_hash_hex)?;
        let expected_transcript_hash_hex = hex::encode(transcript_hash);
        let actual_transcript_hash_hex = report
            .transcript_hash
            .as_deref()
            .map(normalize_hex)
            .transpose()?;
        if actual_transcript_hash_hex.as_deref() != Some(expected_transcript_hash_hex.as_str()) {
            return Err(ConnectorError::Stream(
                "stream final transcript hash does not match appended transcript".into(),
            ));
        }
        if report.chunk_count != chunk_count {
            return Err(ConnectorError::Stream(
                "stream final chunk count does not match appended transcript".into(),
            ));
        }
        let (_payload, summary) = self
            .runtime
            .finish_agent_text_stream(
                &session.account_label,
                &session.group_id,
                AgentTextStreamFinishRequest {
                    stream_id: session.stream_id,
                    start_event_id: session.start_message_id_hex,
                    final_text_or_reference: final_text,
                    transcript_hash,
                    chunk_count,
                    finished_at: unix_now_seconds(),
                },
            )
            .await?;
        Ok(AgentControlResponse::StreamFinalized {
            stream_id_hex,
            message_ids_hex: summary.message_ids,
        })
    }

    fn stream_cancel_response(
        &self,
        stream_id_hex: &str,
    ) -> Result<AgentControlResponse, ConnectorError> {
        let session = self.streams.remove(stream_id_hex)?;
        // Send a graceful cancel over the dedicated cancel signal and let the
        // compose session drain it: the session emits a live `Abort` record (so
        // online subscribers observe the cancellation) and shuts itself down.
        // The cancel signal is its own bounded channel that cannot be starved by
        // queued append/status/progress commands, so it always lands. Do NOT
        // abort the task on a full *command* queue — only fall back to a forced
        // abort if the dedicated cancel channel itself is gone (session not
        // running), which is the only case where the session can no longer
        // publish an Abort.
        if session.cancel_tx.try_send(()).is_err() {
            session.abort.abort();
        }
        Ok(AgentControlResponse::Ack)
    }

    async fn stream_inbound_events<R, W>(
        &self,
        request_id: Option<String>,
        account_id_hex: Option<String>,
        group_id_hex: Option<String>,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<(), ConnectorError>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut runtime_events = self.runtime.subscribe();
        let mut debug_events = self.debug_events.subscribe();
        let (mut catch_up_events, _catch_up_subscription) = self.inbound_catch_up.subscribe();

        let response = AgentControlEnvelope::new(request_id.clone(), AgentControlResponse::Ack);
        write_frame(writer, &response).await?;

        // Request the initial catch-up without blocking this drain loop. catch_up_accounts() can
        // block for up to APP_RUNTIME_ACCOUNT_READY_WAIT per account and can itself emit many
        // runtime events; keeping this loop live prevents the bounded broadcast channel from
        // dropping inbound user messages while catch-up is in flight.
        let catch_up_driver = self.inbound_catch_up.clone();
        let mut initial_catch_up = Box::pin(async move {
            catch_up_driver
                .request()
                .await
                .map_err(ConnectorError::from)
        });
        let mut initial_catch_up_pending = true;

        // Tracks the inbound message ids already delivered on this subscription so a
        // storage-backed replay after broadcast lag re-delivers only genuinely-missed
        // messages (never re-flooding the agent with messages it already handled, and never
        // double-delivering one that raced live delivery). Bounded so a long-lived
        // subscription cannot grow this set without limit; the queue evicts the oldest ids.
        let mut delivered = DeliveredInboundCursor::new(DELIVERED_INBOUND_CURSOR_CAPACITY);

        loop {
            let event = tokio::select! {
                catch_up_result = &mut initial_catch_up, if initial_catch_up_pending => {
                    initial_catch_up_pending = false;
                    if let Err(err) = catch_up_result {
                        let response = AgentControlEnvelope::new(
                            request_id.clone(),
                            self.error_response("stream_inbound_events", &err),
                        );
                        write_frame(writer, &response).await?;
                        return Ok(());
                    }
                    continue;
                }
                // SubscribeInbound is read-only after the initial request: clients are only
                // expected to close the stream. read_envelope() uses read_until(), which is
                // not cancellation-safe for partial frames inside select!; switch this to a
                // cancellation-safe framed read before adding subscriber-side messages here.
                read = read_envelope(reader) => {
                    let read: Result<Option<AgentControlEnvelope<AgentControlRequest>>, AgentControlError> = read;
                    match read {
                        Ok(None) => return Ok(()),
                        Ok(Some(envelope)) => {
                            let request_type = agent_control_request_type(&envelope.payload);
                            tracing::warn!(
                                target: "agent_connector",
                                method = "stream_inbound_events",
                                request_type,
                                "additional request received after SubscribeInbound"
                            );
                            continue;
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
                catch_up = catch_up_events.recv() => {
                    match catch_up {
                        Ok(InboundCatchUpEvent::Completed)
                        | Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                    continue;
                }
                event = runtime_events.recv() => {
                    match event {
                        Ok(event) => control_event_from_runtime_event(
                            event,
                            account_id_hex.as_deref(),
                            group_id_hex.as_deref(),
                        ),
                        Err(broadcast::error::RecvError::Lagged(dropped)) => {
                            // The broadcast channel overflowed: `dropped` events were evicted
                            // before we could deliver them and are gone from the channel for
                            // good (catch-up never re-emits already-broadcast messages). Recover
                            // the missed inbound messages from durable storage and re-deliver
                            // them on the existing InboundMessage path the consumer already
                            // handles, so a reconnect backlog never silently loses user
                            // messages. The delivered-id cursor guarantees we re-deliver only
                            // messages this subscription has not already emitted.
                            match self.replay_missed_inbound(
                                account_id_hex.as_deref(),
                                group_id_hex.as_deref(),
                                &mut delivered,
                            ) {
                                Ok(missed) => {
                                    tracing::warn!(
                                        target: "agent_connector",
                                        method = "stream_inbound_events",
                                        dropped_events = dropped,
                                        replayed_messages = missed.len(),
                                        "inbound broadcast lagged; replayed missed messages from storage"
                                    );
                                    for replayed in missed {
                                        let envelope = AgentControlEnvelope::new(
                                            request_id.clone(),
                                            replayed,
                                        );
                                        write_frame(writer, &envelope).await?;
                                    }
                                    continue;
                                }
                                Err(err) => {
                                    // Storage replay failed: fall back to the resync_required
                                    // signal so the agent still learns it must re-sync state
                                    // rather than silently losing the dropped messages.
                                    tracing::warn!(
                                        target: "agent_connector",
                                        method = "stream_inbound_events",
                                        dropped_events = dropped,
                                        error_code = err.privacy_safe_code(),
                                        "inbound broadcast lagged; storage replay failed, emitting resync_required"
                                    );
                                    Some(resync_required_event(
                                        account_id_hex.as_deref(),
                                        group_id_hex.as_deref(),
                                        dropped,
                                    ))
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                event = debug_events.recv() => {
                    match event {
                        Ok(event) => control_event_from_debug_event(
                            event,
                            account_id_hex.as_deref(),
                            group_id_hex.as_deref(),
                        ),
                        // Debug channel lag is not user-message loss; skip without resync.
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => continue,
                    }
                }
            };
            let Some(event) = event else {
                continue;
            };
            // Track delivered inbound message ids so a later storage replay re-delivers only
            // genuinely-missed messages. Skip a live message the cursor already saw (e.g. one
            // that was just recovered by a replay) so it is never delivered twice.
            if let AgentControlEvent::InboundMessage { message_id_hex, .. } = &event {
                if delivered.contains(message_id_hex) {
                    continue;
                }
                delivered.record(message_id_hex.clone());
            }
            let envelope = AgentControlEnvelope::new(request_id.clone(), event);
            write_frame(writer, &envelope).await?;
        }
    }

    /// Recover inbound messages that were dropped from the broadcast channel by re-reading them
    /// from durable per-account storage. Returns the missed messages as `InboundMessage` events
    /// (the same shape the live path emits) scoped to this subscription's filters, skipping any
    /// id already recorded in `delivered` and recording the ones it returns. This is how a lagged
    /// subscription recovers user messages instead of dropping them: the connector re-queries its
    /// own state, exactly the resync the agent could not perform on its own.
    fn replay_missed_inbound(
        &self,
        account_filter: Option<&str>,
        group_filter: Option<&str>,
        delivered: &mut DeliveredInboundCursor,
    ) -> Result<Vec<AgentControlEvent>, ConnectorError> {
        // Replay for the filtered account, or for every local account on an unscoped
        // subscription (mirroring the live path, which emits for all local accounts).
        let accounts = match account_filter {
            Some(account_id_hex) => vec![self.local_account_for_account_id(account_id_hex)?],
            None => self.account_home.accounts()?,
        };
        let mut events = Vec::new();
        for account in accounts {
            let query = AppMessageQuery {
                group_id_hex: group_filter.map(str::to_owned),
                limit: None,
            };
            let records = self.runtime.messages_with_query(&account.label, query)?;
            for record in records {
                let Some(event) = inbound_message_event_from_record(
                    &account.account_id_hex,
                    record,
                    account_filter,
                    group_filter,
                ) else {
                    continue;
                };
                if let AgentControlEvent::InboundMessage { message_id_hex, .. } = &event {
                    if delivered.contains(message_id_hex) {
                        continue;
                    }
                    delivered.record(message_id_hex.clone());
                }
                events.push(event);
            }
        }
        Ok(events)
    }

    fn spawn_invite_policy_worker(&self) {
        let connector = self.clone();
        tokio::spawn(async move {
            connector.run_invite_policy_worker().await;
        });
    }

    fn spawn_stream_session_sweeper(&self) {
        let streams = self.streams.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(STREAM_SESSION_SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let swept = streams.sweep_idle(STREAM_SESSION_IDLE_TIMEOUT);
                if swept > 0 {
                    tracing::warn!(
                        target: "agent_connector",
                        method = "spawn_stream_session_sweeper",
                        swept,
                        "aborted idle stream compose sessions"
                    );
                }
            }
        });
    }

    async fn run_invite_policy_worker(self) {
        let mut events = self.runtime.subscribe();
        let mut retry_state = InvitePolicyRetryState::default();
        let mut reconcile_interval = tokio::time::interval_at(
            tokio::time::Instant::now(),
            INVITE_POLICY_RECONCILE_INTERVAL,
        );
        reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = reconcile_interval.tick() => {
                    self.reconcile_pending_invite_policies(&mut retry_state).await;
                }
                event = events.recv() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                            tracing::warn!(
                                target: "agent_connector",
                                method = "run_invite_policy_worker",
                                lagged,
                                "invite policy event stream lagged; reconciling pending invites"
                            );
                            self.reconcile_pending_invite_policies(&mut retry_state).await;
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    };
                    let MarmotAppEvent::GroupEvent(group_event) = event else {
                        continue;
                    };
                    let GroupEvent::GroupJoined {
                        group_id, welcomer, ..
                    } = group_event.event
                    else {
                        continue;
                    };
                    let group_id_hex = hex::encode(group_id.as_slice());
                    let candidate = PendingInvitePolicyCandidate {
                        key: InvitePolicyKey::new(&group_event.account_id_hex, &group_id_hex),
                        group_id,
                        welcomer,
                    };
                    let now = tokio::time::Instant::now();
                    if retry_state.is_due(&candidate.key, now) {
                        self.apply_invite_policy_candidate(candidate, &mut retry_state, now)
                            .await;
                    }
                }
            }
        }
    }

    async fn reconcile_pending_invite_policies(&self, retry_state: &mut InvitePolicyRetryState) {
        let candidates = match self.pending_invite_policy_candidates() {
            Ok(candidates) => candidates,
            Err(err) => {
                tracing::warn!(
                    target: "agent_connector",
                    method = "reconcile_pending_invite_policies",
                    error_code = err.privacy_safe_code(),
                    "pending invite policy reconciliation failed"
                );
                return;
            }
        };
        let pending = candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect::<HashSet<_>>();
        retry_state.retain_pending(&pending);
        let now = tokio::time::Instant::now();
        for candidate in candidates {
            if retry_state.is_due(&candidate.key, now) {
                self.apply_invite_policy_candidate(candidate, retry_state, now)
                    .await;
            }
        }
    }

    fn pending_invite_policy_candidates(
        &self,
    ) -> Result<Vec<PendingInvitePolicyCandidate>, ConnectorError> {
        let mut candidates = Vec::new();
        for account in self
            .account_home
            .accounts()?
            .into_iter()
            .filter(|account| account.local_signing)
        {
            for group in self.app.groups(&account.label)? {
                if !group.pending_confirmation || group.archived {
                    continue;
                }
                let group_id_hex = normalize_hex(&group.group_id_hex)?;
                let group_id = GroupId::new(hex::decode(&group_id_hex)?);
                let welcomer = match group.welcomer_account_id_hex.as_deref() {
                    Some(welcomer) => Some(MemberId::new(hex::decode(normalize_hex(welcomer)?)?)),
                    None => None,
                };
                candidates.push(PendingInvitePolicyCandidate {
                    key: InvitePolicyKey::new(&account.account_id_hex, &group_id_hex),
                    group_id,
                    welcomer,
                });
            }
        }
        Ok(candidates)
    }

    async fn apply_invite_policy_candidate(
        &self,
        candidate: PendingInvitePolicyCandidate,
        retry_state: &mut InvitePolicyRetryState,
        now: tokio::time::Instant,
    ) {
        match self
            .apply_invite_policy(
                &candidate.key.account_id_hex,
                &candidate.group_id,
                candidate.welcomer,
            )
            .await
        {
            Ok(()) => retry_state.clear(&candidate.key),
            Err(err) => {
                let (attempts, retry_delay) = retry_state.record_failure(candidate.key, now);
                tracing::warn!(
                    target: "agent_connector",
                    method = "apply_invite_policy_candidate",
                    error_code = err.privacy_safe_code(),
                    attempts,
                    retry_delay_ms = retry_delay.as_millis() as u64,
                    "invite policy application failed; will retry"
                );
            }
        }
    }

    async fn apply_invite_policy(
        &self,
        account_id_hex: &str,
        group_id: &GroupId,
        welcomer: Option<MemberId>,
    ) -> Result<(), ConnectorError> {
        let account = self.local_account_for_account_id(account_id_hex)?;
        let allowed = self.allow_any
            || match welcomer {
                Some(welcomer) => {
                    let welcomer_account_id_hex = hex::encode(welcomer.as_slice());
                    self.allowlists
                        .contains(&account.account_id_hex, &welcomer_account_id_hex)?
                }
                None => false,
            };
        if allowed {
            self.runtime
                .accept_group_invite(&account.label, group_id)
                .await?;
        } else {
            self.runtime
                .decline_group_invite(&account.label, group_id)
                .await?;
        }
        Ok(())
    }
}

pub async fn serve_socket(config: AgentConnectorConfig) -> Result<(), ConnectorError> {
    validate_control_plane_config(&config)?;
    let listener = bind_connector_socket_with_mode(
        &config.socket,
        config.socket_dir_mode,
        config.socket_mode,
    )?;
    let connector = AgentConnector::open(config)?;
    connector.start().await?;
    loop {
        let (stream, _peer_addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                let connection_error =
                    connector.connection_errors.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::warn!(
                    target: "agent_connector",
                    method = "serve_socket",
                    connection_error,
                    error_code = "accept_error",
                    error_kind = ?err.kind(),
                    "accept failed"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let connector = connector.clone();
        tokio::spawn(async move {
            if let Err(err) = connector.handle_connection(stream).await {
                let connection_error =
                    connector.connection_errors.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::warn!(
                    target: "agent_connector",
                    method = "serve_socket",
                    connection_error,
                    error_code = err.privacy_safe_code(),
                    "connection failed"
                );
            }
        });
    }
}
