//! Method dispatch for the C binding.
//!
//! The C ABI is intentionally tiny: a single [`dispatch`] entrypoint takes a
//! method name plus a JSON request string and returns a JSON response string.
//! This keeps the exported symbol set (and the hand-audited header) small and
//! stable while the Rust side can grow new methods without ABI churn — new
//! methods are additive `match` arms, not new exported functions.
//!
//! Every request/response shape is an explicit serde DTO defined here rather
//! than a re-exported internal type, so the JSON contract does not silently
//! drift when an internal record changes. Where an internal record already
//! derives `Serialize` and is a stable projection (e.g. `AppMessageRecord`,
//! `TimelinePage`, `ChatListRow`), it is serialized directly.

use std::time::{SystemTime, UNIX_EPOCH};

use cgka_traits::{GroupId, TransportEndpoint};
use marmot_app::{AccountSetupRequest, AppMessageQuery};
use serde::{Deserialize, Serialize};

use crate::runtime::{MarmotC, MarmotCError};

/// Turn a list of relay URL strings into the engine's [`TransportEndpoint`]
/// wrapper, stripped of empties. Mirrors `marmot_uniffi::endpoints`.
fn endpoints(urls: &[String]) -> Vec<TransportEndpoint> {
    urls.iter()
        .filter(|u| !u.trim().is_empty())
        .map(|u| TransportEndpoint::from(u.as_str()))
        .collect()
}

/// Decode and validate a 32-byte group id from hex.
fn group_id_from_hex(group_id_hex: &str) -> Result<GroupId, MarmotCError> {
    let trimmed = group_id_hex.trim();
    let bytes = hex::decode(trimmed)
        .map_err(|err| MarmotCError::InvalidArgument(format!("invalid group id hex: {err}")))?;
    if bytes.is_empty() {
        return Err(MarmotCError::InvalidArgument(
            "group id hex is empty".to_owned(),
        ));
    }
    Ok(GroupId::new(bytes))
}

fn parse_request<T: for<'de> Deserialize<'de>>(request_json: &str) -> Result<T, MarmotCError> {
    let trimmed = request_json.trim();
    let source = if trimmed.is_empty() { "{}" } else { trimmed };
    serde_json::from_str(source).map_err(MarmotCError::from)
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Request / response DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct AccountInfo {
    label: String,
    account_id_hex: String,
    local_signing: bool,
    running: bool,
}

#[derive(Debug, Deserialize)]
struct AccountSetupArgs {
    #[serde(default)]
    default_relays: Vec<String>,
    #[serde(default)]
    bootstrap_relays: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LoginArgs {
    identity: String,
    #[serde(default)]
    default_relays: Vec<String>,
    #[serde(default)]
    bootstrap_relays: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AccountRefArgs {
    account_ref: String,
}

#[derive(Debug, Deserialize)]
struct CreateGroupArgs {
    account_ref: String,
    name: String,
    #[serde(default)]
    member_refs: Vec<String>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GroupRefArgs {
    account_ref: String,
    group_id_hex: String,
}

#[derive(Debug, Deserialize)]
struct GroupMembersMutationArgs {
    account_ref: String,
    group_id_hex: String,
    #[serde(default)]
    member_refs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SendTextArgs {
    account_ref: String,
    group_id_hex: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct MessageListArgs {
    account_ref: String,
    #[serde(default)]
    group_id_hex: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ChatListArgs {
    account_ref: String,
    #[serde(default)]
    include_archived: bool,
}

#[derive(Debug, Deserialize)]
struct AgentStreamStartArgs {
    account_ref: String,
    group_id_hex: String,
    #[serde(default)]
    stream_id_hex: Option<String>,
    #[serde(default)]
    quic_candidates: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SendResult {
    published: u64,
    message_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct GroupCreateResult {
    group_id_hex: String,
}

#[derive(Debug, Serialize)]
struct AgentStreamStartResult {
    stream_id_hex: String,
    published: u64,
    message_ids: Vec<String>,
}

fn account_info_from_setup(account: marmot_account::AccountSummary) -> AccountInfo {
    AccountInfo {
        label: account.label,
        account_id_hex: account.account_id_hex,
        local_signing: account.local_signing,
        running: true,
    }
}

fn random_stream_id() -> Vec<u8> {
    // 32 bytes of CSPRNG randomness via OsRng, matching the UniFFI agent-stream
    // surface (`marmot-uniffi`'s `random_agent_stream_id`) and the workspace
    // `transport_quic_stream::random_stream_id` generator.
    use rand::RngCore;
    use rand::rngs::OsRng;
    let mut stream_id = vec![0u8; 32];
    OsRng.fill_bytes(&mut stream_id);
    stream_id
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Execute `method` against `kit` with the JSON `request_json` and return the
/// JSON response. Async runtime calls are driven on the handle's owned tokio
/// runtime via `block_on`.
pub fn dispatch(kit: &MarmotC, method: &str, request_json: &str) -> Result<String, MarmotCError> {
    match method {
        // -------------------------------------------------------------------
        // Account / session
        // -------------------------------------------------------------------
        "account.list" => {
            let accounts = kit.runtime.accounts().managed_accounts()?;
            Ok(serde_json::to_string(&accounts)?)
        }
        "account.create_identity" => {
            let args: AccountSetupArgs = parse_request(request_json)?;
            let request = AccountSetupRequest {
                identity: None,
                default_relays: endpoints(&args.default_relays),
                bootstrap_relays: endpoints(&args.bootstrap_relays),
                publish_missing_relay_lists: true,
                publish_initial_key_package: true,
            };
            let result = kit.tokio.block_on(kit.runtime.create_identity(request))?;
            Ok(serde_json::to_string(&account_info_from_setup(
                result.account,
            ))?)
        }
        "account.login" => {
            let args: LoginArgs = parse_request(request_json)?;
            let request = AccountSetupRequest {
                identity: None,
                default_relays: endpoints(&args.default_relays),
                bootstrap_relays: endpoints(&args.bootstrap_relays),
                publish_missing_relay_lists: true,
                publish_initial_key_package: true,
            };
            let result = kit
                .tokio
                .block_on(kit.runtime.login(args.identity, request))?;
            Ok(serde_json::to_string(&account_info_from_setup(
                result.account,
            ))?)
        }
        "account.remove" => {
            let args: AccountRefArgs = parse_request(request_json)?;
            kit.tokio
                .block_on(kit.runtime.accounts().remove_account(&args.account_ref))?;
            Ok("null".to_owned())
        }

        // -------------------------------------------------------------------
        // Group operations
        // -------------------------------------------------------------------
        "group.create" => {
            let args: CreateGroupArgs = parse_request(request_json)?;
            let group_id = kit.tokio.block_on(kit.runtime.create_group(
                &args.account_ref,
                &args.name,
                &args.member_refs,
                args.description,
            ))?;
            Ok(serde_json::to_string(&GroupCreateResult {
                group_id_hex: hex::encode(group_id.as_slice()),
            })?)
        }
        "group.list" => {
            let args: AccountRefArgs = parse_request(request_json)?;
            let account = kit.runtime.accounts().resolve(&args.account_ref)?;
            let groups = kit.app.groups(&account.label)?;
            Ok(serde_json::to_string(&groups)?)
        }
        "group.members" => {
            let args: GroupRefArgs = parse_request(request_json)?;
            let group_id = group_id_from_hex(&args.group_id_hex)?;
            let members = kit
                .tokio
                .block_on(kit.runtime.group_members(&args.account_ref, &group_id))?;
            Ok(serde_json::to_string(&members)?)
        }
        "group.mls_state" => {
            let args: GroupRefArgs = parse_request(request_json)?;
            let group_id = group_id_from_hex(&args.group_id_hex)?;
            let state = kit
                .tokio
                .block_on(kit.runtime.group_mls_state(&args.account_ref, &group_id))?;
            Ok(serde_json::to_string(&state)?)
        }
        "group.invite_members" => {
            let args: GroupMembersMutationArgs = parse_request(request_json)?;
            let group_id = group_id_from_hex(&args.group_id_hex)?;
            let summary = kit.tokio.block_on(kit.runtime.invite_members(
                &args.account_ref,
                &group_id,
                &args.member_refs,
            ))?;
            Ok(serde_json::to_string(&SendResult {
                published: summary.published as u64,
                message_ids: summary.message_ids,
            })?)
        }
        "group.remove_members" => {
            let args: GroupMembersMutationArgs = parse_request(request_json)?;
            let group_id = group_id_from_hex(&args.group_id_hex)?;
            let summary = kit.tokio.block_on(kit.runtime.remove_members(
                &args.account_ref,
                &group_id,
                &args.member_refs,
            ))?;
            Ok(serde_json::to_string(&SendResult {
                published: summary.published as u64,
                message_ids: summary.message_ids,
            })?)
        }

        // -------------------------------------------------------------------
        // Message send / receive
        // -------------------------------------------------------------------
        "message.send_text" => {
            let args: SendTextArgs = parse_request(request_json)?;
            let group_id = group_id_from_hex(&args.group_id_hex)?;
            let summary = kit.tokio.block_on(kit.runtime.send_message(
                &args.account_ref,
                &group_id,
                args.text.into_bytes(),
            ))?;
            Ok(serde_json::to_string(&SendResult {
                published: summary.published as u64,
                message_ids: summary.message_ids,
            })?)
        }
        "message.list" => {
            let args: MessageListArgs = parse_request(request_json)?;
            let query = AppMessageQuery {
                group_id_hex: args.group_id_hex,
                limit: args.limit.map(|n| n as usize),
            };
            let records = kit.runtime.messages_with_query(&args.account_ref, query)?;
            Ok(serde_json::to_string(&records)?)
        }

        // -------------------------------------------------------------------
        // Storage primitives (timeline + chat list projections)
        // -------------------------------------------------------------------
        "timeline.list" => {
            let args: MessageListArgs = parse_request(request_json)?;
            let group_id_hex = match args.group_id_hex {
                Some(value) if !value.trim().is_empty() => {
                    Some(hex::encode(group_id_from_hex(&value)?.as_slice()))
                }
                _ => None,
            };
            let query = marmot_app::TimelineMessageQuery {
                group_id_hex,
                search: None,
                pagination: marmot_app::TimelinePagination {
                    limit: args.limit.map(|n| n as usize),
                    ..marmot_app::TimelinePagination::default()
                },
            };
            let page = kit
                .runtime
                .timeline_messages_with_query(&args.account_ref, query)?;
            Ok(serde_json::to_string(&page)?)
        }
        "chat.list" => {
            let args: ChatListArgs = parse_request(request_json)?;
            let rows = kit
                .runtime
                .chat_list(&args.account_ref, args.include_archived)?;
            Ok(serde_json::to_string(&rows)?)
        }

        // -------------------------------------------------------------------
        // Agent text streams
        // -------------------------------------------------------------------
        "agent_stream.start" => {
            let args: AgentStreamStartArgs = parse_request(request_json)?;
            let group_id = group_id_from_hex(&args.group_id_hex)?;
            let stream_id = match args.stream_id_hex {
                Some(value) => hex::decode(value.trim()).map_err(|err| {
                    MarmotCError::InvalidArgument(format!("invalid stream id hex: {err}"))
                })?,
                None => random_stream_id(),
            };
            let stream_id_hex = hex::encode(&stream_id);
            let (_, summary) = kit.tokio.block_on(kit.runtime.start_agent_text_stream(
                &args.account_ref,
                &group_id,
                &stream_id,
                unix_now_seconds(),
                args.quic_candidates,
            ))?;
            Ok(serde_json::to_string(&AgentStreamStartResult {
                stream_id_hex,
                published: summary.published as u64,
                message_ids: summary.message_ids,
            })?)
        }

        other => Err(MarmotCError::UnknownMethod(other.to_owned())),
    }
}
