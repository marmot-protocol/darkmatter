//! Singular `message` and plural `messages` command surface. Both forms
//! dispatch through the same handler during the Whitenoise transition.

use std::collections::HashMap;

use cgka_traits::GroupId;
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AppMessageQuery, AppMessageRecord, MarmotApp, MarmotAppRuntime};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    CommandOutput, DmError, apply_message_cursors, display_name_for_sender, ensure_local_signing,
    message_record_json, normalize_group_id_hex, npub_for_account_id, resolve_account,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum MessageCommand {
    #[command(about = "Send a message to a group")]
    Send {
        #[arg(long = "group", value_name = "GROUP", help = "Group id to send to")]
        group_flag: Option<String>,
        #[arg(
            value_name = "GROUP_OR_TEXT",
            allow_hyphen_values = true,
            help = "Either GROUP TEXT... or TEXT... when --group is provided"
        )]
        args: Vec<String>,
    },
    #[command(about = "Delete a message for the selected account's local view")]
    Delete {
        #[arg(help = "Group id containing the message")]
        group_id: String,
        #[arg(help = "Message id to delete")]
        message_id: String,
    },
    #[command(about = "Retry a failed outbound message event")]
    Retry {
        #[arg(help = "Group id containing the failed event")]
        group_id: String,
        #[arg(help = "Event id to retry")]
        event_id: String,
    },
    #[command(about = "React to a message")]
    React {
        #[arg(help = "Group id containing the message")]
        group_id: String,
        #[arg(help = "Message id to react to")]
        message_id: String,
        #[arg(default_value = "+", help = "Emoji reaction to add")]
        emoji: String,
    },
    #[command(about = "Remove your reaction from a message")]
    Unreact {
        #[arg(help = "Group id containing the message")]
        group_id: String,
        #[arg(help = "Message id to unreact from")]
        message_id: String,
    },
    #[command(about = "List messages from one group")]
    List {
        #[arg(value_name = "GROUP", help = "Group id to list")]
        group_id: Option<String>,
        #[arg(long, help = "Group id to list")]
        group: Option<String>,
        #[arg(long, help = "Only include messages before this unix timestamp")]
        before: Option<u64>,
        #[arg(long, help = "Only include messages before this message id")]
        before_message_id: Option<String>,
        #[arg(long, help = "Only include messages after this unix timestamp")]
        after: Option<u64>,
        #[arg(long, help = "Only include messages after this message id")]
        after_message_id: Option<String>,
        #[arg(long, help = "Maximum number of messages to return")]
        limit: Option<usize>,
    },
    #[command(about = "Search messages in one group")]
    Search {
        #[arg(help = "Group id to search")]
        group_id: String,
        #[arg(help = "Search query")]
        query: String,
        #[arg(long, help = "Maximum number of results to return")]
        limit: Option<usize>,
    },
    #[command(name = "search-all", about = "Search messages across all local groups")]
    SearchAll {
        #[arg(help = "Search query")]
        query: String,
        #[arg(long, help = "Maximum number of results to return")]
        limit: Option<usize>,
    },
    #[command(about = "Subscribe to live message updates through the daemon")]
    Subscribe {
        #[arg(help = "Group id to watch; omit to watch all local groups")]
        group: Option<String>,
        #[arg(long, help = "Initial replay limit")]
        limit: Option<usize>,
    },
    #[command(about = "List, search, and subscribe to the materialized message timeline")]
    Timeline {
        #[command(subcommand)]
        command: MessageTimelineCommand,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum MessageTimelineCommand {
    #[command(about = "List materialized timeline messages")]
    List {
        #[arg(value_name = "GROUP", help = "Group id to list")]
        group_id: Option<String>,
        #[arg(long, help = "Group id to list")]
        group: Option<String>,
        #[arg(long, help = "Only include timeline rows before this unix timestamp")]
        before: Option<u64>,
        #[arg(long, help = "Only include timeline rows before this message id")]
        before_message_id: Option<String>,
        #[arg(long, help = "Only include timeline rows after this unix timestamp")]
        after: Option<u64>,
        #[arg(long, help = "Only include timeline rows after this message id")]
        after_message_id: Option<String>,
        #[arg(long, help = "Maximum number of timeline rows to return")]
        limit: Option<usize>,
    },
    #[command(about = "Search materialized timeline messages")]
    Search {
        #[arg(help = "Search query")]
        query: String,
        #[arg(value_name = "GROUP", help = "Optional group id to search")]
        group_id: Option<String>,
        #[arg(long, help = "Group id to search")]
        group: Option<String>,
        #[arg(long, help = "Maximum number of results to return")]
        limit: Option<usize>,
    },
    #[command(about = "Subscribe to live materialized timeline updates through the daemon")]
    Subscribe {
        #[arg(help = "Group id to watch; omit to watch all local groups")]
        group: Option<String>,
        #[arg(long, help = "Initial replay limit")]
        limit: Option<usize>,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: MessageCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    let runtime = app.runtime();
    with_runtime(account_home, app, &runtime, command, account_flag).await
}

pub(crate) async fn with_runtime(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: MessageCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        MessageCommand::Send { group_flag, args } => {
            let (group, text) = message_target_and_text(group_flag, args)?;
            if text.is_empty() {
                return Err(DmError::EmptyMessage);
            }
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(group)?);
            let payload = text.join(" ");
            let summary = runtime
                .send_message(&account.label, &group_id, payload.into_bytes())
                .await?;
            Ok(CommandOutput {
                plain: format!("sent message published={}", summary.published),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                }),
            })
        }
        MessageCommand::Delete {
            group_id,
            message_id,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(normalize_group_id_hex(&group_id)?)?);
            let summary = runtime
                .delete_message(&account.label, &group_id, &message_id)
                .await?;
            Ok(CommandOutput {
                plain: format!(
                    "deleted message {message_id} published={}",
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "target_message_id": message_id,
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                }),
            })
        }
        MessageCommand::Retry { group_id, event_id } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(normalize_group_id_hex(&group_id)?)?);
            let summary = runtime
                .retry_group_convergence(&account.label, &group_id)
                .await?;
            Ok(CommandOutput {
                plain: format!(
                    "retried group convergence for {event_id} published={}",
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "target_event_id": event_id,
                    "retry_scope": "group_convergence",
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                }),
            })
        }
        MessageCommand::React {
            group_id,
            message_id,
            emoji,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(normalize_group_id_hex(&group_id)?)?);
            let summary = runtime
                .react_to_message(&account.label, &group_id, &message_id, &emoji)
                .await?;
            Ok(CommandOutput {
                plain: format!(
                    "reacted {emoji} to {message_id} published={}",
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "target_message_id": message_id,
                    "emoji": emoji,
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                }),
            })
        }
        MessageCommand::Unreact {
            group_id,
            message_id,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(normalize_group_id_hex(&group_id)?)?);
            let summary = runtime
                .unreact_from_message(&account.label, &group_id, &message_id)
                .await?;
            Ok(CommandOutput {
                plain: format!(
                    "removed reaction from {message_id} published={}",
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "target_message_id": message_id,
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                }),
            })
        }
        MessageCommand::List {
            group_id,
            group,
            before,
            before_message_id,
            after,
            after_message_id,
            limit,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group = group.or(group_id);
            let uses_cursor = before.is_some()
                || before_message_id.is_some()
                || after.is_some()
                || after_message_id.is_some();
            let mut messages = app.messages_with_query(
                &account.label,
                AppMessageQuery {
                    group_id_hex: group
                        .map(|group| normalize_group_id_hex(&group))
                        .transpose()?,
                    limit: if uses_cursor { None } else { limit },
                },
            )?;
            if uses_cursor {
                messages = apply_message_cursors(
                    messages,
                    before,
                    before_message_id.as_deref(),
                    after,
                    after_message_id.as_deref(),
                    limit,
                );
            }
            Ok(CommandOutput {
                plain: message_list_plain(&messages),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "messages": message_list_json_with_profiles(app, messages),
                }),
            })
        }
        MessageCommand::Search {
            group_id,
            query,
            limit,
        } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let messages = search_messages(app, &account.label, Some(group_id), &query, limit)?;
            Ok(CommandOutput {
                plain: message_list_plain(&messages),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "query": query,
                    "messages": message_list_json_with_profiles(app, messages),
                }),
            })
        }
        MessageCommand::SearchAll { query, limit } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let messages = search_messages(app, &account.label, None, &query, limit)?;
            Ok(CommandOutput {
                plain: message_list_plain(&messages),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "query": query,
                    "messages": message_list_json_with_profiles(app, messages),
                }),
            })
        }
        MessageCommand::Subscribe { .. } => Err(DmError::MessagesSubscribeRequiresDaemon),
        MessageCommand::Timeline { command } => match command {
            MessageTimelineCommand::Subscribe { .. } => {
                Err(DmError::MessagesSubscribeRequiresDaemon)
            }
            MessageTimelineCommand::List { .. } | MessageTimelineCommand::Search { .. } => {
                crate::unsupported_command(
                    "messages timeline",
                    "timeline list/search ports are pending integration of the upstream timeline feature into the decomposed command modules",
                )
            }
        },
    }
}

fn message_target_and_text(
    group_flag: Option<String>,
    mut args: Vec<String>,
) -> Result<(String, Vec<String>), DmError> {
    if let Some(group) = group_flag {
        return Ok((group, args));
    }
    if args.is_empty() {
        return Err(DmError::MissingGroupId);
    }
    let group = args.remove(0);
    Ok((group, args))
}

fn search_messages(
    app: &MarmotApp,
    label: &str,
    group_id: Option<String>,
    query: &str,
    limit: Option<usize>,
) -> Result<Vec<AppMessageRecord>, DmError> {
    let group_id_hex = group_id
        .map(|group| normalize_group_id_hex(&group))
        .transpose()?;
    let mut matches = app
        .messages_with_query(
            label,
            AppMessageQuery {
                group_id_hex,
                limit: None,
            },
        )?
        .into_iter()
        .filter(|message| message.plaintext.contains(query))
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        matches.truncate(limit);
    }
    Ok(matches)
}

fn message_list_plain(messages: &[AppMessageRecord]) -> String {
    if messages.is_empty() {
        return "no messages".to_owned();
    }
    messages
        .iter()
        .map(|message| {
            format!(
                "group={} from={}: {}",
                message.group_id_hex, message.sender, message.plaintext
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn message_list_json_with_profiles(app: &MarmotApp, messages: Vec<AppMessageRecord>) -> Vec<Value> {
    let mut display_names_by_sender: HashMap<String, Option<String>> = HashMap::new();
    messages
        .into_iter()
        .map(|message| {
            let from_display_name = display_names_by_sender
                .entry(message.sender.clone())
                .or_insert_with(|| display_name_for_sender(app, &message.sender))
                .clone();
            message_record_json(message, from_display_name)
        })
        .collect()
}
