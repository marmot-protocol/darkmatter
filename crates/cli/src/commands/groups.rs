//! Whitenoise-canonical `groups` command. Some variants (Create, AddMembers,
//! RemoveMembers, Members, Rename) delegate to the singular `group` handler
//! during the Whitenoise transition.

use cgka_traits::GroupId;
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AppError, AppGroupMlsState, MarmotApp, MarmotAppRuntime};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::commands::group::{self, GroupCommand};
use crate::{
    CommandOutput, DmError, ensure_local_signing, group_json, group_list_plain, group_show_output,
    normalize_group_id_hex, npub_for_account_id, parse_public_key, resolve_account,
    unsupported_command,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum GroupsCommand {
    #[command(about = "List groups for the selected account")]
    List,
    #[command(about = "Create a group and invite members by pubkey")]
    Create {
        #[arg(help = "Group display name")]
        name: String,
        #[arg(value_name = "MEMBER", help = "Member npub or hex pubkey to add")]
        members: Vec<String>,
        #[arg(long, help = "Optional group description")]
        description: Option<String>,
    },
    #[command(about = "Show group metadata and membership state")]
    Show {
        #[arg(help = "Group id to show")]
        group_id: String,
    },
    #[command(name = "add-members", about = "Add members to a group")]
    AddMembers {
        #[arg(help = "Group id to update")]
        group_id: String,
        #[arg(
            value_name = "MEMBER",
            required = true,
            help = "Member npub or hex pubkey to add"
        )]
        members: Vec<String>,
    },
    #[command(name = "remove-members", about = "Remove members from a group")]
    RemoveMembers {
        #[arg(help = "Group id to update")]
        group_id: String,
        #[arg(
            value_name = "MEMBER",
            required = true,
            help = "Member npub or hex pubkey to remove"
        )]
        members: Vec<String>,
    },
    #[command(about = "List group members")]
    Members {
        #[arg(help = "Group id to inspect")]
        group_id: String,
    },
    #[command(about = "List group admins")]
    Admins {
        #[arg(help = "Group id to inspect")]
        group_id: String,
    },
    #[command(about = "List group relay hints")]
    Relays {
        #[arg(help = "Group id to inspect")]
        group_id: String,
    },
    #[command(about = "Leave a group")]
    Leave {
        #[arg(help = "Group id to leave")]
        group_id: String,
    },
    #[command(about = "Rename a group")]
    Rename {
        #[arg(help = "Group id to rename")]
        group_id: String,
        #[arg(help = "New group name")]
        name: String,
    },
    #[command(hide = true)]
    Invites,
    #[command(hide = true)]
    Accept { group_id: String },
    #[command(hide = true)]
    Decline { group_id: String },
    #[command(about = "Promote a member to group admin")]
    Promote {
        #[arg(help = "Group id to update")]
        group_id: String,
        #[arg(help = "Member npub or hex pubkey to promote")]
        pubkey: String,
    },
    #[command(about = "Demote a group admin")]
    Demote {
        #[arg(help = "Group id to update")]
        group_id: String,
        #[arg(help = "Admin npub or hex pubkey to demote")]
        pubkey: String,
    },
    #[command(name = "self-demote", about = "Demote the selected account from admin")]
    SelfDemote {
        #[arg(help = "Group id to update")]
        group_id: String,
    },
    #[command(
        name = "subscribe-state",
        about = "Subscribe to live group-state updates through the daemon"
    )]
    SubscribeState {
        #[arg(help = "Group id to watch")]
        group_id: String,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: GroupsCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    let runtime = app.runtime();
    with_runtime(account_home, app, &runtime, command, account_flag).await
}

pub(crate) async fn with_runtime(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: GroupsCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        GroupsCommand::List => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let groups = app.visible_groups(&account.label)?;
            Ok(CommandOutput {
                plain: group_list_plain(&groups),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "groups": groups.into_iter().map(group_json).collect::<Vec<_>>(),
                }),
            })
        }
        GroupsCommand::Create {
            name,
            members,
            description,
        } => {
            group::with_runtime(
                account_home,
                app,
                runtime,
                GroupCommand::Create {
                    name,
                    members,
                    description,
                },
                account_flag,
            )
            .await
        }
        GroupsCommand::Show { group_id } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            let group_id_hex = normalize_group_id_hex(&group_id)?;
            let group_id = GroupId::new(hex::decode(&group_id_hex)?);
            let mls = runtime
                .group_mls_state(&account.label, &group_id)
                .await
                .map(group_mls_state_json)?;
            group_show_output(app, account, group_id_hex, Some(mls))
        }
        GroupsCommand::AddMembers { group_id, members } => {
            group::with_runtime(
                account_home,
                app,
                runtime,
                GroupCommand::Invite {
                    group: group_id,
                    members,
                },
                account_flag,
            )
            .await
        }
        GroupsCommand::RemoveMembers { group_id, members } => {
            group::with_runtime(
                account_home,
                app,
                runtime,
                GroupCommand::Remove {
                    group: group_id,
                    members,
                },
                account_flag,
            )
            .await
        }
        GroupsCommand::Members { group_id } => {
            group::with_runtime(
                account_home,
                app,
                runtime,
                GroupCommand::Members { group: group_id },
                account_flag,
            )
            .await
        }
        GroupsCommand::Admins { group_id } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = normalize_group_id_hex(&group_id)?;
            let group = app
                .group(&account.label, &group_id)?
                .ok_or_else(|| AppError::UnknownGroup(group_id.clone()))?;
            let admins = group
                .admin_policy
                .admins
                .iter()
                .map(|admin| {
                    json!({
                        "admin_id": admin,
                        "npub": npub_for_account_id(admin),
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandOutput {
                plain: if admins.is_empty() {
                    "no admins".to_owned()
                } else {
                    admins
                        .iter()
                        .filter_map(|admin| admin.get("npub").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": group_id,
                    "admins": admins,
                }),
            })
        }
        GroupsCommand::Relays { group_id } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = normalize_group_id_hex(&group_id)?;
            let group = app
                .group(&account.label, &group_id)?
                .ok_or_else(|| AppError::UnknownGroup(group_id.clone()))?;
            Ok(CommandOutput {
                plain: group.endpoint.clone(),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": group_id,
                    "relays": [group.endpoint],
                }),
            })
        }
        GroupsCommand::Leave { group_id } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let group_id = GroupId::new(hex::decode(normalize_group_id_hex(&group_id)?)?);
            let summary = runtime.leave_group(&account.label, &group_id).await?;
            Ok(CommandOutput {
                plain: format!(
                    "left group {} published={}",
                    hex::encode(group_id.as_slice()),
                    summary.published
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "group_id": hex::encode(group_id.as_slice()),
                    "published": summary.published,
                    "message_ids": summary.message_ids,
                }),
            })
        }
        GroupsCommand::Rename { group_id, name } => {
            group::with_runtime(
                account_home,
                app,
                runtime,
                GroupCommand::Update {
                    group: group_id,
                    name: Some(name),
                    description: None,
                },
                account_flag,
            )
            .await
        }
        GroupsCommand::Invites => unsupported_command(
            "groups invites",
            "user-driven invite accept/decline state is not modeled yet",
        ),
        GroupsCommand::Accept { .. } => unsupported_command(
            "groups accept",
            "welcomes are auto-accepted today; user-driven accept is not modeled yet",
        ),
        GroupsCommand::Decline { .. } => unsupported_command(
            "groups decline",
            "user-driven invite decline is not modeled yet",
        ),
        GroupsCommand::Promote { group_id, pubkey } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            group_admin_policy_output(
                app,
                runtime,
                account,
                group_id,
                GroupAdminAction::Promote(pubkey),
            )
            .await
        }
        GroupsCommand::Demote { group_id, pubkey } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            group_admin_policy_output(
                app,
                runtime,
                account,
                group_id,
                GroupAdminAction::Demote(pubkey),
            )
            .await
        }
        GroupsCommand::SelfDemote { group_id } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            group_admin_policy_output(
                app,
                runtime,
                account,
                group_id,
                GroupAdminAction::SelfDemote,
            )
            .await
        }
        GroupsCommand::SubscribeState { .. } => Err(DmError::MessagesSubscribeRequiresDaemon),
    }
}

enum GroupAdminAction {
    Promote(String),
    Demote(String),
    SelfDemote,
}

async fn group_admin_policy_output(
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    account: marmot_account::AccountSummary,
    group_id: String,
    action: GroupAdminAction,
) -> Result<CommandOutput, DmError> {
    app.status(&account.label)?;
    let group_id = GroupId::new(hex::decode(normalize_group_id_hex(&group_id)?)?);
    let group_id_hex = hex::encode(group_id.as_slice());
    let (verb, admin_id, summary) = match action {
        GroupAdminAction::Promote(pubkey) => {
            let admin_id = parse_public_key(&pubkey)?;
            let summary = runtime
                .promote_admin(&account.label, &group_id, &pubkey)
                .await?;
            ("promoted", admin_id, summary)
        }
        GroupAdminAction::Demote(pubkey) => {
            let admin_id = parse_public_key(&pubkey)?;
            let summary = runtime
                .demote_admin(&account.label, &group_id, &pubkey)
                .await?;
            ("demoted", admin_id, summary)
        }
        GroupAdminAction::SelfDemote => {
            let admin_id = account.account_id_hex.clone();
            let summary = runtime.self_demote_admin(&account.label, &group_id).await?;
            ("self-demoted", admin_id, summary)
        }
    };
    let group = app
        .group(&account.label, &group_id_hex)?
        .ok_or_else(|| AppError::UnknownGroup(group_id_hex.clone()))?;
    let admin_npub = npub_for_account_id(&admin_id);
    Ok(CommandOutput {
        plain: format!("{verb} admin {} published={}", admin_id, summary.published),
        json: json!({
            "account_id": account.account_id_hex,
            "npub": npub_for_account_id(&account.account_id_hex),
            "group_id": group_id_hex,
            "admin_id": admin_id,
            "admin_npub": admin_npub,
            "group": group_json(group),
            "published": summary.published,
            "message_ids": summary.message_ids,
        }),
    })
}

pub(crate) fn group_mls_state_json(state: AppGroupMlsState) -> Value {
    json!({
        "group_id": state.group_id_hex,
        "epoch": state.epoch,
        "member_count": state.member_count,
        "required_app_components": state.required_app_components,
    })
}
