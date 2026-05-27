//! Account follow-list commands.

use cgka_traits::TransportEndpoint;
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AccountRelayListBootstrap, MarmotApp, MarmotAppRuntime};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    CommandOutput, DmError, ensure_local_signing, npub_for_account_id, parse_public_key,
    resolve_account, validate_relay_url,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum FollowsCommand {
    #[command(about = "List followed users")]
    List,
    #[command(about = "Follow a user")]
    Add {
        #[arg(value_name = "NPUB_OR_HEX", help = "User to follow")]
        pubkey: String,
    },
    #[command(about = "Unfollow a user")]
    Remove {
        #[arg(value_name = "NPUB_OR_HEX", help = "User to unfollow")]
        pubkey: String,
    },
    #[command(about = "Check whether a user is followed")]
    Check {
        #[arg(value_name = "NPUB_OR_HEX", help = "User to check")]
        pubkey: String,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: FollowsCommand,
    account_flag: Option<String>,
    relay: Option<String>,
) -> Result<CommandOutput, DmError> {
    let runtime = app.runtime();
    with_runtime(account_home, app, &runtime, command, account_flag, relay).await
}

pub(crate) async fn with_runtime(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: FollowsCommand,
    account_flag: Option<String>,
    relay: Option<String>,
) -> Result<CommandOutput, DmError> {
    let account = resolve_account(account_home, account_flag)?;
    ensure_local_signing(&account)?;
    match command {
        FollowsCommand::List => {
            let follows = app
                .directory_entry_for_account_id(&account.account_id_hex)?
                .map(|entry| entry.follows)
                .unwrap_or_default();
            follows_output(account.account_id_hex, follows)
        }
        FollowsCommand::Check { pubkey } => {
            let target = parse_public_key(&pubkey)?;
            let follows = app
                .directory_entry_for_account_id(&account.account_id_hex)?
                .map(|entry| entry.follows)
                .unwrap_or_default();
            let follows_target = follows.iter().any(|follow| follow == &target);
            Ok(CommandOutput {
                plain: format!("follows {}: {follows_target}", npub_for_account_id(&target)),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "pubkey": target,
                    "user": npub_for_account_id(&target),
                    "follows": follows_target,
                }),
            })
        }
        FollowsCommand::Add { pubkey } => {
            update_follows(app, runtime, account, relay, pubkey, true).await
        }
        FollowsCommand::Remove { pubkey } => {
            update_follows(app, runtime, account, relay, pubkey, false).await
        }
    }
}

async fn update_follows(
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    account: marmot_account::AccountSummary,
    relay: Option<String>,
    pubkey: String,
    add: bool,
) -> Result<CommandOutput, DmError> {
    let target = parse_public_key(&pubkey)?;
    let relay = relay.ok_or(DmError::MissingRelay)?;
    let endpoint = TransportEndpoint(validate_relay_url(&relay)?);
    let mut follows = app
        .directory_entry_for_account_id(&account.account_id_hex)?
        .map(|entry| entry.follows)
        .unwrap_or_default();
    if add {
        if !follows.contains(&target) {
            follows.push(target);
        }
    } else {
        follows.retain(|follow| follow != &target);
    }
    follows.sort();
    follows.dedup();
    runtime
        .publish_account_follow_list(
            &account.label,
            &follows,
            AccountRelayListBootstrap::new(vec![endpoint.clone()], vec![endpoint.clone()]),
        )
        .await?;
    let _ = runtime
        .refresh_user_directory_for_account_id(&account.account_id_hex, vec![endpoint])
        .await;
    follows_output(account.account_id_hex, follows)
}

fn follows_output(account_id: String, follows: Vec<String>) -> Result<CommandOutput, DmError> {
    let follows_json = follows
        .iter()
        .map(|follow| {
            json!({
                "account_id": follow,
                "npub": npub_for_account_id(follow),
            })
        })
        .collect::<Vec<_>>();
    Ok(CommandOutput {
        plain: if follows_json.is_empty() {
            "no follows".to_owned()
        } else {
            follows_json
                .iter()
                .filter_map(|follow| follow.get("npub").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        },
        json: json!({
            "account_id": account_id,
            "npub": npub_for_account_id(&account_id),
            "follows": follows_json,
        }),
    })
}
