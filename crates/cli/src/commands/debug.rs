//! Local runtime diagnostics. Not part of the user-facing command surface
//! contract — see CLAUDE.md.

use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::MarmotApp;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    CommandOutput, DmError, npub_for_account_id, relay_lists_json, resolve_account,
    unsupported_command,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum DebugCommand {
    #[command(
        name = "relay-control-state",
        about = "Show the relay-plane subscription and control-state snapshot"
    )]
    RelayControlState,
    #[command(about = "Run a local runtime health check for the selected account")]
    Health,
    #[command(name = "ratchet-tree", hide = true)]
    RatchetTree { group_id: String },
}

pub(crate) fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: DebugCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        DebugCommand::RelayControlState => {
            let accounts = account_home.accounts()?;
            let statuses = accounts
                .into_iter()
                .map(|account| {
                    let relay_lists = app
                        .account_relay_list_status_for_account_id(&account.account_id_hex)
                        .map(relay_lists_json)
                        .unwrap_or_else(|err| json!({"error": err.to_string()}));
                    json!({
                        "account_id": account.account_id_hex,
                        "npub": npub_for_account_id(&account.account_id_hex),
                        "relay_lists": relay_lists,
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandOutput {
                plain: serde_json::to_string_pretty(&statuses)
                    .expect("JSON response serialization cannot fail"),
                json: json!({ "accounts": statuses }),
            })
        }
        DebugCommand::Health => {
            let account = resolve_account(account_home, account_flag)?;
            let status = app.status(&account.label)?;
            Ok(CommandOutput {
                plain: format!(
                    "healthy account={} groups={} messages={}",
                    account.account_id_hex, status.group_count, status.message_count
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "healthy": true,
                    "groups": status.group_count,
                    "messages": status.message_count,
                    "seen_events": status.seen_events,
                }),
            })
        }
        DebugCommand::RatchetTree { .. } => unsupported_command(
            "debug ratchet-tree",
            "ratchet-tree diagnostics are not exposed by marmot-app yet",
        ),
    }
}
