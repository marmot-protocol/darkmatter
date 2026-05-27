//! Local Nostr user directory inspection.

use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AppError, MarmotApp, UserDirectorySearch};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{CommandOutput, DmError, npub_for_account_id, parse_public_key, resolve_account};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum UsersCommand {
    #[command(about = "Show a known user from the local directory")]
    Show {
        #[arg(value_name = "NPUB_OR_HEX", help = "User to show")]
        pubkey: String,
    },
    #[command(about = "Search known users in the local directory")]
    Search {
        #[arg(help = "Search query")]
        query: String,
        #[arg(
            long,
            default_value = "0..2",
            value_parser = parse_radius,
            help = "Directory graph radius as START..END"
        )]
        radius: (u8, u8),
    },
}

fn parse_radius(s: &str) -> Result<(u8, u8), String> {
    let Some((start, end)) = s.split_once("..") else {
        return Err("expected format START..END".to_owned());
    };
    let start = start
        .parse::<u8>()
        .map_err(|_| format!("invalid radius start: {start}"))?;
    let end = end
        .parse::<u8>()
        .map_err(|_| format!("invalid radius end: {end}"))?;
    if start > end {
        return Err(format!("radius start ({start}) must be <= end ({end})"));
    }
    Ok((start, end))
}

pub(crate) fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: UsersCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        UsersCommand::Show { pubkey } => {
            let account_id = parse_public_key(&pubkey)?;
            let entry = app
                .directory_entry_for_account_id(&account_id)?
                .ok_or_else(|| AppError::MissingDirectoryEntry(account_id.clone()))?;
            Ok(CommandOutput {
                plain: serde_json::to_string_pretty(&entry)
                    .expect("JSON response serialization cannot fail"),
                json: json!({ "user": entry }),
            })
        }
        UsersCommand::Search { query, radius } => {
            let account = resolve_account(account_home, account_flag)?;
            let results = app.search_user_directory(UserDirectorySearch {
                searcher_account_id_hex: account.account_id_hex.clone(),
                query: query.clone(),
                radius_start: radius.0,
                radius_end: radius.1,
                limit: None,
            })?;
            Ok(CommandOutput {
                plain: if results.is_empty() {
                    "no users".to_owned()
                } else {
                    results
                        .iter()
                        .map(|result| result.npub.clone())
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "query": query,
                    "users": results,
                }),
            })
        }
    }
}
