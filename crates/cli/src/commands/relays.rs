//! Account relay-list inspection and updates (NIP-65, inbox, key_package).

use cgka_traits::TransportEndpoint;
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AccountRelayListStatus, MarmotApp, MarmotAppRuntime};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    CommandOutput, DmError, ensure_local_signing, npub_for_account_id, relay_endpoints,
    relay_lists_json, resolve_account, unsupported_command, validate_relay_url,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum RelaysCommand {
    #[command(about = "List account relay URLs")]
    List {
        #[arg(
            long = "type",
            value_name = "TYPE",
            help = "Relay list type: nip65, inbox, or key_package"
        )]
        relay_type: Option<String>,
    },
    #[command(about = "Add a relay URL to an account relay list")]
    Add {
        #[arg(help = "Relay URL to add")]
        url: String,
        #[arg(
            long = "type",
            value_name = "TYPE",
            help = "Relay list type: nip65, inbox, or key_package"
        )]
        relay_type: String,
    },
    #[command(about = "Remove a relay URL from an account relay list")]
    Remove {
        #[arg(help = "Relay URL to remove")]
        url: String,
        #[arg(
            long = "type",
            value_name = "TYPE",
            help = "Relay list type: nip65, inbox, or key_package"
        )]
        relay_type: String,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: RelaysCommand,
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
    command: RelaysCommand,
    account_flag: Option<String>,
    relay: Option<String>,
) -> Result<CommandOutput, DmError> {
    let account = resolve_account(account_home, account_flag)?;
    ensure_local_signing(&account)?;
    match command {
        RelaysCommand::List { relay_type } => {
            let status = app.account_relay_list_status(&account.label)?;
            let relays = relays_for_type(&status, relay_type.as_deref())?;
            Ok(CommandOutput {
                plain: if relays.is_empty() {
                    "no relays".to_owned()
                } else {
                    relays.join("\n")
                },
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "relay_type": relay_type,
                    "relays": relays,
                    "relay_lists": relay_lists_json(status),
                }),
            })
        }
        RelaysCommand::Add { url, relay_type } => {
            update_relay_list(app, runtime, account, relay, relay_type, url, true).await
        }
        RelaysCommand::Remove { url, relay_type } => {
            update_relay_list(app, runtime, account, relay, relay_type, url, false).await
        }
    }
}

async fn update_relay_list(
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    account: marmot_account::AccountSummary,
    relay: Option<String>,
    relay_type: String,
    url: String,
    add: bool,
) -> Result<CommandOutput, DmError> {
    let relay_type = normalize_relay_type(&relay_type)?;
    let url = validate_relay_url(&url)?;
    let status = app.account_relay_list_status(&account.label)?;
    let mut relays = relays_for_type(&status, Some(&relay_type))?;
    if add {
        if !relays.contains(&url) {
            relays.push(url.clone());
        }
    } else {
        relays.retain(|relay| relay != &url);
    }
    relays.sort();
    relays.dedup();
    let publish_relays = relay_endpoints(relays.clone())?;
    let bootstrap = relay
        .map(validate_relay_url)
        .transpose()?
        .or_else(|| relays.first().cloned())
        .ok_or(DmError::MissingRelay)?;
    let bootstrap_relays = vec![TransportEndpoint(bootstrap)];
    let status = runtime
        .publish_account_relay_list_kind(
            &account.label,
            &relay_type,
            publish_relays,
            bootstrap_relays,
        )
        .await?;
    Ok(CommandOutput {
        plain: relays.join("\n"),
        json: json!({
            "account_id": account.account_id_hex,
            "npub": npub_for_account_id(&account.account_id_hex),
            "relay_type": relay_type,
            "relays": relays,
            "relay_lists": relay_lists_json(status),
        }),
    })
}

fn relays_for_type(
    status: &AccountRelayListStatus,
    relay_type: Option<&str>,
) -> Result<Vec<String>, DmError> {
    match relay_type.map(normalize_relay_type).transpose()?.as_deref() {
        Some("nip65") => Ok(status.nip65.relays.clone()),
        Some("inbox") => Ok(status.inbox.relays.clone()),
        Some("key_package") => Ok(status.key_package.relays.clone()),
        None => {
            let mut relays = status.default_relays.clone();
            relays.extend(status.inbox.relays.clone());
            relays.extend(status.key_package.relays.clone());
            relays.sort();
            relays.dedup();
            Ok(relays)
        }
        Some(_) => unreachable!("normalize_relay_type constrains values"),
    }
}

fn normalize_relay_type(value: &str) -> Result<String, DmError> {
    match value {
        "nip65" => Ok("nip65".to_owned()),
        "inbox" => Ok("inbox".to_owned()),
        "key_package" | "key-package" => Ok("key_package".to_owned()),
        _ => unsupported_command("relays", "relay type must be nip65, inbox, or key_package"),
    }
}
