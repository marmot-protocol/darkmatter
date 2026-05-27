//! Singular `account` and plural `accounts` command surface. Both dispatch
//! through the same handler during the Whitenoise transition.

use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AccountSetupRequest, AccountSetupResult, AppError, MarmotApp};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    CliRuntimeInfo, CommandOutput, DmError, account_display_name_or_npub,
    account_selector_or_default, account_summary_json, apply_global_relay_defaults, dm_status_json,
    is_nostr_secret, missing_relay_list_status, npub_for_account_id, public_account_status_json,
    relay_endpoints, relay_list_status_for_account_id, relay_lists_json, relay_setup_plain,
    resolve_account, validate_materialized_secret_identity,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum AccountCommand {
    #[command(about = "Create a local account and publish its bootstrap records")]
    Create {
        #[arg(
            value_name = "NPUB_OR_HEX",
            help = "Optional npub or hex pubkey to track"
        )]
        identity: Option<String>,
        #[serde(default)]
        #[arg(long, help = "Read an nsec private key from stdin instead of argv")]
        nsec_stdin: bool,
        #[arg(
            long,
            value_name = "URLS",
            value_delimiter = ',',
            help = "Comma-separated account relay list to publish"
        )]
        default_relays: Vec<String>,
        #[arg(
            long,
            value_name = "URLS",
            value_delimiter = ',',
            help = "Comma-separated bootstrap relays used to find account records"
        )]
        bootstrap_relays: Vec<String>,
        #[arg(
            long,
            help = "Publish missing relay-list records during account creation"
        )]
        publish_missing_relay_lists: bool,
    },
    #[command(about = "List local accounts")]
    List,
    #[command(about = "Show account readiness, relay-list, and KeyPackage status")]
    Status {
        #[arg(help = "Optional account label, npub, or hex pubkey")]
        account: Option<String>,
    },
    #[command(
        name = "relay-lists",
        about = "Fetch and inspect published relay lists"
    )]
    RelayLists {
        #[arg(
            value_name = "NPUB_OR_HEX",
            help = "Account to inspect; defaults to selected account"
        )]
        account: Option<String>,
        #[arg(
            long,
            value_name = "URLS",
            value_delimiter = ',',
            help = "Comma-separated relays to use for relay-list discovery"
        )]
        bootstrap_relays: Vec<String>,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: AccountCommand,
    runtime_info: CliRuntimeInfo,
    account_flag: Option<String>,
    relay: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        AccountCommand::Create {
            identity,
            nsec_stdin,
            default_relays,
            bootstrap_relays,
            publish_missing_relay_lists,
        } => {
            create_or_import(
                app,
                identity,
                default_relays,
                bootstrap_relays,
                publish_missing_relay_lists,
                false,
                nsec_stdin,
                runtime_info,
                relay,
            )
            .await
        }
        AccountCommand::List => {
            let accounts = account_home.accounts()?;
            let accounts_json = accounts
                .into_iter()
                .map(|account| account_summary_json(app, account))
                .collect::<Vec<_>>();
            let plain = if accounts_json.is_empty() {
                "no accounts".to_owned()
            } else {
                accounts_json
                    .iter()
                    .map(|account| {
                        format!(
                            "{} {} local-signing={}",
                            account_display_name_or_npub(account),
                            account
                                .get("account_id")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            account
                                .get("local_signing")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            Ok(CommandOutput {
                plain,
                json: json!({ "accounts": accounts_json }),
            })
        }
        AccountCommand::Status { account } => {
            let account = resolve_account(account_home, account.or(account_flag))?;
            if !account.local_signing {
                let relay_lists =
                    app.account_relay_list_status_for_account_id(&account.account_id_hex)?;
                let json = public_account_status_json(&account, relay_lists);
                return Ok(CommandOutput {
                    plain: serde_json::to_string_pretty(&json)
                        .expect("JSON response serialization cannot fail"),
                    json,
                });
            }
            let status = app.status(&account.label)?;
            Ok(CommandOutput {
                plain: serde_json::to_string_pretty(&dm_status_json(status.clone(), &runtime_info))
                    .expect("JSON response serialization cannot fail"),
                json: dm_status_json(status, &runtime_info),
            })
        }
        AccountCommand::RelayLists {
            account,
            bootstrap_relays,
        } => {
            let account_id = account_selector_or_default(account_home, account, account_flag)?;
            let relay_lists = relay_list_status_for_account_id(
                app,
                &account_id,
                relay_endpoints(bootstrap_relays)?,
            )
            .await?;
            Ok(CommandOutput {
                plain: relay_setup_plain(&relay_lists),
                json: json!({
                    "account_id": account_id,
                    "npub": npub_for_account_id(&account_id),
                    "relay_lists": relay_lists_json(relay_lists),
                }),
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_or_import(
    app: &MarmotApp,
    identity: Option<String>,
    mut default_relays: Vec<String>,
    mut bootstrap_relays: Vec<String>,
    publish_missing_relay_lists: bool,
    publish_initial_key_package: bool,
    nsec_stdin: bool,
    _runtime_info: CliRuntimeInfo,
    relay: Option<String>,
) -> Result<CommandOutput, DmError> {
    validate_materialized_secret_identity("account create", &identity, nsec_stdin)?;
    let global_relay_defaults =
        apply_global_relay_defaults(&mut default_relays, &mut bootstrap_relays, relay);
    let imports_private_key = identity.as_deref().is_some_and(is_nostr_secret);
    let creates_new_private_key = identity.is_none();
    let adds_public_account = identity
        .as_deref()
        .is_some_and(|value| !is_nostr_secret(value));
    if creates_new_private_key && default_relays.is_empty() {
        return Err(DmError::MissingRelay);
    }
    if imports_private_key && default_relays.is_empty() && bootstrap_relays.is_empty() {
        return Err(DmError::MissingRelay);
    }
    if adds_public_account && bootstrap_relays.is_empty() && default_relays.is_empty() {
        return Err(DmError::MissingRelay);
    }
    if adds_public_account && !default_relays.is_empty() && !global_relay_defaults.default_relays {
        return Err(DmError::PublicAccountCannotSign);
    }

    let default_relays = relay_endpoints(default_relays)?;
    let bootstrap_relays = relay_endpoints(bootstrap_relays)?;
    let setup = app
        .runtime()
        .create_or_import_account(AccountSetupRequest {
            identity,
            default_relays,
            bootstrap_relays,
            publish_missing_relay_lists,
            publish_initial_key_package,
        })
        .await
        .map_err(map_setup_error)?;

    setup_command_output(setup)
}

pub(crate) fn setup_command_output(setup: AccountSetupResult) -> Result<CommandOutput, DmError> {
    let key_package_plain = setup
        .key_package_bytes
        .map(|bytes| format!(" key-package-bytes={bytes}"))
        .unwrap_or_default();
    Ok(CommandOutput {
        plain: format!(
            "created identity {} local-signing={} relay-lists={}{}",
            npub_for_account_id(&setup.account.account_id_hex),
            setup.account.local_signing,
            relay_setup_plain(&setup.relay_lists),
            key_package_plain
        ),
        json: json!({
            "account_id": setup.account.account_id_hex,
            "npub": npub_for_account_id(&setup.account.account_id_hex),
            "local_signing": setup.account.local_signing,
            "relay_lists": relay_lists_json(setup.relay_lists),
            "key_package": setup.key_package_bytes.map(|bytes| json!({
                "published": true,
                "bytes": bytes,
            })),
            "profile": setup.profile,
        }),
    })
}

pub(crate) fn map_setup_error(err: AppError) -> DmError {
    if let AppError::MissingRelayLists(missing) = &err {
        let status = missing_relay_list_status(missing.clone());
        return DmError::MissingRelayLists(missing.clone(), Box::new(status));
    }
    err.into()
}
