//! Local MLS KeyPackage publication, rotation, and fetch.

use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{MarmotApp, MarmotAppRuntime};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    CommandOutput, DmError, account_selector_or_default, ensure_local_signing,
    key_package_fetch_json, npub_for_account_id, parse_public_key, relay_endpoints,
    resolve_account, unsupported_command,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum KeyPackageCommand {
    #[command(about = "List local KeyPackage publication records")]
    List,
    #[command(about = "Republish the currently cached KeyPackage")]
    Publish,
    #[command(
        about = "Force mint and publish a fresh replacement KeyPackage",
        alias = "force-publish"
    )]
    Rotate,
    #[command(hide = true)]
    Delete { event_id: String },
    #[command(name = "delete-all", hide = true)]
    DeleteAll {
        #[arg(long)]
        confirm: bool,
    },
    #[command(about = "Check whether a user has relay lists and a fetchable KeyPackage")]
    Check {
        #[arg(value_name = "NPUB_OR_HEX", help = "User to check")]
        pubkey: String,
    },
    #[command(about = "Fetch and cache another user's KeyPackage")]
    Fetch {
        #[arg(
            value_name = "NPUB_OR_HEX",
            help = "User to fetch; defaults to selected account"
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
    command: KeyPackageCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    let runtime = app.runtime();
    with_runtime(account_home, app, &runtime, command, account_flag).await
}

pub(crate) async fn with_runtime(
    account_home: &AccountHome,
    app: &MarmotApp,
    runtime: &MarmotAppRuntime,
    command: KeyPackageCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        KeyPackageCommand::List => {
            let account = resolve_account(account_home, account_flag)?;
            let relay_lists =
                app.account_relay_list_status_for_account_id(&account.account_id_hex)?;
            let fetched = if relay_lists.key_package.relays.is_empty() {
                None
            } else {
                app.fetch_latest_key_package_for_account_id(
                    &account.account_id_hex,
                    relay_endpoints(relay_lists.key_package.relays.clone())?,
                )
                .await
                .ok()
            };
            let keys = fetched
                .into_iter()
                .map(key_package_fetch_json)
                .collect::<Vec<_>>();
            Ok(CommandOutput {
                plain: if keys.is_empty() {
                    "no key packages".to_owned()
                } else {
                    format!("{} key package(s)", keys.len())
                },
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "keys": keys,
                }),
            })
        }
        KeyPackageCommand::Publish => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let key_package_bytes = runtime.publish_key_package(&account.label).await?;
            Ok(CommandOutput {
                plain: format!(
                    "published key package for {} bytes={}",
                    npub_for_account_id(&account.account_id_hex),
                    key_package_bytes
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "key_package_bytes": key_package_bytes,
                }),
            })
        }
        KeyPackageCommand::Rotate => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let key_package_bytes = runtime.rotate_key_package(&account.label).await?;
            Ok(CommandOutput {
                plain: format!(
                    "rotated key package for {} bytes={}",
                    npub_for_account_id(&account.account_id_hex),
                    key_package_bytes
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "key_package_bytes": key_package_bytes,
                    "rotated": true,
                }),
            })
        }
        KeyPackageCommand::Fetch {
            account,
            bootstrap_relays,
        } => {
            let account_id = account_selector_or_default(account_home, account, account_flag)?;
            let fetched = app
                .fetch_latest_key_package_for_account_id(
                    &account_id,
                    relay_endpoints(bootstrap_relays)?,
                )
                .await?;
            Ok(CommandOutput {
                plain: format!(
                    "fetched key package for {account_id} bytes={} relays={}",
                    fetched.key_package.bytes().len(),
                    fetched.source_relays.join(",")
                ),
                json: key_package_fetch_json(fetched),
            })
        }
        KeyPackageCommand::Check { pubkey } => {
            let account_id = parse_public_key(&pubkey)?;
            let fetched = app
                .fetch_latest_key_package_for_account_id(&account_id, Vec::new())
                .await?;
            Ok(CommandOutput {
                plain: format!("key package available for {account_id}"),
                json: json!({
                    "account_id": account_id,
                    "npub": npub_for_account_id(&account_id),
                    "available": true,
                    "key_package": key_package_fetch_json(fetched),
                }),
            })
        }
        KeyPackageCommand::Delete { .. } => unsupported_command(
            "keys delete",
            "relay deletion for KeyPackage events is not implemented yet",
        ),
        KeyPackageCommand::DeleteAll { confirm } => {
            if !confirm {
                return unsupported_command(
                    "keys delete-all",
                    "pass --confirm once relay deletion is implemented",
                );
            }
            unsupported_command(
                "keys delete-all",
                "relay deletion for KeyPackage events is not implemented yet",
            )
        }
    }
}
