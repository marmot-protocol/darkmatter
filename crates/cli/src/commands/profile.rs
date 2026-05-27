//! Account Nostr profile (`kind:0`) inspection and updates.

use cgka_traits::TransportEndpoint;
use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::{AccountRelayListBootstrap, MarmotApp, MarmotAppRuntime, UserProfileMetadata};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    CommandOutput, DmError, ensure_local_signing, npub_for_account_id, resolve_account,
    unix_now_seconds, validate_relay_url,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum ProfileCommand {
    #[command(about = "Show the selected account Nostr profile")]
    Show,
    #[command(about = "Update and publish the selected account Nostr profile")]
    Update {
        #[arg(long, help = "Set the short profile name")]
        name: Option<String>,
        #[arg(long, help = "Set the display name")]
        display_name: Option<String>,
        #[arg(long, help = "Set the profile bio")]
        about: Option<String>,
        #[arg(long, help = "Set the profile picture URL")]
        picture: Option<String>,
        #[arg(long, help = "Set the NIP-05 identifier")]
        nip05: Option<String>,
        #[arg(long, help = "Set the Lightning address")]
        lud16: Option<String>,
    },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: ProfileCommand,
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
    command: ProfileCommand,
    account_flag: Option<String>,
    relay: Option<String>,
) -> Result<CommandOutput, DmError> {
    let account = resolve_account(account_home, account_flag)?;
    ensure_local_signing(&account)?;
    match command {
        ProfileCommand::Show => {
            let entry = app.directory_entry_for_account_id(&account.account_id_hex)?;
            Ok(CommandOutput {
                plain: serde_json::to_string_pretty(&entry)
                    .expect("JSON response serialization cannot fail"),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "profile": entry.and_then(|entry| entry.profile),
                }),
            })
        }
        ProfileCommand::Update {
            name,
            display_name,
            about,
            picture,
            nip05,
            lud16,
        } => {
            let relay = relay.ok_or(DmError::MissingRelay)?;
            let endpoint = TransportEndpoint(validate_relay_url(&relay)?);
            let profile = UserProfileMetadata {
                name,
                display_name,
                about,
                picture,
                nip05,
                lud16,
                created_at: unix_now_seconds(),
                source_relays: Vec::new(),
            };
            runtime
                .publish_user_profile(
                    &account.label,
                    profile.clone(),
                    AccountRelayListBootstrap::new(vec![endpoint.clone()], vec![endpoint]),
                )
                .await?;
            Ok(CommandOutput {
                plain: format!(
                    "updated profile {}",
                    npub_for_account_id(&account.account_id_hex)
                ),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "profile": profile,
                }),
            })
        }
    }
}
