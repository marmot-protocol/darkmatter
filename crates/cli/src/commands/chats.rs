//! Chat list, archive, and subscribe surface.

use clap::Subcommand;
use marmot_account::AccountHome;
use marmot_app::MarmotApp;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    CommandOutput, DmError, ensure_local_signing, group_archive_output, group_json,
    group_list_plain, group_show_output, npub_for_account_id, resolve_account, unsupported_command,
};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum ChatsCommand {
    #[command(about = "List current chats")]
    List {
        #[arg(long, help = "Include archived chats")]
        include_archived: bool,
    },
    #[command(about = "Show one chat")]
    Show {
        #[arg(help = "Group id to show")]
        group: String,
    },
    #[command(about = "Subscribe to live chat-list updates through the daemon")]
    Subscribe,
    #[command(about = "Archive a chat locally")]
    Archive {
        #[arg(help = "Group id to archive")]
        group: String,
    },
    #[command(about = "Unarchive a chat locally")]
    Unarchive {
        #[arg(help = "Group id to unarchive")]
        group: String,
    },
    #[command(name = "list-archived", about = "List archived chats")]
    ListArchived,
    #[command(
        name = "subscribe-archived",
        about = "Subscribe to live archived-chat updates through the daemon"
    )]
    SubscribeArchived,
    #[command(hide = true)]
    Mute { group: String, duration: String },
    #[command(hide = true)]
    Unmute { group: String },
}

pub(crate) async fn run(
    account_home: &AccountHome,
    app: &MarmotApp,
    command: ChatsCommand,
    account_flag: Option<String>,
) -> Result<CommandOutput, DmError> {
    match command {
        ChatsCommand::List { include_archived } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let chats = if include_archived {
                app.groups(&account.label)?
            } else {
                app.visible_groups(&account.label)?
            };
            Ok(CommandOutput {
                plain: group_list_plain(&chats),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "include_archived": include_archived,
                    "chats": chats.into_iter().map(group_json).collect::<Vec<_>>(),
                }),
            })
        }
        ChatsCommand::Show { group } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            group_show_output(app, account, group, None)
        }
        ChatsCommand::Subscribe => Err(DmError::MessagesSubscribeRequiresDaemon),
        ChatsCommand::Archive { group } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            group_archive_output(app, account, group, true)
        }
        ChatsCommand::Unarchive { group } => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            group_archive_output(app, account, group, false)
        }
        ChatsCommand::ListArchived => {
            let account = resolve_account(account_home, account_flag)?;
            ensure_local_signing(&account)?;
            app.status(&account.label)?;
            let chats = app
                .groups(&account.label)?
                .into_iter()
                .filter(|group| group.archived)
                .collect::<Vec<_>>();
            Ok(CommandOutput {
                plain: group_list_plain(&chats),
                json: json!({
                    "account_id": account.account_id_hex,
                    "npub": npub_for_account_id(&account.account_id_hex),
                    "chats": chats.into_iter().map(group_json).collect::<Vec<_>>(),
                }),
            })
        }
        ChatsCommand::SubscribeArchived => Err(DmError::MessagesSubscribeRequiresDaemon),
        ChatsCommand::Mute { .. } => unsupported_command(
            "chats mute",
            "chat notification mute state is not modeled in marmot-app yet",
        ),
        ChatsCommand::Unmute { .. } => unsupported_command(
            "chats unmute",
            "chat notification mute state is not modeled in marmot-app yet",
        ),
    }
}
