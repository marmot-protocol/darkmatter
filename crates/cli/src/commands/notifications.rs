//! Notification commands. Placeholder surface today; the daemon does not yet
//! derive or deliver notifications, so all subcommands return
//! `unsupported_command` per the contract documented in `crates/cli/CLAUDE.md`.

use clap::Subcommand;
use serde::{Deserialize, Serialize};

use crate::{CommandOutput, DmError, unsupported_command};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum NotificationsCommand {
    #[command(about = "Subscribe to notification updates")]
    Subscribe,
}

pub(crate) fn run(command: NotificationsCommand) -> Result<CommandOutput, DmError> {
    match command {
        NotificationsCommand::Subscribe => unsupported_command(
            "notifications subscribe",
            "notification derivation and delivery are not exposed by the daemon yet",
        ),
    }
}
