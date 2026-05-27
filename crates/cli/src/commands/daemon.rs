//! `dm daemon` subcommand surface. The clap enum lives here so it stays
//! grouped with the other command families; the actual daemon lifecycle and
//! socket protocol live in the top-level `crate::daemon` module, not here.

use clap::Subcommand;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
pub enum DaemonCommand {
    #[command(about = "Start dmd in the background")]
    Start {
        #[arg(
            long,
            value_name = "URLS",
            value_delimiter = ',',
            help = "Comma-separated discovery relays for profiles, relay lists, and KeyPackages"
        )]
        discovery_relays: Vec<String>,
        #[arg(
            long,
            value_name = "URLS",
            value_delimiter = ',',
            help = "Comma-separated default account relays used when creating identities"
        )]
        default_account_relays: Vec<String>,
    },
    #[command(about = "Stop the background dmd daemon")]
    Stop,
    #[command(about = "Show daemon status, relay health, and stream watches")]
    Status,
}
