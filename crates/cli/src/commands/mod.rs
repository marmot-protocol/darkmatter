//! Command handlers for the `dm` CLI surface.
//!
//! Each Whitenoise-shaped command family lives in its own submodule and
//! exposes the clap `Subcommand` enum plus a single `run` entry point that
//! `crate::execute_inner` dispatches to. New command families should follow
//! the same pattern so `lib.rs` stays focused on top-level dispatch.

pub mod debug;
pub mod follows;
pub mod notifications;
pub mod profile;
pub mod relays;
pub mod settings;
pub mod users;
