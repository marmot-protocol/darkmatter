//! Swift / UniFFI bindings for the Marmot app runtime.
//!
//! This crate is a thin FFI adapter over [`marmot_app::MarmotApp`] and
//! [`marmot_app::MarmotAppRuntime`]. It is consumed by `darkmatter-ios` (and
//! anything else that wants a UniFFI-shaped surface) via the generated Swift
//! package and the accompanying `MarmotKit.xcframework`.
//!
//! Design notes:
//! - One process-wide [`Marmot`] handle owns the [`MarmotApp`] + runtime pair.
//! - All async methods rely on UniFFI's tokio integration.
//! - Internal Rust types that don't map cleanly across the FFI boundary are
//!   re-exposed as FFI-friendly records (e.g. byte ids → hex strings).

use std::sync::Arc;

use marmot_app::{MarmotApp, MarmotAppRuntime};

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MarmotKitError {
    #[error("marmot runtime error: {message}")]
    Runtime { message: String },
}

impl From<marmot_app::AppError> for MarmotKitError {
    fn from(value: marmot_app::AppError) -> Self {
        Self::Runtime {
            message: value.to_string(),
        }
    }
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct AccountSummaryFfi {
    pub label: String,
    pub account_id_hex: String,
    pub local_signing: bool,
    pub running: bool,
}

#[derive(uniffi::Object)]
pub struct Marmot {
    #[allow(dead_code)]
    app: MarmotApp,
    runtime: MarmotAppRuntime,
}

#[uniffi::export(async_runtime = "tokio")]
impl Marmot {
    #[uniffi::constructor]
    pub fn new(root_path: String, relay_urls: Vec<String>) -> Arc<Self> {
        let app = MarmotApp::with_relays(&root_path, relay_urls);
        let runtime = app.runtime();
        Arc::new(Self { app, runtime })
    }

    pub fn list_accounts(&self) -> Result<Vec<AccountSummaryFfi>, MarmotKitError> {
        let managed = self.runtime.accounts().managed_accounts()?;
        Ok(managed
            .into_iter()
            .map(|m| AccountSummaryFfi {
                label: m.label,
                account_id_hex: m.account_id_hex,
                local_signing: m.local_signing,
                running: m.running,
            })
            .collect())
    }

    pub async fn start(&self) -> Result<(), MarmotKitError> {
        self.runtime.start().await?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.runtime.shutdown().await;
    }
}
