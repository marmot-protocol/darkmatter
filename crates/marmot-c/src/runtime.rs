//! Internal runtime handle for the C binding.
//!
//! [`MarmotC`] owns the [`MarmotApp`] + [`MarmotAppRuntime`] pair together with
//! a dedicated multi-thread tokio runtime. The C ABI is synchronous, so every
//! async runtime call is driven to completion with [`tokio::runtime::Runtime::block_on`]
//! on this owned runtime. Construction mirrors `marmot_uniffi::Marmot::new`
//! (keychain-backed account home) so the C surface tracks the UniFFI surface.

use marmot_app::{MarmotApp, MarmotAppRuntime};

/// Errors surfaced across the C ABI. The numeric discriminant is what the C
/// status code carries; the message is written to the caller's error out-param.
#[derive(Debug)]
pub enum MarmotCError {
    /// A required pointer argument was null, or a C string was not valid UTF-8.
    InvalidArgument(String),
    /// The requested dispatch method name is not known.
    UnknownMethod(String),
    /// A request/response JSON (de)serialization failed.
    Json(String),
    /// The underlying marmot-app runtime returned an error.
    App(String),
}

impl MarmotCError {
    /// Stable numeric status code carried back over the ABI.
    pub fn code(&self) -> i32 {
        match self {
            // Keep these in sync with the `MARMOT_C_STATUS_*` constants in the
            // generated header and in `include/marmot.h`.
            MarmotCError::InvalidArgument(_) => 1,
            MarmotCError::UnknownMethod(_) => 2,
            MarmotCError::Json(_) => 3,
            MarmotCError::App(_) => 4,
        }
    }
}

impl std::fmt::Display for MarmotCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarmotCError::InvalidArgument(details) => write!(f, "invalid argument: {details}"),
            MarmotCError::UnknownMethod(method) => write!(f, "unknown method: {method}"),
            MarmotCError::Json(details) => write!(f, "json error: {details}"),
            MarmotCError::App(details) => write!(f, "marmot runtime error: {details}"),
        }
    }
}

impl std::error::Error for MarmotCError {}

impl From<marmot_app::AppError> for MarmotCError {
    fn from(value: marmot_app::AppError) -> Self {
        MarmotCError::App(value.to_string())
    }
}

impl From<serde_json::Error> for MarmotCError {
    fn from(value: serde_json::Error) -> Self {
        MarmotCError::Json(value.to_string())
    }
}

/// Opaque handle backing the C ABI. One handle owns one app/runtime pair plus
/// the tokio runtime that drives the pair's async methods.
pub struct MarmotC {
    pub(crate) app: MarmotApp,
    pub(crate) runtime: MarmotAppRuntime,
    pub(crate) tokio: tokio::runtime::Runtime,
}

impl MarmotC {
    /// Open the Marmot app at `root_path`, configured with the given default
    /// relay URLs. Account secrets are stored in the platform keyring via the
    /// default keychain-backed account home (parity with the UniFFI binding).
    ///
    /// Call [`MarmotC::start`] before subscribing to events.
    pub fn open(root_path: &str, relay_urls: Vec<String>) -> Result<Self, MarmotCError> {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| MarmotCError::App(format!("failed to build tokio runtime: {err}")))?;
        let account_home = marmot_account::AccountHome::open_with_default_keychain(root_path)
            .map_err(marmot_app::AppError::from)?;
        let app = MarmotApp::with_relays_and_account_home(root_path, relay_urls, account_home);
        let runtime = app.runtime();
        Ok(Self {
            app,
            runtime,
            tokio,
        })
    }

    /// Bring the runtime online: reconcile known accounts, start workers,
    /// subscribe to transport events.
    pub fn start(&self) -> Result<(), MarmotCError> {
        self.tokio.block_on(self.runtime.start())?;
        Ok(())
    }

    /// Tear the runtime down. Drops all subscriptions.
    pub fn shutdown(&self) {
        self.tokio.block_on(self.runtime.shutdown());
    }

    /// True once shutdown has started.
    pub fn is_stopping(&self) -> bool {
        self.runtime.is_stopping()
    }
}
