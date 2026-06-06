use std::time::Duration;

const DEFAULT_DIRECTORY_MAX_FUTURE_SKEW: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarmotAppConfig {
    pub directory_max_future_skew: Duration,
}

impl Default for MarmotAppConfig {
    fn default() -> Self {
        Self {
            directory_max_future_skew: DEFAULT_DIRECTORY_MAX_FUTURE_SKEW,
        }
    }
}

impl MarmotAppConfig {
    pub fn with_directory_max_future_skew(mut self, skew: Duration) -> Self {
        self.directory_max_future_skew = skew;
        self
    }
}

/// Default export poll/push interval. Coarse on purpose: the data is
/// aggregate and cumulative, so a coarse window respects battery and metered
/// networks without losing resolution.
const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Opt-in configuration for relay-telemetry export.
///
/// Off by default. While `enabled` is `false` nothing is resolved or exported
/// and the app behaves exactly as today. This is the single opt-in switch that
/// gates relay-identity resolution and the OTLP exporter — see the privacy
/// contract in `docs/marmot-architecture/relay-observability.md`.
///
/// The `endpoint` must be a first-party Marmot-operated OTLP/HTTP collector
/// reached over TLS; the exporter POSTs to `{endpoint}/v1/metrics`. Export is
/// inert without one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayTelemetryExportConfig {
    /// Whether the user has opted in to relay-telemetry export. Off by default.
    pub enabled: bool,
    /// First-party OTLP/HTTP collector base URL (TLS). `None` keeps export
    /// inert even when `enabled`.
    pub endpoint: Option<String>,
    /// How often to poll the rollup and push.
    pub interval: Duration,
}

impl Default for RelayTelemetryExportConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

impl RelayTelemetryExportConfig {
    /// A disabled (opt-out) configuration. This is also [`Default`].
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            interval: DEFAULT_EXPORT_INTERVAL,
        }
    }

    /// An opted-in configuration pushing to `endpoint` at the default interval.
    pub fn enabled(endpoint: impl Into<String>) -> Self {
        Self {
            enabled: true,
            endpoint: Some(endpoint.into()),
            interval: DEFAULT_EXPORT_INTERVAL,
        }
    }

    /// Override the poll/push interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}
