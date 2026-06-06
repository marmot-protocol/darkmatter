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

/// Opt-in configuration for relay-telemetry export.
///
/// Off by default. While `enabled` is `false` nothing is resolved or exported
/// and the app behaves exactly as today. This is the single opt-in switch that
/// gates relay-identity resolution and, later, the OTLP exporter — see the
/// privacy contract in `docs/marmot-architecture/relay-observability.md`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayTelemetryExportConfig {
    /// Whether the user has opted in to relay-telemetry export. Off by default.
    pub enabled: bool,
}

impl RelayTelemetryExportConfig {
    /// A disabled (opt-out) configuration. This is also [`Default`].
    pub fn disabled() -> Self {
        Self { enabled: false }
    }
}
