use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cgka_traits::transport::Timestamp;
use cgka_traits::{
    MemberId, TransportAccountActivation, TransportAdapter, TransportAdapterError,
    TransportDelivery, TransportEndpoint, TransportGroupSync, TransportPublishReport,
    TransportPublishRequest, TransportPublishTarget,
};
use nostr_sdk::prelude::{
    Client as NostrSdkClient, Filter, Kind, PublicKey, RelayMessage, RelayPoolNotification,
    RelayUrl, SubscriptionId, Timestamp as NostrTimestamp,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use transport_nostr_adapter::{
    DurationHistogramSnapshot, NostrAdapterMetrics, NostrPublishOutcome, NostrRelayClient,
    NostrSdkRelayClient, NostrSdkRelayHealth, NostrTransportAdapter, RelayDeliverySpread,
    RelayExportConsent, RelayLabelResolution, RelaySyncSnapshot,
};

use crate::config::RelayTelemetryExportConfig;
use transport_nostr_peeler::NostrTransportEvent;

use crate::directory::DirectorySyncPlan;

const ACCOUNT_DELIVERY_BUFFER: usize = 1024;
const DIRECTORY_EVENT_BUFFER: usize = 1024;
const MAX_RELAY_ENDPOINTS_PER_ROUTE: usize = 16;
const DIRECTORY_RELAY_CONNECT_WAIT: Duration = Duration::from_secs(5);
const DIRECTORY_RELAY_FETCH_WAIT: Duration = Duration::from_secs(3);
const RELAY_PLANE_SHUTDOWN_WAIT: Duration = Duration::from_secs(2);
const RELAY_PLANE_TASK_ABORT_WAIT: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct MarmotRelayPlane {
    inner: Arc<MarmotRelayPlaneInner>,
}

struct MarmotRelayPlaneInner {
    subscription_rebuild_lookback: Option<Duration>,
    relay_safety: RelaySafetyPolicy,
    transport: Arc<RelayPlaneTransport>,
    directory: DirectoryRelayPlane,
}

#[derive(Clone, Debug)]
struct RelaySafetyPolicy {
    max_endpoints_per_route: usize,
}

impl Default for RelaySafetyPolicy {
    fn default() -> Self {
        Self {
            max_endpoints_per_route: MAX_RELAY_ENDPOINTS_PER_ROUTE,
        }
    }
}

struct RelayPlaneTransport {
    adapter: NostrTransportAdapter,
    sdk_relay_client: Option<NostrSdkRelayClient>,
    directory_events: broadcast::Sender<DirectoryRelayEventRecord>,
    account_deliveries: RwLock<HashMap<MemberId, mpsc::Sender<TransportDelivery>>>,
    router: Mutex<Option<JoinHandle<()>>>,
    notification_forwarder: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct MarmotRelayPlaneAccountAdapter {
    account_id: MemberId,
    relay_plane: MarmotRelayPlane,
    publish_client: Arc<dyn NostrRelayClient>,
    delivery_rx: Arc<Mutex<mpsc::Receiver<TransportDelivery>>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayPlaneHealth {
    pub sdk_backed: bool,
    pub total_relays: usize,
    pub initialized: usize,
    pub pending: usize,
    pub connecting: usize,
    pub connected: usize,
    pub disconnected: usize,
    pub terminated: usize,
    pub banned: usize,
    pub sleeping: usize,
    pub connection_attempts: usize,
    pub connection_successes: usize,
    pub directory_inflight_fetches: usize,
    pub directory_active_subscriptions: usize,
    pub directory_completed_fetches: usize,
    pub directory_coalesced_waiters: usize,
    pub directory_failed_fetches: usize,
    pub directory_completed_subscription_syncs: usize,
    pub directory_subscriptions_created: usize,
    pub directory_subscriptions_removed: usize,
}

/// Device-local relay telemetry bundled for local inspection.
///
/// This is the read model behind `dm relay-stats`: it surfaces the adapter's
/// existing aggregate, privacy-safe snapshots (lifecycle counters, cross-relay
/// arrival spread, subscription sync timing) alongside redacted relay health.
///
/// Per-relay attribution stays behind opaque [`transport_nostr_adapter::RelayIndex`]
/// values here — resolving an index to a relay URL is reserved for the opt-in
/// export boundary, never for this local read path.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RelayTelemetrySnapshot {
    /// Adapter lifecycle counters (accounts, subscriptions, inbound, publish).
    pub metrics: NostrAdapterMetrics,
    /// Cross-relay arrival spread and per-relay first-deliverer attribution.
    pub delivery_spread: RelayDeliverySpread,
    /// First-event / EOSE subscription sync timing, aggregate and per relay.
    pub sync: RelaySyncSnapshot,
    /// Redacted relay-pool and directory health.
    pub health: RelayPlaneHealth,
}

/// Export-ready rollup of device-local relay telemetry.
///
/// This is the aggregation home for the export path. There is a single shared
/// adapter per device, so the per-relay series are already merged across every
/// local account; this rollup reorganizes them into the export shape and is the
/// one place additional per-account dedup would live if telemetry ever became
/// per-account. It stays keyed by the opaque [`transport_nostr_adapter::RelayIndex`];
/// resolving an index to a relay URL is the exporter's job, behind opt-in.
///
/// Privacy-safe: counts, fixed-bucket millisecond histograms, and opaque relay
/// indices only — no account, group, subscription, or URL fields.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RelayTelemetryRollup {
    /// Per-relay export records, ascending by opaque relay index.
    pub relays: Vec<RelayRollupEntry>,
    /// Population-level cross-relay arrival spread (inherently no relay label).
    pub cross_relay_spread: DurationHistogramSnapshot,
    /// Distinct logical messages observed within the tracking window.
    pub messages_observed: u64,
    /// Messages corroborated by at least a second distinct relay.
    pub messages_corroborated: u64,
    /// Messages seen on exactly one relay within the window.
    pub messages_single_source: u64,
    /// Device-wide relay connection attempts (for connection success rate).
    pub connection_attempts: u64,
    /// Device-wide successful relay connections.
    pub connection_successes: u64,
    /// Device-wide publish attempts (aggregate; per-relay/per-kind publish
    /// attribution is a future adapter enhancement, see `relay-observability.md`).
    pub publish_attempts: u64,
    /// Device-wide accepted publishes.
    pub publish_successes: u64,
    /// Device-wide failed publishes.
    pub publish_failures: u64,
    /// Optional engine-side reorg metrics, folded in once the parallel
    /// `observed_reorg_rate` workstream lands. `None` until then.
    pub engine: Option<EngineReorgMetrics>,
}

impl RelayTelemetryRollup {
    /// Derived `observed_reorg_rate = post_settle_reorgs / settles` from the
    /// folded-in engine metrics, if present and non-empty.
    pub fn observed_reorg_rate(&self) -> Option<f64> {
        let engine = self.engine.as_ref()?;
        (engine.settles > 0).then(|| engine.post_settle_reorgs as f64 / engine.settles as f64)
    }
}

/// One relay's export-ready record, keyed by opaque device-local index.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RelayRollupEntry {
    /// Opaque device-local relay index (resolved to a URL only at export).
    pub relay_index: u32,
    /// First-event latency from subscribe time, in local-time milliseconds.
    pub first_event_latency: DurationHistogramSnapshot,
    /// EOSE latency from subscribe time, in local-time milliseconds.
    pub eose_latency: DurationHistogramSnapshot,
    /// Copies this relay surfaced first (delivery + first-deliverer signal).
    pub delivered_first: u64,
    /// Copies this relay corroborated after another relay surfaced first.
    pub delivered_later: u64,
}

impl RelayRollupEntry {
    /// Total copies this relay delivered (`relay_delivery_count`).
    pub fn delivery_count(&self) -> u64 {
        self.delivered_first + self.delivered_later
    }

    /// Copies that corroborated a message another relay surfaced first
    /// (`relay_redundant_count`).
    pub fn redundant_count(&self) -> u64 {
        self.delivered_later
    }

    /// Fraction of this relay's copies that arrived first, in `0.0..=1.0`.
    /// `None` when the relay has delivered nothing.
    pub fn first_deliverer_rate(&self) -> Option<f64> {
        let total = self.delivery_count();
        (total > 0).then(|| self.delivered_first as f64 / total as f64)
    }
}

/// Engine-side relay-tuning metrics folded into the export rollup.
///
/// Owned by the engine (the parallel `observed_reorg_rate` workstream), not the
/// adapter. This is the seam: [`MarmotRelayPlane::telemetry_rollup`] accepts it
/// as an optional input and the exporter ships it over the same OTLP path.
/// `None` until the engine metric lands. Shapes mirror `relay-delivery-telemetry.md`
/// "Validation: post-settle reorg rate"; the engine session may extend it
/// (for example with `reorg_rewind_depth`) without disturbing the seam.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EngineReorgMetrics {
    /// Settle episodes, summed across groups (denominator).
    pub settles: u64,
    /// Settles later superseded by a diverging branch (numerator).
    pub post_settle_reorgs: u64,
    /// Local time from a superseded settle to the reorg, in milliseconds — the
    /// extra quiescence that would have avoided each reorg.
    pub reorg_lateness_ms: DurationHistogramSnapshot,
}

/// Reshape the adapter snapshots into the export-ready rollup. Pure so the
/// aggregation is unit-testable without a live relay plane.
fn rollup_from_snapshots(
    spread: RelayDeliverySpread,
    sync: RelaySyncSnapshot,
    metrics: NostrAdapterMetrics,
    health: RelayPlaneHealth,
    engine: Option<EngineReorgMetrics>,
) -> RelayTelemetryRollup {
    let mut indices: Vec<u32> = spread
        .per_relay
        .iter()
        .map(|stats| stats.relay_index)
        .chain(sync.per_relay.iter().map(|stats| stats.relay_index))
        .collect();
    indices.sort_unstable();
    indices.dedup();

    let relays = indices
        .into_iter()
        .map(|relay_index| {
            let delivery = spread
                .per_relay
                .iter()
                .find(|stats| stats.relay_index == relay_index);
            let latency = sync
                .per_relay
                .iter()
                .find(|stats| stats.relay_index == relay_index);
            RelayRollupEntry {
                relay_index,
                first_event_latency: latency
                    .map(|stats| stats.first_event.clone())
                    .unwrap_or_default(),
                eose_latency: latency.map(|stats| stats.eose.clone()).unwrap_or_default(),
                delivered_first: delivery
                    .map(|stats| stats.delivered_first)
                    .unwrap_or_default(),
                delivered_later: delivery
                    .map(|stats| stats.delivered_later)
                    .unwrap_or_default(),
            }
        })
        .collect();

    RelayTelemetryRollup {
        relays,
        cross_relay_spread: spread.spread,
        messages_observed: spread.observed,
        messages_corroborated: spread.corroborated,
        messages_single_source: spread.single_source,
        connection_attempts: health.connection_attempts as u64,
        connection_successes: health.connection_successes as u64,
        publish_attempts: metrics.publish_attempts as u64,
        publish_successes: metrics.publish_successes as u64,
        publish_failures: metrics.publish_failures as u64,
        engine,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DirectoryEventQuery {
    pub(crate) kind: u64,
    pub(crate) authors: Vec<String>,
    pub(crate) limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DirectoryRelayEventRecord {
    pub(crate) endpoints: Vec<TransportEndpoint>,
    pub(crate) event: NostrTransportEvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DirectoryFetchRequest {
    endpoints: Vec<TransportEndpoint>,
    queries: Vec<DirectoryEventQuery>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DirectoryFetchKey {
    endpoints: Vec<TransportEndpoint>,
    queries: Vec<DirectoryEventQuery>,
}

#[derive(Clone)]
struct DirectoryRelayPlane {
    fetcher: Arc<dyn DirectoryRelayFetcher>,
    state: Arc<Mutex<DirectoryRelayPlaneState>>,
}

#[derive(Default)]
struct DirectoryRelayPlaneState {
    inflight: HashMap<DirectoryFetchKey, Vec<oneshot::Sender<DirectoryFetchResult>>>,
    active_subscription_ids: HashSet<String>,
    completed_fetches: usize,
    coalesced_waiters: usize,
    failed_fetches: usize,
    completed_subscription_syncs: usize,
    subscriptions_created: usize,
    subscriptions_removed: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DirectoryRelayStats {
    inflight_fetches: usize,
    active_subscriptions: usize,
    completed_fetches: usize,
    coalesced_waiters: usize,
    failed_fetches: usize,
    completed_subscription_syncs: usize,
    subscriptions_created: usize,
    subscriptions_removed: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirectorySubscriptionSyncSummary {
    pub(crate) active_subscriptions: usize,
    pub(crate) subscriptions_created: usize,
    pub(crate) subscriptions_removed: usize,
}

type DirectoryFetchResult = Result<Vec<DirectoryRelayEventRecord>, String>;

#[async_trait]
trait DirectoryRelayFetcher: Send + Sync {
    async fn fetch_directory_events(
        &self,
        request: DirectoryFetchRequest,
    ) -> Result<Vec<DirectoryRelayEventRecord>, String>;
}

#[derive(Clone)]
struct NostrSdkDirectoryRelayFetcher {
    client: NostrSdkClient,
}

impl MarmotRelayPlane {
    pub fn runtime_default(subscription_rebuild_lookback: Duration) -> Self {
        Self::from_sdk(Some(subscription_rebuild_lookback))
    }

    pub fn full_history() -> Self {
        Self::from_sdk(None)
    }

    pub fn with_subscription_rebuild_lookback(lookback: Duration) -> Self {
        Self::from_sdk(Some(lookback))
    }

    pub fn new(
        subscription_rebuild_lookback: Option<Duration>,
        relay_client: Arc<dyn NostrRelayClient>,
    ) -> Self {
        let adapter = NostrTransportAdapter::new(relay_client);
        Self::from_adapter(
            subscription_rebuild_lookback,
            adapter,
            None,
            None,
            Arc::new(NostrSdkDirectoryRelayFetcher::standalone()),
        )
    }

    fn from_sdk(subscription_rebuild_lookback: Option<Duration>) -> Self {
        let client = NostrSdkClient::builder().build();
        let relay_client = NostrSdkRelayClient::new(client.clone());
        let adapter = NostrTransportAdapter::new(Arc::new(relay_client.clone()));
        Self::from_adapter(
            subscription_rebuild_lookback,
            adapter,
            Some(relay_client),
            None,
            Arc::new(NostrSdkDirectoryRelayFetcher::new(client)),
        )
    }

    fn from_adapter(
        subscription_rebuild_lookback: Option<Duration>,
        adapter: NostrTransportAdapter,
        sdk_relay_client: Option<NostrSdkRelayClient>,
        notification_forwarder: Option<JoinHandle<()>>,
        directory_fetcher: Arc<dyn DirectoryRelayFetcher>,
    ) -> Self {
        let transport = Arc::new(RelayPlaneTransport {
            adapter,
            sdk_relay_client,
            directory_events: broadcast::channel(DIRECTORY_EVENT_BUFFER).0,
            account_deliveries: RwLock::new(HashMap::new()),
            router: Mutex::new(None),
            notification_forwarder: Mutex::new(notification_forwarder),
        });
        let this = Self {
            inner: Arc::new(MarmotRelayPlaneInner {
                subscription_rebuild_lookback,
                relay_safety: RelaySafetyPolicy::default(),
                transport,
                directory: DirectoryRelayPlane::new(directory_fetcher),
            }),
        };
        this.spawn_router();
        this
    }

    pub fn account_adapter(
        &self,
        account_id: MemberId,
        publish_client: Arc<dyn NostrRelayClient>,
    ) -> MarmotRelayPlaneAccountAdapter {
        self.spawn_router();
        let (delivery_tx, delivery_rx) = mpsc::channel(ACCOUNT_DELIVERY_BUFFER);
        account_deliveries_write(&self.inner.transport.account_deliveries)
            .insert(account_id.clone(), delivery_tx);
        MarmotRelayPlaneAccountAdapter {
            account_id,
            relay_plane: self.clone(),
            publish_client,
            delivery_rx: Arc::new(Mutex::new(delivery_rx)),
        }
    }

    pub fn subscription_rebuild_since(
        &self,
        last_transport_timestamp: Option<u64>,
    ) -> Option<Timestamp> {
        let lookback = self.inner.subscription_rebuild_lookback?;
        let last_transport_timestamp = last_transport_timestamp?;
        // The persisted cursor is advanced from the sender-controlled inbound
        // `created_at`; a far-future value would push `since` past the present,
        // so relays return no present-dated events and reception silently halts
        // forever (the cursor is persisted and monotonic, so it survives
        // restarts — darkmatter#182).
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // A cursor detectably in the future is corrupted, not authoritative.
        // Merely clamping it to wall-clock would yield `since = now - lookback`
        // and permanently skip any valid backlog older than the (short,
        // production-default 120s) lookback for an account whose cursor was
        // poisoned before the write-side clamp existed. Treat it as untrusted
        // and request a full-history replay (`None`) so the catch-up range is
        // never silently dropped; the write side then heals the stored value
        // back below wall-clock. A cursor at or behind wall-clock is trusted
        // and used as-is.
        if last_transport_timestamp > now {
            return None;
        }
        Some(Timestamp(
            last_transport_timestamp.saturating_sub(lookback.as_secs()),
        ))
    }

    /// Attach an account's signing keys to the shared transport client so it
    /// can answer NIP-42 AUTH challenges. Auth-gated relays withhold
    /// gift-wrapped welcomes from unauthenticated subscribers without
    /// surfacing an error — the events are simply absent — so an inbox
    /// subscription issued before a signer is set never sees the invites
    /// those relays hold. The SDK client (and the directory fetcher sharing
    /// it) is one per plane: with multiple accounts the most recently opened
    /// account's keys win, which matches the one-account-per-process apps.
    /// No-op for planes built on a custom relay client.
    pub async fn set_transport_signer(&self, keys: nostr::Keys) {
        if let Some(sdk_relay_client) = &self.inner.transport.sdk_relay_client {
            sdk_relay_client.client().set_signer(keys).await;
        }
    }

    pub async fn relay_health(&self) -> RelayPlaneHealth {
        let directory = self.inner.directory.stats().await;
        if let Some(sdk_relay_client) = &self.inner.transport.sdk_relay_client {
            return RelayPlaneHealth::from_sdk(sdk_relay_client.relay_health().await, directory);
        }
        RelayPlaneHealth::from_directory(directory)
    }

    /// Snapshot the device-local relay telemetry for local inspection.
    ///
    /// Aggregate and privacy-safe: counts, millisecond histogram buckets, and
    /// opaque relay indices only. There is a single shared adapter per device,
    /// so these counters already span every local account. Resolving the opaque
    /// indices to relay URLs is reserved for the opt-in export path.
    pub async fn relay_telemetry(&self) -> RelayTelemetrySnapshot {
        let adapter = &self.inner.transport.adapter;
        RelayTelemetrySnapshot {
            metrics: adapter.metrics().await,
            delivery_spread: adapter.delivery_spread().await,
            sync: adapter.relay_sync().await,
            health: self.relay_health().await,
        }
    }

    /// Resolve opaque relay indices to relay endpoints — the export label
    /// boundary.
    ///
    /// Crate-private and reachable only through the exporter. It returns `None`
    /// unless [`RelayTelemetryExportConfig::export_allowed`] holds (the same
    /// gate as [`MarmotRelayPlane::telemetry_exporter`]); only then does it mint
    /// a [`RelayExportConsent`] and ask the adapter to reverse-map indices to
    /// relay URLs. No other code path turns a device-local index into a relay
    /// URL. See the privacy contract in `relay-observability.md`.
    pub(crate) async fn resolve_relay_labels(
        &self,
        config: &RelayTelemetryExportConfig,
    ) -> Option<RelayLabelResolution> {
        // Same gate as `telemetry_exporter`: resolution cannot happen unless
        // export is opted in with a TLS/loopback endpoint, auth, and resource
        // metadata.
        if !config.export_allowed() {
            return None;
        }
        let consent = RelayExportConsent::affirm();
        Some(
            self.inner
                .transport
                .adapter
                .resolve_relay_labels(consent)
                .await,
        )
    }

    /// Aggregate the device-local per-relay telemetry into one export-ready
    /// rollup, optionally folding in engine-side reorg metrics.
    ///
    /// Keyed by opaque relay index — no relay URLs. The single shared adapter
    /// already merges across local accounts, so today this is a near-passthrough
    /// reshaping; it is the seam where multi-account dedup and engine metrics are
    /// combined for export. `engine` is `None` until the parallel
    /// `observed_reorg_rate` workstream lands.
    pub async fn telemetry_rollup(
        &self,
        engine: Option<EngineReorgMetrics>,
    ) -> RelayTelemetryRollup {
        let adapter = &self.inner.transport.adapter;
        let spread = adapter.delivery_spread().await;
        let sync = adapter.relay_sync().await;
        let metrics = adapter.metrics().await;
        let health = self.relay_health().await;
        rollup_from_snapshots(spread, sync, metrics, health, engine)
    }

    pub(crate) async fn fetch_directory_events(
        &self,
        endpoints: Vec<TransportEndpoint>,
        queries: Vec<DirectoryEventQuery>,
    ) -> Result<Vec<DirectoryRelayEventRecord>, String> {
        let endpoints = self
            .inner
            .relay_safety
            .sanitize_endpoints(endpoints, "directory fetch")?;
        self.inner
            .directory
            .fetch_events(DirectoryFetchRequest::new(endpoints, queries)?)
            .await
    }

    pub(crate) fn subscribe_directory_events(
        &self,
    ) -> broadcast::Receiver<DirectoryRelayEventRecord> {
        self.inner.transport.directory_events.subscribe()
    }

    pub(crate) async fn sync_directory_user_subscriptions(
        &self,
        plan: DirectorySyncPlan,
    ) -> Result<DirectorySubscriptionSyncSummary, String> {
        self.spawn_router();
        let endpoints = self
            .inner
            .relay_safety
            .sanitize_endpoints(plan.endpoints, "directory subscription")?;
        if plan.batches.is_empty() || endpoints.is_empty() {
            return self
                .inner
                .directory
                .replace_subscription_ids(HashSet::new())
                .await;
        }
        let sdk_relay_client = self
            .inner
            .transport
            .sdk_relay_client
            .as_ref()
            .ok_or_else(|| "directory subscription requires SDK relay plane".to_owned())?;
        let relay_urls = endpoints
            .iter()
            .map(|endpoint| {
                RelayUrl::parse(endpoint.as_str())
                    .map_err(|err| format!("directory subscription: invalid relay endpoint: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for relay_url in &relay_urls {
            sdk_relay_client
                .client()
                .add_relay(relay_url.clone())
                .await
                .map_err(|err| format!("directory subscription add relay: {err}"))?;
            timeout(
                DIRECTORY_RELAY_CONNECT_WAIT,
                sdk_relay_client.client().connect_relay(relay_url.clone()),
            )
            .await
            .map_err(|_| "directory subscription connect relay timed out".to_owned())?
            .map_err(|err| format!("directory subscription connect relay: {err}"))?;
        }

        let desired_ids = plan
            .batches
            .iter()
            .map(|batch| batch.subscription_id.clone())
            .collect::<HashSet<_>>();
        let (to_add, to_remove) = self.inner.directory.subscription_diff(&desired_ids).await;
        for subscription_id in &to_remove {
            sdk_relay_client
                .client()
                .unsubscribe(&SubscriptionId::new(subscription_id.clone()))
                .await;
        }
        for batch in &plan.batches {
            if !to_add.contains(&batch.subscription_id) {
                continue;
            }
            let authors = batch
                .authors
                .iter()
                .map(|author| PublicKey::parse(author).map_err(|_| "invalid directory author"))
                .collect::<Result<Vec<_>, _>>()?;
            let kinds = batch
                .kinds
                .iter()
                .map(|kind| {
                    u16::try_from(*kind)
                        .map(Kind::from)
                        .map_err(|_| format!("unsupported Nostr kind {kind}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut filter = Filter::new()
                .authors(authors)
                .kinds(kinds)
                .limit(batch.authors.len().saturating_mul(batch.kinds.len()).max(1));
            if let Some(since) = batch.since {
                filter = filter.since(NostrTimestamp::from_secs(since));
            }
            sdk_relay_client
                .client()
                .subscribe_with_id_to(
                    relay_urls.clone(),
                    SubscriptionId::new(batch.subscription_id.clone()),
                    filter,
                    None,
                )
                .await
                .map_err(|err| format!("directory subscription subscribe: {err}"))?;
        }

        self.inner
            .directory
            .replace_subscription_ids(desired_ids)
            .await
    }

    pub async fn shutdown(&self) {
        if let Some(sdk_relay_client) = &self.inner.transport.sdk_relay_client {
            let timed_out = timeout(
                RELAY_PLANE_SHUTDOWN_WAIT,
                sdk_relay_client.client().shutdown(),
            )
            .await
            .is_err();
            if timed_out {
                tracing::warn!(
                    target: "marmot_app::relay_plane",
                    method = "shutdown",
                    "SDK relay pool shutdown timed out",
                );
            }
        }
        account_deliveries_write(&self.inner.transport.account_deliveries).clear();
        if let Some(handle) = self.inner.transport.router.lock().await.take() {
            let mut handle = handle;
            handle.abort();
            let _ = timeout(RELAY_PLANE_TASK_ABORT_WAIT, &mut handle).await;
        }
        if let Some(handle) = self
            .inner
            .transport
            .notification_forwarder
            .lock()
            .await
            .take()
        {
            let mut handle = handle;
            handle.abort();
            let _ = timeout(RELAY_PLANE_TASK_ABORT_WAIT, &mut handle).await;
        }
    }

    fn spawn_router(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let Ok(mut router) = self.inner.transport.router.try_lock() else {
            return;
        };
        if router.is_some() {
            return;
        }
        if let Ok(mut notification_forwarder) =
            self.inner.transport.notification_forwarder.try_lock()
            && notification_forwarder.is_none()
            && let Some(sdk_relay_client) = &self.inner.transport.sdk_relay_client
        {
            *notification_forwarder = Some(spawn_relay_notification_forwarder(
                sdk_relay_client.clone(),
                self.inner.transport.adapter.clone(),
                self.inner.transport.directory_events.clone(),
            ));
        }
        let transport = self.inner.transport.clone();
        let adapter = transport.adapter.clone();
        let handle = handle.spawn(async move {
            while let Ok(Some(delivery)) = adapter.receive().await {
                let sender = account_deliveries_read(&transport.account_deliveries)
                    .get(&delivery.account_id)
                    .cloned();
                if let Some(sender) = sender {
                    // Fan out without awaiting the per-account queue: a single
                    // account whose receiver has stalled (full buffer) must not
                    // block this shared router and back-pressure delivery for
                    // every other account (and, upstream, the relay notification
                    // pipeline). Drop the delivery for the lagging account
                    // instead; it recovers on the next subscription catch-up.
                    match sender.try_send(delivery) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(
                                target: "marmot_app::relay_plane",
                                method = "spawn_router",
                                "dropping transport delivery: account delivery queue full",
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }
        });
        *router = Some(handle);
    }

    #[cfg(test)]
    pub(crate) async fn handle_relay_event_for_test(
        &self,
        relay_event: transport_nostr_adapter::NostrRelayEvent,
    ) -> Result<usize, TransportAdapterError> {
        self.inner
            .transport
            .adapter
            .handle_relay_event(relay_event)
            .await
    }
}

impl RelayPlaneHealth {
    fn from_sdk(health: NostrSdkRelayHealth, directory: DirectoryRelayStats) -> Self {
        Self {
            sdk_backed: true,
            total_relays: health.total_relays,
            initialized: health.initialized,
            pending: health.pending,
            connecting: health.connecting,
            connected: health.connected,
            disconnected: health.disconnected,
            terminated: health.terminated,
            banned: health.banned,
            sleeping: health.sleeping,
            connection_attempts: health.connection_attempts,
            connection_successes: health.connection_successes,
            directory_inflight_fetches: directory.inflight_fetches,
            directory_active_subscriptions: directory.active_subscriptions,
            directory_completed_fetches: directory.completed_fetches,
            directory_coalesced_waiters: directory.coalesced_waiters,
            directory_failed_fetches: directory.failed_fetches,
            directory_completed_subscription_syncs: directory.completed_subscription_syncs,
            directory_subscriptions_created: directory.subscriptions_created,
            directory_subscriptions_removed: directory.subscriptions_removed,
        }
    }

    fn from_directory(directory: DirectoryRelayStats) -> Self {
        Self {
            directory_inflight_fetches: directory.inflight_fetches,
            directory_active_subscriptions: directory.active_subscriptions,
            directory_completed_fetches: directory.completed_fetches,
            directory_coalesced_waiters: directory.coalesced_waiters,
            directory_failed_fetches: directory.failed_fetches,
            directory_completed_subscription_syncs: directory.completed_subscription_syncs,
            directory_subscriptions_created: directory.subscriptions_created,
            directory_subscriptions_removed: directory.subscriptions_removed,
            ..Self::default()
        }
    }
}

impl DirectoryEventQuery {
    pub(crate) fn new(kind: u64, mut authors: Vec<String>, limit: usize) -> Self {
        authors.sort();
        authors.dedup();
        Self {
            kind,
            authors,
            limit,
        }
    }
}

impl DirectoryFetchRequest {
    fn new(
        mut endpoints: Vec<TransportEndpoint>,
        mut queries: Vec<DirectoryEventQuery>,
    ) -> Result<Self, String> {
        endpoints.sort();
        endpoints.dedup();
        queries.sort();
        queries.dedup();
        if endpoints.is_empty() {
            return Err("directory fetch: no relay endpoints".to_owned());
        }
        if queries.is_empty() {
            return Err("directory fetch: no queries".to_owned());
        }
        for query in &queries {
            if query.authors.is_empty() {
                return Err("directory fetch: no query authors".to_owned());
            }
            if query.limit == 0 {
                return Err("directory fetch: query limit must be greater than zero".to_owned());
            }
        }
        Ok(Self { endpoints, queries })
    }

    fn key(&self) -> DirectoryFetchKey {
        DirectoryFetchKey {
            endpoints: self.endpoints.clone(),
            queries: self.queries.clone(),
        }
    }
}

impl DirectoryRelayPlane {
    fn new(fetcher: Arc<dyn DirectoryRelayFetcher>) -> Self {
        Self {
            fetcher,
            state: Arc::new(Mutex::new(DirectoryRelayPlaneState::default())),
        }
    }

    async fn fetch_events(
        &self,
        request: DirectoryFetchRequest,
    ) -> Result<Vec<DirectoryRelayEventRecord>, String> {
        let key = request.key();
        let (rx, should_spawn) = {
            let (tx, rx) = oneshot::channel();
            let mut state = self.state.lock().await;
            if let Some(waiters) = state.inflight.get_mut(&key) {
                waiters.push(tx);
                state.coalesced_waiters += 1;
                (rx, false)
            } else {
                state.inflight.insert(key.clone(), vec![tx]);
                (rx, true)
            }
        };

        if should_spawn {
            let fetcher = self.fetcher.clone();
            let state = self.state.clone();
            tokio::spawn(async move {
                let result = fetcher.fetch_directory_events(request).await;
                let mut state = state.lock().await;
                if result.is_ok() {
                    state.completed_fetches += 1;
                } else {
                    state.failed_fetches += 1;
                }
                if let Some(waiters) = state.inflight.remove(&key) {
                    for waiter in waiters {
                        let _ = waiter.send(result.clone());
                    }
                }
            });
        }

        rx.await
            .map_err(|_| "directory fetch owner dropped before completing".to_owned())?
    }

    async fn stats(&self) -> DirectoryRelayStats {
        let state = self.state.lock().await;
        DirectoryRelayStats {
            inflight_fetches: state.inflight.len(),
            active_subscriptions: state.active_subscription_ids.len(),
            completed_fetches: state.completed_fetches,
            coalesced_waiters: state.coalesced_waiters,
            failed_fetches: state.failed_fetches,
            completed_subscription_syncs: state.completed_subscription_syncs,
            subscriptions_created: state.subscriptions_created,
            subscriptions_removed: state.subscriptions_removed,
        }
    }

    async fn subscription_diff(
        &self,
        desired_ids: &HashSet<String>,
    ) -> (HashSet<String>, HashSet<String>) {
        let state = self.state.lock().await;
        let to_add = desired_ids
            .difference(&state.active_subscription_ids)
            .cloned()
            .collect::<HashSet<_>>();
        let to_remove = state
            .active_subscription_ids
            .difference(desired_ids)
            .cloned()
            .collect::<HashSet<_>>();
        (to_add, to_remove)
    }

    async fn replace_subscription_ids(
        &self,
        desired_ids: HashSet<String>,
    ) -> Result<DirectorySubscriptionSyncSummary, String> {
        let mut state = self.state.lock().await;
        let created = desired_ids
            .difference(&state.active_subscription_ids)
            .count();
        let removed = state
            .active_subscription_ids
            .difference(&desired_ids)
            .count();
        state.completed_subscription_syncs += 1;
        state.subscriptions_created += created;
        state.subscriptions_removed += removed;
        state.active_subscription_ids = desired_ids;
        Ok(DirectorySubscriptionSyncSummary {
            active_subscriptions: state.active_subscription_ids.len(),
            subscriptions_created: created,
            subscriptions_removed: removed,
        })
    }
}

fn spawn_relay_notification_forwarder(
    sdk_relay_client: NostrSdkRelayClient,
    adapter: NostrTransportAdapter,
    directory_events: broadcast::Sender<DirectoryRelayEventRecord>,
) -> JoinHandle<()> {
    let client = sdk_relay_client.client().clone();
    tokio::spawn(async move {
        let _ = client
            .handle_notifications(move |notification| {
                let adapter = adapter.clone();
                let directory_events = directory_events.clone();
                async move {
                    match notification {
                        RelayPoolNotification::Event {
                            relay_url,
                            subscription_id,
                            event,
                        } => {
                            if let Ok(event) = NostrTransportEvent::from_nostr_event(&event) {
                                tracing::trace!(
                                    target: "marmot_app::relay_plane",
                                    method = "spawn_relay_notification_forwarder",
                                    "forwarding SDK relay event"
                                );
                                let endpoint = TransportEndpoint(relay_url.to_string());
                                let relay_event = transport_nostr_adapter::NostrRelayEvent {
                                    endpoint: endpoint.clone(),
                                    subscription_id: Some(subscription_id.to_string()),
                                    event: event.clone(),
                                };
                                let _ = adapter.handle_relay_event(relay_event).await;
                                let _ = directory_events.send(DirectoryRelayEventRecord {
                                    endpoints: vec![endpoint],
                                    event,
                                });
                            }
                            Ok(false)
                        }
                        RelayPoolNotification::Message {
                            relay_url,
                            message:
                                RelayMessage::Event {
                                    subscription_id,
                                    event,
                                },
                        } => {
                            // Raw per-relay copy (not deduplicated): telemetry
                            // only, so cross-relay arrival spread and per-relay
                            // first-event timing see every relay's copy. Delivery
                            // happens on the deduplicated `Event` arm above. Keep
                            // this in sync with the relay plane's own tap; the
                            // SDK client's standalone forwarder is unused here.
                            if let Ok(event) = NostrTransportEvent::from_nostr_event(&event) {
                                tracing::trace!(
                                    target: "marmot_app::relay_plane",
                                    method = "spawn_relay_notification_forwarder",
                                    "observing per-relay event copy"
                                );
                                adapter
                                    .observe_relay_event(transport_nostr_adapter::NostrRelayEvent {
                                        endpoint: TransportEndpoint(relay_url.to_string()),
                                        subscription_id: Some(subscription_id.to_string()),
                                        event,
                                    })
                                    .await;
                            }
                            Ok(false)
                        }
                        RelayPoolNotification::Message {
                            relay_url,
                            message: RelayMessage::EndOfStoredEvents(subscription_id),
                        } => {
                            // EOSE tap: advances the per-relay initial-sync gate
                            // and records EOSE latency. No delivery.
                            tracing::trace!(
                                target: "marmot_app::relay_plane",
                                method = "spawn_relay_notification_forwarder",
                                "forwarding SDK relay end-of-stored-events"
                            );
                            adapter
                                .handle_relay_eose(
                                    TransportEndpoint(relay_url.to_string()),
                                    subscription_id.to_string(),
                                )
                                .await;
                            Ok(false)
                        }
                        RelayPoolNotification::Shutdown => {
                            tracing::debug!(
                                target: "marmot_app::relay_plane",
                                method = "spawn_relay_notification_forwarder",
                                "SDK relay pool shutdown observed"
                            );
                            Ok(true)
                        }
                        _ => Ok(false),
                    }
                }
            })
            .await;
    })
}

impl NostrSdkDirectoryRelayFetcher {
    fn new(client: NostrSdkClient) -> Self {
        Self { client }
    }

    fn standalone() -> Self {
        Self::new(NostrSdkClient::builder().build())
    }
}

#[async_trait]
impl DirectoryRelayFetcher for NostrSdkDirectoryRelayFetcher {
    async fn fetch_directory_events(
        &self,
        request: DirectoryFetchRequest,
    ) -> Result<Vec<DirectoryRelayEventRecord>, String> {
        let relay_urls = request
            .endpoints
            .iter()
            .map(|endpoint| {
                RelayUrl::parse(endpoint.as_str())
                    .map_err(|e| format!("invalid relay URL {}: {e}", endpoint.as_str()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for relay_url in &relay_urls {
            self.client
                .add_relay(relay_url.clone())
                .await
                .map_err(|e| format!("add relay: {e}"))?;
            timeout(
                DIRECTORY_RELAY_CONNECT_WAIT,
                self.client.connect_relay(relay_url.clone()),
            )
            .await
            .map_err(|_| "connect relay timed out".to_owned())?
            .map_err(|e| format!("connect relay: {e}"))?;
        }

        let mut records = Vec::new();
        for query in request.queries {
            let public_keys = query
                .authors
                .iter()
                .map(|author| PublicKey::parse(author).map_err(|_| "invalid query author"))
                .collect::<Result<Vec<_>, _>>()?;
            let kind = u16::try_from(query.kind)
                .map(Kind::from)
                .map_err(|_| format!("unsupported Nostr kind {}", query.kind))?;
            let filter = Filter::new()
                .authors(public_keys)
                .kind(kind)
                .limit(query.limit);
            let events = self
                .client
                .fetch_events_from(relay_urls.clone(), filter, DIRECTORY_RELAY_FETCH_WAIT)
                .await
                .map_err(|e| format!("fetch directory events: {e}"))?;
            for event in events {
                let event = NostrTransportEvent::from_nostr_event(&event)
                    .map_err(|e| format!("map directory event: {e}"))?;
                records.push(DirectoryRelayEventRecord {
                    endpoints: request.endpoints.clone(),
                    event,
                });
            }
        }
        Ok(records)
    }
}

impl RelaySafetyPolicy {
    fn sanitize_activation(
        &self,
        mut activation: TransportAccountActivation,
    ) -> Result<TransportAccountActivation, String> {
        activation.inbox_endpoints =
            self.sanitize_endpoints(activation.inbox_endpoints, "account inbox")?;
        for group in &mut activation.group_subscriptions {
            group.endpoints = self.sanitize_endpoints(group.endpoints.clone(), "group route")?;
        }
        Ok(activation)
    }

    fn sanitize_group_sync(
        &self,
        mut sync: TransportGroupSync,
    ) -> Result<TransportGroupSync, String> {
        for group in &mut sync.group_subscriptions {
            group.endpoints = self.sanitize_endpoints(group.endpoints.clone(), "group route")?;
        }
        Ok(sync)
    }

    fn sanitize_publish_request(
        &self,
        mut request: TransportPublishRequest,
    ) -> Result<TransportPublishRequest, String> {
        match &mut request.target {
            TransportPublishTarget::Group { endpoints, .. } => {
                *endpoints = self.sanitize_endpoints(endpoints.clone(), "group publish")?;
            }
            TransportPublishTarget::Inbox { endpoints, .. } => {
                *endpoints = self.sanitize_endpoints(endpoints.clone(), "inbox publish")?;
            }
        }
        Ok(request)
    }

    fn sanitize_endpoints(
        &self,
        endpoints: Vec<TransportEndpoint>,
        context: &str,
    ) -> Result<Vec<TransportEndpoint>, String> {
        let mut sanitized = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let raw = endpoint.as_str().trim();
            if raw.is_empty() {
                return Err(format!("{context}: invalid relay endpoint"));
            }
            let relay_url = RelayUrl::parse(raw)
                .map_err(|err| format!("{context}: invalid relay endpoint: {err}"))?;
            let endpoint = TransportEndpoint(relay_url.to_string());
            if !sanitized.contains(&endpoint) {
                sanitized.push(endpoint);
            }
        }
        if sanitized.len() > self.max_endpoints_per_route {
            return Err(format!(
                "{context}: relay endpoint count {} exceeds limit {}",
                sanitized.len(),
                self.max_endpoints_per_route
            ));
        }
        Ok(sanitized)
    }
}

#[async_trait]
impl TransportAdapter for MarmotRelayPlaneAccountAdapter {
    async fn activate_account(
        &self,
        activation: TransportAccountActivation,
    ) -> Result<(), TransportAdapterError> {
        if activation.account_id != self.account_id {
            return Err(TransportAdapterError::AccountNotActive(
                activation.account_id,
            ));
        }
        let activation = self
            .relay_plane
            .inner
            .relay_safety
            .sanitize_activation(activation)
            .map_err(TransportAdapterError::Subscription)?;
        self.relay_plane
            .inner
            .transport
            .adapter
            .activate_account(activation)
            .await
    }

    async fn sync_account_groups(
        &self,
        sync: TransportGroupSync,
    ) -> Result<(), TransportAdapterError> {
        if sync.account_id != self.account_id {
            return Err(TransportAdapterError::AccountNotActive(sync.account_id));
        }
        let sync = self
            .relay_plane
            .inner
            .relay_safety
            .sanitize_group_sync(sync)
            .map_err(TransportAdapterError::Subscription)?;
        self.relay_plane
            .inner
            .transport
            .adapter
            .sync_account_groups(sync)
            .await
    }

    async fn deactivate_account(&self, account_id: &MemberId) -> Result<(), TransportAdapterError> {
        if account_id != &self.account_id {
            return Err(TransportAdapterError::AccountNotActive(account_id.clone()));
        }
        account_deliveries_write(&self.relay_plane.inner.transport.account_deliveries)
            .remove(account_id);
        self.relay_plane
            .inner
            .transport
            .adapter
            .deactivate_account(account_id)
            .await
    }

    async fn publish(
        &self,
        request: TransportPublishRequest,
    ) -> Result<TransportPublishReport, TransportAdapterError> {
        if request.account_id != self.account_id {
            return Err(TransportAdapterError::AccountNotActive(request.account_id));
        }
        let request = self
            .relay_plane
            .inner
            .relay_safety
            .sanitize_publish_request(request)
            .map_err(TransportAdapterError::Publish)?;
        request.validate_envelope_matches_target()?;
        let event = NostrTransportEvent::from_transport_message(&request.message)
            .map_err(|e| TransportAdapterError::Publish(format!("Nostr payload: {e}")))?;
        let outcome = self
            .publish_client
            .publish_event(request.target.endpoints(), &event, request.required_acks)
            .await?;
        let local_fanout_endpoints = if !outcome.accepted.is_empty() {
            outcome
                .accepted
                .iter()
                .map(|receipt| receipt.endpoint.clone())
                .collect::<Vec<_>>()
        } else if outcome.failed.is_empty() {
            request.target.endpoints().to_vec()
        } else {
            Vec::new()
        };
        if !local_fanout_endpoints.is_empty() {
            let mut local_message = request.message.clone();
            if let Some(message_id) = outcome.message_id.clone() {
                local_message.id = message_id;
            }
            self.relay_plane
                .inner
                .transport
                .adapter
                .deliver_local_publish(&local_message, &local_fanout_endpoints)
                .await?;
        }
        Ok(publish_report_from_outcome(outcome, request))
    }

    async fn receive(&self) -> Result<Option<TransportDelivery>, TransportAdapterError> {
        Ok(self.delivery_rx.lock().await.recv().await)
    }
}

fn account_deliveries_read(
    deliveries: &RwLock<HashMap<MemberId, mpsc::Sender<TransportDelivery>>>,
) -> RwLockReadGuard<'_, HashMap<MemberId, mpsc::Sender<TransportDelivery>>> {
    deliveries
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn account_deliveries_write(
    deliveries: &RwLock<HashMap<MemberId, mpsc::Sender<TransportDelivery>>>,
) -> RwLockWriteGuard<'_, HashMap<MemberId, mpsc::Sender<TransportDelivery>>> {
    deliveries
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn publish_report_from_outcome(
    outcome: NostrPublishOutcome,
    request: TransportPublishRequest,
) -> TransportPublishReport {
    TransportPublishReport {
        message_id: outcome.message_id.unwrap_or(request.message.id),
        accepted: outcome.accepted,
        failed: outcome.failed,
        required_acks: request.required_acks,
    }
}

#[cfg(test)]
mod tests;
