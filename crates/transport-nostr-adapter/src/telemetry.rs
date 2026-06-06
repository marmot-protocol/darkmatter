//! Relay delivery telemetry: cross-relay arrival spread and subscription sync
//! timing.
//!
//! Implements phases 1 and 2 of the measurement model in
//! `docs/marmot-architecture/relay-delivery-telemetry.md`:
//!
//! - [`RelayDeliveryTelemetry`] records, per logical message seen on more than
//!   one relay endpoint, the local-time delta between the first copy and each
//!   later distinct-endpoint copy. That distribution estimates the client's
//!   relay-set delivery jitter, which steady-state convergence quiescence must
//!   cover.
//! - [`RelaySyncTelemetry`] records, per subscription, when each endpoint
//!   delivered its first event and its EOSE relative to subscribe time, and
//!   whether every subscribed endpoint has reached EOSE (the initial-sync
//!   gate).
//!
//! Privacy: all timing uses a local monotonic clock, never Nostr `created_at`
//! (which is identical across copies of an event and publisher-controlled).
//! Snapshots expose only aggregate histogram buckets and counts. Per-message
//! and per-subscription tracking tables are local-only ephemeral state; their
//! keys, relay endpoints, and subscription ids never appear in a snapshot or in
//! logs.

use std::collections::HashMap;
use std::collections::HashSet;

use cgka_traits::MessageId;
use cgka_traits::TransportEndpoint;

/// Upper bounds, in milliseconds, of the duration histogram buckets shared by
/// spread and sync-timing measurements.
///
/// A delta is counted in the first bucket whose bound it does not exceed.
/// Deltas above the last bound fall in a dedicated overflow bucket.
const BUCKET_BOUNDS_MS: [u64; 10] = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// Default retention window for the per-message first-sighting table.
///
/// A message first seen longer ago than this is pruned. If it was only ever
/// seen on one endpoint it is counted as single-source. The window should be
/// comfortably larger than the largest histogram bucket so that genuine laggard
/// copies are still corroborated rather than pruned.
const DEFAULT_TRACKING_WINDOW_MS: u64 = 60_000;

/// Internal fixed-bucket duration histogram in milliseconds.
#[derive(Clone, Debug, Default)]
struct DurationHistogram {
    buckets: [u64; BUCKET_BOUNDS_MS.len()],
    overflow: u64,
}

impl DurationHistogram {
    fn record(&mut self, delta_ms: u64) {
        for (idx, bound) in BUCKET_BOUNDS_MS.iter().enumerate() {
            if delta_ms <= *bound {
                self.buckets[idx] += 1;
                return;
            }
        }
        self.overflow += 1;
    }

    fn snapshot(&self) -> DurationHistogramSnapshot {
        let buckets = BUCKET_BOUNDS_MS
            .iter()
            .zip(self.buckets.iter())
            .map(|(bound, count)| HistogramBucket {
                upper_bound_ms: *bound,
                count: *count,
            })
            .collect();
        DurationHistogramSnapshot {
            buckets,
            overflow_count: self.overflow,
        }
    }
}

/// One histogram bucket of a duration distribution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistogramBucket {
    /// Inclusive upper bound of the bucket, in milliseconds.
    pub upper_bound_ms: u64,
    /// Number of samples whose duration fell in this bucket.
    pub count: u64,
}

/// Aggregate duration histogram snapshot.
///
/// Contains only counts and millisecond bucket bounds: no message ids, relay
/// endpoints, subscription ids, or payload-derived values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurationHistogramSnapshot {
    /// Histogram by ascending upper bound.
    pub buckets: Vec<HistogramBucket>,
    /// Samples whose duration exceeded the largest bucket bound.
    pub overflow_count: u64,
}

impl DurationHistogramSnapshot {
    /// Total number of samples across all buckets and the overflow.
    pub fn sample_count(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.count).sum::<u64>() + self.overflow_count
    }

    /// Approximate `percentile` (0.0..=1.0), returned as the upper bound of the
    /// bucket the percentile falls in. Returns `None` when there are no samples,
    /// and `None` for the overflow region (the value is only known to exceed the
    /// largest bound), so callers treat an overflow-dominated distribution as
    /// "wider than the histogram measures."
    ///
    /// This is the primary input to a quiescence value: take a high percentile
    /// and add margin.
    pub fn approx_percentile_ms(&self, percentile: f64) -> Option<u64> {
        let total = self.sample_count();
        if total == 0 {
            return None;
        }
        let target = ((percentile.clamp(0.0, 1.0) * total as f64).ceil() as u64).max(1);
        let mut cumulative = 0;
        for bucket in &self.buckets {
            cumulative += bucket.count;
            if cumulative >= target {
                return Some(bucket.upper_bound_ms);
            }
        }
        // Remaining samples are in the overflow region: wider than measured.
        None
    }
}

/// First local-time sighting of a logical message and the endpoints that have
/// delivered it so far.
#[derive(Clone, Debug)]
struct FirstSighting {
    first_seen_ms: u64,
    endpoints: HashSet<TransportEndpoint>,
}

/// Local, aggregate cross-relay arrival-spread recorder.
///
/// Diagnostic only. Like [`crate::NostrAdapterMetrics`], it must never feed
/// convergence or branch selection.
#[derive(Clone, Debug)]
pub struct RelayDeliveryTelemetry {
    tracking_window_ms: u64,
    pending: HashMap<MessageId, FirstSighting>,
    spread: DurationHistogram,
    observed: u64,
    corroborated: u64,
    single_source: u64,
}

impl Default for RelayDeliveryTelemetry {
    fn default() -> Self {
        Self::with_window(DEFAULT_TRACKING_WINDOW_MS)
    }
}

impl RelayDeliveryTelemetry {
    pub fn with_window(tracking_window_ms: u64) -> Self {
        Self {
            tracking_window_ms,
            pending: HashMap::new(),
            spread: DurationHistogram::default(),
            observed: 0,
            corroborated: 0,
            single_source: 0,
        }
    }

    /// Record one local-time sighting of `message_id` from `endpoint`.
    ///
    /// `now_ms` is a local monotonic timestamp in milliseconds. The same
    /// endpoint re-delivering a message is ignored; only the first sighting
    /// from each distinct endpoint contributes a spread sample. Pruning of the
    /// tracking window happens here so the table stays bounded without a timer.
    pub fn record_sighting(
        &mut self,
        message_id: &MessageId,
        endpoint: &TransportEndpoint,
        now_ms: u64,
    ) {
        self.prune(now_ms);

        match self.pending.get_mut(message_id) {
            None => {
                self.observed += 1;
                let mut endpoints = HashSet::new();
                endpoints.insert(endpoint.clone());
                self.pending.insert(
                    message_id.clone(),
                    FirstSighting {
                        first_seen_ms: now_ms,
                        endpoints,
                    },
                );
            }
            Some(sighting) => {
                if sighting.endpoints.insert(endpoint.clone()) {
                    // First time this distinct endpoint corroborates the message.
                    if sighting.endpoints.len() == 2 {
                        self.corroborated += 1;
                    }
                    let delta = now_ms.saturating_sub(sighting.first_seen_ms);
                    self.spread.record(delta);
                }
            }
        }
    }

    /// Drop first-sighting entries older than the tracking window, counting any
    /// that never reached a second endpoint as single-source.
    fn prune(&mut self, now_ms: u64) {
        let window = self.tracking_window_ms;
        let mut newly_single = 0;
        self.pending.retain(|_, sighting| {
            let expired = now_ms.saturating_sub(sighting.first_seen_ms) > window;
            if expired && sighting.endpoints.len() == 1 {
                newly_single += 1;
            }
            !expired
        });
        self.single_source += newly_single;
    }

    /// Aggregate, privacy-safe snapshot for diagnostics and quiescence tuning.
    pub fn snapshot(&self) -> RelayDeliverySpread {
        RelayDeliverySpread {
            observed: self.observed,
            corroborated: self.corroborated,
            single_source: self.single_source,
            spread: self.spread.snapshot(),
        }
    }
}

/// Aggregate cross-relay arrival-spread snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayDeliverySpread {
    /// Distinct logical messages observed within the tracking window.
    pub observed: u64,
    /// Messages corroborated by at least a second distinct endpoint.
    pub corroborated: u64,
    /// Messages pruned having been seen on exactly one endpoint.
    pub single_source: u64,
    /// Histogram of first-to-later-endpoint spread, in local-time milliseconds.
    pub spread: DurationHistogramSnapshot,
}

/// Per-endpoint subscription progress relative to subscribe time.
#[derive(Clone, Debug)]
struct EndpointProgress {
    started_ms: u64,
    first_event_seen: bool,
    eose_seen: bool,
}

/// Progress of one subscription across the endpoints it was issued to.
#[derive(Clone, Debug, Default)]
struct SubscriptionProgress {
    endpoints: HashMap<TransportEndpoint, EndpointProgress>,
}

/// Local recorder for subscription sync timing and the initial-sync gate.
///
/// Diagnostic only; must never feed convergence or branch selection. Relay
/// endpoints and subscription ids are tracking keys only and never appear in a
/// snapshot.
#[derive(Clone, Debug, Default)]
pub struct RelaySyncTelemetry {
    subscriptions: HashMap<String, SubscriptionProgress>,
    first_event: DurationHistogram,
    eose: DurationHistogram,
}

impl RelaySyncTelemetry {
    /// Record that `subscription_id` was (re)issued to `endpoints` at `now_ms`.
    ///
    /// Resets per-endpoint progress so a resubscribe is measured from its new
    /// start. Endpoints dropped from the subscription stop being tracked.
    pub fn record_subscription_start(
        &mut self,
        subscription_id: &str,
        endpoints: &[TransportEndpoint],
        now_ms: u64,
    ) {
        let progress = self
            .subscriptions
            .entry(subscription_id.to_string())
            .or_default();
        progress.endpoints = endpoints
            .iter()
            .map(|endpoint| {
                (
                    endpoint.clone(),
                    EndpointProgress {
                        started_ms: now_ms,
                        first_event_seen: false,
                        eose_seen: false,
                    },
                )
            })
            .collect();
    }

    /// Record the first event from `endpoint` for `subscription_id`. Later
    /// events and unknown subscription/endpoint pairs are ignored.
    pub fn record_first_event(
        &mut self,
        subscription_id: &str,
        endpoint: &TransportEndpoint,
        now_ms: u64,
    ) {
        if let Some(progress) = self
            .subscriptions
            .get_mut(subscription_id)
            .and_then(|sub| sub.endpoints.get_mut(endpoint))
            && !progress.first_event_seen
        {
            progress.first_event_seen = true;
            self.first_event
                .record(now_ms.saturating_sub(progress.started_ms));
        }
    }

    /// Record EOSE from `endpoint` for `subscription_id`. Repeat EOSE and
    /// unknown subscription/endpoint pairs are ignored.
    pub fn record_eose(
        &mut self,
        subscription_id: &str,
        endpoint: &TransportEndpoint,
        now_ms: u64,
    ) {
        if let Some(progress) = self
            .subscriptions
            .get_mut(subscription_id)
            .and_then(|sub| sub.endpoints.get_mut(endpoint))
            && !progress.eose_seen
        {
            progress.eose_seen = true;
            self.eose.record(now_ms.saturating_sub(progress.started_ms));
        }
    }

    /// Whether every endpoint of `subscription_id` has reached EOSE.
    ///
    /// Returns `None` for an unknown subscription, `Some(false)` while any
    /// endpoint is still draining, `Some(true)` once all have completed. This
    /// is the initial-sync gate signal.
    pub fn subscription_synced(&self, subscription_id: &str) -> Option<bool> {
        self.subscriptions.get(subscription_id).map(|sub| {
            !sub.endpoints.is_empty() && sub.endpoints.values().all(|endpoint| endpoint.eose_seen)
        })
    }

    /// Aggregate, privacy-safe snapshot of subscription sync timing.
    pub fn snapshot(&self) -> RelaySyncSnapshot {
        let synced = self
            .subscriptions
            .values()
            .filter(|sub| {
                !sub.endpoints.is_empty() && sub.endpoints.values().all(|ep| ep.eose_seen)
            })
            .count() as u64;
        RelaySyncSnapshot {
            tracked_subscriptions: self.subscriptions.len() as u64,
            synced_subscriptions: synced,
            first_event: self.first_event.snapshot(),
            eose: self.eose.snapshot(),
        }
    }
}

/// Aggregate subscription sync-timing snapshot.
///
/// Counts and millisecond histograms only: no subscription ids or relay
/// endpoints.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelaySyncSnapshot {
    /// Subscriptions currently tracked.
    pub tracked_subscriptions: u64,
    /// Tracked subscriptions whose every endpoint has reached EOSE.
    pub synced_subscriptions: u64,
    /// Per-endpoint first-event latency from subscribe time, in local-time ms.
    pub first_event: DurationHistogramSnapshot,
    /// Per-endpoint EOSE latency from subscribe time, in local-time ms.
    pub eose: DurationHistogramSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(byte: u8) -> MessageId {
        MessageId::new(vec![byte; 32])
    }

    fn relay(url: &str) -> TransportEndpoint {
        TransportEndpoint(url.to_string())
    }

    #[test]
    fn single_endpoint_sighting_records_no_spread() {
        let mut telem = RelayDeliveryTelemetry::default();
        telem.record_sighting(&msg(1), &relay("wss://a"), 0);

        let snap = telem.snapshot();
        assert_eq!(snap.observed, 1);
        assert_eq!(snap.corroborated, 0);
        assert_eq!(snap.spread.sample_count(), 0);
    }

    #[test]
    fn second_distinct_endpoint_records_spread_in_local_time() {
        let mut telem = RelayDeliveryTelemetry::default();
        telem.record_sighting(&msg(1), &relay("wss://a"), 100);
        // Same message, later, from a different relay: 40ms spread.
        telem.record_sighting(&msg(1), &relay("wss://b"), 140);

        let snap = telem.snapshot();
        assert_eq!(snap.observed, 1);
        assert_eq!(snap.corroborated, 1);
        assert_eq!(snap.spread.sample_count(), 1);
        // 40ms lands in the <=50ms bucket.
        let bucket = snap
            .spread
            .buckets
            .iter()
            .find(|b| b.upper_bound_ms == 50)
            .expect("50ms bucket");
        assert_eq!(bucket.count, 1);
    }

    #[test]
    fn same_endpoint_redelivery_is_ignored() {
        let mut telem = RelayDeliveryTelemetry::default();
        telem.record_sighting(&msg(1), &relay("wss://a"), 0);
        telem.record_sighting(&msg(1), &relay("wss://a"), 500);

        let snap = telem.snapshot();
        assert_eq!(snap.corroborated, 0);
        assert_eq!(snap.spread.sample_count(), 0);
    }

    #[test]
    fn third_endpoint_adds_a_second_sample_but_not_a_second_corroboration() {
        let mut telem = RelayDeliveryTelemetry::default();
        telem.record_sighting(&msg(1), &relay("wss://a"), 0);
        telem.record_sighting(&msg(1), &relay("wss://b"), 20);
        telem.record_sighting(&msg(1), &relay("wss://c"), 300);

        let snap = telem.snapshot();
        // Corroboration counts the message once; each laggard endpoint is a sample.
        assert_eq!(snap.corroborated, 1);
        assert_eq!(snap.spread.sample_count(), 2);
    }

    #[test]
    fn expired_single_source_message_is_counted_on_prune() {
        let mut telem = RelayDeliveryTelemetry::with_window(1_000);
        telem.record_sighting(&msg(1), &relay("wss://a"), 0);
        // A later, unrelated sighting past the window triggers the prune.
        telem.record_sighting(&msg(2), &relay("wss://a"), 2_000);

        let snap = telem.snapshot();
        assert_eq!(snap.single_source, 1);
    }

    #[test]
    fn percentile_reads_the_bucket_the_target_falls_in() {
        let mut telem = RelayDeliveryTelemetry::default();
        // Nine fast (<=10ms) and one slow (~2000ms) corroboration.
        for byte in 0..9u8 {
            telem.record_sighting(&msg(byte), &relay("wss://a"), 0);
            telem.record_sighting(&msg(byte), &relay("wss://b"), 5);
        }
        telem.record_sighting(&msg(200), &relay("wss://a"), 0);
        telem.record_sighting(&msg(200), &relay("wss://b"), 2_000);

        let snap = telem.snapshot();
        assert_eq!(snap.spread.sample_count(), 10);
        // p50 sits among the fast samples.
        assert_eq!(snap.spread.approx_percentile_ms(0.5), Some(10));
        // p100 reaches the slow laggard's bucket (<=2500ms).
        assert_eq!(snap.spread.approx_percentile_ms(1.0), Some(2500));
    }

    #[test]
    fn percentile_is_none_without_samples() {
        let telem = RelayDeliveryTelemetry::default();
        assert_eq!(telem.snapshot().spread.approx_percentile_ms(0.99), None);
    }

    #[test]
    fn spread_beyond_largest_bucket_counts_as_overflow() {
        let mut telem = RelayDeliveryTelemetry::default();
        telem.record_sighting(&msg(1), &relay("wss://a"), 0);
        telem.record_sighting(&msg(1), &relay("wss://b"), 20_000);

        let snap = telem.snapshot();
        assert_eq!(snap.spread.overflow_count, 1);
        assert_eq!(snap.spread.sample_count(), 1);
        // Overflow-only distribution reports "wider than measured".
        assert_eq!(snap.spread.approx_percentile_ms(1.0), None);
    }

    #[test]
    fn unknown_subscription_has_no_sync_state() {
        let telem = RelaySyncTelemetry::default();
        assert_eq!(telem.subscription_synced("sub"), None);
    }

    #[test]
    fn subscription_is_synced_only_when_all_endpoints_eose() {
        let mut telem = RelaySyncTelemetry::default();
        let (a, b) = (relay("wss://a"), relay("wss://b"));
        telem.record_subscription_start("sub", &[a.clone(), b.clone()], 0);
        assert_eq!(telem.subscription_synced("sub"), Some(false));

        telem.record_eose("sub", &a, 30);
        assert_eq!(telem.subscription_synced("sub"), Some(false));

        telem.record_eose("sub", &b, 70);
        assert_eq!(telem.subscription_synced("sub"), Some(true));

        let snap = telem.snapshot();
        assert_eq!(snap.tracked_subscriptions, 1);
        assert_eq!(snap.synced_subscriptions, 1);
        assert_eq!(snap.eose.sample_count(), 2);
    }

    #[test]
    fn first_event_latency_recorded_once_per_endpoint() {
        let mut telem = RelaySyncTelemetry::default();
        let a = relay("wss://a");
        telem.record_subscription_start("sub", std::slice::from_ref(&a), 100);
        telem.record_first_event("sub", &a, 130);
        // A later event from the same endpoint does not record again.
        telem.record_first_event("sub", &a, 900);

        let snap = telem.snapshot();
        assert_eq!(snap.first_event.sample_count(), 1);
        // 30ms latency lands in the <=50ms bucket.
        let bucket = snap
            .first_event
            .buckets
            .iter()
            .find(|b| b.upper_bound_ms == 50)
            .expect("50ms bucket");
        assert_eq!(bucket.count, 1);
    }

    #[test]
    fn events_for_untracked_subscription_are_ignored() {
        let mut telem = RelaySyncTelemetry::default();
        let a = relay("wss://a");
        telem.record_first_event("ghost", &a, 10);
        telem.record_eose("ghost", &a, 20);

        let snap = telem.snapshot();
        assert_eq!(snap.tracked_subscriptions, 0);
        assert_eq!(snap.first_event.sample_count(), 0);
        assert_eq!(snap.eose.sample_count(), 0);
    }

    #[test]
    fn resubscribe_resets_endpoint_progress() {
        let mut telem = RelaySyncTelemetry::default();
        let a = relay("wss://a");
        telem.record_subscription_start("sub", std::slice::from_ref(&a), 0);
        telem.record_eose("sub", &a, 10);
        assert_eq!(telem.subscription_synced("sub"), Some(true));

        // Reissued: the prior EOSE no longer counts.
        telem.record_subscription_start("sub", std::slice::from_ref(&a), 100);
        assert_eq!(telem.subscription_synced("sub"), Some(false));
    }
}
