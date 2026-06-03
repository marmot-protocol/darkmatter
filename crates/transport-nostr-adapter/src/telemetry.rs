//! Relay delivery telemetry: cross-relay arrival spread.
//!
//! This is Phase 1 of the measurement model in
//! `docs/marmot-architecture/relay-delivery-telemetry.md`. It records, for each
//! logical message seen on more than one relay endpoint, the local-time delta
//! between the first copy and each later distinct-endpoint copy. The
//! distribution of those deltas estimates the client's relay-set delivery
//! jitter, which is the quantity steady-state convergence quiescence must cover.
//!
//! Privacy: all timing uses a local monotonic clock, never Nostr `created_at`
//! (which is identical across copies of an event and publisher-controlled). The
//! snapshot exposes only aggregate histogram buckets and counts. The
//! per-message first-sighting table is local-only ephemeral state, pruned on a
//! window; its keys and relay endpoints never appear in the snapshot or in logs.

use std::collections::HashMap;
use std::collections::HashSet;

use cgka_traits::MessageId;
use cgka_traits::TransportEndpoint;

/// Upper bounds, in milliseconds, of the cross-relay spread histogram buckets.
///
/// A delta is counted in the first bucket whose bound it does not exceed.
/// Deltas above the last bound fall in a dedicated overflow bucket.
const SPREAD_BUCKET_BOUNDS_MS: [u64; 10] = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// Default retention window for the per-message first-sighting table.
///
/// A message first seen longer ago than this is pruned. If it was only ever
/// seen on one endpoint it is counted as single-source. The window should be
/// comfortably larger than the largest spread bucket so that genuine laggard
/// copies are still corroborated rather than pruned.
const DEFAULT_TRACKING_WINDOW_MS: u64 = 60_000;

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
    spread_buckets: [u64; SPREAD_BUCKET_BOUNDS_MS.len()],
    spread_overflow: u64,
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
            spread_buckets: [0; SPREAD_BUCKET_BOUNDS_MS.len()],
            spread_overflow: 0,
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
                    self.record_spread(delta);
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

    fn record_spread(&mut self, delta_ms: u64) {
        for (idx, bound) in SPREAD_BUCKET_BOUNDS_MS.iter().enumerate() {
            if delta_ms <= *bound {
                self.spread_buckets[idx] += 1;
                return;
            }
        }
        self.spread_overflow += 1;
    }

    /// Aggregate, privacy-safe snapshot for diagnostics and quiescence tuning.
    pub fn snapshot(&self) -> RelayDeliverySpread {
        let buckets = SPREAD_BUCKET_BOUNDS_MS
            .iter()
            .zip(self.spread_buckets.iter())
            .map(|(bound, count)| SpreadBucket {
                upper_bound_ms: *bound,
                count: *count,
            })
            .collect();
        RelayDeliverySpread {
            observed: self.observed,
            corroborated: self.corroborated,
            single_source: self.single_source,
            buckets,
            overflow_count: self.spread_overflow,
        }
    }
}

/// One histogram bucket of cross-relay arrival spread.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpreadBucket {
    /// Inclusive upper bound of the bucket, in milliseconds.
    pub upper_bound_ms: u64,
    /// Number of corroborating sightings whose spread fell in this bucket.
    pub count: u64,
}

/// Aggregate cross-relay arrival-spread snapshot.
///
/// Contains only counts and millisecond bucket bounds: no message ids, relay
/// endpoints, pubkeys, or payload-derived values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayDeliverySpread {
    /// Distinct logical messages observed within the tracking window.
    pub observed: u64,
    /// Messages corroborated by at least a second distinct endpoint.
    pub corroborated: u64,
    /// Messages pruned having been seen on exactly one endpoint.
    pub single_source: u64,
    /// Spread histogram by ascending upper bound.
    pub buckets: Vec<SpreadBucket>,
    /// Corroborating sightings whose spread exceeded the largest bucket bound.
    pub overflow_count: u64,
}

impl RelayDeliverySpread {
    /// Total number of spread samples across all buckets and the overflow.
    pub fn sample_count(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.count).sum::<u64>() + self.overflow_count
    }

    /// Approximate `percentile` (0.0..=1.0) of the spread distribution, returned
    /// as the upper bound of the bucket the percentile falls in. Returns `None`
    /// when there are no samples, and `None` for the overflow region (the value
    /// is only known to exceed the largest bound), so callers treat an
    /// overflow-dominated distribution as "wider than the histogram measures."
    ///
    /// This is the primary input to a local steady-state quiescence value: take
    /// a high percentile and add margin.
    pub fn approx_percentile_ms(&self, percentile: f64) -> Option<u64> {
        let total = self.sample_count();
        if total == 0 {
            return None;
        }
        let target = (percentile.clamp(0.0, 1.0) * total as f64).ceil() as u64;
        let target = target.max(1);
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
        assert_eq!(snap.sample_count(), 0);
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
        assert_eq!(snap.sample_count(), 1);
        // 40ms lands in the <=50ms bucket.
        let bucket = snap
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
        assert_eq!(snap.sample_count(), 0);
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
        assert_eq!(snap.sample_count(), 2);
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
        assert_eq!(snap.sample_count(), 10);
        // p50 sits among the fast samples.
        assert_eq!(snap.approx_percentile_ms(0.5), Some(10));
        // p100 reaches the slow laggard's bucket (<=2500ms).
        assert_eq!(snap.approx_percentile_ms(1.0), Some(2500));
    }

    #[test]
    fn percentile_is_none_without_samples() {
        let telem = RelayDeliveryTelemetry::default();
        assert_eq!(telem.snapshot().approx_percentile_ms(0.99), None);
    }

    #[test]
    fn spread_beyond_largest_bucket_counts_as_overflow() {
        let mut telem = RelayDeliveryTelemetry::default();
        telem.record_sighting(&msg(1), &relay("wss://a"), 0);
        telem.record_sighting(&msg(1), &relay("wss://b"), 20_000);

        let snap = telem.snapshot();
        assert_eq!(snap.overflow_count, 1);
        assert_eq!(snap.sample_count(), 1);
        // Overflow-only distribution reports "wider than measured".
        assert_eq!(snap.approx_percentile_ms(1.0), None);
    }
}
