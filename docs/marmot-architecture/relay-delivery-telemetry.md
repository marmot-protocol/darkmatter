# Relay Delivery Telemetry and Quiescence Tuning

**Status:** design draft. This is the target model for measuring Nostr relay delivery behavior so that convergence
quiescence can be tuned from evidence instead of guessed.

This note is the measurement companion to [`distributed-convergence.md`](./distributed-convergence.md). Convergence
defines *how* a client selects a canonical branch from a bag of unordered messages. This note defines *how a client
decides it has waited long enough* before settling, and what it must measure to choose that wait well. Privacy rules for
any emitted signal come from [`overview/observability.md`](./overview/observability.md).

## Problem

A client can never prove it has every message a group has produced. A commit may sit on a relay it does not query, or be
withheld, and in an open asynchronous system there is no way to distinguish "does not exist" from "not delivered to me."
Completeness is unsolvable in the strong sense, and no ordering or consensus mechanism changes that.

The convergence model already accepts this. It does not try to *achieve* completeness; it makes gaps **detectable** (a
commit whose parent is unknown is deferred, not applied) and **bounds the damage** of a missing message (`max_rewind_commits`
plus witness quorum). What remains is the one place where the design still substitutes a heuristic for completeness: the
decision to stop waiting and settle. Today that decision is `settlement_quiescence_ms`, a group-negotiated constant. We
do not have a principled basis for its value.

This note specifies what to measure to set it, and how the measurement maps onto the convergence machine.

### Considered and rejected: Lamport clocks and BFT

We evaluated two structural changes and rejected both. Recording the rationale here so it is not relitigated.

- **Lamport (and vector) clocks.** A Lamport clock is a strictly weaker version of the MLS commit chain we already
  have: the chain links each commit to its parent epoch state cryptographically, so it encodes happens-before with
  authentication, which a self-asserted counter cannot. Clock values are forgeable, so they add nothing in the
  adversarial setting the protocol targets, and `spec/principles.md` already forbids transport-supplied ordering from
  choosing group state for exactly this reason. A scalar Lamport timestamp also does not detect gaps; only a vector
  clock does, and only per sender, which the parent-link already does better. A signed, in-payload **per-sender
  monotonic counter** is the one idea in this family worth keeping, but only as an anti-entropy hint (see
  [Backfill and reconciliation](#backfill-and-reconciliation)), not as state-machine input.
- **BFT consensus.** Classical BFT gives a single agreed order with instant finality, but needs an online quorum and
  multi-round voting over a known validator set. Group-chat members are asynchronously offline, and membership is the
  very thing commits change, so BFT would trade away the offline-tolerance and leaderlessness Marmot is built on. It
  also does not touch the relay-completeness problem: the relays are the asynchronous network the consensus must
  tolerate, not the consensus nodes. The convergence model is already a consensus protocol of the right family for this
  setting — a weighted fork-choice rule (heaviest valid branch by `effective_commit_depth`, attestation weight via
  witness quorum, bounded reorg via `max_rewind_commits`) rather than a quorum-voting one.

## What quiescence actually measures

The naive instinct is to measure relay latency and set quiescence above it. That is the wrong quantity. Quiescence does
not protect against *slow* delivery; it protects against the gap between when the last message arrived and when a
still-in-flight message *would* arrive. The quantity that bounds that gap is the **straggler delay**: for a message that
genuinely exists and is delivered, how much later does its slowest copy show up.

### Separate the two delay sources

Straggler delay has two modes, and only one is coverable by a timer:

- **Delivery jitter:** network and relay flush time for a message that is in flight now. Milliseconds to seconds.
  Bounded, measurable, and what quiescence should cover.
- **Offline republish delay:** a member that was offline reconnects and publishes a commit created earlier. Minutes to
  days. Unbounded. A timer must **not** try to cover this; tuning quiescence against the full straggler distribution
  pins clients in `Syncing` indefinitely (`distributed-convergence.md` warns about the over-high value).

Offline republish is exactly the late-commit case the rollback horizon and witness quorum already handle: such a commit
lands after the client settles and triggers a bounded reorg within `max_rewind_commits`. Therefore quiescence is tuned
against the **delivery-jitter mode only**, and late commits beyond it are a convergence concern, not a timer concern.

### The loss function

Quiescence is not a correctness boundary. Setting it too low costs extra post-settle reorgs and regenerated intents
(annoying, never corrupting — convergence safety holds because both clients converge given the same inputs). Setting it
too high costs liveness: the client stalls in `Syncing`. The objective is therefore to **minimize stall time subject to
keeping the post-settle reorg rate under a target**, where the reorg rate is itself observable. The delivery-jitter
distribution gives one side of that trade; the observed reorg rate gives the other.

### The headline metric: cross-relay arrival spread

Because a group publishes redundantly to multiple relays, the most direct estimator of delivery jitter is already
available without any new protocol: for every message received on more than one relay endpoint, record the delta, in
**local receive time**, between the first copy and each later distinct-endpoint copy. The distribution of those deltas is
the delivery-jitter distribution of the client's own relay set. A high percentile of it (p99-ish) plus margin is the
delivery-jitter quiescence floor.

Local receive time is mandatory here. Nostr `created_at` is identical across copies of the same event and is
publisher-controlled, so it conveys nothing about delivery timing and must never be used as the telemetry clock.

## Two quiescence regimes

"What should quiescence be?" is hard partly because two different situations are conflated. They want different values
and different signals.

- **Initial-sync quiescence.** A reconnecting client is draining stored history from each relay. The natural completion
  signal is not a timer but **EOSE** (end of stored events): once every subscribed relay in the set has sent EOSE for the
  group subscription, the client has each relay's stored set, and a content-level **set reconciliation** pass
  (see below) closes residual gaps. A timer is only the fallback for relays that never send EOSE.
- **Steady-state live quiescence.** A connected client is waiting out the tail of in-flight live messages. Here the
  EOSE signal does not apply and the **cross-relay arrival-spread timer** governs.

Modeling these separately lets each take its appropriate signal instead of forcing one constant to cover both.

## Metric catalogue

All metrics below are local, aggregate, and privacy-safe per [`observability.md`](./overview/observability.md). All
timing is local-receive-time monotonic duration; none uses `created_at`, event ids, relay URLs, or payload-derived
values in any emitted form.

| Metric | Definition | Feeds |
| --- | --- | --- |
| `cross_relay_spread` | Per message seen on ≥2 endpoints: histogram of local-time delta from first copy to each later distinct-endpoint copy. | Steady-state quiescence floor. |
| `corroborated` / `single_source` | Count of messages seen on ≥2 endpoints vs. exactly one within the tracking window. | Relay redundancy health; confidence in the spread estimate. |
| `time_to_first_event` | Per relay (opaque local index): local time from subscription start to first matching event. | Subscription health, slow-relay deprioritization. |
| `time_to_eose` | Per relay: local time from subscription start to EOSE. | Initial-sync gating; relays that never EOSE. |
| `observed_reorg_rate` | Rate of post-settle convergence reorgs attributable to late delivery within the horizon. | The other side of the quiescence loss function. |

`cross_relay_spread` is the foundational one and is fully self-contained in the transport adapter. The per-relay timing
and EOSE metrics require EOSE plumbing the adapter does not yet have. `observed_reorg_rate` is owned by the engine, not
the adapter, and is recorded against settle outcomes.

## Mapping metrics to policy

The convergence policy already says `settlement_quiescence_ms` is a group **floor** that clients MAY exceed locally but
MUST NOT undercut. That is exactly the structure the measurements want:

- **Local adaptive value.** Each client computes a local steady-state quiescence from a high percentile of its own
  `cross_relay_spread` distribution plus margin, clamped to at least the negotiated floor. A client with a tight,
  redundant relay set settles fast; a client with a flaky set waits longer, without forcing everyone to the slowest
  member's value.
- **Negotiated floor.** The aggregate spread distribution across the membership informs what the group floor should be.
  This is a human/policy decision fed by telemetry, not an automated negotiation.

This keeps the static-constant tension resolved: the constant becomes a conservative floor, and adaptation happens
locally and upward from measured evidence.

## Backfill and reconciliation

The current fetch path resubscribes with a `since = last_synced - backfill` hint. This is a fine cheap default but has a
structural blind spot that overlaps the convergence-critical case: a commit created offline and published late can carry
a `created_at` earlier than `last_synced`, so a `since` filter excludes it at the relay. That is precisely a valid late
commit inside the rollback horizon — the input branch selection most needs to see.

Two layers address this:

- **Timestamp backfill** stays as the fast path for honestly-recent late delivery on queried relays.
- **Set reconciliation** (Nostr Negentropy, NIP-77) is the correctness backstop: it is content-addressed and
  time-independent, so a backdated-but-present event is reconciled regardless of `created_at`. It cannot recover events
  on relays the client never queries — nothing can — but it converts a silent backdated miss into a detected-and-fetched
  one. It composes cleanly because the engine already treats transport order as advisory; reconciliation just feeds more
  candidate bytes into the same convergence pass.

A signed, in-payload **per-sender monotonic counter** is an optional further hint: a client that holds sender A's #5 and
#7 but not #6 knows to delay settling and re-fetch. It is protocol evidence (inside the MLS payload, signed), so it is
admissible where transport timestamps are not, but it is a liveness aid, never a branch-selection input, and it adds
equivocation surface (a doubled counter is just a fork the engine already detects).

## Instrumentation interface

Telemetry extends the adapter's existing diagnostic surface rather than introducing a parallel one. The pattern to match
is `NostrAdapterMetrics` (aggregate lifecycle counters, explicitly barred from feeding convergence) and
`NostrSdkRelayHealth` (redacted aggregate relay status).

- The adapter records `cross_relay_spread` inside `handle_relay_event`, keyed by the transport-independent
  `TransportMessage.id`, using a local monotonic clock captured at the adapter, never `created_at`. The per-message
  first-sighting table is local-only ephemeral state, pruned on a window, and its keys never leave the device or appear
  in logs.
- The snapshot accessor returns only aggregate histogram buckets and counts. Relays appear as opaque local indices in
  any per-relay breakdown, never as URLs, keeping the privacy boundary structural rather than a redaction step.
- Per-relay `time_to_first_event` / `time_to_eose` and the EOSE initial-sync gate require routing relay EOSE
  notifications into the adapter (the SDK forwarder currently observes only `Event`). That is a follow-up capability.
- `observed_reorg_rate` is recorded by the engine against settle outcomes and is out of scope for the adapter.

## Phasing

1. **Cross-relay arrival spread (foundational).** Self-contained in the transport adapter; no protocol change. Gives the
   steady-state quiescence distribution and relay-redundancy health. This is the first increment.
2. **EOSE plumbing and per-relay timing.** Routes EOSE into the adapter, enabling the initial-sync gate and per-relay
   first-event / EOSE latencies.
3. **Engine-side reorg-rate telemetry.** Closes the quiescence loss function by measuring post-settle reorgs.
4. **Local adaptive quiescence.** Consumes (1) and (3) to compute a local value above the negotiated floor.
5. **Set reconciliation (NIP-77).** Fetch-completeness backstop for backdated late commits.

Each phase is independently useful and independently shippable, and (1) de-risks the rest by telling us what our relay
population actually looks like before we commit to (4) or (5).

## Open questions

- What percentile and margin of `cross_relay_spread` give an acceptable post-settle reorg rate in practice? Phase 1
  produces the data to answer this; the value is empirical, not assumed.
- Should the initial-sync gate require EOSE from *all* subscribed relays, or a quorum of them, when one relay is
  persistently silent? A strict all-relays gate lets one dead relay stall sync.
- Is the per-sender counter worth its equivocation surface, or does NIP-77 reconciliation make it redundant for the
  gap-detection use it targets?
