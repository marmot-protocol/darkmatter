---
title: "HTTP Single-Server Transport — Design Note"
created: 2026-06-06
updated: 2026-06-06
tags: [marmot, overview, http, transport, reduced-assurance]
status: working-note
---

# HTTP Single-Server Transport — Design Note

This note designs a hypothetical HTTP transport binding in which **one server takes real responsibility**: per-group
and per-recipient message ordering, durable delivery queues, and KeyPackage storage and handout — the full delivery
stack. It is a deliberate trade: clients give up metadata privacy and depend on a single operator in exchange for
operational simplicity.

It is written against the four-component boundary in
[`target-architecture.md`](./target-architecture.md) and reuses the order-tolerant convergence model in
[`../distributed-convergence.md`](../distributed-convergence.md). The near-term repository priority remains the CGKA
engine; this note exists to scope the work, not to schedule it.

Two framing decisions are fixed up front and shape everything below:

1. **Reduced-assurance profile.** The single server is a single point of failure and a metadata observer. Rather than
   weaken the protocol-wide redundancy guarantee, this binding is documented as a *reduced-assurance transport
   profile* — explicitly non-conformant with the redundant-delivery principle, opt-in, with its trade-offs stated.
2. **Ordering is delivery convenience + commit admission only.** The server may sequence delivery and admit at most one
   commit per epoch, but its ordering is **never** authoritative over CGKA state. Clients still run convergence and
   ignore server order for correctness. We do **not** change convergence to consume server sequence numbers.

## What does not change

The engine was built to not trust the transport, so the protocol core is untouched.

- **Convergence.** Branch selection stays content-derived — depth, then witness quorum, then `SHA-256(mls_bytes)`.
  Transport arrival order, timestamps, and event ids MUST NOT participate (see
  [`../distributed-convergence.md`](../distributed-convergence.md) and `spec/protocol-core/convergence.md`). A
  well-behaved server reduces buffering and branching in practice but cannot define group state.
- **Inbound processing.** Dedup by stable `MessageId` derived from transported bytes; stale/deferred/buffered
  classification unchanged (`spec/protocol-core/inbound-processing.md`).
- **Identity.** A Marmot account is still a Nostr public key (foundation layer). The HTTP server authenticates
  connections but is not an identity authority.
- **KeyPackage trust.** KeyPackages still carry the account-identity-proof extension (`0xf2f1`). Clients validate the
  proof, ciphersuite, capabilities, and transport validity on fetch. The server is a directory, not a voucher
  (`spec/foundation/key-packages.md`).
- **Two-layer message model.** Group messages keep an outer transport encryption over the CGKA ciphertext; welcomes
  keep recipient-addressed encryption.

The only protocol-document conflict is the redundant-delivery principle.

> `spec/principles.md`: "Marmot transports MUST support redundant delivery so a group does not depend on one server,
> relay, or endpoint."

A single server violates this by construction. Resolution (decision 1): keep the principle as written and classify
this binding as a **reduced-assurance profile** in `spec/transports/http.md`, with the availability and metadata
trade-offs stated inline. The principle stays meaningful for conformant deployments; this profile is an explicit,
labeled exception.

## The server's role: ordering authority, carefully bounded

The premise is a server with "ordering authority." The protocol allows exactly two well-defined forms of that, both of
which are safe because clients still verify:

### Delivery convenience

The server assigns a monotonic per-group sequence and a per-recipient queue cursor, delivers in that order, dedupes by
`MessageId`, and persists until acknowledged. This is liveness and ordering *hint* machinery. The engine treats the
sequence as opaque transport evidence (like `TransportMessage.timestamp` today) and never feeds it into convergence.

### Commit admission

The server may enforce **at most one accepted commit per source epoch**: when two clients race a commit on the same
epoch, the server accepts the first to land and rejects the second with a "stale epoch, re-commit" response. The loser
re-derives a commit on the new epoch.

This is the practical payoff of an authoritative server. It collapses the common concurrent-commit case to the happy
path, so convergence rarely has to arbitrate branches. It is still only a *policy/optimization* layer:

- A buggy or malicious server can cause extra re-commits, buffering, or denial of service — **never a group fork**,
  because clients independently run convergence and require witness quorum / content-derived branch selection.
- Admission decisions are not inputs to convergence. If the server is wrong, clients recover; they do not adopt the
  server's choice as truth.

### What the server explicitly is NOT

It is not authoritative over CGKA state. We do not add a server-sequence field to the convergence inputs, and we do not
let the server's accept/reject decide which branch is canonical. Doing so would be a convergence rewrite and a
trust-model downgrade; it is out of scope by decision 2.

## What gets built

Three components plus a spec binding. All HTTP-specific code stays out of `cgka-engine`, `crates/traits`, and the
storage crates, per the workspace invariants.

### 1. `transport-http-adapter` — a `TransportAdapter`

Maps the five `TransportAdapter` methods (`crates/traits/src/transport_adapter.rs`) onto HTTP/WebSocket/SSE instead of
relays. It moves opaque blobs only; it never peels MLS or touches convergence, and `TransportDeliverySource` metadata
stays diagnostic-only.

| Trait method | HTTP binding |
| --- | --- |
| `activate_account` | authenticate to the server, open the inbox stream, register group subscriptions, resume from a stored cursor |
| `sync_account_groups` | update the server-side subscription set for the active group set |
| `deactivate_account` | tear down subscriptions and the connection |
| `publish` | `POST` the opaque `TransportMessage` to the group or inbox endpoint; map per-endpoint receipts into `TransportPublishReport` |
| `receive` | yield the next delivery from the inbox/group stream |

The four `TransportDeliveryPlane`s (`transport_adapter.rs`) map directly:

- `AccountInbox` → the welcome / recipient-addressed queue
- `Group` → the per-group queue
- `Discovery` → the KeyPackage directory
- `Ephemeral` → previews (keep the existing QUIC stream/broker, or add an HTTP fallback later)

The genuinely new surface versus Nostr is a **stateful client↔server session**: connection auth, a resumable
cursor/ack protocol, and backpressure — because there is now a durable queue with delivery state, not fire-and-forget
relay pub/sub. The adapter should lean on the HTTP/WS client library's native reconnect/backoff, mirroring how the
Nostr adapter delegates connection lifecycle to its SDK.

### 2. `transport-http-peeler` — a `TransportPeeler`

The peeler is the transport+CGKA pair seam (`crates/traits/src/peeler.rs`). The Nostr peeler's outer encryption is not
Nostr-specific cryptography — it is `ChaCha20-Poly1305` keyed by `MLS-Exporter("marmot", "group-event", 32)` with empty
AAD (`spec/transports/nostr.md`). What is Nostr-specific is the kind-445 event wrapper and fresh-ephemeral-key signing.
So the HTTP peeler is largely the Nostr peeler with the Nostr envelope removed:

- **Group messages.** Keep the same outer `ChaCha20-Poly1305(group_event_key, …)` wrap. Retain it even with a trusted
  server, because it denies the server the MLS framing metadata (epoch, content type). Drop ephemeral Nostr signing and
  the kind-445 shape; use a plain length-prefixed frame. Epoch selection stays the same trial-decrypt over retained
  candidate exporter secrets — there is no epoch hint in the envelope, and the server is not allowed to supply one as a
  convergence input.
- **Welcomes.** Nostr uses NIP-59 gift-wrap to hide both sender and recipient from relays. On a single server the
  recipient is necessarily known (the server queues per recipient), but **sender-hiding still matters** — the server
  should learn "a welcome for Bob" and not who sent it. Use a smaller sender-blind wrap addressed to the recipient
  rather than full NIP-59.
- **`MessageId`.** Must stay content-addressed and identical to what the engine derives, so the server's dedup and the
  engine's dedup agree on the same id.

Per the invariants, this is one peeler for the HTTP+MLS pair; it does not try to be transport-generic.

### 3. The server (outside this repository's trust boundary)

- **Durable queues.** Per-recipient inbox queue and per-group queue, each with a monotonic sequence, ack/cursor
  semantics, and retention windows that respect the engine's anchor/retention bounds (so the server does not retain
  past `beyond-anchor` / `beyond-retention`).
- **Commit-admission policy.** Optional but the main reason to run an authoritative server (see above).
- **KeyPackage directory.** Replaces Nostr's kind-`30443` addressable events plus kind-`10051` relay lists
  (`crates/transport-nostr-adapter/src/key_package.rs`). Endpoints to publish a KeyPackage pool, fetch-and-consume a
  one-time KeyPackage, and serve a last-resort KeyPackage when the pool is exhausted. This is the classic MLS Delivery
  Service KeyPackage store. The server tracks consumption but does not vouch identity — clients validate the identity
  proof on fetch.
- **Connection auth.** Identity remains the Nostr pubkey; a signature-challenge handshake against that key is the
  natural fit. No new identity layer.

### 4. `spec/transports/http.md` — the binding document

A sibling to `spec/transports/nostr.md`, documenting: the reduced-assurance profile and its trade-offs, queue and
ordering/ack semantics, the commit-admission contract, the KeyPackage directory API, welcome addressing and
sender-blinding, connection auth, and `MessageId` derivation. Add the `AGENTS.md` + `CLAUDE.md` sibling symlink and a
`spec/AGENTS.md` table entry, per the workspace conventions.

## Privacy delta

Against a Nostr relay swarm, ephemeral outer keys mean a relay sees a group `h`-tag and unlinkable ephemeral pubkeys
but cannot cheaply link senders to accounts or enumerate membership. A single server that performs inbox queueing,
KeyPackage handout, and per-group fanout learns, by construction:

- the full social graph — group membership, who messages whom, message timing and volume, and who fetched whose
  KeyPackage (a strong signal of an imminent invite);
- all of it tied to one operator and one availability domain.

The outer encryption and sender-blind welcomes keep **content and authorship** from the server, and the MLS layer means
it can neither read nor forge group state. The cost is **metadata and membership** exposure — a materially larger leak
than the relay model, and the core of what users trade away in this profile. `spec/transports/http.md` must state this
plainly so the trade-off is an informed choice.

## Summary

- Protocol core (convergence, inbound processing, identity, KeyPackage trust): **no change**.
- Redundant-delivery principle: **kept**; this binding is a labeled reduced-assurance profile (decision 1).
- Server ordering: **delivery convenience + commit admission only**, never authoritative over CGKA state (decision 2).
- Build: `transport-http-adapter`, `transport-http-peeler` (largely a reshape of the Nostr peeler), the server with its
  KeyPackage directory, and the `spec/transports/http.md` binding doc.
- Users trade away metadata and membership privacy plus single-operator availability — not message confidentiality or
  integrity.
