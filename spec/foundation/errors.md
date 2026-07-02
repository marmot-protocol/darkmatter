# Results and rejections

Status: draft for internal review.

Marmot clients SHOULD be able to describe why an input did not produce application content.

This file names shared categories. It does not require local APIs to use these exact enum names.

## Input categories

An input that does not produce application content SHOULD map to one of these categories:

- `duplicate`: the same protocol input was already seen.
- `own_echo`: the input is the client's own already-accounted-for output.
- `wrong_recipient`: the input targets another account, device, group, or routing id.
- `unknown_group`: the client has no group state that can process the input.
- `already_applied`: the input is represented by the current canonical state.
- `stale_epoch`: the input is from an epoch the client will not process.
- `invalid_encoding`: bytes failed the owning document's parser or length rules.
- `invalid_signature`: a required MLS, Nostr, or component signature check failed.
- `unsupported_required_feature`: the group requires a feature the client does not understand.
- `authorization_failed`: the sender or committer is not allowed to make the change.
- `missing_history`: the client would need retained state it no longer has.
- `evicted`: the input is for a group this identity is no longer a member of.
- `pre_membership`: the client has retained membership history for the group, and the input falls outside every interval
  during which this identity was a member, so it can never be decrypted. A group the client has no state for at all is
  `unknown_group`, not `pre_membership`.
- `transport_rejected`: publication or delivery failed at the transport layer.

Protocol-core docs can split these into more detailed outcomes when needed.

## Dispositions

Inbound protocol input that is structurally valid and authenticated — it parses, its required signatures verify, the
client supports its required features, and its sender is authorized — receives exactly one of four convergence
dispositions; the processing flow that assigns them is
[../protocol-core/inbound-processing.md](../protocol-core/inbound-processing.md):

- `accepted`: the input is part of, or was consumed by, the selected canonical branch.
- `deferred`: the input cannot be processed yet and is retried when missing state arrives or convergence advances.
- `stale`: the input can no longer affect the group.
- `invalidated`: the input's MLS application message decrypts only on a losing branch, so its app payload is withdrawn
  from application output.

Input that fails one of those gates does not reach convergence and receives no convergence disposition. It is rejected
before convergence — the `fail closed` path in
[../protocol-core/inbound-processing.md](../protocol-core/inbound-processing.md) — and is described by its category
alone: `invalid_encoding`, `invalid_signature`, `unsupported_required_feature`, or `authorization_failed`.
`transport_rejected` is likewise a publish/delivery outcome, not a convergence disposition. Every inbound input
therefore has exactly one outcome: a convergence disposition when it was structurally valid and authenticated,
otherwise a rejection category.

`delivered` is not a disposition. It names the application-visible output of an `accepted` MLS application message: the
Marmot app payload handed to the application. `dropped` is not a disposition either; where older drafts said an input
was dropped, this vocabulary says `stale`.

A disposition says what happened to an input. The categories above say why. A `stale` or `deferred` input SHOULD carry
a category, such as `duplicate` or `stale_epoch`.

## Named convergence outcomes

Protocol-core documents name some outcomes in `PascalCase`. Each maps to one disposition and one category:

| Outcome                 | Disposition | Category          | Defined in                                                  |
| ----------------------- | ----------- | ----------------- | ----------------------------------------------------------- |
| `BeyondAnchor`          | `stale`     | `stale_epoch`     | [retained-history.md](../protocol-core/retained-history.md) |
| `MissingRetainedAnchor` | `deferred`  | `missing_history` | [retained-history.md](../protocol-core/retained-history.md) |
| `PreMembership`         | `stale`     | `pre_membership`  | [inbound-processing.md](../protocol-core/inbound-processing.md) |

`BeyondAnchor` is window exclusion by design: the source epoch is older than the retained anchor, and the input will
never be processed. `MissingRetainedAnchor` is storage loss: required retained state inside the rollback horizon is
gone, canonical group state does not change, and the group moves to `Unrecoverable` (a group lifecycle state, not a
disposition) until a verified repair path exists; the input stays deferred rather than terminal.

Non-membership (`Left` / `Evicted`) is a participation state, not a convergence disposition — it is reached by applying
the removal commit or by deriving it above MLS, per [group-state.md](../protocol-core/group-state.md), not read off an
inbound message. The `evicted` category is only for classifying an inbound message that arrives for a group this
identity is no longer a member of; such input is `stale`.

`PreMembership` is terminal by design: the input falls outside every interval during which this identity was a member of
the group, so this client can never hold the keys to decrypt it. Because a group may be left/removed and later rejoined,
membership is a set of epoch intervals, not a single boundary; a client classifies an undecryptable historical message
against those retained intervals. Input inside a prior valid interval may still be recoverable from retained state and is
not `PreMembership`. It is scoped to groups the client has membership history for: with no retained state for the group
at all the input is `unknown_group`, not `PreMembership`. Unlike a deferred `MissingRetainedAnchor`, `PreMembership` MUST
NOT be deferred or retried.

## Protocol and local errors

Protocol rejections are part of interop. Local failures are not.

For example, `invalid_encoding` is a protocol rejection. A database write failure is a local implementation failure. A
transport publish failure matters to publish-before-apply, but the exact retry queue or error object is local.

## Privacy

Diagnostics for these outcomes MUST avoid account ids, group ids, message ids, relay URLs, pubkeys, payloads,
ciphertext, plaintext, and key material unless a document defines a safe redaction rule.
