# Group state

Status: draft for internal review.

Each Marmot group has one canonical MLS state at a time. A client MAY temporarily hold candidate or pending state, but
only one state is visible as the group's canonical state.

## Lifecycle states

The group lifecycle has five states:

- `Stable`: the group has a canonical MLS epoch. Normal inbound processing and outbound work MAY proceed.
- `PendingPublish`: the client has prepared a local group-state commit, but has not confirmed that the required bytes
  were published.
- `Merging`: publication was confirmed, and the client is applying the staged commit to its local canonical state.
- `Recovering`: the client detected a fork-shaped conflict and is trying to select a safe branch from retained state.
- `Unrecoverable`: the client cannot safely select a branch from its retained local material.

`Unrecoverable` is local to one client. It does not mean the Marmot group is dead. It means that this client MUST repair
its group state, restore retained material, rejoin, or discard the local group copy before it can safely send or apply
more group traffic.

`Stable` is the only state where a client MAY prepare a new local group-state commit. Outbound app payloads are also
held while convergence input is unresolved, because they MUST be encrypted against the selected canonical state.

A member that has sent a SelfRemove proposal also enters the local `Leaving` gate defined in
[member-departure.md](./member-departure.md). `Leaving` is not a canonical group lifecycle state: the MLS group state
still contains the member until a commit removes it. It is a durable outbound restriction on the leaving client and may
span multiple epoch-bound SelfRemove proposals.

## Legal transitions

```text
Stable
  -> PendingPublish      local group-state commit prepared
PendingPublish
  -> Merging             publish obligation confirmed
PendingPublish
  -> Stable              publish obligation failed or was abandoned
Merging
  -> Stable              staged commit applied
Stable
  -> Recovering          fork detected and retained recovery is required
Recovering
  -> Stable              a canonical branch was selected and applied
Recovering
  -> Unrecoverable       no safe branch can be selected from retained local material
Unrecoverable
  -> Stable              state was repaired, restored, or replaced by a verified join
```

Fork detection runs only from `Stable`, against settled canonical state. There is no `Merging -> Recovering` edge: a
competing branch observed while the client is applying its own confirmed commit is retained, the merge completes to
`Stable`, and fork detection then runs from `Stable`. `Recovering` re-entry is implicit: convergence-relevant input
that arrives while the group is already in `Recovering` is folded into the same recovery pass, and the group stays in
`Recovering` until a branch is selected and applied (`-> Stable`) or no safe branch exists (`-> Unrecoverable`).

A client MUST reject a local group-state commit while the group is in `PendingPublish`, `Merging`, `Recovering`, or
`Unrecoverable`.

Inbound group messages MAY be retained in any non-`Stable` state. Whether retained inbound may change canonical group
state depends on the state:

- during `PendingPublish` and `Merging`, retained inbound MUST NOT be applied to canonical group state;
- during `Recovering`, retained inbound is replayed only as candidate material for convergence; canonical group state
  changes only when a selected branch is applied (see below);
- during `Unrecoverable`, retained inbound MUST NOT be applied to canonical group state until a verified repair path
  restores, repairs, or replaces the local group state.

While a group is in `Recovering`, a client MAY process or reprocess retained input to build candidate branches, score
them, and select a canonical branch. That processing MUST NOT release outbound work or emit delivered app payloads until
the selected branch has been applied and the lifecycle returns to `Stable`.

## Unrecoverable cases

A client enters `Unrecoverable` when it cannot determine the canonical branch without violating the group's retention or
validation rules.

Examples include:

- the client needs a retained state inside the rollback horizon, but that state is missing;
- the client cannot validate any candidate branch from the retained anchor;
- local group state is corrupted and cannot validate the retained commit path.

A client in `Unrecoverable` MUST NOT choose the current local state merely because it is the only state available. It
MUST stop applying group-state changes until it has a verified repair path.

A repair path MAY restore retained state, import a verified current snapshot, rejoin through MLS, or use another
recovery method defined by a future protocol-core document.

## Convergence status

Convergence has a separate derived status:

- `Syncing`: convergence-relevant input is still arriving or the quiescence window defined in
  [convergence.md](./convergence.md) has not elapsed since the last retained or reclassified convergence-relevant input.
- `Resolving`: the quiescence window has elapsed, but the client still has unresolved convergence work, such as a child
  commit whose parent has not been retained or fetched yet.
- `Settled`: candidate processing reached a fixed point and the selected branch, if any, has been applied.
- `Blocked`: candidate processing cannot safely continue without a repair path or missing retained material.

Convergence status is derived from stored input and policy. It is not a claim made by the transport.

The lifecycle state is authoritative; convergence status is a derived view of how convergence is progressing within it.
The legal combinations are:

| Convergence status | Lifecycle states it can appear in | Notes                                                                 |
| ------------------ | --------------------------------- | --------------------------------------------------------------------- |
| `Syncing`          | `Stable`, `Recovering`            | input still arriving or quiescence not elapsed                        |
| `Resolving`        | `Stable`, `Recovering`            | quiescence elapsed, work outstanding (e.g. a child commit's parent)   |
| `Settled`          | `Stable`                          | fixed point reached and any selected branch applied                   |
| `Blocked`          | `Recovering`, `Unrecoverable`     | needs a repair path or missing retained material                      |

Two couplings follow from this table. A group leaves `Recovering` for `Stable` only after convergence reaches
`Settled` (a selected branch was applied). A `Blocked` convergence status that cannot be cleared by retained material
is the `Unrecoverable` condition: when recovery has no safe branch and no repair path, the lifecycle moves to
`Unrecoverable`. `PendingPublish` and `Merging` are local-publish states, not convergence passes, so convergence status
is not meaningful while the group is in them.

## Local actions during convergence

When convergence status is `Syncing`, `Resolving`, or `Blocked`, a client SHOULD queue local outbound intents instead
of preparing them against a state that MAY lose branch selection or require repair.

Queued app-payload sends are encrypted after convergence status reaches `Settled` and the lifecycle state allows
outbound work.

Queued group-state changes are regenerated after convergence status reaches `Settled` and the lifecycle state allows
outbound work. A staged commit created before branch selection MUST NOT be reused after convergence changes the
canonical state.

## Participation

The lifecycle states and convergence status above describe how a client converges on the group's canonical MLS state.
They do not describe whether the local identity is still a live member of that group. That is a separate, orthogonal
dimension: a group can be `Stable` and `Settled` and yet no longer include the local identity.

Participation has four states:

- `Member`: the local identity is present in the group's canonical roster. This is the only participation state in
  which a client MAY prepare local group-state commits or emit delivered app payloads for the group.
- `Left`: the local identity voluntarily departed — its SelfRemove was committed (see
  [member-departure.md](./member-departure.md)). Non-member; the group is inactive for this identity.
- `Evicted`: the local identity was removed by another member. Non-member; the group is inactive for this identity.
- `Quarantined`: the group is excluded from live processing and from the live group set pending an explicit recovery
  transition. A quarantined group is neither trusted as a live member group nor asserted non-member; it is withheld.

`Left` and `Evicted` are kept distinct — mirroring the `MemberLeft` vs `MemberRemoved` distinction elsewhere — so a
surface can tell "you left" from "you were removed" without labeling one as the other. A client that does not need the
distinction MAY treat both as a single non-member state, but the protocol MUST preserve the reason.

Participation is orthogonal to the lifecycle state, but two couplings hold. `Left`, `Evicted`, and `Quarantined` are
terminal for normal processing the same way `Unrecoverable` is: a client MUST NOT apply group-state changes or release
outbound work while in them. Unlike `Unrecoverable` — a convergence failure the client MAY repair from retained
material — `Left` and `Evicted` reflect the canonical group's membership. They clear only through a verified rejoin or
reinstatement path — normally a new Welcome to a later epoch, or another explicit protocol-defined reinstatement. A
non-member client does not return to `Member` by resuming normal in-group processing: it was removed from the ratchet,
so it cannot apply a later commit for that group, and it MUST NOT try. Reinstatement returns the identity to `Member`
through a fresh membership grant, not through the group's own inbound stream.

### Reaching a non-member state

Removal authority lives in exactly one artifact: the commit that removes the identity. A client transitions to
`Left`/`Evicted` only by applying that commit — its own SelfRemove committed resolves to `Left`, a peer's removal to
`Evicted`; the roster diff after merging shows self in the removed set. No other input is authoritative: not an
undecryptable message, not a transport claim, not an out-of-band notice. Everything else in this section is a
discovery mechanism whose only job is to get that one commit delivered and applied. The removed identity can always
still process it: the removal commit is protected under the last epoch it was a member of, so it remains readable to
the removed member no matter how late it arrives.

The removal commit is not guaranteed to arrive in order. Transport timing or ordering can deliver later, post-removal
traffic first, and MLS gives **no** signal in that case: with the removal commit unmerged the group is still active,
and later traffic merely fails to decrypt — indistinguishable at the MLS layer from future-epoch or corrupt input.
(The MLS `UseAfterEviction` guard is not this signal: it fires only after a merged removal has already made the group
inactive — aftermath of the applied-removal path, not a fresh discovery.) A client MUST therefore treat undecryptable
group traffic as a possible missed-removal symptom and actively pursue delivery of the missing commit:

1. **Recovery probe.** Undecryptable traffic for a group MUST trigger the active transport's missed-input recovery
   mechanism, bounded around the last input the client successfully consumed, looking for group-evolution input its
   retained candidate keys can open. Every transport binding states its recovery mechanism — or the delivery
   guarantees under which group-evolution input cannot be missed, in which case this trigger never fires (see
   [../transports/README.md](../transports/README.md), "Transport document checklist"). If the missing removal commit
   is recovered, it is applied through the ordinary inbound flow and the applied-removal transition above fires —
   late, but identically.
2. **Removal notice.** On a transport binding with a recipient inbox address, the committer of a removal SHOULD also
   send each removed member a removal notice through the member's account inbox, carrying or referencing the removal
   commit (see [member-departure.md](./member-departure.md), "Removal notices"; the binding defines the shape). A
   notice has no authority of its own: the receiver resolves it by validating and applying the carried or fetched
   commit through the ordinary inbound flow, and a client MUST NOT change participation on an unverified notice. A
   notice that does not resolve to a valid commit removing the local identity is ignored. Because a forged notice can
   at most cause a validation attempt, an adversary gains nothing a real removal would not already grant.
3. **Bounded hold.** When undecryptable traffic persists and the probes above have stayed dry past a local policy
   bound, the client MUST move the group to `Quarantined` with the `pending_membership` reason (see "Quarantine"
   below): withheld, still probing, asserting neither `Member` nor a non-member state. It leaves that hold only when
   the removal commit (or other group-evolution input that restores decryption) arrives — never by guessing.

The `Left` vs `Evicted` reason comes only from the applied removal commit, so every route preserves it. A client MUST
NOT fabricate a reason it has not read from an applied commit.

This design accepts an irreducible limit: a removed client that receives nothing at all — no later traffic, no notice —
is indistinguishable from a member of a quiet group, and no mechanism at any layer can distinguish them. The guarantee
is therefore eventual, not immediate: participation resolves to `Left`/`Evicted` once the removal commit is delivered
by any route, and a client keeps the discovery mechanisms above active rather than leaving a dead group readable as
`Member` indefinitely.

### Quarantine

A client places a group in `Quarantined` when it cannot safely treat the group as live but holds no applied removal
commit. Quarantine is a hold, not a verdict about membership, and it carries a reason so surfaces and recovery flows
can tell the holds apart — the two reasons have opposite expected exits:

- `pending_membership`: undecryptable traffic suggests the local identity may have been removed, and the discovery
  probes in "Reaching a non-member state" have not yet recovered the removal commit. The expected exit is resolution:
  the removal commit arrives and the group transitions to `Left`/`Evicted`, or recovered group-evolution input
  restores decryption and the group returns to `Member`.
- `integrity_hold`: stored group material fails to load or validate, or a durable invariant check fails. The expected
  exit is repair: a verified repair path returns the group to live processing, or resolves it to a non-member state.

While a group is `Quarantined`, under either reason:

- it MUST be excluded from the live group set and from live inbound and convergence processing;
- every group accessor MUST agree that the group is withheld: a client MUST NOT expose a durable roster through one
  accessor while another accessor reports the group as unknown. Either all live accessors reflect the quarantine, or the
  group is exposed only through an explicit quarantine accessor;
- the group MUST NOT return to live processing except through an explicit recovery transition. Ordinary inbound or
  convergence input MUST NOT silently re-activate a quarantined group.

### Participation and public surfaces

Public group APIs MUST let a caller distinguish a live member group, a non-member group, `Quarantined`, and "no such
group" from one another. Because `Left`/`Evicted` are reached only by applying the removal commit, a non-member state
always carries its reason; a group whose membership is merely in doubt is reported as `Quarantined` with its quarantine
reason (`pending_membership` vs `integrity_hold`) rather than being assigned a participation it cannot prove.
Collapsing a non-member or `Quarantined` group into either "active member" or "unknown group" is a defect: the first
keeps a dead group usable; the second loses the fact that the group existed and why it is no longer live.
