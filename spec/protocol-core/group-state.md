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

Removal authority in MLS is carried only by the commit that removes the identity. A client reaches `Left`/`Evicted`
through one of two paths, and a correct client handles both:

1. **Applied removal.** The client applies the commit that removes the local identity (its own SelfRemove for `Left`, a
   peer's removal for `Evicted`); the roster diff after merging that commit shows self in the removed set. This is the
   clean path, and it is the only path on which MLS itself changes membership.
2. **Removal commit not applied.** Relay timing or ordering meant the client never applied that specific removal commit
   and instead sees a later, post-removal message. MLS gives **no** eviction signal here: with the removal commit
   unmerged the group is still active, and the later message merely fails to decrypt as a wrong-epoch / no-matching-secret
   message — indistinguishable at the MLS layer from a future-epoch or corrupt message. The MLS `UseAfterEviction` guard
   is **not** this signal: it fires only once the group is already inactive from a merged self-removal, i.e. it is
   path-1 aftermath, not a fresh discovery. A client MUST therefore derive non-membership **above MLS** in this case,
   by either obtaining and applying the missing removal commit (convergence backfill, after which path 1 applies) or
   inferring non-membership from authenticated roster / relay state. A client MUST NOT leave the group readable as
   `Member` indefinitely merely because the removal commit was reordered.

Because the removal commit is not guaranteed to arrive before a post-removal message, path 2 is a required fallback, not
an edge case. A client that surfaces removal only on path 1 will silently keep a dead group active. The mechanism for
path 2 lives above the MLS layer; the disposition of the undecryptable post-removal message itself follows the ordinary
inbound rules (deferred while the missing commit may still be fetched, terminal only when it cannot).

### Quarantine

A client places a group in `Quarantined` when it cannot safely treat the group as live but has no authoritative eviction
signal — for example, stored group material fails to load or validate, or a durable invariant check fails. Quarantine is
a hold, not a verdict about membership.

While a group is `Quarantined`:

- it MUST be excluded from the live group set and from live inbound and convergence processing;
- every group accessor MUST agree that the group is withheld: a client MUST NOT expose a durable roster through one
  accessor while another accessor reports the group as unknown. Either all live accessors reflect the quarantine, or the
  group is exposed only through an explicit quarantine accessor;
- the group MUST NOT return to live processing except through an explicit recovery transition. Ordinary inbound or
  convergence input MUST NOT silently re-activate a quarantined group.

### Participation and public surfaces

Public group APIs MUST let a caller distinguish a live member group, a non-member group (`Left` / `Evicted`, with the
reason preserved), `Quarantined`, and "no such group" from one another. Collapsing a non-member or `Quarantined` group
into either "active member" or "unknown group" is a defect: the first keeps a dead group usable; the second loses the
fact that the group existed and why it is no longer live.
