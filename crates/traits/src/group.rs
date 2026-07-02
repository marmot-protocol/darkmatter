//! `Group` and `Member` records as seen by storage.
//!
//! **Invariant (enforced at trait-definition time):** neither [`Group`] nor
//! [`Member`] contains any transport-layer types. No `nostr_group_id`, no
//! relay URLs, no FIPS mesh ids. That mapping lives in the transport adapter
//! (see `docs/marmot-architecture/further-context/cgka-engine-design.md:247-268`).

use crate::capabilities::GroupCapabilities;
use crate::types::{EpochId, GroupId, MemberId};
use serde::{Deserialize, Serialize};

/// A group, as storage sees it. Mirrors the engine's view of the group's
/// metadata — not the MLS tree (OpenMLS owns that).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    pub id: GroupId,
    pub name: String,
    pub description: String,
    pub epoch: EpochId,
    pub members: Vec<Member>,
    pub required_capabilities: GroupCapabilities,
    /// The local identity's participation in this group (see
    /// `spec/protocol-core/group-state.md`, "Participation"). Lives on the
    /// durable record — not the MLS tree — so it survives live-OpenMLS-state
    /// teardown after a removal, and so fork-recovery snapshot rollback
    /// restores it together with the rest of the group state (a rolled-back
    /// removal commit must also roll back the non-member transition).
    /// Defaults to `Member` for records written before this field existed.
    #[serde(default)]
    pub participation: GroupParticipation,
    /// Epoch intervals during which the local identity was a member (see
    /// [`MembershipInterval`]). Maintained by the participation transitions;
    /// empty means "no retained history" for legacy records, which fails open
    /// in [`membership_intervals_contain`].
    #[serde(default)]
    pub membership_intervals: Vec<MembershipInterval>,
}

/// One member of a group, as storage sees it.
///
/// `id` is the stable cross-epoch identifier (signature public key). The MLS
/// leaf index is **not** stored here — it changes as the tree mutates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub id: MemberId,
    pub credential: Vec<u8>,
}

/// One epoch interval during which the local identity was a member of a
/// group. Because a group may be left/removed and later rejoined, membership
/// is a set of intervals, not a single boundary
/// (spec/foundation/errors.md, `PreMembership`). `ended_at` is `None` for the
/// currently-open interval and holds the epoch the removing commit reached
/// once membership ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipInterval {
    pub joined_at: EpochId,
    pub ended_at: Option<EpochId>,
}

/// Whether `epoch` falls inside any retained membership interval.
///
/// An EMPTY interval set means "no retained membership history" (legacy
/// records) and fails open: without history a client MUST NOT classify input
/// as `PreMembership` (spec/foundation/errors.md scopes the outcome to groups
/// the client has membership history for).
pub fn membership_intervals_contain(intervals: &[MembershipInterval], epoch: EpochId) -> bool {
    if intervals.is_empty() {
        return true;
    }
    intervals.iter().any(|interval| {
        epoch >= interval.joined_at && interval.ended_at.is_none_or(|ended| epoch <= ended)
    })
}

/// The local identity's participation in a group — a dimension orthogonal to
/// the convergence lifecycle (`Stable`/`Recovering`/`Unrecoverable`/…).
///
/// This is the shared vocabulary for the group participation states defined in
/// `spec/protocol-core/group-state.md`. Ingest, convergence, and public group
/// accessors map to it so a caller can tell a live group from one this identity
/// has been evicted from, or one being withheld pending recovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupParticipation {
    /// The local identity is present in the group's canonical roster; the only
    /// state in which local commits or delivered app payloads are allowed.
    #[default]
    Member,
    /// The local identity voluntarily departed (its SelfRemove was committed).
    /// Non-member; the group is inactive for this identity. Kept distinct from
    /// [`GroupParticipation::Evicted`] so a surface can tell "you left" from
    /// "you were removed".
    Left,
    /// The local identity was removed by another member. Non-member; the group
    /// is inactive for this identity. Reached only by applying the removal
    /// commit — delivered in order, recovered through the transport's
    /// missed-input recovery, or carried by a removal notice (see
    /// `spec/protocol-core/group-state.md`, "Reaching a non-member state") —
    /// never asserted from an undecryptable message or an MLS error.
    Evicted,
    /// The group is excluded from the live group set pending an explicit
    /// recovery transition; neither trusted as a live member group nor asserted
    /// non-member. Carries why it is withheld, because the two holds have
    /// opposite expected exits (see [`QuarantineReason`]).
    Quarantined { reason: QuarantineReason },
}

/// Why a group is held in [`GroupParticipation::Quarantined`]. The reason
/// determines the expected exit: `PendingMembership` resolves through the
/// removal commit (or recovered group-evolution input), `IntegrityHold`
/// through a verified repair path. Mirrors the quarantine reasons in
/// `spec/protocol-core/group-state.md`, "Quarantine".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuarantineReason {
    /// Undecryptable group traffic suggests the local identity may have been
    /// removed, and the discovery probes (transport missed-input recovery,
    /// removal notice) have not yet recovered the removal commit.
    PendingMembership,
    /// Stored group material failed to load or validate, or a durable
    /// invariant check failed.
    IntegrityHold,
}

#[cfg(test)]
mod membership_interval_tests {
    use super::{MembershipInterval, membership_intervals_contain};
    use crate::types::EpochId;

    #[test]
    fn empty_history_fails_open() {
        // No retained history: PreMembership MUST NOT be classified
        // (errors.md scopes it to groups with membership history).
        assert!(membership_intervals_contain(&[], EpochId(0)));
        assert!(membership_intervals_contain(&[], EpochId(999)));
    }

    #[test]
    fn intervals_cover_joins_gaps_and_rejoins() {
        // Member epochs 1..=1, removed, rejoined at 3 (still open).
        let intervals = [
            MembershipInterval {
                joined_at: EpochId(1),
                ended_at: Some(EpochId(1)),
            },
            MembershipInterval {
                joined_at: EpochId(3),
                ended_at: None,
            },
        ];
        assert!(
            !membership_intervals_contain(&intervals, EpochId(0)),
            "pre-join epochs are outside"
        );
        assert!(membership_intervals_contain(&intervals, EpochId(1)));
        assert!(
            !membership_intervals_contain(&intervals, EpochId(2)),
            "the removed gap is outside"
        );
        assert!(membership_intervals_contain(&intervals, EpochId(3)));
        assert!(
            membership_intervals_contain(&intervals, EpochId(50)),
            "an open interval extends forward"
        );
    }
}
