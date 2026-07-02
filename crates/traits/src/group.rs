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

/// The local identity's participation in a group — a dimension orthogonal to
/// the convergence lifecycle (`Stable`/`Recovering`/`Unrecoverable`/…).
///
/// This is the shared vocabulary for the group participation states defined in
/// `spec/protocol-core/group-state.md`. Ingest, convergence, and public group
/// accessors map to it so a caller can tell a live group from one this identity
/// has been evicted from, or one being withheld pending recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupParticipation {
    /// The local identity is present in the group's canonical roster; the only
    /// state in which local commits or delivered app payloads are allowed.
    Member,
    /// The local identity voluntarily departed (its SelfRemove was committed).
    /// Non-member; the group is inactive for this identity. Kept distinct from
    /// [`GroupParticipation::Evicted`] so a surface can tell "you left" from
    /// "you were removed".
    Left,
    /// The local identity was removed by another member. Non-member; the group
    /// is inactive for this identity. Reached by applying the removal commit or
    /// by deriving non-membership above MLS when that commit was never applied
    /// (see `spec/protocol-core/group-state.md`) — not from an MLS error.
    Evicted,
    /// The group is excluded from the live group set pending an explicit
    /// recovery transition; neither trusted as a live member group nor asserted
    /// non-member.
    Quarantined,
}
