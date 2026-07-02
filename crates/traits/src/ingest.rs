//! Typed outcomes from [`crate::engine::CgkaEngine::ingest`] plus the
//! peeled-message intermediate form.
//!
//! `IngestOutcome` separates applied messages from classifiable stale cases.
//! Hard errors stay in `EngineError`; stale routing, dedupe, and epoch cases
//! remain ordinary outcomes.

use crate::transport::TransportMessage;
use crate::types::{EpochId, GroupId, MemberId, MessageId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestOutcome {
    /// Message was validated, applied to the group's MLS state, and any
    /// resulting `GroupEvent`s were enqueued for `drain_events`.
    Processed,
    /// Message was accepted into durable storage but not yet applied because
    /// the group is temporarily not ingestible, usually during a local
    /// publish-before-apply transition. The engine replays buffered messages
    /// when the group returns to `Stable`.
    Buffered { group_id: GroupId, epoch: EpochId },
    /// Message was not applied. The variant names why — callers log by
    /// category rather than pattern-matching error strings.
    Stale { reason: StaleReason },
}

/// Why an inbound message was not processed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StaleReason {
    /// The engine has already seen this `MessageId`. Coordinator dedup.
    AlreadySeen,
    /// The engine is already at or past the message's epoch. Commonly hit
    /// when a commit arrives after a welcome that already advanced the
    /// recipient.
    AlreadyAtEpoch {
        current: EpochId,
        msg_epoch: EpochId,
    },
    /// A welcome addressed to a member other than ourselves.
    NotForThisClient,
    /// No local group matches this message's routing.
    UnknownGroup,
    /// The message is our own commit echoed back by the transport.
    OwnEcho,
    /// The peeler rejected the message. The stored message may be terminal or
    /// retryable depending on whether the engine has evidence that another
    /// epoch context could later peel it.
    PeelFailed,
    /// The local identity is no longer a member of this group (participation
    /// `Left` or `Evicted`, or the live MLS state is inactive from a merged
    /// removal): further inbound can no longer affect the group and is stale
    /// with the `evicted` category (spec/foundation/errors.md). Reaching the
    /// non-member state itself is a participation transition, never derived
    /// from this classification.
    Evicted,
    /// The input's source epoch falls outside every retained interval during
    /// which the local identity was a member — terminal by design: this
    /// client can never hold the keys (spec/foundation/errors.md,
    /// `PreMembership` -> `pre_membership`). Never used for groups without
    /// retained membership history, and never retried.
    PreMembership,
    /// The group is withheld (`GroupParticipation::Quarantined`): excluded
    /// from live inbound processing pending an explicit recovery transition
    /// (spec/protocol-core/group-state.md, "Quarantine"). The input is
    /// retained (`Retryable`) so the resolution pass can still consume it —
    /// withheld, not discarded.
    Quarantined,
}

/// Decrypted inbound message ready for engine processing.
///
/// Produced by [`crate::peeler::TransportPeeler::peel_group_message`] /
/// [`crate::peeler::TransportPeeler::peel_welcome`]. The `kind` field is the
/// structural discriminator — application messages, MLS commits, welcomes,
/// etc.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeeledMessage {
    pub id: MessageId,
    pub group_id: Option<GroupId>,
    pub sender: Option<MemberId>,
    pub content: PeeledContent,
    pub origin: TransportMessage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeeledContent {
    /// Inner MLS message (commit, application, proposal, etc.) — engine
    /// decides how to apply.
    MlsMessage { bytes: Vec<u8> },
    /// Welcome payload (MLS welcome bytes).
    Welcome { bytes: Vec<u8> },
    /// A removal notice: an inbox-delivered carrier for a group message the
    /// removed member may have missed — most importantly its own removal
    /// commit (spec/protocol-core/member-departure.md, "Removal notices").
    /// The notice has no authority of its own: the engine re-injects the
    /// embedded, transport-validated group message into the ordinary inbound
    /// pipeline, and only an applied removal commit changes participation.
    RemovalNotice { embedded: TransportMessage },
}
