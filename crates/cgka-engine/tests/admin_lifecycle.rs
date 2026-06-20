//! `SendIntent::GrantAdmin` / `RevokeAdmin` / `TransferAdmin` round trips
//! (darkmatter#488).
//!
//! Covers the issue's test plan:
//! - Grant: admin grants to a member; the member is in the admin set after the
//!   commit ratifies on another client.
//! - Revoke: admin revokes another admin; removed from the admin set.
//! - Revoke last admin (non-empty group) → `LastAdminCannotResign`, no commit.
//! - Non-admin caller attempts grant/revoke → `NotGroupAdmin`, no commit.
//! - Transfer: admin transfers to another member; caller is no longer admin,
//!   target is admin, and another client converges to the same admin set.
//! - Sole-admin + sole-member revoke → `SoleMemberCannotRevoke`.
//! - Idempotent grant of an existing admin → `Noop`, no commit.
//! - Idempotent revoke of a non-admin → `Noop`, no commit.
//! - Grant / transfer targeting a non-member → `UnknownMember`.

use async_trait::async_trait;
use cgka_engine::feature_registry::FeatureRegistry;
use cgka_engine::{Engine, EngineBuilder};
use cgka_traits::EngineError;
use cgka_traits::capabilities::{Capability, CapabilityRequirement, Feature, RequirementLevel};
use cgka_traits::engine::{CgkaEngine, CreateGroupRequest, SendIntent, SendResult};
use cgka_traits::error::PeelerError;
use cgka_traits::group_context::GroupContextSnapshot;
use cgka_traits::ingest::{PeeledContent, PeeledMessage};
use cgka_traits::peeler::TransportPeeler;
use cgka_traits::transport::{
    EncryptedPayload, Timestamp, TransportEnvelope, TransportMessage, TransportSource,
};
use cgka_traits::types::{GroupId, MemberId, MessageId};
use sha2::{Digest, Sha256};
use storage_sqlite::SqliteAccountStorage;

mod support;
use support::proof_signer;

fn pad32(name: &[u8]) -> Vec<u8> {
    use k256::schnorr::SigningKey;
    let mut counter = 0u64;
    loop {
        let mut material = [0u8; 32];
        let mut hasher = Sha256::new();
        hasher.update(b"cgka-engine-test-identity-v1");
        hasher.update(name);
        hasher.update(counter.to_be_bytes());
        material.copy_from_slice(&hasher.finalize());
        if let Ok(sk) = SigningKey::from_bytes(&material) {
            return sk.verifying_key().to_bytes().to_vec();
        }
        counter += 1;
    }
}

fn pk32(name: &[u8]) -> [u8; 32] {
    <[u8; 32]>::try_from(pad32(name).as_slice()).unwrap()
}

fn hash_id(bytes: &[u8]) -> MessageId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    MessageId::new(h.finish().to_be_bytes().to_vec())
}

struct MockPeeler;

#[async_trait]
impl TransportPeeler for MockPeeler {
    async fn peel_group_message(
        &self,
        msg: &TransportMessage,
        _ctx: &GroupContextSnapshot,
    ) -> Result<PeeledMessage, PeelerError> {
        Ok(PeeledMessage {
            id: msg.id.clone(),
            group_id: None,
            sender: None,
            content: PeeledContent::MlsMessage {
                bytes: msg.payload.clone(),
            },
            origin: msg.clone(),
        })
    }

    async fn peel_welcome(&self, msg: &TransportMessage) -> Result<PeeledMessage, PeelerError> {
        Ok(PeeledMessage {
            id: msg.id.clone(),
            group_id: None,
            sender: None,
            content: PeeledContent::Welcome {
                bytes: msg.payload.clone(),
            },
            origin: msg.clone(),
        })
    }

    async fn wrap_group_message(
        &self,
        payload: &EncryptedPayload,
        _ctx: &GroupContextSnapshot,
    ) -> Result<TransportMessage, PeelerError> {
        Ok(TransportMessage {
            id: hash_id(&payload.ciphertext),
            payload: payload.ciphertext.clone(),
            timestamp: Timestamp(0),
            causal_deps: vec![],
            source: TransportSource("mock".into()),
            envelope: TransportEnvelope::GroupMessage {
                transport_group_id: vec![],
            },
        })
    }

    async fn wrap_welcome(
        &self,
        payload: &EncryptedPayload,
        recipient: &MemberId,
    ) -> Result<TransportMessage, PeelerError> {
        Ok(TransportMessage {
            id: hash_id(&payload.ciphertext),
            payload: payload.ciphertext.clone(),
            timestamp: Timestamp(0),
            causal_deps: vec![],
            source: TransportSource("mock".into()),
            envelope: TransportEnvelope::Welcome {
                recipient: recipient.clone(),
            },
        })
    }
}

fn registry() -> FeatureRegistry {
    let mut r = FeatureRegistry::new();
    r.register(
        Feature("self-remove"),
        CapabilityRequirement {
            requires: Capability::Proposal(10),
            level: RequirementLevel::Required,
            description: "MIP-03",
        },
    );
    r
}

fn build(id: &[u8]) -> Engine<SqliteAccountStorage> {
    EngineBuilder::new(SqliteAccountStorage::in_memory().unwrap())
        .identity(pad32(id))
        .account_identity_proof_signer(proof_signer(id))
        .feature_registry(registry())
        .peeler(Box::new(MockPeeler))
        .build()
        .unwrap()
}

/// Route an own-commit `TransportMessage` so its `transport_group_id` matches
/// `gid` (the `MockPeeler` wraps with an empty id).
fn route(commit: TransportMessage, gid: &GroupId) -> TransportMessage {
    TransportMessage {
        envelope: TransportEnvelope::GroupMessage {
            transport_group_id: gid.as_slice().to_vec(),
        },
        ..commit
    }
}

/// Settle a buffered inbound commit on the receiving engine.
fn converge(engine: &mut Engine<SqliteAccountStorage>, gid: &GroupId) {
    let result = engine
        .converge_stored_openmls_messages(gid, 1_000_000)
        .expect("buffered commit converges");
    assert_eq!(
        result.convergence_status,
        cgka_engine::canonicalization::ConvergenceStatus::Settled
    );
}

/// Apply `commit` to a receiver and settle.
async fn deliver(
    receiver: &mut Engine<SqliteAccountStorage>,
    commit: &TransportMessage,
    gid: &GroupId,
) {
    let routed = route(commit.clone(), gid);
    receiver.ingest(routed).await.unwrap();
    converge(receiver, gid);
}

/// Alice (sole admin) + Bob (member).
async fn create_pair() -> (
    Engine<SqliteAccountStorage>,
    Engine<SqliteAccountStorage>,
    GroupId,
) {
    let mut alice = build(b"alice");
    let mut bob = build(b"bob");
    let bob_kp = bob.fresh_key_package().await.unwrap();
    let (gid, create) = alice
        .create_group(CreateGroupRequest {
            name: "g".into(),
            description: "d".into(),
            members: vec![bob_kp],
            required_features: vec![],
            app_components: vec![],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    let (pending, welcomes) = match create {
        SendResult::GroupCreated { pending, welcomes } => (pending, welcomes),
        _ => unreachable!(),
    };
    alice.confirm_published(pending).await.unwrap();
    bob.join_welcome(welcomes.into_iter().next().unwrap())
        .await
        .unwrap();
    (alice, bob, gid)
}

/// Alice (sole admin) + Bob + Carol (members).
async fn create_trio() -> (
    Engine<SqliteAccountStorage>,
    Engine<SqliteAccountStorage>,
    Engine<SqliteAccountStorage>,
    GroupId,
) {
    let mut alice = build(b"alice");
    let mut bob = build(b"bob");
    let mut carol = build(b"carol");
    let bob_kp = bob.fresh_key_package().await.unwrap();
    let carol_kp = carol.fresh_key_package().await.unwrap();
    let (gid, create) = alice
        .create_group(CreateGroupRequest {
            name: "g".into(),
            description: "d".into(),
            members: vec![bob_kp, carol_kp],
            required_features: vec![],
            app_components: vec![],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    let (pending, welcomes) = match create {
        SendResult::GroupCreated { pending, welcomes } => (pending, welcomes),
        _ => unreachable!(),
    };
    alice.confirm_published(pending).await.unwrap();
    for welcome in welcomes {
        // Each welcome is addressed to a specific recipient; route to whoever
        // accepts it. join_welcome ignores welcomes not addressed to self.
        let _ = bob.join_welcome(welcome.clone()).await;
        let _ = carol.join_welcome(welcome).await;
    }
    (alice, bob, carol, gid)
}

// ── Grant ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn grant_admin_adds_member_to_admin_set() {
    let (mut alice, mut bob, gid) = create_pair().await;
    assert_eq!(alice.admin_pubkeys(&gid).unwrap().len(), 1);

    let res = alice
        .send(SendIntent::GrantAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"bob"),
        })
        .await
        .unwrap();
    let (commit, pending) = match res {
        SendResult::GroupEvolution { msg, pending, .. } => (msg, pending),
        other => panic!("expected GroupEvolution, got {other:?}"),
    };
    alice.confirm_published(pending).await.unwrap();

    let mut admins = alice.admin_pubkeys(&gid).unwrap();
    admins.sort();
    assert!(admins.contains(&pk32(b"alice")));
    assert!(admins.contains(&pk32(b"bob")));

    // Bob converges to the same admin set.
    deliver(&mut bob, &commit, &gid).await;
    let mut bob_admins = bob.admin_pubkeys(&gid).unwrap();
    bob_admins.sort();
    assert_eq!(admins, bob_admins);
    // Bob can now perform an admin-gated operation.
    assert!(
        bob.send(SendIntent::UpdateGroupData {
            group_id: gid.clone(),
            name: Some("bob-rename".into()),
            description: None,
        })
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn grant_admin_to_existing_admin_is_noop() {
    let (mut alice, _bob, gid) = create_pair().await;
    let res = alice
        .send(SendIntent::GrantAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"alice"),
        })
        .await
        .unwrap();
    assert!(matches!(res, SendResult::Noop { .. }));
    // Epoch did not advance.
    assert_eq!(alice.epoch(&gid).unwrap().0, 1);
    assert_eq!(alice.admin_pubkeys(&gid).unwrap().len(), 1);
}

#[tokio::test]
async fn grant_admin_to_non_member_is_unknown_member() {
    let (mut alice, _bob, gid) = create_pair().await;
    let err = alice
        .send(SendIntent::GrantAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"stranger"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::UnknownMember { .. }));
    assert_eq!(alice.epoch(&gid).unwrap().0, 1);
}

#[tokio::test]
async fn non_admin_cannot_grant() {
    let (_alice, mut bob, gid) = create_pair().await;
    let err = bob
        .send(SendIntent::GrantAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"bob"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::NotGroupAdmin { .. }));
}

// ── Revoke ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn revoke_admin_removes_from_admin_set() {
    let (mut alice, mut bob, gid) = create_pair().await;
    // Promote bob first.
    let grant = alice
        .send(SendIntent::GrantAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"bob"),
        })
        .await
        .unwrap();
    let (grant_commit, grant_pending) = match grant {
        SendResult::GroupEvolution { msg, pending, .. } => (msg, pending),
        other => panic!("expected GroupEvolution, got {other:?}"),
    };
    alice.confirm_published(grant_pending).await.unwrap();
    deliver(&mut bob, &grant_commit, &gid).await;
    assert_eq!(alice.admin_pubkeys(&gid).unwrap().len(), 2);

    // Now revoke bob.
    let revoke = alice
        .send(SendIntent::RevokeAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"bob"),
        })
        .await
        .unwrap();
    let (revoke_commit, revoke_pending) = match revoke {
        SendResult::GroupEvolution { msg, pending, .. } => (msg, pending),
        other => panic!("expected GroupEvolution, got {other:?}"),
    };
    alice.confirm_published(revoke_pending).await.unwrap();

    let admins = alice.admin_pubkeys(&gid).unwrap();
    assert_eq!(admins, vec![pk32(b"alice")]);
    deliver(&mut bob, &revoke_commit, &gid).await;
    assert_eq!(bob.admin_pubkeys(&gid).unwrap(), vec![pk32(b"alice")]);
}

#[tokio::test]
async fn revoke_non_admin_is_noop() {
    let (mut alice, _bob, gid) = create_pair().await;
    let res = alice
        .send(SendIntent::RevokeAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"bob"),
        })
        .await
        .unwrap();
    assert!(matches!(res, SendResult::Noop { .. }));
    assert_eq!(alice.epoch(&gid).unwrap().0, 1);
}

#[tokio::test]
async fn revoke_non_member_is_noop() {
    // #488 lists MemberNotFound for revoke, but a non-member is by definition
    // not an admin (admin-leaf coupling), so revoking a pubkey that is not a
    // current member — e.g. a mistyped key — is the same benign no-op as
    // revoking a known non-admin member, not a hard error. This pins that
    // intentional semantics (see do_send_revoke_admin in admin_lifecycle.rs).
    let (mut alice, _bob, gid) = create_pair().await;
    let res = alice
        .send(SendIntent::RevokeAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"never-a-member"),
        })
        .await
        .unwrap();
    assert!(matches!(res, SendResult::Noop { .. }));
    // No commit issued: epoch unchanged, admin set untouched.
    assert_eq!(alice.epoch(&gid).unwrap().0, 1);
    assert_eq!(alice.admin_pubkeys(&gid).unwrap(), vec![pk32(b"alice")]);
}

#[tokio::test]
async fn revoke_last_admin_in_non_empty_group_is_refused() {
    let (mut alice, _bob, gid) = create_pair().await;
    let err = alice
        .send(SendIntent::RevokeAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"alice"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::LastAdminCannotResign { .. }));
    // No commit issued: still admin, epoch unchanged.
    assert_eq!(alice.admin_pubkeys(&gid).unwrap(), vec![pk32(b"alice")]);
    assert_eq!(alice.epoch(&gid).unwrap().0, 1);
}

#[tokio::test]
async fn revoke_sole_admin_sole_member_is_distinct_error() {
    // Alice creates a group with no other members → sole admin + sole member.
    let mut alice = build(b"alice");
    let (gid, create) = alice
        .create_group(CreateGroupRequest {
            name: "solo".into(),
            description: "d".into(),
            members: vec![],
            required_features: vec![],
            app_components: vec![],
            initial_admins: vec![],
        })
        .await
        .unwrap();
    let pending = match create {
        SendResult::GroupCreated { pending, .. } => pending,
        _ => unreachable!(),
    };
    alice.confirm_published(pending).await.unwrap();

    let err = alice
        .send(SendIntent::RevokeAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"alice"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::SoleMemberCannotRevoke { .. }));
}

#[tokio::test]
async fn non_admin_cannot_revoke() {
    let (_alice, mut bob, gid) = create_pair().await;
    let err = bob
        .send(SendIntent::RevokeAdmin {
            group_id: gid.clone(),
            member_pubkey: pk32(b"alice"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::NotGroupAdmin { .. }));
}

// ── Transfer ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn transfer_admin_moves_role_to_member() {
    let (mut alice, mut bob, mut carol, gid) = create_trio().await;
    assert_eq!(alice.admin_pubkeys(&gid).unwrap(), vec![pk32(b"alice")]);

    let res = alice
        .send(SendIntent::TransferAdmin {
            group_id: gid.clone(),
            new_admin_pubkey: pk32(b"bob"),
        })
        .await
        .unwrap();
    let (commit, pending) = match res {
        SendResult::GroupEvolution { msg, pending, .. } => (msg, pending),
        other => panic!("expected GroupEvolution, got {other:?}"),
    };
    alice.confirm_published(pending).await.unwrap();

    // Alice is no longer admin; bob is the sole admin.
    let admins = alice.admin_pubkeys(&gid).unwrap();
    assert_eq!(admins, vec![pk32(b"bob")]);

    // Bob and Carol converge to the same admin set.
    deliver(&mut bob, &commit, &gid).await;
    deliver(&mut carol, &commit, &gid).await;
    assert_eq!(bob.admin_pubkeys(&gid).unwrap(), vec![pk32(b"bob")]);
    assert_eq!(carol.admin_pubkeys(&gid).unwrap(), vec![pk32(b"bob")]);

    // Alice can no longer perform an admin-gated operation; bob can.
    assert!(matches!(
        alice
            .send(SendIntent::UpdateGroupData {
                group_id: gid.clone(),
                name: Some("alice-rename".into()),
                description: None,
            })
            .await,
        Err(EngineError::NotGroupAdmin { .. })
    ));
    assert!(
        bob.send(SendIntent::UpdateGroupData {
            group_id: gid.clone(),
            name: Some("bob-rename".into()),
            description: None,
        })
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn transfer_admin_to_self_is_noop() {
    let (mut alice, _bob, gid) = create_pair().await;
    let res = alice
        .send(SendIntent::TransferAdmin {
            group_id: gid.clone(),
            new_admin_pubkey: pk32(b"alice"),
        })
        .await
        .unwrap();
    assert!(matches!(res, SendResult::Noop { .. }));
    assert_eq!(alice.admin_pubkeys(&gid).unwrap(), vec![pk32(b"alice")]);
    assert_eq!(alice.epoch(&gid).unwrap().0, 1);
}

#[tokio::test]
async fn transfer_admin_to_non_member_is_unknown_member() {
    let (mut alice, _bob, gid) = create_pair().await;
    let err = alice
        .send(SendIntent::TransferAdmin {
            group_id: gid.clone(),
            new_admin_pubkey: pk32(b"stranger"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::UnknownMember { .. }));
    // Caller is still admin (transfer aborted before any state change).
    assert_eq!(alice.admin_pubkeys(&gid).unwrap(), vec![pk32(b"alice")]);
}

#[tokio::test]
async fn non_admin_cannot_transfer() {
    let (_alice, mut bob, gid) = create_pair().await;
    let err = bob
        .send(SendIntent::TransferAdmin {
            group_id: gid.clone(),
            new_admin_pubkey: pk32(b"bob"),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::NotGroupAdmin { .. }));
}
