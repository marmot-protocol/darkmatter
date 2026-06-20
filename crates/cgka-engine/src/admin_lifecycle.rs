//! Group admin lifecycle FFI — `SendIntent::GrantAdmin` / `RevokeAdmin` /
//! `TransferAdmin` (darkmatter#488).
//!
//! These are typed, ergonomic wrappers above the generic admin-policy
//! `AppDataUpdate` commit path in [`super::update_group_data`]. The engine —
//! not the application — recomputes the resulting admin set from the live
//! signed group state, enforces the admin-lifecycle invariants below, and only
//! then delegates to [`Engine::do_send_update_app_components`], which performs
//! the caller-authorization (`require_admin`) and admin-leaf-coupling checks
//! and stages the MLS commit.
//!
//! Invariants (issue darkmatter#488, `spec/app-components/admin-policy-v1.md`):
//!
//! 1. **At least one admin while non-admin members remain.** `revoke_admin`
//!    refuses with [`EngineError::LastAdminCannotResign`] if the target is the
//!    sole admin and the group still has non-admin members.
//! 2. **Caller authorization.** Only an existing admin may grant or revoke.
//!    Enforced engine-side by `require_admin` on the delegated update path; we
//!    re-check it up front so authorization failures short-circuit before any
//!    membership / invariant work and before an idempotent no-op is reported.
//! 3. **MLS commit semantics.** Grant / revoke is a single admin-policy
//!    `AppDataUpdate` commit through the standard mutation path; `transfer`
//!    applies its grant and revoke in one commit so the resulting epoch never
//!    transiently lacks an admin.
//! 4. **Idempotency.** Granting admin to an existing admin, or revoking from a
//!    non-admin, is a no-op success ([`SendResult::Noop`]); no commit is
//!    staged.
//! 5. **Sole admin = sole member.** `revoke_admin` on the sole admin who is
//!    also the sole remaining member returns
//!    [`EngineError::SoleMemberCannotRevoke`] so the application can route to
//!    its delete-empty-group path.

use crate::engine::Engine;
use cgka_traits::app_components::{AppComponentData, GROUP_ADMIN_POLICY_COMPONENT_ID};
use cgka_traits::engine::SendResult;
use cgka_traits::error::EngineError;
use cgka_traits::storage::StorageProvider;
use cgka_traits::types::{GroupId, MemberId};
use openmls::group::MlsGroup;
use std::collections::BTreeSet;

/// A pubkey resolved against the live group state: its admin membership and
/// whether it is a current group member.
struct AdminContext {
    /// Current admin pubkeys (sorted, deduped) from signed MLS group state.
    admins: Vec<[u8; 32]>,
    /// Distinct account pubkeys with at least one current member leaf.
    member_accounts: BTreeSet<[u8; 32]>,
}

impl AdminContext {
    fn is_admin(&self, pubkey: &[u8; 32]) -> bool {
        self.admins.iter().any(|a| a == pubkey)
    }

    fn is_member(&self, pubkey: &[u8; 32]) -> bool {
        self.member_accounts.contains(pubkey)
    }
}

impl<S: StorageProvider> Engine<S> {
    /// `SendIntent::GrantAdmin` — grant admin rights to an existing member.
    pub(crate) async fn do_send_grant_admin(
        &mut self,
        group_id: GroupId,
        member_pubkey: [u8; 32],
    ) -> Result<SendResult, EngineError> {
        let ctx = self.load_admin_context(&group_id)?;

        // Invariant 2: caller authorization, checked up front.
        self.require_local_admin(&group_id, &ctx)?;

        // Invariant 4: idempotent grant of an existing admin is a no-op.
        if ctx.is_admin(&member_pubkey) {
            return Ok(SendResult::Noop { group_id });
        }

        // MemberNotFound: target must be a current member.
        if !ctx.is_member(&member_pubkey) {
            return Err(EngineError::UnknownMember {
                group_id,
                member: MemberId::new(member_pubkey.to_vec()),
            });
        }

        let mut next_admins = ctx.admins.clone();
        next_admins.push(member_pubkey);
        self.send_admin_policy_update(group_id, next_admins).await
    }

    /// `SendIntent::RevokeAdmin` — revoke admin rights from an existing member.
    pub(crate) async fn do_send_revoke_admin(
        &mut self,
        group_id: GroupId,
        member_pubkey: [u8; 32],
    ) -> Result<SendResult, EngineError> {
        let ctx = self.load_admin_context(&group_id)?;

        // Invariant 2: caller authorization, checked up front.
        self.require_local_admin(&group_id, &ctx)?;

        // Invariant 4: revoking from a non-admin is a no-op. This also
        // subsumes the MemberNotFound case for revoke: a non-member is by
        // definition not an admin (admin-leaf coupling), so a non-admin target
        // — member or not — is a benign no-op rather than an error.
        if !ctx.is_admin(&member_pubkey) {
            return Ok(SendResult::Noop { group_id });
        }

        let next_admins: Vec<[u8; 32]> = ctx
            .admins
            .iter()
            .copied()
            .filter(|a| a != &member_pubkey)
            .collect();

        // Invariants 1 + 5: refuse to leave a non-empty group admin-less.
        if next_admins.is_empty() {
            // The target is the sole admin. Distinguish the sole-member case
            // (revoke is undefined → route to delete-group) from the
            // non-empty-group case (would strand other members admin-less).
            if ctx.member_accounts.len() <= 1 {
                return Err(EngineError::SoleMemberCannotRevoke { group_id });
            }
            return Err(EngineError::LastAdminCannotResign { group_id });
        }

        self.send_admin_policy_update(group_id, next_admins).await
    }

    /// `SendIntent::TransferAdmin` — grant admin to `new_admin_pubkey` and
    /// revoke it from the local caller, in a single admin-policy commit.
    pub(crate) async fn do_send_transfer_admin(
        &mut self,
        group_id: GroupId,
        new_admin_pubkey: [u8; 32],
    ) -> Result<SendResult, EngineError> {
        let ctx = self.load_admin_context(&group_id)?;

        // Invariant 2: caller authorization, checked up front.
        let local_pubkey = self.local_admin_pubkey()?;
        self.require_local_admin(&group_id, &ctx)?;

        // Transferring to the caller themselves is a no-op: the grant and the
        // self-revoke cancel out, leaving the caller as admin.
        if new_admin_pubkey == local_pubkey {
            return Ok(SendResult::Noop { group_id });
        }

        // MemberNotFound: the new admin must be a current member.
        if !ctx.is_member(&new_admin_pubkey) {
            return Err(EngineError::UnknownMember {
                group_id,
                member: MemberId::new(new_admin_pubkey.to_vec()),
            });
        }

        // Compute the resulting admin set: add new_admin, drop the caller.
        let mut next_admins: Vec<[u8; 32]> = ctx
            .admins
            .iter()
            .copied()
            .filter(|a| a != &local_pubkey)
            .collect();
        if !next_admins.contains(&new_admin_pubkey) {
            next_admins.push(new_admin_pubkey);
        }

        // The grant guarantees a non-empty resulting admin set, so neither
        // LastAdminCannotResign nor SoleMemberCannotRevoke can fire here.
        debug_assert!(!next_admins.is_empty());

        // If the result equals the current set, nothing changed (e.g. the new
        // admin was already an admin and the caller was not). Report a no-op.
        if admin_set_eq(&ctx.admins, &next_admins) {
            return Ok(SendResult::Noop { group_id });
        }

        self.send_admin_policy_update(group_id, next_admins).await
    }

    /// Resolve the local caller's account pubkey.
    fn local_admin_pubkey(&self) -> Result<[u8; 32], EngineError> {
        crate::app_components::admin_pubkey_from_member_id(self.identity.self_id())
    }

    /// Verify the local caller is an admin; otherwise `NotGroupAdmin`
    /// (the issue's `NotAuthorized`).
    fn require_local_admin(
        &self,
        group_id: &GroupId,
        ctx: &AdminContext,
    ) -> Result<(), EngineError> {
        let local = self.local_admin_pubkey()?;
        if ctx.is_admin(&local) {
            Ok(())
        } else {
            Err(EngineError::NotGroupAdmin {
                group_id: group_id.clone(),
            })
        }
    }

    /// Load the current admin set and member-account set from signed MLS group
    /// state. Returns `UnknownGroup` if the group is not in storage.
    fn load_admin_context(&self, group_id: &GroupId) -> Result<AdminContext, EngineError> {
        let provider = crate::provider::EngineOpenMlsProvider::<S>::new(
            &self.crypto,
            self.storage.mls_storage(),
        );
        let mls_gid = openmls::group::GroupId::from_slice(group_id.as_slice());
        let mls_group = MlsGroup::load(
            <crate::provider::EngineOpenMlsProvider<'_, S> as openmls_traits::OpenMlsProvider>::storage(&provider),
            &mls_gid,
        )
        .map_err(|e| EngineError::Backend(format!("load: {e:?}")))?
        .ok_or_else(|| EngineError::UnknownGroup(group_id.clone()))?;

        let mut admins = crate::app_components::admins_of_group(&mls_group)?;
        admins.sort();
        admins.dedup();
        let member_accounts = member_accounts_of_group(&mls_group);
        Ok(AdminContext {
            admins,
            member_accounts,
        })
    }

    /// Stage the admin-policy `AppDataUpdate` commit via the generic update
    /// path, which re-checks `require_admin` and `reject_admins_without_member_leaf`
    /// and emits the `AdminAdded` / `AdminRemoved` `GroupStateChange` events.
    async fn send_admin_policy_update(
        &mut self,
        group_id: GroupId,
        admins: Vec<[u8; 32]>,
    ) -> Result<SendResult, EngineError> {
        let data = crate::app_components::encode_admin_policy(&admins)?;
        self.do_send_update_app_components(
            group_id,
            vec![AppComponentData {
                component_id: GROUP_ADMIN_POLICY_COMPONENT_ID,
                data,
            }],
        )
        .await
    }
}

/// Distinct account pubkeys backing at least one current member leaf.
fn member_accounts_of_group(mls_group: &MlsGroup) -> BTreeSet<[u8; 32]> {
    use openmls::prelude::BasicCredential;
    let mut accounts = BTreeSet::new();
    for member in mls_group.members() {
        if let Ok(basic) = BasicCredential::try_from(member.credential)
            && let Ok(pk) = <[u8; 32]>::try_from(basic.identity())
        {
            accounts.insert(pk);
        }
    }
    accounts
}

/// Order-insensitive admin-set equality (both inputs are deduped; `encode_admin_policy`
/// sorts, so set equality is the right comparison for a no-op check).
fn admin_set_eq(a: &[[u8; 32]], b: &[[u8; 32]]) -> bool {
    let sa: BTreeSet<[u8; 32]> = a.iter().copied().collect();
    let sb: BTreeSet<[u8; 32]> = b.iter().copied().collect();
    sa == sb
}
