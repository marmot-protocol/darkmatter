//! Phase 1 verification smoke test.
//!
//! Opens a fresh Marmot kit in a tempdir, exercises the methods that don't
//! require an external relay (constructor, listAccounts, shutdown), and
//! confirms the empty case lifecycle behaves as expected.
//!
//! Full multi-device send/receive coverage lives in marmot-app's own
//! integration tests against a built-in nostr-relay-builder relay. The job
//! here is just to prove the FFI boundary itself is alive.

use std::sync::Arc;

use marmot_uniffi::Marmot;

#[tokio::test]
async fn empty_kit_lifecycle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let kit: Arc<Marmot> = Marmot::new(
        tmp.path().to_string_lossy().into_owned(),
        vec!["wss://relay.invalid.test".to_string()],
    )
    .expect("open marmot kit");

    // Fresh kit should be openable and report no accounts.
    let accounts = kit.list_accounts().expect("list_accounts on empty kit");
    assert!(
        accounts.is_empty(),
        "expected no accounts on a brand-new root, got {:?}",
        accounts
    );

    // Shutdown must succeed even before start() — the constructor does no I/O
    // beyond opening the account-home dir, so there's nothing to tear down.
    kit.shutdown().await;
}

#[test]
fn display_name_is_none_for_unknown_account() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let kit = Marmot::new(
        tmp.path().to_string_lossy().into_owned(),
        vec!["wss://relay.invalid.test".to_string()],
    )
    .expect("open marmot kit");

    // A hex string that doesn't match any known account should produce None.
    let name = kit.display_name(
        "0000000000000000000000000000000000000000000000000000000000000000".into(),
    );
    assert!(name.is_none(), "expected None for unknown account, got {:?}", name);
}
