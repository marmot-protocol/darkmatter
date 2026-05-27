//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn key_package_fetches_latest_package_via_relay_list_discovery() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let created = create_account_with_relays(home.path(), relay, relay);
    let account_id = created["account_id"].as_str().expect("account id");

    let published = run_json(home.path(), &["--account", account_id, "keys", "publish"]);
    let published_bytes = published["key_package_bytes"].as_u64().expect("bytes");
    assert!(published_bytes > 0);

    let fetched = run_json(
        home.path(),
        &["keys", "fetch", account_id, "--bootstrap-relays", relay],
    );

    assert_eq!(fetched["account_id"], account_id);
    assert_eq!(fetched["key_package_bytes"].as_u64(), Some(published_bytes));
    assert_eq!(
        fetched["relay_lists"]["key_package"]["relays"],
        serde_json::json!([relay])
    );
    assert_eq!(fetched["source_relays"], serde_json::json!([relay]));
}

#[test]
fn keys_publish_reuses_create_identity_key_package() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let created = run_json_with_relay(home.path(), relay, &["create-identity"]);
    let account_id = created["account_id"].as_str().expect("account id");
    let first = run_json(
        home.path(),
        &["keys", "fetch", account_id, "--bootstrap-relays", relay],
    );

    let republished = run_json(home.path(), &["--account", account_id, "keys", "publish"]);
    let second = run_json(
        home.path(),
        &["keys", "fetch", account_id, "--bootstrap-relays", relay],
    );

    assert_eq!(republished["key_package_bytes"], first["key_package_bytes"]);
    assert_eq!(second["key_package_bytes"], first["key_package_bytes"]);
    assert_eq!(second["key_package_id"], first["key_package_id"]);
    assert_eq!(second["key_package_ref"], first["key_package_ref"]);
    assert!(
        first["key_package_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert!(
        second["key_package_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
}

#[test]
fn keys_rotate_forces_a_new_key_package_then_publish_reuses_it() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let created = run_json_with_relay(home.path(), relay, &["create-identity"]);
    let account_id = created["account_id"].as_str().expect("account id");
    let first = run_json(
        home.path(),
        &["keys", "fetch", account_id, "--bootstrap-relays", relay],
    );

    let rotated = run_json(home.path(), &["--account", account_id, "keys", "rotate"]);
    assert_eq!(rotated["rotated"], true);
    let second = run_json(
        home.path(),
        &["keys", "fetch", account_id, "--bootstrap-relays", relay],
    );
    run_json(home.path(), &["--account", account_id, "keys", "publish"]);
    let third = run_json(
        home.path(),
        &["keys", "fetch", account_id, "--bootstrap-relays", relay],
    );

    assert_eq!(second["key_package_id"], first["key_package_id"]);
    assert_ne!(second["key_package_ref"], first["key_package_ref"]);
    assert_eq!(second["key_package_bytes"], rotated["key_package_bytes"]);
    assert_eq!(third["key_package_bytes"], second["key_package_bytes"]);
    assert_eq!(third["key_package_id"], second["key_package_id"]);
    assert_eq!(third["key_package_ref"], second["key_package_ref"]);
    assert!(
        third["key_package_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
}

#[test]
fn global_account_selects_subject_for_keys_fetch_and_relay_lists() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let created = create_account_with_relays(home.path(), relay, relay);
    let account_id = created["account_id"].as_str().expect("account id");

    let relay_lists = run_json(
        home.path(),
        &[
            "--account",
            account_id,
            "account",
            "relay-lists",
            "--bootstrap-relays",
            relay,
        ],
    );
    assert_eq!(relay_lists["account_id"], account_id);
    assert_eq!(relay_lists["relay_lists"]["complete"], true);

    let published = run_json(home.path(), &["--account", account_id, "keys", "publish"]);
    let fetched = run_json(home.path(), &["--account", account_id, "keys", "fetch"]);
    assert_eq!(fetched["account_id"], account_id);
    assert_eq!(fetched["key_package_bytes"], published["key_package_bytes"]);
}

#[test]
fn keys_namespace_uses_account_resolution() {
    let home = tempfile::tempdir().expect("tempdir");

    let account_id = create_account(home.path());

    let published = run_json(home.path(), &["keys", "publish"]);
    assert_eq!(published["account_id"], account_id);
    assert!(published["key_package_bytes"].as_u64().unwrap() > 0);
}

#[test]
fn missing_key_package_errors_include_repair_guidance() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());

    let error = run_json_error(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );

    assert_eq!(error["code"], "missing_key_package");
    assert_eq!(error["account_id"], bob);
    assert_eq!(
        error["repair"]["local"],
        format!("dm --account {bob} keys publish")
    );
    assert_eq!(
        error["repair"]["remote"],
        "dm keys fetch <npub-or-hex> --bootstrap-relays <relay-url>"
    );
}
