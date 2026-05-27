//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn account_create_list_and_status_are_json_addressable() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let created = run_json(home.path(), &["account", "create"]);
    let account_id = created["account_id"].as_str().expect("account id");
    assert_eq!(created["local_signing"], true);
    assert!(created["npub"].as_str().expect("npub").starts_with("npub1"));

    let listed = run_json(home.path(), &["account", "list"]);
    assert_eq!(listed["accounts"][0]["account_id"], account_id);
    assert_eq!(listed["accounts"][0]["npub"], created["npub"]);
    assert_eq!(listed["accounts"][0]["profile"], created["profile"]);
    assert_eq!(
        listed["accounts"][0]["display_name"],
        created["profile"]["display_name"]
    );

    let status = run_json(home.path(), &["account", "status", account_id]);
    assert_eq!(status["account_id"], account_id);
    assert_eq!(status["npub"], created["npub"]);
    assert_eq!(status["relay_lists"]["complete"], true);
    assert_eq!(
        status["relay_lists"]["default_relays"],
        serde_json::json!([relay])
    );
}

#[test]
fn account_create_accepts_nsec_without_echoing_it() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let imported = run_json_with_stdin(
        home.path(),
        &[
            "account",
            "create",
            "--nsec-stdin",
            "--default-relays",
            "wss://relay.example",
            "--bootstrap-relays",
            relay,
            "--publish-missing-relay-lists",
        ],
        &format!("{nsec}\n"),
    );
    assert!(!imported.to_string().contains(nsec));

    let account_id = imported["account_id"].as_str().expect("account id");
    assert_eq!(account_id.len(), 64);
    assert_eq!(imported["local_signing"], true);

    let status = run_json(home.path(), &["account", "status", account_id]);
    assert_eq!(status["account_id"], account_id);
}

#[test]
fn account_create_rejects_nsec_argv_and_accepts_stdin_secret() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let error = run_json_error(
        home.path(),
        &[
            "account",
            "create",
            nsec,
            "--default-relays",
            "wss://relay.example",
            "--bootstrap-relays",
            relay,
        ],
    );
    assert_eq!(error["code"], "secret_argument_rejected");
    assert!(!error.to_string().contains(nsec));

    let imported = run_json_with_stdin(
        home.path(),
        &[
            "account",
            "create",
            "--nsec-stdin",
            "--default-relays",
            "wss://relay.example",
            "--bootstrap-relays",
            relay,
            "--publish-missing-relay-lists",
        ],
        &format!("{nsec}\n"),
    );
    assert_eq!(imported["local_signing"], true);
    assert_eq!(
        imported["account_id"].as_str().expect("account id").len(),
        64
    );
}

#[test]
fn whitenoise_identity_commands_create_login_and_show_accounts() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let created = run_json(home.path(), &["create-identity"]);
    assert_eq!(created["local_signing"], true);
    assert!(created["npub"].as_str().expect("npub").starts_with("npub1"));
    assert_eq!(created["key_package"]["published"], true);
    assert!(created["key_package"]["bytes"].as_u64().expect("bytes") > 0);
    let created_id = created["account_id"].as_str().expect("created account id");
    let profile_name = created["profile"]["name"].as_str().expect("profile name");
    let display_name = created["profile"]["display_name"]
        .as_str()
        .expect("display name");
    assert_eq!(display_name, profile_name);
    assert_two_word_pseudonym(profile_name);

    let shown_profile = run_json(home.path(), &["--account", created_id, "profile", "show"]);
    assert_eq!(shown_profile["profile"], created["profile"]);

    let positional_error = run_json_error(home.path(), &["login", nsec, "--relay", relay]);
    assert_eq!(positional_error["code"], "secret_argument_rejected");
    assert!(!positional_error.to_string().contains(nsec));

    let logged_in = run_json_with_stdin(
        home.path(),
        &["login", "--nsec-stdin", "--relay", relay],
        &format!("{nsec}\n"),
    );
    assert!(!logged_in.to_string().contains(nsec));
    assert_eq!(logged_in["local_signing"], true);
    assert_eq!(logged_in["key_package"]["published"], true);
    assert!(logged_in["key_package"]["bytes"].as_u64().expect("bytes") > 0);

    let whoami = run_json(home.path(), &["whoami"]);
    let accounts = whoami["accounts"].as_array().expect("accounts");
    assert_eq!(accounts.len(), 2);
    assert!(
        accounts
            .iter()
            .all(|account| account["local_signing"] == true)
    );

    let accounts_list = run_json(home.path(), &["accounts", "list"]);
    assert_eq!(
        accounts_list["accounts"]
            .as_array()
            .expect("accounts")
            .len(),
        2
    );
    let created_account = accounts_list["accounts"]
        .as_array()
        .expect("accounts")
        .iter()
        .find(|account| account["account_id"] == created_id)
        .expect("created account in list");
    assert_eq!(created_account["profile"], created["profile"]);
    assert_eq!(
        created_account["display_name"],
        created["profile"]["display_name"]
    );
}

#[test]
fn create_identity_publishes_key_package_for_direct_invites() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = run_json(home.path(), &["create-identity"]);
    let bob = run_json(home.path(), &["create-identity"]);
    let alice_id = alice["account_id"].as_str().expect("alice account id");
    let bob_id = bob["account_id"].as_str().expect("bob account id");

    let created_group = run_json(
        home.path(),
        &["--account", alice_id, "groups", "create", "general", bob_id],
    );
    assert!(created_group["group_id"].as_str().is_some());
}

#[test]
fn account_create_uses_global_relay_for_required_relay_lists() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let created = run_json_with_relay(home.path(), relay, &["account", "create"]);

    assert_eq!(created["relay_lists"]["complete"], true);
    assert_eq!(
        created["relay_lists"]["default_relays"],
        serde_json::json!([relay])
    );
    assert_eq!(
        created["relay_lists"]["bootstrap_relays"],
        serde_json::json!([relay])
    );
    assert_eq!(created["relay_lists"]["nip65"]["kind"], 10002);
    assert_eq!(created["relay_lists"]["inbox"]["kind"], 10050);
    assert_eq!(created["relay_lists"]["key_package"]["kind"], 10051);
}

#[test]
fn account_create_requires_relay_setup() {
    let home = tempfile::tempdir().expect("tempdir");
    let output = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--home")
        .arg(home.path())
        .arg("--json")
        .arg("--secret-store")
        .arg("file")
        .args(["account", "create"])
        .output()
        .expect("dm command should start");

    assert!(
        !output.status.success(),
        "dm unexpectedly succeeded\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["error"]["code"], "missing_relay_url");
}

#[test]
fn account_create_accepts_public_nostr_identity_without_signing() {
    let home = tempfile::tempdir().expect("tempdir");
    let public_key = "npub14f8usejl26twx0dhuxjh9cas7keav9vr0v8nvtwtrjqx3vycc76qqh9nsy";
    let account_id = "aa4fc8665f5696e33db7e1a572e3b0f5b3d615837b0f362dcb1c8068b098c7b4";

    let created = run_json(home.path(), &["account", "create", public_key]);

    assert_eq!(created["account_id"], account_id);
    assert_eq!(created["local_signing"], false);
    assert!(created["npub"].as_str().unwrap().starts_with("npub1"));

    let status = run_json(home.path(), &["account", "status", public_key]);
    assert_eq!(status["account_id"], account_id);
    assert_eq!(status["local_signing"], false);

    let error = run_json_error(home.path(), &["--account", public_key, "keys", "publish"]);
    assert_eq!(error["code"], "public_account_cannot_sign");
}

#[test]
fn account_create_publishes_required_relay_lists_from_default_relays() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();
    let (default_relay_a, default_relay_b, default_relays) = two_default_relays();

    let created = create_account_with_relays(home.path(), &default_relays, relay);
    assert_eq!(created["relay_lists"]["complete"], true);
    assert_eq!(
        created["relay_lists"]["default_relays"],
        relay_pair_json(&default_relay_a, &default_relay_b)
    );
    assert_eq!(
        created["relay_lists"]["bootstrap_relays"],
        serde_json::json!([relay])
    );
    assert_eq!(created["relay_lists"]["nip65"]["kind"], 10002);
    assert_eq!(created["relay_lists"]["inbox"]["kind"], 10050);
    assert_eq!(created["relay_lists"]["key_package"]["kind"], 10051);

    let account_id = created["account_id"].as_str().expect("account id");
    let status = run_json(home.path(), &["account", "status", account_id]);
    assert_eq!(status["relay_lists"], created["relay_lists"]);
}

#[test]
fn account_create_reports_missing_relay_lists_without_storing_the_nsec() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = TestRelay::new();
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let error = run_json_error_with_stdin(
        home.path(),
        &[
            "account",
            "create",
            "--nsec-stdin",
            "--bootstrap-relays",
            relay.url(),
        ],
        &format!("{nsec}\n"),
    );
    assert_eq!(error["code"], "missing_relay_lists");
    assert_eq!(
        error["missing"],
        serde_json::json!(["nip65", "inbox", "key_package"])
    );
    assert_eq!(error["repair"]["requires"], "--default-relays");
    assert!(!error.to_string().contains(nsec));

    let listed = run_json(home.path(), &["account", "list"]);
    assert_eq!(listed["accounts"], serde_json::json!([]));
}

#[test]
fn account_create_rolls_back_when_relay_list_publication_fails() {
    let home = tempfile::tempdir().expect("tempdir");

    let error = run_json_error(
        home.path(),
        &["account", "create", "--default-relays", "not-a-relay-url"],
    );
    assert_ne!(error["code"], "usage");

    let listed = run_json(home.path(), &["account", "list"]);
    assert_eq!(listed["accounts"], serde_json::json!([]));
}

#[test]
fn account_create_can_publish_missing_relay_lists_from_default_relays() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = TestRelay::new();
    let (default_relay_a, default_relay_b, default_relays) = two_default_relays();
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let imported = run_json_with_stdin(
        home.path(),
        &[
            "account",
            "create",
            "--nsec-stdin",
            "--default-relays",
            &default_relays,
            "--bootstrap-relays",
            relay.url(),
            "--publish-missing-relay-lists",
        ],
        &format!("{nsec}\n"),
    );

    assert_eq!(imported["relay_lists"]["complete"], true);
    assert_eq!(
        imported["relay_lists"]["default_relays"],
        relay_pair_json(&default_relay_a, &default_relay_b)
    );
    let listed = run_json(home.path(), &["account", "list"]);
    assert_eq!(listed["accounts"][0]["account_id"], imported["account_id"]);
}

#[test]
fn account_import_requires_explicit_repair_before_publishing_missing_relay_lists() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = TestRelay::new();
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let error = run_json_error_with_stdin(
        home.path(),
        &[
            "account",
            "create",
            "--nsec-stdin",
            "--default-relays",
            relay.url(),
            "--bootstrap-relays",
            relay.url(),
        ],
        &format!("{nsec}\n"),
    );

    assert_eq!(error["code"], "missing_relay_lists");
    assert_eq!(
        error["missing"],
        serde_json::json!(["nip65", "inbox", "key_package"])
    );
    assert_eq!(
        error["repair"]["publish_missing"],
        "--publish-missing-relay-lists"
    );
    assert!(!error.to_string().contains(nsec));

    let listed = run_json(home.path(), &["account", "list"]);
    assert_eq!(listed["accounts"], serde_json::json!([]));
}

#[test]
fn account_create_rolls_back_when_missing_relay_list_publication_fails() {
    let home = tempfile::tempdir().expect("tempdir");
    let nsec = "nsec1j4c6269y9w0q2er2xjw8sv2ehyrtfxq3jwgdlxj6qfn8z4gjsq5qfvfk99";

    let error = run_json_error_with_stdin(
        home.path(),
        &[
            "account",
            "create",
            "--nsec-stdin",
            "--default-relays",
            "not-a-relay-url",
        ],
        &format!("{nsec}\n"),
    );
    assert_ne!(error["code"], "usage");
    assert!(!error.to_string().contains(nsec));

    let listed = run_json(home.path(), &["account", "list"]);
    assert_eq!(listed["accounts"], serde_json::json!([]));
}

#[test]
fn account_relay_lists_checks_a_pubkey_from_bootstrap_relays() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();
    let (_default_relay_a, _default_relay_b, default_relays) = two_default_relays();

    let created = create_account_with_relays(home.path(), &default_relays, relay);
    let account_id = created["account_id"].as_str().expect("account id");

    let checked = run_json(
        home.path(),
        &[
            "account",
            "relay-lists",
            account_id,
            "--bootstrap-relays",
            relay,
        ],
    );

    assert_eq!(checked["account_id"], account_id);
    assert_eq!(checked["relay_lists"]["complete"], true);
    assert_eq!(
        checked["relay_lists"]["bootstrap_relays"],
        serde_json::json!([relay])
    );
}

#[test]
fn account_resolution_errors_are_stable_json_contracts() {
    let home = tempfile::tempdir().expect("tempdir");

    let missing = run_json_error(home.path(), &["keys", "publish"]);
    assert_eq!(missing["code"], "missing_account");
    assert_eq!(missing["repair"]["select"], "--account <npub-or-hex>");

    create_account(home.path());
    create_account(home.path());

    let multiple = run_json_error(home.path(), &["keys", "publish"]);
    assert_eq!(multiple["code"], "multiple_accounts");
    assert_eq!(multiple["repair"]["env"], "DM_ACCOUNT");

    let unknown = run_json_error(
        home.path(),
        &["--account", "not-a-pubkey", "keys", "publish"],
    );
    assert_eq!(unknown["code"], "invalid_public_key");
}
