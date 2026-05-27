//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn real_local_relays_deliver_cli_messages_over_sdk_path() {
    let relays = real_relay_urls();
    let available_relays = relays
        .iter()
        .filter(|relay| local_relay_available(relay))
        .collect::<Vec<_>>();
    if available_relays.is_empty() {
        assert!(
            !require_real_relays(),
            "real relay CLI E2E requires one of these relays to be reachable: {relays:?}"
        );
        eprintln!("skipping real relay CLI E2E: no local relay ports are reachable");
        return;
    }

    for relay in available_relays {
        let relay = relay.as_str();
        let home = tempfile::tempdir().expect("tempdir");
        let alice = create_account_with_real_relay(home.path(), relay);
        let bob = create_account_with_real_relay(home.path(), relay);
        run_json_with_relay(home.path(), relay, &["--account", &bob, "keys", "publish"]);

        let group_name = format!(
            "real-relay-{}",
            relay.rsplit(':').next().unwrap_or("unknown")
        );
        let created_group = run_json_with_relay(
            home.path(),
            relay,
            &["--account", &alice, "group", "create", &group_name, &bob],
        );
        let group_id = created_group["group_id"].as_str().expect("group id");

        let bob_join = sync_until_joined(home.path(), relay, &bob, group_id);
        assert_eq!(bob_join["joined_groups"][0], group_id);

        let body = format!("hello over {relay}");
        run_json_with_relay(
            home.path(),
            relay,
            &[
                "--account",
                &alice,
                "message",
                "send",
                "--group",
                group_id,
                &body,
            ],
        );
        let bob_sync = sync_until_message(home.path(), relay, &bob, &body);
        assert_message_plaintexts(&bob_sync, &[&body]);

        let bob_messages = run_json_with_relay(
            home.path(),
            relay,
            &["--account", &bob, "message", "list", "--group", group_id],
        );
        assert_message_plaintexts(&bob_messages, &[&body]);
    }
}

#[test]
fn daemon_real_relay_keeps_live_subscriptions_without_polling_knobs() {
    let relays = real_relay_urls();
    let Some(relay) = relays.iter().find(|relay| local_relay_available(relay)) else {
        assert!(
            !require_real_relays(),
            "live daemon relay E2E requires one of these relays to be reachable: {relays:?}"
        );
        eprintln!("skipping live daemon relay E2E: no local relay ports are reachable");
        return;
    };
    let relay = relay.as_str();
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");

    let alice = create_account_with_real_relay(home.path(), relay);
    let bob = create_account_with_real_relay(home.path(), relay);
    run_json_with_relay(home.path(), relay, &["--account", &bob, "keys", "publish"]);

    let start = dm_with_relay(home.path(), relay)
        .args(["daemon", "start"])
        .output()
        .expect("dm daemon start should run");
    assert!(
        start.status.success(),
        "daemon start failed\n{}",
        command_output_summary(&start)
    );
    wait_for_daemon(&socket);

    let group_name = format!(
        "live-daemon-{}",
        relay.rsplit(':').next().unwrap_or("unknown")
    );
    let created_group = run_json_with_relay(
        home.path(),
        relay,
        &["--account", &alice, "group", "create", &group_name, &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    wait_until_chat_visible(home.path(), relay, &bob, group_id);

    let body = format!("daemon live hello over {relay}");
    run_json_with_relay(
        home.path(),
        relay,
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            &body,
        ],
    );

    let messages = wait_until_projected_message(home.path(), relay, &bob, group_id, &body);
    assert_message_plaintexts(&messages, &[&body]);

    let _ = dm(home.path()).args(["daemon", "stop"]).output();
}
