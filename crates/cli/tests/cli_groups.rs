//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn positional_group_and_message_commands_use_global_or_env_account() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    let bob_join = run_json_with_env(home.path(), &["sync"], &[("DM_ACCOUNT", &bob)]);
    if bob_join["joined_groups"][0].is_null() {
        let chats = run_json_with_env(home.path(), &["chats", "list"], &[("DM_ACCOUNT", &bob)]);
        assert!(
            chats["chats"]
                .as_array()
                .is_some_and(|chats| chats.iter().any(|chat| chat["group_id"] == group_id))
        );
    } else {
        assert_eq!(bob_join["joined_groups"][0], group_id);
    }

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            group_id,
            "hello bob",
        ],
    );

    let bob_sync = run_json_with_env(home.path(), &["sync"], &[("DM_ACCOUNT", &bob)]);
    if bob_sync["messages"][0]["plaintext"].is_null() {
        let messages =
            run_json_with_env(home.path(), &["message", "list"], &[("DM_ACCOUNT", &bob)]);
        assert!(
            message_plaintexts(&messages)
                .iter()
                .any(|message| message == "hello bob")
        );
    } else {
        assert_eq!(bob_sync["messages"][0]["plaintext"], "hello bob");
    }
}

#[test]
fn group_create_includes_agent_text_streams_by_default() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "agent", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    assert_eq!(created_group["agent_text_stream"]["required"], true);
    assert_eq!(created_group["agent_text_stream"]["component_id"], 0x8006);
    assert_eq!(
        created_group["agent_text_stream"]["component"],
        "marmot.group.agent-text-stream.quic.v1"
    );
    assert_eq!(
        created_group["agent_text_stream"]["data_hex"],
        "010300001000000000000000"
    );
    assert_eq!(
        created_group["agent_text_stream"]["required_member_roles"],
        serde_json::json!(["receive"])
    );

    sync_until_joined(home.path(), test_relay_url(), &bob, group_id);
    let bob_group = run_json(home.path(), &["--account", &bob, "chats", "show", group_id]);
    assert_eq!(bob_group["group"]["agent_text_stream"]["required"], true);
}

#[test]
fn whitenoise_groups_commands_cover_core_group_workflows() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    let carol = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    run_json(home.path(), &["--account", &carol, "keys", "publish"]);

    let created = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "groups",
            "create",
            "general",
            &bob,
            "--description",
            "launch room",
        ],
    );
    let group_id = created["group_id"].as_str().expect("group id");
    assert_eq!(created["profile"]["description"], "launch room");

    let shown = run_json(
        home.path(),
        &["--account", &alice, "groups", "show", group_id],
    );
    assert_eq!(shown["group"]["group_id"], group_id);

    let listed = run_json(home.path(), &["--account", &alice, "groups", "list"]);
    assert!(
        listed["groups"]
            .as_array()
            .is_some_and(|groups| groups.iter().any(|group| group["group_id"] == group_id))
    );

    let renamed = run_json(
        home.path(),
        &["--account", &alice, "groups", "rename", group_id, "ops"],
    );
    assert_eq!(renamed["group"]["profile"]["name"], "ops");

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "groups",
            "add-members",
            group_id,
            &carol,
        ],
    );
    let members = run_json(
        home.path(),
        &["--account", &alice, "groups", "members", group_id],
    );
    assert_eq!(
        member_accounts(&members),
        sorted_accounts([&alice, &bob, &carol])
    );
}

#[test]
fn groups_leave_publishes_self_remove() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "departures", &bob],
    );
    let group_id = created["group_id"].as_str().expect("group id");
    sync_until_joined(home.path(), test_relay_url(), &bob, group_id);

    let leave = run_json(
        home.path(),
        &["--account", &bob, "groups", "leave", group_id],
    );
    assert_eq!(leave["group_id"], group_id);
    assert_eq!(leave["published"], 1);

    let _ = run_json(home.path(), &["--account", &alice, "sync"]);
    let alice_members = run_json(
        home.path(),
        &["--account", &alice, "group", "members", group_id],
    );
    assert!(!member_accounts(&alice_members).contains(&bob));
}

#[test]
fn chats_list_exposes_visible_groups() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    sync_until_joined(home.path(), test_relay_url(), &bob, group_id);

    let chats = run_json(home.path(), &["--account", &bob, "chats", "list"]);
    assert_eq!(chats["chats"][0]["group_id"], group_id);
    assert_eq!(chats["chats"][0]["profile"]["name"], "general");
}

#[test]
fn group_create_can_invite_a_member_by_fetched_pubkey() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let alice = create_account(home.path());
    let bob = create_account_with_relays(home.path(), relay, relay);
    let bob_account_id = bob["account_id"].as_str().expect("bob account id");

    run_json(
        home.path(),
        &["--account", bob_account_id, "keys", "publish"],
    );
    run_json(
        home.path(),
        &["keys", "fetch", bob_account_id, "--bootstrap-relays", relay],
    );

    let created_group = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "group",
            "create",
            "pubkey",
            bob_account_id,
        ],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    let bob_join = sync_until_joined(home.path(), test_relay_url(), bob_account_id, group_id);
    assert_eq!(bob_join["joined_groups"][0], group_id);
}

#[test]
fn group_create_fetches_missing_key_package_for_pubkey_members() {
    let home = tempfile::tempdir().expect("tempdir");
    let relay = test_relay_url();

    let alice = create_account(home.path());
    let bob = create_account_with_relays(home.path(), relay, relay);
    let bob_account_id = bob["account_id"].as_str().expect("bob account id");

    run_json(
        home.path(),
        &["--account", bob_account_id, "keys", "publish"],
    );

    let created_group = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "group",
            "create",
            "pubkey",
            bob_account_id,
        ],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    let bob_join = sync_until_joined(home.path(), test_relay_url(), bob_account_id, group_id);
    assert_eq!(bob_join["joined_groups"][0], group_id);
}

#[test]
fn group_create_fetches_rotated_remote_key_package_via_discovery_relays() {
    let alice_home = tempfile::tempdir().expect("alice tempdir");
    let bob_home = tempfile::tempdir().expect("bob tempdir");
    let relay = test_relay_url();

    let bob_created = run_json_with_relay(bob_home.path(), relay, &["create-identity"]);
    let bob = bob_created["account_id"].as_str().expect("bob account id");
    run_json_with_relay(
        bob_home.path(),
        relay,
        &["--account", bob, "keys", "rotate"],
    );

    let alice_created = run_json_with_relay(alice_home.path(), relay, &["create-identity"]);
    let alice = alice_created["account_id"]
        .as_str()
        .expect("alice account id");

    let created_group = run_json_with_relay(
        alice_home.path(),
        relay,
        &["--account", alice, "groups", "create", "remote", bob],
    );

    assert!(created_group["group_id"].as_str().is_some());
}

#[test]
fn group_archive_is_local_state_not_membership_state() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let archived = run_json(
        home.path(),
        &["--account", &bob, "chats", "archive", group_id],
    );
    assert_eq!(archived["group"]["archived"], true);

    let visible = run_json(home.path(), &["--account", &bob, "chats", "list"]);
    assert_eq!(visible["chats"], serde_json::json!([]));

    let included = run_json(
        home.path(),
        &["--account", &bob, "chats", "list", "--include-archived"],
    );
    assert_eq!(included["chats"][0]["group_id"], group_id);
    assert_eq!(included["chats"][0]["archived"], true);

    let bob_members = run_json(
        home.path(),
        &["--account", &bob, "group", "members", group_id],
    );
    assert_eq!(
        member_accounts(&bob_members),
        sorted_accounts([&alice, &bob])
    );

    let alice_chats = run_json(home.path(), &["--account", &alice, "chats", "list"]);
    assert_eq!(alice_chats["chats"][0]["archived"], false);

    let unarchived = run_json(
        home.path(),
        &["--account", &bob, "chats", "unarchive", group_id],
    );
    assert_eq!(unarchived["group"]["archived"], false);
    let visible = run_json(home.path(), &["--account", &bob, "chats", "list"]);
    assert_eq!(visible["chats"][0]["group_id"], group_id);
}

#[test]
fn group_update_publishes_profile_component_changes() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let updated = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "group",
            "update",
            group_id,
            "--name",
            "team room",
            "--description",
            "daily coordination",
        ],
    );
    assert_eq!(updated["group"]["profile"]["name"], "team room");
    assert_eq!(
        updated["group"]["profile"]["description"],
        "daily coordination"
    );
    assert_eq!(updated["published"], 1);

    run_json(home.path(), &["--account", &bob, "sync"]);
    let bob_group = run_json(home.path(), &["--account", &bob, "chats", "show", group_id]);
    assert_eq!(bob_group["group"]["profile"]["name"], "team room");
    assert_eq!(
        bob_group["group"]["profile"]["description"],
        "daily coordination"
    );
}

#[test]
fn non_admin_group_mutations_return_admin_policy_errors() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    let carol = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    run_json(home.path(), &["--account", &carol, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let invite_error = run_json_error(
        home.path(),
        &["--account", &bob, "group", "invite", group_id, &carol],
    );
    assert_eq!(invite_error["code"], "not_group_admin");

    let update_error = run_json_error(
        home.path(),
        &[
            "--account",
            &bob,
            "group",
            "update",
            group_id,
            "--name",
            "nope",
        ],
    );
    assert_eq!(update_error["code"], "not_group_admin");
}

#[test]
fn groups_promote_and_demote_update_admin_policy_authorization() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "admins", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    let initial_admins = run_json(
        home.path(),
        &["--account", &alice, "groups", "admins", group_id],
    );
    assert_eq!(admin_accounts(&initial_admins), sorted_accounts([&alice]));

    let promoted = run_json(
        home.path(),
        &["--account", &alice, "groups", "promote", group_id, &bob],
    );
    assert_eq!(promoted["published"], 1);
    assert_eq!(
        promoted["group"]["admin_policy"]["admins"],
        serde_json::json!(sorted_accounts([&alice, &bob]))
    );

    sync_until_joined(home.path(), test_relay_url(), &bob, group_id);
    sync_until_admins(home.path(), &bob, group_id, [&alice, &bob]);
    let bob_rename = run_json(
        home.path(),
        &["--account", &bob, "groups", "rename", group_id, "bob-led"],
    );
    assert_eq!(bob_rename["published"], 1);
    assert_eq!(bob_rename["group"]["profile"]["name"], "bob-led");

    let self_demoted = run_json(
        home.path(),
        &["--account", &bob, "groups", "self-demote", group_id],
    );
    assert_eq!(self_demoted["published"], 1);
    assert_eq!(
        self_demoted["group"]["admin_policy"]["admins"],
        serde_json::json!(sorted_accounts([&alice]))
    );
    let self_demoted_error = run_json_error(
        home.path(),
        &["--account", &bob, "groups", "rename", group_id, "nope"],
    );
    assert_eq!(self_demoted_error["code"], "not_group_admin");

    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let demote_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "demotions", &bob],
    );
    let demote_group_id = demote_group["group_id"].as_str().expect("group id");
    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "groups",
            "promote",
            demote_group_id,
            &bob,
        ],
    );
    sync_until_joined(home.path(), test_relay_url(), &bob, demote_group_id);
    sync_until_admins(home.path(), &bob, demote_group_id, [&alice, &bob]);

    let demoted = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "groups",
            "demote",
            demote_group_id,
            &bob,
        ],
    );
    assert_eq!(demoted["published"], 1);
    assert_eq!(
        demoted["group"]["admin_policy"]["admins"],
        serde_json::json!(sorted_accounts([&alice]))
    );

    sync_until_admins(home.path(), &bob, demote_group_id, [&alice]);
    let demoted_error = run_json_error(
        home.path(),
        &[
            "--account",
            &bob,
            "groups",
            "rename",
            demote_group_id,
            "nope",
        ],
    );
    assert_eq!(demoted_error["code"], "not_group_admin");
}

#[test]
fn group_members_invite_and_remove_flow_updates_projected_members() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    let carol = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    run_json(home.path(), &["--account", &carol, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let initial_members = run_json(
        home.path(),
        &["--account", &alice, "group", "members", group_id],
    );
    assert_eq!(
        member_accounts(&initial_members),
        sorted_accounts([&alice, &bob])
    );

    let invite = run_json(
        home.path(),
        &["--account", &alice, "group", "invite", group_id, &carol],
    );
    assert_eq!(invite["published"], 2);
    sync_until_member(home.path(), &bob, group_id, &carol);
    sync_until_joined(home.path(), test_relay_url(), &carol, group_id);

    let invited_members = run_json(
        home.path(),
        &["--account", &alice, "group", "members", group_id],
    );
    assert_eq!(
        member_accounts(&invited_members),
        sorted_accounts([&alice, &bob, &carol])
    );

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            "history",
            "stays",
        ],
    );
    sync_until_message(home.path(), test_relay_url(), &bob, "history stays");
    sync_until_message(home.path(), test_relay_url(), &carol, "history stays");

    let remove = run_json(
        home.path(),
        &["--account", &alice, "group", "remove", group_id, &bob],
    );
    assert_eq!(remove["published"], 1);
    run_json(home.path(), &["--account", &bob, "sync"]);
    run_json(home.path(), &["--account", &carol, "sync"]);

    let alice_members = run_json(
        home.path(),
        &["--account", &alice, "group", "members", group_id],
    );
    assert_eq!(
        member_accounts(&alice_members),
        sorted_accounts([&alice, &carol])
    );

    let carol_members = run_json(
        home.path(),
        &["--account", &carol, "group", "members", group_id],
    );
    assert_eq!(
        member_accounts(&carol_members),
        sorted_accounts([&alice, &carol])
    );

    let bob_group = run_json(home.path(), &["--account", &bob, "chats", "show", group_id]);
    assert_eq!(bob_group["group"]["profile"]["name"], "general");
    let bob_members = run_json(
        home.path(),
        &["--account", &bob, "group", "members", group_id],
    );
    assert_eq!(
        member_accounts(&bob_members),
        sorted_accounts([&alice, &carol])
    );
    let bob_history = run_json(
        home.path(),
        &["--account", &bob, "message", "list", "--group", group_id],
    );
    assert_eq!(bob_history["messages"][0]["plaintext"], "history stays");
}
