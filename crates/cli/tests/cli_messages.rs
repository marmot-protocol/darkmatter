//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn media_upload_and_download_round_trip_through_blossom() {
    let home = tempfile::tempdir().expect("tempdir");
    let blossom = TestBlossom::new();

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "media", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let source_path = home.path().join("note.txt");
    let plaintext = b"hello encrypted cli media";
    std::fs::write(&source_path, plaintext).expect("write source media");
    let source_path = source_path.to_string_lossy().to_string();
    let upload = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "media",
            "upload",
            group_id,
            &source_path,
            "--send",
            "--message",
            "caption",
            "--server",
            blossom.url(),
        ],
    );
    let encrypted_hash = upload["encrypted_hash_hex"]
        .as_str()
        .expect("encrypted hash");
    let stored = blossom.blob(encrypted_hash).expect("stored encrypted blob");
    assert_ne!(stored, plaintext);
    let file_hash = upload["media"]["file_hash_hex"]
        .as_str()
        .expect("plaintext hash")
        .to_owned();

    run_json(home.path(), &["--account", &bob, "sync"]);
    let listed = run_json(home.path(), &["--account", &bob, "media", "list", group_id]);
    assert_eq!(listed["media"][0]["caption"], "caption");
    assert_eq!(listed["media"][0]["file_hash_hex"], file_hash);

    let output_path = home.path().join("downloaded-note.txt");
    let output_path_string = output_path.to_string_lossy().to_string();
    let download = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "media",
            "download",
            group_id,
            &file_hash,
            "--output",
            &output_path_string,
        ],
    );
    assert_eq!(download["output_path"], output_path_string);
    assert_eq!(
        std::fs::read(&output_path).expect("downloaded file"),
        plaintext
    );
}

#[test]
fn message_send_accepts_hyphen_leading_text_after_group_flag() {
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

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            "--starts-with-dash",
        ],
    );

    let bob_sync = sync_until_message(home.path(), test_relay_url(), &bob, "--starts-with-dash");
    assert_eq!(bob_sync["messages"][0]["plaintext"], "--starts-with-dash");
}

#[test]
fn messages_plural_send_and_list_are_the_canonical_message_surface() {
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

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "messages",
            "send",
            group_id,
            "plural",
            "surface",
        ],
    );
    sync_until_message(home.path(), test_relay_url(), &bob, "plural surface");

    let listed = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "list",
            group_id,
            "--limit",
            "20",
        ],
    );
    assert_message_plaintexts(&listed, &["plural surface"]);

    // NOTE: master added `timeline list/search` assertions here. They depend
    // on the `messages timeline` handler whose port into the decomposed
    // command modules was deferred during the merge (see commit ae68010).
    // Restore these checks alongside the handler port.

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "messages",
            "send",
            group_id,
            "another searchable line",
        ],
    );
    sync_until_message(
        home.path(),
        test_relay_url(),
        &bob,
        "another searchable line",
    );

    let search = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "search",
            group_id,
            "searchable",
        ],
    );
    assert_message_plaintexts(&search, &["another searchable line"]);
    assert_no_message_plaintext(&search, "plural surface");

    // NOTE: master added `timeline search` assertions here; see the matching
    // NOTE above. Restore alongside the handler port.

    let search_all = run_json(
        home.path(),
        &["--account", &bob, "messages", "search-all", "plural"],
    );
    assert_message_plaintexts(&search_all, &["plural surface"]);
}

#[test]
fn messages_react_unreact_and_delete_are_typed_app_messages() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "lifecycle", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    sync_until_joined(home.path(), test_relay_url(), &bob, group_id);

    let sent = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "messages",
            "send",
            group_id,
            "needs",
            "a",
            "reaction",
        ],
    );
    let target_message_id = sent["message_ids"][0].as_str().expect("message id");
    sync_until_message(home.path(), test_relay_url(), &bob, "needs a reaction");

    run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "react",
            group_id,
            target_message_id,
            "+",
        ],
    );
    // A reaction is now an inner kind-7 Nostr event: content is the emoji and an
    // `e` tag references the reacted-to message.
    let reaction_sync =
        sync_until_message_with_kind(home.path(), test_relay_url(), &alice, 7, target_message_id);
    let reaction = first_message_with_kind(&reaction_sync, 7).expect("reaction message");
    let reaction_message_id = reaction["message_id"]
        .as_str()
        .expect("reaction message id")
        .to_owned();
    assert_eq!(reaction["plaintext"], "+");
    assert_eq!(message_e_tag(reaction), Some(target_message_id));
    assert_eq!(reaction["agent_text_stream"], Value::Null);

    run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "unreact",
            group_id,
            target_message_id,
        ],
    );
    // Un-react is a NIP-25-style kind-5 delete of the reaction event id, so its
    // `e` tag points at the kind-7 reaction, not the original message.
    let unreact_sync = sync_until_message_with_kind(
        home.path(),
        test_relay_url(),
        &alice,
        5,
        &reaction_message_id,
    );
    let unreact = first_message_with_kind_and_target(&unreact_sync, 5, &reaction_message_id)
        .expect("unreact delete message");
    assert_eq!(message_e_tag(unreact), Some(reaction_message_id.as_str()));

    run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "delete",
            group_id,
            target_message_id,
        ],
    );
    // A delete is a kind-5 tombstone with empty content and an `e` tag.
    let delete_sync =
        sync_until_message_with_kind(home.path(), test_relay_url(), &alice, 5, target_message_id);
    let delete = first_message_with_kind_and_target(&delete_sync, 5, target_message_id)
        .expect("delete message");
    assert_eq!(delete["plaintext"], "");
    assert_eq!(message_e_tag(delete), Some(target_message_id));

    let retry = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "messages",
            "retry",
            group_id,
            target_message_id,
        ],
    );
    assert_eq!(retry["target_event_id"], target_message_id);
    assert_eq!(retry["retry_scope"], "group_convergence");
}

#[test]
fn local_group_message_workflow_runs_through_the_dm_contract() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let alice_profile = run_json(home.path(), &["--account", &alice, "profile", "show"]);
    let alice_display_name = alice_profile["profile"]["display_name"]
        .as_str()
        .expect("alice display name")
        .to_owned();
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    let bob_join = sync_until_joined(home.path(), test_relay_url(), &bob, group_id);
    assert_eq!(bob_join["joined_groups"][0], group_id);

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            "hello",
            "bob",
        ],
    );

    let bob_sync = sync_until_message(home.path(), test_relay_url(), &bob, "hello bob");
    assert_eq!(bob_sync["messages"][0]["from"], alice);
    assert_eq!(
        bob_sync["messages"][0]["from_display_name"],
        alice_display_name
    );
    assert_eq!(bob_sync["messages"][0]["group_id"], group_id);
    assert_eq!(bob_sync["messages"][0]["plaintext"], "hello bob");

    let bob_messages = run_json(home.path(), &["--account", &bob, "message", "list"]);
    assert_eq!(bob_messages["messages"][0]["from"], alice);
    assert_eq!(
        bob_messages["messages"][0]["from_display_name"],
        alice_display_name
    );
    assert_eq!(bob_messages["messages"][0]["group_id"], group_id);
    assert_eq!(bob_messages["messages"][0]["plaintext"], "hello bob");
}

#[test]
fn cli_can_inspect_projected_groups_messages_and_status() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    assert_eq!(created_group["profile"]["component_id"], 0x8001);
    assert_eq!(
        created_group["profile"]["component"],
        "marmot.group.profile.v1"
    );
    assert_eq!(created_group["profile"]["name"], "general");
    assert_eq!(
        created_group["image"]["component"],
        "marmot.group.blossom.image.v1"
    );
    assert_eq!(created_group["image"]["present"], false);
    assert_eq!(created_group["admin_policy"]["component_id"], 0x8003);
    assert_eq!(
        created_group["admin_policy"]["component"],
        "marmot.group.admin-policy.v1"
    );
    assert_eq!(
        created_group["admin_policy"]["admins"],
        serde_json::json!([alice])
    );
    run_json(home.path(), &["--account", &bob, "sync"]);

    let chats = run_json(home.path(), &["--account", &bob, "chats", "list"]);
    assert_eq!(chats["chats"][0]["group_id"], group_id);
    assert_eq!(chats["chats"][0]["profile"]["name"], "general");
    assert_eq!(
        chats["chats"][0]["admin_policy"]["admins"],
        serde_json::json!([alice])
    );

    let group = run_json(home.path(), &["--account", &bob, "chats", "show", group_id]);
    assert_eq!(group["group"]["group_id"], group_id);
    assert_eq!(group["group"]["profile"]["name"], "general");

    let group = run_json(
        home.path(),
        &["--account", &bob, "groups", "show", group_id],
    );
    assert_eq!(group["group"]["group_id"], group_id);
    assert_eq!(group["group"]["profile"]["name"], "general");
    assert_eq!(
        group["group"]["nostr_routing"]["component"],
        "marmot.transport.nostr.routing.v1"
    );
    assert_eq!(group["mls"]["epoch"], 1);
    assert_eq!(group["mls"]["member_count"], 2);

    let first_send = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            "first",
        ],
    );
    let first_message_id = first_send["message_ids"][0].as_str().expect("message id");
    let alice_messages = run_json(home.path(), &["--account", &alice, "message", "list"]);
    assert_eq!(alice_messages["messages"].as_array().unwrap().len(), 1);
    assert_eq!(alice_messages["messages"][0]["direction"], "sent");
    assert_eq!(
        alice_messages["messages"][0]["message_id"],
        first_message_id
    );
    assert_eq!(alice_messages["messages"][0]["from"], alice);
    assert_eq!(alice_messages["messages"][0]["plaintext"], "first");

    run_json(home.path(), &["--account", &alice, "sync"]);
    let alice_messages_after_echo =
        run_json(home.path(), &["--account", &alice, "message", "list"]);
    assert_eq!(
        alice_messages_after_echo["messages"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "author relay echoes should not duplicate a published outbound message"
    );

    let second_send = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            "second",
        ],
    );
    assert!(second_send["message_ids"][0].as_str().is_some());
    sync_until_message(home.path(), test_relay_url(), &bob, "second");

    let messages = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "message",
            "list",
            "--group",
            group_id,
            "--limit",
            "2",
        ],
    );
    assert_eq!(messages["messages"].as_array().unwrap().len(), 2);
    assert_message_plaintexts(&messages, &["first", "second"]);
    assert!(
        messages["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|message| message["direction"] == "received")
    );

    let status = run_json(home.path(), &["account", "status", &bob]);
    assert_eq!(status["counts"]["groups"], 1);
    assert_eq!(status["counts"]["messages"], 2);
    assert_eq!(status["secret_store"]["backend"], "file");
    assert_eq!(status["projections"]["account"]["exists"], true);
    assert_eq!(status["projections"]["account"]["encrypted"], true);
}

#[test]
fn three_user_message_lifecycle_covers_invite_remove_and_later_delivery() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    let carol = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    run_json(home.path(), &["--account", &carol, "keys", "publish"]);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "three-way", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "message",
            "send",
            "--group",
            group_id,
            "before",
            "carol",
        ],
    );
    let bob_sync = sync_until_message(home.path(), test_relay_url(), &bob, "before carol");
    assert_message_plaintexts(&bob_sync, &["before carol"]);

    let invite = run_json(
        home.path(),
        &["--account", &alice, "group", "invite", group_id, &carol],
    );
    assert_eq!(invite["published"], 2);
    run_json(home.path(), &["--account", &bob, "sync"]);
    let carol_join = sync_until_joined(home.path(), test_relay_url(), &carol, group_id);
    assert_eq!(carol_join["joined_groups"][0], group_id);

    run_json(
        home.path(),
        &[
            "--account",
            &carol,
            "message",
            "send",
            "--group",
            group_id,
            "carol",
            "joined",
        ],
    );
    let alice_after_carol =
        sync_until_message(home.path(), test_relay_url(), &alice, "carol joined");
    assert_message_plaintexts(&alice_after_carol, &["carol joined"]);
    let bob_after_carol = sync_until_message(home.path(), test_relay_url(), &bob, "carol joined");
    assert_message_plaintexts(&bob_after_carol, &["carol joined"]);

    let remove = run_json(
        home.path(),
        &["--account", &alice, "group", "remove", group_id, &bob],
    );
    assert_eq!(remove["published"], 1);
    run_json(home.path(), &["--account", &bob, "sync"]);
    run_json(home.path(), &["--account", &carol, "sync"]);

    run_json(
        home.path(),
        &[
            "--account",
            &carol,
            "message",
            "send",
            "--group",
            group_id,
            "after",
            "bob",
            "removed",
        ],
    );
    let alice_after_remove =
        sync_until_message(home.path(), test_relay_url(), &alice, "after bob removed");
    assert_message_plaintexts(&alice_after_remove, &["after bob removed"]);
    let bob_after_remove = run_json(home.path(), &["--account", &bob, "sync"]);
    assert_no_message_plaintext(&bob_after_remove, "after bob removed");

    let bob_messages = run_json(
        home.path(),
        &["--account", &bob, "message", "list", "--group", group_id],
    );
    assert_message_plaintexts(&bob_messages, &["before carol", "carol joined"]);
    assert_no_message_plaintext(&bob_messages, "after bob removed");

    let bob_send_error = run_json_error(
        home.path(),
        &[
            "--account",
            &bob,
            "message",
            "send",
            "--group",
            group_id,
            "removed",
            "sender",
        ],
    );
    assert_eq!(bob_send_error["code"], "engine_error");
}
