//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn daemon_background_stream_watch_records_brokered_preview() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let broker = spawn_quic_broker();

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "agent", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let stream_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let broker_candidate = format!("quic://127.0.0.1:{}", broker.addr.port());
    let started = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "start",
            group_id,
            "--stream-id",
            stream_id,
            "--quic-candidate",
            &broker_candidate,
        ],
    );
    let start_message_id = started["message_ids"][0]
        .as_str()
        .expect("start message id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dmd should start");
    wait_for_daemon(&socket);

    let watch = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "stream",
            "watch",
            group_id,
            "--stream-id",
            stream_id,
            "--insecure-local",
            "--background",
        ],
    );
    assert_eq!(watch["status"], "running");
    assert_eq!(watch["stream_id"], stream_id);
    assert!(watch["watch_id"].as_str().is_some_and(|id| !id.is_empty()));

    let sent = run_json_until_success(
        home.path(),
        &[
            "stream",
            "send",
            "--broker",
            "--connect",
            &broker.addr.to_string(),
            "--server-name",
            "localhost",
            "--insecure-local",
            "--stream-id",
            stream_id,
            "--start-event-id",
            start_message_id,
            "--chunk-bytes",
            "8",
            "daemon",
            "preview",
            "text",
        ],
        Duration::from_secs(5),
    );

    let status = poll_json_until(
        home.path(),
        &["daemon", "status"],
        Duration::from_secs(8),
        |status| {
            status
                .get("stream_watches")
                .and_then(Value::as_array)
                .and_then(|watches| watches.first())
                .is_some_and(|watch| watch["status"] == "completed")
        },
    );
    let stream_watch = status["stream_watches"][0].clone();
    assert_eq!(stream_watch["stream_id"], stream_id);
    assert_eq!(stream_watch["status"], "completed");
    assert_eq!(stream_watch["text"], "daemon preview text");
    assert_eq!(stream_watch["transcript_hash"], sent["transcript_hash"]);

    stop_daemon(&socket, &mut child);
}

#[test]
fn messages_subscribe_streams_messages_and_quic_previews_from_daemon() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let broker = spawn_quic_broker();

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "agent", &bob],
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
            group_id,
            "hello",
            "bob",
        ],
    );
    run_json(home.path(), &["--account", &bob, "sync"]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dmd should start");
    wait_for_daemon(&socket);

    let subscription = spawn_json_subscription(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "subscribe",
            group_id,
            "--limit",
            "20",
        ],
    );
    let initial = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "InitialMessage"
            && line["result"]["type"] == "message"
            && line["result"]["message"]["plaintext"] == "hello bob"
    });
    assert_eq!(initial["result"]["message"]["group_id"], group_id);

    let stream_id = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let broker_candidate = format!("quic://127.0.0.1:{}", broker.addr.port());
    let started = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "start",
            group_id,
            "--stream-id",
            stream_id,
            "--quic-candidate",
            &broker_candidate,
        ],
    );
    let start_message_id = started["message_ids"][0]
        .as_str()
        .expect("start message id");

    subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "AgentStreamStarted"
            && line["result"]["type"] == "agent_stream_start"
            && line["result"]["message"]["agent_text_stream"]["kind"] == "start"
            && line["result"]["message"]["agent_text_stream"]["stream_id"] == stream_id
    });

    let watch = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "stream",
            "watch",
            group_id,
            "--stream-id",
            stream_id,
            "--insecure-local",
            "--background",
        ],
    );
    assert_eq!(watch["status"], "running");

    let sent = run_json_until_success(
        home.path(),
        &[
            "stream",
            "send",
            "--broker",
            "--connect",
            &broker.addr.to_string(),
            "--server-name",
            "localhost",
            "--insecure-local",
            "--stream-id",
            stream_id,
            "--start-event-id",
            start_message_id,
            "--chunk-bytes",
            "8",
            "daemon",
            "preview",
            "line",
        ],
        Duration::from_secs(5),
    );

    let delta = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "AgentStreamDelta"
            && line["result"]["type"] == "agent_stream_delta"
            && line["result"]["agent_stream_delta"]["stream_id"] == stream_id
    });
    assert_eq!(delta["result"]["agent_stream_delta"]["group_id"], group_id);
    assert!(
        delta["result"]["agent_stream_delta"]["text"]
            .as_str()
            .is_some_and(|text| !text.is_empty())
    );

    let preview = subscription.wait_for(Duration::from_secs(15), |line| {
        line["result"]["trigger"] == "StreamPreviewCompleted"
            && line["result"]["type"] == "stream_preview"
            && line["result"]["stream_preview"]["stream_id"] == stream_id
    });
    assert_eq!(
        preview["result"]["stream_preview"]["text"],
        "daemon preview line"
    );
    assert_eq!(
        preview["result"]["stream_preview"]["transcript_hash"],
        sent["transcript_hash"]
    );

    drop(subscription);
    stop_daemon(&socket, &mut child);
}

#[test]
fn tui_style_stream_compose_auto_watches_and_publishes_final_message() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let broker = spawn_quic_broker();

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "agent", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    run_json(home.path(), &["--account", &bob, "sync"]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dmd should start");
    wait_for_daemon(&socket);

    let subscription = spawn_json_subscription(
        home.path(),
        &[
            "--account",
            &bob,
            "messages",
            "subscribe",
            group_id,
            "--limit",
            "20",
        ],
    );
    subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "SubscriptionReady"
            && line["result"]["type"] == "subscription_ready"
            && line["result"]["group_id"] == group_id
    });

    let stream_id = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let broker_candidate = format!("quic://127.0.0.1:{}", broker.addr.port());
    let opened = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "compose-open",
            group_id,
            "--stream-id",
            stream_id,
            "--quic-candidate",
            &broker_candidate,
            "--insecure-local",
            "--chunk-bytes",
            "8",
        ],
    );
    assert_eq!(opened["status"], "streaming");
    assert_eq!(opened["stream_id"], stream_id);

    subscription.wait_for(Duration::from_secs(20), |line| {
        matches!(
            line["result"]["trigger"].as_str(),
            Some("AgentStreamStarted" | "InitialMessage")
        ) && line["result"]["type"] == "agent_stream_start"
            && line["result"]["message"]["agent_text_stream"]["stream_id"] == stream_id
    });

    poll_json_until(
        home.path(),
        &["daemon", "status"],
        Duration::from_secs(8),
        |status| {
            status
                .get("stream_watches")
                .and_then(Value::as_array)
                .is_some_and(|watches| {
                    watches.iter().any(|watch| {
                        watch["account"] == bob
                            && watch["group_id"] == group_id
                            && watch["stream_id"] == stream_id
                            && watch["status"] == "running"
                    })
                })
        },
    );

    subscription.wait_for(Duration::from_secs(20), |line| {
        matches!(
            line["result"]["trigger"].as_str(),
            Some("InitialStreamPreview" | "StreamPreviewUpdated")
        ) && line["result"]["type"] == "stream_preview"
            && line["result"]["stream_preview"]["stream_id"] == stream_id
            && line["result"]["stream_preview"]["status"] == "running"
    });

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "compose-append",
            "--stream-id",
            stream_id,
            "hello ",
        ],
    );
    let delta = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "AgentStreamDelta"
            && line["result"]["type"] == "agent_stream_delta"
            && line["result"]["agent_stream_delta"]["stream_id"] == stream_id
            && line["result"]["agent_stream_delta"]["text"] == "hello "
    });
    assert_eq!(delta["result"]["agent_stream_delta"]["group_id"], group_id);

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "compose-append",
            "--stream-id",
            stream_id,
            "world",
        ],
    );
    let finished = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "compose-finish",
            "--stream-id",
            stream_id,
        ],
    );
    assert_eq!(finished["status"], "finished");
    assert_eq!(finished["text"], "hello world");
    assert_eq!(finished["chunk_count"], 2);
    assert!(finished["transcript_hash"].as_str().is_some());

    let mut preview = None;
    let mut final_marker = None;
    subscription.wait_until(Duration::from_secs(20), |line| {
        if line["result"]["trigger"] == "StreamPreviewCompleted"
            && line["result"]["type"] == "stream_preview"
            && line["result"]["stream_preview"]["stream_id"] == stream_id
        {
            preview = Some(line.clone());
        }
        // The kind-9 stream-final now arrives as a normal timeline message; it
        // is still classified as `agent_stream_final` via its stream tags.
        if line["result"]["trigger"] == "MessageReceived"
            && line["result"]["type"] == "agent_stream_final"
            && line["result"]["message"]["agent_text_stream"]["stream_id"] == stream_id
        {
            final_marker = Some(line.clone());
        }
        preview.is_some() && final_marker.is_some()
    });
    let preview = preview.expect("completed stream preview");
    assert_eq!(preview["result"]["stream_preview"]["text"], "hello world");
    assert_eq!(
        preview["result"]["stream_preview"]["transcript_hash"],
        finished["transcript_hash"]
    );
    let final_marker = final_marker.expect("agent stream final marker");
    assert_eq!(
        final_marker["result"]["message"]["agent_text_stream"]["final_text_or_reference"],
        "hello world"
    );

    drop(subscription);
    stop_daemon(&socket, &mut child);
}

#[test]
fn daemon_defaults_create_identities_and_stream_without_manual_sync_or_relay_env() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let broker = spawn_quic_broker();

    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dmd should start");
    wait_for_daemon(&socket);

    let alice_created = run_json_without_relay(home.path(), &["create-identity"]);
    let bob_created = run_json_without_relay(home.path(), &["create-identity"]);
    assert_eq!(alice_created["relay_lists"]["complete"], true);
    assert_eq!(bob_created["relay_lists"]["complete"], true);
    assert_eq!(alice_created["key_package"]["published"], true);
    assert_eq!(bob_created["key_package"]["published"], true);
    assert!(
        alice_created["key_package"]["bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert!(
        bob_created["key_package"]["bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    let alice = alice_created["account_id"].as_str().expect("alice id");
    let bob = bob_created["account_id"].as_str().expect("bob id");

    let created_group = run_json_without_relay(
        home.path(),
        &["--account", alice, "groups", "create", "agent", bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    poll_json_without_relay_until(
        home.path(),
        &["--account", bob, "chats", "list"],
        Duration::from_secs(20),
        |chats| {
            chats
                .get("chats")
                .and_then(Value::as_array)
                .is_some_and(|chats| chats.iter().any(|chat| chat["group_id"] == group_id))
        },
    );

    let subscription = spawn_json_subscription_without_relay(
        home.path(),
        &[
            "--account",
            bob,
            "messages",
            "subscribe",
            group_id,
            "--limit",
            "20",
        ],
    );
    run_json_without_relay(
        home.path(),
        &[
            "--account",
            alice,
            "messages",
            "send",
            group_id,
            "stream",
            "readiness",
            "probe",
        ],
    );
    subscription.wait_for(Duration::from_secs(15), |line| {
        matches!(
            line["result"]["trigger"].as_str(),
            Some("MessageReceived" | "InitialMessage")
        ) && line["result"]["type"] == "message"
            && line["result"]["message"]["plaintext"] == "stream readiness probe"
    });

    let stream_id = "abababababababababababababababababababababababababababababababab";
    let broker_candidate = format!("quic://127.0.0.1:{}", broker.addr.port());
    let opened = run_json_without_relay(
        home.path(),
        &[
            "--account",
            alice,
            "stream",
            "compose-open",
            group_id,
            "--stream-id",
            stream_id,
            "--quic-candidate",
            &broker_candidate,
            "--insecure-local",
            "--chunk-bytes",
            "8",
        ],
    );
    assert_eq!(opened["status"], "streaming");

    subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "AgentStreamStarted"
            && line["result"]["type"] == "agent_stream_start"
            && line["result"]["message"]["agent_text_stream"]["stream_id"] == stream_id
    });

    run_json_without_relay(
        home.path(),
        &[
            "--account",
            alice,
            "stream",
            "compose-append",
            "--stream-id",
            stream_id,
            "hello ",
        ],
    );
    run_json_without_relay(
        home.path(),
        &[
            "--account",
            alice,
            "stream",
            "compose-append",
            "--stream-id",
            stream_id,
            "stream",
        ],
    );
    let finished = run_json_without_relay(
        home.path(),
        &[
            "--account",
            alice,
            "stream",
            "compose-finish",
            "--stream-id",
            stream_id,
        ],
    );
    assert_eq!(finished["status"], "finished");
    assert_eq!(finished["text"], "hello stream");

    let mut delta_seen = false;
    let mut preview = None;
    let mut final_marker = None;
    subscription.wait_until(Duration::from_secs(20), |line| {
        if line["result"]["trigger"] == "AgentStreamDelta"
            && line["result"]["type"] == "agent_stream_delta"
            && line["result"]["agent_stream_delta"]["stream_id"] == stream_id
        {
            delta_seen = true;
        }
        if line["result"]["trigger"] == "StreamPreviewCompleted"
            && line["result"]["type"] == "stream_preview"
            && line["result"]["stream_preview"]["stream_id"] == stream_id
        {
            preview = Some(line.clone());
        }
        // The kind-9 stream-final now arrives as a normal timeline message; it
        // is still classified as `agent_stream_final` via its stream tags.
        if line["result"]["trigger"] == "MessageReceived"
            && line["result"]["type"] == "agent_stream_final"
            && line["result"]["message"]["agent_text_stream"]["stream_id"] == stream_id
        {
            final_marker = Some(line.clone());
        }
        delta_seen && preview.is_some() && final_marker.is_some()
    });
    let preview = preview.expect("completed stream preview");
    assert_eq!(preview["result"]["stream_preview"]["text"], "hello stream");
    let final_marker = final_marker.expect("agent stream final marker");
    assert_eq!(
        final_marker["result"]["message"]["agent_text_stream"]["final_text_or_reference"],
        "hello stream"
    );

    drop(subscription);
    stop_daemon(&socket, &mut child);
}

#[test]
fn chats_subscribe_streams_initial_chat_rows_from_daemon() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    sync_until_joined(home.path(), test_relay_url(), &bob, group_id);

    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dmd should start");
    wait_for_daemon(&socket);

    let subscription =
        spawn_json_subscription(home.path(), &["--account", &bob, "chats", "subscribe"]);
    let initial = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "InitialChat"
            && line["result"]["type"] == "chat"
            && line["result"]["chat"]["group_id"] == group_id
    });
    assert_eq!(initial["result"]["chat"]["profile"]["name"], "general");

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "groups",
            "rename",
            group_id,
            "general-renamed",
        ],
    );
    let updated = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "ChatUpdated"
            && line["result"]["type"] == "chat"
            && line["result"]["chat"]["group_id"] == group_id
            && line["result"]["chat"]["profile"]["name"] == "general-renamed"
    });
    assert_eq!(updated["result"]["group_id"], group_id);

    drop(subscription);
    stop_daemon(&socket, &mut child);
}

#[test]
fn groups_subscribe_state_streams_initial_group_state_from_daemon() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dmd should start");
    wait_for_daemon(&socket);

    let subscription = spawn_json_subscription(
        home.path(),
        &["--account", &alice, "groups", "subscribe-state", group_id],
    );
    let initial = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "InitialGroupState"
            && line["result"]["type"] == "group_state"
            && line["result"]["group"]["group_id"] == group_id
    });
    assert_eq!(initial["result"]["group"]["profile"]["name"], "general");
    assert_eq!(initial["result"]["mls"]["group_id"], group_id);
    assert_eq!(initial["result"]["mls"]["member_count"], 2);

    run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "groups",
            "rename",
            group_id,
            "general-renamed",
        ],
    );
    let updated = subscription.wait_for(Duration::from_secs(20), |line| {
        line["result"]["trigger"] == "GroupStateUpdated"
            && line["result"]["type"] == "group_state"
            && line["result"]["group"]["group_id"] == group_id
            && line["result"]["group"]["profile"]["name"] == "general-renamed"
    });
    assert_eq!(updated["result"]["group_id"], group_id);
    assert_eq!(updated["result"]["mls"]["group_id"], group_id);
    assert_eq!(updated["result"]["mls"]["member_count"], 2);

    drop(subscription);
    stop_daemon(&socket, &mut child);
}

#[test]
fn daemon_executes_cli_commands_over_socket() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .spawn()
        .expect("dmd should start");

    wait_for_daemon(&socket);

    let output = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["account", "create"])
        .output()
        .expect("dm should start");
    assert!(
        output.status.success(),
        "dm failed\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["result"]["local_signing"], true);
    assert!(
        value["result"]["npub"]
            .as_str()
            .unwrap()
            .starts_with("npub1")
    );

    stop_daemon(&socket, &mut child);
}

#[test]
#[cfg(unix)]
fn daemon_socket_path_is_private() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .spawn()
        .expect("dmd should start");

    wait_for_daemon(&socket);

    let socket_mode = socket
        .metadata()
        .expect("daemon socket metadata")
        .permissions()
        .mode()
        & 0o777;
    let socket_dir_mode = socket
        .parent()
        .expect("socket parent")
        .metadata()
        .expect("daemon socket dir metadata")
        .permissions()
        .mode()
        & 0o777;
    let pid_mode = home
        .path()
        .join("dev")
        .join("dmd.pid")
        .metadata()
        .expect("daemon pid metadata")
        .permissions()
        .mode()
        & 0o777;

    stop_daemon(&socket, &mut child);

    assert_eq!(socket_dir_mode, 0o700);
    assert_eq!(socket_mode, 0o600);
    assert_eq!(pid_mode, 0o600);
}

#[test]
fn daemon_refuses_reset_over_socket() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--discovery-relays")
        .arg(test_relay_url())
        .arg("--default-account-relays")
        .arg(test_relay_url())
        .arg("--secret-store")
        .arg("file")
        .spawn()
        .expect("dmd should start");

    wait_for_daemon(&socket);

    let output = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["reset", "--confirm"])
        .output()
        .expect("dm reset should start");
    assert!(
        !output.status.success(),
        "daemon reset unexpectedly succeeded\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["error"]["code"], "daemon_forbidden");
    assert_eq!(value["error"]["command"], "reset");
    assert!(home.path().exists(), "daemon home should not be deleted");

    stop_daemon(&socket, &mut child);
}

#[test]
fn daemon_start_status_execute_and_stop_are_user_facing_commands() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");

    let start = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--secret-store")
        .arg("file")
        .arg("--json")
        .args([
            "daemon",
            "start",
            "--discovery-relays",
            test_relay_url(),
            "--default-account-relays",
            test_relay_url(),
        ])
        .output()
        .expect("dm daemon start should run");
    assert!(
        start.status.success(),
        "daemon start failed\n{}",
        command_output_summary(&start)
    );

    let status = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["daemon", "status"])
        .output()
        .expect("dm daemon status should run");
    assert!(
        status.status.success(),
        "daemon status failed\n{}",
        command_output_summary(&status)
    );
    let status_json: Value =
        serde_json::from_slice(&status.stdout).expect("status stdout should be JSON");
    assert_eq!(status_json["result"]["running"], true);
    assert!(status_json["result"]["pid"].as_u64().is_some());
    assert!(status_json["result"]["pid_file"].as_str().is_some());
    assert!(status_json["result"].get("sync_interval_ms").is_none());
    assert!(status_json["result"].get("last_sync").is_none());
    assert!(status_json["result"].get("last_runtime_activity").is_some());

    let alice_created = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["create-identity"])
        .output()
        .expect("dm create-identity should run through daemon");
    assert!(
        alice_created.status.success(),
        "daemon execute failed\n{}",
        command_output_summary(&alice_created)
    );
    let created_json: Value =
        serde_json::from_slice(&alice_created.stdout).expect("created stdout should be JSON");
    assert_eq!(created_json["result"]["local_signing"], true);
    assert_eq!(created_json["result"]["key_package"]["published"], true);
    let alice = created_json["result"]["account_id"]
        .as_str()
        .expect("alice account id");

    let bob_created = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["create-identity"])
        .output()
        .expect("dm second create-identity should run through daemon");
    assert!(
        bob_created.status.success(),
        "daemon second create failed\n{}",
        command_output_summary(&bob_created)
    );
    let bob_created_json: Value =
        serde_json::from_slice(&bob_created.stdout).expect("bob created stdout should be JSON");
    let bob = bob_created_json["result"]["account_id"]
        .as_str()
        .expect("bob account id");

    let group_created = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--account")
        .arg(alice)
        .arg("--json")
        .args(["groups", "create", "agent", bob])
        .output()
        .expect("dm groups create should run through daemon");
    assert!(
        group_created.status.success(),
        "daemon group create failed\n{}",
        command_output_summary(&group_created)
    );

    let whoami = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["whoami"])
        .output()
        .expect("dm whoami should run through daemon");
    assert!(
        whoami.status.success(),
        "daemon whoami failed\n{}",
        command_output_summary(&whoami)
    );
    let whoami_json: Value = serde_json::from_slice(&whoami.stdout).expect("whoami stdout JSON");
    assert_eq!(
        whoami_json["result"]["accounts"]
            .as_array()
            .expect("accounts")
            .len(),
        2
    );

    let stop = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["daemon", "stop"])
        .output()
        .expect("dm daemon stop should run");
    assert!(
        stop.status.success(),
        "daemon stop failed\n{}",
        command_output_summary(&stop)
    );
}

#[test]
fn daemon_runtime_subscriptions_update_local_accounts_without_manual_sync() {
    let home = tempfile::tempdir().expect("tempdir");
    let socket = home.path().join("dev").join("dmd.sock");
    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);
    let created_group = run_json(
        home.path(),
        &["--account", &alice, "group", "create", "general", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");

    let start = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--home")
        .arg(home.path())
        .arg("--socket")
        .arg(&socket)
        .arg("--secret-store")
        .arg("file")
        .arg("--json")
        .args([
            "daemon",
            "start",
            "--discovery-relays",
            test_relay_url(),
            "--default-account-relays",
            test_relay_url(),
        ])
        .output()
        .expect("dm daemon start should run");
    assert!(
        start.status.success(),
        "daemon start failed\n{}",
        command_output_summary(&start)
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_group = false;
    while Instant::now() < deadline {
        let output = Command::new(env!("CARGO_BIN_EXE_dm"))
            .arg("--socket")
            .arg(&socket)
            .arg("--account")
            .arg(&bob)
            .arg("--json")
            .args(["chats", "list"])
            .output()
            .expect("dm chats list should run through daemon");
        if output.status.success() {
            let value: Value =
                serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
            if value["result"]["chats"]
                .as_array()
                .is_some_and(|chats| chats.iter().any(|chat| chat["group_id"] == group_id))
            {
                saw_group = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let _ = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--socket")
        .arg(&socket)
        .arg("--json")
        .args(["daemon", "stop"])
        .output();

    assert!(
        saw_group,
        "daemon runtime subscriptions did not join Bob to the group"
    );
}
