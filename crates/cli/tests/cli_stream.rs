//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn stream_send_and_receive_show_quic_text_content() {
    let home = tempfile::tempdir().expect("tempdir");
    let bind = free_udp_addr();
    let mut receiver = dm(home.path());
    receiver
        .args(["stream", "receive", "--bind", &bind])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let receiver = receiver.spawn().expect("stream receiver should start");
    wait_for_udp_listener(&bind, Duration::from_secs(5));

    let sent = run_json_until_success(
        home.path(),
        &[
            "stream",
            "send",
            "--connect",
            &bind,
            "--insecure-local",
            "--chunk-bytes",
            "5",
            "hello",
            "streaming",
        ],
        Duration::from_secs(5),
    );
    assert_eq!(sent["chunk_count"], 3);

    let output =
        wait_child_output_or_panic(receiver, Duration::from_secs(5), "stream receiver failed");
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], true);
    let result = &value["result"];
    assert_eq!(result["text"], "hello streaming");
    assert_eq!(result["chunk_count"], 3);
    assert_eq!(result["chunks"][0]["text"], "hello");
}

#[test]
fn stream_send_insecure_local_rejects_remote_endpoints() {
    let home = tempfile::tempdir().expect("tempdir");

    let error = run_json_error(
        home.path(),
        &[
            "stream",
            "send",
            "--connect",
            "203.0.113.10:4450",
            "--insecure-local",
            "hello",
        ],
    );

    assert_eq!(error["code"], "insecure_local_requires_loopback");

    let broker_error = run_json_error(
        home.path(),
        &[
            "stream",
            "send",
            "--broker",
            "--connect",
            "203.0.113.10:4450",
            "--insecure-local",
            "hello",
        ],
    );

    assert_eq!(broker_error["code"], "insecure_local_requires_loopback");
}

#[test]
fn stream_start_quic_chunks_and_final_payload_verify_through_mls_messages() {
    let home = tempfile::tempdir().expect("tempdir");
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

    let stream_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
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

    let bob_start_message = wait_until_projected_agent_stream_message(
        home.path(),
        test_relay_url(),
        &bob,
        group_id,
        stream_id,
        "start",
    );
    assert_eq!(bob_start_message["agent_text_stream"]["kind"], "start");
    assert_eq!(
        bob_start_message["agent_text_stream"]["stream_id"],
        stream_id
    );
    assert_eq!(
        bob_start_message["agent_text_stream"]["route"],
        "brokered_quic"
    );
    assert_eq!(
        bob_start_message["agent_text_stream"]["quic_candidates"],
        serde_json::json!([broker_candidate])
    );

    let mut watcher = dm(home.path());
    watcher
        .args([
            "--account",
            &bob,
            "stream",
            "watch",
            group_id,
            "--stream-id",
            stream_id,
            "--insecure-local",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let watcher = watcher.spawn().expect("stream watcher should start");
    let broker_addr = broker.addr.to_string();
    let (sent, output) =
        run_json_until_child_exits(home.path(), watcher, Duration::from_secs(60), |home| {
            try_run_json(
                home,
                &[
                    "stream",
                    "send",
                    "--broker",
                    "--connect",
                    &broker_addr,
                    "--server-name",
                    "localhost",
                    "--insecure-local",
                    "--stream-id",
                    stream_id,
                    "--start-event-id",
                    start_message_id,
                    "--chunk-bytes",
                    "5",
                    "--chunk-delay-ms",
                    "25",
                    "hello",
                    "anchored",
                    "stream",
                ],
            )
        });
    assert_eq!(sent["brokered"], true);
    assert!(
        output.status.success(),
        "stream watcher failed\n{}",
        command_output_summary(&output)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("stdout should be JSON");
    assert_eq!(value["ok"], true);
    let received = &value["result"];
    assert_eq!(received["brokered"], true);
    assert_eq!(received["stream_id"], stream_id);
    assert_eq!(received["text"], "hello anchored stream");
    assert_eq!(received["transcript_hash"], sent["transcript_hash"]);

    let finished = run_json(
        home.path(),
        &[
            "--account",
            &alice,
            "stream",
            "finish",
            group_id,
            "--stream-id",
            stream_id,
            "--start-event-id",
            start_message_id,
            "--transcript-hash",
            sent["transcript_hash"].as_str().expect("transcript hash"),
            "--chunk-count",
            &sent["chunk_count"].to_string(),
            "hello",
            "anchored",
            "stream",
        ],
    );
    assert_eq!(finished["agent_text_stream"]["kind"], "final");
    assert_eq!(
        finished["agent_text_stream"]["start_event_id"],
        start_message_id
    );

    let bob_final_message = wait_until_projected_agent_stream_message(
        home.path(),
        test_relay_url(),
        &bob,
        group_id,
        stream_id,
        "final",
    );
    assert_eq!(bob_final_message["agent_text_stream"]["kind"], "final");
    assert_eq!(
        bob_final_message["agent_text_stream"]["transcript_hash"],
        sent["transcript_hash"]
    );

    let verified = run_json(
        home.path(),
        &[
            "--account",
            &bob,
            "stream",
            "verify",
            group_id,
            "--stream-id",
            stream_id,
            "--transcript-hash",
            received["transcript_hash"].as_str().expect("received hash"),
            "--chunk-count",
            &received["chunk_count"].to_string(),
        ],
    );
    assert_eq!(verified["verified"], true);
    assert_eq!(verified["final_message"]["stream_id"], stream_id);
}
