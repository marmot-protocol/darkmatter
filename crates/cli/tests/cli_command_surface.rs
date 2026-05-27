//! Auto-extracted from the previous monolithic crates/cli/tests/cli.rs as part
//! of the CI restructuring described in issue #103. See crates/cli/tests/common
//! for the shared helper module.

mod common;

use common::*;

#[test]
fn whitenoise_command_surface_names_are_present() {
    let dm_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .arg("--help")
        .output()
        .expect("dm help should run");
    assert!(
        dm_help.status.success(),
        "{}",
        command_output_summary(&dm_help)
    );
    let dm_help = format!(
        "{}{}",
        String::from_utf8_lossy(&dm_help.stdout),
        String::from_utf8_lossy(&dm_help.stderr)
    );
    for (command, description) in [
        ("daemon", "Start, stop, and inspect"),
        ("debug", "Inspect local runtime diagnostics"),
        ("create-identity", "Create a new local signing identity"),
        ("login", "Import an nsec from stdin"),
        ("logout", "Log out and remove a local account"),
        ("whoami", "Show current account identities"),
        ("export-nsec", "Exporting private keys is disabled"),
        ("accounts", "Manage local account identities"),
        ("chats", "List chats and subscribe"),
        ("groups", "Create groups and manage membership"),
        ("media", "List media references"),
        ("messages", "Send, list, search"),
        ("follows", "Manage the local account follow list"),
        ("profile", "Show or publish"),
        ("relays", "Inspect and update account relay lists"),
        ("settings", "Read and update local CLI preferences"),
        ("users", "Look up known Nostr users"),
        ("keys", "Inspect and repair MLS KeyPackage"),
        ("stream", "Start, watch, finish"),
        ("reset", "Delete all local Darkmatter CLI data"),
    ] {
        assert!(dm_help.contains(command), "dm --help missing {command}");
        assert!(
            dm_help.contains(description),
            "dm --help missing description for {command}: {description}"
        );
    }
    assert!(
        !dm_help.contains("--relay"),
        "dm --help should not expose a global relay flag"
    );
    assert!(
        !dm_help.contains("notifications"),
        "dm --help should not expose placeholder notification commands"
    );

    let login_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["login", "--help"])
        .output()
        .expect("dm login help should run");
    assert!(
        login_help.status.success(),
        "{}",
        command_output_summary(&login_help)
    );
    let login_help = format!(
        "{}{}",
        String::from_utf8_lossy(&login_help.stdout),
        String::from_utf8_lossy(&login_help.stderr)
    );
    assert!(
        login_help.contains("--relay"),
        "dm login --help should expose the command-local relay override"
    );
    assert!(
        login_help.contains("--nsec-stdin"),
        "dm login --help should expose stdin-based nsec import"
    );

    let dmd_help = Command::new(env!("CARGO_BIN_EXE_dmd"))
        .arg("--help")
        .output()
        .expect("dmd help should run");
    assert!(
        dmd_help.status.success(),
        "{}",
        command_output_summary(&dmd_help)
    );
    let dmd_help = format!(
        "{}{}",
        String::from_utf8_lossy(&dmd_help.stdout),
        String::from_utf8_lossy(&dmd_help.stderr)
    );
    for flag in [
        "--data-dir",
        "--logs-dir",
        "--discovery-relays",
        "--default-account-relays",
    ] {
        assert!(dmd_help.contains(flag), "dmd --help missing {flag}");
    }
    assert!(
        !dmd_help.contains("--relay"),
        "dmd --help should match wnd-style relay defaults instead of singular --relay"
    );

    let daemon_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["daemon", "--help"])
        .output()
        .expect("dm daemon help should run");
    assert!(
        daemon_help.status.success(),
        "{}",
        command_output_summary(&daemon_help)
    );
    let daemon_help = format!(
        "{}{}",
        String::from_utf8_lossy(&daemon_help.stdout),
        String::from_utf8_lossy(&daemon_help.stderr)
    );
    assert!(
        !daemon_help.contains("sync-now"),
        "daemon sync-now should not be a user-facing command"
    );

    let daemon_start_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["daemon", "start", "--help"])
        .output()
        .expect("dm daemon start help should run");
    assert!(
        daemon_start_help.status.success(),
        "{}",
        command_output_summary(&daemon_start_help)
    );
    let daemon_start_help = format!(
        "{}{}",
        String::from_utf8_lossy(&daemon_start_help.stdout),
        String::from_utf8_lossy(&daemon_start_help.stderr)
    );
    for flag in ["--discovery-relays", "--default-account-relays"] {
        assert!(
            daemon_start_help.contains(flag),
            "dm daemon start --help missing {flag}"
        );
    }

    let messages_list_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["messages", "list", "--help"])
        .output()
        .expect("messages list help should run");
    assert!(
        messages_list_help.status.success(),
        "{}",
        command_output_summary(&messages_list_help)
    );
    let messages_list_help = format!(
        "{}{}",
        String::from_utf8_lossy(&messages_list_help.stdout),
        String::from_utf8_lossy(&messages_list_help.stderr)
    );
    for flag in [
        "--before",
        "--before-message-id",
        "--after",
        "--after-message-id",
    ] {
        assert!(
            messages_list_help.contains(flag),
            "dm messages list --help missing {flag}"
        );
    }

    let keys_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["keys", "--help"])
        .output()
        .expect("keys help should run");
    assert!(
        keys_help.status.success(),
        "{}",
        command_output_summary(&keys_help)
    );
    let keys_help = format!(
        "{}{}",
        String::from_utf8_lossy(&keys_help.stdout),
        String::from_utf8_lossy(&keys_help.stderr)
    );
    for expected in [
        "Republish the currently cached KeyPackage",
        "Force mint and publish a fresh replacement KeyPackage",
        "Check whether a user has relay lists",
        "Fetch and cache another user's KeyPackage",
    ] {
        assert!(
            keys_help.contains(expected),
            "dm keys --help missing {expected}"
        );
    }
    for stale in ["delete", "delete-all"] {
        assert!(
            !keys_help.contains(stale),
            "dm keys --help should not expose stale {stale}"
        );
    }

    let groups_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["groups", "--help"])
        .output()
        .expect("groups help should run");
    assert!(
        groups_help.status.success(),
        "{}",
        command_output_summary(&groups_help)
    );
    let groups_help = format!(
        "{}{}",
        String::from_utf8_lossy(&groups_help.stdout),
        String::from_utf8_lossy(&groups_help.stderr)
    );
    for stale in ["invites", "accept", "decline"] {
        assert!(
            !groups_help.contains(stale),
            "dm groups --help should not expose stale {stale}"
        );
    }

    for (args, hidden) in [
        (vec!["debug", "--help"], "ratchet-tree"),
        (vec!["chats", "--help"], "mute"),
    ] {
        let help = Command::new(env!("CARGO_BIN_EXE_dm"))
            .args(args)
            .output()
            .expect("nested help should run");
        assert!(help.status.success(), "{}", command_output_summary(&help));
        let help = format!(
            "{}{}",
            String::from_utf8_lossy(&help.stdout),
            String::from_utf8_lossy(&help.stderr)
        );
        assert!(
            !help.contains(hidden),
            "nested help should not expose stale {hidden}"
        );
    }

    let media_help = Command::new(env!("CARGO_BIN_EXE_dm"))
        .args(["media", "--help"])
        .output()
        .expect("media help should run");
    assert!(
        media_help.status.success(),
        "{}",
        command_output_summary(&media_help)
    );
    let media_help = format!(
        "{}{}",
        String::from_utf8_lossy(&media_help.stdout),
        String::from_utf8_lossy(&media_help.stderr)
    );
    for command in ["upload", "download", "list"] {
        assert!(
            media_help.contains(command),
            "media help should expose real {command}"
        );
    }
}

#[test]
fn run_json_until_child_exits_does_not_repeat_successful_command() {
    let home = tempfile::tempdir().expect("tempdir");
    let child = Command::new("sh")
        .args(["-c", "sleep 0.2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("child should start");
    let calls = std::cell::Cell::new(0);

    let (value, output) =
        run_json_until_child_exits(home.path(), child, Duration::from_secs(2), |_| {
            let next = calls.get() + 1;
            calls.set(next);
            assert_eq!(next, 1, "successful command must not be repeated");
            Ok(serde_json::json!({ "sent": true }))
        });

    assert_eq!(calls.get(), 1);
    assert!(output.status.success());
    assert_eq!(value["sent"], true);
}

#[test]
fn whitenoise_parity_commands_have_real_or_explicit_contracts() {
    let home = tempfile::tempdir().expect("tempdir");

    let alice = create_account(home.path());
    let bob = create_account(home.path());
    run_json(home.path(), &["--account", &bob, "keys", "publish"]);

    let settings = run_json(home.path(), &["settings", "show"]);
    assert_eq!(settings["theme"], "system");
    let settings = run_json(home.path(), &["settings", "theme", "dark"]);
    assert_eq!(settings["theme"], "dark");
    let settings = run_json(home.path(), &["settings", "language", "en"]);
    assert_eq!(settings["language"], "en");
    #[cfg(unix)]
    {
        let dev_dir = home.path().join("dev");
        let settings_path = dev_dir.join("settings.json");
        assert_eq!(
            dev_dir
                .metadata()
                .expect("settings dir metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            settings_path
                .metadata()
                .expect("settings file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    let health = run_json(home.path(), &["--account", &alice, "debug", "health"]);
    assert_eq!(health["healthy"], true);

    let created_group = run_json(
        home.path(),
        &["--account", &alice, "groups", "create", "parity", &bob],
    );
    let group_id = created_group["group_id"].as_str().expect("group id");
    let admins = run_json(
        home.path(),
        &["--account", &alice, "groups", "admins", group_id],
    );
    assert_eq!(admins["admins"][0]["admin_id"], alice);
    let relays = run_json(
        home.path(),
        &["--account", &alice, "groups", "relays", group_id],
    );
    assert!(!relays["relays"].as_array().expect("relays").is_empty());

    let export_error = run_json_error(home.path(), &["export-nsec", &alice]);
    assert_eq!(export_error["code"], "unsupported_command");
    assert_eq!(export_error["command"], "export-nsec");
    let media = run_json(
        home.path(),
        &["--account", &alice, "media", "list", group_id],
    );
    assert_eq!(media["media"], serde_json::json!([]));

    let logout = run_json(home.path(), &["logout", &bob]);
    assert_eq!(logout["logged_out"], true);
    let accounts = run_json(home.path(), &["accounts", "list"]);
    assert_eq!(accounts["accounts"].as_array().expect("accounts").len(), 1);
}

#[test]
fn legacy_or_duplicate_command_shapes_are_not_supported() {
    let home = tempfile::tempdir().expect("tempdir");

    assert_eq!(
        run_json_error(home.path(), &["key-package", "publish"])["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(home.path(), &["directory", "get", "--pubkey", "00"])["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(
            home.path(),
            &["account", "import", "alice", "--nsec", "nsec1"]
        )["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(home.path(), &["group", "list"])["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(home.path(), &["group", "show", "00"])["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(home.path(), &["keys", "publish", "--account", "bob"])["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(home.path(), &["group", "create", "--name", "general"])["code"],
        "usage"
    );
    assert_eq!(
        run_json_error(home.path(), &["group", "invite", "00", "--member", "bob"])["code"],
        "usage"
    );
}
