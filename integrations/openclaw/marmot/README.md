# OpenClaw Marmot Plugin

This directory is an [OpenClaw](https://docs.openclaw.ai) **channel plugin** for the
local `dm-agent` connector. OpenClaw runs the agent, model, tools, and channel
routing. `dm-agent` owns the Marmot account, MLS group state, Nostr transport,
durable encrypted sends, and QUIC live-preview stream records.

The plugin is intentionally thin and **control-plane only**: it speaks the
`marmot.agent-control.v1` newline-delimited JSON protocol to `dm-agent` over a
local Unix socket. It never opens a QUIC connection, encrypts a record, or talks
to a relay — all of that stays in `dm-agent`. It is the OpenClaw counterpart of
the Python Hermes plugin in [`../../hermes/marmot/`](../../hermes/marmot).

- Pinned OpenClaw SDK: **`openclaw@2026.6.8`** (`openclaw/plugin-sdk/*`).
- Toolchain: TypeScript, pnpm, Node ≥ 22.19, Vitest.

## Install (release)

Versioned `dm-agent` builds and this plugin are published as `dm-agent-v*`
GitHub pre-releases. OpenClaw must already be installed with `openclaw` on `PATH`.

```sh
DM_AGENT_VERSION=0.1.0
curl -fsSL "https://github.com/marmot-protocol/darkmatter/releases/download/dm-agent-v${DM_AGENT_VERSION}/install-openclaw-marmot.sh" | bash
# or install + bootstrap the agent account in one step:
curl -fsSL ".../install-openclaw-marmot.sh" | bash -s -- --bootstrap
```

The installer puts `dm-agent` in `~/.local/bin`, downloads the plugin tarball,
runs `openclaw plugins install`, and enables the `marmot` channel.

Then start the connector and bootstrap (same public relays as the phone app):

```sh
dm-agent --home ~/.marmot-agent \
  --relay wss://relay.eu.whitenoise.chat \
  --relay wss://relay.us.whitenoise.chat
dm-agent bootstrap --home ~/.marmot-agent --qr
openclaw gateway run
```

Invite the printed agent account from the phone app.

## Dev setup

```sh
just openclaw-dev-test                 # pnpm install + typecheck + vitest
just openclaw-dev-setup --print-env    # build + isolated dev root + helper scripts
just openclaw-dev-teardown --force     # remove the throwaway dev root
```

`openclaw-dev-setup` builds the plugin, prepares an isolated dev root under
`${TMPDIR:-/tmp}/openclaw-marmot-test`, and generates `run-dm-agent.sh`,
`run-openclaw-gateway.sh`, `smoke-plugin.sh`, and `env.sh`.

## Docker phone test

A Compose profile builds a container with `dm-agent`, OpenClaw, this plugin, and
`qrencode`. It starts `dm-agent` with `MARMOT_AGENT_ALLOW_ANY=1` so the first
phone invite lands without pre-seeding an allowlist (use an explicit allowlist
for a real deployment).

```sh
export OPENAI_API_KEY=...        # or ANTHROPIC_API_KEY / OPENROUTER_API_KEY / ...
just openclaw-phone-test-up
just openclaw-phone-test-bootstrap   # prints the agent npub/nprofile + QR
just openclaw-phone-test-logs
just openclaw-phone-test-down        # or -reset to wipe persisted data
```

## Configuration

Configure under `channels.marmot` in the OpenClaw config, or via `MARMOT_*`
environment variables (config wins). Keys mirror the Hermes plugin so one
`dm-agent` deployment can serve both gateways:

| Key (config) | Env | Default |
| --- | --- | --- |
| `home` | `MARMOT_HOME` | `~/.marmot` |
| `socketPath` | `MARMOT_AGENT_SOCKET` | `$MARMOT_HOME/dev/dm-agent.sock` |
| `authToken` | `MARMOT_AGENT_AUTH_TOKEN` | — |
| `authTokenFile` | `MARMOT_AGENT_AUTH_TOKEN_FILE` | — |
| `accountIdHex` | `MARMOT_ACCOUNT_ID_HEX` | sole local account |
| `groupIdHex` | `MARMOT_GROUP_ID_HEX` | — (no filter) |
| `quicCandidates` | `MARMOT_QUIC_CANDIDATES` | — (final-only) |
| `streaming` | — | `true` |
| `dm.policy` / `dm.allowFrom` | — | `allowlist` |

The default control socket is same-UID only (parent dir `0700`, socket `0600`,
no TCP listener). If OpenClaw and `dm-agent` run as different local users, start
`dm-agent` with `--auth-token-file` + group-readable socket modes (`0660`) and
set `MARMOT_AGENT_AUTH_TOKEN_FILE`. See
[`crates/agent-connector/README.md`](../../../crates/agent-connector/README.md).

## Behavior

- Inbound Marmot messages map to a channel turn with `chatId` = the Marmot group
  id and `userId` = the sender account id.
- Durable replies are sent verbatim as `kind: 9` messages via `send_final`; the
  adapter never merges or rewrites text across sends.
- Live previews map OpenClaw progressive draft updates to append-only
  `stream_append` records (`stream_begin` → `stream_append` → `stream_finalize`).
  A non-append-only update cancels the preview and sends the final verbatim. The
  transcript hash + chunk count are computed to match `dm-agent`'s own
  validation byte-for-byte (Rust-anchored parity test in `test/transcript.test.ts`).
- OpenClaw's per-account `dm.allowFrom` (hex account ids) is mirrored into
  `dm-agent`'s welcomer allowlist; `dm-agent` still performs welcomer-based
  post-join accept/decline.

### Integration status

The Marmot-side logic — transcript hashing, control client, durable send, live
preview state machine, inbound bridge, config/account/allowlist resolution — is
unit-tested in this package (`pnpm test`). Two seams are wired against the
OpenClaw runtime and validated by the **docker phone test** against a live
gateway (they cannot be exercised by the in-package unit tests):

1. **Inbound → agent turn** (`src/inbound-runtime.ts`): the bridge receives and
   maps inbound messages; handing them to OpenClaw's turn kernel uses gateway
   runtime internals available only inside a running gateway.
2. **Live-preview pipeline**: the preview state machine (`src/live.ts`) is
   driven by OpenClaw's streaming/draft pipeline.

## Tests

```sh
cd integrations/openclaw/marmot
pnpm install
pnpm typecheck
pnpm test
```
