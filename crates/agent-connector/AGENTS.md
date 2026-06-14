# AGENTS.md - agent-connector

Local Marmot agent connector daemon; ships the `dm-agent` binary.

## Scope

- Own `serve_socket`/`AgentConnector` and the `dm-agent` Unix-socket daemon that bridges the `agent-control` protocol
  and `agent-stream-compose` previews to `MarmotApp`/`MarmotAppRuntime`.
- Own `dm-agent bootstrap`, which creates or reuses a local agent account through the running control socket and prints
  phone invite details (`npub`, `nprofile`, optional terminal QR).
- Own connector socket binding and permission hardening (`bind_connector_socket`, `default_socket_path`).
- Keep agent-facing wire types in `agent-control` and stream composition in `agent-stream-compose`; this crate is the
  process glue, not the protocol or composition owner.

## Key files

- `src/lib.rs` — `serve_socket`, `AgentConnectorConfig`, and the `AgentConnector` struct plus its (still-monolithic)
  request-handling/invite-policy `impl`. Crate-internal constants live here as `pub(crate)`.
- `src/error.rs` — `ConnectorError` and its `code`/`client_message`/`privacy_safe_code` projections.
- `src/socket.rs` — socket path/bind/hardening (`default_socket_path`, `bind_connector_socket*`, stale-socket recovery).
- `src/allowlist.rs` — `AllowlistStore`/`AllowlistRecord` per-account welcomer allowlist persistence.
- `src/stream_session.rs` — `StreamSessionStore`/`ActiveStreamSession` and the `DebugFinalSendStore` recorder.
- `src/quic.rs` — QUIC broker candidate parsing, address resolution, and trust selection.
- `src/event_projection.rs` — runtime/debug event → control event projection, the `DeliveredInboundCursor`, and the
  `InboundCatchUpDriver`.
- `src/validation.rs` — control-plane/profile/hex validation helpers and the invite-policy retry-state holders.
- `src/bootstrap.rs` — `dm-agent bootstrap` flow.
- `src/tests.rs` — white-box test suite exercising the above `pub(crate)` internals.

## Verification

```sh
cargo test -p agent-connector
```
