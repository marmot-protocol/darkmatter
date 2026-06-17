#!/usr/bin/env bash
# Container entrypoint for the OpenClaw Marmot phone test: start dm-agent, bootstrap
# the agent account, install + enable the Marmot channel plugin, run the gateway.
# Mirrors scripts/hermes_marmot_container_entrypoint.sh.
set -euo pipefail

socket_dir="$(dirname "$MARMOT_AGENT_SOCKET")"
install -d -m "${MARMOT_AGENT_SOCKET_DIR_MODE:-0770}" "$socket_dir"
install -d -m 0700 "$MARMOT_HOME" "$OPENCLAW_HOME"

if [ ! -f "$MARMOT_AGENT_AUTH_TOKEN_FILE" ]; then
    ( umask 0177; head -c 32 /dev/urandom | xxd -p -c 64 > "$MARMOT_AGENT_AUTH_TOKEN_FILE" )
fi

relay_args=()
IFS=',' read -ra relays <<< "${MARMOT_RELAYS:-}"
for relay in "${relays[@]}"; do
    [ -n "$relay" ] && relay_args+=(--relay "$relay")
done

dm-agent \
    --home "$MARMOT_HOME" \
    --socket "$MARMOT_AGENT_SOCKET" \
    --auth-token-file "$MARMOT_AGENT_AUTH_TOKEN_FILE" \
    --socket-dir-mode "${MARMOT_AGENT_SOCKET_DIR_MODE:-0770}" \
    --socket-mode "${MARMOT_AGENT_SOCKET_MODE:-0660}" \
    "${relay_args[@]}" &

# Wait for the control socket before bootstrapping.
for _ in $(seq 1 30); do
    [ -S "$MARMOT_AGENT_SOCKET" ] && break
    sleep 1
done

dm-agent bootstrap \
    --home "$MARMOT_HOME" \
    --socket "$MARMOT_AGENT_SOCKET" \
    --auth-token-file "$MARMOT_AGENT_AUTH_TOKEN_FILE" \
    --qr || true

openclaw plugins install /work/darkmatter/integrations/openclaw/marmot || true
openclaw plugins enable marmot || true

exec openclaw gateway run
