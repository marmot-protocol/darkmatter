// OpenClaw plugin runtime entry. Registers the Marmot channel and starts the
// inbound subscription. See README.md for setup.

import {
  defineChannelPluginEntry,
  type OpenClawPluginApi,
} from "openclaw/plugin-sdk/channel-core";

import { createMarmotChannelPlugin, MARMOT_CHANNEL_ID } from "./src/channel.js";
import { syncMarmotAllowlist } from "./src/inbound-runtime.js";

export default defineChannelPluginEntry({
  id: MARMOT_CHANNEL_ID,
  name: "Marmot",
  description: "End-to-end encrypted Marmot groups through the local dm-agent connector.",
  plugin: createMarmotChannelPlugin(),
  registerFull(api: OpenClawPluginApi) {
    // Mirror configured dm.allowFrom welcomers into dm-agent on startup.
    void syncMarmotAllowlist(api);
    // Inbound -> agent turn dispatch and live QUIC previews are not yet wired
    // into the OpenClaw turn kernel; they are wired and validated against the
    // docker `openclaw-gateway` harness (see docker-compose.yml). Until then the
    // channel sends durable outbound messages only, so do not start a no-op
    // inbound consumer that would silently swallow received messages.
    api.logger.info(
      "marmot: durable sends active; inbound->agent dispatch and live previews are pending gateway wiring",
    );
  },
});
