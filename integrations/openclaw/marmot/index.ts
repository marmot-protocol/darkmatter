// OpenClaw plugin runtime entry. Registers the Marmot channel and starts the
// inbound subscription. See README.md for setup.

import {
  defineChannelPluginEntry,
  type OpenClawPluginApi,
} from "openclaw/plugin-sdk/channel-core";

import { createMarmotChannelPlugin, MARMOT_CHANNEL_ID } from "./src/channel.js";
import { startMarmotInbound } from "./src/inbound-runtime.js";

export default defineChannelPluginEntry({
  id: MARMOT_CHANNEL_ID,
  name: "Marmot",
  description: "End-to-end encrypted Marmot groups through the local dm-agent connector.",
  plugin: createMarmotChannelPlugin(),
  registerFull(api: OpenClawPluginApi) {
    startMarmotInbound(api);
  },
});
