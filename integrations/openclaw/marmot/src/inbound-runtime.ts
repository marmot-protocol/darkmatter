// Inbound runtime wiring: started from the plugin entry's registerFull. Opens
// the dm-agent inbound subscription via MarmotInboundBridge and hands each
// mapped message to an agent-dispatch function.
//
// INTEGRATION SEAM: OpenClaw's inbound turn kernel
// (`runChannelInboundEvent` / `recordInboundSessionAndDispatchReply` /
// `buildChannelInboundEventContext`) assembles an agent turn from gateway
// runtime internals (the agent dispatcher, session store, agent id, delivery
// adapter) that only exist inside a running OpenClaw gateway. That wiring is
// exercised + validated by the docker phone-test against a live gateway, not by
// the in-package unit tests. The agent's reply is delivered back out through the
// channel message adapter registered by createMarmotChannelPlugin() (durable
// send / live preview). `dispatch` is injectable so the bridge handoff can be
// unit-tested without a gateway.

import { resolveSingleAccount } from "./account.js";
import type { MarmotAgentControlClient } from "./client.js";
import {
  clientForAccount,
  resolveMarmotAccount,
  type MarmotChannelAccountConfig,
  type ResolvedMarmotAccount,
} from "./config.js";
import { MarmotInboundBridge, type MarmotInboundMessage } from "./inbound.js";

/** Minimal logger surface (subset of OpenClaw's PluginLogger). */
interface InboundLogger {
  info: (message: string) => void;
  warn: (message: string) => void;
}

/** Minimal plugin-api surface used by the inbound runtime. */
export interface InboundPluginApi {
  pluginConfig?: Record<string, unknown> | undefined;
  logger: InboundLogger;
}

export type InboundAgentDispatcher = (message: MarmotInboundMessage) => void | Promise<void>;

export interface StartMarmotInboundOptions {
  dispatch?: InboundAgentDispatcher;
  signal?: AbortSignal;
  /** Override the control-client factory (tests inject a stub). */
  clientFactory?: (resolved: ResolvedMarmotAccount) => MarmotAgentControlClient;
}

/**
 * Start the inbound subscription. Returns a stop function that aborts the
 * subscription loop.
 */
export function startMarmotInbound(
  api: InboundPluginApi,
  options: StartMarmotInboundOptions = {},
): () => void {
  const controller = new AbortController();
  const signal = options.signal ?? controller.signal;
  const resolved = resolveMarmotAccount(
    (api.pluginConfig ?? {}) as MarmotChannelAccountConfig,
    null,
  );
  const client = (options.clientFactory ?? clientForAccount)(resolved);
  const dispatch = options.dispatch ?? defaultAgentDispatch(api);

  void (async () => {
    let accountIdHex: string;
    try {
      accountIdHex = resolved.marmotAccountIdHex ?? (await resolveSingleAccount(client));
    } catch {
      api.logger.warn("marmot: could not resolve an agent account for the inbound subscription");
      return;
    }
    const bridge = new MarmotInboundBridge(client, {
      accountIdHex,
      groupIdHex: resolved.groupIdHex ?? null,
      onMessage: dispatch,
      onResync: ({ droppedEvents }) => {
        api.logger.warn(
          `marmot: inbound resync required (${droppedEvents} broadcast slots dropped)`,
        );
      },
      onError: () => {
        api.logger.warn("marmot: inbound subscription dropped; reconnecting");
      },
    });
    await bridge.run(signal);
  })();

  return () => controller.abort();
}

function defaultAgentDispatch(api: InboundPluginApi): InboundAgentDispatcher {
  return () => {
    // See the INTEGRATION SEAM note above. Logged privacy-safe (no ids/text).
    api.logger.info("marmot: received an inbound message for the agent turn");
  };
}
