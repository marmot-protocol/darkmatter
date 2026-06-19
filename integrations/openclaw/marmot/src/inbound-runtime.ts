// Inbound runtime wiring + startup allowlist sync.
//
// `startMarmotInbound` runs the dm-agent inbound subscription and hands each
// mapped message to a real agent dispatcher (no production no-op fallback —
// consuming inbound without dispatching would silently swallow messages). The
// dispatcher in `src/dispatch.ts` drives the OpenClaw turn kernel; the plugin
// entry wires it in `registerFull`. End-to-end behavior is validated against the
// docker `openclaw-gateway` harness (it needs a running gateway + a model).
//
// `syncMarmotAllowlist` mirrors the configured `dm.allowFrom` welcomers into
// dm-agent's per-account allowlist so configured welcomers are accepted.

import { resolveSingleAccount } from "./account.js";
import { resolveMarmotChannelAccount } from "./channel.js";
import type { MarmotAgentControlClient } from "./client.js";
import { clientForAccount, type ResolvedMarmotAccount } from "./config.js";
import { MarmotInboundBridge, type MarmotInboundMessage } from "./inbound.js";
import {
  maybeHandleProfileOnboarding,
  ProfileNameOnboardingStore,
} from "./profile-onboarding.js";
import { syncAllowlist } from "./security.js";

/** Minimal logger surface (subset of OpenClaw's PluginLogger). */
interface InboundLogger {
  info: (message: string) => void;
  warn: (message: string) => void;
}

/** Minimal plugin-api surface used by the inbound runtime. */
export interface InboundPluginApi {
  /** Full OpenClaw config; the channel config lives at `channels.marmot`. */
  config: unknown;
  logger: InboundLogger;
}

type ClientFactory = (resolved: ResolvedMarmotAccount) => MarmotAgentControlClient;

function resolveAccount(api: InboundPluginApi): ResolvedMarmotAccount {
  return resolveMarmotChannelAccount(
    api.config as Parameters<typeof resolveMarmotChannelAccount>[0],
    null,
  );
}

export type InboundAgentDispatcher = (message: MarmotInboundMessage) => void | Promise<void>;

export interface StartMarmotInboundOptions {
  signal?: AbortSignal;
  /** Override the control-client factory (tests inject a stub). */
  clientFactory?: ClientFactory;
  /**
   * Configured OpenClaw agent name. When profile-name onboarding is enabled and
   * a name is present, it is inherited and published instead of asking in-chat.
   */
  configuredAgentName?: string | null;
}

/**
 * Run the dm-agent inbound subscription, dispatching each mapped message to
 * `dispatch`. Returns a stop function that aborts the loop. Requires a real
 * dispatcher — see the module note.
 */
export function startMarmotInbound(
  api: InboundPluginApi,
  dispatch: InboundAgentDispatcher,
  options: StartMarmotInboundOptions = {},
): () => void {
  const controller = new AbortController();
  // Always drive the loop off the internal controller so the returned stop() is
  // authoritative; forward an externally-supplied signal into it.
  if (options.signal) {
    if (options.signal.aborted) {
      controller.abort();
    } else {
      options.signal.addEventListener("abort", () => controller.abort(), { once: true });
    }
  }
  const signal = controller.signal;
  const resolved = resolveAccount(api);
  const client = (options.clientFactory ?? clientForAccount)(resolved);
  // One-time, opt-in public profile-name flow (default off). Runs ahead of the
  // agent turn so a consent prompt/reply isn't fed to the model.
  const onboardingStore = resolved.profileNameOnboarding
    ? new ProfileNameOnboardingStore(resolved.profileOnboardingStatePath)
    : null;

  void (async () => {
    let accountIdHex: string;
    try {
      accountIdHex = resolved.marmotAccountIdHex ?? (await resolveSingleAccount(client));
    } catch {
      api.logger.warn("marmot: could not resolve an agent account for the inbound subscription");
      return;
    }
    api.logger.info("marmot: inbound subscription established");
    const bridge = new MarmotInboundBridge(client, {
      accountIdHex,
      groupIdHex: resolved.groupIdHex ?? null,
      onMessage: async (message) => {
        if (onboardingStore) {
          const intercepted = await maybeHandleProfileOnboarding({
            store: onboardingStore,
            client,
            message: {
              accountIdHex: message.accountIdHex,
              groupIdHex: message.groupIdHex,
              messageIdHex: message.messageIdHex,
              text: message.text,
            },
            configuredName: options.configuredAgentName ?? null,
            logger: api.logger,
          }).catch(() => false); // never block dispatch on an onboarding error
          if (intercepted) {
            return;
          }
        }
        api.logger.info("marmot: inbound message received; dispatching agent turn");
        await dispatch(message);
      },
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

export interface SyncAllowlistOptions {
  clientFactory?: ClientFactory;
}

/**
 * Mirror the configured `dm.allowFrom` welcomers into dm-agent's allowlist for
 * the resolved account. No-op when no allow-from is configured, so a bare
 * deployment does not wipe an allowlist managed directly on dm-agent.
 */
export async function syncMarmotAllowlist(
  api: InboundPluginApi,
  options: SyncAllowlistOptions = {},
): Promise<void> {
  try {
    const resolved = resolveAccount(api);
    if (resolved.allowFrom.length === 0) {
      return;
    }
    const client = (options.clientFactory ?? clientForAccount)(resolved);
    const accountIdHex = resolved.marmotAccountIdHex ?? (await resolveSingleAccount(client));
    const result = await syncAllowlist(client, accountIdHex, resolved.allowFrom);
    api.logger.info(
      `marmot: welcomer allowlist synced (added ${result.added.length}, removed ${result.removed.length})`,
    );
  } catch {
    // Best-effort on startup: account/config resolution or the sync itself can
    // throw; keep it inside the guard so the voided caller can't reject.
    api.logger.warn("marmot: failed to sync the welcomer allowlist with dm-agent");
  }
}
