// Inbound -> agent turn dispatch, modeled on the bundled OpenClaw Telegram
// channel (node_modules/openclaw/dist/bot-*.js): the channel owns its inbound
// loop and calls `runChannelInboundEvent` itself, with a `resolveTurn` whose
// `runDispatch` drives `dispatchReplyWithBufferedBlockDispatcher`. The agent's
// reply arrives as progressive `block` deliveries + a `final` via a
// `deliver(payload, info)` callback, which we map onto Marmot sends.
//
// The turn assembly (runChannelInboundEvent / buildChannelInboundEventContext /
// api.runtime.channel) is typechecked against the SDK but is validated
// end-to-end against the `openclaw-gateway` docker harness (it needs a running
// gateway + a model). The MarmotReplySink mapping below is unit-tested.

import {
  buildChannelInboundEventContext,
  runChannelInboundEvent,
} from "openclaw/plugin-sdk/channel-inbound";

import { NonAppendOnlyUpdateError } from "./append-only.js";
import type { MarmotAgentControlClient } from "./client.js";
import type { StreamMode } from "./config.js";
import type { MarmotInboundMessage } from "./inbound.js";
import { MarmotLivePreview, type StreamControlClient } from "./live.js";

// --- reply sink (unit-tested) -----------------------------------------------

/** Kind of a streamed reply delivery from the OpenClaw reply dispatcher. */
export interface ReplyDelivery {
  kind: "final" | "block" | "tool";
}

/** The subset of the reply payload the Marmot sink reads. */
export interface ReplyPayloadLike {
  text?: string;
}

export type MarmotSinkClient = Pick<MarmotAgentControlClient, "sendFinal"> & StreamControlClient;

export interface MarmotReplySinkOptions {
  client: MarmotSinkClient;
  accountIdHex: string;
  groupIdHex: string;
  replyToMessageIdHex?: string | null;
  streamMode: StreamMode;
  quicCandidates: string[];
  chunkBytes?: number;
}

/**
 * Maps the agent's streamed reply onto Marmot sends: progressive `block`
 * deliveries drive an append-only live QUIC preview; the `final` is committed
 * via `stream_finalize` when a preview is active and the final extends it, else
 * a plain `send_final`. A non-append-only update cancels the preview and falls
 * back to a verbatim `send_final` (mirrors the Hermes shim's behavior).
 */
export class MarmotReplySink {
  private preview: MarmotLivePreview | null = null;
  private previewAbandoned = false;

  constructor(private readonly options: MarmotReplySinkOptions) {}

  private get streamingEnabled(): boolean {
    return this.options.streamMode !== "off" && this.options.quicCandidates.length > 0;
  }

  private ensurePreview(): MarmotLivePreview {
    if (!this.preview) {
      this.preview = new MarmotLivePreview(this.options.client, {
        accountIdHex: this.options.accountIdHex,
        groupIdHex: this.options.groupIdHex,
        quicCandidates: this.options.quicCandidates,
        chunkBytes: this.options.chunkBytes,
      });
    }
    return this.preview;
  }

  private async abandonPreview(reason: string): Promise<void> {
    this.previewAbandoned = true;
    if (this.preview) {
      await this.preview.cancel(reason);
    }
  }

  private async sendFinal(text: string): Promise<void> {
    await this.options.client.sendFinal(
      this.options.accountIdHex,
      this.options.groupIdHex,
      text,
      this.options.replyToMessageIdHex ?? null,
    );
  }

  async deliver(payload: ReplyPayloadLike, info: ReplyDelivery): Promise<void> {
    const text = payload?.text ?? "";

    if (info.kind === "tool") {
      // v1: tool/progress chatter is not surfaced to Marmot.
      return;
    }

    if (info.kind === "block") {
      if (!this.streamingEnabled || this.previewAbandoned) {
        return; // folded into the final send
      }
      try {
        await this.ensurePreview().update(text);
      } catch (error) {
        if (error instanceof NonAppendOnlyUpdateError) {
          await this.abandonPreview("non_append_only");
        } else {
          throw error;
        }
      }
      return;
    }

    // final
    if (this.preview && this.preview.isActive && !this.previewAbandoned) {
      try {
        await this.preview.finalize(text);
        return;
      } catch (error) {
        if (error instanceof NonAppendOnlyUpdateError) {
          await this.abandonPreview("final_not_append_only");
        } else {
          throw error;
        }
      }
    }
    await this.sendFinal(text);
  }
}

// --- inbound turn dispatch (SDK-coupled; harness-validated) ------------------

/** Narrow view of `api.runtime.channel` (only the members we drive). */
export interface OpenClawChannelRuntime {
  routing: {
    resolveAgentRoute: (input: unknown) => {
      agentId: string;
      accountId: string;
      sessionKey: string;
    };
  };
  session: {
    resolveStorePath: (store?: string, opts?: unknown) => string;
    recordInboundSession: unknown;
  };
  reply: {
    dispatchReplyWithBufferedBlockDispatcher: (params: unknown) => Promise<unknown>;
  };
}

export interface MarmotDispatchDeps {
  /** Full OpenClaw config (`api.config`). */
  cfg: unknown;
  /** `api.runtime.channel`. */
  runtimeChannel: OpenClawChannelRuntime;
  client: MarmotSinkClient;
  streamMode: StreamMode;
  quicCandidates: string[];
  chunkBytes?: number;
}

/**
 * Build the inbound dispatcher: for each received Marmot message, resolve the
 * agent route, build the inbound context, and run it through the OpenClaw turn
 * kernel, delivering the agent's reply through a per-message MarmotReplySink.
 */
export function createMarmotInboundDispatcher(
  deps: MarmotDispatchDeps,
): (message: MarmotInboundMessage) => Promise<void> {
  return async (message) => {
    const route = deps.runtimeChannel.routing.resolveAgentRoute({
      cfg: deps.cfg,
      channel: "marmot",
      accountId: message.accountIdHex,
      peer: { kind: "group", id: message.groupIdHex },
    });

    const ctxPayload = buildChannelInboundEventContext({
      channel: "marmot",
      accountId: message.accountIdHex,
      messageId: message.messageIdHex,
      from: message.senderAccountIdHex,
      sender: { id: message.senderAccountIdHex },
      conversation: { kind: "group", id: message.groupIdHex },
      route: {
        agentId: route.agentId,
        accountId: route.accountId,
        routeSessionKey: route.sessionKey,
      },
      reply: { to: message.groupIdHex },
      message: { rawBody: message.text, bodyForAgent: message.text },
    });

    const storePath = deps.runtimeChannel.session.resolveStorePath();
    const sink = new MarmotReplySink({
      client: deps.client,
      accountIdHex: message.accountIdHex,
      groupIdHex: message.groupIdHex,
      streamMode: deps.streamMode,
      quicCandidates: deps.quicCandidates,
      chunkBytes: deps.chunkBytes,
    });

    await runChannelInboundEvent({
      channel: "marmot",
      accountId: message.accountIdHex,
      raw: message,
      adapter: {
        ingest: () => ({
          id: message.messageIdHex,
          rawText: message.text,
          textForAgent: message.text,
        }),
        resolveTurn: () => ({
          channel: "marmot",
          accountId: message.accountIdHex,
          routeSessionKey: route.sessionKey,
          storePath,
          ctxPayload,
          recordInboundSession: deps.runtimeChannel.session.recordInboundSession as never,
          runDispatch: () =>
            deps.runtimeChannel.reply.dispatchReplyWithBufferedBlockDispatcher({
              ctx: ctxPayload,
              cfg: deps.cfg,
              dispatcherOptions: {
                deliver: (payload: ReplyPayloadLike, info: ReplyDelivery) =>
                  sink.deliver(payload, info),
              },
            }) as never,
        }),
      },
    });
  };
}
