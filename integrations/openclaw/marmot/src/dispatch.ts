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
  /** Optional privacy-safe lifecycle logger (kinds + lengths only, no content/ids). */
  log?: (message: string) => void;
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
  /** Reply deliveries received from the dispatcher this turn (diagnostic). */
  deliveries = 0;
  /** Latest full reply text seen; used to commit a blocks-only turn at flush(). */
  private lastText = "";
  /** Whether the durable reply has been committed (stream_finalize or send_final). */
  private finalized = false;

  constructor(private readonly options: MarmotReplySinkOptions) {}

  private log(message: string): void {
    this.options.log?.(message);
  }

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
      // Best effort: if the QUIC stream is already unhealthy, cancel may also
      // fail — we still fall back to a plain durable send.
      await this.preview.cancel(reason).catch(() => undefined);
    }
  }

  private async sendFinal(text: string): Promise<void> {
    this.log(`marmot: sending durable final (${text.length} chars)`);
    await this.options.client.sendFinal(
      this.options.accountIdHex,
      this.options.groupIdHex,
      text,
      this.options.replyToMessageIdHex ?? null,
    );
    this.log("marmot: durable final sent");
  }

  async deliver(payload: ReplyPayloadLike, info: ReplyDelivery): Promise<void> {
    const text = payload?.text ?? "";
    this.deliveries += 1;
    this.log(
      `marmot: reply delivery kind=${info.kind} chars=${text.length} streaming=${this.streamingEnabled}`,
    );

    if (info.kind === "tool") {
      // v1: tool/progress chatter is not surfaced to Marmot.
      return;
    }

    // Remember the latest full reply text so a turn that streams blocks but
    // never sends an explicit `final` can still be committed durably at flush().
    this.lastText = text;

    if (info.kind === "block") {
      if (!this.streamingEnabled || this.previewAbandoned) {
        return; // recorded in lastText; committed by the final delivery or flush()
      }
      try {
        await this.ensurePreview().update(text);
      } catch (error) {
        // Any preview failure — non-append-only text, or a QUIC/broker error —
        // abandons the live preview. The full text is still delivered durably by
        // the final delivery or flush(), so a streaming hiccup never drops it.
        const reason =
          error instanceof NonAppendOnlyUpdateError ? "non_append_only" : "preview_error";
        this.log(`marmot: live preview abandoned (${reason}); will send the final durably`);
        await this.abandonPreview(reason);
      }
      return;
    }

    await this.commit(text);
  }

  /**
   * Commit the durable reply exactly once: finalize an active live preview into
   * a stream-final, otherwise send a plain durable final.
   */
  private async commit(text: string): Promise<void> {
    if (this.finalized) {
      return;
    }
    this.finalized = true;
    if (this.preview && this.preview.isActive && !this.previewAbandoned) {
      try {
        await this.preview.finalize(text);
        return;
      } catch (error) {
        const reason =
          error instanceof NonAppendOnlyUpdateError ? "final_not_append_only" : "finalize_error";
        this.log(`marmot: preview finalize failed (${reason}); falling back to a durable send`);
        await this.abandonPreview(reason);
      }
    }
    await this.sendFinal(text);
  }

  /**
   * Commit the reply once the agent turn has finished. Block streaming can
   * deliver the whole reply as `block`s with no trailing `final`; without this
   * the live preview would never be finalized and no durable kind:9 would land.
   * A no-op once a `final` delivery already committed, or if the turn produced
   * no text at all.
   */
  async flush(): Promise<void> {
    if (this.finalized) {
      return;
    }
    if (!this.lastText) {
      this.log("marmot: turn produced no deliverable reply");
      return;
    }
    this.log("marmot: no explicit final delivery; committing the streamed reply at turn end");
    await this.commit(this.lastText);
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
  /** Optional privacy-safe lifecycle logger. */
  log?: (message: string) => void;
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
      log: deps.log,
    });

    deps.log?.("marmot: agent turn starting");
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
    // Block streaming can finish a turn with only `block` deliveries; commit the
    // accumulated reply durably if no explicit `final` did.
    await sink.flush();
    deps.log?.(`marmot: agent turn done (sink deliveries=${sink.deliveries})`);
  };
}
