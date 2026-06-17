// Live-preview state machine bridging OpenClaw progressive draft updates to
// Marmot's QUIC preview stream + durable finalize.
//
// OpenClaw hands us growing full-text snapshots (draft-stream `update(text)`);
// we reduce each to an append-only suffix, mirror it into a local transcript
// (byte-for-byte with dm-agent's), and send `stream_append`. On finalize we send
// the transcript hash + chunk count dm-agent validates against its own. A
// non-append-only update throws so the caller can cancel + send a plain final.

import { AppendOnlyText } from "./append-only.js";
import type { MarmotAgentControlClient } from "./client.js";
import { AgentTextStreamTranscript, DEFAULT_STREAM_CHUNK_BYTES } from "./transcript.js";

/** Narrow control-client surface used by the live preview (eases testing). */
export type StreamControlClient = Pick<
  MarmotAgentControlClient,
  "streamBegin" | "streamAppend" | "streamFinalize" | "streamCancel"
>;

export interface MarmotLivePreviewOptions {
  accountIdHex: string;
  groupIdHex: string;
  quicCandidates: string[];
  chunkBytes?: number;
}

export interface MarmotLiveFinalizeResult {
  streamIdHex: string;
  startMessageIdHex: string;
  messageIdsHex: string[];
}

export class MarmotLivePreview {
  private begun = false;
  private streamIdHex: string | null = null;
  private startMessageIdHex: string | null = null;
  private transcript: AgentTextStreamTranscript | null = null;
  private readonly appendOnly = new AppendOnlyText();
  private readonly chunkBytes: number;

  constructor(
    private readonly client: StreamControlClient,
    private readonly options: MarmotLivePreviewOptions,
  ) {
    this.chunkBytes = options.chunkBytes ?? DEFAULT_STREAM_CHUNK_BYTES;
  }

  get streamId(): string | null {
    return this.streamIdHex;
  }

  get isActive(): boolean {
    return this.begun;
  }

  private async ensureBegun(): Promise<void> {
    if (this.begun) {
      return;
    }
    const response = await this.client.streamBegin(
      this.options.accountIdHex,
      this.options.groupIdHex,
      { quicCandidates: this.options.quicCandidates },
    );
    this.streamIdHex = response.stream_id_hex;
    this.startMessageIdHex = response.start_message_id_hex;
    this.transcript = new AgentTextStreamTranscript(
      Buffer.from(response.stream_id_hex, "hex"),
      Buffer.from(response.start_message_id_hex, "hex"),
    );
    this.begun = true;
  }

  /**
   * Push the latest full preview text. Throws {@link NonAppendOnlyUpdateError}
   * if it is not an extension of what was already streamed.
   */
  async update(fullText: string): Promise<void> {
    await this.ensureBegun();
    const suffix = this.appendOnly.suffixFor(fullText);
    if (suffix.length === 0) {
      return;
    }
    this.transcript!.appendText(suffix, this.chunkBytes);
    await this.client.streamAppend(this.streamIdHex!, suffix);
  }

  /**
   * Append the remaining suffix (if any) and finalize the durable kind-9.
   * Throws {@link NonAppendOnlyUpdateError} if `finalText` is not an extension
   * of the streamed text.
   */
  async finalize(finalText: string): Promise<MarmotLiveFinalizeResult> {
    await this.ensureBegun();
    const suffix = this.appendOnly.suffixFor(finalText);
    if (suffix.length > 0) {
      this.transcript!.appendText(suffix, this.chunkBytes);
      await this.client.streamAppend(this.streamIdHex!, suffix);
    }
    const response = await this.client.streamFinalize(
      this.streamIdHex!,
      finalText,
      this.transcript!.hashHex,
      this.transcript!.chunkCount,
    );
    return {
      streamIdHex: this.streamIdHex!,
      startMessageIdHex: this.startMessageIdHex!,
      messageIdsHex: response.message_ids_hex,
    };
  }

  /** Cancel the live preview (best-effort). No-op if no stream was begun. */
  async cancel(reason?: string): Promise<void> {
    if (!this.begun || !this.streamIdHex) {
      return;
    }
    await this.client.streamCancel(this.streamIdHex, reason ?? null);
  }
}
