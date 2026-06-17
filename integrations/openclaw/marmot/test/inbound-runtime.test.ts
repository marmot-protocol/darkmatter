import { describe, expect, it } from "vitest";

import type { AgentControlEvent, MarmotAgentControlClient } from "../src/client.js";
import {
  startMarmotInbound,
  type InboundPluginApi,
} from "../src/inbound-runtime.js";
import type { MarmotInboundMessage } from "../src/inbound.js";

const HEX32 = (b: string) => b.repeat(32);

function stubClient(events: AgentControlEvent[]): MarmotAgentControlClient {
  return {
    async accountList() {
      return {
        type: "account_list",
        accounts: [{ account_id_hex: HEX32("aa"), label: "agent", local_signing: true }],
      };
    },
    async *subscribeInbound(): AsyncGenerator<AgentControlEvent> {
      for (const event of events) {
        yield event;
      }
    },
  } as unknown as MarmotAgentControlClient;
}

const noopLogger = { info: () => {}, warn: () => {} };

describe("startMarmotInbound", () => {
  it("resolves the agent account and dispatches mapped inbound messages", async () => {
    const dispatched: MarmotInboundMessage[] = [];
    let resolveFirst: () => void = () => {};
    const firstDispatch = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });

    const api: InboundPluginApi = {
      pluginConfig: { socketPath: "/unused-in-test.sock" },
      logger: noopLogger,
    };

    const stop = startMarmotInbound(api, {
      clientFactory: () =>
        stubClient([
          {
            type: "inbound_message",
            account_id_hex: HEX32("aa"),
            group_id_hex: HEX32("cc"),
            message_id_hex: HEX32("dd"),
            sender_account_id_hex: HEX32("bb"),
            text: "hello agent",
          },
        ]),
      dispatch: (message) => {
        dispatched.push(message);
        resolveFirst();
      },
    });

    await firstDispatch;
    stop();

    expect(dispatched).toHaveLength(1);
    expect(dispatched[0]).toMatchObject({
      groupIdHex: HEX32("cc"),
      messageIdHex: HEX32("dd"),
      senderAccountIdHex: HEX32("bb"),
      text: "hello agent",
    });
  });
});
