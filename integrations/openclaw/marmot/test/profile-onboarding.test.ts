import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  MAX_PROFILE_NAME_CHARS,
  maybeHandleProfileOnboarding,
  parseProfileNameReply,
  PROFILE_NAME_EMPTY,
  PROFILE_NAME_PROMPT,
  PROFILE_NAME_PUBLISH_FAILED,
  PROFILE_NAME_PUBLISHED,
  PROFILE_NAME_SKIPPED,
  PROFILE_NAME_TOO_LONG,
  ProfileNameOnboardingStore,
  type OnboardingRecord,
  type ProfileOnboardingClient,
  type ProfileOnboardingStateStore,
} from "../src/profile-onboarding.js";

const HEX = (b: string) => b.repeat(32);
const ACCOUNT = HEX("aa");
const GROUP = HEX("cc");
const MSG = HEX("11");

interface Calls {
  sendFinal: string[];
  publish: { name: string; displayName: string | null }[];
}

function emptyCalls(): Calls {
  return { sendFinal: [], publish: [] };
}

function stubClient(calls: Calls, opts: { failPublish?: boolean } = {}): ProfileOnboardingClient {
  return {
    async sendFinal(_account, _group, text) {
      calls.sendFinal.push(text);
      return { type: "final_sent" };
    },
    async accountPublishProfile(_account, name, displayName) {
      if (opts.failPublish) {
        throw new Error("publish failed");
      }
      calls.publish.push({ name, displayName: displayName ?? null });
      return { type: "profile_published" };
    },
  };
}

class MemStore implements ProfileOnboardingStateStore {
  rec: Partial<OnboardingRecord> = {};
  async get(): Promise<Partial<OnboardingRecord>> {
    return { ...this.rec };
  }
  async markPrompted(_account: string, groupIdHex: string): Promise<void> {
    this.rec = { status: "prompted", group_id_hex: groupIdHex };
  }
  async markPublished(_account: string, name: string): Promise<void> {
    this.rec = { status: "published", name };
  }
  async markSkipped(): Promise<void> {
    this.rec = { status: "skipped" };
  }
}

function msg(text: string, groupIdHex = GROUP) {
  return { accountIdHex: ACCOUNT, groupIdHex, messageIdHex: MSG, text };
}

describe("parseProfileNameReply", () => {
  it("treats empty/whitespace as invalid", () => {
    expect(parseProfileNameReply("   ")).toEqual({ action: "invalid", response: PROFILE_NAME_EMPTY });
  });
  it("recognizes skip words case-insensitively", () => {
    for (const word of ["skip", "/skip", "no", "No Thanks", "CANCEL", "not now"]) {
      expect(parseProfileNameReply(word).action).toBe("skip");
    }
  });
  it("strips one pair of surrounding quotes and collapses whitespace", () => {
    expect(parseProfileNameReply('  "Ada   Lovelace" ')).toMatchObject({
      action: "name",
      name: "Ada Lovelace",
    });
  });
  it("rejects names over the max length", () => {
    expect(parseProfileNameReply("x".repeat(MAX_PROFILE_NAME_CHARS + 1))).toEqual({
      action: "invalid",
      response: PROFILE_NAME_TOO_LONG,
    });
  });
  it("accepts a normal name", () => {
    expect(parseProfileNameReply("Ada")).toEqual({ action: "name", name: "Ada", response: "" });
  });
});

describe("maybeHandleProfileOnboarding", () => {
  it("inherits a configured name: publishes silently and does NOT intercept", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    const intercepted = await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls),
      message: msg("hello"),
      configuredName: "Claude",
    });
    expect(intercepted).toBe(false);
    expect(calls.publish).toEqual([{ name: "Claude", displayName: "Claude" }]);
    expect(calls.sendFinal).toEqual([]);
    expect(store.rec.status).toBe("published");
  });

  it("asks once when no name is configured: prompts and intercepts", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    const intercepted = await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls),
      message: msg("hi"),
      configuredName: null,
    });
    expect(intercepted).toBe(true);
    expect(calls.sendFinal).toEqual([PROFILE_NAME_PROMPT]);
    expect(store.rec).toEqual({ status: "prompted", group_id_hex: GROUP });
    expect(calls.publish).toEqual([]);
  });

  it("publishes a valid reply to the prompt", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    store.rec = { status: "prompted", group_id_hex: GROUP };
    const intercepted = await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls),
      message: msg("Ada"),
      configuredName: null,
    });
    expect(intercepted).toBe(true);
    expect(calls.publish).toEqual([{ name: "Ada", displayName: "Ada" }]);
    expect(calls.sendFinal).toEqual([PROFILE_NAME_PUBLISHED.replace("{name}", "Ada")]);
    expect(store.rec.status).toBe("published");
  });

  it("skips on a skip reply", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    store.rec = { status: "prompted", group_id_hex: GROUP };
    await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls),
      message: msg("skip"),
      configuredName: null,
    });
    expect(store.rec.status).toBe("skipped");
    expect(calls.sendFinal).toEqual([PROFILE_NAME_SKIPPED]);
    expect(calls.publish).toEqual([]);
  });

  it("stays prompted and reports failure when publish throws", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    store.rec = { status: "prompted", group_id_hex: GROUP };
    await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls, { failPublish: true }),
      message: msg("Ada"),
      configuredName: null,
    });
    expect(calls.sendFinal).toEqual([PROFILE_NAME_PUBLISH_FAILED]);
    expect(store.rec.status).toBe("prompted"); // unchanged; retry on next reply
  });

  it("does nothing once published", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    store.rec = { status: "published", name: "Ada" };
    const intercepted = await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls),
      message: msg("hello"),
      configuredName: "Claude",
    });
    expect(intercepted).toBe(false);
    expect(calls.publish).toEqual([]);
    expect(calls.sendFinal).toEqual([]);
  });

  it("ignores a prompted reply that arrives in a different conversation", async () => {
    const calls = emptyCalls();
    const store = new MemStore();
    store.rec = { status: "prompted", group_id_hex: HEX("dd") };
    const intercepted = await maybeHandleProfileOnboarding({
      store,
      client: stubClient(calls),
      message: msg("Ada"),
      configuredName: null,
    });
    expect(intercepted).toBe(false);
    expect(calls.publish).toEqual([]);
  });
});

describe("ProfileNameOnboardingStore", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "marmot-onboarding-"));
  });
  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("persists status transitions, creating parent dirs, visible to a fresh store", async () => {
    const path = join(dir, "nested", "profile-onboarding.json");
    const store = new ProfileNameOnboardingStore(path);
    expect(await store.get(ACCOUNT)).toEqual({});
    await store.markPrompted(ACCOUNT, GROUP);
    expect(await store.get(ACCOUNT)).toEqual({ status: "prompted", group_id_hex: GROUP });
    await store.markPublished(ACCOUNT, "Ada");
    expect(await store.get(ACCOUNT)).toEqual({ status: "published", name: "Ada" });

    const reopened = new ProfileNameOnboardingStore(path);
    expect(await reopened.get(ACCOUNT)).toEqual({ status: "published", name: "Ada" });
    expect(await reopened.get(HEX("bb"))).toEqual({});
  });
});
