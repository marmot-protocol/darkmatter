// One-time, opt-in public Nostr profile (kind:0) name flow for the Marmot agent,
// ported from the Hermes plugin's ProfileNameOnboardingStore. Two paths, gated by
// the `profileNameOnboarding` config flag (default off):
//   (a) inherit the configured OpenClaw agent name and publish it silently, then
//       let the message get a normal reply; or
//   (b) when no name is configured, ask once in-chat for consent before
//       publishing. Per-account status is persisted so we never re-ask.
//
// Privacy: never log names, account ids, or group ids — only generic lifecycle
// messages. The 0600 state file holds names/group ids by necessity; that is local
// state, not logging.

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

export const MAX_PROFILE_NAME_CHARS = 80;

export const PROFILE_NAME_PROMPT =
  "I do not have a public Nostr profile name yet. What should I publish as this " +
  'agent\'s display name? Reply with a name, or reply "skip" to stay unnamed.';
export const PROFILE_NAME_PUBLISHED =
  'Done. I published this agent\'s public Nostr profile name as "{name}".';
export const PROFILE_NAME_SKIPPED = "Okay, I will stay unnamed for now.";
export const PROFILE_NAME_EMPTY = 'Please reply with a name, or reply "skip" to stay unnamed.';
export const PROFILE_NAME_TOO_LONG =
  'That name is too long. Please reply with a shorter name, or reply "skip" to stay unnamed.';
export const PROFILE_NAME_PUBLISH_FAILED =
  "I could not publish that profile name yet. Please try again later.";

const PROFILE_NAME_SKIP_REPLIES = new Set(["/skip", "cancel", "no", "no thanks", "not now", "skip"]);

/** Collapse runs of whitespace and trim, matching Hermes' " ".join(text.split()). */
function normalizeWhitespace(text: string): string {
  return String(text ?? "")
    .split(/\s+/)
    .filter((part) => part.length > 0)
    .join(" ");
}

export type ProfileNameReply =
  | { action: "skip"; response: string }
  | { action: "invalid"; response: string }
  | { action: "name"; name: string; response: string };

/** Interpret a user's reply to the profile-name prompt (mirrors Hermes). */
export function parseProfileNameReply(text: string): ProfileNameReply {
  let value = normalizeWhitespace(text);
  if (!value) {
    return { action: "invalid", response: PROFILE_NAME_EMPTY };
  }
  if (PROFILE_NAME_SKIP_REPLIES.has(value.toLowerCase())) {
    return { action: "skip", response: PROFILE_NAME_SKIPPED };
  }
  // Strip a single pair of surrounding matching quotes.
  const first = value[0];
  if (value.length >= 2 && first === value[value.length - 1] && (first === "'" || first === '"')) {
    value = normalizeWhitespace(value.slice(1, -1));
  }
  if (!value) {
    return { action: "invalid", response: PROFILE_NAME_EMPTY };
  }
  if (value.length > MAX_PROFILE_NAME_CHARS) {
    return { action: "invalid", response: PROFILE_NAME_TOO_LONG };
  }
  return { action: "name", name: value, response: "" };
}

// --- persisted per-account status ------------------------------------------

export type OnboardingStatus = "prompted" | "published" | "skipped";

export interface OnboardingRecord {
  status: OnboardingStatus;
  group_id_hex?: string;
  name?: string;
}

/** State store interface (the runtime uses the file-backed impl; tests stub it). */
export interface ProfileOnboardingStateStore {
  get(accountIdHex: string): Promise<Partial<OnboardingRecord>>;
  markPrompted(accountIdHex: string, groupIdHex: string): Promise<void>;
  markPublished(accountIdHex: string, name: string): Promise<void>;
  markSkipped(accountIdHex: string): Promise<void>;
}

interface StoreFile {
  marmot_profile_onboarding: "v1";
  accounts: Record<string, OnboardingRecord>;
}

/**
 * Tiny local JSON state file (0600) recording the one-time consent flow per
 * account. Writes are atomic (tmp + rename) and serialized in-process.
 */
export class ProfileNameOnboardingStore implements ProfileOnboardingStateStore {
  private chain: Promise<unknown> = Promise.resolve();

  constructor(private readonly path: string) {}

  /** Serialize ops so concurrent reads/writes never interleave or race. */
  private serialize<T>(op: () => Promise<T>): Promise<T> {
    const next = this.chain.then(op, op);
    this.chain = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  private async load(): Promise<StoreFile> {
    try {
      const data = JSON.parse(await readFile(this.path, "utf8")) as Partial<StoreFile>;
      const accounts =
        data && typeof data === "object" && data.accounts && typeof data.accounts === "object"
          ? (data.accounts as Record<string, OnboardingRecord>)
          : {};
      return { marmot_profile_onboarding: "v1", accounts };
    } catch {
      return { marmot_profile_onboarding: "v1", accounts: {} };
    }
  }

  private async persist(data: StoreFile): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    const tmp = `${this.path}.tmp`;
    await writeFile(tmp, `${JSON.stringify(data)}\n`, { mode: 0o600 });
    await rename(tmp, this.path);
  }

  get(accountIdHex: string): Promise<Partial<OnboardingRecord>> {
    return this.serialize(async () => {
      const data = await this.load();
      return { ...(data.accounts[accountIdHex] ?? {}) };
    });
  }

  private set(accountIdHex: string, record: OnboardingRecord): Promise<void> {
    return this.serialize(async () => {
      const data = await this.load();
      data.accounts[accountIdHex] = record;
      await this.persist(data);
    });
  }

  markPrompted(accountIdHex: string, groupIdHex: string): Promise<void> {
    return this.set(accountIdHex, { status: "prompted", group_id_hex: groupIdHex });
  }

  markPublished(accountIdHex: string, name: string): Promise<void> {
    return this.set(accountIdHex, { status: "published", name });
  }

  markSkipped(accountIdHex: string): Promise<void> {
    return this.set(accountIdHex, { status: "skipped" });
  }
}

// --- the intercept handler ---------------------------------------------------

export interface ProfileOnboardingClient {
  sendFinal(
    accountIdHex: string,
    groupIdHex: string,
    text: string,
    replyToMessageIdHex?: string | null,
  ): Promise<unknown>;
  accountPublishProfile(
    accountIdHex: string,
    name: string,
    displayName?: string | null,
  ): Promise<unknown>;
}

export interface ProfileOnboardingMessage {
  accountIdHex: string;
  groupIdHex: string;
  messageIdHex: string;
  text: string;
}

export interface MaybeHandleProfileOnboardingDeps {
  store: ProfileOnboardingStateStore;
  client: ProfileOnboardingClient;
  message: ProfileOnboardingMessage;
  /** Configured OpenClaw agent name, if any (drives the inherit-and-publish path). */
  configuredName?: string | null;
  logger?: { info?: (message: string) => void; warn?: (message: string) => void };
}

function validConfiguredName(name: string | null | undefined): string | undefined {
  const trimmed = normalizeWhitespace(String(name ?? ""));
  if (!trimmed || trimmed.length > MAX_PROFILE_NAME_CHARS) {
    return undefined;
  }
  return trimmed;
}

/**
 * Drive the one-time profile-name flow for an inbound message. Returns true when
 * the message was consumed (skip the normal agent turn). Path (a) — inheriting a
 * configured name — publishes silently and returns false so the message still
 * gets a normal reply.
 */
export async function maybeHandleProfileOnboarding(
  deps: MaybeHandleProfileOnboardingDeps,
): Promise<boolean> {
  const { store, client, message, logger } = deps;
  const account = message.accountIdHex;
  const group = message.groupIdHex;
  const replyTo = message.messageIdHex;

  const record = await store.get(account);
  if (record.status === "published" || record.status === "skipped") {
    return false;
  }

  if (record.status === "prompted") {
    // Only the conversation we prompted in carries the answer.
    if (record.group_id_hex !== group) {
      return false;
    }
    const parsed = parseProfileNameReply(message.text);
    if (parsed.action === "skip") {
      await store.markSkipped(account);
      await client.sendFinal(account, group, parsed.response, replyTo);
      logger?.info?.("marmot: profile onboarding skipped");
      return true;
    }
    if (parsed.action === "invalid") {
      await client.sendFinal(account, group, parsed.response, replyTo);
      return true; // stay prompted; the prompt is implicitly re-asked
    }
    try {
      await client.accountPublishProfile(account, parsed.name, parsed.name);
      await store.markPublished(account, parsed.name);
      await client.sendFinal(
        account,
        group,
        PROFILE_NAME_PUBLISHED.replace("{name}", parsed.name),
        replyTo,
      );
      logger?.info?.("marmot: profile name published");
    } catch {
      await client.sendFinal(account, group, PROFILE_NAME_PUBLISH_FAILED, replyTo);
      logger?.warn?.("marmot: profile name publish failed");
    }
    return true;
  }

  // No status yet: first interaction with this account.
  const inherited = validConfiguredName(deps.configuredName);
  if (inherited) {
    // Path (a): publish the configured name silently, then let the turn run.
    try {
      await client.accountPublishProfile(account, inherited, inherited);
      await store.markPublished(account, inherited);
      logger?.info?.("marmot: published configured profile name");
    } catch {
      // Leave unmarked so a later message retries.
      logger?.warn?.("marmot: failed to publish configured profile name");
    }
    return false;
  }

  // Path (b): ask once for consent before publishing anything public.
  await client.sendFinal(account, group, PROFILE_NAME_PROMPT, replyTo);
  await store.markPrompted(account, group);
  logger?.info?.("marmot: profile onboarding prompt sent");
  return true;
}
