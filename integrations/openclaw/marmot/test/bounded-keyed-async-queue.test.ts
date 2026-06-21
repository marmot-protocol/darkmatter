import { describe, expect, it, vi } from "vitest";

import { BoundedKeyedAsyncQueue } from "../src/bounded-keyed-async-queue.js";

describe("BoundedKeyedAsyncQueue", () => {
  it("sheds incoming turns once per-key depth is reached", async () => {
    let releaseFirst: (() => void) | undefined;
    const firstStarted = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const ran: string[] = [];
    const shed = vi.fn();
    const queue = new BoundedKeyedAsyncQueue(2, shed);

    queue.enqueue("group-a", async () => {
      ran.push("first-start");
      await firstStarted;
      ran.push("first-done");
    });
    queue.enqueue("group-a", async () => {
      ran.push("second");
    });
    queue.enqueue("group-a", async () => {
      ran.push("third");
    });

    await vi.waitFor(() => expect(ran).toContain("first-start"));
    expect(shed).toHaveBeenCalledWith("marmot: inbound queue depth exceeded; shedding turn");

    releaseFirst?.();
    await vi.waitFor(() => expect(ran).toEqual(["first-start", "first-done", "second"]));
  });
});
