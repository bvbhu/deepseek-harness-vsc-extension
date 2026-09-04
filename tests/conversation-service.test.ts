/**
 * ConversationService 历史分页测试（0.1.2：session/follow snapshot + session/page）：
 *   - attach 从 follow snapshot 种窗口并记录 hasMore；
 *   - loadOlder 经 session/page（throughSeq = earliestSeq - 1）向前翻页并前置合并；
 *   - 并发 live 帧在 fetch 期间直接应用到已 attached 的 fold（seq 去重）；
 *   - resync/重 attach 不丢已加载的更早事件；
 *   - hasMore=false / loadingOlder 期间的重复 loadOlder 为 no-op。
 */

import { describe, expect, it, vi } from "vitest";
import { ConversationService } from "../src/services/conversation-service.ts";
import type { WireSessionEvent } from "../src/conversation/fold.ts";
import type { WireClient, ServerRequest } from "../src/dsh/wire.ts";

/** 一条可折叠为 user 气泡的最小事件。 */
function userEvent(seq: number, text: string): WireSessionEvent {
  return {
    type: "user/message",
    seq,
    time: seq * 1000,
    data: { content: [{ type: "text", text }] },
  };
}

/** 按 seq 升序返回的多条 user 事件。 */
function userEvents(seqs: number[]): WireSessionEvent[] {
  return seqs.map((seq) => userEvent(seq, `message-${seq}`));
}

/** follow snapshot / page 的 records（event 条目）。 */
function recordsOf(
  events: WireSessionEvent[],
): { type: "event"; event: WireSessionEvent }[] {
  return events.map((event) => ({ type: "event" as const, event }));
}

function snap(events: WireSessionEvent[], hasMore: boolean, cursor: number) {
  return { records: recordsOf(events), hasMore, cursor };
}

interface PageResult {
  records: { type: "event"; event: WireSessionEvent }[];
  hasMore: boolean;
}

/**
 * 可编程 fake WireClient：openStream 模拟 session/follow（发一帧 snapshot），
 * call 模拟 session/page。live 帧由 applyFrame 经 mux 路径投递，不经流。
 */
function fakeClient(opts: {
  snapshot: (sessionId: string) => {
    records: { type: "event"; event: WireSessionEvent }[];
    hasMore: boolean;
    cursor: number;
  };
  page: (throughSeq: number) => Promise<PageResult>;
}) {
  const openStream = vi.fn(
    (
      endpoint: string,
      args: Record<string, unknown>,
      handlers: { onItem?: (value: unknown) => void },
    ) => {
      if (endpoint === "session/follow") {
        const sessionId = (
          args.request as { address: { sessionId: string } }
        ).address.sessionId;
        const s = opts.snapshot(sessionId);
        queueMicrotask(() =>
          handlers.onItem?.({
            type: "snapshot",
            cursor: s.cursor,
            records: s.records,
            hasMore: s.hasMore,
          }),
        );
      }
      return () => undefined;
    },
  );
  const call = vi.fn((method: string, args: Record<string, unknown>) => {
    if (method === "session/page") {
      const throughSeq = (args.request as { throughSeq: number }).throughSeq;
      return opts.page(throughSeq);
    }
    return Promise.resolve({});
  });
  return {
    call,
    openStream,
    client: { call, openStream } as unknown as WireClient,
  };
}

function frame(sessionId: string, event: WireSessionEvent): ServerRequest {
  return {
    type: "server-request",
    rpcId: "rpc-test",
    method: "session/event",
    payload: { type: "session/event", sessionId, event },
  };
}

function userTexts(snapshot: {
  items: { kind: string; text?: string }[];
}): string[] {
  return snapshot.items
    .filter((item) => item.kind === "user")
    .map((item) => item.text ?? "");
}

function pageCalls(call: ReturnType<typeof fakeClient>["call"]): unknown[][] {
  return call.mock.calls.filter(([method]) => method === "session/page");
}

describe("ConversationService history pagination", () => {
  it("attach seeds the tail window from the follow snapshot and records hasMore", async () => {
    const { call, client } = fakeClient({
      snapshot: () => snap(userEvents([8, 9, 10]), true, 10),
      page: () => Promise.resolve({ records: [], hasMore: false }),
    });
    const service = new ConversationService(() => client);

    const snapshot = await service.attach("s1");

    expect(userTexts(snapshot)).toEqual([
      "message-8",
      "message-9",
      "message-10",
    ]);
    expect(snapshot.lastSeq).toBe(10);
    expect(snapshot.hasMore).toBe(true);
    expect(pageCalls(call).length).toBe(0); // attach 走 openStream，不发 page
  });

  it("loadOlder prepends the previous page via throughSeq and updates hasMore", async () => {
    const { call, client } = fakeClient({
      snapshot: () => snap(userEvents([8, 9, 10]), true, 10),
      page: (throughSeq) => {
        expect(throughSeq).toBe(7); // earliestSeq(8) - 1
        return Promise.resolve({
          records: recordsOf(userEvents([5, 6, 7])),
          hasMore: false,
        });
      },
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");

    const snapshot = await service.loadOlder("s1");

    expect(userTexts(snapshot)).toEqual([
      "message-5",
      "message-6",
      "message-7",
      "message-8",
      "message-9",
      "message-10",
    ]);
    expect(snapshot.lastSeq).toBe(10);
    expect(snapshot.hasMore).toBe(false);
    expect(pageCalls(call).length).toBe(1);
  });

  it("loadOlder is a no-op when the window is complete", async () => {
    const { call, client } = fakeClient({
      snapshot: () => snap(userEvents([1, 2]), false, 2),
      page: () => Promise.resolve({ records: [], hasMore: false }),
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");
    const before = call.mock.calls.length;

    const snapshot = await service.loadOlder("s1");

    expect(call.mock.calls.length).toBe(before); // 不再发 page
    expect(userTexts(snapshot)).toEqual(["message-1", "message-2"]);
  });

  it("guards against concurrent loadOlder calls (loadingOlder flag)", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { call, client } = fakeClient({
      snapshot: () => snap(userEvents([3, 4]), true, 4),
      page: async () => {
        await gate; // 挂起第一次翻页
        return { records: recordsOf(userEvents([1, 2])), hasMore: false };
      },
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");

    const first = service.loadOlder("s1");
    const second = await service.loadOlder("s1"); // 应立刻返回，不再发 page

    expect(userTexts(second)).toEqual(["message-3", "message-4"]);
    expect(pageCalls(call).length).toBe(1); // 仅一次 loadOlder
    release?.();
    const settled = await first;
    expect(userTexts(settled)).toEqual([
      "message-1",
      "message-2",
      "message-3",
      "message-4",
    ]);
  });

  it("applies live frames buffered during a loadOlder fetch (seq-deduped)", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client } = fakeClient({
      snapshot: () => snap(userEvents([3, 4]), true, 4),
      page: async () => {
        await gate;
        return { records: recordsOf(userEvents([1, 2])), hasMore: false };
      },
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");

    const loading = service.loadOlder("s1");
    // 翻页期间的并发 live 帧直接应用到已 attached 的 fold。
    service.applyFrame(frame("s1", userEvent(5, "message-5")));
    service.applyFrame(frame("s1", userEvent(4, "message-4-dup"))); // 已覆盖 → 丢弃
    release?.();
    const snapshot = await loading;

    expect(userTexts(snapshot)).toEqual([
      "message-1",
      "message-2",
      "message-3",
      "message-4",
      "message-5",
    ]);
    expect(snapshot.lastSeq).toBe(5);
  });

  it("re-attach preserves events loaded from older pages (resync-safe)", async () => {
    const { client } = fakeClient({
      snapshot: () => snap(userEvents([4, 5, 6]), true, 6),
      page: () =>
        Promise.resolve({
          records: recordsOf(userEvents([1, 2, 3])),
          hasMore: true,
        }),
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");
    await service.loadOlder("s1");

    // 重连 resync：尾页重放不得让已加载的更早消息消失。
    await service.attach("s1");
    const snapshot = service.snapshot("s1");

    expect(userTexts(snapshot!)).toEqual([
      "message-1",
      "message-2",
      "message-3",
      "message-4",
      "message-5",
      "message-6",
    ]);
  });

  it("attach merges overlapping pages by seq without duplicating items", async () => {
    const { client } = fakeClient({
      snapshot: () => snap(userEvents([8, 9, 10]), true, 10),
      page: () =>
        Promise.resolve({
          records: recordsOf(userEvents([5, 6, 7, 8])),
          hasMore: false,
        }),
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");
    const snapshot = await service.loadOlder("s1");

    // seq 8 重叠：只保留一份。
    expect(userTexts(snapshot)).toEqual([
      "message-5",
      "message-6",
      "message-7",
      "message-8",
      "message-9",
      "message-10",
    ]);
  });

  it("applies live frames to an attached window and emits change", async () => {
    const { client } = fakeClient({
      snapshot: () => snap(userEvents([1]), true, 1),
      page: () => Promise.resolve({ records: [], hasMore: false }),
    });
    const service = new ConversationService(() => client);
    await service.attach("s1");
    const onChange = vi.fn();
    service.on("change", onChange);

    service.applyFrame(frame("s1", userEvent(2, "message-2")));
    service.applyFrame(frame("s1", userEvent(2, "message-2-stale"))); // 同 seq → 丢弃

    const snapshot = service.snapshot("s1")!;
    expect(userTexts(snapshot)).toEqual(["message-1", "message-2"]);
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("snapshot returns null before attach", () => {
    const { client } = fakeClient({
      snapshot: () => snap([], false, -1),
      page: () => Promise.resolve({ records: [], hasMore: false }),
    });
    const service = new ConversationService(() => client);

    expect(service.snapshot("unattached")).toBeNull();
  });
});
