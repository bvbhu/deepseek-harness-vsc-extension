/**
 * 0.1.7（Session 日志 V4）适配测试：
 *   - expandAssistantStream 展开 compact 记录（含时间重建与畸形记录拒绝）；
 *   - AssistantStreamJournal 采用快照基线、续写 delta、序号跳空 → rebaseline；
 *   - fold 侧的合成流式事件与持久结算事件的替换关系；
 *   - tool/result 的 V4 顶层 toolCallId/直接 content 与旧信封块两条形状；
 *   - assistant/attempt 退休流式占位、turn/end 新增原因。
 */

import { describe, expect, it } from "vitest";
import {
  AssistantStreamJournal,
  expandAssistantStream,
  type AssistantStreamFrame,
} from "../src/conversation/assistant-stream.ts";
import {
  ConversationFold,
  type WireSessionEvent,
} from "../src/conversation/fold.ts";

function assistantText(fold: ConversationFold): string[] {
  return fold
    .snapshot()
    .items.filter((item) => item.kind === "assistant")
    .map((item) => (item.kind === "assistant" ? item.text : ""));
}

function partialFlags(fold: ConversationFold): boolean[] {
  return fold
    .snapshot()
    .items.filter((item) => item.kind === "assistant")
    .map((item) => (item.kind === "assistant" ? item.partial : false));
}

describe("expandAssistantStream", () => {
  it("expands a packed text run and reconstructs each member's timestamp", () => {
    const members = expandAssistantStream([
      {
        type: "text-chunks",
        time0: 1_000,
        index: 0,
        dt: [5, 7],
        texts: ["A", "B", "C"],
      },
    ]);
    expect(members.map((member) => member.time)).toEqual([1_000, 1_005, 1_012]);
    expect(members.map((member) => member.chunk)).toEqual([
      { type: "text-delta", index: 0, text: "A" },
      { type: "text-delta", index: 0, text: "B" },
      { type: "text-delta", index: 0, text: "C" },
    ]);
  });

  it("keeps raw chunk records and expands tool-call runs as argument deltas", () => {
    const members = expandAssistantStream([
      { type: "chunk", time: 42, chunk: { type: "block-start", index: 0, blockType: "text" } },
      {
        type: "tool-call-chunks",
        time0: 50,
        index: 1,
        dt: [1],
        id: "call-1",
        name: "read",
        args: ['{"file', '_path":'],
      },
    ]);
    expect(members[0]).toEqual({
      time: 42,
      chunk: { type: "block-start", index: 0, blockType: "text" },
    });
    expect(members[1]?.chunk).toEqual({
      type: "tool-call-delta",
      index: 1,
      id: "call-1",
      name: "read",
      argumentsDelta: '{"file',
    });
    expect(members[2]?.chunk).toEqual({
      type: "tool-call-delta",
      index: 1,
      id: "call-1",
      name: "read",
      argumentsDelta: "_path\":",
    });
  });

  it("rejects a malformed record wholesale instead of rendering shifted text", () => {
    expect(
      expandAssistantStream([
        { type: "text-chunks", time0: 1, index: 0, dt: [], texts: ["ok"] },
        { type: "text-chunks", time0: 2, index: 0, dt: "nope", texts: ["x"] },
      ]),
    ).toEqual([]);
  });
});

describe("AssistantStreamJournal", () => {
  const baseline = {
    revision: 3,
    activeAttempt: {
      attemptId: "a1",
      startedAfterSeq: 0,
      turn: 1,
      step: 1,
      nextIndex: 2,
      stream: [
        {
          type: "text-chunks",
          time0: 100,
          index: 0,
          // dt 是相邻成员的时间差：长度 = texts.length - 1。
          dt: [10, 10],
          texts: ["A", "B", "C"],
        },
      ],
    },
  };

  it("adopts the snapshot baseline and replays only up to nextIndex", () => {
    const fold = new ConversationFold();
    const journal = new AssistantStreamJournal();
    fold.apply({ type: "turn/start", seq: 0, time: 0, data: { turn: 1 } });
    journal.observeDurableSeq(0);
    const synthetic = journal.adoptBaseline(baseline);
    expect(synthetic).toHaveLength(2);
    for (const event of synthetic) fold.apply(event);
    expect(assistantText(fold)).toEqual(["AB"]);
    expect(partialFlags(fold)).toEqual([true]);
  });

  it("continues the attempt from the next delta and retires on the durable settlement", () => {
    const fold = new ConversationFold();
    const journal = new AssistantStreamJournal();
    fold.apply({ type: "turn/start", seq: 0, time: 0, data: { turn: 1 } });
    journal.observeDurableSeq(0);
    for (const event of journal.adoptBaseline(baseline)) fold.apply(event);

    const decision = journal.acceptFrame({
      type: "chunk",
      attemptId: "a1",
      revision: 4,
      index: 2,
      time: 130,
      chunk: { type: "text-delta", index: 0, text: "C" },
    });
    expect(decision?.kind).toBe("apply");
    if (decision?.kind !== "apply") return;
    for (const event of decision.events) fold.apply(event);
    expect(assistantText(fold)).toEqual(["ABC"]);

    // 结算事件（整数 seq）晚于合成事件（小数 seq）→ 原地替换流式占位。
    const settlement: WireSessionEvent = {
      type: "assistant/message",
      seq: 1,
      time: 200,
      data: {
        turn: 1,
        step: 1,
        message: { content: [{ type: "text", text: "ABC" }] },
        stream: [],
      },
    };
    journal.observeDurableSeq(settlement.seq);
    journal.settleDurable("assistant/message", 1, 1);
    expect(fold.applyIfNewer(settlement)).toBe(true);
    expect(assistantText(fold)).toEqual(["ABC"]);
    expect(partialFlags(fold)).toEqual([false]);
  });

  it("asks for a rebaseline when a delta index jumps", () => {
    const journal = new AssistantStreamJournal();
    journal.observeDurableSeq(0);
    journal.adoptBaseline(baseline);
    expect(
      journal.acceptFrame({
        type: "chunk",
        attemptId: "a1",
        revision: 4,
        index: 7,
        time: 1,
        chunk: { type: "text-delta", index: 0, text: "gap" },
      }),
    ).toEqual({ kind: "rebaseline" });
  });

  it("asks for a rebaseline when a frame revision skips", () => {
    const journal = new AssistantStreamJournal();
    journal.observeDurableSeq(-1);
    expect(
      journal.acceptFrame({
        type: "start",
        attemptId: "a9",
        revision: 1,
        startedAfterSeq: -1,
        turn: 2,
        step: 1,
      }),
    ).toBeNull();
    expect(
      journal.acceptFrame({
        type: "chunk",
        attemptId: "a9",
        revision: 3,
        index: 0,
        time: 1,
        chunk: { type: "text-delta", index: 0, text: "x" },
      }),
    ).toEqual({ kind: "rebaseline" });
  });

  it("clears the attempt on an end frame so the next start is not a rebaseline", () => {
    const journal = new AssistantStreamJournal();
    journal.observeDurableSeq(0);
    journal.acceptFrame({
      type: "start",
      attemptId: "a1",
      revision: 1,
      startedAfterSeq: 0,
      turn: 1,
      step: 1,
    });
    journal.acceptFrame({
      type: "chunk",
      attemptId: "a1",
      revision: 2,
      index: 0,
      time: 1,
      chunk: { type: "text-delta", index: 0, text: "A" },
    });
    journal.acceptFrame({
      type: "end",
      attemptId: "a1",
      revision: 3,
      index: 1,
      outcome: { kind: "committed", eventType: "assistant/message", seq: 1 },
    } satisfies AssistantStreamFrame);
    journal.observeDurableSeq(1);
    expect(
      journal.acceptFrame({
        type: "start",
        attemptId: "a2",
        revision: 4,
        startedAfterSeq: 1,
        turn: 1,
        step: 2,
      }),
    ).toBeNull();
  });
});

describe("ConversationFold 0.1.7 event vocabulary", () => {
  it("pairs a V4 tool/result (top-level toolCallId, direct content) with its call", () => {
    const fold = new ConversationFold();
    fold.apply({
      type: "tool/call",
      seq: 0,
      time: 0,
      data: { callId: "call-1", name: "read", arguments: '{"file_path":"a.ts"}' },
    });
    fold.apply({
      type: "tool/result",
      seq: 1,
      time: 1,
      data: {
        turn: 1,
        step: 1,
        message: {
          role: "tool",
          toolCallId: "call-1",
          content: [{ type: "text", text: "file body" }],
        },
      },
    });
    const tool = fold
      .snapshot()
      .items.find((item) => item.kind === "tool");
    expect(tool).toMatchObject({
      kind: "tool",
      callId: "call-1",
      name: "read",
      outcome: { kind: "success", text: "file body" },
    });
  });

  it("still pairs the 0.1.6 envelope shape (content[0] tool-result block)", () => {
    const fold = new ConversationFold();
    fold.apply({
      type: "tool/call",
      seq: 0,
      time: 0,
      data: { callId: "call-2", name: "bash", arguments: "{}" },
    });
    fold.apply({
      type: "tool/result",
      seq: 1,
      time: 1,
      data: {
        message: {
          content: [
            {
              type: "tool-result",
              toolCallId: "call-2",
              content: [{ type: "text", text: "legacy body" }],
            },
          ],
        },
      },
    });
    const tool = fold.snapshot().items.find((item) => item.kind === "tool");
    expect(tool).toMatchObject({
      kind: "tool",
      callId: "call-2",
      outcome: { kind: "success", text: "legacy body" },
    });
  });

  it("retires the streamed partial when the attempt is durably recorded as failed", () => {
    const fold = new ConversationFold();
    fold.apply({ type: "turn/start", seq: 0, time: 0, data: { turn: 1 } });
    fold.apply({
      type: "assistant/chunk",
      seq: 0.5,
      time: 1,
      data: { turn: 1, step: 1, chunk: { type: "text-delta", index: 0, text: "half" } },
    });
    expect(assistantText(fold)).toEqual(["half"]);
    fold.apply({
      type: "assistant/attempt",
      seq: 1,
      time: 2,
      data: { turn: 1, step: 1, stream: [] },
    });
    expect(assistantText(fold)).toEqual([]);
  });

  it("notes the new turn/end reasons", () => {
    const fold = new ConversationFold();
    for (const [seq, kind] of [
      [0, "interrupted"],
      [1, "blocked"],
      [2, "max-tokens"],
    ] as const) {
      fold.apply({
        type: "turn/end",
        seq,
        time: seq,
        data: { turn: 1, reason: { kind } },
      });
    }
    const notes = fold
      .snapshot()
      .items.filter((item) => item.kind === "note")
      .map((item) => (item.kind === "note" ? item.text : ""));
    expect(notes).toEqual(["运行被中断", "已阻止", "已达输出上限"]);
  });
});
