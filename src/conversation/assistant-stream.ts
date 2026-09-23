/**
 * Assistant 实时流（dsh 0.1.7 / Session 日志 V4）——把 `session/follow` 的
 * out-of-band assistant 帧折成 fold 能吃的合成 `assistant/chunk` 事件。
 *
 * 0.1.6 及更早：正文逐 delta 以 `assistant/chunk` 会话事件落日志，客户端只需折叠事件。
 * 0.1.7 起该事件不再存在（V4 事件表里没有 `assistant/chunk`），实时正文改由
 * `session/follow` 自己的帧承载，且必须显式请求 `assistantStream: true`：
 *
 *   snapshot.assistantStream?: { revision, activeAttempt?: {
 *     attemptId, startedAfterSeq, turn, step, nextIndex, stream } }   // 重连基线
 *   { type: 'assistant-stream', frame: {
 *       type: 'start', attemptId, revision, startedAfterSeq, turn, step }
 *   | { type: 'chunk', attemptId, revision, index, time, chunk }
 *   | { type: 'end', attemptId, revision, index,
 *       outcome: { kind: 'committed', eventType, seq } | { kind: 'abandoned' } } }
 *
 * `frame.revision` 每条 +1；`index` 是 attempt 内单调的 delta 序号（重试会重开
 * 一条新 attempt）。基线里的 `stream` 是已 compact 的前缀记录（与
 * `assistant/message.stream` 同构），必须展开回逐 delta。任一序号/序号跳空都
 * 意味着本地窗口已不完整 —— 参考实现（dsh-api-session-controller 的
 * client/sessions/assistant-stream）此时重启 follow 流重取快照，这里同样以
 * `rebaseline` 决策交回上层重连。
 *
 * 合成事件的 seq 取「下一个持久 seq 之下的小数」：既严格大于已见水位、又严格
 * 小于将要落定的持久结算事件，因此 `assistant/message`（整数 seq）到达时能原地
 * 替换流式占位，与 0.1.6 的 `assistant/chunk` 折叠路径共用同一段 fold 逻辑。
 */

/** 结构性镜像 dsh-llm 的 StreamChunk（只列出 fold 需要区分的变体）。 */
export type StreamChunk =
  | { type: "block-start"; index: number; blockType: string }
  | { type: "text-delta"; index: number; text: string }
  | { type: "reasoning-delta"; index: number; text: string }
  | {
      type: "tool-call-delta";
      index: number;
      id: string;
      name?: string;
      argumentsDelta: string;
    }
  | { type: "block-end"; index: number; block: { type: string; text?: string } }
  | { type: "usage"; usage: unknown }
  | { type: "finish"; reason: unknown };

/** 一条 chunk 及其原始时间戳（dsh-llm TimedStreamChunk）。 */
export interface TimedStreamChunk {
  time: number;
  chunk: StreamChunk;
}

/** dsh-llm AssistantStreamRecord：一次 attempt 的无损紧凑记录。 */
export type AssistantStreamRecord =
  | {
      type: "text-chunks";
      time0: number;
      index: number;
      dt: readonly number[];
      texts: readonly string[];
    }
  | {
      type: "reasoning-chunks";
      time0: number;
      index: number;
      dt: readonly number[];
      texts: readonly string[];
    }
  | {
      type: "tool-call-chunks";
      time0: number;
      index: number;
      dt: readonly number[];
      id: string;
      name?: string;
      args: readonly string[];
    }
  | { type: "chunk"; time: number; chunk: StreamChunk };

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function numberOf(record: Record<string, unknown>, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringArray(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const out: string[] = [];
  for (const entry of value) {
    if (typeof entry !== "string") return null;
    out.push(entry);
  }
  return out;
}

/**
 * 展开紧凑记录为逐 delta（镜像 dsh-llm expandAssistantStream 的校验与时间重建）。
 * 任一条记录不可读即整体返回 []（宁可不渲染，也不渲染错位的正文）。
 */
export function expandAssistantStream(
  records: readonly unknown[],
): TimedStreamChunk[] {
  const out: TimedStreamChunk[] = [];
  for (const candidate of records) {
    const record = asRecord(candidate);
    if (record === null) return [];
    const type = record["type"];
    if (type === "chunk") {
      const time = numberOf(record, "time");
      const chunk = asRecord(record["chunk"]);
      if (time === null || chunk === null) return [];
      out.push({ time, chunk: chunk as unknown as StreamChunk });
      continue;
    }
    if (
      type !== "text-chunks" &&
      type !== "reasoning-chunks" &&
      type !== "tool-call-chunks"
    )
      return [];
    const time0 = numberOf(record, "time0");
    const index = numberOf(record, "index");
    const dt = Array.isArray(record["dt"]) ? record["dt"] : null;
    if (time0 === null || index === null || dt === null) return [];
    const members =
      type === "tool-call-chunks"
        ? stringArray(record["args"])
        : stringArray(record["texts"]);
    if (members === null) return [];
    let time = time0;
    for (let position = 0; position < members.length; position += 1) {
      if (position > 0) {
        const delta = dt[position - 1];
        if (typeof delta !== "number") return [];
        time += delta;
      }
      const text = members[position] ?? "";
      const chunk: StreamChunk =
        type === "text-chunks"
          ? { type: "text-delta", index, text }
          : type === "reasoning-chunks"
            ? { type: "reasoning-delta", index, text }
            : {
                type: "tool-call-delta",
                index,
                id: typeof record["id"] === "string" ? record["id"] : "",
                ...(typeof record["name"] === "string"
                  ? { name: record["name"] }
                  : {}),
                argumentsDelta: text,
              };
      out.push({ time, chunk });
    }
  }
  return out;
}

/** 快照里仍存活的 attempt 基线（session/follow snapshot.assistantStream）。 */
export interface AssistantStreamBaseline {
  revision: number;
  activeAttempt?: {
    attemptId: string;
    startedAfterSeq: number;
    turn: number;
    step: number;
    nextIndex: number;
    stream: readonly unknown[];
  };
}

/** session/follow 的 assistant 帧（SessionAssistantStreamFrame）。 */
export type AssistantStreamFrame =
  | {
      type: "start";
      attemptId: string;
      revision: number;
      startedAfterSeq: number;
      turn: number;
      step: number;
    }
  | {
      type: "chunk";
      attemptId: string;
      revision: number;
      index: number;
      time: number;
      chunk: unknown;
    }
  | {
      type: "end";
      attemptId: string;
      revision: number;
      index: number;
      outcome:
        | { kind: "committed"; eventType: string; seq: number }
        | { kind: "abandoned" };
    };

/** 一条待折叠的合成流式事件（fold 侧与 assistant/chunk 同形）。 */
export interface SyntheticChunkEvent {
  type: "assistant/chunk";
  seq: number;
  time: number;
  data: { turn: number; step: number; chunk: StreamChunk };
}

/** journal 对一帧的判定。 */
export type AssistantStreamDecision =
  | { kind: "apply"; events: SyntheticChunkEvent[] }
  | { kind: "rebaseline" }
  | null;

/** 当前存活的 attempt 状态（不含正文）。 */
interface ActiveAttempt {
  attemptId: string;
  turn: number;
  step: number;
  nextIndex: number;
  /** 该 attempt 起于哪个持久 seq 之后（合成 seq 的下界）。 */
  startedAfterSeq: number;
}

/**
 * 单会话的 assistant 流折叠器：消费快照基线与 follow 帧，产出合成流式事件。
 * 与参考实现 ClientAssistantStream 同构（start/chunk/end 的序号与 revision 校验、
 * rebaseline 判定），但不做持久结算事件的中转 —— 扩展侧的 fold 本来就用
 * 「同 (turn, step) 的 assistant/message 原地替换流式占位」表达结算。
 */
export class AssistantStreamJournal {
  private active: ActiveAttempt | null = null;
  private revision: number | null = null;
  /** 供 fold 重建时重放的合成事件（当前 attempt 的全部 delta）。 */
  private emitted: SyntheticChunkEvent[] = [];
  /** 下一个持久 seq 的基准：合成 seq 必须落在它之下。 */
  private durableCursor = -1;

  /** 记录一个已应用的持久事件 seq（合成 seq 的水位）。 */
  observeDurableSeq(seq: number): void {
    if (seq > this.durableCursor) this.durableCursor = seq;
  }

  /** 当前 attempt 的合成事件（loadOlder 等重建后重放用）。 */
  transientEvents(): SyntheticChunkEvent[] {
    return [...this.emitted];
  }

  /**
   * 持久结算事件（assistant/message / assistant/attempt）到达时清掉对应的活动
   * attempt：重连后我们可能错过了它的 `end` 帧，而「活动 attempt 还在」会让下一条
   * start 被误判成 rebaseline。占位替换本身由 fold 按 (turn, step) 完成。
   * @returns 是否确实清掉了一个 attempt。
   */
  settleDurable(type: string, turn: unknown, step: unknown): boolean {
    if (type !== "assistant/message" && type !== "assistant/attempt")
      return false;
    const active = this.active;
    if (active === null || active.turn !== turn || active.step !== step)
      return false;
    this.active = null;
    return true;
  }

  /**
   * 采用快照基线：清空本地流状态，按 activeAttempt 的 compact 前缀重建
   * 已见 delta（只重放到 nextIndex），并返回需要追加到持久窗口之后的合成事件。
   */
  adoptBaseline(baseline: AssistantStreamBaseline | undefined): SyntheticChunkEvent[] {
    this.active = null;
    this.revision = null;
    this.emitted = [];
    const opening = baseline?.activeAttempt;
    if (baseline !== undefined && typeof baseline.revision === "number")
      this.revision = baseline.revision;
    if (opening === undefined) return [];
    this.active = {
      attemptId: opening.attemptId,
      turn: opening.turn,
      step: opening.step,
      nextIndex: opening.nextIndex,
      startedAfterSeq: opening.startedAfterSeq,
    };
    const members = expandAssistantStream(opening.stream);
    const limit = Math.min(opening.nextIndex, members.length);
    for (let index = 0; index < limit; index += 1) {
      const member = members[index];
      if (member === undefined) break;
      this.emit(opening.turn, opening.step, member);
    }
    return [...this.emitted];
  }

  /** 折叠一帧 assistant-stream；返回「应用合成事件 / 需要重连重取快照 / 忽略」。 */
  acceptFrame(frame: AssistantStreamFrame): AssistantStreamDecision {
    switch (frame.type) {
      case "start": {
        // 已在流式中又来一条 start：本地窗口已不确定（重试/换 attempt）。
        if (this.active !== null) return { kind: "rebaseline" };
        this.active = {
          attemptId: frame.attemptId,
          turn: frame.turn,
          step: frame.step,
          nextIndex: 0,
          startedAfterSeq: frame.startedAfterSeq,
        };
        this.emitted = [];
        this.revision = frame.revision;
        return null;
      }
      case "chunk": {
        const active = this.active;
        // 挂载前就开始的 attempt 没有 start 帧：忽略其 delta 尾巴，
        // 由持久结算事件直接落定（对齐参考实现）。
        if (active === null || active.attemptId !== frame.attemptId) return null;
        if (active.nextIndex !== frame.index) return { kind: "rebaseline" };
        if (this.revision !== null && frame.revision !== this.revision + 1)
          return { kind: "rebaseline" };
        const chunk = asRecord(frame.chunk);
        if (chunk === null) return { kind: "rebaseline" };
        active.nextIndex += 1;
        this.revision = frame.revision;
        this.emit(active.turn, active.step, {
          time: frame.time,
          chunk: chunk as unknown as StreamChunk,
        });
        return {
          kind: "apply",
          events: [this.emitted[this.emitted.length - 1]!],
        };
      }
      case "end": {
        const active = this.active;
        if (active === null || active.attemptId !== frame.attemptId) return null;
        if (active.nextIndex !== frame.index) return { kind: "rebaseline" };
        this.revision = frame.revision;
        this.active = null;
        // 成功/放弃：合成占位由随后的持久事件（assistant/message 或
        // assistant/attempt）或下一次 start 覆盖，这里只清 attempt 状态。
        return null;
      }
    }
  }

  /** 合成一条流式事件并登记（seq 落在持久水位与下一个持久 seq 之间）。 */
  private emit(turn: number, step: number, member: TimedStreamChunk): void {
    const gap = this.emitted.length + 1;
    // 上限取「已见持久水位」与「attempt 起始水位」的较大者再加 1：即使会话日志
    // 还没有任何持久事件（水位 -1），合成 seq 也严格大于 fold 的 -1 水位、
    // 且严格小于紧接着落定的持久事件。
    const ceiling =
      Math.max(this.durableCursor, this.active?.startedAfterSeq ?? -1) + 1;
    const event: SyntheticChunkEvent = {
      type: "assistant/chunk",
      seq: ceiling - 1 / (gap + 1),
      time: member.time,
      data: { turn, step, chunk: member.chunk },
    };
    this.emitted.push(event);
  }
}
