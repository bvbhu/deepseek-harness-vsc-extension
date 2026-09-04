/**
 * ConversationService (M2): per-session ConversationFold state keyed by
 * sessionId, fed from two sources that must converge on the same surface —
 * live mux frames and `session/follow` replay (0.1.2 replaced session.history
 * with a follow stream: one `snapshot` frame then gap-free `event` frames).
 * `attach` opens the follow stream, seeds a fold from the snapshot (which
 * already carries the in-flight partial), and keeps the stream open to fold
 * live frames. `loadOlder` pages backwards via `session/page`.
 *
 * 0.1.2 packs the snapshot's in-flight chunk stream into `chunkrow/*` run
 * records; those are unpacked back into the per-delta `assistant/chunk`
 * events the fold already understands (live follow frames are already raw
 * `assistant/chunk` events and need no unpacking).
 *
 * M4b: the follow snapshot's `projections` block is forwarded through the
 * optional `onProjections` callback so the ProjectionService can seed its
 * store from the same single call.
 */

import { EventEmitter } from "node:events";
import {
  ConversationFold,
  type WireSessionEvent,
} from "../conversation/fold.ts";
import type { WireClient, ServerRequest } from "../dsh/wire.ts";
import type { ConversationSnapshot } from "../shared/protocol.ts";
import type { ProjectionsBlock } from "./projection-service.ts";

/** The mux frame payloads this service consumes (others are M4/v2). */
type MuxFrame = {
  type: "session/event";
  sessionId: string;
  event: WireSessionEvent;
};

/** Per-session tracked state: the fold plus the raw event window it was built from. */
interface TrackedSession {
  fold: ConversationFold;
  /** All applied events in strictly ascending seq order (deduped by seq). */
  events: WireSessionEvent[];
  /** True when older events exist below the loaded window（loadOlder 可用）。 */
  hasMore: boolean;
  /** loadOlder in flight（防重复翻页）。 */
  loadingOlder: boolean;
  /** Last committed seq from the follow snapshot（loadOlder 的 throughSeq 基准）。 */
  cursor: number;
  /** Dispose the live follow stream on detach/re-attach. */
  disposeFollow?: () => void;
}

/** Rebuild a fold from an ordered, seq-deduped event list (replay determinism). */
function buildFold(events: WireSessionEvent[]): ConversationFold {
  const fold = new ConversationFold();
  for (const event of events) fold.apply(event);
  return fold;
}

/** Merge event lists into one ascending, seq-deduped list (first wins per seq). */
function mergeEvents(lists: WireSessionEvent[][]): WireSessionEvent[] {
  const bySeq = new Map<number, WireSessionEvent>();
  for (const list of lists) {
    for (const event of list)
      if (!bySeq.has(event.seq)) bySeq.set(event.seq, event);
  }
  return [...bySeq.values()].sort((a, b) => a.seq - b.seq);
}

/**
 * Unpack a `chunkrow/text-chunks` / `chunkrow/reasoning-chunks` run back into
 * the per-delta `assistant/chunk` events the fold consumes. `tool-call-chunks`
 * are skipped (fold already ignores tool-call-delta; the tool card comes from
 * `tool/call` events).
 */
function unpackChunkRun(
  row: WireSessionEvent,
  out: WireSessionEvent[],
): void {
  if (row.type === "chunkrow/tool-call-chunks") return;
  const data = row.data as
    | { turn?: number; step?: number; texts?: unknown }
    | undefined;
  const texts = data?.texts;
  if (
    !Array.isArray(texts) ||
    typeof data?.turn !== "number" ||
    typeof data?.step !== "number"
  )
    return;
  const kind =
    row.type === "chunkrow/reasoning-chunks" ? "reasoning-delta" : "text-delta";
  texts.forEach((text, index) => {
    if (typeof text !== "string") return;
    out.push({
      type: "assistant/chunk",
      seq: row.seq + index,
      time: row.time + index,
      data: {
        turn: data.turn,
        step: data.step,
        chunk: { type: kind, index: 0, text },
      },
    });
  });
}

/** Unpack a session/page or session/follow snapshot's records into fold events. */
function unpackRecords(records: unknown[]): WireSessionEvent[] {
  const out: WireSessionEvent[] = [];
  for (const record of records) {
    const row = record as
      | { type?: string; event?: WireSessionEvent }
      | undefined;
    if (!row?.event) continue;
    if (row.type === "event") out.push(row.event);
    else if (row.type === "chunks") unpackChunkRun(row.event, out);
  }
  return out;
}

export class ConversationService extends EventEmitter {
  private readonly tracked = new Map<string, TrackedSession>();
  /** Frames buffered while a fetch is mid-flight (drained seq-deduped). */
  private readonly pending = new Map<string, WireSessionEvent[]>();
  /** M4b: optional projections-block sink (seeded from the follow snapshot). */
  private readonly onProjections?: (
    sessionId: string,
    block: ProjectionsBlock,
  ) => void;

  constructor(
    private readonly wire: () => WireClient | null,
    onProjections?: (sessionId: string, block: ProjectionsBlock) => void,
  ) {
    super();
    this.onProjections = onProjections;
  }

  /** The folded snapshot for a session, or null when not attached yet. */
  snapshot(sessionId: string): ConversationSnapshot | null {
    const tracked = this.tracked.get(sessionId);
    if (!tracked) return null;
    return {
      ...tracked.fold.snapshot(),
      hasMore: tracked.hasMore,
    };
  }

  /**
   * Rebuild the fold for a session from its follow snapshot, then keep the
   * stream open for live `event` frames. Frames that race in during the
   * snapshot merge into the window by seq. Events already held survive — a
   * resync/re-select never drops messages the user previously loaded.
   */
  async attach(sessionId: string): Promise<ConversationSnapshot> {
    const client = this.requireClient();
    const previous = this.tracked.get(sessionId);
    previous?.disposeFollow?.();
    const buffered = this.pending.get(sessionId) ?? [];
    this.pending.set(sessionId, buffered);

    return await new Promise<ConversationSnapshot>((resolve, reject) => {
      let settled = false;
      let timer: NodeJS.Timeout | undefined;
      const cleanup = (): void => {
        if (timer !== undefined) clearTimeout(timer);
      };
      const finish = (snapshot: ConversationSnapshot): void => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(snapshot);
      };
      const fail = (error: Error): void => {
        if (settled) return;
        settled = true;
        cleanup();
        // 失败路径：关闭流订阅（避免幽灵订阅在 WS 重连后被重新提交）
        // 并清除 pending 缓冲（否则后续事件永久缓冲而不应用 → 僵尸会话）。
        dispose();
        this.pending.delete(sessionId);
        reject(error);
      };

      const dispose = client.openStream(
        "session/follow",
        { request: { address: { kind: "session", sessionId } } },
        {
          onItem: (value) => {
            const row = value as
              | {
                  type?: string;
                  cursor?: number;
                  records?: unknown[];
                  hasMore?: boolean;
                  projections?: unknown;
                  event?: WireSessionEvent;
                }
              | undefined;
            if (row?.type === "snapshot") {
              const records = Array.isArray(row.records) ? row.records : [];
              const events = unpackRecords(records);
              if (row.projections !== undefined)
                this.onProjections?.(
                  sessionId,
                  row.projections as ProjectionsBlock,
                );
              const merged = mergeEvents([
                previous?.events ?? [],
                events,
                buffered.splice(0),
              ]);
              const tracked: TrackedSession = {
                fold: buildFold(merged),
                events: merged,
                hasMore: row.hasMore === true,
                loadingOlder: false,
                cursor: typeof row.cursor === "number" ? row.cursor : -1,
                disposeFollow: dispose,
              };
              this.tracked.set(sessionId, tracked);
              // 快照已应用：清除 pending 缓冲。attach 入口曾将 sessionId 放入
              // pending 以缓冲快照前到达的实时帧；splice 清空后空数组仍为
              // truthy，applyLiveEvent 会把后续帧持续缓冲而不应用，导致对话
              // 只在切换会话时加载一次、不再实时更新。清除后实时帧直入 fold。
              this.pending.delete(sessionId);
              this.emit("change", sessionId);
              finish(this.snapshot(sessionId) as ConversationSnapshot);
              return; // stream stays open for live frames
            }
            if (row?.type === "event" && row.event) {
              this.applyLiveEvent(sessionId, row.event);
            }
          },
          onError: (error) =>
            fail(new Error(`session/follow 失败: ${error.message}`)),
          onEnd: () =>
            fail(new Error("session/follow 在快照前结束")),
        },
      );
      timer = setTimeout(
        () => fail(new Error("session/follow 快照超时")),
        10_000,
      );
    });
  }

  /**
   * Load the previous history page (backwards from the window's earliest seq)
   * via session/page and prepend it, rebuilding the fold. No-op when complete
   * or already loading. session/page.throughSeq is strict-required.
   */
  async loadOlder(sessionId: string): Promise<ConversationSnapshot> {
    const initial = this.tracked.get(sessionId);
    if (!initial || !initial.hasMore || initial.loadingOlder) {
      return this.snapshot(sessionId) as ConversationSnapshot;
    }
    const earliestSeq = initial.events[0]?.seq;
    if (earliestSeq === undefined)
      return this.snapshot(sessionId) as ConversationSnapshot;
    initial.loadingOlder = true;
    try {
      const page = await this.requireClient().call<{
        records: unknown[];
        hasMore: boolean;
      }>("session/page", {
        request: {
          address: { kind: "session", sessionId },
          throughSeq: earliestSeq - 1,
          maxMessages: 50,
        },
      });
      const events = unpackRecords(page.records);
      const target = this.tracked.get(sessionId) ?? initial;
      target.events = mergeEvents([events, target.events]);
      target.fold = buildFold(target.events);
      target.hasMore = page.hasMore;
      this.emit("change", sessionId);
      return this.snapshot(sessionId) as ConversationSnapshot;
    } finally {
      const target = this.tracked.get(sessionId) ?? initial;
      target.loadingOlder = false;
    }
  }

  /**
   * Seed only the projection store for a session (no fold rebuild). Opens a
   * follow stream, forwards the snapshot projections, then disposes it.
   */
  async seedProjections(sessionId: string): Promise<void> {
    const client = this.requireClient();
    await new Promise<void>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      const dispose = client.openStream(
        "session/follow",
        { request: { address: { kind: "session", sessionId }, maxMessages: 0 } },
        {
          onItem: (value) => {
            const row = value as
              | { type?: string; projections?: unknown }
              | undefined;
            if (row?.type !== "snapshot") return;
            if (row.projections !== undefined)
              this.onProjections?.(
                sessionId,
                row.projections as ProjectionsBlock,
              );
            if (timer !== undefined) clearTimeout(timer);
            dispose();
            resolve();
          },
          onError: (error) => {
            if (timer !== undefined) clearTimeout(timer);
            reject(new Error(error.message));
          },
          onEnd: () => {
            if (timer !== undefined) clearTimeout(timer);
            resolve();
          },
        },
      );
      timer = setTimeout(() => {
        dispose();
        resolve();
      }, 8_000);
    });
  }

  /** Route one mux frame into the owning session's fold (or its pending buffer). */
  applyFrame(frame: ServerRequest): void {
    const payload = frame.payload as MuxFrame;
    if (payload.type !== "session/event" || !payload.event) return;
    this.applyLiveEvent(payload.sessionId, payload.event);
  }

  /** Detach one session (close its follow stream and drop its fold). */
  detach(sessionId: string): void {
    const tracked = this.tracked.get(sessionId);
    tracked?.disposeFollow?.();
    this.tracked.delete(sessionId);
    this.pending.delete(sessionId);
  }

  /** Re-attach every tracked session (mux reconnect; see module doc). */
  async resync(): Promise<void> {
    for (const sessionId of [...this.tracked.keys()])
      await this.attach(sessionId);
  }

  private applyLiveEvent(sessionId: string, event: WireSessionEvent): void {
    const buffered = this.pending.get(sessionId);
    if (buffered) {
      buffered.push(event);
      return;
    }
    const tracked = this.tracked.get(sessionId);
    if (!tracked) return; // unattached session: follow snapshot will cover it
    if (tracked.fold.applyIfNewer(event)) {
      tracked.events.push(event);
      this.emit("change", sessionId);
    }
    // 多端兜底：turn 边界事件（无论是否新）→ 通知外部结算该会话的 pending 交互。
    // DSH 仅在取消时发 cancel 行，正常应答（allow/deny/answer）不产生 resolved 帧，
    // 浏览器端应答后扩展端靠回合边界兜底关闭，避免多端不同步。
    if (event.type === "turn/end" || event.type === "turn/start") {
      this.emit("turnBoundary", sessionId);
    }
  }

  private requireClient(): WireClient {
    const client = this.wire();
    if (!client) throw new Error("dsh web 尚未就绪");
    return client;
  }
}
