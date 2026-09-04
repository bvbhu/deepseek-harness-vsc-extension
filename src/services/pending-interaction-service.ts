/**
 * PendingInteractionService (M4): the pending-interaction closed loop —
 * approval / ask-user / plan-review minimal dialogs. Mirrors the reference
 * client's PendingWait list: one per-session Map keyed `a:<approvalId>` /
 * `q:<rpcId>`, fed from the four mux frames, settled only by the resolved
 * frames (member removal IS settlement — a failed respond never removes an
 * entry). The rpcId stays inside the extension host: webview answers carry
 * the opaque key, this service backfills the envelope rpcId and posts
 * /api/respond, checking the carrier receipt. Reconnect recovery is free:
 * the mux re-replays still-pending requested frames with the same rpcId, so
 * re-apply = payload refresh (Map.set), never a duplicate card.
 *
 * plan-review is not a separate event: it is an AskUserQuestionItem whose
 * `intent.kind === 'plan-review'` — narrowed here exactly like the reference
 * `planReviewOf` (single question, not multiSelect, options contain the
 * approve label and at most one other option), falling back to the generic
 * question flow otherwise.
 */

import { EventEmitter } from "node:events";
import type { WireClient, ServerRequest, RpcReceipt } from "../dsh/wire.ts";
import type {
  PendingAnswer,
  PendingItemView,
  PendingQuestionView,
} from "../shared/protocol.ts";

/** 一个 question 的 wire 原始形态（AskUserQuestionItem 结构镜像；数据留在此层）。 */
interface WireQuestion {
  id: string;
  question: string;
  detail?: string;
  header?: string;
  options?: { label: string; description?: string }[];
  multiSelect?: boolean;
  intent?: { kind: "plan-review"; approve: string };
}

interface PendingEntry {
  key: string;
  sessionId: string;
  /** 0.1.2 waterfall eventId（$events/result 应答凭据）。 */
  eventId: string;
  kind: "approval" | "question";
  view: PendingItemView;
  /** 创建时间戳：兜底结算只清理"足够老"的条目，避免同批帧竞态误关刚弹的框。 */
  createdAt: number;
}

/** 结构镜像的 mux 帧（本服务消费 4 类；其余帧不属本服务）。 */
type PendingFrame =
  | {
      type: "approval/requested";
      sessionId: string;
      approvalId: string;
      toolName: string;
      callId?: string;
      reason?: string;
    }
  | {
      type: "approval/resolved";
      sessionId: string;
      approvalId: string;
      outcome: string;
    }
  | { type: "question/requested"; sessionId: string; questions: WireQuestion[] }
  | {
      type: "question/resolved";
      sessionId: string;
      questionRpcId: string;
      outcome: string;
    };

export class PendingInteractionService extends EventEmitter {
  private readonly entries = new Map<string, PendingEntry>();

  constructor(private readonly wire: () => WireClient | null) {
    super();
  }

  /** The pending views for a session, oldest-first (Map insertion order). */
  snapshot(sessionId: string): PendingItemView[] {
    const views: PendingItemView[] = [];
    for (const entry of this.entries.values()) {
      if (entry.sessionId === sessionId) views.push(entry.view);
    }
    return views;
  }

  /** Route one mux frame into the pending map. */
  applyFrame(frame: ServerRequest): void {
    const payload = frame.payload as PendingFrame;
    switch (payload.type) {
      case "approval/requested": {
        const key = `a:${payload.approvalId}`;
        const view: PendingItemView = {
          kind: "approval",
          key,
          toolName: payload.toolName,
          ...(payload.reason !== undefined ? { reason: payload.reason } : {}),
          ...(payload.callId !== undefined ? { callId: payload.callId } : {}),
        };
        this.upsert(key, payload.sessionId, payload.approvalId, "approval", view);
        break;
      }
      case "approval/resolved":
        this.settle(`a:${payload.approvalId}`);
        break;
      case "question/requested": {
        const key = `q:${frame.rpcId}`;
        const view = narrowQuestions(key, payload.questions);
        this.upsert(key, payload.sessionId, frame.rpcId, "question", view);
        break;
      }
      case "question/resolved":
        this.settle(`q:${payload.questionRpcId}`);
        break;
      default:
        return; // not ours
    }
  }

  /**
   * Answer a pending interaction. approval answers the wire outcome;
   * question/plan-review answers the structured answer batch. Returns the
   * carrier receipt: `accepted:false` (not-pending/bad-response) is NOT an
   * error — the host already settled (race) or the encoding was wrong.
   *
   * 应答成功（accepted:true）即移除条目：0.1.2 的 $events 流仅在**取消**时
   * 发 cancel 行（→ onCancel → approval/resolved mux 帧），正常应答
   * （allow/deny/answer）不产生 resolved 帧——若依赖 resolved 帧结算，
   * 弹窗将永远不关闭。cancel 帧迟到时 settle 已无条目可删（幂等安全）。
   */
  async answer(
    sessionId: string,
    key: string,
    answer: PendingAnswer,
  ): Promise<RpcReceipt> {
    const entry = this.requireEntry(sessionId, key);
    const client = this.requireClient();
    const clientId = client.eventsClientIdValue;
    if (!clientId)
      return { accepted: false, reason: "dsh 尚未完成握手，无法应答" };
    const receipt =
      answer.kind === "approval"
        ? await client.answerEvent(clientId, entry.eventId, {
            kind: "result",
            value: answer.outcome,
          })
        : await client.answerEvent(clientId, entry.eventId, {
            kind: "result",
            value: { answers: answer.answers },
          });
    if (receipt.accepted === true) this.settle(key);
    return receipt;
  }

  /** Cancel a pending question/plan-review (= cancelled error; approval has no client cancel). */
  async cancel(sessionId: string, key: string): Promise<RpcReceipt> {
    const entry = this.requireEntry(sessionId, key);
    if (entry.view.kind === "approval") {
      throw new Error(
        "审批请求没有取消出口（wire 仅 allowed-once/rejected 两结局）",
      );
    }
    const client = this.requireClient();
    const clientId = client.eventsClientIdValue;
    if (!clientId)
      return { accepted: false, reason: "dsh 尚未完成握手，无法应答" };
    const receipt = await client.answerEvent(clientId, entry.eventId, {
      kind: "rejected",
      error: {
        code: "cancelled",
        message: "the user closed this question request",
        details: {},
      },
    });
    if (receipt.accepted === true) this.settle(key);
    return receipt;
  }

  private upsert(
    key: string,
    sessionId: string,
    eventId: string,
    kind: "approval" | "question",
    view: PendingItemView,
  ): void {
    // Replay of the same eventId refreshes the payload in place (Map.set keeps
    // insertion order — oldest-first preserved); still notify so the webview
    // re-renders the (possibly refreshed) card, but never a duplicate entry.
    // 重放不得重置 createdAt：兜底结算按条目年龄保护，重放刷新生效后不应
    // 让"新弹的框"立刻变成"老条目"而被误关。
    const existing = this.entries.get(key);
    this.entries.set(key, {
      key,
      sessionId,
      eventId,
      kind,
      view,
      createdAt: existing?.createdAt ?? Date.now(),
    });
    this.emit("change", sessionId);
  }

  /** Frame-driven settlement: the authoritative resolved frame removes the wait. */
  private settle(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    this.emit("change", entry.sessionId);
  }

  /**
   * 多端兜底结算：agent 推进到明确的回合边界（turn/end、turn/start）时，说明
   * 该会话的 pending 交互必然已结算——浏览器端应答后扩展端收不到 resolved 帧
   * （DSH 仅在取消时发 cancel 行），靠回合边界兜底关闭，避免多端不同步。
   *
   * 只清理"足够老"的条目：approval/requested 与随后的回合边界帧可能在同一批
   * mux 数据里到达（或被流交错），刚弹的框若立即被清，用户来不及点击"允许"。
   * minAgeMs 默认 1000ms——正常流程下回合边界必然在用户应答（或对端应答）
   * 之后才出现，弹窗已展示足够时间。
   */
  settleBySession(sessionId: string, minAgeMs = 1_000): void {
    const cutoff = Date.now() - minAgeMs;
    let changed = false;
    for (const entry of [...this.entries.values()]) {
      if (entry.sessionId === sessionId && entry.createdAt <= cutoff) {
        this.entries.delete(entry.key);
        changed = true;
      }
    }
    if (changed) this.emit("change", sessionId);
  }

  private requireEntry(sessionId: string, key: string): PendingEntry {
    const entry = this.entries.get(key);
    if (!entry || entry.sessionId !== sessionId)
      throw new Error("待应答交互不存在或已结算");
    return entry;
  }

  private requireClient(): WireClient {
    const client = this.wire();
    if (!client) throw new Error("dsh web 尚未就绪");
    return client;
  }
}

/**
 * 收窄 question 请求为可渲染视图：plan-review 决策卡当且仅当单问题 + 声明
 * plan-review intent + detail 即计划正文 + 选项含 approve label 且至多一个
 * 其它选项 + 非多选（对齐参考 planReviewOf——第三方选项或多选批次是两按钮表达
 * 不了的，退回通用问询，保证每个请求都可应答）。
 */
function narrowQuestions(
  key: string,
  questions: WireQuestion[],
): PendingItemView {
  const review = planReviewOf(questions);
  if (review) return { kind: "plan-review", key, ...review };
  return {
    kind: "question",
    key,
    items: questions.map((q): PendingQuestionView => {
      const view: PendingQuestionView = {
        id: q.id,
        question: q.question,
        ...(q.detail !== undefined ? { detail: q.detail } : {}),
        ...(q.header !== undefined ? { header: q.header } : {}),
        ...(q.multiSelect === true ? { multiSelect: true } : {}),
        ...(q.options !== undefined && q.options.length > 0
          ? { options: q.options }
          : {}),
      };
      return view;
    }),
  };
}

function planReviewOf(
  questions: WireQuestion[],
): Omit<
  Extract<PendingItemView, { kind: "plan-review" }>,
  "kind" | "key"
> | null {
  if (questions.length !== 1) return null;
  const q = questions[0];
  if (q === undefined) return null;
  if (q.intent?.kind !== "plan-review") return null;
  if (q.detail === undefined) return null;
  if (q.multiSelect === true) return null;
  const options = q.options ?? [];
  const approve = options.find((o) => o.label === q.intent?.approve);
  if (approve === undefined) return null;
  const others = options.filter((o) => o !== approve);
  if (others.length > 1) return null;
  return {
    id: q.id,
    question: q.question,
    plan: q.detail,
    approve: approve.label,
    ...(others.length === 1 ? { decline: others[0]!.label } : {}),
  };
}
