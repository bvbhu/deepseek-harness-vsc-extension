/**
 * dsh web transport (0.1.2 generation, "typert").
 *
 * 0.1.2 removed the old `dsh-host-apiproxy` dot-method surface and replaced it
 * with one HTTP RPC route plus one multiplexed WebSocket:
 *   POST <base>/api/<namespace>/<method>   {type:"client-request", rpcId,
 *                                           method, payload:{args:{...}}}
 *   WS   <ws>/api/remote.mux               {type:"open", streamId, endpoint,
 *                                           payload:{args:{...}}}
 *                                          <- {type:"item"|"end"|"error",
 *                                             streamId, value|error}
 * Every request and the mux upgrade also need the session cookie minted by the
 * launch-token exchange (auth.ts). Envelope shapes (client-request /
 * server-response, rpcId echo) are unchanged, so the legacy ServerRequest
 * frame shape is preserved for the consumers below the transport.
 */

import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { WebSocket } from "ws";

// ---- envelope ----

export interface RpcError {
  code: string;
  message: string;
  details?: unknown;
}

export type RpcResult<T> =
  | { ok: true; value: T }
  | { ok: false; error: RpcError };

export interface ClientRequest {
  type: "client-request";
  rpcId: string;
  method: string;
  payload: { args: Record<string, unknown> };
}

export interface ServerResponse {
  type: "server-response";
  rpcId: string;
  result: RpcResult<unknown>;
}

/** Legacy internal frame shape, still emitted by dsh-service for consumers. */
export interface ServerRequest {
  type: "server-request";
  rpcId: string;
  method: string;
  payload: unknown;
}

/** Business failure (`result.ok === false`). */
export class DshRpcError extends Error {
  readonly code: string;
  readonly details: unknown;

  constructor(error: RpcError) {
    super(error.message);
    this.name = "DshRpcError";
    this.code = error.code;
    this.details = error.details;
  }
}

/** Receipt for a one-shot submit (waterfall answer); legacy webview shape. */
export type RpcReceipt =
  | { accepted: true }
  | { accepted: false; reason: string };

/** Optional envelope validator; absent = structural checks only. */
export interface EnvelopeValidator {
  validateServerResponse(value: unknown): boolean;
  validateServerRequest(value: unknown): boolean;
}

// ---- multiplexed stream transport ----

/** Frame handlers for one logical stream over /api/remote.mux. */
export interface StreamHandlers {
  onItem?: (value: unknown) => void;
  onEnd?: () => void;
  onError?: (error: { code?: string; message: string }) => void;
}

interface Subscription {
  id: string;
  endpoint: string;
  args: Record<string, unknown>;
  handlers: StreamHandlers;
}

/**
 * One physical WebSocket to /api/remote.mux carrying every logical stream.
 * Emits `open` / `close` for lifecycle, `frame` for raw frames, and
 * `streamError` for mux-level failures. Subscriptions survive reconnects: they
 * are resubmitted on every `open`.
 */
export class RemoteMux extends EventEmitter {
  private readonly url: string;
  private readonly cookie?: string;
  private readonly subscriptions = new Map<string, Subscription>();
  private ws: WebSocket | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelayMs = 1_000;
  private readonly maxReconnectDelayMs: number;
  private closed = false;

  constructor(options: {
    baseUrl: string;
    cookie?: string;
    reconnectDelayMs?: number;
    maxReconnectDelayMs?: number;
  }) {
    super();
    this.url =
      options.baseUrl.replace(/^http:/u, "ws:").replace(/^https:/u, "wss:").replace(/\/+$/u, "") +
      "/api/remote.mux";
    this.cookie = options.cookie;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1_000;
    this.maxReconnectDelayMs = options.maxReconnectDelayMs ?? 15_000;
  }

  get isOpen(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  /** Total live logical streams (diagnostics). */
  get streamCount(): number {
    return this.subscriptions.size;
  }

  /** Subscribe one logical stream; returns a dispose function. */
  openStream(
    endpoint: string,
    args: Record<string, unknown>,
    handlers: StreamHandlers,
  ): () => void {
    const id = randomUUID();
    this.subscriptions.set(id, { id, endpoint, args, handlers });
    if (this.isOpen) this.sendOpen(id, endpoint, args);
    return () => {
      this.subscriptions.delete(id);
      // 服务端只认 `cancel` 帧来关闭逻辑流（parseRemoteStreamClientMessage 只
      // 接受 `open` / `cancel`）；发 `close` 会被当成非法消息 → 1008 踢掉整个 mux。
      this.sendRaw({ type: "cancel", streamId: id });
    };
  }

  connect(): void {
    if (!this.closed && this.reconnectTimer === null && this.ws === null) {
      this.attemptConnect();
    }
  }

  close(): void {
    this.closed = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.terminate();
    this.ws = null;
    this.subscriptions.clear();
  }

  private attemptConnect(): void {
    const ws = new WebSocket(this.url, {
      headers: this.cookie ? { cookie: this.cookie } : {},
      followRedirects: false,
    });
    this.ws = ws;

    ws.on("open", () => {
      this.reconnectDelayMs = 1_000;
      this.emit("open");
      for (const sub of this.subscriptions.values()) {
        this.sendOpen(sub.id, sub.endpoint, sub.args);
      }
    });
    ws.on("message", (data) => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(String(data));
      } catch {
        this.emit("frame", "(unparsed)");
        return;
      }
      this.dispatch(parsed);
    });
    ws.on("error", () => {
      // close follows; surfaced there
    });
    ws.on("close", (code, reason) => {
      this.ws = null;
      this.emit("close", code, reason.toString("utf8"));
      if (this.closed) return;
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.attemptConnect();
      }, this.reconnectDelayMs);
      this.reconnectDelayMs = Math.min(
        this.reconnectDelayMs * 2,
        this.maxReconnectDelayMs,
      );
    });
  }

  private sendOpen(id: string, endpoint: string, args: Record<string, unknown>): void {
    if (
      !this.sendRaw({
        type: "open",
        streamId: id,
        endpoint,
        payload: { args },
      })
    ) {
      this.subscriptions.delete(id);
      this.emit(
        "streamError",
        new Error(`mux 打开 ${endpoint} 失败：连接已断开`),
      );
    }
  }

  private sendRaw(message: Record<string, unknown>): boolean {
    if (this.ws?.readyState !== WebSocket.OPEN) return false;
    try {
      this.ws.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }

  private dispatch(message: unknown): void {
    this.emit("frame", message);
    if (message === null || typeof message !== "object") return;
    const row = message as Record<string, unknown>;
    const streamId = row.streamId;
    if (typeof streamId !== "string") return;
    const sub = this.subscriptions.get(streamId);
    if (sub === undefined) return;
    // 处理函数抛错绝不能击穿 WS 消息循环（否则连接被错误终止 → 重连风暴）。
    try {
      switch (row.type) {
        case "item":
          sub.handlers.onItem?.(row.value);
          break;
        case "end":
          sub.handlers.onEnd?.();
          this.subscriptions.delete(streamId);
          break;
        case "error": {
          const err = row.error;
          const object =
            err !== null && typeof err === "object"
              ? (err as { code?: unknown; message?: unknown })
              : undefined;
          sub.handlers.onError?.({
            code:
              typeof object?.code === "string" ? object.code : undefined,
            message:
              typeof object?.message === "string"
                ? object.message
                : `stream error (${row.type})`,
          });
          break;
        }
        default:
          break;
      }
    } catch (error) {
      this.emit(
        "streamError",
        error instanceof Error
          ? error
          : new Error(`stream handler threw: ${String(error)}`),
      );
    }
  }
}

// ---- HTTP RPC client ----

export interface WireClientOptions {
  cookie?: string;
  validator?: EnvelopeValidator;
  /** Reuse an existing mux; otherwise one is created and owned here. */
  mux?: RemoteMux;
}

/** HTTP RPC client for one dsh web base URL plus its multiplexed stream mux. */
export class WireClient {
  private readonly baseUrl: string;
  private readonly cookie?: string;
  private readonly validator?: EnvelopeValidator;
  private readonly mux: RemoteMux;
  private readonly ownsMux: boolean;
  private eventsClientId: string | null = null;

  constructor(baseUrl: string, options: WireClientOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/u, "");
    this.cookie = options.cookie;
    this.validator = options.validator;
    this.mux =
      options.mux ?? new RemoteMux({ baseUrl: this.baseUrl, cookie: this.cookie });
    this.ownsMux = options.mux === undefined;
  }

  get muxInstance(): RemoteMux {
    return this.mux;
  }

  /** The base URL this client talks to. */
  get url(): string {
    return this.baseUrl;
  }

  /**
   * The `$events` stream identity for this connection (minted by the first
   * `ready` item). Every `$events/result` answer requires it.
   */
  get eventsClientIdValue(): string | null {
    return this.eventsClientId;
  }

  setEventsClientId(clientId: string): void {
    this.eventsClientId = clientId;
  }

  /**
   * POST /api/<endpoint>. `args` is the typert arguments object
   * ({request:{...}} / {_request:{...}} / {agentId,...} / {}) — wrapped here.
   * @throws DshRpcError on business failure, Error on transport/parse failure.
   */
  async call<T>(
    endpoint: string,
    args: Record<string, unknown> = {},
    signal?: AbortSignal,
  ): Promise<T> {
    const message = await this.post(endpoint, args, signal);
    const result = message.result;
    if (result.ok === false) throw new DshRpcError(result.error);
    return result.value as T;
  }

  /**
   * Settle one forwarded waterfall event via $events/result.
   * `outcome` is {kind:"next"} | {kind:"result", value} | {kind:"rejected", error}.
   */
  async answerEvent(
    clientId: string,
    eventId: string,
    outcome: unknown,
    signal?: AbortSignal,
  ): Promise<RpcReceipt> {
    try {
      const message = await this.post(
        "$events/result",
        { clientId, eventId, outcome },
        signal,
      );
      if (message.result.ok === false) {
        return { accepted: false, reason: message.result.error.message };
      }
      return { accepted: true };
    } catch (error) {
      return {
        accepted: false,
        reason: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /** Open one logical stream over the shared mux. */
  openStream(
    endpoint: string,
    args: Record<string, unknown>,
    handlers: StreamHandlers,
  ): () => void {
    return this.mux.openStream(endpoint, args, handlers);
  }

  /**
   * workspace/follow is the 0.1.2 replacement for the removed workspace.list:
   * open the stream, take the first baseline frame, and close it again.
   */
  async workspaceList(): Promise<{
    items: unknown[];
    archivedSessionIds: string[];
  }> {
    return await new Promise((resolve, reject) => {
      let disposed = false;
      let timer: NodeJS.Timeout | undefined;
      const finish = (ok: { items: unknown[]; archivedSessionIds: string[] } | null, error?: Error): void => {
        if (disposed) return;
        disposed = true;
        if (timer !== undefined) clearTimeout(timer);
        dispose();
        if (ok !== null) resolve(ok);
        else reject(error ?? new Error("workspace/follow 未返回 baseline"));
      };
      const dispose = this.mux.openStream("workspace/follow", {}, {
        onItem: (value) => {
          const row = value as { type?: string; value?: unknown } | undefined;
          if (row?.type !== "baseline") return;
          const baseline = row.value as {
            items?: unknown[];
            archivedSessionIds?: string[];
          };
          finish({
            items: Array.isArray(baseline?.items) ? baseline.items : [],
            archivedSessionIds: Array.isArray(baseline?.archivedSessionIds)
              ? baseline.archivedSessionIds
              : [],
          });
        },
        onError: (error) =>
          finish(null, new Error(`workspace/follow 失败: ${error.message}`)),
        onEnd: () =>
          finish(null, new Error("workspace/follow 流在 baseline 前结束")),
      });
      timer = setTimeout(
        () =>
          finish(
            null,
            new Error(
              "workspace/follow baseline 超时（mux 可能仍在重连）",
            ),
          ),
        30_000,
      );
    });
  }

  close(): void {
    if (this.ownsMux) this.mux.close();
  }

  private async post(
    endpoint: string,
    args: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<ServerResponse> {
    const rpcId = randomUUID();
    const body: ClientRequest = {
      type: "client-request",
      rpcId,
      method: endpoint,
      payload: { args },
    };
    let response: Response;
    try {
      response = await fetch(`${this.baseUrl}/api/${endpoint}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(this.cookie ? { cookie: this.cookie } : {}),
        },
        body: JSON.stringify(body),
        signal,
      });
    } catch (error) {
      throw new Error(
        `dsh web RPC ${endpoint} 传输失败: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    if (!response.ok) {
      throw new Error(
        `dsh web RPC ${endpoint} 载体错误: HTTP ${response.status}`,
      );
    }
    let parsed: unknown;
    try {
      parsed = await response.json();
    } catch {
      throw new Error(`dsh web RPC ${endpoint} 响应不是 JSON`);
    }
    if (this.validator && !this.validator.validateServerResponse(parsed)) {
      throw new Error(`dsh web RPC ${endpoint} 响应未通过运行时 schema 校验`);
    }
    const message = parsed as ServerResponse;
    if (message.type !== "server-response" || message.rpcId !== rpcId) {
      throw new Error(`dsh web RPC ${endpoint} 响应帧异常`);
    }
    return message;
  }
}

/** The forwarded-event stream endpoint (host-wide events + waterfalls). */
export const EVENT_STREAM_ENDPOINT = "$events";

/** The host-wide live state stream (queues, jobs, projections). */
export const CONTROL_STREAM_ENDPOINT = "session/control";
