/**
 * DSH endpoint recognition (0.1.2 "typert" generation).
 *
 * The old handshake called host.describe over HTTP and opened /api/events.mux +
 * /api/events.host. Both endpoints and the describe RPC were removed in 0.1.2:
 * a usable endpoint now must accept the multiplexed stream WebSocket
 * (/api/remote.mux, cookie-authenticated) and begin the `$events` stream with
 * its `ready` item ({type:"ready", clientId, host:{home}}).
 *
 * dsh web 0.1.2 起所有 /api 请求与 WS upgrade 都要求会话 cookie；没有 cookie
 * 的 GET / 返回 401。因此探测分三档：握手成功 = dsh，仅认证失败 = auth-required，
 * 其它 = not-dsh。`host.describe` 已不存在，版本信息改由 discovery 的
 * `dsh --version` 探测提供，这里的 cwd 只能取 ready 的 host.home。
 */

import { connect } from "node:net";
import { WebSocket } from "ws";
import { randomUUID } from "node:crypto";

export interface DshHostDescription {
  /** 0.1.2 起不再有 describe RPC；通常为未知（由 discovery 的 --version 覆盖）。 */
  version: string;
  /** $events ready 帧的 host.home。 */
  cwd: string;
  provider?: string;
  model?: string;
  attachedSessions: number;
  canOpenPath: boolean;
}

export type DshProbeResult =
  | {
      kind: "dsh";
      baseUrl: string;
      description: DshHostDescription;
      /** $events 流的 clientId（waterfall 应答需要）。 */
      clientId: string;
    }
  | { kind: "auth-required"; baseUrl: string; reason: string }
  | { kind: "not-dsh"; baseUrl: string; reason: string };

// 仅作分类说明（不再含"请粘贴 URL"指令——指令由 UI prompt 提供，避免拼接重复）。
const AUTH_REQUIRED_REASON =
  "该 DSH 实例（0.1.2+）要求会话认证";

export function normalizeDshBaseUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`DSH 地址无效: ${raw}`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error(`DSH 地址只支持 http/https: ${raw}`);
  }
  if (url.username || url.password)
    throw new Error("DSH 地址不能包含用户名或密码");
  // 0.1.2+ 的 ready URL 带 ?token=（launch token，换取会话 cookie 后不再需要）；
  // 剥离 query/hash 取干净根地址，而不是拒绝带 token 的 URL。
  url.search = "";
  url.hash = "";
  if (url.pathname !== "/" && url.pathname !== "") {
    throw new Error("DSH 地址必须指向服务根路径");
  }
  url.pathname = "";
  return url.toString().replace(/\/$/u, "");
}

export function loopbackDshUrl(port: number): string {
  assertPort(port);
  return `http://127.0.0.1:${String(port)}`;
}

export function assertPort(port: number): void {
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`DSH 端口必须是 1 到 65535 的整数，收到: ${String(port)}`);
  }
}

/**
 * 识别一个可连接的 DSH 实例：带 cookie 时完成完整握手（$events ready 帧），
 * 无 cookie 且仅认证失败时报告 auth-required。
 */
export async function probeDsh(
  baseUrlInput: string,
  timeoutMs = 3_000,
  cookie?: string,
): Promise<DshProbeResult> {
  const baseUrl = normalizeDshBaseUrl(baseUrlInput);
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), timeoutMs);
  try {
    const opened = await openEventsReady(baseUrl, cookie, abort.signal);
    return {
      kind: "dsh",
      baseUrl,
      description: hostDescriptionFromReady(opened),
      clientId: opened.clientId,
    };
  } catch (error) {
    if (abort.signal.aborted) {
      return { kind: "not-dsh", baseUrl, reason: "DSH 探测超时" };
    }
    if (await isAuthRequired(baseUrl)) {
      return {
        kind: "auth-required",
        baseUrl,
        reason: AUTH_REQUIRED_REASON,
      };
    }
    return {
      kind: "not-dsh",
      baseUrl,
      reason: error instanceof Error ? error.message : String(error),
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * 打开 /api/remote.mux 并订阅 $events 流，等到 ready 项即认为握手成立。
 * 失败抛错（认证失败时 ws 会在 upgrade 后被拒绝或立即关闭）。
 */
async function openEventsReady(
  baseUrl: string,
  cookie: string | undefined,
  signal: AbortSignal,
): Promise<{ clientId: string; home: string }> {
  const wsUrl =
    baseUrl.replace(/^http:/u, "ws:").replace(/^https:/u, "wss:") +
    "/api/remote.mux";
  const streamId = randomUUID();
  let socket: WebSocket;
  try {
    socket = new WebSocket(wsUrl, {
      headers: cookie ? { cookie } : {},
      followRedirects: false,
    });
  } catch (error) {
    throw new Error(
      `DSH WebSocket 握手失败: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  return await new Promise<{ clientId: string; home: string }>(
    (resolve, reject) => {
      let settled = false;
      const cleanup = (): void => {
        signal.removeEventListener("abort", onAbort);
        socket.removeListener("open", onOpen);
        socket.removeListener("message", onMessage);
        socket.removeListener("error", onError);
        socket.removeListener("close", onClose);
        try {
          socket.terminate();
        } catch {
          // already terminated
        }
      };
      const fail = (message: string): void => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(new Error(message));
      };

      const onOpen = (): void => {
        socket.send(
          JSON.stringify({
            type: "open",
            streamId,
            endpoint: "$events",
            payload: { args: {} },
          }),
        );
      };

      const onMessage = (data: Buffer | string): void => {
        if (settled) return;
        let frame: unknown;
        try {
          frame = JSON.parse(String(data));
        } catch {
          return;
        }
        const row = frame as Record<string, unknown>;
        if (row.streamId !== streamId) return;
        if (row.type === "error") {
          const err = row.error;
          fail(
            `DSH 事件流打开失败: ${
              typeof err === "string" ? err : JSON.stringify(err ?? {})
            }`,
          );
          return;
        }
        if (row.type !== "item") return;
        const value = row.value as Record<string, unknown> | undefined;
        if (value?.type !== "ready") return;
        if (
          typeof value.clientId !== "string" ||
          value.host === null ||
          typeof value.host !== "object" ||
          typeof (value.host as { home?: unknown }).home !== "string"
        ) {
          fail("DSH 事件流 ready 帧结构不符合 DSH 契约");
          return;
        }
        settled = true;
        cleanup();
        resolve({
          clientId: value.clientId,
          home: (value.host as { home: string }).home,
        });
      };

      const onError = (): void => fail("DSH WebSocket 握手失败");
      const onClose = (): void => fail("DSH WebSocket 握手失败");
      const onAbort = (): void => fail("DSH 探测超时");

      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) return onAbort();
      socket.once("open", onOpen);
      socket.on("message", onMessage);
      socket.once("error", onError);
      socket.once("close", onClose);
    },
  );
}

/** GET / 未认证时应返回 401；据此区分"是 DSH 但缺 cookie"与"根本不是 DSH"。 */
async function isAuthRequired(baseUrl: string): Promise<boolean> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 2_000);
  try {
    const response = await fetch(`${baseUrl}/`, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
    });
    if (response.status !== 401) return false;
    const body = await response.text().catch(() => "");
    return /auth|dsh web/iu.test(body) || body.trim().length === 0;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

function hostDescriptionFromReady(opened: {
  clientId: string;
  home: string;
}): DshHostDescription {
  return {
    version: "",
    cwd: opened.home,
    attachedSessions: 0,
    canOpenPath: true,
  };
}

/**
 * 兼容旧调用方：校验历史 host.describe 结构的辅助函数（0.1.2 起不再使用，
 * 保留以兼容测试与旧版本 dsh 的探测路径）。
 */
export function parseDshHostDescription(
  value: unknown,
): DshHostDescription | null {
  if (value === null || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  if (
    typeof row.version !== "string" ||
    typeof row.cwd !== "string" ||
    typeof row.attachedSessions !== "number" ||
    !Number.isInteger(row.attachedSessions) ||
    row.attachedSessions < 0 ||
    typeof row.canOpenPath !== "boolean" ||
    (row.provider !== undefined && typeof row.provider !== "string") ||
    (row.model !== undefined && typeof row.model !== "string")
  )
    return null;
  return {
    version: row.version,
    cwd: row.cwd,
    attachedSessions: row.attachedSessions,
    canOpenPath: row.canOpenPath,
    ...(typeof row.provider === "string" ? { provider: row.provider } : {}),
    ...(typeof row.model === "string" ? { model: row.model } : {}),
  };
}

/** True when something accepts TCP connections at this host/port. */
export async function isTcpPortOccupied(
  host: string,
  port: number,
  timeoutMs = 800,
): Promise<boolean> {
  assertPort(port);
  return await new Promise((resolve) => {
    const socket = connect({ host, port });
    let settled = false;
    const finish = (value: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve(value);
    };
    const timer = setTimeout(() => finish(false), timeoutMs);
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}
