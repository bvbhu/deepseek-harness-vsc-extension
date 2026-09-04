#!/usr/bin/env node
/**
 * User-level DSH Runtime Broker. One detached Broker owns the managed DSH
 * child while one or more VS Code extension hosts hold IPC leases.
 */

import { chmod, mkdir, unlink, writeFile } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { dirname } from "node:path";
import type { DshLauncher } from "./discovery.ts";
import { acquireAuth } from "./auth.ts";
import { isTcpPortOccupied, loopbackDshUrl, probeDsh } from "./probe.ts";
import {
  BROKER_PROTOCOL_VERSION,
  type BrokerAcquireRequest,
  type BrokerErrorCode,
  type BrokerMetadata,
  type BrokerReply,
} from "./runtime-broker-protocol.ts";
import { startDshWeb, type StartedDshServer } from "./server.ts";

const socketPath = requiredArgument(process.argv[2]);
const metadataPath = requiredArgument(process.argv[3]);

interface RuntimeState {
  baseUrl: string;
  port: number;
  server: StartedDshServer | null;
  reportedVersion: string;
  /** 0.1.2+ 的会话 cookie（launch token 换取）；旧版 dsh 缺席。 */
  cookie?: string;
}

class RuntimeBrokerError extends Error {
  constructor(
    readonly code: BrokerErrorCode,
    message: string,
  ) {
    super(message);
  }
}

let runtime: RuntimeState | null = null;
let boot: Promise<RuntimeState> | null = null;
let shuttingDown = false;
let shutdownTimer: NodeJS.Timeout | null = null;
let listening = false;
const leases = new Set<Socket>();
const connected = new Set<Socket>();

const ipc = createServer((socket) => {
  connected.add(socket);
  let buffer = "";
  let acquired = false;
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    if (acquired) return;
    buffer += chunk;
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    acquired = true;
    void handleAcquire(socket, buffer.slice(0, newline));
  });
  socket.on("close", () => {
    connected.delete(socket);
    leases.delete(socket);
    scheduleShutdownIfIdle();
  });
  socket.on("error", () => {
    connected.delete(socket);
    leases.delete(socket);
    scheduleShutdownIfIdle();
  });
});

ipc.on("error", (error: Error) => {
  // A previous Broker may still hold the Windows named pipe while it tears
  // down (its dsh stop takes up to ~5s). Retry the bind for a short grace
  // instead of exiting immediately; otherwise the client must re-spawn us and
  // the new process would hit the same conflict.
  const code = (error as NodeJS.ErrnoException).code;
  if (code === "EADDRINUSE" && !shuttingDown) {
    retryListen();
    return;
  }
  // 正在优雅关闭（SIGTERM）期间出现的 EADDRINUSE 属于正常竞态，退出码 0。
  if (code === "EADDRINUSE" && shuttingDown) {
    process.exit(0);
  }
  process.exit(1);
});

void startIpc();

process.on("SIGTERM", () => {
  void shutdown();
});
process.on("SIGINT", () => {
  void shutdown();
});

/** Bind retry budget: ~10s, covering a prior Broker's dsh-stop grace period. */
const LISTEN_RETRY_MS = 250;
const LISTEN_RETRY_MAX = 40;
let listenRetries = 0;

async function startIpc(): Promise<void> {
  if (process.platform !== "win32")
    await unlink(socketPath).catch(() => undefined);
  listen();
}

function listen(): void {
  if (shuttingDown) return;
  ipc.listen(socketPath, () => {
    listening = true;
    if (process.platform !== "win32")
      void chmod(socketPath, 0o600).catch(() => undefined);
  });
}

function retryListen(): void {
  if (shuttingDown) return;
  listenRetries += 1;
  if (listenRetries >= LISTEN_RETRY_MAX) process.exit(1);
  setTimeout(() => {
    if (!listening) listen();
  }, LISTEN_RETRY_MS);
}

function requiredArgument(value: string | undefined): string {
  if (value === undefined || value === "")
    throw new Error("Runtime Broker 缺少启动参数");
  return value;
}

async function handleAcquire(socket: Socket, raw: string): Promise<void> {
  if (shuttingDown) {
    send(socket, {
      protocol: BROKER_PROTOCOL_VERSION,
      type: "error",
      code: "configuration",
      message: "Runtime Broker 正在关闭",
    });
    socket.end();
    return;
  }
  let request: BrokerAcquireRequest;
  try {
    request = JSON.parse(raw) as BrokerAcquireRequest;
    if (
      request.protocol !== BROKER_PROTOCOL_VERSION ||
      request.type !== "acquire" ||
      !Number.isInteger(request.port) ||
      request.port < 0 ||
      request.port > 65_535
    )
      throw new RuntimeBrokerError(
        "configuration",
        "无效的 Broker acquire 请求",
      );
    // port 0 = 临时端口回退启动（管理端口被外部实例占用时）。托管子进程的
    // 端口可漂移（就绪行决定实际端口），因此仅"采用的外部实例"
    // （server === null，固定端口）坚持请求端口与运行态一致。
    if (runtime && runtime.port !== request.port && runtime.server === null) {
      throw new RuntimeBrokerError(
        "configuration",
        `全局 DSH 已固定在端口 ${String(runtime.port)}，不能切换到 ${String(request.port)}`,
      );
    }
    if (shutdownTimer) clearTimeout(shutdownTimer);
    shutdownTimer = null;
    const ready = await ensureRuntime(request.port, request.launcher);
    if (socket.destroyed) {
      scheduleShutdownIfIdle();
      return;
    }
    leases.add(socket);
    send(socket, {
      protocol: BROKER_PROTOCOL_VERSION,
      type: "ready",
      baseUrl: ready.baseUrl,
      pid: ready.server?.child.pid ?? null,
      managed: ready.server !== null,
      reportedVersion: ready.reportedVersion,
      cookie: ready.cookie,
      // 托管实例的 launch token（0.1.2+）：进程存活期间可复用，浏览器打开
      // 需要 ?token= 完成登录；采用的外部实例（server === null）缺席。
      ...(ready.server?.token === undefined
        ? {}
        : { token: ready.server.token }),
    });
  } catch (error) {
    send(socket, {
      protocol: BROKER_PROTOCOL_VERSION,
      type: "error",
      code: error instanceof RuntimeBrokerError ? error.code : "internal",
      message: error instanceof Error ? error.message : String(error),
    });
    socket.end();
  }
}

async function ensureRuntime(
  port: number,
  launcher?: DshLauncher,
): Promise<RuntimeState> {
  if (runtime) return runtime;
  if (boot) return await boot;
  boot = bootRuntime(port, launcher);
  try {
    runtime = await boot;
    return runtime;
  } finally {
    boot = null;
  }
}

async function bootRuntime(
  port: number,
  launcher?: DshLauncher,
): Promise<RuntimeState> {
  if (port === 0) {
    // 临时端口模式：管理端口被未认证 dsh/其它程序占用时的回退启动。
    // 让 OS 挑空闲端口，就绪行携带实际端口（startDshWeb 负责解析），
    // 不存在绑定竞争，无需 probe/占用预检。
    if (!launcher)
      throw new RuntimeBrokerError(
        "launcher-required",
        "启动 DSH 需要可执行文件信息",
      );
    const server = await startDshWeb({ launcher, port: 0 });
    const state = await adoptStartedServer(server, launcher);
    await publishMetadata(state);
    return state;
  }
  const baseUrl = loopbackDshUrl(port);
  const existing = await probeDsh(baseUrl);
  if (existing.kind === "dsh") {
    const state = {
      baseUrl,
      port,
      server: null,
      reportedVersion: existing.description.version || launcher?.version || "",
    };
    await publishMetadata(state);
    return state;
  }
  if (existing.kind === "auth-required") {
    // A live 0.1.2+ dsh on the managed port, but this Broker has no launch
    // token to mint a cookie. We cannot adopt it.
    throw new RuntimeBrokerError(
      "port-conflict",
      `DSH 管理端口 ${String(port)} 已有一个要求认证的实例（0.1.2+），但 Broker 缺少其 launch token；请先关闭它或换用其它端口`,
    );
  }
  if (await isTcpPortOccupied("127.0.0.1", port)) {
    throw new RuntimeBrokerError(
      "port-conflict",
      `DSH 管理端口 ${String(port)} 已被其他程序占用：${existing.reason}`,
    );
  }
  if (!launcher)
    throw new RuntimeBrokerError(
      "launcher-required",
      "启动 DSH 需要可执行文件信息",
    );

  let server: StartedDshServer;
  try {
    server = await startDshWeb({ launcher, port });
  } catch (error) {
    // Another Broker/process may have won the check-to-bind race. Reuse only
    // when the winner completes the full DSH handshake.
    const winner = await probeDsh(baseUrl, 5_000);
    if (winner.kind === "dsh") {
      const state = {
        baseUrl,
        port,
        server: null,
        reportedVersion: winner.description.version || launcher.version || "",
      };
      await publishMetadata(state);
      return state;
    }
    if (await isTcpPortOccupied("127.0.0.1", port)) {
      throw new RuntimeBrokerError(
        "port-conflict",
        `DSH 管理端口 ${String(port)} 启动竞争失败且端口已被占用：${winner.reason}`,
      );
    }
    throw error;
  }

  const state = await adoptStartedServer(server, launcher);
  await publishMetadata(state);
  return state;
}

/** 认证交换 + 握手验证 + 组装运行态（固定端口与临时端口启动共用）。 */
async function adoptStartedServer(
  server: StartedDshServer,
  launcher: DshLauncher,
): Promise<RuntimeState> {
  const baseUrl = server.baseUrl;
  // 0.1.2+ 打印带 ?token= 的 URL：换取会话 cookie 后才有权访问 /api 与 mux。
  let cookie: string | undefined;
  if (server.token) {
    try {
      cookie = (await acquireAuth(baseUrl, server.token)).cookie;
    } catch (error) {
      await server.stop();
      throw new Error(
        `dsh web 已启动但认证交换失败：${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  const verified = await probeDsh(baseUrl, 5_000, cookie);
  if (verified.kind !== "dsh") {
    await server.stop();
    throw new Error(`已启动进程但 DSH 握手失败：${verified.reason}`);
  }
  const state: RuntimeState = {
    baseUrl,
    port: server.port,
    server,
    cookie,
    reportedVersion: launcher.version ?? verified.description.version,
  };
  void server.exited.then(() => {
    if (runtime?.server === server) runtime = null;
  });
  return state;
}

async function publishMetadata(state: RuntimeState): Promise<void> {
  const metadata: BrokerMetadata = {
    protocol: BROKER_PROTOCOL_VERSION,
    brokerPid: process.pid,
    dshPid: state.server?.child.pid ?? null,
    baseUrl: state.baseUrl,
    port: state.port,
    managed: state.server !== null,
    reportedVersion: state.reportedVersion,
    startedAt: new Date().toISOString(),
  };
  await mkdir(dirname(metadataPath), { recursive: true });
  await writeFile(
    metadataPath,
    `${JSON.stringify(metadata, null, 2)}\n`,
    "utf8",
  );
}

function send(socket: Socket, reply: BrokerReply): void {
  socket.write(`${JSON.stringify(reply)}\n`);
}

/**
 * Idle lifetime after the last lease drops. 必须远大于工作区切换时扩展宿主
 * 重启的激活间隙（旧窗口租约断开 → 新窗口重新 acquire，通常几秒）：太短会
 * 在切换间隙自杀并杀掉 dsh，导致新窗口重新走发现/认证流程（外部认证实例
 * 场景下表现为反复弹 token 输入框）。留足余量后，同一个 dsh 子进程可跨
 * 工作区切换甚至整窗重启持续服务；dsh 会话本身持久化，无泄漏风险。
 */
const IDLE_SHUTDOWN_MS = 30_000;

function scheduleShutdownIfIdle(): void {
  if (shuttingDown || leases.size > 0 || shutdownTimer) return;
  shutdownTimer = setTimeout(() => {
    void shutdown();
  }, IDLE_SHUTDOWN_MS);
}

async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  if (shutdownTimer) clearTimeout(shutdownTimer);
  // Refuse new leases first: closing the listener before tearing down dsh
  // means a late acquire can never re-boot a child mid-shutdown (it will
  // instead spawn a fresh Broker). Then drop any open lease sockets.
  if (ipc.listening) ipc.close(() => undefined);
  for (const socket of connected) socket.destroy();
  connected.clear();
  leases.clear();
  const current = runtime;
  runtime = null;
  if (current?.server) await current.server.stop().catch(() => undefined);
  await unlink(metadataPath).catch(() => undefined);
  if (process.platform !== "win32")
    await unlink(socketPath).catch(() => undefined);
  process.exit(0);
}
