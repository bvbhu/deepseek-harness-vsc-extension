/** Client half of the cross-window DSH runtime Broker. */

import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, open, readFile, stat, unlink } from "node:fs/promises";
import { connect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DshLauncher } from "./discovery.ts";
import {
  BROKER_PROTOCOL_VERSION,
  type BrokerAcquireRequest,
  type BrokerErrorCode,
  type BrokerReply,
} from "./runtime-broker-protocol.ts";

export interface RuntimeBrokerPaths {
  socket: string;
  launchLock: string;
  metadata: string;
}

export interface RuntimeBrokerLease {
  baseUrl: string;
  pid: number | null;
  managed: boolean;
  reportedVersion: string;
  /** 0.1.2+ 的会话 cookie；旧版 dsh 没有认证时缺席。 */
  cookie?: string;
  /** 0.1.2+ 的 launch token（托管实例就绪 URL 携带；浏览器打开需 ?token=）。 */
  token?: string;
  dispose(): void;
}

class BrokerRejectedError extends Error {
  constructor(
    readonly code: BrokerErrorCode,
    message: string,
  ) {
    super(message);
  }
}

export function runtimeBrokerPaths(
  globalStoragePath: string,
): RuntimeBrokerPaths {
  const id = createHash("sha256")
    .update(globalStoragePath)
    .digest("hex")
    .slice(0, 20);
  return {
    socket:
      process.platform === "win32"
        ? `\\\\.\\pipe\\dsh-vsc-${id}`
        : join(tmpdir(), `dsh-vsc-${id}.sock`),
    launchLock: join(globalStoragePath, "runtime-broker.launch.lock"),
    metadata: join(globalStoragePath, "runtime-broker.json"),
  };
}

/** Attach to an already-running Broker without creating one. */
export async function tryAcquireRuntimeBroker(options: {
  paths: RuntimeBrokerPaths;
  port: number;
  timeoutMs?: number;
}): Promise<RuntimeBrokerLease | null> {
  const deadline = Date.now() + (options.timeoutMs ?? 800);
  do {
    const lease = await connectAndAcquire(
      options.paths.socket,
      options.port,
      undefined,
      500,
      65_000,
    ).catch((error: unknown) => {
      // tryAcquireRuntimeBroker 仅尝试连接已存在的 Broker，不负责处理端口
      // 占用分类（那是 resolveTarget 的职责）。若 Broker 在启动时因端口占用
      // （port-conflict：需认证的 dsh 或非 dsh 程序）拒绝，应返回 null 让
      // resolveTarget 走 attachToAuthRequiredDsh/退避路径，而非抛错中断。
      if (
        error instanceof BrokerRejectedError &&
        error.code !== "launcher-required" &&
        error.code !== "port-conflict"
      )
        throw error;
      return null;
    });
    if (lease) return lease;
    if (Date.now() < deadline) await delay(80);
  } while (Date.now() < deadline);
  return null;
}

/** Start the Broker if necessary, then hold one lifetime lease. */
export async function acquireRuntimeBroker(options: {
  paths: RuntimeBrokerPaths;
  brokerScript: string;
  port: number;
  launcher: DshLauncher;
  globalStoragePath: string;
  timeoutMs?: number;
}): Promise<RuntimeBrokerLease> {
  const timeoutMs = options.timeoutMs ?? 70_000;
  const deadline = Date.now() + timeoutMs;
  await mkdir(options.globalStoragePath, { recursive: true });

  let ownsLaunchLock = false;
  try {
    while (Date.now() < deadline) {
      const connected = await connectAndAcquire(
        options.paths.socket,
        options.port,
        options.launcher,
        1_000,
        Math.max(500, deadline - Date.now()),
      ).catch((error: unknown) => {
        if (error instanceof BrokerRejectedError) {
          // 临时端口请求（port 0）遇上旧版本 Broker（不认识 port 0）会被拒为
          // configuration。旧 Broker 会在最后一个租约清空约 1.5s 后自行退出，
          // 继续重试即可由新 Broker 接管，因此视为可重试而非致命错误。
          if (!(options.port === 0 && error.code === "configuration"))
            throw error;
        }
        return null;
      });
      if (connected) return connected;

      if (!ownsLaunchLock) {
        ownsLaunchLock = await tryTakeLaunchLock(options.paths.launchLock);
        if (ownsLaunchLock) {
          if (process.platform !== "win32")
            await unlink(options.paths.socket).catch(() => undefined);
          const broker = spawn(
            process.execPath,
            [
              options.brokerScript,
              options.paths.socket,
              options.paths.metadata,
            ],
            {
              detached: true,
              stdio: "ignore",
              windowsHide: true,
              env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
            },
          );
          broker.once("error", () => {
            // spawn 失败（权限/路径无效等）不会触发 exit 事件——若不在此释放
            // launch lock，锁会被持有到 deadline 超时，期间其它窗口因看到
            // holder PID 存活而持续空转。
            void releaseLaunchLockIfOurs(
              options.paths.launchLock,
              process.pid,
            ).then((released) => {
              if (released) ownsLaunchLock = false;
            });
          });
          broker.unref();
          // If this broker dies before we acquire a lease (e.g. the previous
          // broker still held the Windows named pipe while it tore down), give
          // our launch lock back so a later iteration can re-spawn instead of
          // spinning to the deadline against a dead broker.
          broker.once("exit", () => {
            void releaseLaunchLockIfOurs(
              options.paths.launchLock,
              process.pid,
            ).then((released) => {
              if (released) ownsLaunchLock = false;
            });
          });
        }
      }
      await delay(100);
    }
    throw new Error(`等待全局 DSH Runtime Broker 超时(${String(timeoutMs)}ms)`);
  } finally {
    if (ownsLaunchLock)
      await unlink(options.paths.launchLock).catch(() => undefined);
  }
}

async function tryTakeLaunchLock(path: string): Promise<boolean> {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const handle = await open(path, "wx");
      await handle.writeFile(
        `${String(process.pid)}\n${new Date().toISOString()}\n`,
      );
      await handle.close();
      return true;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST") throw error;
      // A crashed launcher must not block every future VS Code window forever.
      // Reap the lock when its recorded holder is dead, then retry the open
      // once; otherwise leave it for its live owner.
      if (!(await launchLockIsStale(path))) return false;
      await unlink(path).catch(() => undefined);
    }
  }
  return false;
}

/** True when the lock can be reaped: holder PID dead, or file gone/old-unreadable. */
async function launchLockIsStale(path: string): Promise<boolean> {
  const info = await stat(path).catch(() => null);
  if (!info) return true; // vanished mid-check
  // Hard floor for corrupt/unreadable locks: never block longer than this.
  if (Date.now() - info.mtimeMs > 75_000) return true;
  try {
    const content = await readFile(path, "utf8");
    const pid = Number.parseInt(content.split("\n")[0] ?? "", 10);
    if (Number.isInteger(pid) && pid > 0 && pid !== process.pid) {
      return !isProcessAlive(pid);
    }
  } catch {
    // unreadable: fall through to the mtime-only verdict (false).
  }
  return false;
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    // EPERM: the process exists but we cannot signal it — still alive.
    return code === "EPERM";
  }
}

/** Remove the launch lock only when it still records our own PID. */
async function releaseLaunchLockIfOurs(
  path: string,
  pid: number,
): Promise<boolean> {
  try {
    const content = await readFile(path, "utf8");
    if ((content.split("\n")[0] ?? "").trim() !== String(pid)) return false;
    await unlink(path).catch(() => undefined);
    return true;
  } catch {
    return false; // already gone or unreadable
  }
}

async function connectAndAcquire(
  socketPath: string,
  port: number,
  launcher?: DshLauncher,
  connectTimeoutMs = 1_000,
  replyTimeoutMs = 5_000,
): Promise<RuntimeBrokerLease> {
  const socket = await connectSocket(socketPath, connectTimeoutMs);
  const request: BrokerAcquireRequest = {
    protocol: BROKER_PROTOCOL_VERSION,
    type: "acquire",
    port,
    ...(launcher ? { launcher } : {}),
  };
  socket.write(`${JSON.stringify(request)}\n`);
  let reply: BrokerReply;
  try {
    reply = await readReply(socket, replyTimeoutMs);
  } catch (error) {
    socket.destroy();
    throw error;
  }
  if (reply.protocol !== BROKER_PROTOCOL_VERSION) {
    socket.destroy();
    throw new Error("全局 DSH Runtime Broker 协议版本不匹配");
  }
  if (reply.type === "error") {
    socket.destroy();
    throw new BrokerRejectedError(reply.code, reply.message);
  }
  // The lease is represented by this open socket. A later Broker shutdown is
  // observed by DSH transport reconnect logic; never let an idle socket error
  // become an uncaught EventEmitter exception in the extension host.
  socket.on("error", () => undefined);
  return {
    baseUrl: reply.baseUrl,
    pid: reply.pid,
    managed: reply.managed,
    reportedVersion: reply.reportedVersion,
    cookie: reply.cookie,
    token: reply.token,
    dispose: () => socket.end(),
  };
}

function connectSocket(path: string, timeoutMs: number): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connect(path);
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error("连接全局 DSH Runtime Broker 超时"));
    }, timeoutMs);
    socket.once("connect", () => {
      clearTimeout(timer);
      socket.removeAllListeners("error");
      resolve(socket);
    });
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

function readReply(socket: Socket, timeoutMs: number): Promise<BrokerReply> {
  return new Promise((resolve, reject) => {
    let buffer = "";
    const cleanup = (): void => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("close", onClose);
    };
    const onData = (chunk: Buffer): void => {
      buffer += chunk.toString("utf8");
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      cleanup();
      try {
        resolve(JSON.parse(buffer.slice(0, newline)) as BrokerReply);
      } catch {
        reject(new Error("全局 DSH Runtime Broker 返回了无效响应"));
      }
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const onClose = (): void => {
      cleanup();
      reject(new Error("全局 DSH Runtime Broker 在响应前关闭"));
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("等待全局 DSH Runtime Broker 响应超时"));
    }, timeoutMs);
    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
