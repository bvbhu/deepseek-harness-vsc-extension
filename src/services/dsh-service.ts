/**
 * One window-local DSH connection interface. Managed children live in the
 * cross-window Runtime Broker; external/discovered instances are never killed.
 *
 * 0.1.2 ("typert") transport: one HTTP RPC client + one multiplexed WebSocket
 * (/api/remote.mux) carrying `$events` (host events + approval/question
 * waterfalls), `workspace/follow` (archive/workspace membership), and
 * `session/control` (live projections). The new event surface is translated
 * back into the legacy `host/*` and `mux` frame shapes so extension.ts and the
 * per-feature services keep working unchanged.
 */

import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import {
  acquireAuthFromUrl,
  extractLaunchToken,
} from "../dsh/auth.ts";
import { discoverDsh, type DshLauncher } from "../dsh/discovery.ts";
import {
  isTcpPortOccupied,
  loopbackDshUrl,
  normalizeDshBaseUrl,
  probeDsh,
} from "../dsh/probe.ts";
import {
  acquireRuntimeBroker,
  runtimeBrokerPaths,
  tryAcquireRuntimeBroker,
  type RuntimeBrokerLease,
} from "../dsh/runtime-broker-client.ts";
import { createWireValidator } from "../dsh/schemas.ts";
import {
  WireClient,
  type EnvelopeValidator,
  type ServerRequest,
} from "../dsh/wire.ts";

export type DshStatus =
  | "discovering"
  | "starting"
  | "ready"
  | "reconnecting"
  | "stopped"
  | "error";
export type DshOwnership =
  | "external-specified"
  | "external-discovered"
  | "external-managed-port"
  | "managed";

export interface DshServiceOptions {
  explicitPath?: string | null;
  externalUrl?: string | null;
  discoveryPort: number;
  managedPort: number;
  globalStoragePath: string;
  brokerScript: string;
  onStatus?: (status: DshStatus, detail?: string) => void;
  onLog?: (line: string) => void;
  /**
   * 0.1.2+: managed 端口被一个需要认证的 dsh 占用（非本插件启动，或前一次
   * Broker 崩溃后遗留）时，向用户索取该 dsh 打印的完整 ?token= URL；
   * 返回 null = 用户取消。返回的 URL 用于换 cookie 后作外部实例连接。
   */
  onRequestExternalToken?: (info: {
    port: number;
    reason: string;
  }) => Promise<string | null>;
  /**
   * 跨扩展宿主重启（切换工作区等）复用已换取的会话 cookie。launch token 是一次性
   * 的，但 cookie 在其 dsh 进程存活期间有效——缓存后重启无需重新输入 token。
   * key = 目标 baseUrl；缺席 = 不缓存。
   */
  loadCachedCookie?: (baseUrl: string) => Promise<string | null>;
  saveCachedCookie?: (baseUrl: string, cookie: string) => Promise<void>;
  clearCachedCookie?: (baseUrl: string) => Promise<void>;
  /**
   * launch token 的跨重启缓存（与 cookie 一起存）。浏览器打开（openInBrowser）
   * 需要 ?token= 完成登录；token 在其 dsh 进程存活期间有效。
   */
  loadCachedToken?: (baseUrl: string) => Promise<string | null>;
  saveCachedToken?: (baseUrl: string, token: string) => Promise<void>;
  clearCachedToken?: (baseUrl: string) => Promise<void>;
  /**
   * "用户在该端口取消了认证输入（Esc）"的持久记忆：置位后后续激活不再弹框，
   * 直接退避为自托管启动。端口不再出现需认证实例时自动清除。
   */
  loadAuthDeclined?: (baseUrl: string) => Promise<boolean>;
  setAuthDeclined?: (baseUrl: string, declined: boolean) => Promise<void>;
  /** 需要用户显式知晓的运行事件（如退避启动完成）；实现应弹非模态提示。 */
  onNotice?: (text: string) => void;
}

interface ResolvedTarget {
  baseUrl: string;
  ownership: DshOwnership;
  reportedVersion: string;
  /** 0.1.2+ 的会话 cookie（launch token 换取）；旧版缺席。 */
  cookie?: string;
  /** 0.1.2+ 的 launch token（进程存活期间可复用；浏览器打开需 ?token=）。 */
  token?: string;
}

/** Consecutive physical WS drops before escalating to a full service restart. */
const RECONNECT_BEFORE_RESTART = 3;
/** Backoff between service restart attempts after a failed restart. */
const RESTART_BACKOFF_MS = 5_000;
/** Max automatic restart attempts before surfacing a terminal error. */
const RESTART_MAX_ATTEMPTS = 10;
/** How long to wait for the $events `ready` handshake before failing. */
const READY_TIMEOUT_MS = 10_000;

export class DshService extends EventEmitter {
  private readonly options: DshServiceOptions;
  private launcher: DshLauncher | null = null;
  private brokerLease: RuntimeBrokerLease | null = null;
  private wire: WireClient | null = null;
  private validator: EnvelopeValidator | null = null;
  private currentBaseUrl: string | null = null;
  private currentCookie: string | undefined;
  /** 0.1.2+ launch token（进程存活期间可复用；"在浏览器打开"用 ?token= 登录）。 */
  private currentToken: string | undefined;
  private ownership: DshOwnership | null = null;
  private reportedVersion: string | null = null;
  private started = false;
  private stopping = false;
  private status: DshStatus = "stopped";
  private generation = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectFailures = 0;
  private restarting = false;
  private restartAttempts = 0;
  private explicitPath: string | null | undefined;
  private streamDisposers: Array<() => void> = [];
  /** Waterfall eventId → kind, so a `cancel` frame resolves the right entry. */
  private waterfallKinds = new Map<string, "approval" | "question">();

  constructor(options: DshServiceOptions) {
    super();
    this.options = options;
    this.explicitPath = options.explicitPath;
  }

  /**
   * 尝试用缓存的 cookie 探测目标；命中返回 {cookie, token?}，未命中/失效返回
   * null 并清除缓存。跨扩展宿主重启（切换工作区）后复用，避免重新输入一次性
   * launch token。token 一并缓存，供"在浏览器打开"登录使用。
   */
  private async tryCachedCookie(
    baseUrl: string,
  ): Promise<{ cookie: string; token?: string } | null> {
    const loader = this.options.loadCachedCookie;
    if (!loader) return null;
    const cached = await loader(baseUrl).catch((error: unknown) => {
      this.options.onLog?.(
        `读取缓存认证 cookie 失败: ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    });
    if (!cached) {
      this.options.onLog?.(
        `端口 ${baseUrl} 无缓存的认证 cookie（或用户此前未完成过认证）`,
      );
      return null;
    }
    this.options.onLog?.(
      `端口 ${baseUrl} 发现缓存的认证 cookie，正在静默验证…`,
    );
    const result = await probeDsh(baseUrl, 10_000, cached);
    if (result.kind === "dsh") {
      const tokenLoader = this.options.loadCachedToken;
      const cachedToken = tokenLoader
        ? await tokenLoader(baseUrl).catch(() => null)
        : null;
      this.options.onLog?.("缓存的认证 cookie 有效，已静默重连（无需输入 token）");
      return {
        cookie: cached,
        ...(cachedToken ? { token: cachedToken } : {}),
      };
    }
    // 缓存失效（dsh 重启 / cookie 过期）：清除，后续走重新认证。
    this.options.onLog?.(
      `缓存的认证 cookie 已失效 (${result.kind})，已清除；将重新认证`,
    );
    const clearer = this.options.clearCachedCookie;
    if (clearer) await clearer(baseUrl).catch(() => undefined);
    const tokenClearer = this.options.clearCachedToken;
    if (tokenClearer) await tokenClearer(baseUrl).catch(() => undefined);
    return null;
  }

  /** 缓存一次认证换取的 cookie（+可选 launch token），下次激活复用。 */
  private async saveCookie(
    baseUrl: string,
    cookie: string,
    token?: string,
  ): Promise<void> {
    const saver = this.options.saveCachedCookie;
    if (saver) await saver(baseUrl, cookie).catch(() => undefined);
    if (token) {
      const tokenSaver = this.options.saveCachedToken;
      if (tokenSaver)
        await tokenSaver(baseUrl, token).catch(() => undefined);
    }
  }

  get statusValue(): DshStatus {
    return this.status;
  }
  get baseUrl(): string | null {
    return this.currentBaseUrl;
  }
  /**
   * 浏览器可直接登录的完整 URL：baseUrl + ?token=...（launch token 在进程
   * 存活期间可复用）。无 token 时退回裸 baseUrl（旧版 dsh 无需认证）。
   */
  get browserUrl(): string | null {
    if (!this.currentBaseUrl) return null;
    if (!this.currentToken) return this.currentBaseUrl;
    const url = new URL(this.currentBaseUrl);
    url.search = "";
    url.searchParams.set("token", this.currentToken);
    return url.toString();
  }
  get client(): WireClient | null {
    return this.wire;
  }
  get launcherValue(): DshLauncher | null {
    return this.launcher;
  }
  get ownershipValue(): DshOwnership | null {
    return this.ownership;
  }
  get reportedVersionValue(): string | null {
    return this.reportedVersion;
  }

  async restart(explicitPath?: string | null): Promise<void> {
    await this.stop();
    if (explicitPath !== undefined) this.explicitPath = explicitPath;
    await this.start();
  }

  private setStatus(status: DshStatus, detail?: string): void {
    this.status = status;
    this.options.onStatus?.(status, detail);
    this.emit("status", status, detail);
  }

  /** Resolve one target, connect the mux, and await the $events handshake. */
  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    this.stopping = false;
    this.launcher = null;
    try {
      this.setStatus("discovering");
      const target = await this.resolveTarget();
      // resolveTarget 可能耗时（认证提示弹窗）；期间 stop() 会置位 stopping
      // 并清空状态——此时不应继续设 "starting"/"ready"。
      if (this.stopping) return;
      this.currentBaseUrl = target.baseUrl;
      this.currentCookie = target.cookie;
      this.currentToken = target.token;
      this.ownership = target.ownership;
      this.reportedVersion = target.reportedVersion || null;
      this.options.onLog?.(
        `dsh endpoint: ${target.baseUrl}; ownership=${target.ownership}; version=${target.reportedVersion || "unknown"}`,
      );

      this.setStatus("starting");
      this.validator =
        target.ownership === "managed" && this.launcher
          ? await createWireValidator()
          : null;
      await this.openGeneration();
      this.setStatus("ready");
    } catch (error) {
      this.started = false;
      await this.releaseTarget();
      const message = error instanceof Error ? error.message : String(error);
      this.setStatus("error", message);
      throw error;
    }
  }

  /** Disconnect this window; only the Broker may stop a managed child. */
  async stop(): Promise<void> {
    this.stopping = true;
    this.started = false;
    this.generation += 1;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.closeTransport();
    await this.releaseTarget();
    this.currentBaseUrl = null;
    this.currentCookie = undefined;
    this.currentToken = undefined;
    this.ownership = null;
    this.reportedVersion = null;
    this.validator = null;
    this.setStatus("stopped");
  }

  private async resolveTarget(): Promise<ResolvedTarget> {
    const external = this.options.externalUrl?.trim();
    if (external) {
      const baseUrl = normalizeDshBaseUrl(external);
      // 0.1.2+ 的 externalUrl 可携带一次性 token；跨重启后 token 已消费，
      // 优先复用缓存的 cookie（避免重新获取 token）。
      const cached = await this.tryCachedCookie(baseUrl);
      if (cached) {
        return {
          baseUrl,
          ownership: "external-specified",
          reportedVersion: "",
          cookie: cached.cookie,
          ...(cached.token === undefined ? {} : { token: cached.token }),
        };
      }
      const auth = await acquireAuthFromUrl(external);
      const result = await probeDsh(baseUrl, 10_000, auth?.cookie);
      if (result.kind === "auth-required") {
        throw new Error(
          `指定地址是 dsh（0.1.2+），但缺少认证：请粘贴 dsh web 启动时打印的带 ?token= 的完整 URL`,
        );
      }
      if (result.kind !== "dsh")
        throw new Error(`指定地址不是可用的 DSH：${result.reason}`);
      const token = extractLaunchToken(external) ?? undefined;
      if (auth) await this.saveCookie(baseUrl, auth.cookie, token);
      return {
        baseUrl,
        ownership: "external-specified",
        reportedVersion: "",
        ...(auth ? { cookie: auth.cookie } : {}),
        ...(token === undefined ? {} : { token }),
      };
    }

    const discoveryUrl = loopbackDshUrl(this.options.discoveryPort);
    const discovered = await probeDsh(discoveryUrl);
    if (discovered.kind === "dsh") {
      return {
        baseUrl: discovered.baseUrl,
        ownership: "external-discovered",
        reportedVersion: "",
      };
    }
    this.options.onLog?.(
      discovered.kind === "auth-required"
        ? `端口 ${discoveryUrl} 上有 dsh 但需要认证：${discovered.reason}`
        : `默认端口未发现可用 DSH (${discoveryUrl}): ${discovered.reason}`,
    );

    const managedUrl = loopbackDshUrl(this.options.managedPort);
    const managed = await probeDsh(managedUrl);
    // 端口不再出现"需要认证的外部 dsh"时，自然清除 Esc 跳过记忆：
    // 用户关闭自己的 dsh 后，后续新的认证需求会重新提示，不留死锁。
    if (managed.kind !== "auth-required") {
      await this.forgetAuthDeclined(managedUrl);
    }
    const paths = runtimeBrokerPaths(this.options.globalStoragePath);
    if (managed.kind === "dsh") {
      this.brokerLease = await tryAcquireRuntimeBroker({
        paths,
        port: this.options.managedPort,
      });
      if (this.brokerLease) {
        return {
          baseUrl: this.brokerLease.baseUrl,
          ownership: this.brokerLease.managed
            ? "managed"
            : "external-managed-port",
          reportedVersion: this.brokerLease.reportedVersion,
          ...(this.brokerLease.cookie
            ? { cookie: this.brokerLease.cookie }
            : {}),
          ...(this.brokerLease.token
            ? { token: this.brokerLease.token }
            : {}),
        };
      }
      return {
        baseUrl: managed.baseUrl,
        ownership: "external-managed-port",
        reportedVersion: "",
      };
    }
    // A Broker may already own the port while DSH is still booting.
    this.brokerLease = await tryAcquireRuntimeBroker({
      paths,
      port: this.options.managedPort,
    });
    if (this.brokerLease) {
      return {
        baseUrl: this.brokerLease.baseUrl,
        ownership: this.brokerLease.managed ? "managed" : "external-managed-port",
        reportedVersion: this.brokerLease.reportedVersion,
        ...(this.brokerLease.cookie ? { cookie: this.brokerLease.cookie } : {}),
        ...(this.brokerLease.token ? { token: this.brokerLease.token } : {}),
      };
    }
    // Broker 不可达：按占用者分类（0.1.2 dsh 需认证 → 索取 token；非 dsh → 退避）。
    if (managed.kind === "auth-required") {
      const attached = await this.attachToAuthRequiredDsh(managed.reason);
      if (attached) return attached;
      // 用户取消（Esc）或无提示渠道：退避该外部实例，回退为自托管 dsh。
      // 临时端口（--port 0，OS 挑空闲端口）避开被占用的管理端口；Broker
      // 跨窗口存活，后续工作区切换直接经租约复用该实例，不再弹认证框。
      this.options.onLog?.(
        "已跳过外部 dsh 的认证接入，退避为启动扩展自托管 dsh（临时端口）",
      );
      return await this.spawnManagedOnPort(0);
    }
    if (await isTcpPortOccupied("127.0.0.1", this.options.managedPort)) {
      throw new Error(
        `DSH 端口 ${String(this.options.managedPort)} 被非 dsh 程序占用（已退避）：请在设置中更换 managedPort 或释放该端口`,
      );
    }
    return await this.spawnManagedOnPort(this.options.managedPort);
  }

  /** 发现 dsh 可执行文件并经 Broker 在指定端口拉起自托管实例（port 0 = 临时端口）。 */
  private async spawnManagedOnPort(port: number): Promise<ResolvedTarget> {
    this.setStatus(
      "discovering",
      port === 0
        ? "外部 dsh 认证已跳过，正在启动自托管 dsh（临时端口）"
        : "正在查找 dsh 可执行文件",
    );
    this.launcher = await discoverDsh({ explicitPath: this.explicitPath });
    this.options.onLog?.(
      `dsh package: ${this.launcher.version ?? "unknown"} @ ${this.launcher.command} (${this.launcher.source})`,
    );
    const paths = runtimeBrokerPaths(this.options.globalStoragePath);
    this.brokerLease = await acquireRuntimeBroker({
      paths,
      brokerScript: this.options.brokerScript,
      port,
      launcher: this.launcher,
      globalStoragePath: this.options.globalStoragePath,
    });
    if (port === 0) {
      // 用户可见的回退反馈：Esc 跳过认证后已改用自托管实例。
      this.options.onNotice?.(
        `已跳过外部 dsh 的认证，退避为自托管 dsh：${this.brokerLease.baseUrl}（命令面板可运行「dsh: 重新提示外部 dsh 认证」恢复接入）`,
      );
    }
    return {
      baseUrl: this.brokerLease.baseUrl,
      ownership: this.brokerLease.managed ? "managed" : "external-managed-port",
      reportedVersion: this.brokerLease.reportedVersion,
      ...(this.brokerLease.cookie ? { cookie: this.brokerLease.cookie } : {}),
      ...(this.brokerLease.token ? { token: this.brokerLease.token } : {}),
    };
  }

  /**
   * 0.1.2: managed 端口被一个需要认证的 dsh 占用（Broker 不可达 = 非本插件
   * 启动，或前一次 Broker 崩溃后遗留）。无法凭空获得其 launch token，向用户
   * 索取该 dsh 打印的完整 ?token= URL，换 cookie 后作为外部实例连接（不持有
   * Broker 租约，断开时不会停止它）。
   *
   * 跨扩展宿主重启（切换工作区）后优先复用缓存的 cookie，避免重新输入 token。
   *
   * @returns 认证并验证成功的目标；用户取消（Esc）或无提示渠道时返回 null，
   * 由调用方退避为自托管启动（临时端口），不再反复打扰。
   * @throws 用户提供了 token 但认证/连接失败（应让用户看到失败原因并重试）。
   */
  private async attachToAuthRequiredDsh(
    reason: string,
  ): Promise<ResolvedTarget | null> {
    const port = this.options.managedPort;
    const baseUrl = loopbackDshUrl(port);
    // 复用缓存的 cookie（跨重启/工作区切换后免重新输入 token）。
    const cached = await this.tryCachedCookie(baseUrl);
    if (cached) {
      this.options.onLog?.(
        `dsh 端口 ${String(port)} 使用缓存的认证 cookie 连接`,
      );
      return {
        baseUrl,
        ownership: "external-specified",
        reportedVersion: "",
        cookie: cached.cookie,
        ...(cached.token === undefined ? {} : { token: cached.token }),
      };
    }
    if (!this.options.onRequestExternalToken) {
      this.options.onLog?.("未配置认证提示渠道，跳过外部 dsh 接入");
      return null;
    }
    // 用户此前已按 Esc 跳过：不再弹框，直接退避为自托管启动。
    const declined = this.options.loadAuthDeclined
      ? await this.options.loadAuthDeclined(baseUrl).catch(() => false)
      : false;
    if (declined) {
      this.options.onLog?.(
        "用户此前已取消该 dsh 的认证输入（Esc），本次直接退避为自托管启动",
      );
      return null;
    }
    const tokenUrl = await this.options.onRequestExternalToken({
      port,
      reason,
    });
    if (!tokenUrl) {
      // Esc：记住"跳过"（跨重启生效，避免切换工作区反复弹框），
      // 不在此时报错——调用方会退避为自托管启动。
      this.options.onLog?.(
        "用户取消了认证输入（Esc），已记住跳过；本次退避为自托管启动",
      );
      const setter = this.options.setAuthDeclined;
      if (setter) await setter(baseUrl, true).catch(() => undefined);
      return null;
    }
    const tokenBaseUrl = normalizeDshBaseUrl(tokenUrl);
    const auth = await acquireAuthFromUrl(tokenUrl);
    if (!auth) {
      throw new Error("提供的 URL 不含 ?token=，无法完成 dsh 认证");
    }
    const result = await probeDsh(tokenBaseUrl, 10_000, auth.cookie);
    if (result.kind !== "dsh") {
      throw new Error(`无法连接到该 dsh：${result.reason}`);
    }
    // 缓存新 cookie + launch token + 清除跳过记忆（下次激活复用，
    // 浏览器打开用 token 登录）。
    const token = extractLaunchToken(tokenUrl) ?? undefined;
    await this.saveCookie(baseUrl, auth.cookie, token);
    await this.forgetAuthDeclined(baseUrl);
    return {
      baseUrl: result.baseUrl,
      ownership: "external-specified",
      reportedVersion: "",
      cookie: auth.cookie,
      ...(token === undefined ? {} : { token }),
    };
  }

  /** 清除"用户跳过认证"记忆（重置命令用）：下次遇到认证需求重新提示。 */
  async clearAuthDecline(): Promise<void> {
    await this.forgetAuthDeclined(loopbackDshUrl(this.options.managedPort));
  }

  private async forgetAuthDeclined(baseUrl: string): Promise<void> {
    const setter = this.options.setAuthDeclined;
    if (!setter) return;
    await setter(baseUrl, false).catch(() => undefined);
  }

  private async openGeneration(): Promise<void> {
    const baseUrl = this.currentBaseUrl;
    if (!baseUrl) throw new Error("DSH 连接目标尚未解析");
    const generation = ++this.generation;
    const wire = new WireClient(baseUrl, {
      cookie: this.currentCookie,
      validator: this.validator ?? undefined,
    });
    const mux = wire.muxInstance;
    this.wire = wire;
    this.streamDisposers = [];
    this.waterfallKinds.clear();

    let ready = false;
    let readyResolve: (() => void) | null = null;
    const readyPromise = new Promise<void>((resolve) => {
      readyResolve = resolve;
    });

    mux.on("open", () => {
      this.reconnectFailures = 0;
      if (generation !== this.generation || this.stopping) return;
      // 首次握手的 ready 由 start() 在 openGeneration 返回后设置；这里仅在
      // 重连（ready 已为 true）时恢复 ready，避免握手未完成就触发工作区绑定。
      if (ready) this.setStatus("ready");
    });
    mux.on("close", (code, reason) => {
      if (generation !== this.generation || this.stopping) return;
      if (!ready) return; // handshake未完成：由 openGeneration 的失败路径处理
      this.options.onLog?.(
        `[mux] 连接断开 code=${String(code)} reason=${String(reason)}`,
      );
      this.emit("muxClose");
      this.setStatus("reconnecting");
      this.onPhysicalDrop();
    });

    // ---- $events：host 事件 + waterfall + 握手 ready 帧 ----
    this.streamDisposers.push(
      mux.openStream("$events", {}, {
        onItem: (value) => {
          const row = value as Record<string, unknown>;
          if (row.type === "ready") {
            const clientId =
              typeof row.clientId === "string" ? row.clientId : "";
            wire.setEventsClientId(clientId);
            if (!ready) {
              ready = true;
              readyResolve?.();
            }
            return;
          }
          this.onEventsItem(generation, row);
        },
        onError: (error) =>
          this.options.onLog?.(`[$events] ${error.message}`),
      }),
    );

    // ---- workspace/follow：归档集合 + 工作区成员变化 ----
    this.streamDisposers.push(
      mux.openStream("workspace/follow", {}, {
        onItem: (value) => this.onWorkspaceItem(value),
        onError: (error) =>
          this.options.onLog?.(`[workspace/follow] ${error.message}`),
      }),
    );

    // ---- session/control：实时投影（todos 等） ----
    this.streamDisposers.push(
      mux.openStream("session/control", {}, {
        onItem: (value) => this.onControlItem(value),
        onError: (error) =>
          this.options.onLog?.(`[session/control] ${error.message}`),
      }),
    );

    mux.connect();

    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      // 超时后仍需 resolve readyPromise，否则 await 永不返回、失败路径
      // (if timedOut && !ready) 永远不可达——WS 握手期间连接失败即死锁。
      readyResolve?.();
    }, READY_TIMEOUT_MS);
    try {
      await readyPromise;
    } finally {
      clearTimeout(timer);
    }
    if (timedOut && !ready) {
      if (generation === this.generation) {
        this.generation += 1;
        this.closeTransport();
      }
      throw new Error("DSH WebSocket 握手失败：等待 $events ready 超时");
    }
  }

  private onPhysicalDrop(): void {
    this.reconnectFailures += 1;
    if (this.reconnectFailures >= RECONNECT_BEFORE_RESTART) {
      this.reconnectFailures = 0;
      void this.restartService();
    }
  }

  /** Translate one $events item (emit / waterfall / cancel) into legacy frames. */
  private onEventsItem(generation: number, row: Record<string, unknown>): void {
    if (generation !== this.generation) return;
    switch (row.type) {
      case "emit": {
        const event = typeof row.event === "string" ? row.event : "";
        const args = Array.isArray(row.args) ? row.args : [];
        this.onEmit(event, args);
        break;
      }
      case "waterfall": {
        const event = typeof row.event === "string" ? row.event : "";
        const eventId = typeof row.eventId === "string" ? row.eventId : "";
        const agentId = typeof row.agentId === "string" ? row.agentId : "";
        const request = (row.request ?? {}) as Record<string, unknown>;
        this.onWaterfall(event, eventId, agentId, request);
        break;
      }
      case "cancel": {
        const eventId = typeof row.eventId === "string" ? row.eventId : "";
        this.onCancel(eventId);
        break;
      }
      default:
        break;
    }
  }

  private onEmit(event: string, args: unknown[]): void {
    const a0 = args[0];
    const a1 = args[1];
    switch (event) {
      case "api-session/status":
        this.emitHost("host/session-status", {});
        break;
      case "api-session/added":
        this.emitHost("host/session-added", {});
        break;
      case "api-session/error":
        if (typeof a0 === "string" && typeof a1 === "string")
          this.emitHost("host/agent-error", { sessionId: a0, message: a1 });
        break;
      case "api-session/removed":
        // 会话删除/归档 → 刷新列表（归档集合由 workspace/follow 提供）。
        this.emitHost("host/workspace-changed", {});
        break;
      case "api-session/activity":
        // 更新 updatedAt → 刷新列表以重排。
        this.emitHost("host/session-status", {});
        break;
      case "commands/change":
      case "agent-preset/selected":
      case "settings/document-updated":
        this.emitRemoteEvent(event, args);
        break;
      default:
        // 其它 allowlist 事件（cordis/*、llm/adapters-updated、credentials/*）：
        // 不驱动 UI，忽略。
        break;
    }
  }

  private onWaterfall(
    event: string,
    eventId: string,
    agentId: string,
    request: Record<string, unknown>,
  ): void {
    if (event === "approval/request") {
      this.waterfallKinds.set(eventId, "approval");
      this.emitMux({
        type: "approval/requested",
        sessionId: agentId,
        approvalId: eventId,
        toolName: typeof request.toolName === "string" ? request.toolName : "",
        ...(typeof request.callId === "string" ? { callId: request.callId } : {}),
        ...(typeof request.reason === "string" ? { reason: request.reason } : {}),
      });
      return;
    }
    if (event === "user-questions/request") {
      this.waterfallKinds.set(eventId, "question");
      const questions = Array.isArray(request.questions)
        ? request.questions
        : [];
      // rpcId 复用 eventId，保证 question 入口 key（q:<eventId>）稳定可答。
      this.emitMux(
        { type: "question/requested", sessionId: agentId, questions },
        eventId,
      );
      return;
    }
  }

  private onCancel(eventId: string): void {
    const kind = this.waterfallKinds.get(eventId);
    this.waterfallKinds.delete(eventId);
    if (kind === "approval") {
      this.emitMux({
        type: "approval/resolved",
        sessionId: "",
        approvalId: eventId,
        outcome: "cancelled",
      });
    } else if (kind === "question") {
      this.emitMux({
        type: "question/resolved",
        sessionId: "",
        questionRpcId: eventId,
        outcome: "cancelled",
      });
    }
  }

  private onWorkspaceItem(value: unknown): void {
    const row = value as {
      type?: string;
      value?: Record<string, unknown>;
      archivedSessionIds?: unknown;
    };
    if (row.type === "baseline") {
      const archived = Array.isArray(row.value?.archivedSessionIds)
        ? (row.value.archivedSessionIds as string[])
        : [];
      this.emitHost("host/archived-sessions-changed", {
        archivedSessionIds: archived,
      });
      return;
    }
    if (row.type === "archived" && Array.isArray(row.archivedSessionIds)) {
      this.emitHost("host/archived-sessions-changed", {
        archivedSessionIds: row.archivedSessionIds as string[],
      });
      return;
    }
    // upsert / remove / order → 刷新列表即可。
    this.emitHost("host/workspace-changed", {});
  }

  private onControlItem(value: unknown): void {
    const row = value as {
      type?: string;
      sessionId?: string;
      key?: string;
      value?: unknown;
      seq?: number;
    };
    if (row.type === "projection") {
      if (typeof row.sessionId === "string" && typeof row.key === "string") {
        this.emitMux({
          type: "session/projection",
          sessionId: row.sessionId,
          key: row.key,
          value: row.value,
          seq: typeof row.seq === "number" ? row.seq : 0,
        });
      }
      return;
    }
    if (row.type === "baseline") {
      const baseline = row.value as
        | {
            projections?: Record<
              string,
              { asOfSeq?: number; values?: Record<string, unknown> }
            >;
          }
        | undefined;
      if (!baseline?.projections) return;
      for (const [sessionId, block] of Object.entries(baseline.projections)) {
        const asOfSeq = block?.asOfSeq ?? 0;
        for (const [key, projectionValue] of Object.entries(
          block?.values ?? {},
        )) {
          this.emitMux({
            type: "session/projection",
            sessionId,
            key,
            value: projectionValue,
            seq: asOfSeq,
          });
        }
      }
    }
  }

  private emitRemoteEvent(event: string, args: unknown[]): void {
    this.emitHost("host/remote-event", {
      type: "host/remote-event",
      event,
      args,
    });
  }

  private emitHost(method: string, payload: unknown): void {
    this.emit("host", this.frame(method, payload));
  }

  private emitMux(payload: unknown, rpcId: string = randomUUID()): void {
    this.emit("mux", this.frame("mux", payload, rpcId));
  }

  private frame(
    method: string,
    payload: unknown,
    rpcId: string = randomUUID(),
  ): ServerRequest {
    return { type: "server-request", rpcId, method, payload };
  }

  /** Re-acquire (or re-spawn) the dsh target after reconnects stop working. */
  private async restartService(): Promise<void> {
    if (this.stopping || this.restarting) return;
    this.restarting = true;
    this.restartAttempts += 1;
    if (this.restartAttempts > RESTART_MAX_ATTEMPTS) {
      this.restarting = false;
      this.restartAttempts = 0;
      this.setStatus(
        "error",
        "dsh 服务多次重启失败，请检查 dsh 可执行文件/端口后手动重试",
      );
      return;
    }
    this.options.onLog?.(
      `dsh 连接持续失败，尝试重启服务以恢复 (第 ${String(this.restartAttempts)} 次)`,
    );
    try {
      await this.restart();
      this.restartAttempts = 0;
    } catch (error) {
      this.options.onLog?.(
        `dsh 服务重启失败: ${error instanceof Error ? error.message : String(error)}`,
      );
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        void this.restartService();
      }, RESTART_BACKOFF_MS);
    } finally {
      this.restarting = false;
    }
  }

  private closeTransport(): void {
    const wire = this.wire;
    this.wire = null;
    for (const dispose of this.streamDisposers.splice(0)) {
      try {
        dispose();
      } catch {
        // ignore
      }
    }
    this.streamDisposers = [];
    this.waterfallKinds.clear();
    wire?.close();
  }

  private async releaseTarget(): Promise<void> {
    const lease = this.brokerLease;
    this.brokerLease = null;
    lease?.dispose();
  }
}

export { extractLaunchToken };
