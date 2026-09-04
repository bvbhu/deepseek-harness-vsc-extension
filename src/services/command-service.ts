/**
 * CommandService（M3b/D14）：/ 指令的 wire 客户端 + /model 客户端贡献的数据面。
 *   - `commands/list`（宿主命令目录，契约跟随：探测可用性，缺失时 `/` 当普通文本）；
 *   - `commands/execute`（准入式执行：未知/语法未命中返回 null，已进入 handler 的
 *     生命周期由 command/run + command/done 事件折叠为对话命令节点，composer 不回显）；
 *   - `session.models` / `session.selectModel`（/model 的 provider 分组 → 模型）。
 *
 * 命令面走 dsh 的 typert gateway 通道，endpoint 为**斜杠分隔**的 `commands/list`、
 * `commands/execute`（/api/<namespace>/<method>，payload `{ args: { agentId, ... } }`
 * 单字段包装；对齐 rc.6 实测 + api-gateway invokeRpc）。
 */

import { DshRpcError, type WireClient } from "../dsh/wire.ts";
import type {
  CommandDescriptorView,
  SessionModelsView,
  SessionSummary,
} from "../shared/protocol.ts";

/** commands/list 的原始 wire 行（descriptor）。 */
interface RawCommandDescriptor {
  name: string;
  description: string;
  input?: { hint: string };
}

/** commands/execute 的已结算执行（undefined = 未命中，无生命周期事件）。 */
export interface CommandExecutionResult {
  commandId: string;
  kind: "success" | "error";
  text?: string;
}

interface RawCommandExecution {
  commandId: string;
  result: { kind: "success" | "error"; text?: string };
}

interface RawModelEffort {
  id: string;
  name: string;
  description?: string;
}

interface RawModelReasoning {
  efforts: RawModelEffort[];
  defaultEffort?: string;
}

interface RawModelEntry {
  id: string;
  name: string;
  description?: string;
  reasoning?: RawModelReasoning;
}

interface RawModelGroup {
  id: string;
  name: string;
  models: RawModelEntry[];
}

interface RawModelFailure {
  id: string;
  name: string;
  message: string;
}

/**
 * session/modelCatalog 响应（0.1.2 起替代 per-session 的 session.models）：
 * catalog 是全局的，会话当前选择改由 session/list 的 projections.modelSelection
 * 提供（见 models()）。
 */
interface RawModelCatalog {
  default: { provider: string; model: string; reasoningEffort?: string };
  routableProviders: string[];
  groups: RawModelGroup[];
  failures?: RawModelFailure[];
}

export class CommandService {
  /** 命令面可用性探测结果（契约跟随 §7.2；null = 未探测）。 */
  private available: boolean | null = null;
  /** 每会话命令目录缓存（agent-backed；commands/change 失效重拉）。 */
  private readonly directory = new Map<string, CommandDescriptorView[]>();
  private readonly inFlight = new Map<
    string,
    Promise<CommandDescriptorView[]>
  >();

  /**
   * @param wire - lazy accessor for the live wire client；每次调用现取，
   *   保证 dsh 重启前后服务都可用。
   */
  constructor(private readonly wire: () => WireClient | null) {}

  private requireClient(): WireClient {
    const client = this.wire();
    if (!client) throw new Error("dsh web 尚未就绪");
    return client;
  }

  /**
   * 探测命令面是否可达（契约跟随，结果缓存）：用**真实会话 id** 探测——假 id 会被
   * agent-lookup 在 HTTP 层判 404（rc.6 实测 `commands.list` + 未知 agentId → 404），
   * 误判为"路由缺失"。区分：成功或业务错误（DshRpcError，如 agent 忙）= 路由存在；
   * 载体 404/传输失败 = 低版本 dsh 无此方法 → `/` 菜单不呼出、当普通文本。
   */
  async commandsAvailable(sessionId: string): Promise<boolean> {
    if (this.available !== null) return this.available;
    const client = this.requireClient();
    try {
      await client.call<unknown>("commands/list", { agentId: sessionId });
      this.available = true;
    } catch (error) {
      this.available = error instanceof DshRpcError;
    }
    return this.available;
  }

  /** 一个会话的命令目录（缓存 + 单飞；commands/change 后失效重拉）。 */
  async list(sessionId: string): Promise<CommandDescriptorView[]> {
    const cached = this.directory.get(sessionId);
    if (cached) return cached;
    const flight = this.inFlight.get(sessionId) ?? this.pull(sessionId);
    this.inFlight.set(sessionId, flight);
    try {
      return await flight;
    } finally {
      if (this.inFlight.get(sessionId) === flight)
        this.inFlight.delete(sessionId);
    }
  }

  /** 失效命令目录缓存（commands/change 或会话切换；不传 sessionId = 全清）。 */
  invalidate(sessionId?: string): void {
    if (sessionId) this.directory.delete(sessionId);
    else this.directory.clear();
  }

  /** 执行一条完整 / 命令行（claim 提交或裸命令）；null = 未命中（当普通消息的错误反馈）。 */
  async execute(
    sessionId: string,
    line: string,
  ): Promise<CommandExecutionResult | null> {
    const client = this.requireClient();
    const value = await client.call<RawCommandExecution | undefined>(
      "commands/execute",
      { agentId: sessionId, line, images: [] },
    );
    if (!value) return null;
    return {
      commandId: value.commandId,
      kind: value.result.kind,
      text: value.result.text,
    };
  }

  /** session/modelCatalog 投影（/model 弹出层 + 输入框下方席位共享的目录快照）。 */
  async models(sessionId: string): Promise<SessionModelsView> {
    const client = this.requireClient();
    const raw = await client.call<RawModelCatalog>("session/modelCatalog", {});
    let current:
      | { provider: string; model: string; reasoningEffort?: string }
      | null = {
      provider: raw.default.provider,
      model: raw.default.model,
      ...(raw.default.reasoningEffort === undefined
        ? {}
        : { reasoningEffort: raw.default.reasoningEffort }),
    };
    const result: SessionModelsView = {
      current,
      routable: raw.routableProviders.length > 0 ? true : null,
      groups: raw.groups.map((g) => ({
        id: g.id,
        name: g.name,
        models: g.models.map((m) => ({
          id: m.id,
          name: m.name,
          ...(m.description === undefined
            ? {}
            : { description: m.description }),
          ...(m.reasoning === undefined
            ? {}
            : {
                reasoning: {
                  efforts: m.reasoning.efforts,
                  ...(m.reasoning.defaultEffort === undefined
                    ? {}
                    : { defaultEffort: m.reasoning.defaultEffort }),
                },
              }),
        })),
      })),
      failures: (raw.failures ?? []).map((f) => ({
        id: f.id,
        name: f.name,
        message: f.message,
      })),
      error: null,
    };
    try {
      // 0.1.2 起没有 per-session 的 models RPC；会话当前选择来自
      // session/list 的 projections.modelSelection.lastUsed（投影缺席 → 回落
      // catalog 默认）。
      const { items } = await client.call<{ items: SessionSummary[] }>(
        "session/list",
        { _request: {} },
      );
      const row = items.find(
        (item) => item.sessionId === sessionId,
      ) as
        | {
            projections?: {
              values?: {
                modelSelection?: {
                  lastUsed?: {
                    provider: string;
                    model: string;
                    reasoningEffort?: string;
                  } | null;
                };
              };
            };
          }
        | undefined;
      const lastUsed = row?.projections?.values?.modelSelection?.lastUsed;
      if (lastUsed !== null && lastUsed !== undefined) {
        current = {
          provider: lastUsed.provider,
          model: lastUsed.model,
          ...(lastUsed.reasoningEffort === undefined
            ? {}
            : { reasoningEffort: lastUsed.reasoningEffort }),
        };
        result.current = current;
      }
    } catch (error) {
      // 会话未知（尚未落库）→ 保留 catalog 默认。仅吞业务错误
      // (DshRpcError)；传输故障（网络/超时/服务器不可达）向上传播，
      // 避免静默显示错误模型。
      if (error instanceof DshRpcError) return result;
      throw error;
    }
    return result;
  }

  /** session.selectModel（/model 选中 / 席位切换；effort 缺席 = 回落模型默认）。 */
  async selectModel(
    sessionId: string,
    provider: string,
    model: string,
    effort?: string,
  ): Promise<void> {
    const client = this.requireClient();
    await client.call<{ selected: unknown }>("session/selectModel", {
      request: {
        sessionId,
        provider,
        model,
        ...(effort === undefined ? {} : { reasoningEffort: effort }),
      },
    });
  }

  private async pull(sessionId: string): Promise<CommandDescriptorView[]> {
    const client = this.requireClient();
    const raw = await client.call<RawCommandDescriptor[]>("commands/list", {
      agentId: sessionId,
    });
    const items = raw.map((c) => ({
      name: c.name,
      description: c.description,
      ...(c.input?.hint !== undefined ? { hint: c.input.hint } : {}),
    }));
    this.directory.set(sessionId, items);
    return items;
  }
}
