import type { DshLauncher } from "./discovery.ts";

export const BROKER_PROTOCOL_VERSION = 1 as const;

export type BrokerErrorCode =
  "launcher-required" | "port-conflict" | "configuration" | "internal";

export interface BrokerAcquireRequest {
  protocol: typeof BROKER_PROTOCOL_VERSION;
  type: "acquire";
  port: number;
  launcher?: DshLauncher;
}

export type BrokerReply =
  | {
      protocol: typeof BROKER_PROTOCOL_VERSION;
      type: "ready";
      baseUrl: string;
      pid: number | null;
      managed: boolean;
      reportedVersion: string;
      /**
       * 0.1.2+ 的会话 cookie（由 launch token 换取）。旧版 dsh 没有认证，
       * 该字段缺席；扩展凭它调用 /api 与 /api/remote.mux。
       */
      cookie?: string;
      /**
       * 0.1.2+ 的 launch token（就绪 URL 携带，进程存活期间可复用）。浏览器
       * 打开扩展的"在浏览器打开"需要 `?token=` 完成登录，故随租约透传。
       */
      token?: string;
    }
  | {
      protocol: typeof BROKER_PROTOCOL_VERSION;
      type: "error";
      code: BrokerErrorCode;
      message: string;
    };

export interface BrokerMetadata {
  protocol: typeof BROKER_PROTOCOL_VERSION;
  brokerPid: number;
  dshPid: number | null;
  baseUrl: string;
  port: number;
  managed: boolean;
  reportedVersion: string;
  startedAt: string;
}
