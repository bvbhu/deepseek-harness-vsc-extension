/**
 * 设置面板的 wire 抽象：webview 不直接调 dsh，而是经 App.tsx 的 postMessage +
 * settingsReply 应答（requestId 关联）包装成 Promise。组件只依赖这个接口。
 */

import type {
  BusyEnterBehavior,
  DiscoveredModelView,
  SettingsProfileApply,
  SettingsProbe,
  SettingsProviderCreate,
  SettingsRemoveTarget,
} from "../../../../src/shared/protocol.ts";

export type SettingsReply =
  | { ok: true; value?: unknown }
  | { ok: false; text: string; conflict?: boolean };

export interface SettingsWire {
  apply(profile: SettingsProfileApply): Promise<SettingsReply>;
  remove(target: SettingsRemoveTarget): Promise<SettingsReply>;
  declare(create: SettingsProviderCreate): Promise<SettingsReply>;
  discover(probe: SettingsProbe): Promise<DiscoveredModelView[]>;
  /** 写默认权限模式（permission namespace defaultPreset）。 */
  selectPermissionDefault(
    preset: string,
    expectedRevision: number,
  ): Promise<SettingsReply>;
  /** 写繁忙时 Enter 键行为（ui-conversation namespace busyEnter）。 */
  selectBusyEnter(
    behavior: BusyEnterBehavior,
    expectedRevision: number,
  ): Promise<SettingsReply>;
  /** 打开 VS Code 原生设置页，并过滤到本扩展贡献的 DSH 设置。 */
  openExtensionSettings(): void;
  refresh(): void;
  pickDshPath(): void;
  /** 重启 dsh 服务（停掉进程后重新解析并连接）。 */
  restartDsh(): void;
  /** 仅重连：不动 dsh 进程，只重建本窗口的 mux 传输。 */
  reconnectDsh(): void;
  /** 关闭当前 dsh，并把「断连后自动重启」置为 false（不再被自动拉起）。 */
  stopDsh(): void;
  /** 关于页「repo 链接」→ 扩展侧在系统浏览器打开指定 URL。 */
  openExternalUrl(url: string): void;
}
