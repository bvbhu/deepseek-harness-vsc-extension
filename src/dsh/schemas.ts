/**
 * 运行时契约校验 (0.1.2 "typert" generation)。
 *
 * 旧实现（D11.2「契约跟随」）会从用户安装的 dsh 里 require
 * `@deepseek-ai/dsh-host-apiproxy`，加载它导出的 zod schema 做运行时校验。
 * 0.1.2 起该包已随 `dsh-host-apiproxy` 一起删除，改为 typert 声明表
 * （`lib/typert.host.js`）+ `lib/types/*.d.ts`，不再有可 require 的
 * `clientRequestSchema` / `eventsSchema` 入口。
 *
 * 因此运行时 schema 校验不再可用，降级为 wire.ts 内的结构化校验
 * （envelope type + rpcId 回显）。该函数保留原签名以便上层调用不变，
 * 恒返回 null。
 */

import type { EnvelopeValidator } from "./wire.ts";

/**
 * 0.1.2 起恒为 null：契约来源已从可加载的 schema 包迁移到 typert 声明表。
 * 保留函数形态，避免上层需要区分"校验可用/不可用"的分支失效。
 */
export async function createWireValidator(): Promise<EnvelopeValidator | null> {
  return null;
}
