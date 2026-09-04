/**
 * dsh web 0.1.2 浏览器认证。新版 dsh-web-app 打印的 ready URL 携带一次性
 * launch token（`?token=...`）；对该 URL 发起一次 GET 会换取一个 HttpOnly、
 * SameSite=Strict 的会话 cookie（`Set-Cookie: dsh-auth-<hash>=v1...`）。
 * 之后每个 /api/* HTTP RPC 与 /api/remote.mux WebSocket upgrade 都必须带上
 * 这个 cookie，否则统一返回 401（无 cookie 的 GET / 返回 401 + 说明文案）。
 *
 * 扩展自托管（Broker 拉起 dsh）时拿到的是完整 ready URL，可以完成交换；
 * 连接外部实例时用户必须粘贴带 token 的 URL（否则无法认证）。
 */

/** 从 dsh 打印的 URL 中取出 launch token。 */
export function extractLaunchToken(url: string): string | null {
  try {
    return new URL(url).searchParams.get("token");
  } catch {
    return null;
  }
}

/** 去掉 token 等查询参数，得到干净的 loopback 根地址。 */
export function stripToken(baseUrl: string): string {
  try {
    const url = new URL(baseUrl);
    url.search = "";
    url.hash = "";
    url.pathname = url.pathname.replace(/\/+$/u, "/") || "/";
    return url.origin + (url.pathname === "/" ? "" : url.pathname);
  } catch {
    return baseUrl.replace(/\/+$/u, "");
  }
}

export interface DshAuth {
  /** 可直接放进 Cookie 请求头的值，例如 `dsh-auth-abc=v1.eyJ...`。 */
  cookie: string;
}

/**
 * 用 launch token 换取会话 cookie。
 * @throws 当交换没有返回 Set-Cookie（token 缺失/失效/已被消费）。
 */
export async function acquireAuth(
  baseUrl: string,
  token: string,
  timeoutMs = 10_000,
): Promise<DshAuth> {
  const root = stripToken(baseUrl);
  const url = new URL(root.replace(/\/+$/u, "/"));
  url.searchParams.set("token", token);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url.toString(), {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
    });
    const setCookie = response.headers.get("set-cookie");
    if (!setCookie) {
      throw new Error(
        `dsh web 认证交换未返回会话 cookie (HTTP ${response.status})：launch token 可能已失效`,
      );
    }
    // 只取第一个 name=value 片段；Max-Age/Path/HttpOnly/SameSite 都是属性。
    const cookie = setCookie.split(";")[0]?.trim();
    if (!cookie || !cookie.includes("=")) {
      throw new Error("dsh web 认证交换返回了空的 Set-Cookie");
    }
    return { cookie };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`dsh web 认证交换超时(${Math.round(timeoutMs / 1000)}s)`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

/** 一次性完成：从带 token 的 URL 换 cookie。 */
export async function acquireAuthFromUrl(
  url: string,
  timeoutMs?: number,
): Promise<DshAuth | null> {
  const token = extractLaunchToken(url);
  if (!token) return null;
  return await acquireAuth(url, token, timeoutMs);
}
