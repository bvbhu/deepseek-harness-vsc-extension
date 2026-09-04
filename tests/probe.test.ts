import { createServer, type Server } from "node:http";
import { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import { normalizeDshBaseUrl, probeDsh } from "../src/dsh/probe.ts";

const servers: Server[] = [];
const websocketServers: WebSocketServer[] = [];

afterEach(async () => {
  for (const wss of websocketServers.splice(0))
    await new Promise<void>((resolve) => wss.close(() => resolve()));
  for (const server of servers.splice(0))
    await new Promise<void>((resolve) => server.close(() => resolve()));
});

describe("probeDsh", () => {
  it("recognizes a 0.1.2 endpoint via the $events ready handshake", async () => {
    const baseUrl = await startFixture(true);

    await expect(probeDsh(baseUrl)).resolves.toMatchObject({
      kind: "dsh",
      baseUrl,
      description: { cwd: "/fixture" },
      clientId: "test-client",
    });
  });

  it("reports not-dsh when the remote.mux upgrade is absent", async () => {
    const baseUrl = await startFixture(false);

    await expect(probeDsh(baseUrl)).resolves.toMatchObject({
      kind: "not-dsh",
      baseUrl,
    });
  });

  it("rejects credentials and non-root paths in external URLs", () => {
    expect(() =>
      normalizeDshBaseUrl("https://user:secret@example.com:3080"),
    ).toThrow(/用户名或密码/u);
    expect(() => normalizeDshBaseUrl("https://example.com:3080/dsh")).toThrow(
      /根路径/u,
    );
  });

  it("strips a ?token= launch token from an external URL", () => {
    expect(normalizeDshBaseUrl("http://127.0.0.1:3080/?token=abc")).toBe(
      "http://127.0.0.1:3080",
    );
    expect(normalizeDshBaseUrl("http://127.0.0.1:3080?token=abc#frag")).toBe(
      "http://127.0.0.1:3080",
    );
  });
});

/**
 * 0.1.2 fixture: a WebSocket upgrader on /api/remote.mux that answers the
 * `$events` open with a `ready` item. With `withMux=false` no upgrader is
 * registered, so the probe's WS handshake fails and (GET / → 404) it reports
 * not-dsh.
 */
async function startFixture(withMux: boolean): Promise<string> {
  const server = createServer((_request, response) => {
    response.writeHead(404).end();
  });
  const wss = new WebSocketServer({ noServer: true });
  if (withMux) {
    server.on("upgrade", (request, socket, head) => {
      if (request.url !== "/api/remote.mux") {
        socket.destroy();
        return;
      }
      wss.handleUpgrade(request, socket, head, (ws) => {
        ws.on("message", (data) => {
          let frame: { type?: string; streamId?: string; endpoint?: string };
          try {
            frame = JSON.parse(String(data));
          } catch {
            return;
          }
          if (frame.type === "open" && frame.endpoint === "$events") {
            ws.send(
              JSON.stringify({
                type: "item",
                streamId: frame.streamId,
                value: {
                  type: "ready",
                  clientId: "test-client",
                  host: { home: "/fixture" },
                },
              }),
            );
          }
        });
      });
    });
  }
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  servers.push(server);
  websocketServers.push(wss);
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${String(address.port)}`;
}
