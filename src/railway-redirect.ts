import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

export const RAILWAY_REDIRECT_TARGET = "https://8004-indexer-dev.qnt.sh";
const CORS_ALLOWED_HEADERS = "Content-Type, Authorization, Prefer";
const CORS_ALLOWED_METHODS = "GET, HEAD, POST, OPTIONS";

function normalizeRedirectPort(rawPort: string | undefined): number {
  if (!rawPort || !/^\d+$/.test(rawPort)) {
    return 3001;
  }

  const parsedPort = Number.parseInt(rawPort, 10);
  return parsedPort > 0 ? parsedPort : 3001;
}

export function resolveRailwayRedirectPort(env: NodeJS.ProcessEnv = process.env): number {
  return normalizeRedirectPort(env.PORT ?? env.API_PORT);
}

export function resolveRailwayRedirectLocation(
  requestUrl: string | undefined,
  targetBase: string = RAILWAY_REDIRECT_TARGET,
): string {
  const target = new URL(targetBase);
  const request = new URL(requestUrl ?? "/", "http://railway.local");

  const targetPath = target.pathname === "/" ? "" : target.pathname.replace(/\/+$/, "");
  const requestPath = request.pathname === "/" ? "" : request.pathname;

  target.pathname = `${targetPath}${requestPath}` || "/";
  target.search = request.search;
  target.hash = "";

  return target.toString();
}

export function handleRailwayRedirect(
  req: IncomingMessage,
  res: ServerResponse,
  targetBase: string = RAILWAY_REDIRECT_TARGET,
): void {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", CORS_ALLOWED_METHODS);
  res.setHeader("Access-Control-Allow-Headers", CORS_ALLOWED_HEADERS);
  res.setHeader("Access-Control-Max-Age", "86400");

  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    res.end();
    return;
  }

  res.statusCode = 308;
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Location", resolveRailwayRedirectLocation(req.url, targetBase));
  res.end();
}

export async function startRailwayRedirectServer(options: {
  host?: string;
  port?: number;
  targetBase?: string;
} = {}): Promise<Server> {
  const host = options.host;
  const port = options.port ?? resolveRailwayRedirectPort();
  const targetBase = options.targetBase ?? RAILWAY_REDIRECT_TARGET;
  const server = createServer((req, res) => handleRailwayRedirect(req, res, targetBase));

  return await new Promise<Server>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => resolve(server));
  });
}
