import {
  RAILWAY_REDIRECT_TARGET,
  resolveRailwayRedirectPort,
  startRailwayRedirectServer,
} from "./railway-redirect.js";

const port = resolveRailwayRedirectPort();
const server = await startRailwayRedirectServer({ port });

console.info(`[railway-redirect] listening on :${port} and redirecting to ${RAILWAY_REDIRECT_TARGET}`);

function shutdown(signal: string): void {
  console.info(`[railway-redirect] received ${signal}, shutting down`);
  server.close((error) => {
    if (error) {
      console.error("[railway-redirect] shutdown failed", error);
      process.exit(1);
      return;
    }
    process.exit(0);
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
