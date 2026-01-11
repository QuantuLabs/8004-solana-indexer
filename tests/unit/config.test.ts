import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Store original env
const originalEnv = process.env;

describe("Config", () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe("config values", () => {
    it("should use default values when env vars not set", async () => {
      delete process.env.DATABASE_URL;
      delete process.env.RPC_URL;
      delete process.env.WS_URL;
      delete process.env.PROGRAM_ID;
      delete process.env.INDEXER_MODE;
      delete process.env.POLLING_INTERVAL;
      delete process.env.BATCH_SIZE;
      delete process.env.WS_RECONNECT_INTERVAL;
      delete process.env.WS_MAX_RETRIES;
      delete process.env.GRAPHQL_PORT;
      delete process.env.LOG_LEVEL;

      const { config } = await import("../../src/config.js");

      expect(config.databaseUrl).toContain("postgresql://");
      expect(config.rpcUrl).toBe("https://api.devnet.solana.com");
      expect(config.wsUrl).toBe("wss://api.devnet.solana.com");
      expect(config.programId).toBe("3GGkAWC3mYYdud8GVBsKXK5QC9siXtFkWVZFYtbueVbC");
      expect(config.indexerMode).toBe("auto");
      expect(config.pollingInterval).toBe(5000);
      expect(config.batchSize).toBe(100);
      expect(config.wsReconnectInterval).toBe(3000);
      expect(config.wsMaxRetries).toBe(5);
      expect(config.graphqlPort).toBe(4000);
      expect(config.logLevel).toBe("info");
    });

    it("should use custom env values when set", async () => {
      process.env.DATABASE_URL = "postgresql://custom:custom@localhost/custom";
      process.env.RPC_URL = "https://custom.rpc.com";
      process.env.WS_URL = "wss://custom.ws.com";
      process.env.PROGRAM_ID = "CustomProgramId123";
      process.env.INDEXER_MODE = "polling";
      process.env.POLLING_INTERVAL = "10000";
      process.env.BATCH_SIZE = "200";
      process.env.WS_RECONNECT_INTERVAL = "5000";
      process.env.WS_MAX_RETRIES = "10";
      process.env.GRAPHQL_PORT = "5000";
      process.env.LOG_LEVEL = "debug";

      const { config } = await import("../../src/config.js");

      expect(config.databaseUrl).toBe("postgresql://custom:custom@localhost/custom");
      expect(config.rpcUrl).toBe("https://custom.rpc.com");
      expect(config.wsUrl).toBe("wss://custom.ws.com");
      expect(config.programId).toBe("CustomProgramId123");
      expect(config.indexerMode).toBe("polling");
      expect(config.pollingInterval).toBe(10000);
      expect(config.batchSize).toBe(200);
      expect(config.wsReconnectInterval).toBe(5000);
      expect(config.wsMaxRetries).toBe(10);
      expect(config.graphqlPort).toBe(5000);
      expect(config.logLevel).toBe("debug");
    });

    it("should support websocket mode", async () => {
      process.env.INDEXER_MODE = "websocket";

      const { config } = await import("../../src/config.js");

      expect(config.indexerMode).toBe("websocket");
    });
  });

  describe("validateConfig", () => {
    it("should pass validation with valid config", async () => {
      process.env.DATABASE_URL = "postgresql://test:test@localhost/test";
      process.env.RPC_URL = "https://api.devnet.solana.com";
      process.env.PROGRAM_ID = "TestProgramId";
      process.env.INDEXER_MODE = "auto";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).not.toThrow();
    });

    it("should throw when INDEXER_MODE is invalid", async () => {
      process.env.DATABASE_URL = "postgresql://test:test@localhost/test";
      process.env.RPC_URL = "https://api.devnet.solana.com";
      process.env.PROGRAM_ID = "TestProgramId";
      process.env.INDEXER_MODE = "invalid";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "INDEXER_MODE must be 'auto', 'polling', or 'websocket'"
      );
    });
  });
});
