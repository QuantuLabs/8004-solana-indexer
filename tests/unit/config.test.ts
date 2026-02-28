import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Prevent dotenv from re-loading .env on each dynamic import
vi.mock("dotenv/config", () => ({}));

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
      // Note: dotenv loads .env before tests, so we clear ALL relevant vars
      delete process.env.DATABASE_URL;
      delete process.env.DB_MODE;
      delete process.env.RPC_URL;
      delete process.env.WS_URL;
      delete process.env.INDEXER_MODE;
      delete process.env.POLLING_INTERVAL;
      delete process.env.BATCH_SIZE;
      delete process.env.WS_RECONNECT_INTERVAL;
      delete process.env.WS_MAX_RETRIES;
      delete process.env.LOG_LEVEL;
      delete process.env.API_MODE;
      delete process.env.ENABLE_GRAPHQL;
      delete process.env.INDEX_METADATA;
      delete process.env.PROGRAM_ID;
      delete process.env.SOLANA_NETWORK;
      delete process.env.GRAPHQL_STATS_CACHE_TTL_MS;
      delete process.env.IPFS_GATEWAY_BASE;
      delete process.env.URI_DIGEST_TRUSTED_HOSTS;
      delete process.env.INDEXER_START_SIGNATURE;
      delete process.env.INDEXER_START_SLOT;
      delete process.env.POSTGREST_URL;
      delete process.env.POSTGREST_TOKEN;

      const { config } = await import("../../src/config.js");

      expect(config.databaseUrl).toBe("file:./data/indexer.db");
      expect(config.solanaNetwork).toBe("devnet");
      expect(config.rpcUrl).toBe("https://api.devnet.solana.com");
      expect(config.wsUrl).toBe("wss://api.devnet.solana.com");
      // programId comes from SDK (PROGRAM_ID.toBase58()), not env var
      expect(config.programId).toBe("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
      expect(config.indexerMode).toBe("auto");
      expect(config.pollingInterval).toBe(5000);
      expect(config.batchSize).toBe(100);
      expect(config.wsReconnectInterval).toBe(3000);
      expect(config.wsMaxRetries).toBe(5);
      expect(config.logLevel).toBe("info");
      expect(config.apiMode).toBe("both");
      expect(config.graphqlStatsCacheTtlMs).toBe(60000);
      expect(config.ipfsGatewayBase).toBe("https://ipfs.io");
      expect(config.uriDigestTrustedHosts).toEqual([]);
      expect(config.indexerStartSignature).toBeNull();
      expect(config.indexerStartSlot).toBeNull();
    });

    it("should use custom env values when set", async () => {
      process.env.DATABASE_URL = "POSTGRES_DSN_REDACTED";
      process.env.RPC_URL = "https://custom.rpc.com";
      process.env.WS_URL = "wss://custom.ws.com";
      process.env.INDEXER_MODE = "polling";
      process.env.POLLING_INTERVAL = "10000";
      process.env.BATCH_SIZE = "200";
      process.env.WS_RECONNECT_INTERVAL = "5000";
      process.env.WS_MAX_RETRIES = "10";
      process.env.LOG_LEVEL = "debug";
      process.env.API_MODE = "rest";
      process.env.GRAPHQL_STATS_CACHE_TTL_MS = "45000";
      process.env.IPFS_GATEWAY_BASE = "https://gateway.example.com/";
      process.env.URI_DIGEST_TRUSTED_HOSTS = " localhost,127.0.0.1 ";
      process.env.INDEXER_START_SIGNATURE = "env-start-signature";
      process.env.INDEXER_START_SLOT = "123456789";

      const { config } = await import("../../src/config.js");

      expect(config.databaseUrl).toBe("POSTGRES_DSN_REDACTED");
      expect(config.rpcUrl).toBe("https://custom.rpc.com");
      expect(config.wsUrl).toBe("wss://custom.ws.com");
      // programId always comes from SDK, not configurable via env
      expect(config.programId).toBe("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
      expect(config.indexerMode).toBe("polling");
      expect(config.pollingInterval).toBe(10000);
      expect(config.batchSize).toBe(200);
      expect(config.wsReconnectInterval).toBe(5000);
      expect(config.wsMaxRetries).toBe(10);
      expect(config.logLevel).toBe("debug");
      expect(config.apiMode).toBe("rest");
      expect(config.graphqlStatsCacheTtlMs).toBe(45000);
      expect(config.ipfsGatewayBase).toBe("https://gateway.example.com");
      expect(config.uriDigestTrustedHosts).toEqual(["localhost", "127.0.0.1"]);
      expect(config.indexerStartSignature).toBe("env-start-signature");
      expect(config.indexerStartSlot).toBe(123456789n);
    });

    it("should support POSTGREST_URL/POSTGREST_TOKEN aliases for REST proxy config", async () => {
      delete process.env.SUPABASE_URL;
      delete process.env.SUPABASE_KEY;
      process.env.POSTGREST_URL = "https://proxy-alias.supabase.co";
      process.env.POSTGREST_TOKEN = "alias-service-role-key";

      const { config } = await import("../../src/config.js");

      expect(config.supabaseUrl).toBe("https://proxy-alias.supabase.co");
      expect(config.supabaseKey).toBe("alias-service-role-key");
    });

    it("should prioritize SUPABASE_* over POSTGREST_* aliases when both are set", async () => {
      process.env.SUPABASE_URL = "https://primary.supabase.co";
      process.env.SUPABASE_KEY = "primary-key";
      process.env.POSTGREST_URL = "https://alias.supabase.co";
      process.env.POSTGREST_TOKEN = "alias-key";

      const { config } = await import("../../src/config.js");

      expect(config.supabaseUrl).toBe("https://primary.supabase.co");
      expect(config.supabaseKey).toBe("primary-key");
    });

    it("should support websocket mode", async () => {
      process.env.INDEXER_MODE = "websocket";

      const { config } = await import("../../src/config.js");

      expect(config.indexerMode).toBe("websocket");
    });

    it("should derive RPC/WS defaults from SOLANA_NETWORK", async () => {
      process.env.SOLANA_NETWORK = "mainnet-beta";
      delete process.env.RPC_URL;
      delete process.env.WS_URL;

      let imported = await import("../../src/config.js");
      expect(imported.config.solanaNetwork).toBe("mainnet-beta");
      expect(imported.config.rpcUrl).toBe("https://api.mainnet-beta.solana.com");
      expect(imported.config.wsUrl).toBe("wss://api.mainnet-beta.solana.com");

      vi.resetModules();
      process.env = { ...originalEnv, SOLANA_NETWORK: "testnet" };
      delete process.env.RPC_URL;
      delete process.env.WS_URL;
      imported = await import("../../src/config.js");
      expect(imported.config.rpcUrl).toBe("https://api.testnet.solana.com");
      expect(imported.config.wsUrl).toBe("wss://api.testnet.solana.com");

      vi.resetModules();
      process.env = { ...originalEnv, SOLANA_NETWORK: "localnet" };
      delete process.env.RPC_URL;
      delete process.env.WS_URL;
      imported = await import("../../src/config.js");
      expect(imported.config.rpcUrl).toBe("http://127.0.0.1:8899");
      expect(imported.config.wsUrl).toBe("ws://127.0.0.1:8900");
    });

    it("should prioritize explicit RPC_URL/WS_URL over SOLANA_NETWORK defaults", async () => {
      process.env.SOLANA_NETWORK = "mainnet-beta";
      process.env.RPC_URL = "https://custom.rpc.com";
      process.env.WS_URL = "wss://custom.ws.com";

      const { config } = await import("../../src/config.js");

      expect(config.rpcUrl).toBe("https://custom.rpc.com");
      expect(config.wsUrl).toBe("wss://custom.ws.com");
    });
  });

  describe("parse-time validation", () => {
    it("should throw when DB_MODE is invalid", async () => {
      process.env.DB_MODE = "invalid_db";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid DB_MODE 'invalid_db'/
      );
    });

    it("should throw when INDEXER_MODE is invalid", async () => {
      process.env.INDEXER_MODE = "invalid";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid INDEXER_MODE/
      );
    });

    it("should normalize legacy API_MODE aliases", async () => {
      process.env.API_MODE = "hybrid";
      let imported = await import("../../src/config.js");
      expect(imported.config.apiMode).toBe("both");

      vi.resetModules();
      process.env = { ...originalEnv, API_MODE: "graph" };
      imported = await import("../../src/config.js");
      expect(imported.config.apiMode).toBe("graphql");
    });

    it("should throw when API_MODE is invalid", async () => {
      process.env.API_MODE = "bad_mode";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid API_MODE/
      );
    });

    it("should throw when INDEX_METADATA is invalid", async () => {
      process.env.INDEX_METADATA = "bogus";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid INDEX_METADATA 'bogus'/
      );
    });

    it("should throw when SOLANA_NETWORK is invalid", async () => {
      process.env.SOLANA_NETWORK = "mainnet";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid SOLANA_NETWORK 'mainnet'/
      );
    });

    it("should throw when IPFS_GATEWAY_BASE is invalid", async () => {
      process.env.IPFS_GATEWAY_BASE = "not-a-url";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid IPFS_GATEWAY_BASE/
      );
    });

    it("should throw when URI_DIGEST_TRUSTED_HOSTS contains non-local hosts", async () => {
      process.env.URI_DIGEST_TRUSTED_HOSTS = "example.com";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid URI_DIGEST_TRUSTED_HOSTS entry/
      );
    });

    it("should throw when INDEXER_START_SLOT is invalid", async () => {
      process.env.INDEXER_START_SLOT = "not-a-slot";

      await expect(import("../../src/config.js")).rejects.toThrow(
        /Invalid INDEXER_START_SLOT/
      );
    });
  });

  describe("validateConfig", () => {
    it("should pass validation with valid config", async () => {
      process.env.DATABASE_URL = "file:./data/test.db";
      process.env.RPC_URL = "https://api.devnet.solana.com";
      process.env.PROGRAM_ID = "TestProgramId";
      process.env.INDEXER_MODE = "auto";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).not.toThrow();
    });

    it("should warn when SUPABASE_SSL_VERIFY is false", async () => {
      process.env.SUPABASE_SSL_VERIFY = "false";
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      const { validateConfig } = await import("../../src/config.js");
      validateConfig();

      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining("SUPABASE_SSL_VERIFY=false")
      );
      warnSpy.mockRestore();
    });

    it("should warn when mainnet-beta uses default devnet PROGRAM_ID", async () => {
      process.env.SOLANA_NETWORK = "mainnet-beta";
      delete process.env.PROGRAM_ID;
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      const { validateConfig } = await import("../../src/config.js");
      validateConfig();

      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining("SOLANA_NETWORK=mainnet-beta")
      );
      warnSpy.mockRestore();
    });

    it("should throw when DB_MODE=supabase without SUPABASE_DSN", async () => {
      process.env.DB_MODE = "supabase";
      delete process.env.SUPABASE_DSN;

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "SUPABASE_DSN required when DB_MODE=supabase"
      );
    });

    it("should not throw when DB_MODE=supabase with SUPABASE_DSN set", async () => {
      process.env.DB_MODE = "supabase";
      process.env.SUPABASE_DSN = "POSTGRES_DSN_REDACTED";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).not.toThrow();
    });

    it("should throw when VERIFY_INTERVAL_MS is below 5000", async () => {
      process.env.VERIFY_INTERVAL_MS = "1000";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_INTERVAL_MS must be at least 5000ms"
      );
    });

    it("should throw when VERIFY_BATCH_SIZE is below 1", async () => {
      process.env.VERIFY_BATCH_SIZE = "0";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_BATCH_SIZE must be between 1 and 1000"
      );
    });

    it("should throw when VERIFY_BATCH_SIZE exceeds 1000", async () => {
      process.env.VERIFY_BATCH_SIZE = "1001";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_BATCH_SIZE must be between 1 and 1000"
      );
    });

    it("should throw when VERIFY_SAFETY_MARGIN_SLOTS is negative", async () => {
      process.env.VERIFY_SAFETY_MARGIN_SLOTS = "-1";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_SAFETY_MARGIN_SLOTS must be between 0 and 150"
      );
    });

    it("should throw when VERIFY_SAFETY_MARGIN_SLOTS exceeds 150", async () => {
      process.env.VERIFY_SAFETY_MARGIN_SLOTS = "200";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_SAFETY_MARGIN_SLOTS must be between 0 and 150"
      );
    });

    it("should throw when VERIFY_RECOVERY_CYCLES is negative", async () => {
      process.env.VERIFY_RECOVERY_CYCLES = "-5";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_RECOVERY_CYCLES must be between 0 and 1000"
      );
    });

    it("should throw when VERIFY_RECOVERY_CYCLES exceeds 1000", async () => {
      process.env.VERIFY_RECOVERY_CYCLES = "5000";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_RECOVERY_CYCLES must be between 0 and 1000"
      );
    });

    it("should throw when VERIFY_RECOVERY_BATCH_SIZE is below 1", async () => {
      process.env.VERIFY_RECOVERY_BATCH_SIZE = "0";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_RECOVERY_BATCH_SIZE must be between 1 and 1000"
      );
    });

    it("should throw when VERIFY_RECOVERY_BATCH_SIZE exceeds 1000", async () => {
      process.env.VERIFY_RECOVERY_BATCH_SIZE = "9999";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "VERIFY_RECOVERY_BATCH_SIZE must be between 1 and 1000"
      );
    });

    it("should throw when GRAPHQL_STATS_CACHE_TTL_MS is below 1000", async () => {
      process.env.GRAPHQL_STATS_CACHE_TTL_MS = "500";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "GRAPHQL_STATS_CACHE_TTL_MS must be between 1000 and 3600000"
      );
    });

    it("should throw when GRAPHQL_STATS_CACHE_TTL_MS exceeds 3600000", async () => {
      process.env.GRAPHQL_STATS_CACHE_TTL_MS = "3600001";

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "GRAPHQL_STATS_CACHE_TTL_MS must be between 1000 and 3600000"
      );
    });

    it("should throw when INDEXER_START_SLOT is set without INDEXER_START_SIGNATURE", async () => {
      process.env.INDEXER_START_SLOT = "123";
      delete process.env.INDEXER_START_SIGNATURE;

      const { validateConfig } = await import("../../src/config.js");

      expect(() => validateConfig()).toThrow(
        "INDEXER_START_SLOT requires INDEXER_START_SIGNATURE"
      );
    });
  });
});
