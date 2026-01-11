import { describe, it, expect, vi, beforeEach } from "vitest";

describe("Logger", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  describe("logger", () => {
    it("should export a logger instance", async () => {
      const { logger } = await import("../../src/logger.js");

      expect(logger).toBeDefined();
      expect(typeof logger.info).toBe("function");
      expect(typeof logger.warn).toBe("function");
      expect(typeof logger.error).toBe("function");
      expect(typeof logger.debug).toBe("function");
    });
  });

  describe("createChildLogger", () => {
    it("should create a child logger with component name", async () => {
      const { createChildLogger } = await import("../../src/logger.js");

      const childLogger = createChildLogger("test-component");

      expect(childLogger).toBeDefined();
      expect(typeof childLogger.info).toBe("function");
      expect(typeof childLogger.warn).toBe("function");
      expect(typeof childLogger.error).toBe("function");
      expect(typeof childLogger.debug).toBe("function");
    });

    it("should create multiple child loggers", async () => {
      const { createChildLogger } = await import("../../src/logger.js");

      const child1 = createChildLogger("component-1");
      const child2 = createChildLogger("component-2");

      expect(child1).toBeDefined();
      expect(child2).toBeDefined();
      // Both should have the expected methods
      expect(typeof child1.info).toBe("function");
      expect(typeof child2.info).toBe("function");
    });
  });
});
