import { defineConfig } from "vitest/config";

const e2eWorkers = Number.parseInt(process.env.VITEST_E2E_MAX_WORKERS ?? "1", 10);

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/e2e/**/*.test.ts"],
    setupFiles: ["./tests/e2e/setup.ts"],
    testTimeout: 60000,
    // Default to 1 worker for deterministic chain state; override via VITEST_E2E_MAX_WORKERS.
    pool: "forks",
    maxWorkers: Number.isNaN(e2eWorkers) ? 1 : Math.max(1, e2eWorkers),
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      reportsDirectory: "./coverage/e2e",
      include: ["src/**/*.ts"],
      exclude: ["src/index.ts", "src/**/index.ts", "src/**/*.d.ts"],
    },
  },
});
