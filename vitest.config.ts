import { defineConfig } from "vitest/config";
import { availableParallelism } from "node:os";

const envWorkers = process.env.VITEST_MAX_WORKERS;
const defaultWorkers = Math.max(2, Math.min(6, Math.ceil(availableParallelism() * 0.5)));
const maxWorkers =
  envWorkers && /^\d+%$/.test(envWorkers)
    ? envWorkers
    : envWorkers && /^\d+$/.test(envWorkers)
      ? Number.parseInt(envWorkers, 10)
      : defaultWorkers;

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/**/*.test.ts"],
    exclude: ["tests/e2e/**/*.test.ts"],
    pool: "forks",
    maxWorkers,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      reportsDirectory: "./coverage",
      include: ["src/**/*.ts"],
      exclude: [
        "src/**/index.ts", // Barrel files (re-exports only)
        "src/**/*.d.ts",
      ],
      thresholds: {
        statements: 99,
        branches: 94,
        functions: 96,
        lines: 99,
      },
    },
    setupFiles: ["./tests/setup.ts"],
    testTimeout: 30000,
  },
});
