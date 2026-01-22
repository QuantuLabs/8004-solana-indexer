import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/**/*.test.ts"],
    exclude: ["tests/e2e/**/*.test.ts", "tests/unit/api/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      reportsDirectory: "./coverage",
      include: ["src/**/*.ts"],
      exclude: [
        "src/index.ts", // Entry point - tested in e2e
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
