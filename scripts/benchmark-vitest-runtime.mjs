#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const DEFAULT_VITEST_ARGS = ["run", "tests/unit"];

function parseArgs(argv) {
  let runtime = "both";
  let vitestArgs = [];
  let passthroughMode = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (passthroughMode) {
      vitestArgs.push(arg);
      continue;
    }
    if (arg.startsWith("--runtime=")) {
      runtime = arg.split("=")[1];
      continue;
    }
    if (arg === "--runtime" && argv[i + 1]) {
      runtime = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--") {
      passthroughMode = true;
      vitestArgs.push(...argv.slice(i + 1));
      break;
    }
    vitestArgs.push(arg);
  }

  if (vitestArgs.length === 0) {
    vitestArgs = DEFAULT_VITEST_ARGS;
  }

  if (!["node", "bun", "both"].includes(runtime)) {
    throw new Error(`Invalid runtime "${runtime}" (expected node|bun|both)`);
  }

  return { runtime, vitestArgs };
}

function parseMaxRssKb(stderr) {
  if (process.platform === "darwin") {
    const rssMatch = stderr.match(/(\d+)\s+maximum resident set size/i);
    if (rssMatch) {
      const rss = Number.parseInt(rssMatch[1], 10);
      return rss > 10_000_000 ? Math.ceil(rss / 1024) : rss;
    }
    const peakMatch = stderr.match(/(\d+)\s+peak memory footprint/i);
    return peakMatch ? Math.ceil(Number.parseInt(peakMatch[1], 10) / 1024) : null;
  }
  const match = stderr.match(/Maximum resident set size[^:]*:\s*(\d+)/i);
  return match ? Number.parseInt(match[1], 10) : null;
}

function runOneRuntime(name, command, commandArgs, vitestArgs) {
  const timeArgs =
    process.platform === "darwin"
      ? ["-l", command, ...commandArgs, ...vitestArgs]
      : ["-v", command, ...commandArgs, ...vitestArgs];

  const startedAt = Date.now();
  const result = spawnSync("/usr/bin/time", timeArgs, {
    env: {
      ...process.env,
      CI: process.env.CI ?? "1",
      FORCE_COLOR: "0",
    },
    encoding: "utf8",
    shell: false,
    stdio: "pipe",
  });
  const elapsedMs = Date.now() - startedAt;

  return {
    runtime: name,
    command: `${command} ${[...commandArgs, ...vitestArgs].join(" ")}`.trim(),
    exitCode: result.status ?? 1,
    elapsedMs,
    maxRssKb: parseMaxRssKb(result.stderr ?? ""),
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
}

function formatMb(kb) {
  if (kb == null) return "n/a";
  return `${(kb / 1024).toFixed(1)} MB`;
}

function main() {
  const { runtime, vitestArgs } = parseArgs(process.argv.slice(2));
  const targets = runtime === "both" ? ["node", "bun"] : [runtime];

  const results = [];
  for (const target of targets) {
    const command = target === "node" ? "npx" : "bunx";
    const commandArgs = ["vitest"];
    console.log(`\n[bench] ${target}: ${command} ${commandArgs.join(" ")} ${vitestArgs.join(" ")}`);
    results.push(runOneRuntime(target, command, commandArgs, vitestArgs));
  }

  const benchmarkDir = join(process.cwd(), "benchmarks");
  mkdirSync(benchmarkDir, { recursive: true });
  const outputFile = join(
    benchmarkDir,
    `vitest-runtime-${new Date().toISOString().replace(/[:.]/g, "-")}.json`
  );
  writeFileSync(
    outputFile,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        vitestArgs,
        results: results.map((result) => ({
          runtime: result.runtime,
          command: result.command,
          exitCode: result.exitCode,
          elapsedMs: result.elapsedMs,
          maxRssKb: result.maxRssKb,
        })),
      },
      null,
      2
    )
  );

  console.log("\nRuntime | Exit | Time (ms) | Max RSS");
  console.log("--------------------------------------");
  for (const result of results) {
    console.log(
      `${result.runtime.padEnd(7)}| ${String(result.exitCode).padEnd(5)}| ${String(
        result.elapsedMs
      ).padEnd(10)}| ${formatMb(result.maxRssKb)}`
    );
  }

  console.log(`\n[bench] JSON report: ${outputFile}`);

  const failing = results.find((result) => result.exitCode !== 0);
  if (failing) {
    console.error(`\n[bench] ${failing.runtime} failed (exit ${failing.exitCode}).`);
    process.exit(failing.exitCode);
  }
}

main();
