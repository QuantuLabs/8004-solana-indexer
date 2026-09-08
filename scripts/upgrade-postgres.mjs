#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const knownWrapped = new Set([
  "20260301123000_gapless_ids_for_all_scopes.sql",
  "20260303160000_add_collection_sequential_id.sql",
]);
const transactionControl = /^(?:BEGIN|COMMIT|ROLLBACK|ABORT|START\s+TRANSACTION|SAVEPOINT|RELEASE\s+SAVEPOINT|PREPARE\s+TRANSACTION)(?:\s+(?:WORK|TRANSACTION))?\s*;$/i;

function topLevelLines(sql) {
  const visible = [];
  let dollar = null;
  let blockDepth = 0;
  let quote = null;

  for (const line of sql.split("\n")) {
    let output = "";
    for (let i = 0; i < line.length;) {
      if (dollar) {
        const end = line.indexOf(dollar, i);
        if (end === -1) break;
        i = end + dollar.length;
        dollar = null;
        continue;
      }
      if (blockDepth) {
        if (line.startsWith("/*", i)) { blockDepth++; i += 2; continue; }
        if (line.startsWith("*/", i)) { blockDepth--; i += 2; continue; }
        i++;
        continue;
      }
      if (quote) {
        if (line[i] === quote && line[i + 1] === quote) { i += 2; continue; }
        if (line[i] === quote) quote = null;
        i++;
        continue;
      }
      if (line.startsWith("--", i)) break;
      if (line.startsWith("/*", i)) { blockDepth = 1; i += 2; continue; }
      if (line[i] === "'" || line[i] === '"') { quote = line[i]; i++; continue; }
      const tag = line.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
      if (tag) { dollar = tag; i += tag.length; continue; }
      output += line[i++];
    }
    visible.push(output.trim());
  }
  if (dollar || blockDepth || quote) throw new Error("unterminated SQL quote or comment");
  return visible;
}

function prepare(filename, sql) {
  const controls = topLevelLines(sql)
    .map((line, index) => transactionControl.test(line) ? { line, index } : null)
    .filter(Boolean);
  if (!knownWrapped.has(filename)) {
    if (controls.length) throw new Error(`${filename}: unexpected standalone transaction control`);
    return sql;
  }
  if (controls.length !== 2 || controls[0].line !== "BEGIN;" || controls[1].line !== "COMMIT;") {
    throw new Error(`${filename}: known transaction wrapper shape changed`);
  }
  const lines = sql.split("\n");
  lines[controls[0].index] = "";
  lines[controls[1].index] = "";
  return lines.join("\n");
}

export function collectPendingSql(after, migrationsDir = resolve(root, "supabase/migrations")) {
  if (!after || basename(after) !== after || !after.endsWith(".sql")) {
    throw new Error("--after must be an exact migration basename");
  }
  const filenames = readdirSync(migrationsDir)
    .filter((name) => name.endsWith(".sql"))
    .sort();
  const index = filenames.indexOf(after);
  if (index === -1) throw new Error(`--after migration not shipped: ${after}`);
  const pending = filenames.slice(index + 1);
  const sql = pending.map((filename) => {
    const prepared = prepare(filename, readFileSync(resolve(migrationsDir, filename), "utf8"));
    return `\n-- ${filename}\n${prepared}\n`;
  }).join("");
  return { filenames: pending, sql };
}

function main() {
  const after = process.argv.length === 4 && process.argv[2] === "--after" ? process.argv[3] : null;
  try {
    const pending = collectPendingSql(after);
    for (const filename of pending.filenames) console.log(filename);
    const result = spawnSync(process.env.PSQL || "psql", ["-X", "-v", "ON_ERROR_STOP=1", "--single-transaction"], {
      input: pending.sql,
      stdio: ["pipe", "ignore", "ignore"],
    });
    if (result.error || result.status !== 0) throw new Error("psql failed");
    console.log("upgrade succeeded");
  } catch {
    console.error("upgrade failed");
    process.exitCode = 1;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
