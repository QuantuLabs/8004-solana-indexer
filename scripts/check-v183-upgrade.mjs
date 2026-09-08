#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import pg from "pg";
import { collectPendingSql } from "./upgrade-postgres.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const pending = collectPendingSql("20260315195500_fix_recovered_orphan_agent_id_assignment.sql");
const connection = {
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER || "postgres",
};
const cohorts = ["devnet", "mainnet", "mainnet2"];
const targetPrefix = process.env.UPGRADE_TEST_PREFIX || `migration_fix_${process.pid}`;
const qi = (value) => `"${value.replaceAll('"', '""')}"`;
const digest = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex");

async function withDb(database, run) {
  const client = new pg.Client({ ...connection, database });
  await client.connect();
  try { return await run(client); } finally { await client.end(); }
}

async function snapshot(database, layout) {
  return withDb(database, async (client) => {
    if (!layout) {
      const { rows } = await client.query(`
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name
      `);
      layout = {};
      for (const { table_name } of rows) {
        const columns = await client.query(`
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position
        `, [table_name]);
        layout[table_name] = columns.rows.map(({ column_name }) => column_name)
          .filter((column) => column !== "mutation_order_key");
      }
    }
    const tables = {};
    for (const [table, columns] of Object.entries(layout)) {
      const { rows } = await client.query(`SELECT ${columns.map(qi).join(", ")} FROM ${qi(table)}`);
      const canonical = rows.map((row) => JSON.stringify(row)).sort();
      tables[table] = { count: rows.length, hash: digest(canonical) };
    }
    const identityColumns = {
      agents: ["asset", "agent_id"],
      feedbacks: ["id", "asset", "feedback_id"],
      feedback_responses: ["id", "asset", "response_id"],
      revocations: ["id", "asset", "revocation_id"],
      collection_pointers: ["col", "creator", "collection_id"],
    };
    const identities = {};
    for (const [table, columns] of Object.entries(identityColumns)) {
      if (!layout[table] || columns.some((column) => !layout[table].includes(column))) continue;
      const { rows } = await client.query(`SELECT ${columns.map(qi).join(", ")} FROM ${qi(table)}`);
      identities[table] = digest(rows.map((row) => JSON.stringify(row)).sort());
    }
    return { layout, tables, identities };
  });
}

async function schemaHash(database) {
  return withDb(database, async (client) => {
    const { rows } = await client.query(`
      SELECT 'column' kind, table_name object, ordinal_position::text name,
             concat_ws('|', column_name, data_type, is_nullable, column_default) definition
      FROM information_schema.columns WHERE table_schema = 'public'
      UNION ALL
      SELECT 'index', tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'
      UNION ALL
      SELECT 'constraint', c.conrelid::regclass::text, c.conname, pg_get_constraintdef(c.oid)
      FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = 'public'
      UNION ALL
      SELECT 'function', n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', pg_get_functiondef(p.oid)
      FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'
      ORDER BY 1, 2, 3
    `);
    return digest(rows);
  });
}

function psql(database, injectFailure = false) {
  const args = ["-X", "-v", "ON_ERROR_STOP=1", "--single-transaction", "-d", database];
  return spawnSync(process.env.PSQL || "psql", args, {
    input: `${pending.sql}${injectFailure ? "\nSELECT 1/0;\n" : ""}`,
    cwd: root,
    env: { ...process.env, PGHOST: connection.host, PGPORT: String(connection.port), PGUSER: connection.user },
    stdio: ["pipe", "inherit", "inherit"],
  }).status;
}

async function clone(source, target) {
  await withDb("postgres", async (client) => {
    const exists = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [target]);
    if (exists.rowCount) throw new Error(`${target} already exists; refusing to overwrite it`);
    await client.query(`CREATE DATABASE ${qi(target)} TEMPLATE ${qi(source)}`);
  });
}

for (const cohort of cohorts) {
  const startedAt = new Date().toISOString();
  const source = `baseline_${cohort}`;
  const target = `${targetPrefix}_${cohort}`;
  await clone(source, target);
  const before = await snapshot(source);
  if (psql(target) !== 0) throw new Error(`${target}: first upgrade failed`);
  const after = await snapshot(target, before.layout);
  if (JSON.stringify(after.tables) !== JSON.stringify(before.tables) || JSON.stringify(after.identities) !== JSON.stringify(before.identities)) throw new Error(`${target}: old-column data or public-ID mapping changed`);
  const firstSchema = await schemaHash(target);
  if (psql(target) !== 0) throw new Error(`${target}: repeat upgrade failed`);
  const repeated = await snapshot(target, before.layout);
  if (JSON.stringify(repeated.tables) !== JSON.stringify(before.tables) || JSON.stringify(repeated.identities) !== JSON.stringify(before.identities) || await schemaHash(target) !== firstSchema) throw new Error(`${target}: repeat changed schema, old-column data, or public-ID mapping`);
  console.log(JSON.stringify({ database: target, startedAt, finishedAt: new Date().toISOString(), tables: Object.keys(before.tables).length, publicIdMappings: Object.keys(before.identities).length, counters: before.tables.id_counters, cursors: before.tables.indexer_state, preserved: true, repeatSafe: true }));
}

const rollbackStartedAt = new Date().toISOString();
const rollbackTarget = `${targetPrefix}_rollback`;
await clone("baseline_devnet", rollbackTarget);
const rollbackData = await snapshot(rollbackTarget);
const rollbackSchema = await schemaHash(rollbackTarget);
if (psql(rollbackTarget, true) === 0) throw new Error("forced-failure batch unexpectedly succeeded");
const rolledBack = await snapshot(rollbackTarget, rollbackData.layout);
if (JSON.stringify(rolledBack.tables) !== JSON.stringify(rollbackData.tables) || await schemaHash(rollbackTarget) !== rollbackSchema) {
  throw new Error("forced-failure batch did not fully roll back");
}
console.log(JSON.stringify({ database: rollbackTarget, startedAt: rollbackStartedAt, finishedAt: new Date().toISOString(), atomicRollback: true }));
