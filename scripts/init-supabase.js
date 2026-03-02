#!/usr/bin/env node
/**
 * Initialize Supabase database from supabase/schema.sql.
 * WARNING: destructive init-only reset.
 * Usage: node scripts/init-supabase.js
 */

import { promises as fs } from 'node:fs';
import path from 'node:path';
import readline from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
const { Client } = pg;

const DATABASE_URL = process.env.DATABASE_URL || process.env.SUPABASE_DSN;
const FORCE_SCHEMA_RESET = process.env.FORCE_SCHEMA_RESET === '1';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..');
const SCHEMA_PATH = path.join(REPO_ROOT, 'supabase', 'schema.sql');
const MIGRATIONS_DIR = path.join(REPO_ROOT, 'supabase', 'migrations');

const INDEXER_TABLES = [
  // Current schema
  'collections',
  'collection_pointers',
  'agents',
  'metadata',
  'feedbacks',
  'feedback_responses',
  'revocations',
  'validations',
  'atom_config',
  'id_counters',
  'indexer_state',
  'agent_digest_cache',
  // Legacy schema
  'Agent',
  'AgentMetadata',
  'Feedback',
  'FeedbackResponse',
  'Validation',
  'Registry',
  'IndexerState',
  'EventLog',
];

class UserAbortError extends Error {}

function abort(message) {
  throw new UserAbortError(message);
}

async function loadSchemaSql() {
  const sql = await fs.readFile(SCHEMA_PATH, 'utf8');
  if (!sql.trim()) {
    abort(`Schema file is empty: ${SCHEMA_PATH}`);
  }
  return sql;
}

async function loadMigrationFiles() {
  const entries = await fs.readdir(MIGRATIONS_DIR, { withFileTypes: true });
  const sqlFiles = entries
    .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.sql'))
    .map((entry) => entry.name)
    .sort();

  if (sqlFiles.length === 0) {
    abort('No SQL files found in supabase/migrations. Refusing destructive init.');
  }

  const first = sqlFiles[0];
  const last = sqlFiles[sqlFiles.length - 1];
  console.log(`📦 Found ${sqlFiles.length} migration SQL files (${first} -> ${last})`);
  return sqlFiles;
}

async function confirmDestructiveReset(existingTables) {
  if (FORCE_SCHEMA_RESET) {
    console.log('⚠️  FORCE_SCHEMA_RESET=1 set, skipping interactive confirmation.');
    return;
  }

  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    abort(
      `Existing indexer tables detected (${existingTables.join(', ')}) and no interactive TTY is available.\n` +
      '   Use supabase/migrations/*.sql for upgrades.\n' +
      '   To force reset in non-interactive runs, set FORCE_SCHEMA_RESET=1.'
    );
  }

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  try {
    const input = await rl.question(
      `⚠️  Existing indexer tables detected (${existingTables.join(', ')}).\n` +
      'Type RESET to continue destructive initialization from supabase/schema.sql: '
    );
    if (input.trim() !== 'RESET') {
      abort('Reset cancelled. Use supabase/migrations/*.sql for upgrades.');
    }
  } finally {
    rl.close();
  }
}

async function main() {
  let client = null;

  try {
    await loadMigrationFiles();

    if (!DATABASE_URL) {
      abort('DATABASE_URL or SUPABASE_DSN environment variable is required.');
    }

    const schemaSql = await loadSchemaSql();

    console.log('🔗 Connecting to Supabase...');
    client = new Client({ connectionString: DATABASE_URL });
    await client.connect();
    console.log('✅ Connected to Supabase');

    const tableCheck = await client.query(
      `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        AND table_name = ANY($1::text[])
      ORDER BY table_name;
      `,
      [INDEXER_TABLES]
    );

    const existingTables = tableCheck.rows.map((row) => row.table_name);
    if (existingTables.length > 0) {
      await confirmDestructiveReset(existingTables);
    }

    console.log('🧨 Applying supabase/schema.sql ...');
    await client.query('BEGIN');
    if (existingTables.length > 0 || FORCE_SCHEMA_RESET) {
      await client.query("SET LOCAL app.allow_destructive_schema_reset = 'on';");
    }
    await client.query(schemaSql);
    await client.query('COMMIT');
    console.log('✅ Database initialized from supabase/schema.sql');

  } catch (error) {
    if (client) {
      await client.query('ROLLBACK').catch(() => {});
    }
    console.error('❌ Initialization failed:', error.message);
    if (!(error instanceof UserAbortError) && error.stack) {
      console.error(error.stack);
    }
    process.exit(1);
  } finally {
    if (client) {
      await client.end().catch(() => {});
    }
  }
}

main();
