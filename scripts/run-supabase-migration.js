#!/usr/bin/env node
/**
 * Run Supabase migration: Add client_address to feedback_responses
 * Usage: node scripts/run-supabase-migration.js
 */

import "dotenv/config";
import { Pool } from 'pg';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function runMigration() {
  console.log('🚀 Starting Supabase migration: Add client_address to feedback_responses\n');

  // Read DATABASE_URL from environment
  const databaseUrl = process.env.SUPABASE_DSN || process.env.DATABASE_URL;

  if (!databaseUrl) {
    console.error('❌ Error: DATABASE_URL or SUPABASE_DSN not found in environment');
    console.error('   Set one of these variables to your Supabase connection string');
    process.exit(1);
  }

  const sslModeRaw = (process.env.PGSSLMODE || process.env.SUPABASE_SSL || "").toLowerCase();
  const sslDisabled =
    sslModeRaw === "disable" ||
    sslModeRaw === "false" ||
    sslModeRaw === "0" ||
    databaseUrl.includes("sslmode=disable");

  const createPool = (forceNoSsl = false) =>
    new Pool({
      connectionString: databaseUrl,
      ssl: forceNoSsl || sslDisabled ? false : { rejectUnauthorized: false },
    });

  let pool = createPool();

  try {
    // Read migration file
    const migrationPath = join(__dirname, '../supabase/migrations/20260119_add_client_address_to_responses.sql');
    const migrationSQL = readFileSync(migrationPath, 'utf-8');

    console.log('📄 Migration file loaded:', migrationPath);
    console.log('\n--- Migration SQL ---');
    console.log(migrationSQL);
    console.log('--- End SQL ---\n');

    // Ask for confirmation
    console.log('⚠️  This will modify the feedback_responses table in production.');
    console.log('   Press Ctrl+C to cancel, or wait 5 seconds to continue...\n');

    await new Promise(resolve => setTimeout(resolve, 5000));

    console.log('▶️  Executing migration...\n');

    // Execute migration
    try {
      await pool.query(migrationSQL);
    } catch (error) {
      if (String(error?.message || "").includes("does not support SSL")) {
        await pool.end();
        pool = createPool(true);
        await pool.query(migrationSQL);
      } else {
        throw error;
      }
    }

    console.log('✅ Migration completed successfully!\n');

    // Verify
    const result = await pool.query(`
      SELECT
        COUNT(*) as total_responses,
        COUNT(client_address) as responses_with_client
      FROM feedback_responses
    `);

    console.log('📊 Verification:');
    console.log('   Total responses:', result.rows[0].total_responses);
    console.log('   With client_address:', result.rows[0].responses_with_client);

  } catch (error) {
    console.error('\n❌ Migration failed:', error.message);
    console.error(error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

runMigration();
