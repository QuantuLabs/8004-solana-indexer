#!/usr/bin/env node
/**
 * Apply migration to Supabase production database
 * Usage: node scripts/migrate-supabase.js
 */

import pg from 'pg';
const { Client } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL environment variable is required');
  console.error('   Set it to your Supabase connection string');
  process.exit(1);
}

const migrationSQL = `
-- Migration: align Supabase schema with current indexer expectations

-- agents.atom_enabled flag (default true)
ALTER TABLE agents
  ADD COLUMN IF NOT EXISTS atom_enabled BOOLEAN DEFAULT TRUE;

-- feedbacks unique constraint by (asset, client_address, feedback_index)
CREATE UNIQUE INDEX IF NOT EXISTS feedbacks_asset_client_feedback_index_key
  ON feedbacks(asset, client_address, feedback_index);

-- feedback_responses dedupe by (asset, feedback_index, responder)
CREATE UNIQUE INDEX IF NOT EXISTS feedback_responses_asset_feedback_index_responder_key
  ON feedback_responses(asset, feedback_index, responder);

-- Verification
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('feedbacks', 'feedback_responses');
`;

async function main() {
  console.log('🔗 Connecting to Supabase...');
  const client = new Client({ connectionString: DATABASE_URL });

  try {
    await client.connect();
    console.log('✅ Connected to Supabase\n');

    console.log('📋 Applying migration...');
    console.log(migrationSQL);

    const result = await client.query(migrationSQL);

    console.log('\n✅ Migration applied successfully!');
    console.log('\n📊 Current indexes on Feedback table:');
    console.log(result.rows);

  } catch (error) {
    console.error('❌ Migration failed:', error.message);
    process.exit(1);
  } finally {
    await client.end();
  }
}

main();
