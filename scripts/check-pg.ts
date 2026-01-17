#!/usr/bin/env npx tsx
import 'dotenv/config';
import pg from 'pg';

const dsn = process.env.SUPABASE_DSN;

if (!dsn) {
  console.log('Missing SUPABASE_DSN');
  console.log('Set it in .env or run: export SUPABASE_DSN="postgresql://..."');
  process.exit(1);
}

async function main() {
  console.log('📊 Database Check\n');

  const pool = new pg.Pool({ connectionString: dsn });

  try {
    // Check schema first
    const schemaRes = await pool.query(`
      SELECT column_name FROM information_schema.columns WHERE table_name = 'agents'
    `);
    console.log('Agents columns:', schemaRes.rows.map((r: any) => r.column_name).join(', '));

    // Recent agents
    const agentsRes = await pool.query(`
      SELECT asset, owner, feedback_count, quality_score, trust_tier
      FROM agents
      ORDER BY created_at DESC
      LIMIT 5
    `);

    console.log('\nRecent agents:');
    agentsRes.rows.forEach((a: any) => {
      console.log(`  ${a.asset.slice(0, 20)}... | feedbacks: ${a.feedback_count} | score: ${a.quality_score} | tier: ${a.trust_tier}`);
    });

    // Check feedbacks schema
    const fbSchema = await pool.query(`
      SELECT column_name FROM information_schema.columns WHERE table_name = 'feedbacks'
    `);
    console.log('\nFeedbacks columns:', fbSchema.rows.map((r: any) => r.column_name).join(', '));

    // Recent feedbacks
    const fbRes = await pool.query(`
      SELECT asset, score, tag1, tag2
      FROM feedbacks
      ORDER BY created_at DESC
      LIMIT 5
    `);

    console.log('\nRecent feedbacks:');
    if (fbRes.rows.length === 0) {
      console.log('  (no feedbacks found)');
    } else {
      fbRes.rows.forEach((f: any) => {
        console.log(`  Agent: ${f.asset?.slice(0, 20)}... | Score: ${f.score} | Tags: ${f.tag1 || '-'}, ${f.tag2 || '-'}`);
      });
    }

    // Recent validations
    const valRes = await pool.query(`
      SELECT asset, validator, nonce, response
      FROM validations
      ORDER BY created_at DESC
      LIMIT 5
    `);

    console.log('\nRecent validations:');
    valRes.rows.forEach((v: any) => {
      console.log(`  Agent: ${v.asset?.slice(0, 20)}... | Validator: ${v.validator?.slice(0, 20)}... | Nonce: ${v.nonce} | Response: ${v.response || 'pending'}`);
    });

    // Stats
    const stats = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM agents) as agents,
        (SELECT COUNT(*) FROM feedbacks) as feedbacks,
        (SELECT COUNT(*) FROM validations) as validations
    `);

    console.log('\n📈 Stats:');
    console.log(`  Total agents: ${stats.rows[0].agents}`);
    console.log(`  Total feedbacks: ${stats.rows[0].feedbacks}`);
    console.log(`  Total validations: ${stats.rows[0].validations}`);

  } finally {
    await pool.end();
  }
}

main().catch(console.error);
