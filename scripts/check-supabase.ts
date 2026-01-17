#!/usr/bin/env npx tsx
import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_KEY;

if (!url || !key) {
  console.log('Missing SUPABASE_URL or SUPABASE_KEY');
  process.exit(1);
}

const supabase = createClient(url, key);

async function main() {
  console.log('📊 Supabase Data Check\n');

  // Recent agents
  const { data: agents, error: agentErr } = await supabase
    .from('agents')
    .select('id, owner, feedback_count, quality_score')
    .order('created_at', { ascending: false })
    .limit(5);

  if (agentErr) {
    console.log('Agent error:', agentErr.message);
  } else {
    console.log('Recent agents:');
    agents?.forEach(a => {
      console.log(`  ${a.id.slice(0, 12)}... | feedbacks: ${a.feedback_count} | score: ${a.quality_score}`);
    });
  }

  // Recent feedbacks
  const { data: feedbacks, error: fbErr } = await supabase
    .from('feedbacks')
    .select('agent_id, score, tag1, tag2, client')
    .order('created_at', { ascending: false })
    .limit(5);

  if (fbErr) {
    console.log('\nFeedback error:', fbErr.message);
  } else {
    console.log('\nRecent feedbacks:');
    feedbacks?.forEach(f => {
      console.log(`  Agent: ${f.agent_id?.slice(0, 12)}... | Score: ${f.score} | Tags: ${f.tag1 || '-'}, ${f.tag2 || '-'}`);
    });
  }

  // Recent validations
  const { data: validations, error: valErr } = await supabase
    .from('validations')
    .select('agent_id, validator, nonce, response')
    .order('created_at', { ascending: false })
    .limit(5);

  if (valErr) {
    console.log('\nValidation error:', valErr.message);
  } else {
    console.log('\nRecent validations:');
    validations?.forEach(v => {
      console.log(`  Agent: ${v.agent_id?.slice(0, 12)}... | Validator: ${v.validator?.slice(0, 12)}... | Response: ${v.response || 'pending'}`);
    });
  }

  // Stats
  const { count: agentCount } = await supabase
    .from('agents')
    .select('*', { count: 'exact', head: true });

  const { count: fbCount } = await supabase
    .from('feedbacks')
    .select('*', { count: 'exact', head: true });

  const { count: valCount } = await supabase
    .from('validations')
    .select('*', { count: 'exact', head: true });

  console.log('\n📈 Stats:');
  console.log(`  Total agents: ${agentCount}`);
  console.log(`  Total feedbacks: ${fbCount}`);
  console.log(`  Total validations: ${valCount}`);
}

main().catch(console.error);
