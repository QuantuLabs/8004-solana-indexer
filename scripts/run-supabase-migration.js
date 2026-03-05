#!/usr/bin/env node
/**
 * Legacy migration helper kept for backward compatibility.
 *
 * This script is intentionally blocked because it targets an old one-off
 * migration and can drift from canonical migration order.
 *
 * Use:
 *   - supabase/migrations/*.sql (ordered)
 *   - scripts/init-supabase.js only for destructive fresh init
 */

console.error('❌ scripts/run-supabase-migration.js is deprecated and disabled.');
console.error('   Use ordered files in supabase/migrations/*.sql instead.');
console.error('   For destructive fresh init only, use scripts/init-supabase.js.');
process.exit(1);
