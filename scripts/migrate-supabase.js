#!/usr/bin/env node
/**
 * Legacy migration helper kept for backward compatibility.
 *
 * This script is intentionally blocked because it applies ad-hoc SQL that can
 * diverge from canonical schema/migration state.
 *
 * Use:
 *   - supabase/migrations/*.sql (ordered)
 *   - scripts/init-supabase.js only for destructive fresh init
 */

console.error('❌ scripts/migrate-supabase.js is deprecated and disabled.');
console.error('   Use ordered files in supabase/migrations/*.sql instead.');
console.error('   For destructive fresh init only, use scripts/init-supabase.js.');
process.exit(1);
