-- Fix SECURITY DEFINER views -> SECURITY INVOKER
-- Views should run with caller's permissions, not creator's

-- PostgreSQL 15+ syntax: security_invoker = true

ALTER VIEW metadata_decoded SET (security_invoker = true);
ALTER VIEW metadata_decoded_raw SET (security_invoker = true);
ALTER VIEW leaderboard SET (security_invoker = true);
ALTER VIEW collection_stats SET (security_invoker = true);
ALTER VIEW global_stats SET (security_invoker = true);
ALTER VIEW verification_stats SET (security_invoker = true);
