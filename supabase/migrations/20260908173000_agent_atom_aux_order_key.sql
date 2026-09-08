ALTER TABLE agents ADD COLUMN IF NOT EXISTS atom_aux_order_key TEXT;

WITH anchors AS (
  SELECT asset,
         LPAD(block_slot::text, 20, '0')
         || ':' || LPAD(COALESCE(tx_index, 2147483647)::text, 10, '0')
         || ':' || LPAD(COALESCE(event_ordinal, 2147483647)::text, 10, '0')
         || ':' || COALESCE(tx_signature, '') AS value
  FROM agents
  WHERE block_slot IS NOT NULL
  UNION ALL
  SELECT asset, mutation_order_key FROM agents WHERE mutation_order_key IS NOT NULL
  UNION ALL
  SELECT a.asset,
         LPAD(s.last_slot::text, 20, '0')
         || ':' || LPAD(COALESCE(s.last_tx_index, 2147483647)::text, 10, '0')
         || ':2147483647:' || s.last_signature
  FROM agents a
  JOIN indexer_state s ON s.id = 'main'
  WHERE s.last_slot IS NOT NULL AND s.last_signature IS NOT NULL
), floors AS (
  SELECT asset, MAX(value COLLATE "C") AS value FROM anchors GROUP BY asset
)
UPDATE agents a
SET atom_aux_order_key = COALESCE(a.atom_aux_order_key, f.value)
FROM floors f
WHERE f.asset = a.asset;
