ALTER TABLE agents ADD COLUMN IF NOT EXISTS mutation_order_key TEXT;
ALTER TABLE metadata ADD COLUMN IF NOT EXISTS mutation_order_key TEXT;

UPDATE agents
SET mutation_order_key =
  LPAD(block_slot::text, 20, '0')
  || ':' || LPAD(COALESCE(tx_index, 2147483647)::text, 10, '0')
  || ':' || LPAD(COALESCE(event_ordinal, 2147483647)::text, 10, '0')
  || ':' || COALESCE(tx_signature, '')
WHERE mutation_order_key IS NULL;

UPDATE metadata
SET mutation_order_key =
  LPAD(block_slot::text, 20, '0')
  || ':' || LPAD(COALESCE(tx_index, 2147483647)::text, 10, '0')
  || ':' || LPAD(COALESCE(event_ordinal, 2147483647)::text, 10, '0')
  || ':' || COALESCE(tx_signature, '')
WHERE mutation_order_key IS NULL;
