ALTER TABLE collection_pointers
ADD COLUMN IF NOT EXISTS last_seen_tx_index INTEGER;
