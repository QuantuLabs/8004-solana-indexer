ALTER TABLE indexer_state
ADD COLUMN IF NOT EXISTS last_tx_index INTEGER;
