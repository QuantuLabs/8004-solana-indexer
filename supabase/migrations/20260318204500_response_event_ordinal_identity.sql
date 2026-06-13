ALTER TABLE feedback_responses ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;
ALTER TABLE orphan_responses ADD COLUMN IF NOT EXISTS event_ordinal INTEGER;

DO $$
DECLARE
  constraint_row RECORD;
BEGIN
  FOR constraint_row IN
    SELECT c.conname
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = current_schema()
      AND r.relname = 'feedback_responses'
      AND c.contype = 'u'
      AND pg_get_constraintdef(c.oid) = 'UNIQUE (asset, client_address, feedback_index, responder, tx_signature)'
  LOOP
    EXECUTE format('ALTER TABLE feedback_responses DROP CONSTRAINT %I', constraint_row.conname);
  END LOOP;

  FOR constraint_row IN
    SELECT c.conname
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = current_schema()
      AND r.relname = 'orphan_responses'
      AND c.contype = 'u'
      AND pg_get_constraintdef(c.oid) = 'UNIQUE (asset, client_address, feedback_index, responder, tx_signature)'
  LOOP
    EXECUTE format('ALTER TABLE orphan_responses DROP CONSTRAINT %I', constraint_row.conname);
  END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_feedback_responses_response_event_identity
ON feedback_responses(asset, client_address, feedback_index, responder, tx_signature, (COALESCE(event_ordinal, -1)));

CREATE UNIQUE INDEX IF NOT EXISTS idx_orphan_responses_response_event_identity
ON orphan_responses(asset, client_address, feedback_index, responder, tx_signature, (COALESCE(event_ordinal, -1)));

CREATE OR REPLACE FUNCTION assign_response_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_response_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.response_id IS NOT NULL THEN
    NEW.response_id := OLD.response_id;
    RETURN NEW;
  END IF;

  IF NEW.response_id IS NOT NULL THEN
    NEW.response_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'response:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text || ':' || NEW.responder || ':' || COALESCE(NEW.tx_signature, '') || ':' || COALESCE(NEW.event_ordinal, -1)::text,
      0
    )
  );

  SELECT response_id
  INTO existing_response_id
  FROM feedback_responses
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
    AND responder = NEW.responder
    AND tx_signature IS NOT DISTINCT FROM NEW.tx_signature
    AND COALESCE(event_ordinal, -1) = COALESCE(NEW.event_ordinal, -1)
  LIMIT 1;

  IF FOUND AND existing_response_id IS NOT NULL THEN
    NEW.response_id := existing_response_id;
    RETURN NEW;
  END IF;

  NEW.response_id := alloc_gapless_id(
    'response:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text,
    COALESCE(NEW.created_at, NOW())
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
