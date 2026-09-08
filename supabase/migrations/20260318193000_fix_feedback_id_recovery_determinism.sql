CREATE OR REPLACE FUNCTION assign_feedback_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_feedback_id BIGINT;
BEGIN
  -- An assigned public ID is immutable, including when a row becomes orphaned.
  IF TG_OP = 'UPDATE' AND OLD.feedback_id IS NOT NULL THEN
    NEW.feedback_id := OLD.feedback_id;
    RETURN NEW;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    NEW.feedback_id := NULL;
    RETURN NEW;
  END IF;

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.feedback_id IS NOT NULL THEN
    NEW.feedback_id := NULL;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended('feedback:asset:' || NEW.asset, 0)
  );

  -- Serialize by canonical feedback identity to make duplicate upserts idempotent.
  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'feedback:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text,
      0
    )
  );

  SELECT feedback_id
  INTO existing_feedback_id
  FROM feedbacks
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
  LIMIT 1;

  IF FOUND AND existing_feedback_id IS NOT NULL THEN
    NEW.feedback_id := existing_feedback_id;
    RETURN NEW;
  END IF;

  NEW.feedback_id := alloc_gapless_id('feedback:' || NEW.asset, COALESCE(NEW.created_at, NOW()));

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
