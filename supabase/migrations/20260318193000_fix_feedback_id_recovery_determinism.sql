CREATE OR REPLACE FUNCTION assign_feedback_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_feedback_id BIGINT;
BEGIN
  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    NEW.feedback_id := NULL;
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE'
    AND current_setting('app.recanonicalize_feedback_ids', true) = 'on'
  THEN
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' AND OLD.feedback_id IS NOT NULL THEN
    NEW.feedback_id := OLD.feedback_id;
    RETURN NEW;
  END IF;

  -- Ignore caller-provided IDs and assign under DB lock instead.
  IF NEW.feedback_id IS NOT NULL THEN
    NEW.feedback_id := NULL;
  END IF;

  -- Serialize asset-scoped assignment so targeted re-canonicalization can safely
  -- run without racing new inserts for the same asset.
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

CREATE OR REPLACE FUNCTION recanonicalize_feedback_ids(p_asset TEXT)
RETURNS INTEGER AS $$
DECLARE
  affected_rows INTEGER := 0;
  next_feedback_id BIGINT := 1;
BEGIN
  IF p_asset IS NULL OR btrim(p_asset) = '' THEN
    RETURN 0;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended('feedback:asset:' || p_asset, 0)
  );

  UPDATE feedbacks
  SET feedback_id = NULL
  WHERE asset = p_asset
    AND status = 'ORPHANED'
    AND feedback_id IS NOT NULL;

  WITH ranked AS (
    SELECT
      id,
      ROW_NUMBER() OVER (
        ORDER BY block_slot ASC,
                 tx_index ASC NULLS LAST,
                 event_ordinal ASC NULLS LAST,
                 tx_signature ASC,
                 client_address ASC,
                 feedback_index ASC,
                 id ASC
      )::bigint AS canonical_feedback_id
    FROM feedbacks
    WHERE asset = p_asset
      AND status != 'ORPHANED'
  ),
  mismatched AS (
    SELECT
      r.id,
      r.canonical_feedback_id
    FROM ranked r
    JOIN feedbacks f ON f.id = r.id
    WHERE f.feedback_id IS DISTINCT FROM r.canonical_feedback_id
  )
  SELECT COUNT(*)::int
  INTO affected_rows
  FROM mismatched;

  IF affected_rows > 0 THEN
    PERFORM set_config('app.recanonicalize_feedback_ids', 'on', true);

    WITH ranked AS (
      SELECT
        id,
        ROW_NUMBER() OVER (
          ORDER BY block_slot ASC,
                   tx_index ASC NULLS LAST,
                   event_ordinal ASC NULLS LAST,
                   tx_signature ASC,
                   client_address ASC,
                   feedback_index ASC,
                   id ASC
        )::bigint AS canonical_feedback_id
      FROM feedbacks
      WHERE asset = p_asset
        AND status != 'ORPHANED'
    ),
    mismatched AS (
      SELECT
        r.id,
        r.canonical_feedback_id
      FROM ranked r
      JOIN feedbacks f ON f.id = r.id
      WHERE f.feedback_id IS DISTINCT FROM r.canonical_feedback_id
    )
    UPDATE feedbacks f
    SET feedback_id = -m.canonical_feedback_id
    FROM mismatched m
    WHERE f.id = m.id;

    UPDATE feedbacks
    SET feedback_id = ABS(feedback_id)
    WHERE asset = p_asset
      AND status != 'ORPHANED'
      AND feedback_id IS NOT NULL
      AND feedback_id < 0;

    PERFORM set_config('app.recanonicalize_feedback_ids', 'off', true);
  END IF;

  SELECT COALESCE(MAX(feedback_id) + 1, 1)
  INTO next_feedback_id
  FROM feedbacks
  WHERE asset = p_asset
    AND status != 'ORPHANED';

  INSERT INTO id_counters (scope, next_value, updated_at)
  VALUES ('feedback:' || p_asset, next_feedback_id, NOW())
  ON CONFLICT (scope) DO UPDATE
    SET next_value = EXCLUDED.next_value,
        updated_at = EXCLUDED.updated_at
    WHERE id_counters.next_value IS DISTINCT FROM EXCLUDED.next_value
       OR affected_rows > 0;

  RETURN affected_rows;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  asset_row RECORD;
BEGIN
  FOR asset_row IN
    WITH ranked AS (
      SELECT
        asset,
        feedback_id,
        ROW_NUMBER() OVER (
          PARTITION BY asset
          ORDER BY block_slot ASC,
                   tx_index ASC NULLS LAST,
                   event_ordinal ASC NULLS LAST,
                   tx_signature ASC,
                   client_address ASC,
                   feedback_index ASC,
                   id ASC
        )::bigint AS canonical_feedback_id
      FROM feedbacks
      WHERE status != 'ORPHANED'
    ),
    affected_assets AS (
      SELECT DISTINCT asset
      FROM feedbacks
      WHERE status = 'ORPHANED'
        AND feedback_id IS NOT NULL
      UNION
      SELECT DISTINCT asset
      FROM feedbacks
      WHERE status != 'ORPHANED'
        AND feedback_id IS NULL
      UNION
      SELECT DISTINCT asset
      FROM ranked
      WHERE feedback_id IS DISTINCT FROM canonical_feedback_id
    )
    SELECT asset
    FROM affected_assets
    ORDER BY asset ASC
  LOOP
    PERFORM recanonicalize_feedback_ids(asset_row.asset);
  END LOOP;
END;
$$;
