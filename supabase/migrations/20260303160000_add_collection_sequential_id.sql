BEGIN;

ALTER TABLE collection_pointers
ADD COLUMN IF NOT EXISTS collection_id BIGINT;

ALTER TABLE collection_pointers
DROP CONSTRAINT IF EXISTS collection_pointers_collection_id_check;

ALTER TABLE collection_pointers
ADD CONSTRAINT collection_pointers_collection_id_check
CHECK (collection_id IS NULL OR collection_id >= 1);

WITH seed AS (
  SELECT COALESCE(MAX(collection_id), 0) AS max_id
  FROM collection_pointers
  WHERE collection_id IS NOT NULL
),
ranked AS (
  SELECT
    col,
    creator,
    ROW_NUMBER() OVER (
      ORDER BY
        first_seen_slot ASC NULLS LAST,
        first_seen_at ASC NULLS LAST,
        first_seen_tx_signature ASC NULLS LAST,
        col ASC,
        creator ASC
    ) AS rn
  FROM collection_pointers
  WHERE collection_id IS NULL
),
assigned AS (
  SELECT
    r.col,
    r.creator,
    (s.max_id + r.rn)::bigint AS collection_id
  FROM ranked r
  CROSS JOIN seed s
)
UPDATE collection_pointers cp
SET collection_id = a.collection_id
FROM assigned a
WHERE cp.col = a.col
  AND cp.creator = a.creator
  AND cp.collection_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_collection_pointers_collection_id
ON collection_pointers(collection_id)
WHERE collection_id IS NOT NULL;

DROP TRIGGER IF EXISTS trg_assign_collection_id ON collection_pointers;
DROP FUNCTION IF EXISTS assign_collection_id();

CREATE OR REPLACE FUNCTION assign_collection_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_collection_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.collection_id IS NOT NULL THEN
    NEW.collection_id := OLD.collection_id;
    RETURN NEW;
  END IF;

  IF NEW.collection_id IS NOT NULL THEN
    NEW.collection_id := NULL;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'collection:id:' || NEW.col || ':' || NEW.creator,
      0
    )
  );

  SELECT collection_id
  INTO existing_collection_id
  FROM collection_pointers
  WHERE col = NEW.col
    AND creator = NEW.creator
  LIMIT 1;

  IF FOUND AND existing_collection_id IS NOT NULL THEN
    NEW.collection_id := existing_collection_id;
    RETURN NEW;
  END IF;

  NEW.collection_id := alloc_gapless_id('collection:global');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_assign_collection_id
  BEFORE INSERT OR UPDATE ON collection_pointers
  FOR EACH ROW
  EXECUTE FUNCTION assign_collection_id();

INSERT INTO id_counters (scope, next_value, updated_at)
SELECT 'collection:global', COALESCE(MAX(collection_id), 0) + 1, NOW()
FROM collection_pointers
ON CONFLICT (scope) DO UPDATE
SET next_value = GREATEST(id_counters.next_value, EXCLUDED.next_value),
    updated_at = NOW();

COMMIT;
