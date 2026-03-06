CREATE OR REPLACE FUNCTION alloc_gapless_id(p_scope TEXT, p_updated_at TIMESTAMPTZ DEFAULT NULL)
RETURNS BIGINT AS $$
DECLARE
  allocated BIGINT;
BEGIN
  INSERT INTO id_counters (scope, next_value, updated_at)
  VALUES (p_scope, 2, COALESCE(p_updated_at, NOW()))
  ON CONFLICT (scope) DO UPDATE
    SET next_value = id_counters.next_value + 1,
        updated_at = COALESCE(p_updated_at, NOW())
  RETURNING next_value - 1 INTO allocated;

  RETURN allocated;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_agent_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_agent_id BIGINT;
  existing_status TEXT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.agent_id IS NOT NULL THEN
    NEW.agent_id := OLD.agent_id;
    RETURN NEW;
  END IF;

  IF NEW.agent_id IS NOT NULL THEN
    NEW.agent_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended('agent:id:' || NEW.asset, 0));

  SELECT agent_id, status
  INTO existing_agent_id, existing_status
  FROM agents
  WHERE asset = NEW.asset
  LIMIT 1;

  IF FOUND THEN
    IF existing_agent_id IS NOT NULL THEN
      NEW.agent_id := existing_agent_id;
      RETURN NEW;
    END IF;

    IF existing_status IS NULL OR existing_status != 'ORPHANED' THEN
      NEW.agent_id := alloc_gapless_id('agent:global', COALESCE(NEW.created_at, NEW.updated_at, NOW()));
      UPDATE agents
      SET agent_id = NEW.agent_id
      WHERE asset = NEW.asset
        AND agent_id IS NULL
        AND (status IS NULL OR status != 'ORPHANED');
    END IF;

    RETURN NEW;
  END IF;

  NEW.agent_id := alloc_gapless_id('agent:global', COALESCE(NEW.created_at, NEW.updated_at, NOW()));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_feedback_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_feedback_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.feedback_id IS NOT NULL THEN
    NEW.feedback_id := OLD.feedback_id;
    RETURN NEW;
  END IF;

  IF NEW.feedback_id IS NOT NULL THEN
    NEW.feedback_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

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

  NEW.collection_id := alloc_gapless_id(
    'collection:global',
    COALESCE(NEW.first_seen_at, NEW.last_seen_at, NOW())
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
      'response:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text || ':' || NEW.responder || ':' || COALESCE(NEW.tx_signature, ''),
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
    AND tx_signature = NEW.tx_signature
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

CREATE OR REPLACE FUNCTION assign_revocation_id()
RETURNS TRIGGER AS $$
DECLARE
  existing_revocation_id BIGINT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.revocation_id IS NOT NULL THEN
    NEW.revocation_id := OLD.revocation_id;
    RETURN NEW;
  END IF;

  IF NEW.revocation_id IS NOT NULL THEN
    NEW.revocation_id := NULL;
  END IF;

  IF NEW.status IS NOT NULL AND NEW.status = 'ORPHANED' THEN
    RETURN NEW;
  END IF;

  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'revocation:id:' || NEW.asset || ':' || NEW.client_address || ':' || NEW.feedback_index::text,
      0
    )
  );

  SELECT revocation_id
  INTO existing_revocation_id
  FROM revocations
  WHERE asset = NEW.asset
    AND client_address = NEW.client_address
    AND feedback_index = NEW.feedback_index
  LIMIT 1;

  IF FOUND AND existing_revocation_id IS NOT NULL THEN
    NEW.revocation_id := existing_revocation_id;
    RETURN NEW;
  END IF;

  NEW.revocation_id := alloc_gapless_id('revocation:' || NEW.asset, COALESCE(NEW.created_at, NOW()));

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS alloc_gapless_id(TEXT);
