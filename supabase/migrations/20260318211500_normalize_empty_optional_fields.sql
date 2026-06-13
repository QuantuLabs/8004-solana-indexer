UPDATE agents
SET agent_uri = NULL
WHERE agent_uri = '';

UPDATE feedback_responses
SET response_uri = NULL
WHERE response_uri = '';

UPDATE validations
SET request_uri = NULL
WHERE request_uri = '';

UPDATE validations
SET response_uri = NULL
WHERE response_uri = '';

UPDATE validations
SET tag = NULL
WHERE tag = '';
