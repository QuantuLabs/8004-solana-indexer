UPDATE "Validation"
SET "requestUri" = NULL
WHERE "requestUri" = '';

UPDATE "Validation"
SET "responseUri" = NULL
WHERE "responseUri" = '';

UPDATE "Validation"
SET "tag" = NULL
WHERE "tag" = '';
