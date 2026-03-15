import type { PrismaClient } from "@prisma/client";

type SqlExecutor = Pick<PrismaClient, "$executeRawUnsafe" | "$queryRawUnsafe">;

const REFRESH_AGENT_COUNTER_SQL = `
  INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
  VALUES (
    'agent:global',
    COALESCE((SELECT MAX("agent_id") FROM "Agent"), 0) + 1,
    CURRENT_TIMESTAMP
  )
  ON CONFLICT("scope") DO UPDATE SET
    "nextValue" = CASE
      WHEN excluded."nextValue" > "IdCounter"."nextValue" THEN excluded."nextValue"
      ELSE "IdCounter"."nextValue"
    END,
    "updatedAt" = CURRENT_TIMESTAMP
`;

const REPAIR_AGENT_IDS_SQL = `
  WITH "agent_ranked" AS (
    SELECT
      a."id" AS "id",
      CAST(
        ROW_NUMBER() OVER (
          ORDER BY
            COALESCE(a."createdSlot", 9223372036854775807) ASC,
            COALESCE(a."txIndex", 2147483647) ASC,
            COALESCE(a."eventOrdinal", 2147483647) ASC,
            CASE WHEN a."createdTxSignature" IS NULL THEN 1 ELSE 0 END ASC,
            COALESCE(a."createdTxSignature", '') ASC,
            a."id" ASC
        ) AS BIGINT
      ) AS "assignedId"
    FROM "Agent" a
    WHERE a."status" IS NULL OR a."status" != 'ORPHANED'
  ),
  "agent_updates" AS (
    SELECT
      a."id" AS "id",
      ranked."assignedId" AS "assignedId"
    FROM "Agent" a
    INNER JOIN "agent_ranked" ranked ON ranked."id" = a."id"
    WHERE a."agent_id" IS NULL
       OR CAST(a."agent_id" AS BIGINT) != ranked."assignedId"
  )
  UPDATE "Agent"
  SET "agent_id" = (
    SELECT u."assignedId"
    FROM "agent_updates" u
    WHERE u."id" = "Agent"."id"
  )
  WHERE "Agent"."id" IN (
    SELECT u."id"
    FROM "agent_updates" u
  )
`;

export const MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE =
  "Missing collection_id schema in local database. Apply Prisma migrations (prisma migrate deploy) before starting the indexer.";

const LOCAL_COLLECTION_ID_SCHEMA_REPAIR_STATEMENTS = [
  `
    CREATE TABLE IF NOT EXISTS "IdCounter" (
      "scope" TEXT NOT NULL PRIMARY KEY,
      "nextValue" BIGINT NOT NULL,
      "updatedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `,
  `
    INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
    VALUES (
      'collection:global',
      COALESCE((SELECT MAX("collection_id") FROM "CollectionPointer"), 0) + 1,
      CURRENT_TIMESTAMP
    )
    ON CONFLICT("scope") DO UPDATE SET
      "nextValue" = CASE
        WHEN excluded."nextValue" > "IdCounter"."nextValue" THEN excluded."nextValue"
        ELSE "IdCounter"."nextValue"
      END,
      "updatedAt" = CURRENT_TIMESTAMP
  `,
  `
    INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
    VALUES (
      'agent:global',
      COALESCE((SELECT MAX("agent_id") FROM "Agent"), 0) + 1,
      CURRENT_TIMESTAMP
    )
    ON CONFLICT("scope") DO UPDATE SET
      "nextValue" = CASE
        WHEN excluded."nextValue" > "IdCounter"."nextValue" THEN excluded."nextValue"
        ELSE "IdCounter"."nextValue"
      END,
      "updatedAt" = CURRENT_TIMESTAMP
  `,
  `DROP TRIGGER IF EXISTS "CollectionPointer_assign_collection_id_after_insert"`,
  `DROP TRIGGER IF EXISTS "CollectionPointer_assign_collection_id_after_update"`,
  `DROP TRIGGER IF EXISTS "Agent_assign_agent_id_after_insert"`,
  `DROP TRIGGER IF EXISTS "Agent_assign_agent_id_after_update"`,
  `
    CREATE TRIGGER "CollectionPointer_assign_collection_id_after_insert"
    AFTER INSERT ON "CollectionPointer"
    FOR EACH ROW
    WHEN NEW."collection_id" IS NULL
    BEGIN
      INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
      VALUES ('collection:global', 2, CURRENT_TIMESTAMP)
      ON CONFLICT("scope") DO UPDATE SET
        "nextValue" = "IdCounter"."nextValue" + 1,
        "updatedAt" = CURRENT_TIMESTAMP;

      UPDATE "CollectionPointer"
      SET "collection_id" = (
        SELECT "nextValue" - 1
        FROM "IdCounter"
        WHERE "scope" = 'collection:global'
      )
      WHERE "col" = NEW."col"
        AND "creator" = NEW."creator"
        AND "collection_id" IS NULL;
    END
  `,
  `
    CREATE TRIGGER "CollectionPointer_assign_collection_id_after_update"
    AFTER UPDATE ON "CollectionPointer"
    FOR EACH ROW
    WHEN NEW."collection_id" IS NULL
    BEGIN
      INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
      VALUES ('collection:global', 2, CURRENT_TIMESTAMP)
      ON CONFLICT("scope") DO UPDATE SET
        "nextValue" = "IdCounter"."nextValue" + 1,
        "updatedAt" = CURRENT_TIMESTAMP;

      UPDATE "CollectionPointer"
      SET "collection_id" = (
        SELECT "nextValue" - 1
        FROM "IdCounter"
        WHERE "scope" = 'collection:global'
      )
      WHERE "col" = NEW."col"
        AND "creator" = NEW."creator"
        AND "collection_id" IS NULL;
    END
  `,
  `
    UPDATE "CollectionPointer"
    SET "collection_id" = "collection_id"
    WHERE "collection_id" IS NULL
  `,
  REPAIR_AGENT_IDS_SQL,
  REFRESH_AGENT_COUNTER_SQL,
  `
    CREATE TRIGGER "Agent_assign_agent_id_after_insert"
    AFTER INSERT ON "Agent"
    FOR EACH ROW
    WHEN NEW."agent_id" IS NULL
      AND (NEW."status" IS NULL OR NEW."status" != 'ORPHANED')
    BEGIN
      INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
      VALUES ('agent:global', 2, CURRENT_TIMESTAMP)
      ON CONFLICT("scope") DO UPDATE SET
        "nextValue" = "IdCounter"."nextValue" + 1,
        "updatedAt" = CURRENT_TIMESTAMP;

      UPDATE "Agent"
      SET "agent_id" = (
        SELECT "nextValue" - 1
        FROM "IdCounter"
        WHERE "scope" = 'agent:global'
      )
      WHERE "id" = NEW."id"
        AND "agent_id" IS NULL
        AND ("status" IS NULL OR "status" != 'ORPHANED');
    END
  `,
  `
    CREATE TRIGGER "Agent_assign_agent_id_after_update"
    AFTER UPDATE ON "Agent"
    FOR EACH ROW
    WHEN NEW."agent_id" IS NULL
      AND (NEW."status" IS NULL OR NEW."status" != 'ORPHANED')
    BEGIN
      INSERT INTO "IdCounter" ("scope", "nextValue", "updatedAt")
      VALUES ('agent:global', 2, CURRENT_TIMESTAMP)
      ON CONFLICT("scope") DO UPDATE SET
        "nextValue" = "IdCounter"."nextValue" + 1,
        "updatedAt" = CURRENT_TIMESTAMP;

      UPDATE "Agent"
      SET "agent_id" = (
        SELECT "nextValue" - 1
        FROM "IdCounter"
        WHERE "scope" = 'agent:global'
      )
      WHERE "id" = NEW."id"
        AND "agent_id" IS NULL
        AND ("status" IS NULL OR "status" != 'ORPHANED');
    END
  `,
];

export function isMissingCollectionIdSchemaError(error: unknown): boolean {
  const maybe = error as { code?: unknown; message?: unknown } | null;
  const code = typeof maybe?.code === "string" ? maybe.code : "";
  const message = typeof maybe?.message === "string" ? maybe.message : String(error);
  const missingSchemaPattern = /missing collection_id schema|column .*collection_id|column .*lastSeenTxIndex|column .*agent_id|column .*nextValue|no such column: collection_id|no such column: lastSeenTxIndex|no such column: agent_id|no such column: nextValue|no such table: IdCounter|has no column named collection_id|has no column named lastSeenTxIndex|has no column named agent_id|has no column named nextValue|column "?collection_id"? does not exist|column "?lastSeenTxIndex"? does not exist|column "?agent_id"? does not exist|column "?nextValue"? does not exist|Unknown arg .*collectionId|Unknown arg .*lastSeenTxIndex|Unknown arg .*agentId|IdCounter/i;
  if (code === "P2022") {
    return /collection_id|collectionId|lastSeenTxIndex|agent_id|agentId|CollectionPointer|Agent/i.test(message);
  }
  if (code === "P2010") {
    return missingSchemaPattern.test(message);
  }
  return missingSchemaPattern.test(message);
}

export async function repairLocalCollectionIdSchema(prisma: SqlExecutor): Promise<void> {
  for (const statement of LOCAL_COLLECTION_ID_SCHEMA_REPAIR_STATEMENTS) {
    await prisma.$executeRawUnsafe(statement);
  }
}

async function assertSqliteColumns(
  prisma: SqlExecutor,
  tableName: string,
  requiredColumns: string[]
): Promise<void> {
  const rows = (await prisma.$queryRawUnsafe(
    `PRAGMA table_info("${tableName}")`
  )) as Array<{ name?: string }>;
  const columns = new Set(
    rows
      .map((row) => (typeof row.name === "string" ? row.name : ""))
      .filter((name) => name.length > 0)
  );
  const missing = requiredColumns.filter((column) => !columns.has(column));
  if (missing.length > 0) {
    throw new Error(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
  }
}

export async function assertLocalCollectionIdSchema(prisma: SqlExecutor): Promise<void> {
  try {
    await assertSqliteColumns(prisma, "CollectionPointer", ["collection_id", "lastSeenTxIndex"]);
    await assertSqliteColumns(prisma, "Agent", ["agent_id"]);
    const collectionCounterRows = (await prisma.$queryRawUnsafe(`
      SELECT "nextValue"
      FROM "IdCounter"
      WHERE "scope" = 'collection:global'
      LIMIT 1
    `)) as Array<{ nextValue?: number | bigint }>;
    const agentCounterRows = (await prisma.$queryRawUnsafe(`
      SELECT "nextValue"
      FROM "IdCounter"
      WHERE "scope" = 'agent:global'
      LIMIT 1
    `)) as Array<{ nextValue?: number | bigint }>;
    if (collectionCounterRows.length === 0 || agentCounterRows.length === 0) {
      throw new Error(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
    }
    const triggerRows = (await prisma.$queryRawUnsafe(`
      SELECT COUNT(*) AS count
      FROM sqlite_master
      WHERE type = 'trigger'
        AND name IN (
          'CollectionPointer_assign_collection_id_after_insert',
          'CollectionPointer_assign_collection_id_after_update',
          'Agent_assign_agent_id_after_insert',
          'Agent_assign_agent_id_after_update'
        )
    `)) as Array<{ count?: number | bigint }>;
    const triggerCount = Number(triggerRows[0]?.count ?? 0);
    if (triggerCount !== 4) {
      throw new Error(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
    }
  } catch (error) {
    if (isMissingCollectionIdSchemaError(error)) {
      throw new Error(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
    }
    throw error;
  }
}
