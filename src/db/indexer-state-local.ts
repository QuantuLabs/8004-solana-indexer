type LocalIndexerStateSqlClient = {
  $executeRawUnsafe?: (query: string, ...values: unknown[]) => Promise<unknown>;
};

type LocalIndexerStateUpdate = {
  signature: string;
  slot: bigint;
  txIndex?: number | null;
  source?: string | null;
  updatedAt: Date;
};

const SQLITE_INDEXER_STATE_UPSERT_SQL = `
  INSERT INTO "IndexerState" (
    "id",
    "lastSignature",
    "lastSlot",
    "lastTxIndex",
    "source",
    "updatedAt"
  )
  VALUES ('main', ?, CAST(? AS INTEGER), ?, ?, ?)
  ON CONFLICT("id") DO UPDATE SET
    "lastSignature" = excluded."lastSignature",
    "lastSlot" = excluded."lastSlot",
    "lastTxIndex" = excluded."lastTxIndex",
    "source" = excluded."source",
    "updatedAt" = excluded."updatedAt"
  WHERE "IndexerState"."lastSlot" IS NULL
     OR "IndexerState"."lastSlot" < excluded."lastSlot"
     OR (
       "IndexerState"."lastSlot" = excluded."lastSlot"
       AND (
         COALESCE("IndexerState"."lastTxIndex", -1)
           < COALESCE(excluded."lastTxIndex", -1)
         OR (
           COALESCE("IndexerState"."lastTxIndex", -1)
             = COALESCE(excluded."lastTxIndex", -1)
           AND COALESCE("IndexerState"."lastSignature", '')
             <= excluded."lastSignature"
         )
       )
     )
`;

function shouldUseLocalSqliteIndexerStateSql(): boolean {
  const databaseUrl = process.env.DATABASE_URL?.trim() ?? "";
  return databaseUrl.startsWith("file:");
}

export async function trySaveLocalIndexerStateWithSql(
  client: LocalIndexerStateSqlClient,
  update: LocalIndexerStateUpdate
): Promise<boolean> {
  if (!shouldUseLocalSqliteIndexerStateSql()) {
    return false;
  }

  if (typeof client.$executeRawUnsafe !== "function") {
    return false;
  }

  await client.$executeRawUnsafe(
    SQLITE_INDEXER_STATE_UPSERT_SQL,
    update.signature,
    update.slot.toString(),
    update.txIndex ?? null,
    update.source ?? "poller",
    update.updatedAt.toISOString()
  );

  return true;
}
