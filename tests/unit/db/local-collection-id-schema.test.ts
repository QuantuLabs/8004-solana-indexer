import { describe, expect, it, vi } from "vitest";
import {
  MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE,
  assertLocalCollectionIdSchema,
  repairLocalCollectionIdSchema,
} from "../../../src/db/local-collection-id-schema.js";

describe("local collection_id schema helpers", () => {
  it("accepts a database with both counters and all runtime triggers present", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce([{ name: "collection_id" }, { name: "lastSeenTxIndex" }])
      .mockResolvedValueOnce([{ name: "agent_id" }])
      .mockResolvedValueOnce([{ nextValue: 7 }])
      .mockResolvedValueOnce([{ nextValue: 18 }])
      .mockResolvedValueOnce([{ count: 4 }]);

    await expect(
      assertLocalCollectionIdSchema({
        $queryRawUnsafe: query,
        $executeRawUnsafe: vi.fn(),
      } as never)
    ).resolves.toBeUndefined();
  });

  it("raises the fatal message when IdCounter is missing", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce([{ name: "collection_id" }, { name: "lastSeenTxIndex" }])
      .mockResolvedValueOnce([{ name: "agent_id" }])
      .mockRejectedValueOnce(new Error("no such table: IdCounter"));

    await expect(
      assertLocalCollectionIdSchema({
        $queryRawUnsafe: query,
        $executeRawUnsafe: vi.fn(),
      } as never)
    ).rejects.toThrow(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
  });

  it("raises the fatal message when collection:global counter seed is missing", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce([{ name: "collection_id" }, { name: "lastSeenTxIndex" }])
      .mockResolvedValueOnce([{ name: "agent_id" }])
      .mockResolvedValueOnce([]);

    await expect(
      assertLocalCollectionIdSchema({
        $queryRawUnsafe: query,
        $executeRawUnsafe: vi.fn(),
      } as never)
    ).rejects.toThrow(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
  });

  it("raises the fatal message when agent:global counter seed is missing", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce([{ name: "collection_id" }, { name: "lastSeenTxIndex" }])
      .mockResolvedValueOnce([{ name: "agent_id" }])
      .mockResolvedValueOnce([{ nextValue: 7 }])
      .mockResolvedValueOnce([]);

    await expect(
      assertLocalCollectionIdSchema({
        $queryRawUnsafe: query,
        $executeRawUnsafe: vi.fn(),
      } as never)
    ).rejects.toThrow(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
  });

  it("applies the idempotent repair statements for migration-only SQLite objects", async () => {
    const exec = vi.fn().mockResolvedValue(undefined);

    await repairLocalCollectionIdSchema({
      $queryRawUnsafe: vi.fn(),
      $executeRawUnsafe: exec,
    } as never);

    expect(exec).toHaveBeenCalledTimes(14);
    expect(exec.mock.calls[0][0]).toContain('CREATE TABLE IF NOT EXISTS "IdCounter"');
    expect(exec.mock.calls[2][0]).toContain("'agent:global'");
    expect(exec.mock.calls[3][0]).toContain('DROP TRIGGER IF EXISTS "CollectionPointer_assign_collection_id_after_insert"');
    expect(exec.mock.calls[7][0]).toContain('CREATE TRIGGER "CollectionPointer_assign_collection_id_after_insert"');
    expect(exec.mock.calls[8][0]).toContain('CREATE TRIGGER "CollectionPointer_assign_collection_id_after_update"');
    expect(exec.mock.calls[9][0]).toContain('UPDATE "CollectionPointer"');
    expect(exec.mock.calls[10][0]).toContain('WITH "agent_ranked" AS');
    expect(exec.mock.calls[10][0]).toContain('a."agent_id" IS NULL');
    expect(exec.mock.calls[10][0]).toContain(`a."status" != 'ORPHANED'`);
    expect(exec.mock.calls[11][0]).toContain("'agent:global'");
    expect(exec.mock.calls[12][0]).toContain('CREATE TRIGGER "Agent_assign_agent_id_after_insert"');
    expect(exec.mock.calls[12][0]).toContain(`NEW."status" != 'ORPHANED'`);
    expect(exec.mock.calls[13][0]).toContain('CREATE TRIGGER "Agent_assign_agent_id_after_update"');
    expect(exec.mock.calls[13][0]).toContain(`NEW."status" != 'ORPHANED'`);
  });

  it("raises the fatal message when PRAGMA shows a missing required column", async () => {
    const query = vi.fn()
      .mockResolvedValueOnce([{ name: "collection_id" }]);

    await expect(
      assertLocalCollectionIdSchema({
        $queryRawUnsafe: query,
        $executeRawUnsafe: vi.fn(),
      } as never)
    ).rejects.toThrow(MISSING_COLLECTION_ID_SCHEMA_FATAL_MESSAGE);
  });
});
