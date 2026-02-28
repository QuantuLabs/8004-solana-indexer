import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const schemaSql = readFileSync(resolve(__dirname, "../../../supabase/schema.sql"), "utf8");
const migrationSql = readFileSync(
  resolve(
    __dirname,
    "../../../supabase/migrations/20260228183000_fix_agent_id_sequence_conflicts.sql"
  ),
  "utf8"
);
const supabaseRuntime = readFileSync(resolve(__dirname, "../../../src/db/supabase.ts"), "utf8");
const batchProcessorRuntime = readFileSync(
  resolve(__dirname, "../../../src/indexer/batch-processor.ts"),
  "utf8"
);

type AgentStatus = "PENDING" | "ORPHANED";
type AgentRow = {
  agentId: bigint | null;
  status: AgentStatus;
};
type AgentUpsertState = {
  sequence: bigint;
  persistedAgents: Map<string, AgentRow>;
};

function applyAgentUpsertWithFixedSchemaSemantics(
  state: AgentUpsertState,
  asset: string,
  status: AgentStatus = "PENDING"
): {
  attemptedAgentId: bigint | null;
  persistedAgentId: bigint | null;
  conflict: boolean;
  repairedExisting: boolean;
} {
  const existing = state.persistedAgents.get(asset);
  let attemptedAgentId: bigint | null = null;
  let repairedExisting = false;

  // Mirrors trigger semantics: assign only for non-orphan inserts/upserts.
  if (status !== "ORPHANED") {
    if (existing !== undefined) {
      // Duplicate/replay conflict path: reuse existing assigned ID.
      if (existing.agentId !== null) {
        attemptedAgentId = existing.agentId;
      } else if (existing.status !== "ORPHANED") {
        // Legacy repair path for non-orphan rows with NULL agent_id.
        state.sequence += 1n;
        existing.agentId = state.sequence;
        attemptedAgentId = existing.agentId;
        repairedExisting = true;
      }
    } else {
      state.sequence += 1n;
      attemptedAgentId = state.sequence;
    }
  }

  if (existing !== undefined) {
    // Mirrors `ON CONFLICT (asset) DO UPDATE` without mutating status/agent_id.
    return { attemptedAgentId, persistedAgentId: existing.agentId, conflict: true, repairedExisting };
  }

  state.persistedAgents.set(asset, { agentId: attemptedAgentId, status });
  return { attemptedAgentId, persistedAgentId: attemptedAgentId, conflict: false, repairedExisting };
}

function assertAgentInsertContract(source: string): void {
  const insertClauses = [...source.matchAll(/INSERT INTO agents \(([^)]+)\)/g)];
  expect(insertClauses.length).toBeGreaterThan(0);
  for (const [, columns] of insertClauses) {
    expect(columns).not.toContain("agent_id");
  }

  expect(source).toContain("ON CONFLICT (asset) DO UPDATE SET");
  expect(source).not.toContain("agent_id =");
}

describe("agent_id conflict-safe trigger semantics for ON CONFLICT", () => {
  it("defines lock-guarded BEFORE INSERT assignment in bootstrap schema", () => {
    expect(schemaSql).toContain("CREATE SEQUENCE IF NOT EXISTS agent_id_seq START 1;");
    expect(schemaSql).toContain("CREATE OR REPLACE FUNCTION assign_agent_id()");
    expect(schemaSql).toContain("pg_advisory_xact_lock(hashtextextended(NEW.asset, 0));");
    expect(schemaSql).toContain("SELECT agent_id, status");
    expect(schemaSql).toContain("IF FOUND THEN");
    expect(schemaSql).toContain("SET agent_id = nextval('agent_id_seq')");
    expect(schemaSql).toContain("CREATE TRIGGER trg_assign_agent_id");
    expect(schemaSql).toContain("BEFORE INSERT ON agents");
  });

  it("applies the same trigger contract in live migration SQL", () => {
    expect(migrationSql).toContain("DROP TRIGGER IF EXISTS trg_assign_agent_id ON agents;");
    expect(migrationSql).toContain("CREATE SEQUENCE IF NOT EXISTS agent_id_seq START 1;");
    expect(migrationSql).toContain("CREATE OR REPLACE FUNCTION assign_agent_id()");
    expect(migrationSql).toContain("pg_advisory_xact_lock(hashtextextended(NEW.asset, 0));");
    expect(migrationSql).toContain("repair_pending AS");
    expect(migrationSql).toContain("ROW_NUMBER() OVER");
    expect(migrationSql).toContain(
      "SELECT setval('agent_id_seq', COALESCE((SELECT MAX(agent_id) FROM agents), 0));"
    );
  });

  it("upserts agents via ON CONFLICT(asset) without mutating agent_id in runtime SQL", () => {
    assertAgentInsertContract(supabaseRuntime);
    assertAgentInsertContract(batchProcessorRuntime);
  });

  it("does not burn sequence values when duplicate upserts hit ON CONFLICT", () => {
    const state: AgentUpsertState = {
      sequence: 0n,
      persistedAgents: new Map(),
    };

    const firstInsert = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-A");
    const conflictUpdate = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-A");
    const nextNewAsset = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-B");

    expect(firstInsert).toEqual({
      attemptedAgentId: 1n,
      persistedAgentId: 1n,
      conflict: false,
      repairedExisting: false,
    });

    expect(conflictUpdate).toEqual({
      attemptedAgentId: 1n,
      persistedAgentId: 1n,
      conflict: true,
      repairedExisting: false,
    });

    expect(nextNewAsset).toEqual({
      attemptedAgentId: 2n,
      persistedAgentId: 2n,
      conflict: false,
      repairedExisting: false,
    });

    expect([...state.persistedAgents.entries()]).toEqual([
      ["asset-A", { agentId: 1n, status: "PENDING" }],
      ["asset-B", { agentId: 2n, status: "PENDING" }],
    ]);
  });

  it("repairs legacy non-orphan rows missing agent_id on replay conflicts", () => {
    const state: AgentUpsertState = {
      sequence: 2n,
      persistedAgents: new Map<string, AgentRow>([
        ["asset-A", { agentId: 1n, status: "PENDING" }],
        ["asset-B", { agentId: 2n, status: "PENDING" }],
        ["asset-C", { agentId: null, status: "PENDING" }],
      ]),
    };

    const replayConflict = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-C");
    const nextNewAsset = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-D");

    expect(replayConflict).toEqual({
      attemptedAgentId: 3n,
      persistedAgentId: 3n,
      conflict: true,
      repairedExisting: true,
    });

    expect(nextNewAsset).toEqual({
      attemptedAgentId: 4n,
      persistedAgentId: 4n,
      conflict: false,
      repairedExisting: false,
    });

    expect(state.persistedAgents.get("asset-C")).toEqual({
      agentId: 3n,
      status: "PENDING",
    });
  });

  it("does not assign or burn IDs for orphaned rows during replay conflicts", () => {
    const state: AgentUpsertState = {
      sequence: 10n,
      persistedAgents: new Map<string, AgentRow>([["asset-Z", { agentId: null, status: "ORPHANED" }]]),
    };

    const replayConflict = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-Z");
    const newAsset = applyAgentUpsertWithFixedSchemaSemantics(state, "asset-N");

    expect(replayConflict).toEqual({
      attemptedAgentId: null,
      persistedAgentId: null,
      conflict: true,
      repairedExisting: false,
    });

    expect(newAsset).toEqual({
      attemptedAgentId: 11n,
      persistedAgentId: 11n,
      conflict: false,
      repairedExisting: false,
    });
  });
});
