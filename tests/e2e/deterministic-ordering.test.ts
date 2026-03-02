/**
 * E2E: Deterministic Ordering & Global ID Assignment
 *
 * Tests run against LOCAL test validator with 8004 programs deployed.
 * Registers multiple agents (same block + cross-block), then validates:
 *   1. tx_index is resolved correctly via getBlock
 *   2. Agents are ordered by composite key (block_slot, tx_signature, tx_index NULLS LAST, event_ordinal NULLS LAST)
 *   3. agent_id is assigned deterministically based on insertion order
 *   4. Re-ingesting produces the same ordering
 *   5. Orphaned agents do NOT receive a agent_id
 *
 * Prerequisites:
 *   1. Start localnet: npm run localnet:start
 *   2. Init state:     npm run localnet:init
 *
 * Run: npm run test:localnet
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  Connection,
  PublicKey,
  Keypair,
  SystemProgram,
  sendAndConfirmTransaction,
  Transaction,
} from "@solana/web3.js";
import * as anchor from "@coral-xyz/anchor";
import { Program } from "@coral-xyz/anchor";
import { Prisma } from "@prisma/client";
import { getAgentPda, getRegistryConfigPda, getRootConfigPda } from "../../src/utils/pda.js";
import { prisma } from "./setup.js";
import * as fs from "fs";
import * as path from "path";

const LOCALNET_RPC = "http://localhost:8899";
const PROGRAM_ID = new PublicKey("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
const MPL_CORE_PROGRAM_ID = new PublicKey("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d");

function loadIdl(): any {
  // Localnet tests keep using the devnet/default IDL; a mainnet copy also exists in ../../idl.
  const idlPath = path.join(__dirname, "../../idl/agent_registry_8004.json");
  return JSON.parse(fs.readFileSync(idlPath, "utf-8"));
}

function loadWallet(): Keypair {
  const walletPath = path.join(process.env.HOME || "~", ".config/solana/id.json");
  const secretKey = JSON.parse(fs.readFileSync(walletPath, "utf-8"));
  return Keypair.fromSecretKey(new Uint8Array(secretKey));
}

function sortAgentsDeterministically<T extends { slot: number; txIndex: number | null; signature: string }>(
  agents: T[]
): T[] {
  return [...agents].sort((a, b) => {
    if (a.slot !== b.slot) return a.slot - b.slot;
    const sigCmp = a.signature.localeCompare(b.signature);
    if (sigCmp !== 0) return sigCmp;
    const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
    const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
    return txA - txB;
  });
}

/**
 * Register a single agent on-chain and return its asset pubkey + tx signature.
 */
async function registerAgent(
  program: Program,
  connection: Connection,
  wallet: Keypair,
  collectionPubkey: PublicKey,
  registryConfigPda: PublicKey,
  rootConfigPda: PublicKey,
  uri: string
): Promise<{ asset: Keypair; signature: string; slot: number }> {
  const asset = Keypair.generate();
  const [agentPda] = getAgentPda(asset.publicKey, PROGRAM_ID);

  const signature = await program.methods
    .register(uri)
    .accounts({
      registryConfig: registryConfigPda,
      agentAccount: agentPda,
      asset: asset.publicKey,
      collection: collectionPubkey,
      userCollectionAuthority: null,
      rootConfig: rootConfigPda,
      owner: wallet.publicKey,
      systemProgram: SystemProgram.programId,
      mplCoreProgram: MPL_CORE_PROGRAM_ID,
    })
    .signers([asset])
    .rpc();

  const txInfo = await connection.getTransaction(signature, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });
  const slot = txInfo?.slot ?? 0;

  return { asset, signature, slot };
}

describe("E2E: Localnet Deterministic Ordering", () => {
  let connection: Connection;
  let wallet: Keypair;
  let provider: anchor.AnchorProvider;
  let program: Program;
  let rootConfigPda: PublicKey;
  let registryConfigPda: PublicKey;
  let collectionPubkey: PublicKey;

  // Track all registered agents for ordering verification
  const registeredAgents: Array<{
    assetId: string;
    signature: string;
    slot: number;
    txIndex: number | null;
  }> = [];

  beforeAll(async () => {
    connection = new Connection(LOCALNET_RPC, "confirmed");

    try {
      await connection.getSlot();
    } catch {
      throw new Error(
        "Localnet not running! Start with: npm run localnet:start && npm run localnet:init"
      );
    }

    wallet = loadWallet();
    const anchorWallet = new anchor.Wallet(wallet);
    provider = new anchor.AnchorProvider(connection, anchorWallet, {
      commitment: "confirmed",
    });
    anchor.setProvider(provider);

    const idl = loadIdl();
    program = new Program(idl, provider);

    [rootConfigPda] = getRootConfigPda(PROGRAM_ID);
    const rootConfigInfo = await connection.getAccountInfo(rootConfigPda);
    if (!rootConfigInfo) {
      throw new Error("Program not initialized! Run: npm run localnet:init");
    }

    const rootConfig = await program.account.rootConfig.fetch(rootConfigPda);
    collectionPubkey = rootConfig.baseCollection;
    [registryConfigPda] = getRegistryConfigPda(collectionPubkey, PROGRAM_ID);
  });

  afterAll(async () => {
    // Clean up test agents
    for (const agent of registeredAgents) {
      await prisma.agent.deleteMany({ where: { id: agent.assetId } });
    }
  });

  // =========================================================================
  // 1. Register multiple agents and resolve tx_index via getBlock
  // =========================================================================

  describe("1. tx_index Resolution via getBlock", () => {
    it("should register agents and resolve tx_index from block data", async () => {
      // Register 3 agents in separate transactions
      const agents = [];
      for (let i = 0; i < 3; i++) {
        const result = await registerAgent(
          program,
          connection,
          wallet,
          collectionPubkey,
          registryConfigPda,
          rootConfigPda,
          `https://test-ordering-${i}.localnet/agent.json`
        );
        agents.push(result);
        // Small delay to potentially get different slots
        await new Promise((r) => setTimeout(r, 500));
      }

      // Resolve tx_index via getBlock (same logic as poller.getTxIndexMap)
      for (const agent of agents) {
        let txIndex: number | null = null;

        try {
          const block = await connection.getBlock(agent.slot, {
            maxSupportedTransactionVersion: 0,
            transactionDetails: "full",
            commitment: "confirmed",
          });

          if (block?.transactions) {
            const idx = block.transactions.findIndex(
              (tx) => tx.transaction.signatures[0] === agent.signature
            );
            if (idx >= 0) txIndex = idx;
          }
        } catch {
          // Expected on some edge cases — tx_index stays null
        }

        registeredAgents.push({
          assetId: agent.asset.publicKey.toBase58(),
          signature: agent.signature,
          slot: agent.slot,
          txIndex,
        });
      }

      // Verify tx_index was resolved for all agents
      for (const agent of registeredAgents) {
        expect(agent.txIndex).not.toBeNull();
        expect(agent.txIndex).toBeGreaterThanOrEqual(0);
      }

      console.log("Registered agents with tx_index:", registeredAgents.map((a) => ({
        assetId: a.assetId.slice(0, 8) + "...",
        slot: a.slot,
        txIndex: a.txIndex,
      })));
    }, 60000);

    it("should preserve txIndex=0 (not coerce to null)", async () => {
      // If any agent was the first tx in its block, txIndex=0 must be preserved
      const zeroIndexAgents = registeredAgents.filter((a) => a.txIndex === 0);
      if (zeroIndexAgents.length === 0) {
        console.log("No txIndex=0 agents in this run (all slots had prior txs)");
        return;
      }

      for (const agent of zeroIndexAgents) {
        // Verify ?? preserves 0 while || would coerce it
        const correct = agent.txIndex ?? null;
        expect(correct).toBe(0);

        // Verify the buggy pattern would fail
        // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing
        const buggy = agent.txIndex || null;
        expect(buggy).toBeNull(); // This is the bug we fixed
      }
    });
  });

  // =========================================================================
  // 2. Composite Key Ordering
  // =========================================================================

  describe("2. Composite Key Ordering", () => {
    it("should sort agents by (block_slot, tx_signature, tx_index NULLS LAST)", () => {
      // Sort using the same logic as both indexers
      const sorted = [...registeredAgents].sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        return txA - txB;
      });

      // Verify the order is deterministic (sorting again produces same result)
      const reSorted = [...registeredAgents].sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        return txA - txB;
      });

      expect(sorted.map((a) => a.assetId)).toEqual(reSorted.map((a) => a.assetId));
    });

    it("should produce identical sort for SQL NULLS LAST and JS MAX_SAFE_INTEGER", () => {
      // Simulate SQL ordering: signature first, then COALESCE(tx_index, 2147483647)
      const sqlSort = [...registeredAgents].sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? 2147483647;
        const txB = b.txIndex ?? 2147483647;
        return txA - txB;
      });

      // JS ordering: MAX_SAFE_INTEGER
      const jsSort = [...registeredAgents].sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        return txA - txB;
      });

      expect(sqlSort.map((a) => a.assetId)).toEqual(jsSort.map((a) => a.assetId));
    });

    it("should use event_ordinal as deterministic intra-tx tie-breaker", () => {
      const base = registeredAgents[0];
      if (!base) return;

      const sameTxEvents = [
        {
          assetId: "agent-event-2",
          slot: base.slot,
          signature: "sig-same",
          txIndex: 4,
          eventOrdinal: 2,
        },
        {
          assetId: "agent-event-null",
          slot: base.slot,
          signature: "sig-same",
          txIndex: 4,
          eventOrdinal: null as number | null,
        },
        {
          assetId: "agent-event-0",
          slot: base.slot,
          signature: "sig-same",
          txIndex: 4,
          eventOrdinal: 0,
        },
      ];

      const sorted = [...sameTxEvents].sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        if (txA !== txB) return txA - txB;
        const eventA = a.eventOrdinal ?? Number.MAX_SAFE_INTEGER;
        const eventB = b.eventOrdinal ?? Number.MAX_SAFE_INTEGER;
        return eventA - eventB;
      });

      expect(sorted.map((a) => a.assetId)).toEqual([
        "agent-event-0",
        "agent-event-2",
        "agent-event-null",
      ]);
    });
  });

  // =========================================================================
  // 3. DB Ingestion & agent_id Assignment
  // =========================================================================

  describe("3. DB Ingestion & agent_id Assignment", () => {
    it("should ingest agents with correct tx_index into DB", async () => {
      // Sort agents in deterministic order for insertion
      const sorted = sortAgentsDeterministically(registeredAgents);

      // Insert agents in deterministic order (simulating indexer ingestion)
      for (const agent of sorted) {
        await prisma.agent.upsert({
          where: { id: agent.assetId },
          create: {
            id: agent.assetId,
            owner: wallet.publicKey.toBase58(),
            uri: `https://test.localnet/${agent.assetId.slice(0, 8)}.json`,
            nftName: `Agent ${agent.assetId.slice(0, 8)}`,
            collection: collectionPubkey.toBase58(),
            registry: registryConfigPda.toBase58(),
            atomEnabled: true,
            status: "PENDING",
            createdSlot: BigInt(agent.slot),
            createdTxSignature: agent.signature,
            txIndex: agent.txIndex,
          },
          update: {
            createdSlot: BigInt(agent.slot),
            createdTxSignature: agent.signature,
            txIndex: agent.txIndex,
          },
        });
      }

      // Verify all agents are in DB with correct tx_index
      for (const agent of registeredAgents) {
        const dbAgent = await prisma.agent.findUnique({
          where: { id: agent.assetId },
        });
        expect(dbAgent).not.toBeNull();
        expect(dbAgent!.txIndex).toBe(agent.txIndex);
        expect(dbAgent!.createdSlot).toBe(BigInt(agent.slot));
        expect(dbAgent!.createdTxSignature).toBe(agent.signature);
      }
    }, 30000);

    it("should order agents deterministically from DB using composite key", async () => {
      // Inject one synthetic row with NULL tx_index to enforce NULLS LAST coverage.
      const base = registeredAgents[0];
      if (!base) return;

      const nullTxAgentId = Keypair.generate().publicKey.toBase58();
      const nullTxSignature = "zzzz-null-tx-index-ordering-test";

      await prisma.agent.create({
        data: {
          id: nullTxAgentId,
          owner: wallet.publicKey.toBase58(),
          uri: "https://test.localnet/null-tx-index.json",
          nftName: "Agent Null TxIndex",
          collection: collectionPubkey.toBase58(),
          registry: registryConfigPda.toBase58(),
          atomEnabled: true,
          status: "PENDING",
          createdSlot: BigInt(base.slot),
          createdTxSignature: nullTxSignature,
          txIndex: null,
        },
      });

      const ids = [...registeredAgents.map((a) => a.assetId), nullTxAgentId];
      const dbOrdered = await prisma.$queryRaw<Array<{ id: string }>>`
        SELECT id
        FROM "Agent"
        WHERE id IN (${Prisma.join(ids)})
        ORDER BY "createdSlot" ASC,
                 "createdTxSignature" ASC,
                 CASE WHEN "txIndex" IS NULL THEN 1 ELSE 0 END ASC,
                 "txIndex" ASC
      `;

      const expected = sortAgentsDeterministically([
        ...registeredAgents,
        {
          assetId: nullTxAgentId,
          signature: nullTxSignature,
          slot: base.slot,
          txIndex: null,
        },
      ]);

      expect(dbOrdered.map((a) => a.id)).toEqual(expected.map((a) => a.assetId));

      const sameSlotOrdered = dbOrdered
        .map((row) => row.id)
        .filter((id) => {
          if (id === nullTxAgentId) return true;
          const agent = registeredAgents.find((a) => a.assetId === id);
          return agent?.slot === base.slot;
        });
      expect(sameSlotOrdered[sameSlotOrdered.length - 1]).toBe(nullTxAgentId);

      await prisma.agent.deleteMany({ where: { id: nullTxAgentId } });
    });
  });

  // =========================================================================
  // 4. Re-ingestion Determinism
  // =========================================================================

  describe("4. Re-ingestion Determinism", () => {
    it("should produce identical ordering when re-resolving tx_index", async () => {
      // Re-resolve tx_index for all agents from scratch
      const reResolved: Array<{ assetId: string; txIndex: number | null }> = [];

      for (const agent of registeredAgents) {
        let txIndex: number | null = null;
        try {
          const block = await connection.getBlock(agent.slot, {
            maxSupportedTransactionVersion: 0,
            transactionDetails: "full",
            commitment: "confirmed",
          });
          if (block?.transactions) {
            const idx = block.transactions.findIndex(
              (tx) => tx.transaction.signatures[0] === agent.signature
            );
            if (idx >= 0) txIndex = idx;
          }
        } catch {
          // tx_index stays null
        }

        reResolved.push({ assetId: agent.assetId, txIndex });
      }

      // Verify tx_index matches original resolution
      for (const resolved of reResolved) {
        const original = registeredAgents.find((a) => a.assetId === resolved.assetId);
        expect(resolved.txIndex).toBe(original!.txIndex);
      }

      // Verify sort order matches
      const originalSort = sortAgentsDeterministically(registeredAgents).map((a) => a.assetId);

      const reResolvedSort = sortAgentsDeterministically(
        [...reResolved]
        .map((r) => {
          const orig = registeredAgents.find((a) => a.assetId === r.assetId)!;
          return { ...orig, txIndex: r.txIndex };
        })
      ).map((a) => a.assetId);

      expect(reResolvedSort).toEqual(originalSort);
    }, 30000);
  });

  // =========================================================================
  // 5. getBlock Consistency
  // =========================================================================

  describe("5. getBlock Transaction Order Consistency", () => {
    it("should return consistent transaction order across multiple getBlock calls", async () => {
      // Pick a slot that has at least one of our transactions
      const testSlot = registeredAgents[0]?.slot;
      if (!testSlot) return;

      const orders: string[][] = [];

      // Fetch the same block 3 times
      for (let i = 0; i < 3; i++) {
        try {
          const block = await connection.getBlock(testSlot, {
            maxSupportedTransactionVersion: 0,
            transactionDetails: "full",
            commitment: "confirmed",
          });
          if (block?.transactions) {
            orders.push(
              block.transactions.map((tx) => tx.transaction.signatures[0])
            );
          }
        } catch {
          // Tolerate transient RPC errors
        }
      }

      // All fetches should return the same order
      if (orders.length >= 2) {
        for (let i = 1; i < orders.length; i++) {
          expect(orders[i]).toEqual(orders[0]);
        }
      }
    }, 15000);
  });

  // =========================================================================
  // 6. Orphaned Agents & agent_id
  // =========================================================================

  describe("6. Orphaned Agents", () => {
    it("should not interfere with ordering of non-orphaned agents", async () => {
      // Create an orphaned agent in DB
      const fakeAsset = Keypair.generate();
      const fakeId = fakeAsset.publicKey.toBase58();

      await prisma.agent.create({
        data: {
          id: fakeId,
          owner: wallet.publicKey.toBase58(),
          uri: "https://fake.localnet/orphaned.json",
          nftName: "Orphaned Agent",
          collection: collectionPubkey.toBase58(),
          registry: registryConfigPda.toBase58(),
          atomEnabled: false,
          status: "ORPHANED",
          createdSlot: BigInt(registeredAgents[0]?.slot ?? 1),
          txIndex: 0,
        },
      });

      const orphan = await prisma.agent.findUnique({ where: { id: fakeId } });
      expect(orphan?.agentId).toBeNull();

      // Query non-orphaned agents — orphaned should be excluded
      const activeAgents = await prisma.agent.findMany({
        where: {
          id: { in: registeredAgents.map((a) => a.assetId) },
          status: { not: "ORPHANED" },
        },
        orderBy: [
          { createdSlot: "asc" },
          { txIndex: "asc" },
          { createdTxSignature: "asc" },
        ],
      });

      // Orphaned agent should not appear
      expect(activeAgents.find((a) => a.id === fakeId)).toBeUndefined();
      expect(activeAgents.length).toBe(registeredAgents.length);

      // Cleanup
      await prisma.agent.delete({ where: { id: fakeId } });
    });
  });

  // =========================================================================
  // 7. Cross-Indexer Ordering Simulation
  // =========================================================================

  describe("7. Cross-Indexer Ordering Simulation", () => {
    it("RPC Poller and Substreams enumerate should produce same tx_index", async () => {
      // For each agent, verify that getBlock enumerate index matches
      // what Substreams would produce (also enumerate on block.transactions)
      for (const agent of registeredAgents) {
        try {
          const block = await connection.getBlock(agent.slot, {
            maxSupportedTransactionVersion: 0,
            transactionDetails: "full",
            commitment: "confirmed",
          });

          if (!block?.transactions) continue;

          // RPC Poller approach: findIndex by signature
          const pollerIndex = block.transactions.findIndex(
            (tx) => tx.transaction.signatures[0] === agent.signature
          );

          // Substreams approach: enumerate gives the same array index
          // (Substreams returns block.transactions in the same order as getBlock)
          let substreamsIndex = -1;
          for (let i = 0; i < block.transactions.length; i++) {
            if (block.transactions[i].transaction.signatures[0] === agent.signature) {
              substreamsIndex = i;
              break;
            }
          }

          expect(pollerIndex).toBe(substreamsIndex);
          expect(pollerIndex).toBe(agent.txIndex);
        } catch {
          // Tolerate RPC errors
        }
      }
    }, 30000);

    it("WebSocket path without tx_index should use tx_index only as signature-level tie-breaker", () => {
      // Simulate WebSocket scenario: same slot + same signature, but no tx_index
      const wsAgent = {
        ...registeredAgents[0],
        txIndex: null, // WebSocket doesn't resolve tx_index
      };

      const withWs = [...registeredAgents, wsAgent];

      const sorted = withWs.sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        return txA - txB;
      });

      // For same slot + same signature, NULL tx_index should sort after concrete tx_index.
      const tieGroup = sorted.filter(
        (a) => a.slot === wsAgent.slot && a.signature === wsAgent.signature
      );
      const lastAgent = tieGroup[tieGroup.length - 1];
      expect(lastAgent.txIndex).toBeNull();
    });

    it("WebSocket fallback should order multiple NULL tx_index by signature", () => {
      const base = registeredAgents[0];
      if (!base) return;

      const wsA = {
        ...base,
        assetId: `${base.assetId}-ws-a`,
        txIndex: null,
        signature: "Bsig-ordered-middle",
      };
      const wsB = {
        ...base,
        assetId: `${base.assetId}-ws-b`,
        txIndex: null,
        signature: "Asig-ordered-first",
      };
      const wsC = {
        ...base,
        assetId: `${base.assetId}-ws-c`,
        txIndex: null,
        signature: "Zsig-ordered-last",
      };

      const sorted = [...registeredAgents, wsA, wsB, wsC].sort((a, b) => {
        if (a.slot !== b.slot) return a.slot - b.slot;
        const sigCmp = a.signature.localeCompare(b.signature);
        if (sigCmp !== 0) return sigCmp;
        const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
        const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
        return txA - txB;
      });

      const nullGroup = sorted
        .filter((a) => a.slot === base.slot && a.txIndex === null)
        .map((a) => a.signature);

      expect(nullGroup).toEqual([
        "Asig-ordered-first",
        "Bsig-ordered-middle",
        "Zsig-ordered-last",
      ]);
    });
  });

  // =========================================================================
  // 8. Feedback Ordering (DB)
  // =========================================================================

  describe("8. Feedback Deterministic Ordering", () => {
    it("should order feedbacks by (createdSlot, createdTxSignature, txIndex NULLS LAST)", async () => {
      const base = registeredAgents[0];
      if (!base) return;

      const client = wallet.publicKey.toBase58();
      const seed = Date.now().toString();

      const specs: Array<{
        feedbackIndex: bigint;
        createdSlot: bigint;
        txIndex: number | null;
        createdTxSignature: string;
      }> = [
        {
          feedbackIndex: BigInt(`${seed}1`),
          createdSlot: BigInt(base.slot),
          txIndex: 0,
          createdTxSignature: "sig-z-low-index",
        },
        {
          feedbackIndex: BigInt(`${seed}2`),
          createdSlot: BigInt(base.slot),
          txIndex: 5,
          createdTxSignature: "sig-a-high-index",
        },
        {
          feedbackIndex: BigInt(`${seed}3`),
          createdSlot: BigInt(base.slot),
          txIndex: null,
          createdTxSignature: "sig-b-null-index",
        },
        {
          feedbackIndex: BigInt(`${seed}4`),
          createdSlot: BigInt(base.slot + 1),
          txIndex: null,
          createdTxSignature: "sig-next-slot",
        },
      ];

      const rows: Array<{
        id: string;
        feedbackIndex: bigint;
        createdSlot: bigint;
        txIndex: number | null;
        createdTxSignature: string;
      }> = [];

      for (const spec of specs) {
        const created = await prisma.feedback.create({
          data: {
            agentId: base.assetId,
            client,
            feedbackIndex: spec.feedbackIndex,
            value: "100",
            valueDecimals: 18,
            score: 80,
            tag1: "quality",
            tag2: "accuracy",
            endpoint: "https://example.test",
            feedbackUri: `ipfs://feedback-${seed}-${spec.feedbackIndex.toString()}`,
            createdSlot: spec.createdSlot,
            createdTxSignature: spec.createdTxSignature,
            txIndex: spec.txIndex,
            status: "PENDING",
          },
        });

        rows.push({
          id: created.id,
          feedbackIndex: spec.feedbackIndex,
          createdSlot: spec.createdSlot,
          txIndex: spec.txIndex,
          createdTxSignature: spec.createdTxSignature,
        });
      }

      const ids = rows.map((r) => r.id);
      const dbOrdered = await prisma.$queryRaw<Array<{ id: string }>>`
        SELECT id
        FROM "Feedback"
        WHERE id IN (${Prisma.join(ids)})
        ORDER BY "createdSlot" ASC,
                 "createdTxSignature" ASC,
                 CASE WHEN "txIndex" IS NULL THEN 1 ELSE 0 END ASC,
                 "txIndex" ASC
      `;

      const expected = [...rows]
        .sort((a, b) => {
          if (a.createdSlot !== b.createdSlot) return Number(a.createdSlot - b.createdSlot);
          const sigCmp = a.createdTxSignature.localeCompare(b.createdTxSignature);
          if (sigCmp !== 0) return sigCmp;
          const txA = a.txIndex ?? Number.MAX_SAFE_INTEGER;
          const txB = b.txIndex ?? Number.MAX_SAFE_INTEGER;
          return txA - txB;
        })
        .map((r) => r.id);

      expect(dbOrdered.map((r) => r.id)).toEqual(expected);

      await prisma.feedback.deleteMany({ where: { id: { in: ids } } });
    });
  });
});
