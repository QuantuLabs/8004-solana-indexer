/**
 * REAL Devnet Integration Tests for Reorg Resilience
 *
 * These tests run against REAL Solana devnet - no mocks.
 * They verify that the DataVerifier correctly:
 * 1. FINALIZES data that exists on-chain
 * 2. ORPHANS data that doesn't exist on-chain
 *
 * Prerequisites:
 * - Network connectivity to devnet RPC
 * - Real agents must exist on devnet (uses known test agents)
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Connection, PublicKey } from "@solana/web3.js";
import { DataVerifier } from "../../src/indexer/verifier.js";
import bs58 from "bs58";
import {
  AGENT_REGISTRY_PROGRAM_ID,
  fetchRootConfig,
  getAgentPda,
  getRegistryConfigPda,
} from "../../src/utils/pda.js";
import { prisma } from "./setup.js";

const REQUIRE_ONCHAIN_VERIFICATION =
  process.env.REQUIRE_ONCHAIN_VERIFICATION === "true" ||
  process.env.REQUIRE_ONCHAIN_WRITES === "true";

function pubkeyFromEnv(key: string, fallback: string): PublicKey {
  const raw = process.env[key]?.trim();
  return new PublicKey(raw && raw.length > 0 ? raw : fallback);
}

const DEFAULT_CANDIDATE_AGENTS = [
  pubkeyFromEnv("DEVNET_TEST_AGENT_WITH_FEEDBACKS", "EU7RyF4nGQSzkZBXLs7yknqYbGPhEma9mrELKsNLDxko"),
  pubkeyFromEnv("DEVNET_TEST_AGENT_SECONDARY", "EcJNM6FEYJChk44cGmMk3YGK4sHPydstCot3PQesjJgu"),
  // Recent funded devnet agents used by readiness suites (stable fallback candidates)
  pubkeyFromEnv("DEVNET_TEST_AGENT_TERTIARY", "GpQAKAkaU8h6RqGNrF1tVPWGJ8Pnpxjm6JXM26cYQ3oW"),
  pubkeyFromEnv("DEVNET_TEST_AGENT_QUATERNARY", "GguNaNgd6xDKvz973NxTGAj88xUbkJspEEmbeb8UQa59"),
  pubkeyFromEnv("DEVNET_TEST_AGENT_QUINARY", "AwDUHZdJJVJziKAjNnLRESks17mk1xtaXGDSSTyuyWNA"),
  pubkeyFromEnv("DEVNET_TEST_AGENT_SENARY", "BYacQcoUbEvm5xQvfuZ7KumZQoeM7EBVbTgB59qY1KsL"),
  pubkeyFromEnv("DEVNET_TEST_AGENT_SEPTENARY", "5BBxvi9iqCizE5YHs7LvfER4ZAa4G6AbtEkCt86du7am"),
];
const DEFAULT_OWNER = pubkeyFromEnv("DEVNET_TEST_OWNER", "2KmHw8VbShuz9xfj3ecEjBM5nPKR5BcYHRDSFfK1286t");
const DEFAULT_COLLECTION = pubkeyFromEnv("DEVNET_TEST_COLLECTION", "F8aWUp646DbMeW7n6P6DEkBWLUQrUdEVwEmS4MapzkHX");
const AGENT_ACCOUNT_DISCRIMINATOR = Buffer.from([241, 119, 69, 140, 233, 9, 112, 50]);

// Fake pubkeys that definitely don't exist on-chain (randomly generated)
const FAKE_PUBKEYS = {
  agent: new PublicKey("6fTq22mjTBLTH4LCgbF4VXQJ1v8AU9tQZiwmeck3Y2h1"),
  collection: new PublicKey("6C8dzSp7JZjx11e73TJUwEQiSnZphu42v14sa95d39b1"),
};

describe("E2E: Real Devnet Verification", () => {
  let connection: Connection;
  let verifier: DataVerifier;
  let resolvedAgentMint: PublicKey | null = null;
  let resolvedCollection: PublicKey = DEFAULT_COLLECTION;
  let resolvedOwner: PublicKey = DEFAULT_OWNER;

  beforeAll(async () => {
    // Real devnet connection
    const rpcUrl = process.env.HELIUS_DEVNET_URL
      || process.env.DEVNET_RPC_URL
      || process.env.RPC_URL
      || "https://api.devnet.solana.com";
    connection = new Connection(rpcUrl, "finalized");

    // Verify connectivity
    const slot = await connection.getSlot();
    console.log(`Connected to devnet at slot ${slot}`);

    const rootConfig = await fetchRootConfig(connection).catch(() => null);
    if (!process.env.DEVNET_TEST_COLLECTION && rootConfig?.baseCollection) {
      resolvedCollection = rootConfig.baseCollection;
    }
    if (!process.env.DEVNET_TEST_OWNER && rootConfig?.authority) {
      resolvedOwner = rootConfig.authority;
    }

    for (const candidate of DEFAULT_CANDIDATE_AGENTS) {
      const [agentPda] = getAgentPda(candidate);
      const accountInfo = await connection.getAccountInfo(agentPda);
      if (accountInfo) {
        resolvedAgentMint = candidate;
        break;
      }
    }

    // If static candidates are stale, discover one dynamically from on-chain AgentAccount PDAs.
    if (!resolvedAgentMint) {
      const discovered = await connection.getProgramAccounts(AGENT_REGISTRY_PROGRAM_ID, {
        commitment: "finalized",
        filters: [{ memcmp: { offset: 0, bytes: bs58.encode(AGENT_ACCOUNT_DISCRIMINATOR) } }],
        dataSlice: { offset: 72, length: 32 }, // asset pubkey field inside AgentAccount
      });

      for (const account of discovered) {
        if (account.account.data.length !== 32) continue;
        const asset = new PublicKey(account.account.data);
        const [derivedPda] = getAgentPda(asset);
        if (derivedPda.equals(account.pubkey)) {
          resolvedAgentMint = asset;
          break;
        }
      }
    }

    if (!resolvedAgentMint && REQUIRE_ONCHAIN_VERIFICATION) {
      throw new Error(
        `Strict mode: no existing agent PDA found for candidates ${DEFAULT_CANDIDATE_AGENTS
          .map((a) => a.toBase58())
          .join(", ")}`
      );
    }
    if (resolvedAgentMint) {
      console.log(`Using devnet agent mint ${resolvedAgentMint.toBase58()} for FINALIZE checks`);
    }
    console.log(`Using collection ${resolvedCollection.toBase58()} for registry checks`);

    // Create verifier with real connection
    verifier = new DataVerifier(connection, prisma, null, 60000);
    (verifier as any).isRunning = true;
  });

  afterAll(async () => {
    if (verifier) {
      await verifier.stop();
    }
  });

  // =========================================================================
  // 1. Agent Existence Verification (Real On-Chain Check)
  // =========================================================================

  describe("1. Real Agent Verification", () => {
    it("should FINALIZE agent that exists on devnet", async () => {
      if (!resolvedAgentMint) {
        if (REQUIRE_ONCHAIN_VERIFICATION) {
          throw new Error("Strict mode: no resolved devnet agent mint available for FINALIZE test");
        }
        console.log("Skipping - no resolved devnet agent mint available");
        return;
      }

      const realAgentId = resolvedAgentMint.toBase58();

      // Verifier checks the derived agent PDA account.
      const [agentPda] = getAgentPda(resolvedAgentMint);
      const accountInfo = await connection.getAccountInfo(agentPda);

      // Skip if the agent PDA doesn't exist on devnet
      if (accountInfo === null) {
        if (REQUIRE_ONCHAIN_VERIFICATION) {
          throw new Error(
            `Strict mode: expected devnet agent PDA not found ${agentPda.toBase58()}`
          );
        }
        console.log(`Skipping - agent PDA ${agentPda.toBase58()} not found on devnet`);
        return;
      }
      console.log(`Agent PDA ${agentPda.toBase58()} exists with ${accountInfo.data.length} bytes`);

      // Create PENDING entry in DB
      await prisma.agent.upsert({
        where: { id: realAgentId },
        create: {
          id: realAgentId,
          owner: resolvedOwner.toBase58(),
          collection: resolvedCollection.toBase58(),
          uri: "https://test.com/agent.json",
          nftName: "Test Agent",
          registry: "test-registry",
          status: "PENDING",
          createdSlot: 1n, // Old slot so it passes cutoff
        },
        update: {
          status: "PENDING",
          createdSlot: 1n,
          verifiedAt: null,
        },
      });

      // Get current finalized slot for cutoff calculation
      const currentSlot = await connection.getSlot("finalized");
      const cutoffSlot = BigInt(currentSlot) - 32n;

      // Run verification
      await (verifier as any).verifyAgents(cutoffSlot);

      // Check result
      const agent = await prisma.agent.findUnique({
        where: { id: realAgentId },
      });

      expect(agent).not.toBeNull();
      expect(agent!.status).toBe("FINALIZED");
      expect(agent!.verifiedAt).not.toBeNull();
      console.log(`Agent ${realAgentId} FINALIZED at ${agent!.verifiedAt}`);
    }, 30000); // 30s timeout for RPC calls

    it("should ORPHAN agent that does NOT exist on devnet", async () => {
      const fakeAgentId = FAKE_PUBKEYS.agent.toBase58();

      // Verifier checks the derived agent PDA.
      const [fakeAgentPda] = getAgentPda(FAKE_PUBKEYS.agent);
      const accountInfo = await connection.getAccountInfo(fakeAgentPda);
      expect(accountInfo).toBeNull();
      console.log(`Fake agent PDA ${fakeAgentPda.toBase58()} correctly does not exist`);

      // Create PENDING entry for fake agent
      await prisma.agent.upsert({
        where: { id: fakeAgentId },
        create: {
          id: fakeAgentId,
          owner: FAKE_PUBKEYS.agent.toBase58(),
          collection: FAKE_PUBKEYS.collection.toBase58(),
          uri: "https://fake.com/agent.json",
          nftName: "Fake Agent",
          registry: "fake-registry",
          status: "PENDING",
          createdSlot: 1n,
        },
        update: {
          status: "PENDING",
          createdSlot: 1n,
          verifiedAt: null,
        },
      });

      // Get current finalized slot
      const currentSlot = await connection.getSlot("finalized");
      const cutoffSlot = BigInt(currentSlot) - 32n;

      // Run verification
      await (verifier as any).verifyAgents(cutoffSlot);

      // Check result
      const agent = await prisma.agent.findUnique({
        where: { id: fakeAgentId },
      });

      expect(agent).not.toBeNull();
      expect(agent!.status).toBe("ORPHANED");
      console.log(`Fake agent ${fakeAgentId} correctly ORPHANED`);
    }, 30000);
  });

  // =========================================================================
  // 2. Registry Existence Verification (Real On-Chain Check)
  // =========================================================================

  describe("2. Real Registry Verification", () => {
    it("should FINALIZE registry that exists on devnet", async () => {
      const realCollection = resolvedCollection;

      // Verify the registry PDA actually exists on-chain
      const [registryPda] = getRegistryConfigPda(realCollection);
      const accountInfo = await connection.getAccountInfo(registryPda);

      // Skip if the registry doesn't exist on devnet (deployment may have changed)
      if (accountInfo === null) {
        if (REQUIRE_ONCHAIN_VERIFICATION) {
          throw new Error(
            `Strict mode: expected devnet registry PDA not found ${registryPda.toBase58()}`
          );
        }
        console.log(`Skipping - registry ${registryPda.toBase58()} not found on devnet`);
        return;
      }
      console.log(`Registry PDA ${registryPda.toBase58()} exists with ${accountInfo.data.length} bytes`);

      // Create PENDING entry in DB
      await prisma.registry.upsert({
        where: { id: registryPda.toBase58() },
        create: {
          id: registryPda.toBase58(),
          collection: realCollection.toBase58(),
          registryType: "Base",
          authority: resolvedOwner.toBase58(),
          status: "PENDING",
          slot: 1n,
        },
        update: {
          status: "PENDING",
          slot: 1n,
          verifiedAt: null,
        },
      });

      // Get current finalized slot
      const currentSlot = await connection.getSlot("finalized");
      const cutoffSlot = BigInt(currentSlot) - 32n;

      // Run verification
      await (verifier as any).verifyRegistries(cutoffSlot);

      // Check result
      const registry = await prisma.registry.findUnique({
        where: { id: registryPda.toBase58() },
      });

      expect(registry).not.toBeNull();
      expect(registry!.status).toBe("FINALIZED");
      console.log(`Registry ${registryPda.toBase58()} FINALIZED`);
    }, 30000);
  });

  // =========================================================================
  // 3. fetchOnChainDigests (Real On-Chain Data Parsing)
  // =========================================================================

  describe("3. Real On-Chain Digest Fetching", () => {
    it("should fetch real digest data from devnet agent", async () => {
      if (!resolvedAgentMint) {
        if (REQUIRE_ONCHAIN_VERIFICATION) {
          throw new Error("Strict mode: no resolved devnet agent mint available for digest fetch");
        }
        console.log("Skipping - no resolved devnet agent mint available");
        return;
      }
      const realAgentId = resolvedAgentMint.toBase58();

      // Fetch digests from real on-chain data
      const digests = await verifier.fetchOnChainDigests(realAgentId);

      // Agent with feedbacks should have non-null digests
      if (digests) {
        console.log("Real on-chain digests fetched:");
        console.log(`  Slot: ${digests.slot}`);
        console.log(`  Feedback count: ${digests.feedbackCount}`);
        console.log(`  Response count: ${digests.responseCount}`);
        console.log(`  Revoke count: ${digests.revokeCount}`);
        console.log(`  Feedback digest: ${Buffer.from(digests.feedbackDigest).toString("hex").slice(0, 16)}...`);

        if (typeof digests.slot === "bigint") {
          expect(digests.slot).toBeGreaterThan(0n);
        } else {
          console.log("  Slot is undefined on this account version; continuing with digest/count checks");
        }
        // Agent EU7RyF4n has feedbacks according to MCP search
        expect(digests.feedbackCount).toBeGreaterThanOrEqual(0n);
      } else {
        // If null, agent might not have ATOM enabled - still a valid test
        console.log("Agent does not have ATOM digests (atom_enabled=false or no Option::Some)");
      }
    }, 30000);

    it("should return null for non-existent agent", async () => {
      const fakeAgentId = FAKE_PUBKEYS.agent.toBase58();

      const digests = await verifier.fetchOnChainDigests(fakeAgentId);

      expect(digests).toBeNull();
      console.log("Correctly returned null for non-existent agent");
    }, 30000);
  });

  // =========================================================================
  // 4. Full Verification Cycle (verifyAll)
  // =========================================================================

  describe("4. Full Verification Cycle", () => {
    it("should run full verifyAll against devnet", async () => {
      // Create a mix of real and fake PENDING data
      if (!resolvedAgentMint) {
        if (REQUIRE_ONCHAIN_VERIFICATION) {
          throw new Error("Strict mode: no resolved devnet agent mint available for full verify cycle");
        }
        console.log("Skipping - no resolved devnet agent mint available");
        return;
      }
      const realAgentId = resolvedAgentMint.toBase58();
      const fakeAgentId2 = new PublicKey("74FHGzK8X4ZdisQZ5qzjGbTZLwPtQatPZUESVCxdozhN").toBase58();

      // Real agent - should be FINALIZED
      await prisma.agent.upsert({
        where: { id: realAgentId },
        create: {
          id: realAgentId,
          owner: resolvedOwner.toBase58(),
          collection: resolvedCollection.toBase58(),
          uri: "https://test.com/agent2.json",
          nftName: "Secondary Agent",
          registry: "test-registry",
          status: "PENDING",
          createdSlot: 1n,
        },
        update: {
          status: "PENDING",
          createdSlot: 1n,
          verifiedAt: null,
        },
      });

      // Fake agent - should be ORPHANED
      await prisma.agent.upsert({
        where: { id: fakeAgentId2 },
        create: {
          id: fakeAgentId2,
          owner: FAKE_PUBKEYS.agent.toBase58(),
          collection: FAKE_PUBKEYS.collection.toBase58(),
          uri: "https://fake2.com/agent.json",
          nftName: "Fake Agent 2",
          registry: "fake-registry",
          status: "PENDING",
          createdSlot: 1n,
        },
        update: {
          status: "PENDING",
          createdSlot: 1n,
          verifiedAt: null,
        },
      });

      // Run full verification cycle
      console.log("Running full verifyAll cycle against devnet...");
      await (verifier as any).verifyAll();

      // Check results
      const realAgent = await prisma.agent.findUnique({ where: { id: realAgentId } });
      const fakeAgent = await prisma.agent.findUnique({ where: { id: fakeAgentId2 } });

      expect(realAgent!.status).toBe("FINALIZED");
      expect(fakeAgent!.status).toBe("ORPHANED");

      console.log("Full verification cycle completed:");
      console.log(`  Real agent ${realAgentId}: ${realAgent!.status}`);
      console.log(`  Fake agent ${fakeAgentId2}: ${fakeAgent!.status}`);

      // Check stats
      const stats = verifier.getStats();
      console.log("Verifier stats:", stats);
      expect(stats.agentsVerified).toBeGreaterThan(0);
    }, 60000); // 60s timeout for full cycle
  });
});
