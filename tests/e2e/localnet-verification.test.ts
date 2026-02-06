/**
 * REAL Localnet Integration Tests for Reorg Resilience
 *
 * Tests run against LOCAL test validator with 8004 programs deployed.
 * Creates REAL on-chain data and verifies the DataVerifier correctly:
 * 1. FINALIZES data that exists on-chain
 * 2. ORPHANS data that doesn't exist on-chain
 *
 * Prerequisites:
 * 1. Start localnet: cd ../8004-solana && anchor test --skip-test --detach
 * 2. Programs must be initialized (run init-localnet.ts first)
 *
 * Run: npm run test:localnet
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { Connection, PublicKey, Keypair, SystemProgram, LAMPORTS_PER_SOL } from "@solana/web3.js";
import * as anchor from "@coral-xyz/anchor";
import { Program, BN } from "@coral-xyz/anchor";
import { DataVerifier } from "../../src/indexer/verifier.js";
import { getAgentPda, getRegistryConfigPda, getRootConfigPda } from "../../src/utils/pda.js";
import { prisma } from "./setup.js";
import * as fs from "fs";
import * as path from "path";

// Localnet RPC
const LOCALNET_RPC = "http://localhost:8899";
const PROGRAM_ID = new PublicKey("8oo48pya1SZD23ZhzoNMhxR2UGb8BRa41Su4qP9EuaWm");
const MPL_CORE_PROGRAM_ID = new PublicKey("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d");

// Load IDL
function loadIdl(): any {
  const idlPath = path.join(__dirname, "../../idl/agent_registry_8004.json");
  return JSON.parse(fs.readFileSync(idlPath, "utf-8"));
}

// Load wallet from default Solana config
function loadWallet(): Keypair {
  const walletPath = path.join(process.env.HOME || "~", ".config/solana/id.json");
  const secretKey = JSON.parse(fs.readFileSync(walletPath, "utf-8"));
  return Keypair.fromSecretKey(new Uint8Array(secretKey));
}

// Wait for an account to exist at finalized commitment
async function waitForFinalization(
  connection: Connection,
  pubkey: PublicKey,
  timeoutMs = 30000
): Promise<boolean> {
  const startTime = Date.now();
  while (Date.now() - startTime < timeoutMs) {
    const info = await connection.getAccountInfo(pubkey, { commitment: "finalized" });
    if (info !== null) {
      return true;
    }
    // Poll every 500ms
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  return false;
}

describe("E2E: Localnet Verification", () => {
  let connection: Connection;
  let verifier: DataVerifier;
  let wallet: Keypair;
  let provider: anchor.AnchorProvider;
  let program: Program;

  // PDAs
  let rootConfigPda: PublicKey;
  let registryConfigPda: PublicKey;
  let collectionPubkey: PublicKey;
  let registryAuthorityPda: PublicKey;

  // Test data
  let testAsset: Keypair;
  let testAgent: { id: string; pda: PublicKey };

  beforeAll(async () => {
    // Connect to localnet
    connection = new Connection(LOCALNET_RPC, "confirmed");

    // Check localnet is running
    try {
      const slot = await connection.getSlot();
      console.log(`Connected to localnet at slot ${slot}`);
    } catch (e) {
      throw new Error(
        "Localnet not running! Start with: cd ../8004-solana && anchor test --skip-test --detach"
      );
    }

    // Load wallet
    wallet = loadWallet();
    console.log(`Wallet: ${wallet.publicKey.toBase58()}`);

    // Setup Anchor provider
    const anchorWallet = new anchor.Wallet(wallet);
    provider = new anchor.AnchorProvider(connection, anchorWallet, {
      commitment: "confirmed",
    });
    anchor.setProvider(provider);

    // Load program
    const idl = loadIdl();
    program = new Program(idl, provider);

    // Get PDAs
    [rootConfigPda] = getRootConfigPda(PROGRAM_ID);

    // Check if program is initialized
    const rootConfigInfo = await connection.getAccountInfo(rootConfigPda);
    if (!rootConfigInfo) {
      throw new Error(
        "Program not initialized! Run: cd ../8004-solana && anchor run full"
      );
    }

    // Fetch root config to get registry details
    const rootConfig = await program.account.rootConfig.fetch(rootConfigPda);
    registryConfigPda = rootConfig.baseRegistry;

    const registryConfig = await program.account.registryConfig.fetch(registryConfigPda);
    collectionPubkey = registryConfig.collection;

    // Registry authority PDA
    [registryAuthorityPda] = PublicKey.findProgramAddressSync(
      [Buffer.from("registry_authority"), collectionPubkey.toBuffer()],
      PROGRAM_ID
    );

    console.log("Root Config:", rootConfigPda.toBase58());
    console.log("Registry Config:", registryConfigPda.toBase58());
    console.log("Collection:", collectionPubkey.toBase58());

    // Create verifier with real localnet connection
    verifier = new DataVerifier(connection, prisma, null, 60000);
    (verifier as any).isRunning = true;
  });

  afterAll(async () => {
    if (verifier) {
      await verifier.stop();
    }
  });

  // =========================================================================
  // 1. Real Agent Registration & Verification
  // =========================================================================

  describe("1. Real Agent Registration & Verification", () => {
    it("should register agent on-chain and verify it gets FINALIZED", async () => {
      // Generate new asset keypair
      testAsset = Keypair.generate();
      const [agentPda] = getAgentPda(testAsset.publicKey, PROGRAM_ID);

      console.log("\n=== Registering Agent ===");
      console.log("Asset:", testAsset.publicKey.toBase58());
      console.log("Agent PDA:", agentPda.toBase58());

      // Register agent on-chain using correct method signature
      const agentUri = "https://test.localnet/agent.json";

      try {
        const tx = await program.methods
          .register(agentUri)
          .accounts({
            registryConfig: registryConfigPda,
            agentAccount: agentPda,
            asset: testAsset.publicKey,
            collection: collectionPubkey,
            userCollectionAuthority: null, // Not needed for base registry
            rootConfig: rootConfigPda,
            owner: wallet.publicKey,
            systemProgram: SystemProgram.programId,
            mplCoreProgram: MPL_CORE_PROGRAM_ID,
          })
          .signers([testAsset])
          .rpc();

        console.log("Register tx:", tx);
      } catch (e: any) {
        console.error("Registration failed:", e.message);
        throw e;
      }

      // Verify agent PDA exists on-chain
      const agentAccount = await connection.getAccountInfo(agentPda);
      expect(agentAccount).not.toBeNull();
      console.log("Agent PDA exists with", agentAccount!.data.length, "bytes");

      // Verify Core asset exists at confirmed commitment
      const assetAccountConfirmed = await connection.getAccountInfo(testAsset.publicKey, { commitment: "confirmed" });
      console.log("Asset at confirmed:", assetAccountConfirmed ? `${assetAccountConfirmed.data.length} bytes` : "NULL");

      // Wait for finalization on localnet (can take several seconds)
      console.log("Waiting for finalization...");
      const finalized = await waitForFinalization(connection, testAsset.publicKey, 15000);
      console.log("Asset finalized:", finalized);
      expect(finalized).toBe(true);

      // Create PENDING entry in DB (simulating indexer ingestion)
      const agentId = testAsset.publicKey.toBase58();
      console.log("Agent ID for DB:", agentId);

      await prisma.agent.upsert({
        where: { id: agentId },
        create: {
          id: agentId,
          owner: wallet.publicKey.toBase58(),
          uri: agentUri,
          nftName: "Test Agent",
          collection: collectionPubkey.toBase58(),
          registry: registryConfigPda.toBase58(),
          atomEnabled: true,
          status: "PENDING",
          createdSlot: 0n, // Use 0 to ensure it passes cutoff filter
        },
        update: {
          status: "PENDING",
          createdSlot: 0n,
          verifiedAt: null,
        },
      });

      // Get current slot for cutoff (with safety guard like verifyAll)
      const currentSlot = await connection.getSlot("finalized");
      const safetyMargin = 32n;
      const cutoffSlot = BigInt(currentSlot) > safetyMargin
        ? BigInt(currentSlot) - safetyMargin
        : BigInt(currentSlot); // On fresh localnet, use currentSlot directly

      console.log("Current finalized slot:", currentSlot);
      console.log("Running verifyAgents with cutoff:", cutoffSlot.toString());

      // Run verification
      await (verifier as any).verifyAgents(cutoffSlot);

      // Check result
      const agent = await prisma.agent.findUnique({
        where: { id: agentId },
      });

      expect(agent).not.toBeNull();
      expect(agent!.status).toBe("FINALIZED");
      expect(agent!.verifiedAt).not.toBeNull();

      console.log(`Agent ${agentId} FINALIZED at ${agent!.verifiedAt}`);

      // Save for later tests
      testAgent = { id: agentId, pda: agentPda };
    }, 60000);
  });

  // =========================================================================
  // 2. Fake Agent Should Be ORPHANED
  // =========================================================================

  describe("2. Fake Agent Verification (ORPHANED)", () => {
    it("should ORPHAN agent that doesn't exist on-chain", async () => {
      // Generate random pubkey that won't exist
      const fakeAsset = Keypair.generate();
      const fakeAgentId = fakeAsset.publicKey.toBase58();
      const [fakePda] = getAgentPda(fakeAsset.publicKey, PROGRAM_ID);

      console.log("\n=== Verifying Fake Agent ===");
      console.log("Fake Asset:", fakeAgentId);
      console.log("Fake PDA:", fakePda.toBase58());

      // Verify PDA doesn't exist
      const pdaInfo = await connection.getAccountInfo(fakePda);
      expect(pdaInfo).toBeNull();
      console.log("Confirmed: Fake PDA does not exist");

      // Create PENDING entry for fake agent
      await prisma.agent.upsert({
        where: { id: fakeAgentId },
        create: {
          id: fakeAgentId,
          owner: fakeAsset.publicKey.toBase58(),
          uri: "https://fake.test/agent.json",
          nftName: "Fake Agent",
          collection: collectionPubkey.toBase58(),
          registry: registryConfigPda.toBase58(),
          atomEnabled: false,
          status: "PENDING",
          createdSlot: 1n,
        },
        update: {
          status: "PENDING",
          createdSlot: 1n,
          verifiedAt: null,
        },
      });

      // Get current slot
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
  // 3. Real Registry Verification
  // =========================================================================

  describe("3. Real Registry Verification", () => {
    it("should FINALIZE registry that exists on-chain", async () => {
      console.log("\n=== Verifying Real Registry ===");
      console.log("Registry PDA:", registryConfigPda.toBase58());

      // Verify registry exists on-chain
      const registryInfo = await connection.getAccountInfo(registryConfigPda);
      expect(registryInfo).not.toBeNull();
      console.log("Registry exists with", registryInfo!.data.length, "bytes");

      // Create PENDING entry
      await prisma.registry.upsert({
        where: { id: registryConfigPda.toBase58() },
        create: {
          id: registryConfigPda.toBase58(),
          collection: collectionPubkey.toBase58(),
          registryType: "Base",
          authority: wallet.publicKey.toBase58(),
          status: "PENDING",
          slot: 1n,
        },
        update: {
          status: "PENDING",
          slot: 1n,
          verifiedAt: null,
        },
      });

      // Get current slot
      const currentSlot = await connection.getSlot("finalized");
      const cutoffSlot = BigInt(currentSlot) - 32n;

      // Run verification
      await (verifier as any).verifyRegistries(cutoffSlot);

      // Check result
      const registry = await prisma.registry.findUnique({
        where: { id: registryConfigPda.toBase58() },
      });

      expect(registry).not.toBeNull();
      expect(registry!.status).toBe("FINALIZED");
      console.log("Registry FINALIZED");
    }, 30000);
  });

  // =========================================================================
  // 4. fetchOnChainDigests with Real Data
  // =========================================================================

  describe("4. Real On-Chain Digest Fetching", () => {
    it("should fetch digests from real agent (if ATOM enabled)", async () => {
      if (!testAgent) {
        console.log("Skipping - no test agent created");
        return;
      }

      console.log("\n=== Fetching Real Digests ===");
      console.log("Agent:", testAgent.id);

      const digests = await verifier.fetchOnChainDigests(testAgent.id);

      if (digests) {
        console.log("Digests found:");
        console.log("  Slot:", digests.slot.toString());
        console.log("  Feedback count:", digests.feedbackCount.toString());
        console.log("  Response count:", digests.responseCount.toString());
        console.log("  Revoke count:", digests.revokeCount.toString());

        expect(digests.slot).toBeGreaterThan(0n);
        // New agent should have zero counts
        expect(digests.feedbackCount).toBe(0n);
      } else {
        console.log("No digests (ATOM may not be enabled or agent structure different)");
      }
    }, 30000);
  });

  // =========================================================================
  // 5. Full verifyAll Cycle
  // =========================================================================

  describe("5. Full Verification Cycle", () => {
    it("should run complete verifyAll against localnet", async () => {
      console.log("\n=== Full verifyAll Cycle ===");

      // Create mix of real and fake data
      const fakeAgent2 = Keypair.generate();
      const fakeAgentId2 = fakeAgent2.publicKey.toBase58();

      await prisma.agent.upsert({
        where: { id: fakeAgentId2 },
        create: {
          id: fakeAgentId2,
          owner: fakeAgent2.publicKey.toBase58(),
          uri: "https://fake2.test/agent.json",
          nftName: "Fake Agent 2",
          collection: collectionPubkey.toBase58(),
          registry: registryConfigPda.toBase58(),
          atomEnabled: false,
          status: "PENDING",
          createdSlot: 1n,
        },
        update: {
          status: "PENDING",
          createdSlot: 1n,
          verifiedAt: null,
        },
      });

      // Run full verification
      console.log("Running verifyAll...");
      await (verifier as any).verifyAll();

      // Check stats
      const stats = verifier.getStats();
      console.log("Verification stats:", stats);

      // Check fake agent was orphaned
      const fake = await prisma.agent.findUnique({
        where: { id: fakeAgentId2 },
      });
      expect(fake!.status).toBe("ORPHANED");

      console.log("Full cycle complete!");
      console.log(`  Agents verified: ${stats.agentsVerified}`);
      console.log(`  Agents orphaned: ${stats.agentsOrphaned}`);
    }, 60000);
  });
});
