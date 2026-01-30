/**
 * Background Verification Worker for Reorg Resilience
 *
 * Dual verification strategy:
 * 1. Hash-chain verification for Feedback/Response/Revoke (cryptographic)
 * 2. Existence verification for Agent/Metadata/Registry/Validation (on-chain)
 *
 * All data is ingested at "confirmed" commitment and verified against "finalized"
 * to detect reorgs and orphaned data.
 */

import { Connection, PublicKey, Commitment } from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { Pool } from "pg";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";
import {
  getAgentPda,
  getValidationRequestPda,
  getMetadataEntryPda,
  getRegistryConfigPda,
  computeKeyHash,
  parseAssetPubkey,
} from "../utils/pda.js";

const logger = createChildLogger("verifier");

// Borsh deserialization for AgentAccount (simplified - just extract digests)
// AGENT_ACCOUNT_DISCRIMINATOR will be needed when implementing hash-chain verification

interface OnChainDigests {
  feedbackDigest: Uint8Array;
  feedbackCount: bigint;
  responseDigest: Uint8Array;
  responseCount: bigint;
  revokeDigest: Uint8Array;
  revokeCount: bigint;
  slot: bigint;
}

interface VerificationStats {
  agentsVerified: number;
  agentsOrphaned: number;
  feedbacksVerified: number;
  feedbacksOrphaned: number;
  validationsVerified: number;
  validationsOrphaned: number;
  metadataVerified: number;
  metadataOrphaned: number;
  registriesVerified: number;
  registriesOrphaned: number;
  hashChainMismatches: number;
  lastRunAt: Date | null;
  lastRunDurationMs: number;
}

export class DataVerifier {
  private interval: ReturnType<typeof setInterval> | null = null;
  private isRunning = false;
  private verifyInProgress = false; // Reentrancy guard for async verification cycles
  private stats: VerificationStats = {
    agentsVerified: 0,
    agentsOrphaned: 0,
    feedbacksVerified: 0,
    feedbacksOrphaned: 0,
    validationsVerified: 0,
    validationsOrphaned: 0,
    metadataVerified: 0,
    metadataOrphaned: 0,
    registriesVerified: 0,
    registriesOrphaned: 0,
    hashChainMismatches: 0,
    lastRunAt: null,
    lastRunDurationMs: 0,
  };

  constructor(
    private connection: Connection,
    private prisma: PrismaClient | null,
    pool: Pool | null, // Reserved for future Supabase mode support
    private verifyIntervalMs = config.verifyIntervalMs
  ) {
    // Suppress unused parameter warning for pool (future Supabase support)
    void pool;
  }

  async start(): Promise<void> {
    if (!config.verificationEnabled) {
      logger.info("Verification disabled via config");
      return;
    }

    this.isRunning = true;
    logger.info({ intervalMs: this.verifyIntervalMs }, "Starting data verifier");

    // Run immediately on start
    await this.verifyAll().catch(err => {
      logger.error({ error: err.message }, "Initial verification failed");
    });

    // Then run periodically
    this.interval = setInterval(() => {
      this.verifyAll().catch(err => {
        logger.error({ error: err.message }, "Verification cycle failed");
      });
    }, this.verifyIntervalMs);
  }

  async stop(): Promise<void> {
    logger.info("Stopping data verifier");
    this.isRunning = false;
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }

  getStats(): VerificationStats {
    return { ...this.stats };
  }

  private async verifyAll(): Promise<void> {
    if (!this.isRunning) return;

    // Reentrancy guard - skip if previous cycle still running
    if (this.verifyInProgress) {
      logger.debug("Skipping verification cycle - previous cycle still in progress");
      return;
    }

    this.verifyInProgress = true;
    const startTime = Date.now();
    logger.debug("Starting verification cycle");

    try {
      // Get current finalized slot for cutoff calculation
      const currentSlot = await this.connection.getSlot("finalized");
      // Guard against negative cutoff (e.g., if safety margin > current slot on new networks)
      const cutoffSlot = BigInt(currentSlot) > BigInt(config.verifySafetyMarginSlots)
        ? BigInt(currentSlot) - BigInt(config.verifySafetyMarginSlots)
        : 0n;

      // Verify in parallel for better performance
      await Promise.all([
        this.verifyAgents(cutoffSlot),
        this.verifyValidations(cutoffSlot),
        this.verifyMetadata(cutoffSlot),
        this.verifyRegistries(cutoffSlot),
        this.verifyFeedbacks(cutoffSlot),
        this.verifyFeedbackResponses(cutoffSlot),
      ]);

      const duration = Date.now() - startTime;
      this.stats.lastRunAt = new Date();
      this.stats.lastRunDurationMs = duration;

      logger.info({
        duration,
        agents: this.stats.agentsVerified,
        feedbacks: this.stats.feedbacksVerified,
        validations: this.stats.validationsVerified,
      }, "Verification cycle complete");
    } catch (error: any) {
      logger.error({ error: error.message }, "Verification cycle failed");
    } finally {
      this.verifyInProgress = false;
    }
  }

  // =========================================================================
  // Existence Verification (Agent, Metadata, Registry, Validation)
  // =========================================================================

  private async verifyAgents(cutoffSlot: bigint): Promise<void> {
    if (!this.prisma) return;

    const pending = await this.prisma.agent.findMany({
      where: {
        status: "PENDING",
        createdSlot: { lte: cutoffSlot },
      },
      take: config.verifyBatchSize,
      select: { id: true, createdSlot: true },
    });

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying agents");

    // Batch verify all agents at once (100 per RPC call)
    const pubkeys = pending.map(a => a.id);
    const existsMap = await this.batchVerifyAccounts(pubkeys, "finalized");

    const now = new Date();
    const currentSlot = await this.connection.getSlot("finalized");

    for (const agent of pending) {
      if (!this.isRunning) break;

      try {
        const exists = existsMap.get(agent.id) ?? false;

        if (exists) {
          await this.prisma.agent.update({
            where: { id: agent.id },
            data: {
              status: "FINALIZED",
              verifiedAt: now,
              verifiedSlot: BigInt(currentSlot),
            },
          });
          this.stats.agentsVerified++;
        } else {
          await this.prisma.agent.update({
            where: { id: agent.id },
            data: { status: "ORPHANED", verifiedAt: now },
          });
          this.stats.agentsOrphaned++;
          logger.warn({ agentId: agent.id }, "Agent orphaned - not found at finalized");
        }
      } catch (error: any) {
        logger.error({ agentId: agent.id, error: error.message }, "Agent verification failed");
      }
    }
  }

  private async verifyValidations(cutoffSlot: bigint): Promise<void> {
    if (!this.prisma) return;

    const pending = await this.prisma.validation.findMany({
      where: {
        chainStatus: "PENDING",
        requestSlot: { lte: cutoffSlot },
      },
      take: config.verifyBatchSize,
      select: { id: true, agentId: true, validator: true, nonce: true },
    });

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying validations");

    // Derive all PDAs first, then batch verify
    const pdaMap = new Map<string, typeof pending[0]>();
    for (const v of pending) {
      try {
        const assetPubkey = parseAssetPubkey(v.agentId);
        const validatorPubkey = new PublicKey(v.validator);
        const [pda] = getValidationRequestPda(assetPubkey, validatorPubkey, Number(v.nonce));
        pdaMap.set(pda.toBase58(), v);
      } catch (error: any) {
        logger.error({ validationId: v.id, error: error.message }, "Failed to derive validation PDA");
      }
    }

    const pubkeys = Array.from(pdaMap.keys());
    const existsMap = await this.batchVerifyAccounts(pubkeys, "finalized");

    const now = new Date();
    for (const [pda, v] of pdaMap) {
      if (!this.isRunning) break;

      try {
        const exists = existsMap.get(pda) ?? false;

        if (exists) {
          await this.prisma.validation.update({
            where: { id: v.id },
            data: { chainStatus: "FINALIZED", chainVerifiedAt: now },
          });
          this.stats.validationsVerified++;
        } else {
          await this.prisma.validation.update({
            where: { id: v.id },
            data: { chainStatus: "ORPHANED", chainVerifiedAt: now },
          });
          this.stats.validationsOrphaned++;
          logger.warn({ validationId: v.id }, "Validation orphaned - PDA not found");
        }
      } catch (error: any) {
        logger.error({ validationId: v.id, error: error.message }, "Validation verification failed");
      }
    }
  }

  private async verifyMetadata(cutoffSlot: bigint): Promise<void> {
    if (!this.prisma) return;

    const pending = await this.prisma.agentMetadata.findMany({
      where: {
        status: "PENDING",
        slot: { lte: cutoffSlot },
      },
      take: config.verifyBatchSize,
      select: { id: true, agentId: true, key: true },
    });

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying metadata");

    // Separate URI-derived (auto-finalize) from on-chain metadata
    const uriMetadata: typeof pending = [];
    const onChainMetadata: typeof pending = [];
    const pdaMap = new Map<string, typeof pending[0]>();

    for (const m of pending) {
      if (m.key.startsWith("_uri:")) {
        uriMetadata.push(m);
      } else {
        onChainMetadata.push(m);
        try {
          const assetPubkey = parseAssetPubkey(m.agentId);
          const keyHash = computeKeyHash(m.key);
          const [pda] = getMetadataEntryPda(assetPubkey, keyHash);
          pdaMap.set(pda.toBase58(), m);
        } catch (error: any) {
          logger.error({ metadataId: m.id, error: error.message }, "Failed to derive metadata PDA");
        }
      }
    }

    const now = new Date();

    // Auto-finalize URI-derived metadata
    for (const m of uriMetadata) {
      if (!this.isRunning) break;
      try {
        await this.prisma.agentMetadata.update({
          where: { id: m.id },
          data: { status: "FINALIZED", verifiedAt: now },
        });
        this.stats.metadataVerified++;
      } catch (error: any) {
        logger.error({ metadataId: m.id, error: error.message }, "URI metadata finalization failed");
      }
    }

    // Batch verify on-chain metadata
    if (pdaMap.size > 0) {
      const pubkeys = Array.from(pdaMap.keys());
      const existsMap = await this.batchVerifyAccounts(pubkeys, "finalized");

      for (const [pda, m] of pdaMap) {
        if (!this.isRunning) break;

        try {
          const exists = existsMap.get(pda) ?? false;

          if (exists) {
            await this.prisma.agentMetadata.update({
              where: { id: m.id },
              data: { status: "FINALIZED", verifiedAt: now },
            });
            this.stats.metadataVerified++;
          } else {
            await this.prisma.agentMetadata.update({
              where: { id: m.id },
              data: { status: "ORPHANED", verifiedAt: now },
            });
            this.stats.metadataOrphaned++;
            logger.warn({ metadataId: m.id, key: m.key }, "Metadata orphaned - PDA not found");
          }
        } catch (error: any) {
          logger.error({ metadataId: m.id, error: error.message }, "Metadata verification failed");
        }
      }
    }
  }

  private async verifyRegistries(cutoffSlot: bigint): Promise<void> {
    if (!this.prisma) return;

    const pending = await this.prisma.registry.findMany({
      where: {
        status: "PENDING",
        slot: { lte: cutoffSlot },
      },
      take: config.verifyBatchSize,
      select: { id: true, collection: true },
    });

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying registries");

    // Derive all PDAs first, then batch verify
    const pdaMap = new Map<string, typeof pending[0]>();
    for (const r of pending) {
      try {
        const collectionPubkey = new PublicKey(r.collection);
        const [pda] = getRegistryConfigPda(collectionPubkey);
        pdaMap.set(pda.toBase58(), r);
      } catch (error: any) {
        logger.error({ registryId: r.id, error: error.message }, "Failed to derive registry PDA");
      }
    }

    const pubkeys = Array.from(pdaMap.keys());
    const existsMap = await this.batchVerifyAccounts(pubkeys, "finalized");

    const now = new Date();
    for (const [pda, r] of pdaMap) {
      if (!this.isRunning) break;

      try {
        const exists = existsMap.get(pda) ?? false;

        if (exists) {
          await this.prisma.registry.update({
            where: { id: r.id },
            data: { status: "FINALIZED", verifiedAt: now },
          });
          this.stats.registriesVerified++;
        } else {
          await this.prisma.registry.update({
            where: { id: r.id },
            data: { status: "ORPHANED", verifiedAt: now },
          });
          this.stats.registriesOrphaned++;
          logger.warn({ registryId: r.id }, "Registry orphaned - PDA not found");
        }
      } catch (error: any) {
        logger.error({ registryId: r.id, error: error.message }, "Registry verification failed");
      }
    }
  }

  // =========================================================================
  // Hash-Chain Verification (Feedback, Response, Revoke)
  // =========================================================================

  /**
   * Verify feedbacks using hash-chain digests from AgentAccount
   * This is more complex than existence checks - requires fetching on-chain digests
   * and comparing against computed digests from indexed events.
   *
   * For now, we use a simplified approach: mark feedbacks as FINALIZED after
   * the safety margin if their agent exists. Full hash-chain verification
   * can be added later for paranoid mode.
   */
  private async verifyFeedbacks(cutoffSlot: bigint): Promise<void> {
    if (!this.prisma) return;

    const pending = await this.prisma.feedback.findMany({
      where: {
        status: "PENDING",
        createdSlot: { lte: cutoffSlot },
      },
      take: config.verifyBatchSize,
      select: { id: true, agentId: true, createdSlot: true },
    });

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying feedbacks");

    // Get unique agent IDs and batch verify
    const agentIds = [...new Set(pending.map(f => f.agentId))];
    const existsMap = await this.batchVerifyAccounts(agentIds, "finalized");

    const now = new Date();
    for (const f of pending) {
      if (!this.isRunning) break;

      try {
        const agentExists = existsMap.get(f.agentId) ?? false;

        if (agentExists) {
          await this.prisma.feedback.update({
            where: { id: f.id },
            data: { status: "FINALIZED", verifiedAt: now },
          });
          this.stats.feedbacksVerified++;
        } else {
          await this.prisma.feedback.update({
            where: { id: f.id },
            data: { status: "ORPHANED", verifiedAt: now },
          });
          this.stats.feedbacksOrphaned++;
          logger.warn({ feedbackId: f.id }, "Feedback orphaned - agent not found");
        }
      } catch (error: any) {
        logger.error({ feedbackId: f.id, error: error.message }, "Feedback verification failed");
      }
    }
  }

  private async verifyFeedbackResponses(cutoffSlot: bigint): Promise<void> {
    if (!this.prisma) return;

    const pending = await this.prisma.feedbackResponse.findMany({
      where: {
        status: "PENDING",
        slot: { lte: cutoffSlot },
      },
      take: config.verifyBatchSize,
      include: { feedback: { select: { agentId: true, status: true } } },
    });

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying feedback responses");

    // Separate orphaned (auto-orphan) from those needing agent check
    const orphanedResponses: typeof pending = [];
    const needsAgentCheck: typeof pending = [];

    for (const r of pending) {
      if (r.feedback.status === "ORPHANED") {
        orphanedResponses.push(r);
      } else {
        needsAgentCheck.push(r);
      }
    }

    const now = new Date();

    // Auto-orphan responses with orphaned parent feedback
    for (const r of orphanedResponses) {
      if (!this.isRunning) break;
      try {
        await this.prisma.feedbackResponse.update({
          where: { id: r.id },
          data: { status: "ORPHANED", verifiedAt: now },
        });
        this.stats.feedbacksOrphaned++;
      } catch (error: any) {
        logger.error({ responseId: r.id, error: error.message }, "Response orphan update failed");
      }
    }

    // Batch verify agent existence for remaining responses
    if (needsAgentCheck.length > 0) {
      const agentIds = [...new Set(needsAgentCheck.map(r => r.feedback.agentId))];
      const existsMap = await this.batchVerifyAccounts(agentIds, "finalized");

      for (const r of needsAgentCheck) {
        if (!this.isRunning) break;

        try {
          const agentExists = existsMap.get(r.feedback.agentId) ?? false;

          if (agentExists) {
            await this.prisma.feedbackResponse.update({
              where: { id: r.id },
              data: { status: "FINALIZED", verifiedAt: now },
            });
            this.stats.feedbacksVerified++;
          } else {
            await this.prisma.feedbackResponse.update({
              where: { id: r.id },
              data: { status: "ORPHANED", verifiedAt: now },
            });
            this.stats.feedbacksOrphaned++;
          }
        } catch (error: any) {
          logger.error({ responseId: r.id, error: error.message }, "Response verification failed");
        }
      }
    }
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Batch verify multiple accounts using getMultipleAccountsInfo
   * Much more efficient than individual calls (100 accounts per RPC call vs 100 RPC calls)
   */
  private async batchVerifyAccounts(
    pubkeys: string[],
    commitment: Commitment = "finalized"
  ): Promise<Map<string, boolean>> {
    const results = new Map<string, boolean>();
    if (pubkeys.length === 0) return results;

    const BATCH_SIZE = 100; // Solana RPC limit
    const batches: string[][] = [];

    for (let i = 0; i < pubkeys.length; i += BATCH_SIZE) {
      batches.push(pubkeys.slice(i, i + BATCH_SIZE));
    }

    logger.debug({ totalPubkeys: pubkeys.length, batches: batches.length }, "Batch verifying accounts");

    for (const batch of batches) {
      if (!this.isRunning) break;

      try {
        const publicKeys = batch.map(pk => new PublicKey(pk));
        const accounts = await this.connection.getMultipleAccountsInfo(publicKeys, { commitment });

        for (let i = 0; i < batch.length; i++) {
          results.set(batch[i], accounts[i] !== null);
        }
      } catch (error: any) {
        logger.warn({ batchSize: batch.length, error: error.message }, "Batch verification failed, falling back to individual checks");
        // Fallback to individual checks for this batch
        for (const pk of batch) {
          if (!this.isRunning) break;
          const exists = await this.verifyWithRetry(pk, commitment);
          results.set(pk, exists);
        }
      }
    }

    return results;
  }

  /**
   * Check if an account exists at the given commitment level
   * Retries with exponential backoff to avoid false negatives from RPC lag
   * Used as fallback when batch verification fails
   */
  private async verifyWithRetry(
    pubkey: string,
    commitment: Commitment,
    maxRetries = config.verifyMaxRetries
  ): Promise<boolean> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const info = await this.connection.getAccountInfo(
          new PublicKey(pubkey),
          { commitment }
        );
        if (info !== null) return true;

        // Exponential backoff: 1s, 2s, 4s
        if (attempt < maxRetries - 1) {
          await this.sleep(1000 * Math.pow(2, attempt));
        }
      } catch (error: any) {
        logger.warn({ pubkey, attempt, error: error.message }, "Account check failed, retrying");
        if (attempt < maxRetries - 1) {
          await this.sleep(1000 * Math.pow(2, attempt));
        }
      }
    }
    return false;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // =========================================================================
  // Future: Full Hash-Chain Verification
  // =========================================================================

  /**
   * Fetch on-chain digests from AgentAccount at finalized commitment.
   * Used for hash-chain verification to compare computed digest against on-chain value.
   * Public for testing and external verification tools.
   */
  async fetchOnChainDigests(agentId: string): Promise<OnChainDigests | null> {
    try {
      const assetPubkey = parseAssetPubkey(agentId);
      const [agentPda] = getAgentPda(assetPubkey);

      const accountInfo = await this.connection.getAccountInfo(agentPda, {
        commitment: "finalized",
      });

      if (!accountInfo) return null;

      // Parse AgentAccount to extract digests
      // This is simplified - full implementation would use borsh deserialization
      const data = accountInfo.data;
      if (data.length < 227) return null; // Minimum AgentAccount size

      // Skip discriminator (8) + collection(32) + owner(32) + asset(32) + bump(1) + atom_enabled(1) + agent_wallet option
      let offset = 8 + 32 + 32 + 32 + 1 + 1;
      const optionTag = data[offset];
      offset += optionTag === 1 ? 33 : 1; // Option<Pubkey>

      // Read digests and counts
      const feedbackDigest = data.slice(offset, offset + 32);
      offset += 32;
      const feedbackCount = data.readBigUInt64LE(offset);
      offset += 8;
      const responseDigest = data.slice(offset, offset + 32);
      offset += 32;
      const responseCount = data.readBigUInt64LE(offset);
      offset += 8;
      const revokeDigest = data.slice(offset, offset + 32);
      offset += 32;
      const revokeCount = data.readBigUInt64LE(offset);

      const slot = await this.connection.getSlot("finalized");

      return {
        feedbackDigest: new Uint8Array(feedbackDigest),
        feedbackCount,
        responseDigest: new Uint8Array(responseDigest),
        responseCount,
        revokeDigest: new Uint8Array(revokeDigest),
        revokeCount,
        slot: BigInt(slot),
      };
    } catch (error: any) {
      logger.warn({ agentId, error: error.message }, "Failed to fetch on-chain digests");
      return null;
    }
  }
}
