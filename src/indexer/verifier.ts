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

// Borsh deserialization for AgentAccount (simplified - extract digest triplets)

interface OnChainDigests {
  feedbackDigest: Uint8Array;
  feedbackCount: bigint;
  responseDigest: Uint8Array;
  responseCount: bigint;
  revokeDigest: Uint8Array;
  revokeCount: bigint;
}

interface DbDigestState {
  feedbackDigest: Buffer | null;
  feedbackCount: bigint;
  responseDigest: Buffer | null;
  responseCount: bigint;
  revokeDigest: Buffer | null;
  revokeCount: bigint;
}

type ChainType = 'feedback' | 'response' | 'revoke';

interface VerificationStats {
  agentsVerified: number;
  agentsOrphaned: number;
  feedbacksVerified: number;
  feedbacksOrphaned: number;
  responsesVerified: number;
  responsesOrphaned: number;
  revocationsVerified: number;
  revocationsOrphaned: number;
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
    responsesVerified: 0,
    responsesOrphaned: 0,
    revocationsVerified: 0,
    revocationsOrphaned: 0,
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
  // Per-cycle cache for on-chain digests (cleared each cycle)
  private digestCache = new Map<string, OnChainDigests | null>();

  constructor(
    private connection: Connection,
    private prisma: PrismaClient | null,
    private pool: Pool | null,
    private verifyIntervalMs = config.verifyIntervalMs
  ) {}

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
      // Clear per-cycle digest cache
      this.digestCache.clear();

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
        this.verifyRevocations(cutoffSlot),
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
    let pending: Array<{ id: string }>;

    if (this.prisma) {
      pending = await this.prisma.agent.findMany({
        where: { status: "PENDING", createdSlot: { lte: cutoffSlot } },
        take: config.verifyBatchSize,
        select: { id: true },
      });
    } else if (this.pool) {
      const result = await this.pool.query(
        `SELECT asset AS id FROM agents WHERE status = 'PENDING' AND block_slot <= $1 LIMIT $2`,
        [cutoffSlot.toString(), config.verifyBatchSize]
      );
      pending = result.rows;
    } else {
      return;
    }

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying agents");

    const pubkeys = pending.map(a => a.id);
    const existsMap = await this.batchVerifyAccounts(pubkeys, "finalized");

    const now = new Date();

    for (const agent of pending) {
      if (!this.isRunning) break;

      try {
        const exists = existsMap.get(agent.id) ?? false;

        if (this.prisma) {
          await this.prisma.agent.update({
            where: { id: agent.id },
            data: exists
              ? { status: "FINALIZED", verifiedAt: now, verifiedSlot: cutoffSlot }
              : { status: "ORPHANED", verifiedAt: now },
          });
        } else if (this.pool) {
          if (exists) {
            await this.pool.query(
              `UPDATE agents SET status = $1, verified_at = $2, verified_slot = $3 WHERE asset = $4`,
              ["FINALIZED", now.toISOString(), cutoffSlot.toString(), agent.id]
            );
          } else {
            await this.pool.query(
              `UPDATE agents SET status = $1, verified_at = $2 WHERE asset = $3`,
              ["ORPHANED", now.toISOString(), agent.id]
            );
          }
        }

        if (exists) {
          this.stats.agentsVerified++;
        } else {
          this.stats.agentsOrphaned++;
          logger.warn({ agentId: agent.id }, "Agent orphaned - not found at finalized");
        }
      } catch (error: any) {
        logger.error({ agentId: agent.id, error: error.message }, "Agent verification failed");
      }
    }
  }

  private async verifyValidations(cutoffSlot: bigint): Promise<void> {
    let pending: Array<{ id: string; agentId: string; validator: string; nonce: bigint }>;

    if (this.prisma) {
      pending = await this.prisma.validation.findMany({
        where: {
          chainStatus: "PENDING",
          requestSlot: { lte: cutoffSlot },
        },
        take: config.verifyBatchSize,
        select: { id: true, agentId: true, validator: true, nonce: true },
      });
    } else if (this.pool) {
      const result = await this.pool.query(
        `SELECT id, asset AS "agentId", validator_address AS validator, nonce FROM validations WHERE chain_status = 'PENDING' AND block_slot <= $1 LIMIT $2`,
        [cutoffSlot.toString(), config.verifyBatchSize]
      );
      pending = result.rows;
    } else {
      return;
    }

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying validations");

    // Derive all PDAs first, then batch verify
    const pdaMap = new Map<string, typeof pending[0]>();
    for (const v of pending) {
      try {
        const assetPubkey = parseAssetPubkey(v.agentId);
        const validatorPubkey = new PublicKey(v.validator);
        if (v.nonce > BigInt(Number.MAX_SAFE_INTEGER)) {
          logger.warn({ validationId: v.id, nonce: v.nonce.toString() }, "Nonce exceeds MAX_SAFE_INTEGER, skipping");
          continue;
        }
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
        const newStatus = exists ? "FINALIZED" : "ORPHANED";

        if (this.prisma) {
          await this.prisma.validation.update({
            where: { id: v.id },
            data: { chainStatus: newStatus, chainVerifiedAt: now },
          });
        } else if (this.pool) {
          await this.pool.query(
            `UPDATE validations SET chain_status = $1, chain_verified_at = $2 WHERE id = $3`,
            [newStatus, now.toISOString(), v.id]
          );
        }

        if (exists) {
          this.stats.validationsVerified++;
        } else {
          this.stats.validationsOrphaned++;
          logger.warn({ validationId: v.id }, "Validation orphaned - PDA not found");
        }
      } catch (error: any) {
        logger.error({ validationId: v.id, error: error.message }, "Validation verification failed");
      }
    }
  }

  private async verifyMetadata(cutoffSlot: bigint): Promise<void> {
    let pending: Array<{ id: string; agentId: string; key: string }>;

    if (this.prisma) {
      pending = await this.prisma.agentMetadata.findMany({
        where: {
          status: "PENDING",
          slot: { lte: cutoffSlot },
        },
        take: config.verifyBatchSize,
        select: { id: true, agentId: true, key: true },
      });
    } else if (this.pool) {
      const result = await this.pool.query(
        `SELECT id, asset AS "agentId", key FROM metadata WHERE status = 'PENDING' AND block_slot <= $1 LIMIT $2`,
        [cutoffSlot.toString(), config.verifyBatchSize]
      );
      pending = result.rows;
    } else {
      return;
    }

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
        if (this.prisma) {
          await this.prisma.agentMetadata.update({
            where: { id: m.id },
            data: { status: "FINALIZED", verifiedAt: now },
          });
        } else if (this.pool) {
          await this.pool.query(
            `UPDATE metadata SET status = 'FINALIZED', verified_at = $1 WHERE id = $2`,
            [now.toISOString(), m.id]
          );
        }
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
          const newStatus = exists ? "FINALIZED" : "ORPHANED";

          if (this.prisma) {
            await this.prisma.agentMetadata.update({
              where: { id: m.id },
              data: { status: newStatus, verifiedAt: now },
            });
          } else if (this.pool) {
            await this.pool.query(
              `UPDATE metadata SET status = $1, verified_at = $2 WHERE id = $3`,
              [newStatus, now.toISOString(), m.id]
            );
          }

          if (exists) {
            this.stats.metadataVerified++;
          } else {
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
    let pending: Array<{ id: string; collection: string }>;

    if (this.prisma) {
      pending = await this.prisma.registry.findMany({
        where: {
          status: "PENDING",
          slot: { lte: cutoffSlot },
        },
        take: config.verifyBatchSize,
        select: { id: true, collection: true },
      });
    } else if (this.pool) {
      // collections table has no block_slot column; verify all PENDING
      const result = await this.pool.query(
        `SELECT collection AS id, collection FROM collections WHERE status = 'PENDING' LIMIT $1`,
        [config.verifyBatchSize]
      );
      pending = result.rows;
    } else {
      return;
    }

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
        const newStatus = exists ? "FINALIZED" : "ORPHANED";

        if (this.prisma) {
          await this.prisma.registry.update({
            where: { id: r.id },
            data: { status: newStatus, verifiedAt: now },
          });
        } else if (this.pool) {
          await this.pool.query(
            `UPDATE collections SET status = $1, verified_at = $2 WHERE collection = $3`,
            [newStatus, now.toISOString(), r.collection]
          );
        }

        if (exists) {
          this.stats.registriesVerified++;
        } else {
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
   * Verify feedbacks using hash-chain digests from AgentAccount.
   * Compares last DB running_digest against on-chain digest for cryptographic
   * verification. Falls back to existence check if digest data unavailable.
   */
  private async verifyFeedbacks(cutoffSlot: bigint): Promise<void> {
    let pending: Array<{ id: string; agentId: string }>;

    if (this.prisma) {
      pending = await this.prisma.feedback.findMany({
        where: {
          status: "PENDING",
          createdSlot: { lte: cutoffSlot },
        },
        take: config.verifyBatchSize,
        select: { id: true, agentId: true },
      });
    } else if (this.pool) {
      const result = await this.pool.query(
        `SELECT id, asset AS "agentId" FROM feedbacks WHERE status = 'PENDING' AND block_slot <= $1 LIMIT $2`,
        [cutoffSlot.toString(), config.verifyBatchSize]
      );
      pending = result.rows;
    } else {
      return;
    }

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying feedbacks");

    // Group by agent for efficient batch processing
    const byAgent = new Map<string, Array<{ id: string; agentId: string }>>();
    for (const f of pending) {
      const arr = byAgent.get(f.agentId) || [];
      arr.push(f);
      byAgent.set(f.agentId, arr);
    }

    const agentIds = [...byAgent.keys()];
    const existsMap = await this.batchVerifyAccounts(agentIds, "finalized");

    const now = new Date();

    for (const [agentId, feedbacks] of byAgent) {
      if (!this.isRunning) break;

      const agentExists = existsMap.get(agentId) ?? false;

      if (!agentExists) {
        await this.batchUpdateStatus('feedbacks', 'id', feedbacks.map(f => f.id), 'ORPHANED', now);
        this.stats.feedbacksOrphaned += feedbacks.length;
        logger.warn({ agentId, count: feedbacks.length }, "Feedbacks orphaned - agent not found");
        continue;
      }

      // Hash-chain verification: compare DB digest with on-chain
      const digestOk = await this.checkDigestMatch(agentId, 'feedback');

      if (digestOk) {
        await this.batchUpdateStatus('feedbacks', 'id', feedbacks.map(f => f.id), 'FINALIZED', now);
        this.stats.feedbacksVerified += feedbacks.length;
      } else {
        // Mismatch → leave as PENDING for re-verification next cycle
        logger.warn({ agentId, count: feedbacks.length }, "Feedbacks left PENDING due to hash-chain mismatch");
      }
    }
  }

  /**
   * Verify feedback responses using hash-chain digests + existence checks.
   */
  private async verifyFeedbackResponses(cutoffSlot: bigint): Promise<void> {
    let pending: Array<{ id: string; agentId: string; feedbackOrphaned: boolean }>;

    if (this.prisma) {
      const rows = await this.prisma.feedbackResponse.findMany({
        where: { status: "PENDING", slot: { lte: cutoffSlot } },
        take: config.verifyBatchSize,
        include: { feedback: { select: { agentId: true, status: true } } },
      });
      pending = rows.map(r => ({
        id: r.id,
        agentId: r.feedback.agentId,
        feedbackOrphaned: r.feedback.status === "ORPHANED",
      }));
    } else if (this.pool) {
      const result = await this.pool.query(
        `SELECT fr.id, fr.asset AS "agentId", COALESCE(f.status, 'ORPHANED') AS "feedbackStatus"
         FROM feedback_responses fr
         LEFT JOIN feedbacks f ON f.asset = fr.asset AND f.client_address = fr.client_address AND f.feedback_index = fr.feedback_index
         WHERE fr.status = 'PENDING' AND fr.block_slot <= $1
         LIMIT $2`,
        [cutoffSlot.toString(), config.verifyBatchSize]
      );
      pending = result.rows.map((r: any) => ({
        id: r.id,
        agentId: r.agentId,
        feedbackOrphaned: r.feedbackStatus === "ORPHANED",
      }));
    } else {
      return;
    }

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying feedback responses");

    const now = new Date();

    // Separate orphaned from valid
    const orphaned = pending.filter(r => r.feedbackOrphaned);
    const valid = pending.filter(r => !r.feedbackOrphaned);

    if (orphaned.length > 0) {
      await this.batchUpdateStatus('feedback_responses', 'id', orphaned.map(r => r.id), 'ORPHANED', now);
      this.stats.responsesOrphaned += orphaned.length;
    }

    if (valid.length > 0) {
      // Group by agent
      const byAgent = new Map<string, string[]>();
      for (const r of valid) {
        const arr = byAgent.get(r.agentId) || [];
        arr.push(r.id);
        byAgent.set(r.agentId, arr);
      }

      const agentIds = [...byAgent.keys()];
      const existsMap = await this.batchVerifyAccounts(agentIds, "finalized");

      for (const [agentId, ids] of byAgent) {
        if (!this.isRunning) break;

        const agentExists = existsMap.get(agentId) ?? false;

        if (!agentExists) {
          await this.batchUpdateStatus('feedback_responses', 'id', ids, 'ORPHANED', now);
          this.stats.responsesOrphaned += ids.length;
          continue;
        }

        // Hash-chain verification for response chain
        const digestOk = await this.checkDigestMatch(agentId, 'response');

        if (digestOk) {
          await this.batchUpdateStatus('feedback_responses', 'id', ids, 'FINALIZED', now);
          this.stats.responsesVerified += ids.length;
        } else {
          logger.warn({ agentId, count: ids.length }, "Responses left PENDING due to hash-chain mismatch");
        }
      }
    }
  }

  /**
   * Verify revocations using hash-chain digests + existence checks.
   */
  private async verifyRevocations(cutoffSlot: bigint): Promise<void> {
    let pending: Array<{ id: string; agentId: string }>;

    if (this.prisma) {
      const rows = await this.prisma.revocation.findMany({
        where: { status: "PENDING", slot: { lte: cutoffSlot } },
        take: config.verifyBatchSize,
        select: { id: true, agentId: true },
      });
      pending = rows;
    } else if (this.pool) {
      const result = await this.pool.query(
        `SELECT id, asset AS "agentId" FROM revocations WHERE status = 'PENDING' AND slot <= $1 LIMIT $2`,
        [cutoffSlot.toString(), config.verifyBatchSize]
      );
      pending = result.rows;
    } else {
      return;
    }

    if (pending.length === 0) return;

    logger.debug({ count: pending.length }, "Verifying revocations");

    const byAgent = new Map<string, string[]>();
    for (const r of pending) {
      const arr = byAgent.get(r.agentId) || [];
      arr.push(r.id);
      byAgent.set(r.agentId, arr);
    }

    const agentIds = [...byAgent.keys()];
    const existsMap = await this.batchVerifyAccounts(agentIds, "finalized");

    const now = new Date();

    for (const [agentId, ids] of byAgent) {
      if (!this.isRunning) break;

      const agentExists = existsMap.get(agentId) ?? false;

      if (!agentExists) {
        await this.batchUpdateStatus('revocations', 'id', ids, 'ORPHANED', now);
        this.stats.revocationsOrphaned += ids.length;
        logger.warn({ agentId, count: ids.length }, "Revocations orphaned - agent not found");
        continue;
      }

      // Hash-chain verification for revoke chain
      const digestOk = await this.checkDigestMatch(agentId, 'revoke');

      if (digestOk) {
        await this.batchUpdateStatus('revocations', 'id', ids, 'FINALIZED', now);
        this.stats.revocationsVerified += ids.length;
      } else {
        logger.warn({ agentId, count: ids.length }, "Revocations left PENDING due to hash-chain mismatch");
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
  // Hash-Chain Verification Helpers
  // =========================================================================

  /**
   * Compare DB digest against on-chain digest for a specific chain type.
   * Logs mismatches and increments hashChainMismatches counter.
   */
  /**
   * Returns true if digest matches or is inconclusive (behind/unavailable).
   * Returns false only on confirmed mismatch (same count, different digest).
   */
  private async checkDigestMatch(agentId: string, chain: ChainType): Promise<boolean> {
    try {
      // Use per-cycle cache to avoid redundant RPC calls for same agent
      let onChain: OnChainDigests | null | undefined = this.digestCache.get(agentId);
      if (onChain === undefined) {
        onChain = await this.fetchOnChainDigests(agentId);
        this.digestCache.set(agentId, onChain);
      }
      if (!onChain) {
        logger.debug({ agentId, chain }, "On-chain digests unavailable, keeping PENDING");
        return false;
      }

      const dbState = await this.getLastDbDigests(agentId);

      let onChainDigest: Uint8Array;
      let onChainCount: bigint;
      let dbDigest: Buffer | null;
      let dbCount: bigint;

      switch (chain) {
        case 'feedback':
          onChainDigest = onChain.feedbackDigest;
          onChainCount = onChain.feedbackCount;
          dbDigest = dbState.feedbackDigest;
          dbCount = dbState.feedbackCount;
          break;
        case 'response':
          onChainDigest = onChain.responseDigest;
          onChainCount = onChain.responseCount;
          dbDigest = dbState.responseDigest;
          dbCount = dbState.responseCount;
          break;
        case 'revoke':
          onChainDigest = onChain.revokeDigest;
          onChainCount = onChain.revokeCount;
          dbDigest = dbState.revokeDigest;
          dbCount = dbState.revokeCount;
          break;
      }

      // No DB events and on-chain count is also zero → legitimate empty chain
      if (!dbDigest && onChainCount === 0n) return true;

      // No DB digest but on-chain has events → can't verify, keep PENDING
      if (!dbDigest) return false;

      // DB count < on-chain → indexer behind, skip
      if (dbCount < onChainCount) {
        logger.debug({ agentId, chain, dbCount: Number(dbCount), onChainCount: Number(onChainCount) },
          "Hash-chain behind on-chain, skipping digest check");
        return true;
      }

      // DB count > on-chain → possible reorg
      if (dbCount > onChainCount) {
        this.stats.hashChainMismatches++;
        logger.warn({ agentId, chain, dbCount: Number(dbCount), onChainCount: Number(onChainCount) },
          "Hash-chain count EXCEEDS on-chain (possible reorg)");
        return false;
      }

      // Counts match → compare digests
      const onChainBuf = Buffer.from(onChainDigest);
      if (onChainBuf.equals(dbDigest)) {
        logger.debug({ agentId, chain, count: Number(dbCount) },
          "Hash-chain VERIFIED");
        return true;
      } else {
        this.stats.hashChainMismatches++;
        logger.warn({
          agentId, chain,
          count: Number(dbCount),
          dbDigest: dbDigest.toString('hex').slice(0, 16) + '...',
          onChainDigest: onChainBuf.toString('hex').slice(0, 16) + '...',
        }, "Hash-chain MISMATCH - digests differ at same count");
        return false;
      }
    } catch (error: any) {
      logger.warn({ agentId, chain, error: error.message },
        "Hash-chain check failed, keeping PENDING");
      return false;
    }
  }

  /**
   * Get the last running_digest and event count from DB for each chain type.
   */
  private async getLastDbDigests(agentId: string): Promise<DbDigestState> {
    const zero: DbDigestState = {
      feedbackDigest: null, feedbackCount: 0n,
      responseDigest: null, responseCount: 0n,
      revokeDigest: null, revokeCount: 0n,
    };

    if (this.prisma) {
      const notOrphaned = { not: 'ORPHANED' };
      const [lastFb, fbCount, lastResp, respCount, lastRev, revCount] = await Promise.all([
        this.prisma.feedback.findFirst({
          where: { agentId, runningDigest: { not: null }, status: notOrphaned },
          orderBy: { feedbackIndex: 'desc' },
          select: { runningDigest: true },
        }),
        this.prisma.feedback.count({ where: { agentId, status: notOrphaned } }),
        this.prisma.feedbackResponse.findFirst({
          where: { feedback: { agentId }, runningDigest: { not: null }, status: notOrphaned },
          orderBy: { slot: 'desc' },
          select: { runningDigest: true },
        }),
        this.prisma.feedbackResponse.count({ where: { feedback: { agentId }, status: notOrphaned } }),
        this.prisma.revocation.findFirst({
          where: { agentId, runningDigest: { not: null }, status: notOrphaned },
          orderBy: { revokeCount: 'desc' },
          select: { runningDigest: true },
        }),
        this.prisma.revocation.count({ where: { agentId, status: notOrphaned } }),
      ]);

      return {
        feedbackDigest: lastFb?.runningDigest ? Buffer.from(lastFb.runningDigest) : null,
        feedbackCount: BigInt(fbCount),
        responseDigest: lastResp?.runningDigest ? Buffer.from(lastResp.runningDigest) : null,
        responseCount: BigInt(respCount),
        revokeDigest: lastRev?.runningDigest ? Buffer.from(lastRev.runningDigest) : null,
        revokeCount: BigInt(revCount),
      };
    } else if (this.pool) {
      const [fbRes, fbCnt, respRes, respCnt, revRes, revCnt] = await Promise.all([
        this.pool.query(
          `SELECT running_digest FROM feedbacks WHERE asset = $1 AND running_digest IS NOT NULL AND status != 'ORPHANED' ORDER BY feedback_index DESC LIMIT 1`,
          [agentId]
        ),
        this.pool.query(`SELECT COUNT(*)::bigint AS cnt FROM feedbacks WHERE asset = $1 AND status != 'ORPHANED'`, [agentId]),
        this.pool.query(
          `SELECT running_digest FROM feedback_responses WHERE asset = $1 AND running_digest IS NOT NULL AND status != 'ORPHANED' ORDER BY block_slot DESC, tx_index DESC LIMIT 1`,
          [agentId]
        ),
        this.pool.query(`SELECT COUNT(*)::bigint AS cnt FROM feedback_responses WHERE asset = $1 AND status != 'ORPHANED'`, [agentId]),
        this.pool.query(
          `SELECT running_digest FROM revocations WHERE asset = $1 AND running_digest IS NOT NULL AND status != 'ORPHANED' ORDER BY revoke_count DESC LIMIT 1`,
          [agentId]
        ),
        this.pool.query(`SELECT COUNT(*)::bigint AS cnt FROM revocations WHERE asset = $1 AND status != 'ORPHANED'`, [agentId]),
      ]);

      return {
        feedbackDigest: fbRes.rows[0]?.running_digest ?? null,
        feedbackCount: BigInt(fbCnt.rows[0]?.cnt ?? 0),
        responseDigest: respRes.rows[0]?.running_digest ?? null,
        responseCount: BigInt(respCnt.rows[0]?.cnt ?? 0),
        revokeDigest: revRes.rows[0]?.running_digest ?? null,
        revokeCount: BigInt(revCnt.rows[0]?.cnt ?? 0),
      };
    }

    return zero;
  }

  /**
   * Batch-update status for multiple rows in a table.
   */
  private static readonly ALLOWED_TABLES: Record<string, string> = {
    'feedbacks': 'feedbacks',
    'feedback_responses': 'feedback_responses',
    'revocations': 'revocations',
  };

  private static readonly ALLOWED_ID_COLUMNS: Record<string, string> = {
    'id': 'id',
  };

  private async batchUpdateStatus(
    table: string,
    idColumn: string,
    ids: string[],
    status: string,
    verifiedAt: Date,
  ): Promise<void> {
    if (ids.length === 0) return;

    if (this.prisma) {
      const modelMap: Record<string, any> = {
        'feedbacks': this.prisma.feedback,
        'feedback_responses': this.prisma.feedbackResponse,
        'revocations': this.prisma.revocation,
      };
      const model = modelMap[table];
      if (model) {
        await model.updateMany({
          where: { id: { in: ids } },
          data: { status, verifiedAt },
        });
      }
    } else if (this.pool) {
      const safeTable = DataVerifier.ALLOWED_TABLES[table];
      const safeColumn = DataVerifier.ALLOWED_ID_COLUMNS[idColumn];
      if (!safeTable || !safeColumn) {
        logger.error({ table, idColumn }, "Invalid table or column name in batchUpdateStatus");
        return;
      }
      await this.pool.query(
        `UPDATE ${safeTable} SET status = $1, verified_at = $2 WHERE ${safeColumn} = ANY($3::text[])`,
        [status, verifiedAt.toISOString(), ids]
      );
    }
  }

  /**
   * Fetch on-chain digests from AgentAccount at finalized commitment.
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

      const data = accountInfo.data;
      // Minimum: discriminator(8) + collection(32) + owner(32) + asset(32) + bump(1) + atom_enabled(1) + Option::None(1) + 3*(digest(32)+count(8)) = 227
      if (data.length < 227) return null;

      // Skip discriminator(8) + collection(32) + owner(32) + asset(32) + bump(1) + atom_enabled(1) + agent_wallet Option<Pubkey>
      let offset = 8 + 32 + 32 + 32 + 1 + 1;
      const optionTag = data[offset];
      offset += optionTag === 1 ? 33 : 1;

      // Verify buffer has enough data for 3 digest+count triplets (3 * 40 = 120 bytes)
      if (data.length < offset + 120) return null;

      // Read digest+count triplets (feedback, response, revoke)
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

      return {
        feedbackDigest: new Uint8Array(feedbackDigest),
        feedbackCount,
        responseDigest: new Uint8Array(responseDigest),
        responseCount,
        revokeDigest: new Uint8Array(revokeDigest),
        revokeCount,
      };
    } catch (error: any) {
      logger.warn({ agentId, error: error.message }, "Failed to fetch on-chain digests");
      return null;
    }
  }
}
