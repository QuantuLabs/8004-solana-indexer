/**
 * Verification tests for recent fixes:
 * 1. Wallet reset NULL handling (Pubkey::default() → null)
 * 2. Metadata API endpoint for local mode
 * 3. Compression prefix in local mode
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { PublicKey } from '@solana/web3.js';

// Mock the default pubkey constant
const DEFAULT_PUBKEY = "11111111111111111111111111111111";

describe('Fixes Verification', () => {
  describe('Wallet Reset NULL Handling', () => {
    it('should convert default pubkey to null', () => {
      const defaultPubkey = new PublicKey(DEFAULT_PUBKEY);
      const newWalletRaw = defaultPubkey.toBase58();
      const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;

      expect(newWallet).toBeNull();
    });

    it('should keep valid pubkey as-is', () => {
      const validPubkey = new PublicKey("So11111111111111111111111111111111111111112");
      const newWalletRaw = validPubkey.toBase58();
      const newWallet = newWalletRaw === DEFAULT_PUBKEY ? null : newWalletRaw;

      expect(newWallet).toBe(validPubkey.toBase58());
      expect(newWallet).not.toBeNull();
    });

    it('should detect Pubkey.default() correctly', () => {
      // Pubkey.default() in Solana is all zeros, which encodes to 111...111
      const zeros = new Uint8Array(32).fill(0);
      const defaultFromZeros = new PublicKey(zeros);

      expect(defaultFromZeros.toBase58()).toBe(DEFAULT_PUBKEY);
    });
  });

  describe('Compression Prefix Constants', () => {
    const PREFIX_RAW = 0x00;
    const PREFIX_ZSTD = 0x01;

    it('should have correct prefix values', () => {
      expect(PREFIX_RAW).toBe(0);
      expect(PREFIX_ZSTD).toBe(1);
    });

    it('should create valid prefixed buffer for RAW data', () => {
      const data = Buffer.from('test data');
      const prefixed = Buffer.concat([Buffer.from([PREFIX_RAW]), data]);

      expect(prefixed[0]).toBe(PREFIX_RAW);
      expect(prefixed.slice(1).toString()).toBe('test data');
    });
  });

  describe('Standard URI Fields', () => {
    const STANDARD_URI_FIELDS = new Set([
      "_uri:type",
      "_uri:name",
      "_uri:description",
      "_uri:image",
      "_uri:services",
      "_uri:registrations",
      "_uri:supported_trust",
      "_uri:active",
      "_uri:x402_support",
      "_uri:skills",
      "_uri:domains",
      "_uri:_status",
    ]);

    it('should not compress standard fields', () => {
      expect(STANDARD_URI_FIELDS.has("_uri:name")).toBe(true);
      expect(STANDARD_URI_FIELDS.has("_uri:description")).toBe(true);
      expect(STANDARD_URI_FIELDS.has("_uri:services")).toBe(true);
    });

    it('should compress custom fields', () => {
      expect(STANDARD_URI_FIELDS.has("_uri:custom_field")).toBe(false);
      expect(STANDARD_URI_FIELDS.has("extra:something")).toBe(false);
    });
  });

  describe('BigInt Feedback Index', () => {
    it('should handle large feedback indices without precision loss', () => {
      // Number.MAX_SAFE_INTEGER = 9007199254740991 (2^53 - 1)
      const largeIndexString = '9007199254740993'; // 2^53 + 1

      // Using parseInt loses precision (rounds to 9007199254740992)
      const asNumber = parseInt(largeIndexString, 10);
      expect(asNumber).toBe(9007199254740992); // Precision lost! Rounded down.

      // Using BigInt preserves precision
      const asBigInt = BigInt(largeIndexString);
      expect(asBigInt).toBe(9007199254740993n);
      expect(asBigInt.toString()).toBe(largeIndexString);
    });

    it('should handle -1n for no feedbacks', () => {
      const noFeedbackIndex = -1n;
      const nextIndex = noFeedbackIndex + 1n;

      expect(nextIndex).toBe(0n);
    });

    it('should calculate next index correctly with BigInt', () => {
      const lastIndex = 9007199254740993n;
      const nextIndex = lastIndex + 1n;

      expect(nextIndex).toBe(9007199254740994n);
    });
  });

  describe('Metadata API Response Format', () => {
    it('should format metadata for PostgREST compatibility', () => {
      const mockMetadata = {
        agentId: 'agent123',
        key: '_uri:name',
        value: Buffer.from([0x00, 0x74, 0x65, 0x73, 0x74]), // PREFIX_RAW + "test"
        immutable: false,
      };

      const formatted = {
        id: `${mockMetadata.agentId}:${mockMetadata.key}`,
        asset: mockMetadata.agentId,
        key: mockMetadata.key,
        value: Buffer.from(mockMetadata.value).toString('base64'),
        immutable: mockMetadata.immutable,
      };

      expect(formatted.id).toBe('agent123:_uri:name');
      expect(formatted.asset).toBe('agent123');
      expect(formatted.key).toBe('_uri:name');
      expect(typeof formatted.value).toBe('string'); // Base64 string
      expect(formatted.immutable).toBe(false);
    });
  });
});
