import { describe, it, expect } from 'vitest';
import {
  encodeAgentId,
  decodeAgentId,
  encodeFeedbackId,
  decodeFeedbackId,
  encodeResponseId,
  decodeResponseId,
  encodeValidationId,
  decodeValidationId,
  encodeMetadataId,
  decodeMetadataId,
} from '../../../src/api/graphql/utils/ids.js';

describe('GraphQL ID encoding/decoding', () => {
  describe('Agent IDs', () => {
    it('encodes agent ID as raw asset', () => {
      expect(encodeAgentId('8xKzAbcDef')).toBe('8xKzAbcDef');
    });

    it('decodes raw agent ID', () => {
      expect(decodeAgentId('8xKzAbcDef')).toBe('8xKzAbcDef');
    });

    it('decodes legacy sol-prefixed agent ID', () => {
      expect(decodeAgentId('sol:8xKzAbcDef')).toBe('8xKzAbcDef');
    });

    it('returns null for invalid agent ID', () => {
      expect(decodeAgentId('')).toBeNull();
      expect(decodeAgentId('eth:0x123')).toBeNull();
      expect(decodeAgentId('sol:')).toBeNull();
      expect(decodeAgentId('sol:a:b')).toBeNull();
    });

    it('roundtrips agent ID', () => {
      const asset = '7yRqBNXtest123456789012345678901234';
      expect(decodeAgentId(encodeAgentId(asset))).toBe(asset);
    });
  });

  describe('Feedback IDs', () => {
    it('encodes feedback ID', () => {
      expect(encodeFeedbackId('asset1', 'client1', 42n)).toBe('sol:asset1:client1:42');
      expect(encodeFeedbackId('asset1', 'client1', '0')).toBe('sol:asset1:client1:0');
    });

    it('decodes valid feedback ID', () => {
      const decoded = decodeFeedbackId('sol:asset1:client1:42');
      expect(decoded).toEqual({ asset: 'asset1', client: 'client1', index: '42' });
    });

    it('returns null for invalid feedback ID', () => {
      expect(decodeFeedbackId('sol:asset1:client1')).toBeNull();
      expect(decodeFeedbackId('asset1:client1:42')).toBeNull();
      expect(decodeFeedbackId('')).toBeNull();
    });
  });

  describe('Response IDs', () => {
    it('encodes response ID with full signature', () => {
      const id = encodeResponseId('asset1', 'client1', 0, 'resp1', '5UXfAbcDeFgHiJkLm');
      expect(id).toBe('sol:asset1:client1:0:resp1:5UXfAbcDeFgHiJkLm');
    });

    it('decodes valid response ID', () => {
      const decoded = decodeResponseId('sol:asset1:client1:0:resp1:5UXfAbcDeFgHiJkLm');
      expect(decoded).toEqual({
        asset: 'asset1',
        client: 'client1',
        index: '0',
        responder: 'resp1',
        sig: '5UXfAbcDeFgHiJkLm',
      });
    });

    it('returns null for invalid response ID', () => {
      expect(decodeResponseId('sol:asset1:client1:0')).toBeNull();
      expect(decodeResponseId('')).toBeNull();
    });
  });

  describe('Validation IDs', () => {
    it('encodes and decodes validation ID', () => {
      const encoded = encodeValidationId('asset1', 'validator1', 5n);
      expect(encoded).toBe('sol:asset1:validator1:5');

      const decoded = decodeValidationId(encoded);
      expect(decoded).toEqual({ asset: 'asset1', validator: 'validator1', nonce: '5' });
    });

    it('returns null for invalid validation ID', () => {
      expect(decodeValidationId('sol:asset1:validator1')).toBeNull();
    });
  });

  describe('Metadata IDs', () => {
    it('encodes and decodes metadata ID', () => {
      const encoded = encodeMetadataId('asset1', 'capabilities');
      expect(encoded).toBe('sol:asset1:capabilities');

      const decoded = decodeMetadataId(encoded);
      expect(decoded).toEqual({ asset: 'asset1', key: 'capabilities' });
    });

    it('returns null for invalid metadata ID', () => {
      expect(decodeMetadataId('sol:asset1')).toBeNull();
      expect(decodeMetadataId('')).toBeNull();
    });
  });
});
