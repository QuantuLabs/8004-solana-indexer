/**
 * Tests for /rest/v1/metadata API endpoint logic
 * Tests the transformation logic without requiring HTTP server
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// Helper function to parse PostgREST value (mirrors server.ts logic)
function parsePostgRESTValue(value: string | undefined): string | undefined {
  if (!value) return undefined;
  if (value.startsWith('eq.')) return value.slice(3);
  if (value.startsWith('neq.')) return value.slice(4);
  return value;
}

// Metadata response formatter (mirrors server.ts logic)
function formatMetadataResponse(metadata: Array<{
  agentId: string;
  key: string;
  value: Buffer | Uint8Array;
  immutable: boolean;
}>) {
  return metadata.map((m) => ({
    id: `${m.agentId}:${m.key}`,
    asset: m.agentId,
    key: m.key,
    value: Buffer.from(m.value).toString('base64'),
    immutable: m.immutable,
  }));
}

describe('Metadata API Endpoint Logic', () => {
  describe('parsePostgRESTValue', () => {
    it('should handle undefined', () => {
      expect(parsePostgRESTValue(undefined)).toBeUndefined();
    });

    it('should extract value from eq.prefix', () => {
      expect(parsePostgRESTValue('eq.agent123')).toBe('agent123');
    });

    it('should extract value from neq.prefix', () => {
      expect(parsePostgRESTValue('neq.excluded')).toBe('excluded');
    });

    it('should return raw value if no prefix', () => {
      expect(parsePostgRESTValue('rawvalue')).toBe('rawvalue');
    });
  });

  describe('formatMetadataResponse', () => {
    it('should format metadata in PostgREST format', () => {
      const mockData = [
        {
          agentId: 'agent123',
          key: '_uri:name',
          value: Buffer.from([0x00, 0x54, 0x65, 0x73, 0x74]), // PREFIX_RAW + "Test"
          immutable: false,
        },
        {
          agentId: 'agent123',
          key: '_uri:description',
          value: Buffer.from([0x00, 0x44, 0x65, 0x73, 0x63]), // PREFIX_RAW + "Desc"
          immutable: true,
        },
      ];

      const result = formatMetadataResponse(mockData);

      expect(result).toHaveLength(2);
      expect(result[0].asset).toBe('agent123');
      expect(result[0].key).toBe('_uri:name');
      expect(result[0].id).toBe('agent123:_uri:name');
      expect(result[0].immutable).toBe(false);
      expect(typeof result[0].value).toBe('string'); // Base64

      expect(result[1].id).toBe('agent123:_uri:description');
      expect(result[1].immutable).toBe(true);
    });

    it('should handle empty array', () => {
      expect(formatMetadataResponse([])).toEqual([]);
    });

    it('should encode value as base64', () => {
      const mockData = [{
        agentId: 'test',
        key: 'test:key',
        value: Buffer.from([0x00, 0x48, 0x65, 0x6c, 0x6c, 0x6f]), // PREFIX_RAW + "Hello"
        immutable: false,
      }];

      const result = formatMetadataResponse(mockData);

      // Decode base64 and verify
      const decoded = Buffer.from(result[0].value, 'base64');
      expect(decoded[0]).toBe(0x00); // PREFIX_RAW
      expect(decoded.slice(1).toString()).toBe('Hello');
    });

    it('should handle Uint8Array values', () => {
      const mockData = [{
        agentId: 'test',
        key: 'test:key',
        value: new Uint8Array([0x00, 0x54, 0x65, 0x73, 0x74]),
        immutable: false,
      }];

      const result = formatMetadataResponse(mockData);
      expect(result[0].value).toBeDefined();
      expect(typeof result[0].value).toBe('string');
    });
  });
});
