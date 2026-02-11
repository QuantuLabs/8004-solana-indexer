/**
 * Additional coverage tests for compression utilities
 *
 * Targets uncovered lines:
 * - Lines 104-106: decompression errors (invalid ZSTD data, size limits)
 * - Lines 127-133: compressForStorageSync
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("@mongodb-js/zstd", () => ({
  compress: vi.fn(),
  decompress: vi.fn(),
}));

import { compress as zstdCompress, decompress as zstdDecompress } from "@mongodb-js/zstd";
import {
  compressForStorage,
  decompressFromStorage,
  compressForStorageSync,
  COMPRESSION_THRESHOLD,
  COMPRESSION_LEVEL,
  MAX_COMPRESS_SIZE,
  MAX_DECOMPRESS_SIZE,
} from "../../src/utils/compression.js";

const mockedCompress = vi.mocked(zstdCompress);
const mockedDecompress = vi.mocked(zstdDecompress);

describe("Compression Coverage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("exported constants", () => {
    it("should export COMPRESSION_THRESHOLD as 256", () => {
      expect(COMPRESSION_THRESHOLD).toBe(256);
    });

    it("should export COMPRESSION_LEVEL as 3", () => {
      expect(COMPRESSION_LEVEL).toBe(3);
    });

    it("should export MAX_COMPRESS_SIZE as 10KB", () => {
      expect(MAX_COMPRESS_SIZE).toBe(10 * 1024);
    });

    it("should export MAX_DECOMPRESS_SIZE as 1MB", () => {
      expect(MAX_DECOMPRESS_SIZE).toBe(1024 * 1024);
    });
  });

  describe("decompressFromStorage - error paths", () => {
    it("should throw 'Decompression failed' when zstd decompress rejects", async () => {
      mockedDecompress.mockRejectedValue(new Error("Invalid ZSTD frame"));

      // Build a buffer with ZSTD prefix (0x01) + small payload
      const invalidPayload = Buffer.concat([
        Buffer.from([0x01]),
        Buffer.from("not-valid-zstd-data"),
      ]);

      await expect(decompressFromStorage(invalidPayload)).rejects.toThrow(
        "Decompression failed: Invalid ZSTD frame"
      );
    });

    it("should throw 'Decompression failed' for generic non-Error throw from zstd", async () => {
      mockedDecompress.mockRejectedValue("raw string error");

      const invalidPayload = Buffer.concat([
        Buffer.from([0x01]),
        Buffer.from("bad-data"),
      ]);

      await expect(decompressFromStorage(invalidPayload)).rejects.toThrow(
        "Decompression failed"
      );
    });

    it("should throw when compressed payload exceeds MAX_COMPRESSED_SIZE", async () => {
      // Create a ZSTD-prefixed buffer where the compressed payload is > 10KB
      const oversizedPayload = Buffer.alloc(MAX_COMPRESS_SIZE + 100, 0xab);
      const prefixed = Buffer.concat([Buffer.from([0x01]), oversizedPayload]);

      await expect(decompressFromStorage(prefixed)).rejects.toThrow(
        `Compressed size ${oversizedPayload.length} exceeds limit ${MAX_COMPRESS_SIZE}`
      );

      // decompress should NOT have been called (rejected before decompression)
      expect(mockedDecompress).not.toHaveBeenCalled();
    });

    it("should throw when decompressed output exceeds MAX_DECOMPRESSED_SIZE", async () => {
      // Simulate a small compressed payload that decompresses to > 1MB (decompression bomb)
      const hugeOutput = Buffer.alloc(MAX_DECOMPRESS_SIZE + 1, 0xff);
      mockedDecompress.mockResolvedValue(hugeOutput);

      // Small compressed payload (within MAX_COMPRESSED_SIZE)
      const smallCompressed = Buffer.alloc(100, 0xcc);
      const prefixed = Buffer.concat([Buffer.from([0x01]), smallCompressed]);

      await expect(decompressFromStorage(prefixed)).rejects.toThrow(
        `Decompressed size ${hugeOutput.length} exceeds limit ${MAX_DECOMPRESS_SIZE}`
      );
    });

    it("should successfully decompress when output is exactly at MAX_DECOMPRESSED_SIZE", async () => {
      const exactLimitOutput = Buffer.alloc(MAX_DECOMPRESS_SIZE, 0xaa);
      mockedDecompress.mockResolvedValue(exactLimitOutput);

      const smallCompressed = Buffer.alloc(50, 0xbb);
      const prefixed = Buffer.concat([Buffer.from([0x01]), smallCompressed]);

      const result = await decompressFromStorage(prefixed);
      expect(result.length).toBe(MAX_DECOMPRESS_SIZE);
      expect(result).toBe(exactLimitOutput);
    });

    it("should successfully decompress when compressed payload is exactly at MAX_COMPRESSED_SIZE", async () => {
      const validOutput = Buffer.from("decompressed content");
      mockedDecompress.mockResolvedValue(validOutput);

      const exactSizePayload = Buffer.alloc(MAX_COMPRESS_SIZE, 0xdd);
      const prefixed = Buffer.concat([Buffer.from([0x01]), exactSizePayload]);

      const result = await decompressFromStorage(prefixed);
      expect(result.toString()).toBe("decompressed content");
    });
  });

  describe("compressForStorage - error path", () => {
    it("should fall back to raw storage when zstd compress throws", async () => {
      mockedCompress.mockRejectedValue(new Error("Compression library error"));

      // Data larger than threshold to trigger compression attempt
      const largeData = Buffer.alloc(COMPRESSION_THRESHOLD + 100, 0x42);
      const result = await compressForStorage(largeData);

      // Should have PREFIX_RAW (0x00) and original data
      expect(result[0]).toBe(0x00);
      expect(result.slice(1).equals(largeData)).toBe(true);
    });

    it("should store raw when compressed data is larger than original", async () => {
      // Simulate compression that makes data larger
      const largeData = Buffer.alloc(COMPRESSION_THRESHOLD + 10, 0x42);
      const biggerCompressed = Buffer.alloc(COMPRESSION_THRESHOLD + 100, 0x99);
      mockedCompress.mockResolvedValue(biggerCompressed);

      const result = await compressForStorage(largeData);

      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.slice(1).equals(largeData)).toBe(true);
    });

    it("should use ZSTD prefix when compression helps", async () => {
      const largeData = Buffer.alloc(COMPRESSION_THRESHOLD + 100, 0x42);
      const smallerCompressed = Buffer.alloc(50, 0x11);
      mockedCompress.mockResolvedValue(smallerCompressed);

      const result = await compressForStorage(largeData);

      expect(result[0]).toBe(0x01); // PREFIX_ZSTD
      expect(result.slice(1).equals(smallerCompressed)).toBe(true);
    });
  });

  describe("compressForStorageSync", () => {
    it("should return raw prefix for small data (below threshold)", () => {
      const smallData = Buffer.from("tiny");
      const result = compressForStorageSync(smallData);

      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.slice(1).toString()).toBe("tiny");
      expect(result.length).toBe(smallData.length + 1);
    });

    it("should return raw prefix for data exactly at threshold", () => {
      const atThreshold = Buffer.alloc(COMPRESSION_THRESHOLD, 0x61);
      const result = compressForStorageSync(atThreshold);

      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.slice(1).equals(atThreshold)).toBe(true);
    });

    it("should return raw prefix for large data (sync path does not compress)", () => {
      const largeData = Buffer.alloc(COMPRESSION_THRESHOLD + 500, 0x62);
      const result = compressForStorageSync(largeData);

      // Sync path always stores raw regardless of size
      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.slice(1).equals(largeData)).toBe(true);
      expect(result.length).toBe(largeData.length + 1);
    });

    it("should return raw prefix for empty data", () => {
      const emptyData = Buffer.alloc(0);
      const result = compressForStorageSync(emptyData);

      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.length).toBe(1);
    });

    it("should not call zstd compress (sync path avoids async compression)", () => {
      const largeData = Buffer.alloc(1024, 0x63);
      compressForStorageSync(largeData);

      // zstdCompress should NOT be called in sync path
      expect(mockedCompress).not.toHaveBeenCalled();
    });
  });
});
