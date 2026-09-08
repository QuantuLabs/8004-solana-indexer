import { readFileSync } from "node:fs";
import { Connection, MessageV1 } from "@solana/web3.js";
import { describe, expect, it, vi } from "vitest";

type RpcRequest = {
  id: string;
  method: string;
  params: [unknown, Record<string, unknown>];
};

const blockResponse = JSON.parse(
  readFileSync(new URL("../../fixtures/txv1/block-493978170.json", import.meta.url), "utf8")
);
const parsedResponse = JSON.parse(
  readFileSync(new URL("../../fixtures/txv1/v1-parsed-transaction.json", import.meta.url), "utf8")
);

describe("Web3 TxV1 RPC reads", () => {
  it("decodes a real mixed-version block and parsed V1 transaction with cap 1", async () => {
    const requests: RpcRequest[] = [];
    const fetchMock = vi.fn(async (_url: unknown, init?: { body?: unknown }) => {
      const request = JSON.parse(String(init?.body)) as RpcRequest | RpcRequest[];
      requests.push(...(Array.isArray(request) ? request : [request]));
      const reply = (item: RpcRequest) => ({
        ...(item.method === "getBlock" ? blockResponse : parsedResponse),
        id: item.id,
      });
      const body = Array.isArray(request) ? request.map(reply) : reply(request);
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const connection = new Connection("http://txv1.test", {
      commitment: "confirmed",
      fetch: fetchMock as never,
    });

    const block = await connection.getBlock(493978170, {
      transactionDetails: "full",
      maxSupportedTransactionVersion: 1,
    });
    const signature = parsedResponse.result.transaction.signatures[0] as string;
    const parsed = await connection.getParsedTransaction(signature, {
      maxSupportedTransactionVersion: 1,
    });
    const parsedBatch = await connection.getParsedTransactions([signature], {
      maxSupportedTransactionVersion: 1,
    });

    expect(block).not.toBeNull();
    const transactions = block!.transactions;
    expect(transactions).toHaveLength(35);
    expect(transactions.map((tx) => tx.version)).toEqual(
      expect.arrayContaining(["legacy", 0, 1])
    );
    expect(transactions.filter((tx) => tx.version === "legacy")).toHaveLength(24);
    expect(transactions.filter((tx) => tx.version === 0)).toHaveLength(2);
    expect(transactions.filter((tx) => tx.version === 1)).toHaveLength(9);
    expect(transactions.map((tx) => tx.transaction.signatures[0])).toEqual(
      blockResponse.result.transactions.map(
        (tx: { transaction: { signatures: string[] } }) => tx.transaction.signatures[0]
      )
    );

    const legacy = transactions.find((tx) => tx.version === "legacy")!;
    const v0 = transactions.find((tx) => tx.version === 0)!;
    const v1 = transactions.find((tx) => tx.version === 1)!;
    expect(legacy.transaction.message.version).toBe("legacy");
    expect(v0.transaction.message.version).toBe(0);
    expect(v1.transaction.message).toBeInstanceOf(MessageV1);
    expect(v1.transaction.message.version).toBe(1);
    expect((v1.transaction.message as MessageV1).transactionConfig).toEqual({
      computeUnitLimit: 1152240,
      heapSize: null,
      loadedAccountsDataSizeLimit: 2000000,
      priorityFee: null,
    });

    expect(parsed?.version).toBe(1);
    expect(parsed?.transaction.message.transactionConfig).toEqual(
      parsedResponse.result.transaction.message.transactionConfig
    );
    expect(parsed?.transaction.message.instructions).toHaveLength(1);
    expect(parsed?.meta?.innerInstructions?.[0].instructions[0]).toHaveProperty("stackHeight", 2);
    expect(parsed?.meta?.logMessages?.[0]).toBe(
      "Program BkKcxDw56L1dwFgSerUt5deWZheYpqFuLFnnSd9MAr9M invoke [1]"
    );
    expect(parsedBatch).toHaveLength(1);
    expect(parsedBatch[0]?.transaction.message.transactionConfig).toEqual(
      parsedResponse.result.transaction.message.transactionConfig
    );

    expect(requests).toHaveLength(3);
    expect(requests.map((request) => request.params[1].maxSupportedTransactionVersion)).toEqual([
      1,
      1,
      1,
    ]);
    expect(requests.slice(1).map((request) => request.params[1].encoding)).toEqual([
      "jsonParsed",
      "jsonParsed",
    ]);
  });
});
