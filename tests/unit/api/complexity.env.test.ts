import { afterEach, describe, expect, it, vi } from 'vitest';
import { parse } from 'graphql';

const ORIGINAL_MAX_COMPLEXITY = process.env.GRAPHQL_MAX_COMPLEXITY;
const ORIGINAL_MAX_FIRST_CAP = process.env.GRAPHQL_MAX_FIRST_CAP;

afterEach(() => {
  if (ORIGINAL_MAX_COMPLEXITY === undefined) {
    delete process.env.GRAPHQL_MAX_COMPLEXITY;
  } else {
    process.env.GRAPHQL_MAX_COMPLEXITY = ORIGINAL_MAX_COMPLEXITY;
  }
  if (ORIGINAL_MAX_FIRST_CAP === undefined) {
    delete process.env.GRAPHQL_MAX_FIRST_CAP;
  } else {
    process.env.GRAPHQL_MAX_FIRST_CAP = ORIGINAL_MAX_FIRST_CAP;
  }
  vi.resetModules();
});

describe('GraphQL complexity env parsing', () => {
  it('falls back to safe defaults when env values are invalid', async () => {
    process.env.GRAPHQL_MAX_COMPLEXITY = 'not-a-number';
    process.env.GRAPHQL_MAX_FIRST_CAP = 'nope';
    vi.resetModules();

    const mod = await import('../../../src/api/graphql/plugins/complexity.js');
    expect(mod.MAX_COMPLEXITY).toBe(500);
    expect(mod.MAX_FIRST_CAP).toBe(250);

    const doc = parse(`{
      a1: agents(first: 250) { id feedback(first: 250) { id responses { id } } }
      a2: agents(first: 250) { id feedback(first: 250) { id responses { id } } }
      a3: agents(first: 250) { id feedback(first: 250) { id responses { id } } }
    }`);
    const result = mod.analyzeQuery(doc);
    expect(result.allowed).toBe(false);
    expect(Number.isFinite(result.cost)).toBe(true);
  });

  it('uses configured limits when env values are valid', async () => {
    process.env.GRAPHQL_MAX_COMPLEXITY = '1200';
    process.env.GRAPHQL_MAX_FIRST_CAP = '400';
    vi.resetModules();

    const mod = await import('../../../src/api/graphql/plugins/complexity.js');
    expect(mod.MAX_COMPLEXITY).toBe(1200);
    expect(mod.MAX_FIRST_CAP).toBe(400);

    const capped = mod.calculateComplexity(parse('{ agents(first: 9999) { id } }'));
    const atCap = mod.calculateComplexity(parse('{ agents(first: 400) { id } }'));
    expect(capped).toBe(atCap);
  });
});
