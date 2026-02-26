import {
  type DocumentNode,
  type FieldNode,
  Kind,
  visit,
} from 'graphql';

const DEFAULT_MAX_COMPLEXITY = 500;
const DEFAULT_MAX_FIRST_CAP = 250;

function parsePositiveIntEnv(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

const MAX_COMPLEXITY = parsePositiveIntEnv(
  process.env.GRAPHQL_MAX_COMPLEXITY,
  DEFAULT_MAX_COMPLEXITY,
);
const MAX_FIRST_CAP = parsePositiveIntEnv(
  process.env.GRAPHQL_MAX_FIRST_CAP,
  DEFAULT_MAX_FIRST_CAP,
);
const MAX_ALIASES = 10;

const FIELD_COSTS: Record<string, number> = {
  agents: 2,
  feedbacks: 2,
  feedbackResponses: 2,
  validations: 2,
  agentMetadatas: 2,
  feedback: 3,
  responses: 2,
  registrationFile: 3,
  globalStats: 5,
  protocol: 5,
  protocols: 5,
  agentSearch: 3,
  agentRegistrationFiles: 2,
  hashChainHeads: 5,
  hashChainLatestCheckpoints: 5,
  hashChainReplayData: 8,
};

const LIST_FIELDS = new Set([
  'agents', 'feedbacks', 'feedbackResponses', 'validations',
  'agentMetadatas', 'protocols', 'agentSearch', 'agentRegistrationFiles',
  'feedback', 'responses', 'metadata',
  'hashChainReplayData',
]);

function getFirstArg(node: FieldNode): number {
  const firstArg = node.arguments?.find(a => a.name.value === 'first');
  if (!firstArg) return 100;
  if (firstArg.value.kind === Kind.INT) {
    const requested = Number.parseInt(firstArg.value.value, 10);
    if (!Number.isFinite(requested) || requested <= 0) return 0;
    return Math.min(requested, MAX_FIRST_CAP);
  }
  if (firstArg.value.kind === Kind.VARIABLE) {
    // Variables are unknown at parse-time; assume worst-case page size.
    return MAX_FIRST_CAP;
  }
  return 100;
}

export function calculateComplexity(document: DocumentNode): number {
  let cost = 0;
  const multiplierStack: number[] = [1];

  visit(document, {
    Field: {
      enter(node: FieldNode) {
        const parentMultiplier = multiplierStack[multiplierStack.length - 1] ?? 1;
        const fieldName = node.name.value;
        const baseCost = FIELD_COSTS[fieldName] ?? 0;
        const listMultiplier = LIST_FIELDS.has(fieldName) ? getFirstArg(node) : 1;
        const currentMultiplier = parentMultiplier * listMultiplier;

        if (baseCost > 0) {
          cost += baseCost * currentMultiplier;
        } else if (node.selectionSet) {
          cost += parentMultiplier;
        }
        multiplierStack.push(currentMultiplier);
      },
      leave() {
        multiplierStack.pop();
      },
    },
  });

  return cost;
}

export function countAliases(document: DocumentNode): number {
  let aliasCount = 0;
  visit(document, {
    Field(node: FieldNode) {
      if (node.alias) aliasCount++;
    },
  });
  return aliasCount;
}

export interface ComplexityResult {
  allowed: boolean;
  cost: number;
  maxCost: number;
  reason?: string;
}

export function analyzeQuery(document: DocumentNode): ComplexityResult {
  const aliases = countAliases(document);
  if (aliases > MAX_ALIASES) {
    return {
      allowed: false,
      cost: 0,
      maxCost: MAX_COMPLEXITY,
      reason: `Query uses ${aliases} aliases (max ${MAX_ALIASES})`,
    };
  }

  const cost = calculateComplexity(document);
  if (cost > MAX_COMPLEXITY) {
    return {
      allowed: false,
      cost,
      maxCost: MAX_COMPLEXITY,
      reason: `Query complexity ${cost} exceeds maximum ${MAX_COMPLEXITY}`,
    };
  }

  return { allowed: true, cost, maxCost: MAX_COMPLEXITY };
}

export { MAX_COMPLEXITY, MAX_FIRST_CAP, MAX_ALIASES };
