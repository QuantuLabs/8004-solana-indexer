import { decodeAgentId, decodeFeedbackId } from './ids.js';

interface FilterConfig {
  graphqlField: string;
  dbColumn: string;
  operator: 'eq' | 'in' | 'gt' | 'gte' | 'lt' | 'lte' | 'bool' | 'feedbackRef' | 'validationStatus';
}

const AGENT_FILTERS: FilterConfig[] = [
  { graphqlField: 'id', dbColumn: 'asset', operator: 'eq' },
  { graphqlField: 'id_in', dbColumn: 'asset', operator: 'in' },
  { graphqlField: 'agentId', dbColumn: 'agent_id', operator: 'eq' },
  { graphqlField: 'agentid', dbColumn: 'agent_id', operator: 'eq' },
  { graphqlField: 'owner', dbColumn: 'owner', operator: 'eq' },
  { graphqlField: 'owner_in', dbColumn: 'owner', operator: 'in' },
  { graphqlField: 'creator', dbColumn: 'creator', operator: 'eq' },
  { graphqlField: 'agentWallet', dbColumn: 'agent_wallet', operator: 'eq' },
  { graphqlField: 'collection', dbColumn: 'collection', operator: 'eq' },
  { graphqlField: 'collectionPointer', dbColumn: 'canonical_col', operator: 'eq' },
  { graphqlField: 'parentAsset', dbColumn: 'parent_asset', operator: 'eq' },
  { graphqlField: 'parentCreator', dbColumn: 'parent_creator', operator: 'eq' },
  { graphqlField: 'atomEnabled', dbColumn: 'atom_enabled', operator: 'bool' },
  { graphqlField: 'colLocked', dbColumn: 'col_locked', operator: 'bool' },
  { graphqlField: 'parentLocked', dbColumn: 'parent_locked', operator: 'bool' },
  { graphqlField: 'trustTier_gte', dbColumn: 'trust_tier', operator: 'gte' },
  { graphqlField: 'totalFeedback_gt', dbColumn: 'COALESCE(adc.digest_feedback_count, a.feedback_count, 0)', operator: 'gt' },
  { graphqlField: 'totalFeedback_gte', dbColumn: 'COALESCE(adc.digest_feedback_count, a.feedback_count, 0)', operator: 'gte' },
  { graphqlField: 'createdAt_gt', dbColumn: 'created_at', operator: 'gt' },
  { graphqlField: 'createdAt_lt', dbColumn: 'created_at', operator: 'lt' },
  { graphqlField: 'updatedAt_gt', dbColumn: 'updated_at', operator: 'gt' },
  { graphqlField: 'updatedAt_lt', dbColumn: 'updated_at', operator: 'lt' },
];

const FEEDBACK_FILTERS: FilterConfig[] = [
  { graphqlField: 'agent', dbColumn: 'asset', operator: 'eq' },
  { graphqlField: 'feedbackId', dbColumn: 'feedback_id', operator: 'eq' },
  { graphqlField: 'feedbackId_gt', dbColumn: 'feedback_id', operator: 'gt' },
  { graphqlField: 'feedbackId_gte', dbColumn: 'feedback_id', operator: 'gte' },
  { graphqlField: 'feedbackId_lt', dbColumn: 'feedback_id', operator: 'lt' },
  { graphqlField: 'feedbackId_lte', dbColumn: 'feedback_id', operator: 'lte' },
  { graphqlField: 'clientAddress', dbColumn: 'client_address', operator: 'eq' },
  { graphqlField: 'tag1', dbColumn: 'tag1', operator: 'eq' },
  { graphqlField: 'tag2', dbColumn: 'tag2', operator: 'eq' },
  { graphqlField: 'endpoint', dbColumn: 'endpoint', operator: 'eq' },
  { graphqlField: 'isRevoked', dbColumn: 'is_revoked', operator: 'bool' },
  { graphqlField: 'createdAt_gt', dbColumn: 'created_at', operator: 'gt' },
  { graphqlField: 'createdAt_lt', dbColumn: 'created_at', operator: 'lt' },
];

const RESPONSE_FILTERS: FilterConfig[] = [
  { graphqlField: 'feedback', dbColumn: '', operator: 'feedbackRef' },
  { graphqlField: 'responseId', dbColumn: 'response_id', operator: 'eq' },
  { graphqlField: 'responseId_gt', dbColumn: 'response_id', operator: 'gt' },
  { graphqlField: 'responseId_gte', dbColumn: 'response_id', operator: 'gte' },
  { graphqlField: 'responseId_lt', dbColumn: 'response_id', operator: 'lt' },
  { graphqlField: 'responseId_lte', dbColumn: 'response_id', operator: 'lte' },
  { graphqlField: 'responder', dbColumn: 'responder', operator: 'eq' },
  { graphqlField: 'createdAt_gt', dbColumn: 'created_at', operator: 'gt' },
  { graphqlField: 'createdAt_lt', dbColumn: 'created_at', operator: 'lt' },
];

const REVOCATION_FILTERS: FilterConfig[] = [
  { graphqlField: 'agent', dbColumn: 'asset', operator: 'eq' },
  { graphqlField: 'revocationId', dbColumn: 'revocation_id', operator: 'eq' },
  { graphqlField: 'revocationId_gt', dbColumn: 'revocation_id', operator: 'gt' },
  { graphqlField: 'revocationId_gte', dbColumn: 'revocation_id', operator: 'gte' },
  { graphqlField: 'revocationId_lt', dbColumn: 'revocation_id', operator: 'lt' },
  { graphqlField: 'revocationId_lte', dbColumn: 'revocation_id', operator: 'lte' },
  { graphqlField: 'clientAddress', dbColumn: 'client_address', operator: 'eq' },
  { graphqlField: 'feedbackIndex', dbColumn: 'feedback_index', operator: 'eq' },
  { graphqlField: 'createdAt_gt', dbColumn: 'created_at', operator: 'gt' },
  { graphqlField: 'createdAt_lt', dbColumn: 'created_at', operator: 'lt' },
];

const VALIDATION_FILTERS: FilterConfig[] = [
  { graphqlField: 'agent', dbColumn: 'asset', operator: 'eq' },
  { graphqlField: 'validatorAddress', dbColumn: 'validator_address', operator: 'eq' },
  { graphqlField: 'status', dbColumn: '', operator: 'validationStatus' },
];

const METADATA_FILTERS: FilterConfig[] = [
  { graphqlField: 'agent', dbColumn: 'asset', operator: 'eq' },
  { graphqlField: 'key', dbColumn: 'key', operator: 'eq' },
];

const FILTER_MAP: Record<string, FilterConfig[]> = {
  agent: AGENT_FILTERS,
  feedback: FEEDBACK_FILTERS,
  response: RESPONSE_FILTERS,
  revocation: REVOCATION_FILTERS,
  validation: VALIDATION_FILTERS,
  metadata: METADATA_FILTERS,
};

const TIMESTAMP_OPERATORS = new Set(['gt', 'gte', 'lt', 'lte']);

const OPERATOR_SQL: Record<string, string> = {
  eq: '=',
  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',
};

const ID_FIELDS = new Set(['id', 'agent']);
const MAX_IN_FILTER_VALUES = 250;

export interface WhereClause {
  sql: string;
  params: unknown[];
  paramIndex: number;
}

function resolveIdValue(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  return decodeAgentId(value);
}

function resolveIdArray(values: unknown): string[] | null {
  if (!Array.isArray(values)) return null;
  const resolved: string[] = [];
  for (const v of values) {
    if (typeof v !== 'string') return null;
    const decoded = decodeAgentId(v);
    if (decoded === null) return null;
    resolved.push(decoded);
  }
  return resolved;
}

function normalizeSqlParam(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeSqlParam(item));
  }
  return value;
}

export function buildWhereClause(
  entityType: 'agent' | 'feedback' | 'response' | 'revocation' | 'validation' | 'metadata',
  filter: Record<string, unknown> | undefined | null,
  startParamIndex?: number,
): WhereClause {
  const conditions: string[] = [];
  const params: unknown[] = [];
  const pushParam = (value: unknown): void => {
    params.push(normalizeSqlParam(value));
  };
  let idx = startParamIndex ?? 1;

  const allowedFilters = FILTER_MAP[entityType];
  if (!allowedFilters) {
    const statusCol = entityType === 'validation' ? 'chain_status' : 'status';
    return {
      sql: `WHERE ${statusCol} != 'ORPHANED'`,
      params: [],
      paramIndex: idx,
    };
  }

  if (filter) {
    const configByField = new Map<string, FilterConfig>();
    for (const cfg of allowedFilters) {
      configByField.set(cfg.graphqlField, cfg);
    }

    for (const [field, value] of Object.entries(filter)) {
      if (value === undefined || value === null) continue;

      const cfg = configByField.get(field);
      if (!cfg) continue;

      if (cfg.operator === 'feedbackRef') {
        if (typeof value !== 'string') continue;
        const canonicalFeedbackRef = decodeFeedbackId(value);
        if (canonicalFeedbackRef && /^-?\d+$/.test(canonicalFeedbackRef.index)) {
          conditions.push(
            `asset = $${idx} AND client_address = $${idx + 1} AND feedback_index = $${idx + 2}::bigint`
          );
          pushParam(canonicalFeedbackRef.asset);
          pushParam(canonicalFeedbackRef.client);
          pushParam(canonicalFeedbackRef.index);
          idx += 3;
          continue;
        }
        if (/^-?\d+$/.test(value)) {
          conditions.push(`EXISTS (
            SELECT 1
            FROM feedbacks f
            WHERE f.asset = feedback_responses.asset
              AND f.client_address = feedback_responses.client_address
              AND f.feedback_index = feedback_responses.feedback_index
              AND f.feedback_id = $${idx}::bigint
          )`);
          pushParam(value);
          idx++;
        }
        continue;
      }

      if (cfg.operator === 'validationStatus') {
        if (typeof value !== 'string') continue;
        if (value === 'PENDING') {
          conditions.push('response IS NULL');
        } else if (value === 'COMPLETED') {
          conditions.push('response IS NOT NULL');
        } else if (value === 'EXPIRED') {
          conditions.push('FALSE');
        }
        continue;
      }

      if (cfg.operator === 'in') {
        const resolved = ID_FIELDS.has(field.replace(/_in$/, ''))
          ? resolveIdArray(value)
          : (Array.isArray(value) ? value as string[] : null);
        if (!resolved || resolved.length === 0) continue;
        if (resolved.length > MAX_IN_FILTER_VALUES) {
          const trimmed = resolved.slice(0, MAX_IN_FILTER_VALUES);
          conditions.push(`${cfg.dbColumn} = ANY($${idx}::text[])`);
          pushParam(trimmed);
          idx++;
          continue;
        }
        conditions.push(`${cfg.dbColumn} = ANY($${idx}::text[])`);
        pushParam(resolved);
        idx++;
        continue;
      }

      if (cfg.operator === 'bool') {
        conditions.push(`${cfg.dbColumn} = $${idx}`);
        pushParam(Boolean(value));
        idx++;
        continue;
      }

      if (cfg.operator === 'eq') {
        let resolved: unknown = value;
        if (ID_FIELDS.has(field)) {
          resolved = resolveIdValue(value);
          if (resolved === null) continue;
        }
        conditions.push(`${cfg.dbColumn} = $${idx}`);
        pushParam(resolved);
        idx++;
        continue;
      }

      const sqlOp = OPERATOR_SQL[cfg.operator];
      if (!sqlOp) continue;

      if (TIMESTAMP_OPERATORS.has(cfg.operator) && cfg.dbColumn.endsWith('_at')) {
        conditions.push(`${cfg.dbColumn} ${sqlOp} to_timestamp($${idx})`);
        pushParam(Number(value));
        idx++;
      } else {
        conditions.push(`${cfg.dbColumn} ${sqlOp} $${idx}`);
        pushParam(value);
        idx++;
      }
    }
  }

  const statusCol = entityType === 'validation' ? 'chain_status' : 'status';
  conditions.push(`${statusCol} != 'ORPHANED'`);

  const sql = conditions.length > 0
    ? `WHERE ${conditions.join(' AND ')}`
    : `WHERE ${statusCol} != 'ORPHANED'`;

  return { sql, params, paramIndex: idx };
}
