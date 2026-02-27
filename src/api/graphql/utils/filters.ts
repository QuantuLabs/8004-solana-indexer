import { decodeAgentId, decodeFeedbackId } from './ids.js';

interface FilterConfig {
  graphqlField: string;
  dbColumn: string;
  operator: 'eq' | 'in' | 'gt' | 'gte' | 'lt' | 'lte' | 'bool' | 'feedbackRef' | 'validationStatus';
}

const AGENT_FILTERS: FilterConfig[] = [
  { graphqlField: 'id', dbColumn: 'asset', operator: 'eq' },
  { graphqlField: 'id_in', dbColumn: 'asset', operator: 'in' },
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
  { graphqlField: 'createdAt_gt', dbColumn: 'created_at', operator: 'gt' },
  { graphqlField: 'createdAt_lt', dbColumn: 'created_at', operator: 'lt' },
  { graphqlField: 'updatedAt_gt', dbColumn: 'updated_at', operator: 'gt' },
  { graphqlField: 'updatedAt_lt', dbColumn: 'updated_at', operator: 'lt' },
];

const FEEDBACK_FILTERS: FilterConfig[] = [
  { graphqlField: 'agent', dbColumn: 'asset', operator: 'eq' },
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
  { graphqlField: 'responder', dbColumn: 'responder', operator: 'eq' },
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

export function buildWhereClause(
  entityType: 'agent' | 'feedback' | 'response' | 'validation' | 'metadata',
  filter: Record<string, unknown> | undefined | null,
  startParamIndex?: number,
): WhereClause {
  const conditions: string[] = [];
  const params: unknown[] = [];
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
        const decoded = decodeFeedbackId(value) ?? (() => {
          const parts = value.split(':');
          if (parts.length === 3 && parts[0] && parts[1] && parts[2]) {
            return { asset: parts[0], client: parts[1], index: parts[2] };
          }
          return null;
        })();
        if (!decoded) continue;
        conditions.push(`asset = $${idx}`);
        params.push(decoded.asset);
        idx++;
        conditions.push(`client_address = $${idx}`);
        params.push(decoded.client);
        idx++;
        conditions.push(`feedback_index = $${idx}::bigint`);
        params.push(decoded.index);
        idx++;
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
          params.push(trimmed);
          idx++;
          continue;
        }
        conditions.push(`${cfg.dbColumn} = ANY($${idx}::text[])`);
        params.push(resolved);
        idx++;
        continue;
      }

      if (cfg.operator === 'bool') {
        conditions.push(`${cfg.dbColumn} = $${idx}`);
        params.push(Boolean(value));
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
        params.push(resolved);
        idx++;
        continue;
      }

      const sqlOp = OPERATOR_SQL[cfg.operator];
      if (!sqlOp) continue;

      if (TIMESTAMP_OPERATORS.has(cfg.operator) && cfg.dbColumn.endsWith('_at')) {
        conditions.push(`${cfg.dbColumn} ${sqlOp} to_timestamp($${idx})`);
        params.push(Number(value));
        idx++;
      } else {
        conditions.push(`${cfg.dbColumn} ${sqlOp} $${idx}`);
        params.push(value);
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
