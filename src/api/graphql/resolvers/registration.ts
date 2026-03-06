import type { GraphQLContext } from '../context.js';
import type { RegistrationRow } from '../dataloaders.js';
import { decompressFromStorage } from '../../../utils/compression.js';
import { createChildLogger } from '../../../logger.js';

const logger = createChildLogger('graphql-registration');

async function safeDecompress(value: Buffer): Promise<string | null> {
  try {
    const buf = await decompressFromStorage(value);
    return buf.toString('utf-8');
  } catch (err) {
    logger.warn({ error: (err as Error).message }, 'Failed to decompress registration field');
    return null;
  }
}

export interface RegistrationParent {
  _asset: string;
}

interface ServiceEntry {
  name?: string;
  type?: string;
  endpoint?: string;
  version?: string;
  tools?: string[];
  skills?: string[];
  mcpTools?: string[];
  a2aSkills?: string[];
  domains?: string[];
}

interface ParsedRegistration {
  fields: Map<string, string>;
  services: ServiceEntry[];
}

const CANONICAL_SERVICE_NAMES = new Set([
  'mcp',
  'a2a',
  'oasf',
  'ens',
  'sns',
  'did',
  'agentwallet',
  'wallet',
]);

const registrationCache = new WeakMap<GraphQLContext, Map<string, Promise<ParsedRegistration>>>();

function getRequestCache(ctx: GraphQLContext): Map<string, Promise<ParsedRegistration>> {
  const cached = registrationCache.get(ctx);
  if (cached) return cached;
  const created = new Map<string, Promise<ParsedRegistration>>();
  registrationCache.set(ctx, created);
  return created;
}

async function parseRegistrationRows(rows: RegistrationRow[]): Promise<ParsedRegistration> {
  const fields = new Map<string, string>();

  await Promise.all(rows.map(async (row) => {
    const bytes = row.value instanceof Buffer ? row.value : Buffer.from(row.value);
    const value = await safeDecompress(bytes);
    if (value !== null) {
      fields.set(row.key, value);
    }
  }));

  let services: ServiceEntry[] = [];
  const servicesRaw = fields.get('_uri:services');
  if (servicesRaw) {
    try {
      const parsed = JSON.parse(servicesRaw);
      if (Array.isArray(parsed)) {
        services = parsed;
      }
    } catch {
      services = [];
    }
  }

  return { fields, services };
}

async function getParsedRegistration(asset: string, ctx: GraphQLContext): Promise<ParsedRegistration> {
  const requestCache = getRequestCache(ctx);
  const existing = requestCache.get(asset);
  if (existing) return existing;

  const parsePromise = ctx.loaders.registrationByAgent
    .load(asset)
    .then(rows => parseRegistrationRows(rows));

  requestCache.set(asset, parsePromise);
  return parsePromise;
}

function parseBoolean(raw: string | undefined): boolean | null {
  if (raw === undefined) return null;
  const value = raw.trim().toLowerCase();
  if (value === 'true' || value === '1') return true;
  if (value === 'false' || value === '0') return false;
  return null;
}

function parseStringArray(raw: string | undefined): string[] | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed) && parsed.every(v => typeof v === 'string')) {
      return parsed;
    }
    return null;
  } catch {
    return null;
  }
}

function normalizeServiceName(name: unknown): string | null {
  if (typeof name !== 'string') return null;
  const normalized = name.trim().toLowerCase();
  return normalized.length > 0 ? normalized : null;
}

function canonicalServiceKind(service: ServiceEntry): string | null {
  const name = normalizeServiceName(service.name);
  if (name && CANONICAL_SERVICE_NAMES.has(name)) {
    return name;
  }
  const type = normalizeServiceName(service.type);
  if (type && CANONICAL_SERVICE_NAMES.has(type)) {
    return type;
  }
  return null;
}

function findService(services: ServiceEntry[], svcName: string): ServiceEntry | undefined {
  const target = normalizeServiceName(svcName);
  if (!target) return undefined;

  return services.find((s) => canonicalServiceKind(s) === target);
}

function getServiceTools(svc: ServiceEntry | undefined): string[] | null {
  return svc?.mcpTools ?? svc?.tools ?? null;
}

function getServiceSkills(svc: ServiceEntry | undefined): string[] | null {
  return svc?.a2aSkills ?? svc?.skills ?? null;
}

function gatherServiceSkills(services: ServiceEntry[]): string[] {
  const allSkills: string[] = [];
  for (const s of services) {
    if (canonicalServiceKind(s) !== 'oasf') continue;
    if (Array.isArray(s.skills)) {
      for (const skill of s.skills) {
        if (typeof skill === 'string') allSkills.push(skill);
      }
    }
  }
  return allSkills;
}

function gatherServiceDomains(services: ServiceEntry[]): string[] {
  const allDomains: string[] = [];
  for (const s of services) {
    if (canonicalServiceKind(s) !== 'oasf') continue;
    if (Array.isArray(s.domains)) {
      for (const domain of s.domains) {
        if (typeof domain === 'string') allDomains.push(domain);
      }
    }
  }
  return allDomains;
}

export const registrationResolvers = {
  AgentRegistrationFile: {
    async name(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return parsed.fields.get('_uri:name') ?? null;
    },
    async description(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return parsed.fields.get('_uri:description') ?? null;
    },
    async image(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return parsed.fields.get('_uri:image') ?? null;
    },
    async active(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return parseBoolean(parsed.fields.get('_uri:active'));
    },
    async x402Support(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return parseBoolean(parsed.fields.get('_uri:x402_support'));
    },
    async mcpEndpoint(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      const mcp = findService(parsed.services, 'mcp');
      return mcp?.endpoint ?? null;
    },
    async mcpTools(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      const mcp = findService(parsed.services, 'mcp');
      return getServiceTools(mcp);
    },
    async a2aEndpoint(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      const a2a = findService(parsed.services, 'a2a');
      return a2a?.endpoint ?? null;
    },
    async a2aSkills(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      const a2a = findService(parsed.services, 'a2a');
      return getServiceSkills(a2a);
    },
    async oasfSkills(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      const allSkills = gatherServiceSkills(parsed.services);
      if (allSkills.length > 0) return allSkills;
      return parseStringArray(parsed.fields.get('_uri:skills'));
    },
    async oasfDomains(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      const allDomains = gatherServiceDomains(parsed.services);
      if (allDomains.length > 0) return allDomains;
      return parseStringArray(parsed.fields.get('_uri:domains'));
    },
    async hasOASF(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return (
        gatherServiceSkills(parsed.services).length > 0
        || gatherServiceDomains(parsed.services).length > 0
        || !!(parsed.fields.get('_uri:skills') || parsed.fields.get('_uri:domains'))
      );
    },
    async supportedTrusts(parent: RegistrationParent, _args: unknown, ctx: GraphQLContext) {
      const parsed = await getParsedRegistration(parent._asset, ctx);
      return parseStringArray(parsed.fields.get('_uri:supported_trust'));
    },
  },
};
