import { describe, expect, it, vi } from 'vitest';
import { registrationResolvers } from '../../../src/api/graphql/resolvers/registration.js';

function makeRegistrationCtx(services: unknown) {
  return {
    loaders: {
      registrationByAgent: {
        load: vi.fn().mockResolvedValue([
          {
            asset: 'Asset111',
            key: '_uri:services',
            value: Buffer.concat([Buffer.from([0x00]), Buffer.from(JSON.stringify(services), 'utf8')]),
          },
        ]),
      },
    },
  } as any;
}

describe('Registration Resolver Service Matching', () => {
  it('matches services by name/type case-insensitively', async () => {
    const ctx = makeRegistrationCtx([
      { name: 'MCP', endpoint: 'https://mcp.example.com', mcpTools: ['search'] },
      { type: 'A2A', endpoint: 'https://a2a.example.com', a2aSkills: ['summarize'] },
    ]);
    const parent = { _asset: 'Asset111' };

    const mcpEndpoint = await registrationResolvers.AgentRegistrationFile.mcpEndpoint(parent, {}, ctx);
    const mcpTools = await registrationResolvers.AgentRegistrationFile.mcpTools(parent, {}, ctx);
    const a2aEndpoint = await registrationResolvers.AgentRegistrationFile.a2aEndpoint(parent, {}, ctx);
    const a2aSkills = await registrationResolvers.AgentRegistrationFile.a2aSkills(parent, {}, ctx);

    expect(mcpEndpoint).toBe('https://mcp.example.com');
    expect(mcpTools).toEqual(['search']);
    expect(a2aEndpoint).toBe('https://a2a.example.com');
    expect(a2aSkills).toEqual(['summarize']);
  });

  it('falls back from mcpTools/a2aSkills to legacy tools/skills', async () => {
    const ctx = makeRegistrationCtx([
      { name: 'mcp', endpoint: 'https://mcp.example.com', tools: ['tool-a'] },
      { name: 'a2a', endpoint: 'https://a2a.example.com', skills: ['skill-a'] },
    ]);
    const parent = { _asset: 'Asset111' };

    const mcpTools = await registrationResolvers.AgentRegistrationFile.mcpTools(parent, {}, ctx);
    const a2aSkills = await registrationResolvers.AgentRegistrationFile.a2aSkills(parent, {}, ctx);

    expect(mcpTools).toEqual(['tool-a']);
    expect(a2aSkills).toEqual(['skill-a']);
  });

  it('uses type fallback when name is non-canonical and ignores non-string names safely', async () => {
    const ctx = makeRegistrationCtx([
      { name: 123, type: 'mcp', endpoint: 'https://mcp.example.com' },
      { name: 'A2A Service', type: 'a2a', endpoint: 'https://a2a.example.com' },
    ]);
    const parent = { _asset: 'Asset111' };

    const mcpEndpoint = await registrationResolvers.AgentRegistrationFile.mcpEndpoint(parent, {}, ctx);
    const a2aEndpoint = await registrationResolvers.AgentRegistrationFile.a2aEndpoint(parent, {}, ctx);

    expect(mcpEndpoint).toBe('https://mcp.example.com');
    expect(a2aEndpoint).toBe('https://a2a.example.com');
  });

  it('does not surface a2aSkills as oasfSkills', async () => {
    const ctx = makeRegistrationCtx([
      { name: 'a2a', endpoint: 'https://a2a.example.com', a2aSkills: ['summarize'] },
      { name: 'oasf', endpoint: 'https://oasf.example.com', skills: ['finance'] },
    ]);
    const parent = { _asset: 'Asset111' };

    const oasfSkills = await registrationResolvers.AgentRegistrationFile.oasfSkills(parent, {}, ctx);

    expect(oasfSkills).toEqual(['finance']);
  });

  it('resolves oasfDomains/hasOASF from OASF service payload and supportedTrusts from _uri field', async () => {
    const ctx = {
      loaders: {
        registrationByAgent: {
          load: vi.fn().mockResolvedValue([
            {
              asset: 'Asset111',
              key: '_uri:services',
              value: Buffer.concat([
                Buffer.from([0x00]),
                Buffer.from(JSON.stringify([
                  { name: 'oasf', endpoint: 'https://oasf.example.com', skills: ['ai/plan'], domains: ['finance/investment'] },
                ]), 'utf8'),
              ]),
            },
            {
              asset: 'Asset111',
              key: '_uri:supported_trust',
              value: Buffer.concat([Buffer.from([0x00]), Buffer.from(JSON.stringify(['reputation', 'tee-attestation']), 'utf8')]),
            },
          ]),
        },
      },
    } as any;
    const parent = { _asset: 'Asset111' };

    const oasfDomains = await registrationResolvers.AgentRegistrationFile.oasfDomains(parent, {}, ctx);
    const hasOASF = await registrationResolvers.AgentRegistrationFile.hasOASF(parent, {}, ctx);
    const supportedTrusts = await registrationResolvers.AgentRegistrationFile.supportedTrusts(parent, {}, ctx);

    expect(oasfDomains).toEqual(['finance/investment']);
    expect(hasOASF).toBe(true);
    expect(supportedTrusts).toEqual(['reputation', 'tee-attestation']);
  });
});
