import { existsSync, readFileSync } from 'node:fs';
import { mkdir, readFile, readdir, rename, writeFile } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createInterface } from 'node:readline/promises';
import process from 'node:process';

import {
      ensureLaneADirs,
      ensureLaneBDirs,
      ensureKnowledgeDirs,
} from '../paths/project-paths.js';
import { ensureKnowledgeStructure } from '../lane_a/knowledge/knowledge-utils.js';
import {
      withRegistryLock,
      loadRegistry,
      allocatePorts,
      upsertProject,
      getProject,
      writeRegistry,
} from '../registry/project-registry.js';
import {
      generateAgentsConfig,
      validateAgentsConfigCoversTeams,
} from '../project/agents-generator.js';

function normStr(x) {
      return typeof x === 'string' ? x.trim() : '';
}

function looksLikeRemoteRepoSpec(spec) {
      const s = normStr(spec);
      if (!s) return false;
      if (s.includes('://')) return true;
      if (s.startsWith('git@')) return true; // scp-ish
      if (/^[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+:/.test(s)) return true; // user@host:path
      return false;
}

function inferRepoDirNameFromSource(source) {
      const raw = normStr(source);
      if (!raw) return null;
      // Handle scp-style: git@host:org/repo(.git)
      const scpPath =
            raw.includes(':') && !raw.includes('://')
                  ? raw.split(':').slice(1).join(':')
                  : raw;
      const trimmed = scpPath.replace(/\/+$/, '');
      const base = trimmed.split('/').filter(Boolean).at(-1) || '';
      const noGit = base.toLowerCase().endsWith('.git')
            ? base.slice(0, -4)
            : base;
      const out = normStr(noGit);
      return out || null;
}

function normalizeRepoId(raw) {
      const s = String(raw || '')
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9-]+/g, '-')
            .replace(/-+/g, '-')
            .replace(/^-+|-+$/g, '');
      if (!s) return null;
      return s;
}

function isPlainObject(x) {
      return !!x && typeof x === 'object' && !Array.isArray(x);
}

function stableSort(arr, keyFn) {
      const withIdx = (Array.isArray(arr) ? arr : []).map((v, i) => ({
            v,
            i,
            k: keyFn(v),
      }));
      withIdx.sort(
            (a, b) =>
                  String(a.k).localeCompare(String(b.k)) ||
                  Number(a.i) - Number(b.i),
      );
      return withIdx.map((x) => x.v);
}

function tokenizeKeywords(folderName) {
      const raw = String(folderName || '').trim();
      const parts = raw.split(/[^A-Za-z0-9]+/g).filter(Boolean);
      const out = [];
      for (const p of parts) {
            const t = p.toLowerCase();
            if (!t) continue;
            if (!out.includes(t)) out.push(t);
      }
      return out;
}

function inferDomainsFromName(repoFolderName) {
      const p = String(repoFolderName || '').trim();
      const lower = p.toLowerCase();
      const domains = new Set();

      if (p.startsWith('DP_')) domains.add('DP');
      if (p === 'IDP' || lower.includes('idp')) domains.add('IDP');
      if (lower.includes('_gd_') || p.startsWith('TMS_GD_')) domains.add('GD');
      if (lower.includes('notification') || p.startsWith('Notification_'))
            domains.add('NTF');
      if (lower.includes('hexablox')) domains.add('HexaBlox');
      if (p.startsWith('TMS_Common_')) domains.add('Common');
      if (p.startsWith('TMS_') && !domains.has('GD')) domains.add('Core');
      if (p === 'HiveJS') domains.add('Core');
      if (p === 'DP_JSON-Master') domains.add('DP');

      if (domains.size === 0) domains.add('Core');
      const out = Array.from(domains);
      out.sort((a, b) => a.localeCompare(b));
      return out;
}

function parseDomainsInput(raw) {
      const allowed = new Set([
            'DP',
            'Core',
            'GD',
            'IDP',
            'NTF',
            'Media',
            'Common',
            'HexaBlox',
      ]);
      const parts = String(raw || '')
            .split(/[, ]+/g)
            .map((p) => p.trim())
            .filter(Boolean);
      const out = [];
      for (const p of parts) {
            const upper = p.toUpperCase();
            const canon =
                  upper === 'CORE'
                        ? 'Core'
                        : upper === 'COMMON'
                          ? 'Common'
                          : upper === 'MEDIA'
                            ? 'Media'
                            : upper === 'GD'
                              ? 'GD'
                              : upper === 'DP'
                                ? 'DP'
                                : upper === 'IDP'
                                  ? 'IDP'
                                  : upper === 'NTF'
                                    ? 'NTF'
                                    : upper === 'HEXABLOX'
                                      ? 'HexaBlox'
                                      : upper;
            if (!allowed.has(canon)) continue;
            if (!out.includes(canon)) out.push(canon);
      }
      out.sort((a, b) => a.localeCompare(b));
      return out;
}

function detectRepoFiles(repoPath) {
      const has = (p) => existsSync(resolve(repoPath, p));
      return {
            node: {
                  package_json: has('package.json'),
                  pnpm_lock: has('pnpm-lock.yaml'),
                  yarn_lock: has('yarn.lock'),
                  npm_lock: has('package-lock.json') || has('npm-shrinkwrap.json'),
                  nx: has('nx.json'),
                  turbo: has('turbo.json'),
            },
            python: {
                  pyproject: has('pyproject.toml'),
                  requirements: has('requirements.txt'),
                  tox: has('tox.ini'),
            },
            java: {
                  pom: has('pom.xml'),
                  gradle: has('build.gradle') || has('build.gradle.kts'),
            },
      };
}

function readPackageJsonScripts(repoPath) {
      const pkgPath = resolve(repoPath, 'package.json');
      if (!existsSync(pkgPath)) return { ok: true, scripts: {}, package_json: null };
      try {
            const text = readFileSync(pkgPath, 'utf8');
            const json = JSON.parse(String(text || ''));
            const scripts = isPlainObject(json?.scripts) ? json.scripts : {};
            return { ok: true, scripts, package_json: pkgPath };
      } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            return { ok: false, message: msg, scripts: {}, package_json: pkgPath };
      }
}

function inferNodeCommands({ files, scripts }) {
      const pm = files.node.pnpm_lock
            ? 'pnpm'
            : files.node.yarn_lock
              ? 'yarn'
              : files.node.package_json
                ? 'npm'
                : null;
      const hasScript = (k) => Object.prototype.hasOwnProperty.call(scripts || {}, k);

      const commands = {
            cwd: '.',
            package_manager: pm,
            install: null,
            lint: null,
            test: null,
            build: null,
      };
      if (!pm) return { ok: true, commands, missing: { node: true } };

      if (pm === 'pnpm')
            commands.install = files.node.pnpm_lock
                  ? 'pnpm install --frozen-lockfile'
                  : 'pnpm install';
      if (pm === 'yarn')
            commands.install = files.node.yarn_lock
                  ? 'yarn install --frozen-lockfile'
                  : 'yarn install';
      if (pm === 'npm')
            commands.install = files.node.npm_lock ? 'npm ci' : 'npm install';

      for (const k of ['lint', 'test', 'build']) {
            if (!hasScript(k)) continue;
            commands[k] =
                  pm === 'yarn'
                        ? `yarn ${k}`
                        : pm === 'pnpm'
                          ? `pnpm ${k}`
                          : `npm run ${k}`;
      }

      return {
            ok: true,
            commands,
            missing: {
                  lint: !hasScript('lint'),
                  test: !hasScript('test'),
                  build: !hasScript('build'),
            },
      };
}

function inferKind({ repoFolderName, files, scripts }) {
      const lower = String(repoFolderName || '').toLowerCase();
      const scriptKeys = Object.keys(scripts || {}).map((s) => s.toLowerCase());
      const isAppLike =
            lower.includes('frontend') ||
            lower.includes('portal') ||
            lower.includes('mobileapp') ||
            lower.includes('signup');
      const isToolLike =
            lower.includes('tool') ||
            lower.includes('hive') ||
            lower.includes('master') ||
            lower.includes('plugin');

      if (files.node.package_json) {
            if (scriptKeys.includes('dev') || scriptKeys.includes('start'))
                  return isAppLike ? 'App' : 'Service';
            if (lower.includes('library') || lower.includes('common'))
                  return 'Package';
            if (isToolLike) return 'Tool';
            return isAppLike ? 'App' : 'Package';
      }
      if (files.python.pyproject || files.python.requirements) return 'Service';
      if (files.java.pom || files.java.gradle) return 'Service';
      if (isToolLike) return 'Tool';
      return 'Service';
}

function inferUsageAndTeam({ repoFolderName, kind, domains, teams }) {
      const name = String(repoFolderName || '');
      const lower = name.toLowerCase();
      let usage = 'Tooling';
      if (lower.includes('mobileapp') || lower.includes('mobile')) usage = 'Mobile';
      else if (
            lower.includes('frontend') ||
            lower.includes('portal') ||
            lower.includes('ui') ||
            lower.includes('signup')
      )
            usage = 'Frontend';
      else if (lower.includes('backend') || lower.includes('api')) usage = 'Backend';
      else if (kind === 'App') usage = 'Frontend';

      const signals = [
            lower,
            String(kind).toLowerCase(),
            usage.toLowerCase(),
            (domains || []).join(' ').toLowerCase(),
      ].join(' ');
      let best = null;
      for (const t of teams || []) {
            const hints = Array.isArray(t.scope_hints) ? t.scope_hints : [];
            let score = 0;
            for (const h of hints) {
                  const needle = String(h || '').toLowerCase().trim();
                  if (!needle) continue;
                  if (signals.includes(needle)) score += 1;
            }
            const candidate = { team_id: t.team_id, score };
            if (!best) best = candidate;
            else if (candidate.score > best.score) best = candidate;
            else if (
                  candidate.score === best.score &&
                  String(candidate.team_id).localeCompare(String(best.team_id)) < 0
            )
                  best = candidate;
      }

      const teamIds = (Array.isArray(teams) ? teams : [])
            .map((t) => String(t?.team_id || '').trim())
            .filter(Boolean);
      const fallback =
            teamIds.includes('Tooling')
                  ? 'Tooling'
                  : teamIds.includes('BackendTMSCore')
                    ? 'BackendTMSCore'
                    : teamIds[0] || null;

      return {
            usage,
            team_id: best && best.score > 0 ? best.team_id : fallback,
      };
}

function defaultTeamsSuggested() {
      return [
            {
                  team_id: 'BackendTMSCore',
                  description:
                        'Backend services and APIs for the TMS Core domain (Core)',
                  scope_hints: [
                        'backend',
                        'api',
                        'service',
                        'endpoint',
                        'controller',
                        'nest',
                        'database',
                        'tms',
                        'core',
                        'ruleengine',
                  ],
                  risk_level: 'normal',
            },
            {
                  team_id: 'BackendGD',
                  description:
                        'Global Definition backend services and APIs (GD domain).',
                  scope_hints: [
                        'gd',
                        'global definition',
                        'definition',
                        'signup',
                        'tenant',
                        'media',
                        'catalog',
                  ],
                  risk_level: 'normal',
            },
            {
                  team_id: 'BackendPlatform',
                  description:
                        'Shared/platform backends outside Core/GD/IDP (e.g., Notification, VPS glue, utilities).',
                  scope_hints: [
                        'notification',
                        'message',
                        'msg',
                        'ntf',
                        'vps',
                        'glue',
                        'integration',
                        'webhook',
                        'scheduler',
                        'cron',
                        'common',
                        'shared',
                        'base',
                        'library',
                        'util',
                        'sdk',
                  ],
                  risk_level: 'normal',
            },
            {
                  team_id: 'FrontendDP',
                  description:
                        'Dynamic Portal frontend host and DP UI libraries (DP_* frontends).',
                  scope_hints: [
                        'dp',
                        'dynamic portal',
                        'dynamic pages',
                        'micro frontend',
                        'component json',
                        'dp json',
                        'dp portal',
                        'dp ui',
                        'ui kit',
                        'ui-kit',
                        'iconpack',
                        'design system',
                        'component library',
                  ],
                  risk_level: 'normal',
            },
            {
                  team_id: 'FrontendApp',
                  description:
                        'Standard frontend applications (non-DP), including portals, dashboards, landing pages, and signup apps.',
                  scope_hints: [
                        'frontend',
                        'frontend app',
                        'dashboard',
                        'signup',
                        'landing',
                        'seo',
                        'nextjs',
                        'next.js',
                        'react',
                        'ui',
                        'portal',
                        'customer portal',
                  ],
                  risk_level: 'normal',
            },
            {
                  team_id: 'Tooling',
                  description: 'Tooling and generators.',
                  scope_hints: [
                        'tooling',
                        'generator',
                        'codegen',
                        'hivejs',
                        'hexa',
                        'blockly',
                        'json master',
                        'compiler',
                        'packager',
                        'cli',
                        'scaffold',
                        'template',
                        'builder',
                  ],
                  risk_level: 'high',
            },
            {
                  team_id: 'IdentitySecurity',
                  description:
                        'Identity provider, authentication/authorization, and security boundaries.',
                  scope_hints: [
                        'idp',
                        'identity',
                        'auth',
                        'oauth',
                        'oidc',
                        'sso',
                        'token',
                        'jwt',
                        'encryption',
                        'crypto',
                        'key',
                  ],
                  risk_level: 'high',
            },
            {
                  team_id: 'Mobile',
                  description:
                        'Mobile applications (React Native and native Android).',
                  scope_hints: ['mobile', 'react native', 'rn', 'android', 'kotlin', 'gradle'],
                  risk_level: 'normal',
            },
            {
                  team_id: 'DevOps',
                  description: 'CI/CD, deployment, and infrastructure automation.',
                  scope_hints: [
                        'devops',
                        'ci',
                        'cd',
                        'github actions',
                        'pipeline',
                        'deploy',
                        'terraform',
                        'helm',
                        'k8s',
                  ],
                  risk_level: 'normal',
            },
            {
                  team_id: 'QA',
                  description:
                        'Quality assurance, automated testing, and validation.',
                  scope_hints: ['qa', 'test', 'tests', 'e2e', 'integration', 'regression'],
                  risk_level: 'normal',
            },
      ];
}

function normalizeTeamIdOrThrow(raw) {
      const s = normStr(raw);
      if (!s) throw new Error('team_id is required.');
      if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(s))
            throw new Error(
                  'team_id must match ^[A-Za-z][A-Za-z0-9_-]*$ (no spaces).',
            );
      return s;
}

function teamIdTokens(teamId) {
      const s = String(teamId || '').trim();
      if (!s) return [];
      const spaced = s.replace(/([a-z0-9])([A-Z])/g, '$1 $2');
      const parts = spaced
            .split(/[^A-Za-z0-9]+/g)
            .map((p) => p.trim())
            .filter(Boolean)
            .map((p) => p.toLowerCase());
      const out = [];
      for (const p of parts) if (!out.includes(p)) out.push(p);
      return out;
}

function defaultHintsForCategory(category) {
      const c = String(category || '').toLowerCase();
      if (c === 'backend')
            return [
                  'backend',
                  'api',
                  'service',
                  'endpoint',
                  'controller',
                  'server',
                  'database',
                  'db',
            ];
      if (c === 'frontend')
            return [
                  'frontend',
                  'ui',
                  'web',
                  'portal',
                  'react',
                  'nextjs',
                  'dashboard',
            ];
      if (c === 'fullstack')
            return ['fullstack', 'frontend', 'backend', 'api', 'ui', 'web'];
      if (c === 'mobile')
            return ['mobile', 'android', 'ios', 'react native', 'rn', 'kotlin'];
      if (c === 'tooling')
            return ['tooling', 'generator', 'codegen', 'cli', 'template', 'builder'];
      if (c === 'devops')
            return [
                  'devops',
                  'ci',
                  'cd',
                  'pipeline',
                  'deploy',
                  'terraform',
                  'helm',
                  'k8s',
            ];
      if (c === 'identitysecurity')
            return ['identity', 'auth', 'oauth', 'oidc', 'sso', 'jwt', 'security'];
      return [];
}

function riskForCategory(category) {
      const c = String(category || '').toLowerCase();
      if (c === 'tooling') return 'high';
      if (c === 'identitysecurity') return 'high';
      return 'normal';
}

function descriptionForCategory(category, teamId) {
      const c = String(category || '').toLowerCase();
      if (c === 'backend') return `Backend services and APIs (${teamId}).`;
      if (c === 'frontend') return `Frontend web applications and UI (${teamId}).`;
      if (c === 'fullstack') return 'Full-stack web application team.';
      if (c === 'mobile') return `Mobile applications (${teamId}).`;
      if (c === 'tooling') return 'Tooling and generators.';
      if (c === 'devops') return 'CI/CD, deployment, and infrastructure automation.';
      if (c === 'identitysecurity')
            return 'Identity provider, authentication/authorization, and security boundaries.';
      return `Team ${teamId}.`;
}

function buildTeamFromTemplateOrCategory({ teamId, category, templatesById }) {
      const t = templatesById.get(teamId) || null;
      if (t) return t;

      const hints = [];
      for (const h of defaultHintsForCategory(category)) {
            const x = String(h || '').trim();
            if (x && !hints.includes(x)) hints.push(x);
      }
      for (const tok of teamIdTokens(teamId)) if (!hints.includes(tok)) hints.push(tok);

      return {
            team_id: teamId,
            description: descriptionForCategory(category, teamId),
            scope_hints: hints,
            risk_level: riskForCategory(category),
      };
}

async function promptCount(rl, question, { defaultValue }) {
      const q = String(question || '').trim();
      // eslint-disable-next-line no-constant-condition
      while (true) {
            const raw = await rl.question(`${q} (default ${defaultValue}): `);
            const n = parsePositiveIntOrNull(raw);
            if (n) return n;
            if (!normStr(raw)) return defaultValue;
      }
}

async function promptTeamId(rl, question, { defaultValue = null, used = null } = {}) {
      const q = String(question || '').trim();
      // eslint-disable-next-line no-constant-condition
      while (true) {
            const raw = await rl.question(
                  defaultValue ? `${q} (default ${defaultValue}): ` : `${q}: `,
            );
            const s = normStr(raw) || defaultValue;
            if (!s) continue;
            let teamId;
            try {
                  teamId = normalizeTeamIdOrThrow(s);
            } catch {
                  continue;
            }
            if (used && used.has(teamId)) continue;
            return teamId;
      }
}

async function promptTeamsDocInteractive({ rl, templates }) {
      const templatesById = new Map(
            (templates || [])
                  .filter((t) => t && typeof t.team_id === 'string')
                  .map((t) => [t.team_id, t]),
      );

      const used = new Set();
      const selected = [];

      function addTeam({ teamId, category }) {
            const t = buildTeamFromTemplateOrCategory({
                  teamId,
                  category,
                  templatesById,
            });
            if (used.has(t.team_id)) return;
            used.add(t.team_id);
            selected.push(t);
      }

      const wantWeb = await promptYesNo(rl, 'Do you need WebApplication Team?', {
            defaultYes: true,
      });
      if (wantWeb) {
            const separate = await promptYesNo(
                  rl,
                  'Do you need Separation of Front and Back?',
                  { defaultYes: true },
            );
            if (!separate) {
                  addTeam({ teamId: 'FullStack', category: 'fullstack' });
            } else {
                  const backendDefaults = [
                        'BackendTMSCore',
                        'BackendGD',
                        'BackendPlatform',
                  ];
                  const frontendDefaults = ['FrontendApp', 'FrontendDP'];

                  const backendCount = await promptCount(rl, 'How many backend teams?', {
                        defaultValue: 1,
                  });
                  for (let i = 0; i < backendCount; i += 1) {
                        const def = backendDefaults[i] || `BackendTeam${i + 1}`;
                        const id = await promptTeamId(
                              rl,
                              `Backend team #${i + 1} name`,
                              { defaultValue: def, used },
                        );
                        addTeam({ teamId: id, category: 'backend' });
                  }

                  const frontendCount = await promptCount(
                        rl,
                        'How many frontend teams?',
                        { defaultValue: 1 },
                  );
                  for (let i = 0; i < frontendCount; i += 1) {
                        const def = frontendDefaults[i] || `FrontendTeam${i + 1}`;
                        const id = await promptTeamId(
                              rl,
                              `Frontend team #${i + 1} name`,
                              { defaultValue: def, used },
                        );
                        addTeam({ teamId: id, category: 'frontend' });
                  }
            }
      }

      const wantMobile = await promptYesNo(
            rl,
            'Do you need MobileApplication Team?',
            { defaultYes: false },
      );
      if (wantMobile) {
            const count = await promptCount(
                  rl,
                  'How many mobile application teams?',
                  { defaultValue: 1 },
            );
            for (let i = 0; i < count; i += 1) {
                  const def = i === 0 ? 'Mobile' : `Mobile${i + 1}`;
                  const id = await promptTeamId(
                        rl,
                        `Mobile team #${i + 1} name`,
                        { defaultValue: def, used },
                  );
                  addTeam({ teamId: id, category: 'mobile' });
            }
      }

      const wantTooling = await promptYesNo(rl, 'Do you need Tooling Team?', {
            defaultYes: true,
      });
      if (wantTooling) addTeam({ teamId: 'Tooling', category: 'tooling' });

      const wantDevOps = await promptYesNo(rl, 'Do you need DevOps Team?', {
            defaultYes: true,
      });
      if (wantDevOps) addTeam({ teamId: 'DevOps', category: 'devops' });

      const wantIdentitySecurity = await promptYesNo(
            rl,
            'Do you need Identity/Security Team?',
            { defaultYes: false },
      );
      if (wantIdentitySecurity)
            addTeam({ teamId: 'IdentitySecurity', category: 'identitysecurity' });

      // Contract: every project must have QA.
      addTeam({ teamId: 'QA', category: 'qa' });

      // Ensure at least one non-QA team exists so routing has an execution target.
      if (selected.filter((t) => t.team_id !== 'QA').length === 0)
            addTeam({ teamId: 'FullStack', category: 'fullstack' });

      return {
            version: 1,
            teams: stableSort(selected, (t) => String(t.team_id)),
      };
}

function gitTextOrNull(repoAbs, args) {
      const res = git(repoAbs, args);
      if (!res.ok) return null;
      const out = String(res.stdout || '').trim();
      return out || null;
}

function branchExists(repoPath, name) {
      const n = String(name || '').trim();
      if (!n) return false;
      const res =
            git(repoPath, ['show-ref', '--verify', '--quiet', `refs/heads/${n}`])
                  .ok ||
            git(repoPath, [
                  'show-ref',
                  '--verify',
                  '--quiet',
                  `refs/remotes/origin/${n}`,
            ]).ok;
      return !!res;
}

function lastCommitDate(repoPath, ref) {
      const r = String(ref || '').trim();
      if (!r) return null;
      const out = gitTextOrNull(repoPath, ['log', '-1', '--format=%cI', r]);
      return out;
}

function hasCiConfig(repoPath, ref) {
      const r = String(ref || '').trim();
      if (!r) return false;
      const candidates = [
            '.github/workflows',
            '.gitlab-ci.yml',
            'azure-pipelines.yml',
            'Jenkinsfile',
            '.circleci/config.yml',
      ];
      for (const p of candidates) {
            const ok = git(repoPath, ['cat-file', '-e', `${r}:${p}`]).ok;
            if (ok) return true;
      }
      return false;
}

function pickLikelyCanonicalBranch({ defaultBranch, candidates }) {
      const items = (candidates || [])
            .filter((c) => c && c.name && c.exists)
            .map((c) => {
                  let score = 0;
                  const name = String(c.name);
                  if (name === defaultBranch) score += 3;
                  if (name === 'main' || name === 'master') score += 2;
                  if (name === 'develop') score += 1;
                  if (c.has_ci) score += 2;
                  if (c.is_most_recent) score += 2;
                  return { ...c, score };
            });
      if (!items.length)
            return {
                  likely_canonical_branch: null,
                  confidence: 'low',
                  reasons: ['no candidate branches found'],
            };
      items.sort(
            (a, b) =>
                  b.score - a.score ||
                  String(a.name).localeCompare(String(b.name)),
      );
      const top = items[0];
      const second = items[1] || null;
      const gap = second ? top.score - second.score : top.score;
      const confidence = gap >= 3 ? 'high' : gap >= 1 ? 'medium' : 'low';
      const reasons = [];
      reasons.push(`score=${top.score}`);
      if (top.name === defaultBranch) reasons.push('matches origin/HEAD default');
      if (top.has_ci) reasons.push('CI config detected');
      if (top.is_most_recent)
            reasons.push('most recent commit among candidates');
      if (top.name === 'main' || top.name === 'master' || top.name === 'develop')
            reasons.push('common branch name');
      return { likely_canonical_branch: top.name, confidence, reasons };
}

async function isGitRepoDir(absDir) {
      const dotgit = resolve(absDir, '.git');
      try {
            const entries = await readdir(dotgit);
            return Array.isArray(entries);
      } catch {
            return existsSync(dotgit);
      }
}

async function findGitRepos({ reposRoot, maxDepth = 2 }) {
      const out = [];
      const queue = [{ dir: reposRoot, depth: 0 }];

      while (queue.length) {
            const { dir, depth } = queue.shift();
            // eslint-disable-next-line no-await-in-loop
            if (await isGitRepoDir(dir)) {
                  out.push(dir);
                  continue;
            }
            if (depth >= maxDepth) continue;

            let entries = [];
            try {
                  // eslint-disable-next-line no-await-in-loop
                  entries = await readdir(dir, { withFileTypes: true });
            } catch {
                  continue;
            }

            const dirs = entries
                  .filter((e) => e.isDirectory())
                  .map((e) => e.name)
                  .filter(
                        (n) =>
                              n &&
                              n !== 'node_modules' &&
                              n !== '.git' &&
                              !n.startsWith('.'),
                  );
            dirs.sort((a, b) => a.localeCompare(b));
            for (const name of dirs)
                  queue.push({
                        dir: resolve(dir, name),
                        depth: depth + 1,
                  });
      }

      const uniq = [];
      for (const p of stableSort(out, (x) => x))
            if (!uniq.includes(p)) uniq.push(p);
      return uniq;
}

async function promptYesNo(rl, question, { defaultYes = null } = {}) {
      const q = String(question || '').trim();
      const suffix =
            defaultYes === true ? ' (Y/n)' : defaultYes === false ? ' (y/N)' : ' (y/n)';
      // eslint-disable-next-line no-constant-condition
      while (true) {
            const raw = normStr(await rl.question(`${q}${suffix}: `)).toLowerCase();
            if (!raw && defaultYes !== null) return defaultYes;
            if (['y', 'yes'].includes(raw)) return true;
            if (['n', 'no'].includes(raw)) return false;
      }
}

function parsePositiveIntOrNull(raw) {
      const s = typeof raw === 'string' ? raw.trim() : '';
      if (!s) return null;
      const n = Number.parseInt(s, 10);
      if (!Number.isFinite(n) || n < 1) return null;
      return n;
}

function nowISO() {
      return new Date().toISOString();
}

function validateProjectCode(raw) {
      const s = normStr(raw);
      if (!s) throw new Error('project_code is required.');
      if (!/^[a-z0-9-]+$/.test(s))
            throw new Error('project_code must match ^[a-z0-9-]+$.');
      return s;
}

function requireAbs(p, name) {
      const s = normStr(p);
      if (!s) throw new Error(`Missing ${name}.`);
      if (!isAbsolute(s)) throw new Error(`${name} must be an absolute path.`);
      return resolve(s);
}

function parseBool(v, fallback) {
      if (v === undefined || v === null) return !!fallback;
      const s = String(v).trim().toLowerCase();
      if (!s) return !!fallback;
      return s === 'true' || s === '1' || s === 'yes';
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
      const abs = resolve(String(absPath || ''));
      await mkdir(dirname(abs), { recursive: true });
      atomicCounter += 1;
      const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
      await writeFile(tmp, String(text || ''), 'utf8');
      await rename(tmp, abs);
}

function gitInitIfMissing(repoAbs, { dryRun }) {
      const gitDir = join(repoAbs, '.git');
      if (existsSync(gitDir)) return { ok: true, created: false };
      if (dryRun) return { ok: true, created: true, dry_run: true };
      const r = spawnSync('git', ['init', '-q'], {
            cwd: repoAbs,
            encoding: 'utf8',
      });
      if (r.status !== 0)
            return {
                  ok: false,
                  message:
                        String(r.stderr || r.stdout || '').trim() ||
                        'git init failed',
            };
      return { ok: true, created: true };
}

function git(repoAbs, args) {
      const r = spawnSync('git', ['-C', repoAbs, ...args], {
            encoding: 'utf8',
      });
      return {
            ok: r.status === 0,
            status: r.status,
            stdout: String(r.stdout || '').trim(),
            stderr: String(r.stderr || '').trim(),
      };
}

async function ensureLocalBareRepoExists(bareRepoAbs, { dryRun }) {
      const abs = requireAbs(bareRepoAbs, 'Repo Path');
      if (existsSync(abs)) {
            const probe = spawnSync(
                  'git',
                  ['-C', abs, 'rev-parse', '--git-dir'],
                  { encoding: 'utf8' },
            );
            if (probe.status === 0) return { ok: true, created: false, abs };
            // If the directory exists but isn't a git repo, only initialize if it's empty (avoid clobbering).
            const entries = dryRun ? [] : await readdir(abs);
            if (!entries.length) {
                  if (dryRun)
                        return { ok: true, created: true, dry_run: true, abs };
                  const r = spawnSync('git', ['init', '--bare', abs], {
                        encoding: 'utf8',
                  });
                  if (r.status !== 0)
                        return {
                              ok: false,
                              message:
                                    String(r.stderr || r.stdout || '').trim() ||
                                    'git init --bare failed',
                              abs,
                        };
                  return { ok: true, created: true, abs };
            }
            return {
                  ok: false,
                  message: `Path exists but is not a git repo (and is not empty): ${abs}`,
                  abs,
            };
      }

      if (dryRun) return { ok: true, created: true, dry_run: true, abs };
      await mkdir(dirname(abs), { recursive: true });
      const r = spawnSync('git', ['init', '--bare', abs], { encoding: 'utf8' });
      if (r.status !== 0)
            return {
                  ok: false,
                  message:
                        String(r.stderr || r.stdout || '').trim() ||
                        'git init --bare failed',
                  abs,
            };
      return { ok: true, created: true, abs };
}

async function gitClone({ source, destAbs, dryRun }) {
      const src = normStr(source);
      const dst = requireAbs(destAbs, 'Destination Path');
      if (!src) return { ok: false, message: 'Missing source repo path.' };

      if (existsSync(dst)) {
            // Idempotency: if it's already a git repo, do not overwrite.
            if (existsSync(join(dst, '.git')))
                  return {
                        ok: true,
                        cloned: false,
                        skipped: true,
                        reason: 'destination already has .git',
                        dest_abs: dst,
                  };
            return {
                  ok: false,
                  message: `Destination exists but is not a git repo: ${dst}`,
            };
      }

      if (dryRun)
            return { ok: true, cloned: true, dry_run: true, dest_abs: dst };

      await mkdir(dirname(dst), { recursive: true });
      const r = spawnSync('git', ['clone', src, dst], { encoding: 'utf8' });
      if (r.status !== 0) {
            return {
                  ok: false,
                  message:
                        String(r.stderr || r.stdout || '').trim() ||
                        'git clone failed',
                  dest_abs: dst,
            };
      }
      return { ok: true, cloned: true, dest_abs: dst };
}

function ensureGitSafeDirectory(repoAbs, { dryRun }) {
      if (dryRun) return { ok: true, wrote: false, dry_run: true };
      const r = spawnSync(
            'git',
            ['config', '--global', '--add', 'safe.directory', repoAbs],
            { encoding: 'utf8' },
      );
      if (r.status !== 0) {
            return {
                  ok: false,
                  message:
                        `Failed to set git safe.directory for ${repoAbs}.\n` +
                        `Run:\n  git config --global --add safe.directory ${repoAbs}\n` +
                        `stderr: ${String(r.stderr || '').trim() || '(empty)'}`,
            };
      }
      return { ok: true, wrote: true };
}

function ensureOriginIfProvided(repoAbs, { dryRun }) {
      const provided = normStr(process.env.KNOWLEDGE_GIT_REMOTE);
      const branch = normStr(process.env.KNOWLEDGE_GIT_BRANCH) || 'main';
      const existing = git(repoAbs, ['remote', 'get-url', 'origin']);
      if (existing.ok)
            return {
                  ok: true,
                  remote: existing.stdout,
                  default_branch: branch,
                  created: false,
            };
      if (!provided)
            return {
                  ok: true,
                  remote: '',
                  default_branch: branch,
                  created: false,
            };
      if (dryRun)
            return {
                  ok: true,
                  remote: provided,
                  default_branch: branch,
                  created: true,
                  dry_run: true,
            };
      const add = git(repoAbs, ['remote', 'add', 'origin', provided]);
      if (!add.ok)
            return {
                  ok: false,
                  message:
                        add.stderr ||
                        add.stdout ||
                        'git remote add origin failed',
            };
      return {
            ok: true,
            remote: provided,
            default_branch: branch,
            created: true,
      };
}

function defaultSystemAdapters({ dryRun } = {}) {
      return {
            git: {
                  initIfMissing(repoAbs) {
                        return gitInitIfMissing(repoAbs, { dryRun });
                  },
                  ensureSafeDirectory(repoAbs) {
                        return ensureGitSafeDirectory(repoAbs, { dryRun });
                  },
                  ensureOriginIfProvided(repoAbs) {
                        return ensureOriginIfProvided(repoAbs, { dryRun });
                  },
                  headSha(repoAbs) {
                        const r = git(repoAbs, ['rev-parse', 'HEAD']);
                        return r.ok ? r.stdout : null;
                  },
            },
            cron: {
                  installBlock({ project_code, entries }) {
                        return installCronBlock({
                              project_code,
                              entries,
                              dryRun,
                        });
                  },
            },
      };
}

function renderPm2Ecosystem({
      project_code,
      toolAbs,
      opsRootAbs,
      reposRootAbs,
      knowledgeRootAbs,
      ports,
}) {
      const webuiName = `${project_code}-webui`;
      const websvcName = `${project_code}-websvc`;
      const outDir = join(opsRootAbs, 'pm2', 'logs');
      const envCommon = {
            NODE_ENV: 'production',
            DOTENV_CONFIG_PATH: join(opsRootAbs, '.env'),
            AI_PROJECT_ROOT: opsRootAbs,
            REPOS_ROOT: reposRootAbs,
            KNOWLEDGE_REPO_DIR: knowledgeRootAbs,
            AI_PROJECT_KEY: project_code,
            AI_TEAM_REPO: toolAbs,
            CLI_PATH: join(toolAbs, 'src', 'cli.js'),
      };

      // WebUI uses WEB_PORT; WebSvc uses INTAKE_PORT.
      const lines = [];
      lines.push('module.exports = {');
      lines.push('  apps: [');
      lines.push('    {');
      lines.push(`      name: ${JSON.stringify(webuiName)},`);
      lines.push(`      cwd: ${JSON.stringify(toolAbs)},`);
      lines.push(`      interpreter: "node",`);
      lines.push(`      script: "src/web/server.js",`);
      lines.push(`      node_args: ["-r", "dotenv/config"],`);
      lines.push('      env: {');
      for (const [k, v] of Object.entries({
            ...envCommon,
            WEB_PORT: String(ports.webui_port),
      })) {
            lines.push(
                  `        ${JSON.stringify(k)}: ${JSON.stringify(String(v))},`,
            );
      }
      lines.push('      },');
      lines.push(
            `      out_file: ${JSON.stringify(join(outDir, `${webuiName}.out.log`))},`,
      );
      lines.push(
            `      error_file: ${JSON.stringify(join(outDir, `${webuiName}.err.log`))},`,
      );
      lines.push('      merge_logs: true,');
      lines.push('      autorestart: true,');
      lines.push('      max_restarts: 5,');
      lines.push('      restart_delay: 2000,');
      lines.push('      time: true,');
      lines.push('    },');
      lines.push('    {');
      lines.push(`      name: ${JSON.stringify(websvcName)},`);
      lines.push(`      cwd: ${JSON.stringify(toolAbs)},`);
      lines.push(`      interpreter: "node",`);
      lines.push(`      script: "src/lane_b/intake/server.js",`);
      lines.push(`      node_args: ["-r", "dotenv/config"],`);
      lines.push('      env: {');
      for (const [k, v] of Object.entries({
            ...envCommon,
            INTAKE_PORT: String(ports.websvc_port),
      })) {
            lines.push(
                  `        ${JSON.stringify(k)}: ${JSON.stringify(String(v))},`,
            );
      }
      lines.push('      },');
      lines.push(
            `      out_file: ${JSON.stringify(join(outDir, `${websvcName}.out.log`))},`,
      );
      lines.push(
            `      error_file: ${JSON.stringify(join(outDir, `${websvcName}.err.log`))},`,
      );
      lines.push('      merge_logs: true,');
      lines.push('      autorestart: true,');
      lines.push('      max_restarts: 5,');
      lines.push('      restart_delay: 2000,');
      lines.push('      time: true,');
      lines.push('    },');
      lines.push('  ],');
      lines.push('};');
      lines.push('');
      return { text: lines.join('\n'), apps: [webuiName, websvcName] };
}

function renderCronFile({ project_code, toolAbs, opsRootAbs }) {
      const ops = opsRootAbs;
      const cli = join(toolAbs, 'src', 'cli.js');
      const entries = [];
      entries.push(
            `*/5 * * * * cd ${toolAbs} && AI_PROJECT_ROOT=${ops} node ${cli} --watchdog --limit 1 --watchdog-ci true --watchdog-prepr true`,
      );
      entries.push(
            `*/5 * * * * cd ${toolAbs} && AI_PROJECT_ROOT=${ops} node ${cli} --lane-a-orchestrate --limit 1`,
      );
      entries.push(
            `*/10 * * * * cd ${toolAbs} && AI_PROJECT_ROOT=${ops} node ${cli} --knowledge-refresh-from-events --projectRoot ${ops} --stop-on-error`,
      );
      const text = entries.join('\n') + '\n';
      return { entries, text };
}

function crontabList() {
      const r = spawnSync('crontab', ['-l'], { encoding: 'utf8' });
      if (r.status !== 0) {
            const stderr = String(r.stderr || '').toLowerCase();
            if (stderr.includes('no crontab')) return { ok: true, text: '' };
            return {
                  ok: false,
                  text: '',
                  message:
                        String(r.stderr || r.stdout || '').trim() ||
                        'crontab -l failed',
            };
      }
      return { ok: true, text: String(r.stdout || '') };
}

function crontabWrite(text) {
      const r = spawnSync('crontab', ['-'], { input: text, encoding: 'utf8' });
      if (r.status !== 0)
            return {
                  ok: false,
                  message:
                        String(r.stderr || r.stdout || '').trim() ||
                        'crontab install failed',
            };
      return { ok: true };
}

function installCronBlock({ project_code, entries, dryRun }) {
      const begin = `# AI-TEAM ${project_code} BEGIN`;
      const end = `# AI-TEAM ${project_code} END`;
      const block = [begin, ...entries, end].join('\n') + '\n';
      if (dryRun)
            return {
                  ok: true,
                  installed: false,
                  entries,
                  dry_run: true,
                  block,
            };
      const current = crontabList();
      if (!current.ok)
            return {
                  ok: false,
                  installed: false,
                  entries,
                  message: current.message,
            };
      const raw = String(current.text || '');
      const lines = raw.split('\n');
      const out = [];
      let inBlock = false;
      for (const l of lines) {
            if (l.trim() === begin) {
                  inBlock = true;
                  continue;
            }
            if (l.trim() === end) {
                  inBlock = false;
                  continue;
            }
            if (!inBlock) out.push(l);
      }
      // Trim trailing blank lines
      while (out.length && out[out.length - 1] === '') out.pop();
      const nextText = (out.join('\n') + '\n\n' + block).replace(
            /\n{3,}/g,
            '\n\n',
      );
      const wrote = crontabWrite(nextText);
      if (!wrote.ok)
            return {
                  ok: false,
                  installed: false,
                  entries,
                  message: wrote.message,
            };
      return { ok: true, installed: true, entries };
}

async function copyTemplateIfMissing({ srcAbs, dstAbs, dryRun }) {
      if (existsSync(dstAbs)) return { ok: true, wrote: false, path: dstAbs };
      if (!existsSync(srcAbs))
            return { ok: false, message: `Missing template: ${srcAbs}` };
      const text = await readFile(srcAbs, 'utf8');
      if (dryRun)
            return { ok: true, wrote: false, path: dstAbs, dry_run: true };
      await writeTextAtomic(dstAbs, text);
      return { ok: true, wrote: true, path: dstAbs };
}

async function mergeLlmProfilesFromTemplateIfMissingKeys({
      templateAbs,
      dstAbs,
      requiredProfileKeys,
      dryRun,
} = {}) {
      if (dryRun) return { ok: true, wrote: false, dry_run: true, path: dstAbs };
      if (!existsSync(dstAbs))
            return { ok: false, message: `Missing LLM profiles at ${dstAbs}` };
      if (!existsSync(templateAbs))
            return { ok: false, message: `Missing template: ${templateAbs}` };

      let cur;
      let tmpl;
      try {
            cur = JSON.parse(await readFile(dstAbs, 'utf8'));
      } catch {
            return { ok: false, message: `Invalid JSON in ${dstAbs}` };
      }
      try {
            tmpl = JSON.parse(await readFile(templateAbs, 'utf8'));
      } catch {
            return { ok: false, message: `Invalid JSON in ${templateAbs}` };
      }

      const curProfiles =
            cur && typeof cur === 'object' && cur.profiles && typeof cur.profiles === 'object'
                  ? cur.profiles
                  : null;
      const tmplProfiles =
            tmpl && typeof tmpl === 'object' && tmpl.profiles && typeof tmpl.profiles === 'object'
                  ? tmpl.profiles
                  : null;
      if (!curProfiles)
            return { ok: false, message: `Invalid shape in ${dstAbs}: missing profiles object.` };
      if (!tmplProfiles)
            return { ok: false, message: `Invalid shape in ${templateAbs}: missing profiles object.` };

      let changed = false;
      for (const key of Array.isArray(requiredProfileKeys) ? requiredProfileKeys : []) {
            const k = String(key || '').trim();
            if (!k) continue;
            if (Object.prototype.hasOwnProperty.call(curProfiles, k)) continue;
            if (!Object.prototype.hasOwnProperty.call(tmplProfiles, k))
                  return {
                        ok: false,
                        message: `Template ${templateAbs} missing required profile key '${k}'.`,
                  };
            curProfiles[k] = tmplProfiles[k];
            changed = true;
      }

      if (!changed) return { ok: true, wrote: false, path: dstAbs };
      await writeTextAtomic(dstAbs, JSON.stringify(cur, null, 2) + '\n');
      return { ok: true, wrote: true, path: dstAbs };
}

async function writeJsonAtomic(absPath, obj, { dryRun }) {
      const text = JSON.stringify(obj, null, 2) + '\n';
      if (dryRun)
            return { ok: true, wrote: false, path: absPath, dry_run: true };
      await writeTextAtomic(absPath, text);
      return { ok: true, wrote: true, path: absPath };
}

export async function runInitialProjectOnboarding({
      toolRepoRoot,
      dryRun = false,
      project = null,
      nonInteractive = false,
      system = null,
} = {}) {
      const toolAbs = requireAbs(toolRepoRoot || process.cwd(), 'toolRepoRoot');
      const sys =
            system && typeof system === 'object'
                  ? system
                  : defaultSystemAdapters({ dryRun });

      const nonInt = !!nonInteractive;
      const projectArg = normStr(project);
      if (nonInt && !projectArg)
            throw new Error(
                  'Missing --project <project_code> (required in --non-interactive mode).',
            );

      const rl = nonInt
            ? null
            : createInterface({ input: process.stdin, output: process.stdout });
      try {
            const projectCodeRaw =
                  projectArg ||
                  (nonInt
                        ? ''
                        : await rl.question('Enter project_code (e.g. tms): '));
            const project_code = validateProjectCode(projectCodeRaw);
            const defaultProjectsRoot =
                  normStr(process.env.AI_TEAM_PROJECTS_ROOT) ||
                  '/opt/AI-Projects';
            const baseRootRaw = nonInt
                  ? defaultProjectsRoot
                  : await rl.question(
                          `Where do you want to create the Project Folder? (default ${defaultProjectsRoot}): `,
                    );
            const projectsRootAbs = requireAbs(
                  baseRootRaw ? baseRootRaw : defaultProjectsRoot,
                  'Project Path',
            );

            const projectHomeAbs = resolve(projectsRootAbs, project_code);
            const opsRootAbs = join(projectHomeAbs, 'ops');
            const reposRootAbs = join(projectHomeAbs, 'repos');
            const knowledgeRootAbs = join(projectHomeAbs, 'knowledge');

            if (!dryRun) {
                  await mkdir(join(opsRootAbs, 'config'), { recursive: true });
                  await mkdir(join(opsRootAbs, 'pm2', 'logs'), {
                        recursive: true,
                  });
                  await mkdir(join(opsRootAbs, 'cron'), { recursive: true });
                  await mkdir(join(opsRootAbs, 'logs', 'lane_a'), {
                        recursive: true,
                  });
                  await mkdir(join(opsRootAbs, 'logs', 'lane_b'), {
                        recursive: true,
                  });
                  await mkdir(reposRootAbs, { recursive: true });
            }

            // Knowledge repo: prompt for a source path and clone into knowledge_root_abs.
            // - If the provided path is a local filesystem path and doesn't exist, create a bare repo there first.
            // - If not provided (non-interactive or blank), fall back to local `git init`.
            const warnings = [];
            let knowledgeRepoSource = nonInt
                  ? normStr(process.env.AI_TEAM_KNOWLEDGE_REPO_PATH)
                  : '';
            if (!nonInt) {
                  const defaultHint =
                        normStr(process.env.AI_TEAM_KNOWLEDGE_REPO_PATH) ||
                        normStr(process.env.KNOWLEDGE_GIT_REMOTE);
                  // Require an answer in interactive mode (per user workflow); allow blank only if env default is provided.
                  // If both are blank, re-prompt until non-empty.
                  // eslint-disable-next-line no-constant-condition
                  while (true) {
                        const q = `Enter the Knowledge repo Path for project ${project_code}${defaultHint ? ` (default ${defaultHint})` : ''}: `;
                        const ans =
                              normStr(await rl.question(q)) || defaultHint;
                        if (ans) {
                              knowledgeRepoSource = ans;
                              break;
                        }
                  }
            }

            const knowledgeSource = normStr(knowledgeRepoSource);
            if (knowledgeSource) {
                  if (
                        !looksLikeRemoteRepoSpec(knowledgeSource) &&
                        isAbsolute(knowledgeSource)
                  ) {
                        const ensured = await ensureLocalBareRepoExists(
                              knowledgeSource,
                              { dryRun },
                        );
                        if (!ensured.ok)
                              return {
                                    ok: false,
                                    message: `Failed to create knowledge repo at ${knowledgeSource}: ${ensured.message}`,
                              };
                  }
                  const cloned = await gitClone({
                        source: knowledgeSource,
                        destAbs: knowledgeRootAbs,
                        dryRun,
                  });
                  if (!cloned.ok)
                        return {
                              ok: false,
                              message: `Failed to clone knowledge repo into ${knowledgeRootAbs}: ${cloned.message}`,
                        };
                  if (cloned.skipped)
                        warnings.push(
                              `Knowledge repo clone skipped: ${cloned.reason} (${knowledgeRootAbs})`,
                        );
            } else if (!dryRun) {
                  await mkdir(knowledgeRootAbs, { recursive: true });
            }

            // Code repos: prompt for at least one source; then optionally loop.
            const codeRepoSources = [];
            if (!nonInt) {
                  // eslint-disable-next-line no-constant-condition
                  while (true) {
                        const first = normStr(
                              await rl.question(
                                    `Enter the first Code Repo Path for project ${project_code}: `,
                              ),
                        );
                        if (first) {
                              codeRepoSources.push(first);
                              break;
                        }
                  }
                  // eslint-disable-next-line no-constant-condition
                  while (true) {
                        const more = normStr(
                              await rl.question(
                                    `Do you want to clone any additional code repo to ${project_code} project? (Y/N): `,
                              ),
                        ).toLowerCase();
                        if (more === 'n' || more === 'no') break;
                        if (more !== 'y' && more !== 'yes') continue;
                        // eslint-disable-next-line no-constant-condition
                        while (true) {
                              const next = normStr(
                                    await rl.question(
                                          `Enter the next Code Repo Path for project ${project_code}: `,
                                    ),
                              );
                              if (next) {
                                    codeRepoSources.push(next);
                                    break;
                              }
                        }
                  }
            } else {
                  const raw = normStr(process.env.AI_TEAM_CODE_REPO_PATHS);
                  if (raw) {
                        for (const part of raw
                              .split(',')
                              .map((x) => normStr(x))
                              .filter(Boolean))
                              codeRepoSources.push(part);
                  }
            }

            const clonedRepoDirs = [];
            const usedRepoDirs = new Set();
            for (let i = 0; i < codeRepoSources.length; i += 1) {
                  const source = codeRepoSources[i];
                  if (!looksLikeRemoteRepoSpec(source) && isAbsolute(source)) {
                        const ensured = await ensureLocalBareRepoExists(
                              source,
                              { dryRun },
                        );
                        if (!ensured.ok)
                              return {
                                    ok: false,
                                    message: `Failed to create code repo at ${source}: ${ensured.message}`,
                              };
                  }

                  const inferred =
                        inferRepoDirNameFromSource(source) || `repo-${i + 1}`;
                  let dirName = inferred;
                  let suffix = 2;
                  while (usedRepoDirs.has(dirName)) {
                        dirName = `${inferred}-${suffix}`;
                        suffix += 1;
                  }
                  usedRepoDirs.add(dirName);

                  const repo_id = normalizeRepoId(dirName);
                  if (!repo_id)
                        return {
                              ok: false,
                              message: `Unable to infer repo_id from source: ${source}`,
                        };

                  const destAbs = join(reposRootAbs, dirName);
                  const cloned = await gitClone({ source, destAbs, dryRun });
                  if (!cloned.ok)
                        return {
                              ok: false,
                              message: `Failed to clone code repo into ${destAbs}: ${cloned.message}`,
                        };
                  if (cloned.skipped)
                        warnings.push(
                              `Code repo clone skipped: ${cloned.reason} (${destAbs})`,
                        );

                  clonedRepoDirs.push({ repo_id, dir_name: dirName, dest_abs: destAbs });
            }

            // Generate routing-grade TEAMS/REPOS/AGENTS configs (legacy onboarding scanner, ported to OPS layout).
            const allTeams = defaultTeamsSuggested();
            const teamsDoc = nonInt
                  ? {
                        version: 1,
                        teams: stableSort(
                              allTeams.filter((t) =>
                                    [
                                          'BackendTMSCore',
                                          'FrontendApp',
                                          'Tooling',
                                          'DevOps',
                                          'QA',
                                    ].includes(String(t.team_id)),
                              ),
                              (t) => String(t.team_id),
                        ),
                  }
                  : await promptTeamsDocInteractive({ rl, templates: allTeams });

            let plannersPerTeam = 1;
            let createApplierPerTeam = true;
            if (!nonInt) {
                  const rawCount = await rl.question('How many LLM planners per team? (default 1): ');
                  plannersPerTeam = parsePositiveIntOrNull(rawCount) || 1;
                  createApplierPerTeam = await promptYesNo(rl, 'Create code applier per team?', { defaultYes: true });
            }

            const agentsDoc = generateAgentsConfig({
                  teamsConfig: teamsDoc,
                  plannersPerTeam,
                  createApplierPerTeam,
                  createWriterAgent: true,
                  createQaStrategistAgent: true,
            });
            const cover = validateAgentsConfigCoversTeams({
                  teamsConfig: teamsDoc,
                  agentsConfig: agentsDoc,
            });
            if (!cover.ok) {
                  return {
                        ok: false,
                        message: 'Generated AGENTS.json did not validate.',
                        errors: cover.errors,
                  };
            }

            // Scan repos under REPOS_ROOT for branch + commands + keyword inference.
            const repoPaths = await findGitRepos({ reposRoot: reposRootAbs, maxDepth: 2 });
            const branchScan = [];
            const commandsSuggested = [];
            const scan = [];

            const reposSuggested = [];
            for (const repoPath of repoPaths) {
                  const folder = basename(repoPath) || 'repo';
                  const repo_id = normalizeRepoId(folder) || normalizeRepoId(`repo-${reposSuggested.length + 1}`);
                  if (!repo_id) continue;

                  const pathRelRaw = relative(reposRootAbs, repoPath) || folder;
                  const pathRel = pathRelRaw.split(sep).join('/');

                  const originHead = git(repoPath, ['symbolic-ref', 'refs/remotes/origin/HEAD']);
                  let defaultBranch = null;
                  if (originHead.ok && originHead.stdout.startsWith('refs/remotes/origin/')) {
                        defaultBranch = originHead.stdout.split('/').slice(-1)[0];
                  }

                  const localBranches = git(repoPath, ['for-each-ref', 'refs/heads', '--format=%(refname:short)']);
                  const remoteBranches = git(repoPath, ['for-each-ref', 'refs/remotes', '--format=%(refname:short)']);
                  const locals = localBranches.ok
                        ? localBranches.stdout.split('\n').map((l) => l.trim()).filter(Boolean)
                        : [];
                  const remotes = remoteBranches.ok
                        ? remoteBranches.stdout.split('\n').map((l) => l.trim()).filter(Boolean)
                        : [];
                  locals.sort((a, b) => a.localeCompare(b));
                  remotes.sort((a, b) => a.localeCompare(b));

                  if (!defaultBranch) {
                        const candidates = ['main', 'master', 'develop'];
                        for (const c of candidates) {
                              if (locals.includes(c) || remotes.includes(`origin/${c}`)) {
                                    defaultBranch = c;
                                    break;
                              }
                        }
                  }

                  if (!defaultBranch) {
                        const head = git(repoPath, ['rev-parse', '--abbrev-ref', 'HEAD']);
                        defaultBranch = head.ok && head.stdout && head.stdout !== 'HEAD' ? head.stdout : 'unknown';
                  }

                  const files = detectRepoFiles(repoPath);
                  const scriptsRes = files.node.package_json ? readPackageJsonScripts(repoPath) : { ok: true, scripts: {}, package_json: null };
                  const scripts = scriptsRes.ok ? scriptsRes.scripts : {};
                  const nodeCommands = inferNodeCommands({ files, scripts });
                  const domains = inferDomainsFromName(folder);
                  const kind = inferKind({ repoFolderName: folder, files, scripts });
                  const keywords = tokenizeKeywords(folder);
                  const usageTeam = inferUsageAndTeam({ repoFolderName: folder, kind, domains, teams: teamsDoc.teams });

                  // Branch canonicalization analysis.
                  const branchCandidates = [
                        { name: defaultBranch, exists: branchExists(repoPath, defaultBranch) },
                        { name: 'develop', exists: branchExists(repoPath, 'develop') },
                        { name: 'main', exists: branchExists(repoPath, 'main') },
                        { name: 'master', exists: branchExists(repoPath, 'master') },
                  ]
                        .filter((x) => x.name && x.name !== 'unknown')
                        .filter((x, idx, arr) => arr.findIndex((y) => y.name === x.name) === idx);

                  for (const c of branchCandidates) {
                        c.last_commit_date = c.exists ? lastCommitDate(repoPath, c.name) : null;
                        c.has_ci = c.exists ? hasCiConfig(repoPath, c.name) : false;
                  }
                  const mostRecent = branchCandidates
                        .filter((c) => c.exists && c.last_commit_date)
                        .map((c) => c.last_commit_date)
                        .sort((a, b) => String(b).localeCompare(String(a)))[0];
                  for (const c of branchCandidates) {
                        c.is_most_recent = !!(mostRecent && c.last_commit_date === mostRecent);
                  }
                  const pick = pickLikelyCanonicalBranch({ defaultBranch, candidates: branchCandidates });
                  const active_branch = pick.likely_canonical_branch || (defaultBranch && defaultBranch !== 'unknown' ? defaultBranch : null);

                  branchScan.push({
                        repo_id,
                        path_abs: repoPath,
                        default_branch: defaultBranch,
                        candidates: stableSort(branchCandidates, (x) => x.name),
                        likely_canonical_branch: pick.likely_canonical_branch,
                        reasons: pick.reasons,
                        confidence: pick.confidence,
                  });

                  if (files.node.package_json) {
                        const missingScripts = ['lint', 'test', 'build'].filter((k) => !Object.prototype.hasOwnProperty.call(scripts || {}, k));
                        if (missingScripts.length) {
                              commandsSuggested.push({
                                    repo_id,
                                    note: 'Node repo has missing scripts; default behavior is to skip missing scripts.',
                                    missing_scripts: missingScripts,
                                    missing_script_behavior: 'skip',
                                    commands: nodeCommands.commands,
                              });
                        }
                  } else if (files.node.pnpm_lock || files.node.yarn_lock || files.node.npm_lock) {
                        commandsSuggested.push({
                              repo_id,
                              note: 'Lockfile detected but package.json missing at repo root; set commands.cwd to the actual Node project directory.',
                              missing_scripts: ['lint', 'test', 'build'],
                              missing_script_behavior: 'skip',
                              commands: {
                                    cwd: '<set to actual project folder>',
                                    package_manager: null,
                                    install: null,
                                    lint: null,
                                    test: null,
                                    build: null,
                              },
                        });
                  }

                  scan.push({
                        repo_id,
                        path_abs: repoPath,
                        folder,
                        default_branch: defaultBranch,
                        branches_count: { local: locals.length, remote: remotes.length },
                        branches: { local: locals, remote: remotes },
                        detected: {
                              files,
                              node: {
                                    scripts: stableSort(Object.keys(scripts || {}), (s) => s),
                                    commands: nodeCommands.commands,
                              },
                        },
                  });

                  const repoEntry = {
                        repo_id,
                        name: folder,
                        path: pathRel,
                        status: 'active',
                        team_id: usageTeam.team_id,
                        keywords,
                        active_branch,
                        Kind: kind,
                        Domains: domains,
                        Usage: usageTeam.usage,
                        ...(nodeCommands?.commands?.package_manager ? { commands: nodeCommands.commands } : {}),
                  };
                  reposSuggested.push(repoEntry);
            }

            const reposDocGenerated = {
                  version: 1,
                  __comment:
                        'Repo registry (generated by --initial-project). Edit team_id/keywords/active_branch/commands as needed.',
                  __enums: {
                        Domains: ['DP', 'IDP', 'Core', 'GD', 'NTF', 'Media', 'HexaBlox', 'Common'],
                        Usage: ['Backend', 'Frontend', 'Mobile', 'Tooling'],
                        Kind: ['Service', 'App', 'Package', 'Tool'],
                  },
                  repos: stableSort(reposSuggested, (r) => String(r.repo_id)),
            };

            // In interactive mode, confirm before writing generated configs.
            const writeGeneratedConfig = nonInt
                  ? true
                  : await promptYesNo(rl, 'Write generated TEAMS.json / REPOS.json / AGENTS.json into project now?', { defaultYes: true });

            const cfg = {
                  version: 4,
                  project_code,
                  repos_root_abs: reposRootAbs,
                  ops_root_abs: opsRootAbs,
                  knowledge_repo_dir: knowledgeRootAbs,
                  ssot_bundle_policy: { global_packs: [] },
            };

            const wroteProject = await writeJsonAtomic(
                  join(opsRootAbs, 'config', 'PROJECT.json'),
                  cfg,
                  { dryRun },
            );
            const wroteRepos = await writeJsonAtomic(
                  join(opsRootAbs, 'config', 'REPOS.json'),
                  writeGeneratedConfig ? reposDocGenerated : { version: 1, repos: clonedRepoDirs.map((r) => ({ repo_id: r.repo_id, path: r.dir_name, status: 'active' })) },
                  { dryRun },
            );
            const wroteTeams = await writeJsonAtomic(
                  join(opsRootAbs, 'config', 'TEAMS.json'),
                  writeGeneratedConfig ? teamsDoc : { version: 1, teams: [] },
                  { dryRun },
            );
            const wroteAgents = await writeJsonAtomic(
                  join(opsRootAbs, 'config', 'AGENTS.json'),
                  writeGeneratedConfig ? agentsDoc : { version: 3, agents: [] },
                  { dryRun },
            );

            if (!dryRun && writeGeneratedConfig) {
                  const onboardingDir = join(opsRootAbs, 'ai', 'onboarding');
                  await mkdir(onboardingDir, { recursive: true });
                  await writeTextAtomic(
                        join(onboardingDir, 'repo_scan.json'),
                        JSON.stringify(
                              { version: 1, repos_root: reposRootAbs, scanned: stableSort(scan, (x) => x.repo_id) },
                              null,
                              2,
                        ) + '\n',
                  );
                  await writeTextAtomic(
                        join(onboardingDir, 'branch_scan.json'),
                        JSON.stringify(
                              { version: 1, repos_root: reposRootAbs, scanned: stableSort(branchScan, (x) => x.repo_id) },
                              null,
                              2,
                        ) + '\n',
                  );
                  await writeTextAtomic(
                        join(onboardingDir, 'commands_suggested.json'),
                        JSON.stringify(
                              { version: 1, repos_root: reposRootAbs, suggested: stableSort(commandsSuggested, (x) => x.repo_id) },
                              null,
                              2,
                        ) + '\n',
                  );
            }
            const wroteDocs = await writeJsonAtomic(
                  join(opsRootAbs, 'config', 'DOCS.json'),
                  {
                        version: 1,
                        project_key: project_code,
                        docs_repo_path: knowledgeRootAbs,
                        knowledge_repo_path: knowledgeRootAbs,
                        output_format: 'markdown',
                        parts_word_target: 1800,
                        max_docs_per_run: 3,
                        commit: {
                              enabled: true,
                              branch: 'main',
                              allow_dirty: false,
                        },
                  },
                  { dryRun },
            );

            const templatesDir = join(toolAbs, 'ai', 'templates');
            const wrotePolicies = await copyTemplateIfMissing({
                  srcAbs: join(templatesDir, 'POLICIES.json'),
                  dstAbs: join(opsRootAbs, 'config', 'POLICIES.json'),
                  dryRun,
            });
            const wroteProfiles = await copyTemplateIfMissing({
                  srcAbs: join(templatesDir, 'LLM_PROFILES.json'),
                  dstAbs: join(opsRootAbs, 'config', 'LLM_PROFILES.json'),
                  dryRun,
            });
            const mergedProfiles = await mergeLlmProfilesFromTemplateIfMissingKeys({
                  templateAbs: join(templatesDir, 'LLM_PROFILES.json'),
                  dstAbs: join(opsRootAbs, 'config', 'LLM_PROFILES.json'),
                  requiredProfileKeys: [
                        // Contract: Lane B QA inspector.
                        'qa.inspector',
                        // Contract: Lane A committee (4 LLMs).
                        'committee.repo_architect',
                        'committee.repo_skeptic',
                        'committee.integration_chair',
                        'committee.qa_strategist',
                  ],
                  dryRun,
            });
            if (!mergedProfiles.ok)
                  return { ok: false, message: mergedProfiles.message };
            await copyTemplateIfMissing({
                  srcAbs: join(templatesDir, 'DECISIONS_NEEDED.md'),
                  dstAbs: join(
                        opsRootAbs,
                        'ai',
                        'lane_b',
                        'DECISIONS_NEEDED.md',
                  ),
                  dryRun,
            });

            if (!existsSync(join(opsRootAbs, 'config', 'TEAMS.json')))
                  await writeJsonAtomic(
                        join(opsRootAbs, 'config', 'TEAMS.json'),
                        { version: 1, teams: [] },
                        { dryRun },
                  );
            if (!existsSync(join(opsRootAbs, 'config', 'AGENTS.json')))
                  await writeJsonAtomic(
                        join(opsRootAbs, 'config', 'AGENTS.json'),
                        { version: 3, agents: [] },
                        { dryRun },
                  );

            if (!dryRun) {
                  const prev = process.env.AI_PROJECT_ROOT;
                  process.env.AI_PROJECT_ROOT = opsRootAbs;
                  try {
                        await ensureLaneADirs({ projectRoot: opsRootAbs });
                        await ensureLaneBDirs({ projectRoot: opsRootAbs });
                        await ensureKnowledgeDirs({ projectRoot: opsRootAbs });
                  } finally {
                        if (typeof prev === 'string')
                              process.env.AI_PROJECT_ROOT = prev;
                        else delete process.env.AI_PROJECT_ROOT;
                  }
            }

            const gitInit = sys.git.initIfMissing(knowledgeRootAbs);
            if (!gitInit.ok) {
                  return {
                        ok: false,
                        message: `Failed to initialize knowledge git repo at ${knowledgeRootAbs}: ${gitInit.message}`,
                        project_code,
                        project_home: projectHomeAbs,
                  };
            }

            const safeDir = sys.git.ensureSafeDirectory(knowledgeRootAbs);
            const originRes = sys.git.ensureOriginIfProvided(knowledgeRootAbs);
            if (!originRes.ok) {
                  return {
                        ok: false,
                        message: `Failed to configure knowledge origin: ${originRes.message}`,
                        project_code,
                        knowledge_repo_dir: knowledgeRootAbs,
                  };
            }

            if (!dryRun) {
                  await ensureKnowledgeStructure({ knowledgeRootAbs });
                  const readmeAbs = join(knowledgeRootAbs, 'README.md');
                  if (!existsSync(readmeAbs)) {
                        await writeTextAtomic(
                              readmeAbs,
                              [
                                    '# Project Knowledge Repo',
                                    '',
                                    '- Canonical knowledge for this project only.',
                                    '- Structure:',
                                    '  - ssot/ (curated truth)',
                                    '  - evidence/ (curated evidence + index)',
                                    '  - views/ (deterministic views)',
                                    '  - sessions/ (interviews + kickoff)',
                                    '  - decisions/ (human answers)',
                                    '  - docs/ (writer output)',
                                    '  - events/summary.json (compacted)',
                                    '',
                              ].join('\n'),
                        );
                  }
            }

            const envLines = [
                  `AI_PROJECT_ROOT=${opsRootAbs}`,
                  `REPOS_ROOT=${reposRootAbs}`,
                  `KNOWLEDGE_REPO_DIR=${knowledgeRootAbs}`,
                  `AI_PROJECT_KEY=${project_code}`,
                  `AI_TEAM_REPO=${toolAbs}`,
                  `CLI_PATH=${join(toolAbs, 'src', 'cli.js')}`,
            ];
            if (!dryRun) {
                  await writeTextAtomic(
                        join(opsRootAbs, '.env'),
                        envLines.join('\n') + '\n',
                  );
            }

            // Registry update (global, in tool repo) + port allocation (lock-protected).
            const registryResult = await withRegistryLock(
                  async () => {
                        const regRes = await loadRegistry({
                              toolRepoRoot: toolAbs,
                              createIfMissing: true,
                        });
                        const reg = regRes.registry;
                        const existing = getProject(reg, project_code);
                        const allocated =
                              existing && existing.status === 'active'
                                    ? {
                                            webui_port:
                                                  existing.ports.webui_port,
                                            websvc_port:
                                                  existing.ports.websvc_port,
                                      }
                                    : allocatePorts(reg);

                        const ecoAbs = join(
                              opsRootAbs,
                              'pm2',
                              'ecosystem.config.cjs',
                        );
                        const eco = renderPm2Ecosystem({
                              project_code,
                              toolAbs,
                              opsRootAbs,
                              reposRootAbs,
                              knowledgeRootAbs,
                              ports: allocated,
                        });
                        if (!dryRun) await writeTextAtomic(ecoAbs, eco.text);

                        const cronAbs = join(
                              opsRootAbs,
                              'cron',
                              'ai-team.cron',
                        );
                        const cron = renderCronFile({
                              project_code,
                              toolAbs,
                              opsRootAbs,
                        });
                        if (!dryRun) await writeTextAtomic(cronAbs, cron.text);
                        const cronInstall = sys.cron.installBlock({
                              project_code,
                              entries: cron.entries,
                        });
                        if (!cronInstall.ok && !dryRun) {
                              return {
                                    ok: false,
                                    message: `Failed to install cron for ${project_code}: ${cronInstall.message || 'unknown error'}`,
                              };
                        }

                        const lastCommit = dryRun
                              ? null
                              : sys.git.headSha(knowledgeRootAbs);
                        const projectRec = {
                              project_code,
                              status: 'active',
                              root_dir: projectHomeAbs,
                              ops_dir: opsRootAbs,
                              repos_dir: reposRootAbs,
                              created_at: existing?.created_at || nowISO(),
                              updated_at: nowISO(),
                              ports: {
                                    webui_port: allocated.webui_port,
                                    websvc_port: allocated.websvc_port,
                              },
                              pm2: {
                                    ecosystem_path: ecoAbs,
                                    apps: eco.apps.slice(),
                              },
                              cron: {
                                    installed: cronInstall.installed === true,
                                    entries: cron.entries.slice(),
                              },
                              knowledge: {
                                    type: 'git',
                                    abs_path: knowledgeRootAbs,
                                    git_remote: originRes.remote || '',
                                    default_branch:
                                          originRes.default_branch || 'main',
                                    active_branch:
                                          originRes.default_branch || 'main',
                                    last_commit_sha: lastCommit || null,
                              },
                              repos: Array.isArray(existing?.repos)
                                    ? existing.repos
                                    : [],
                        };

                        upsertProject(reg, projectRec);
                        if (!dryRun)
                              await writeRegistry(reg, {
                                    toolRepoRoot: toolAbs,
                              });
                        return {
                              ok: true,
                              allocated,
                              ecosystem_path: ecoAbs,
                              cron_path: cronAbs,
                              cron_installed: cronInstall.installed === true,
                        };
                  },
                  { toolRepoRoot: toolAbs },
            );

            if (!registryResult.ok) {
                  return {
                        ok: false,
                        message:
                              registryResult.message ||
                              `Failed to update project registry for ${project_code}.`,
                        project_code,
                        project_home: projectHomeAbs,
                        ops_root_abs: opsRootAbs,
                        repos_root_abs: reposRootAbs,
                        knowledge_repo_dir: knowledgeRootAbs,
                  };
            }

            return {
                  ok: true,
                  dry_run: !!dryRun,
                  created_at: nowISO(),
                  project_code,
                  project_home: projectHomeAbs,
                  ops_root_abs: opsRootAbs,
                  repos_root_abs: reposRootAbs,
                  knowledge_repo_dir: knowledgeRootAbs,
                  ports: registryResult.ok ? registryResult.allocated : null,
                  pm2: registryResult.ok
                        ? { ecosystem_path: registryResult.ecosystem_path }
                        : null,
                  cron: registryResult.ok
                        ? {
                                installed: registryResult.cron_installed,
                                cron_path: registryResult.cron_path,
                          }
                        : null,
                  warnings: [
                        safeDir.ok ? null : safeDir.message,
                        ...warnings,
                  ].filter(Boolean),
                  wrote: {
                        project_json: wroteProject.path,
                        repos_json: wroteRepos.path,
                        teams_json: wroteTeams.path,
                        agents_json: wroteAgents.path,
                        docs_json: wroteDocs.path,
                        policies_json: wrotePolicies.path,
                        llm_profiles_json: wroteProfiles.path,
                  },
                  note: `Project created under ${projectHomeAbs} with OPS_ROOT as AI_PROJECT_ROOT.`,
            };
      } finally {
            if (rl) rl.close();
      }
}

export async function runMigrateProjectLayout({
      legacyRootAbs = null,
      dryRun = false,
} = {}) {
      const legacy = legacyRootAbs
            ? requireAbs(legacyRootAbs, 'legacyRootAbs')
            : requireAbs(
                    process.env.AI_PROJECT_ROOT,
                    'AI_PROJECT_ROOT (legacy)',
              );
      if (!legacy.includes('/opt/AI-Projects/')) {
            return {
                  ok: false,
                  message: `Refuse to migrate: AI_PROJECT_ROOT does not look like legacy /opt/AI-Projects/* (got: ${legacy})`,
            };
      }
      const project_code = legacy.split('/').filter(Boolean).at(-1);
      if (!project_code)
            return {
                  ok: false,
                  message: 'Unable to infer project_code from legacy root.',
            };

      const projectHomeAbs = resolve('/opt/AI-Projects', project_code);
      const opsRootAbs = join(projectHomeAbs, 'ops');
      const reposRootAbs = join(projectHomeAbs, 'repos');
      const knowledgeRootAbs = join(projectHomeAbs, 'knowledge');

      if (!dryRun) {
            await mkdir(join(opsRootAbs, 'config'), { recursive: true });
            await mkdir(reposRootAbs, { recursive: true });
            await mkdir(knowledgeRootAbs, { recursive: true });
      }

      // Move runtime state into ops (idempotent: skip if destination exists).
      const legacyAiAbs = join(legacy, 'ai');
      const legacyConfigAbs = join(legacy, 'config');
      const dstAiAbs = join(opsRootAbs, 'ai');
      const dstCfgAbs = join(opsRootAbs, 'config');

      const moves = [];
      if (existsSync(legacyAiAbs) && !existsSync(dstAiAbs))
            moves.push({ from: legacyAiAbs, to: dstAiAbs });
      if (existsSync(legacyConfigAbs) && !existsSync(dstCfgAbs))
            moves.push({ from: legacyConfigAbs, to: dstCfgAbs });
      if (!dryRun) {
            for (const m of moves) {
                  // eslint-disable-next-line no-await-in-loop
                  await rename(m.from, m.to);
            }
      }

      // Rewrite PROJECT.json to hard-contract v1.
      const cfg = {
            version: 4,
            project_code,
            repos_root_abs: reposRootAbs,
            ops_root_abs: opsRootAbs,
            knowledge_repo_dir: knowledgeRootAbs,
            ssot_bundle_policy: { global_packs: [] },
      };
      await writeJsonAtomic(join(opsRootAbs, 'config', 'PROJECT.json'), cfg, {
            dryRun,
      });
      await writeJsonAtomic(
            join(opsRootAbs, 'config', 'DOCS.json'),
            {
                  version: 1,
                  project_key: project_code,
                  docs_repo_path: knowledgeRootAbs,
                  knowledge_repo_path: knowledgeRootAbs,
                  output_format: 'markdown',
                  parts_word_target: 1800,
                  max_docs_per_run: 3,
                  commit: { enabled: true, branch: 'main', allow_dirty: false },
            },
            { dryRun },
      );

      const gitInit = gitInitIfMissing(knowledgeRootAbs, { dryRun });
      if (!gitInit.ok)
            return {
                  ok: false,
                  message: `Failed to init knowledge repo: ${gitInit.message}`,
            };
      if (!dryRun) await ensureKnowledgeStructure({ knowledgeRootAbs });

      if (!dryRun) {
            await writeTextAtomic(
                  join(opsRootAbs, '.env'),
                  [
                        `AI_PROJECT_ROOT=${opsRootAbs}`,
                        `REPOS_ROOT=${reposRootAbs}`,
                        `AI_PROJECT_KEY=${project_code}`,
                  ].join('\n') + '\n',
            );
      }

      return {
            ok: true,
            dry_run: !!dryRun,
            migrated_at: nowISO(),
            project_code,
            legacy_root: legacy,
            project_home: projectHomeAbs,
            ops_root_abs: opsRootAbs,
            repos_root_abs: reposRootAbs,
            knowledge_repo_dir: knowledgeRootAbs,
            moved: moves,
      };
}
