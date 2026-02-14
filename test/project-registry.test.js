import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync, readFileSync } from 'node:fs';
import { mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

import {
      loadRegistry,
      withRegistryLock,
      allocatePorts,
      writeRegistry,
      listProjects,
      getProject,
      removeProject,
} from '../src/registry/project-registry.js';
import { runInitialProjectOnboarding } from '../src/onboarding/onboarding-runner.js';
import { resolveProjectPaths } from '../src/registry/dependency-resolver.js';

function repoRootAbs() {
      return resolve(dirname(fileURLToPath(import.meta.url)), '..');
}

function runGitInit(repoAbs) {
      const r = spawnSync('git', ['init', '-q'], {
            cwd: repoAbs,
            encoding: 'utf8',
      });
      return { ok: r.status === 0, stderr: String(r.stderr || '') };
}

function runGitBareInit(repoAbs) {
      const r = spawnSync('git', ['init', '--bare', '-q', repoAbs], {
            encoding: 'utf8',
      });
      return { ok: r.status === 0, stderr: String(r.stderr || '') };
}

test('project registry initializes when missing', async () => {
      const regDir = mkdtempSync(join(tmpdir(), 'ai-team-registry-'));
      process.env.AI_TEAM_REGISTRY_DIR = regDir;
      try {
            const res = await loadRegistry({
                  toolRepoRoot: repoRootAbs(),
                  createIfMissing: true,
            });
            assert.equal(res.ok, true);
            assert.equal(existsSync(join(regDir, 'REGISTRY.json')), true);
            const parsed = JSON.parse(
                  readFileSync(join(regDir, 'REGISTRY.json'), 'utf8'),
            );
            assert.equal(parsed.version, 2);
            assert.ok(
                  typeof parsed.host_id === 'string' &&
                        parsed.host_id.length > 0,
            );
            assert.ok(
                  typeof parsed.created_at === 'string' &&
                        parsed.created_at.includes('T'),
            );
            assert.ok(
                  typeof parsed.updated_at === 'string' &&
                        parsed.updated_at.includes('T'),
            );
      } finally {
            delete process.env.AI_TEAM_REGISTRY_DIR;
      }
});

test('allocatePorts increments safely under registry lock', async () => {
      const regDir = mkdtempSync(join(tmpdir(), 'ai-team-registry-ports-'));
      process.env.AI_TEAM_REGISTRY_DIR = regDir;
      try {
            await withRegistryLock(
                  async () => {
                        const r = await loadRegistry({
                              toolRepoRoot: repoRootAbs(),
                              createIfMissing: true,
                        });
                        const p1 = allocatePorts(r.registry);
                        await writeRegistry(r.registry, {
                              toolRepoRoot: repoRootAbs(),
                        });
                        assert.deepEqual(p1, {
                              webui_port: 8090,
                              websvc_port: 8091,
                        });
                  },
                  { toolRepoRoot: repoRootAbs() },
            );

            await withRegistryLock(
                  async () => {
                        const r = await loadRegistry({
                              toolRepoRoot: repoRootAbs(),
                              createIfMissing: true,
                        });
                        const p2 = allocatePorts(r.registry);
                        await writeRegistry(r.registry, {
                              toolRepoRoot: repoRootAbs(),
                        });
                        assert.deepEqual(p2, {
                              webui_port: 8091,
                              websvc_port: 8092,
                        });
                  },
                  { toolRepoRoot: repoRootAbs() },
            );
      } finally {
            delete process.env.AI_TEAM_REGISTRY_DIR;
      }
});

test('initial-project writes registry entry with ports and paths (no real crontab writes)', async () => {
      const regDir = mkdtempSync(join(tmpdir(), 'ai-team-registry-init-'));
      const projectsRoot = mkdtempSync(
            join(tmpdir(), 'ai-team-projects-root-'),
      );
      process.env.AI_TEAM_REGISTRY_DIR = regDir;
      process.env.AI_TEAM_PROJECTS_ROOT = projectsRoot;
      try {
            const toolAbs = repoRootAbs();
            const system = {
                  git: {
                        initIfMissing(repoAbs) {
                              return runGitInit(repoAbs);
                        },
                        ensureSafeDirectory() {
                              return { ok: true, wrote: false };
                        },
                        ensureOriginIfProvided() {
                              return {
                                    ok: true,
                                    remote: '',
                                    default_branch: 'main',
                                    created: false,
                              };
                        },
                        headSha() {
                              return null;
                        },
                  },
                  cron: {
                        installBlock({ entries }) {
                              return { ok: true, installed: true, entries };
                        },
                  },
            };

            const res = await runInitialProjectOnboarding({
                  toolRepoRoot: toolAbs,
                  dryRun: false,
                  project: 'alpha',
                  nonInteractive: true,
                  system,
            });
            assert.equal(res.ok, true);
            assert.equal(res.project_code, 'alpha');
            assert.equal(
                  existsSync(
                        join(
                              projectsRoot,
                              'alpha',
                              'ops',
                              'config',
                              'PROJECT.json',
                        ),
                  ),
                  true,
            );

            const reg = JSON.parse(
                  readFileSync(join(regDir, 'REGISTRY.json'), 'utf8'),
            );
            const p = reg.projects.find((x) => x.project_code === 'alpha');
            assert.ok(p);
            assert.equal(p.status, 'active');
            assert.equal(p.root_dir, join(projectsRoot, 'alpha'));
            assert.equal(p.ops_dir, join(projectsRoot, 'alpha', 'ops'));
            assert.equal(p.repos_dir, join(projectsRoot, 'alpha', 'repos'));
            assert.deepEqual(p.ports, { webui_port: 8090, websvc_port: 8091 });
            assert.ok(
                  typeof p.pm2?.ecosystem_path === 'string' &&
                        p.pm2.ecosystem_path.includes('/ops/pm2/'),
            );
            assert.equal(Array.isArray(p.pm2?.apps), true);
            assert.equal(p.cron.installed, true);
            assert.equal(Array.isArray(p.cron.entries), true);
            assert.ok(
                  typeof p.knowledge?.abs_path === 'string' &&
                        p.knowledge.abs_path.endsWith('/knowledge'),
            );
      } finally {
            delete process.env.AI_TEAM_REGISTRY_DIR;
            delete process.env.AI_TEAM_PROJECTS_ROOT;
      }
});

test('initial-project records cloned repos into registry project.repos', async () => {
      const regDir = mkdtempSync(
            join(tmpdir(), 'ai-team-registry-init-repos-'),
      );
      const projectsRoot = mkdtempSync(
            join(tmpdir(), 'ai-team-projects-root-repos-'),
      );
      const remotesRoot = mkdtempSync(join(tmpdir(), 'ai-team-remotes-'));
      const codeRemote = join(remotesRoot, 'code-repo.git');
      const bare = runGitBareInit(codeRemote);
      assert.equal(bare.ok, true, bare.stderr);

      process.env.AI_TEAM_REGISTRY_DIR = regDir;
      process.env.AI_TEAM_PROJECTS_ROOT = projectsRoot;
      process.env.AI_TEAM_CODE_REPO_PATHS = codeRemote;
      try {
            const toolAbs = repoRootAbs();
            const system = {
                  git: {
                        initIfMissing(repoAbs) {
                              return runGitInit(repoAbs);
                        },
                        ensureSafeDirectory() {
                              return { ok: true, wrote: false };
                        },
                        ensureOriginIfProvided() {
                              return {
                                    ok: true,
                                    remote: '',
                                    default_branch: 'main',
                                    created: false,
                              };
                        },
                        headSha() {
                              return null;
                        },
                  },
                  cron: {
                        installBlock({ entries }) {
                              return { ok: true, installed: true, entries };
                        },
                  },
            };

            const res = await runInitialProjectOnboarding({
                  toolRepoRoot: toolAbs,
                  dryRun: false,
                  project: 'alpha-repos',
                  nonInteractive: true,
                  system,
            });
            assert.equal(res.ok, true);

            const reg = JSON.parse(
                  readFileSync(join(regDir, 'REGISTRY.json'), 'utf8'),
            );
            const p = reg.projects.find((x) => x.project_code === 'alpha-repos');
            assert.ok(p);
            assert.equal(Array.isArray(p.repos), true);
            assert.equal(p.repos.length, 1);
            assert.equal(p.repos[0].repo_id, 'code-repo');
            assert.equal(p.repos[0].active, true);
            assert.ok(
                  typeof p.repos[0].abs_path === 'string' &&
                        p.repos[0].abs_path.endsWith('/repos/code-repo'),
            );
      } finally {
            delete process.env.AI_TEAM_REGISTRY_DIR;
            delete process.env.AI_TEAM_PROJECTS_ROOT;
            delete process.env.AI_TEAM_CODE_REPO_PATHS;
      }
});

test('remove-project defaults to keep-files and marks removed', async () => {
      const regDir = mkdtempSync(join(tmpdir(), 'ai-team-registry-remove-'));
      process.env.AI_TEAM_REGISTRY_DIR = regDir;
      try {
            const r = await withRegistryLock(
                  async () => {
                        const res = await loadRegistry({
                              toolRepoRoot: repoRootAbs(),
                              createIfMissing: true,
                        });
                        res.registry.projects.push({
                              project_code: 'alpha',
                              status: 'active',
                              root_dir: '/opt/AI-Projects/alpha',
                              ops_dir: '/opt/AI-Projects/alpha/ops',
                              repos_dir: '/opt/AI-Projects/alpha/repos',
                              created_at: new Date().toISOString(),
                              updated_at: new Date().toISOString(),
                              ports: { webui_port: 8090, websvc_port: 8091 },
                              pm2: {
                                    ecosystem_path:
                                          '/opt/AI-Projects/alpha/ops/pm2/ecosystem.config.cjs',
                                    apps: ['alpha-webui', 'alpha-websvc'],
                              },
                              cron: { installed: false, entries: [] },
                              knowledge: {
                                    type: 'git',
                                    abs_path: '/opt/AI-Projects/alpha/knowledge',
                                    git_remote: '',
                                    default_branch: 'main',
                                    active_branch: 'main',
                                    last_commit_sha: null,
                              },
                              repos: [],
                        });
                        await writeRegistry(res.registry, {
                              toolRepoRoot: repoRootAbs(),
                        });
                        return res.registry;
                  },
                  { toolRepoRoot: repoRootAbs() },
            );
            assert.equal(
                  listProjects(r).some((p) => p.project_code === 'alpha'),
                  true,
            );

            const out = await removeProject('alpha', {
                  toolRepoRoot: repoRootAbs(),
                  keepFiles: true,
                  dryRun: false,
            });
            assert.equal(out.ok, true);
            const reread = JSON.parse(
                  readFileSync(join(regDir, 'REGISTRY.json'), 'utf8'),
            );
            const p = getProject(reread, 'alpha');
            assert.ok(p);
            assert.equal(p.status, 'removed');
      } finally {
            delete process.env.AI_TEAM_REGISTRY_DIR;
      }
});

test('dependency resolver writes a dependency_missing decision artifact when project not in registry', async () => {
      const root = mkdtempSync(join(tmpdir(), 'ai-team-dep-resolve-'));
      const opsRoot = join(root, 'ops');
      mkdirSync(join(opsRoot, 'config'), { recursive: true });
      mkdirSync(join(opsRoot, 'ai', 'lane_a', 'decisions_needed'), {
            recursive: true,
      });
      process.env.AI_PROJECT_ROOT = opsRoot;
      try {
            const regDir = mkdtempSync(join(tmpdir(), 'ai-team-registry-dep-'));
            process.env.AI_TEAM_REGISTRY_DIR = regDir;
            const res = await resolveProjectPaths({
                  project_code: 'missing',
                  onMissing: 'decision_packet',
                  opsRootAbs: opsRoot,
            });
            assert.equal(res.ok, false);
            assert.equal(res.missing, true);
            assert.ok(
                  typeof res.decision_path === 'string' &&
                        res.decision_path.includes('DEPENDENCY_MISSING-'),
            );
            assert.equal(existsSync(res.decision_path), true);
      } finally {
            delete process.env.AI_PROJECT_ROOT;
            delete process.env.AI_TEAM_REGISTRY_DIR;
      }
});
