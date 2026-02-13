import { existsSync } from 'node:fs';
import {
      mkdir,
      rename,
      unlink,
      writeFile,
      open,
      readFile,
      stat,
      rm,
} from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { hostname } from 'node:os';
import { dirname, isAbsolute, join, resolve } from 'node:path';

import { validateProjectRegistry } from '../contracts/validators/index.js';

function normStr(x) {
      return typeof x === 'string' ? x.trim() : '';
}

function nowISO() {
      return new Date().toISOString();
}

function isPlainObject(x) {
      return !!x && typeof x === 'object' && !Array.isArray(x);
}

function defaultRegistry({ hostId }) {
      const iso = nowISO();
      const obj = {
            version: 2,
            host_id: hostId,
            created_at: iso,
            updated_at: iso,
            ports: {
                  webui_base: 8090,
                  webui_next: 8090,
                  websvc_base: 8091,
                  websvc_next: 8091,
            },
            projects: [],
      };
      validateProjectRegistry(obj);
      return obj;
}

function resolveAiTeamRepoRoot({ toolRepoRoot = null } = {}) {
      const fromEnv = normStr(process.env.AI_TEAM_REPO);
      const fromArg = normStr(toolRepoRoot);
      const root = fromArg || fromEnv || process.cwd();
      const abs = resolve(root);
      if (!isAbsolute(abs))
            throw new Error(
                  `AI_TEAM_REPO must be an absolute path (got: ${root}).`,
            );
      return abs;
}

export function resolveRegistryDirAbs({ toolRepoRoot = null } = {}) {
      const override = normStr(process.env.AI_TEAM_REGISTRY_DIR);
      if (override) return resolve(override);
      const repoRootAbs = resolveAiTeamRepoRoot({ toolRepoRoot });
      return join(repoRootAbs, 'ai', 'registry');
}

function registryPaths({ toolRepoRoot = null } = {}) {
      const dirAbs = resolveRegistryDirAbs({ toolRepoRoot });
      return {
            dirAbs,
            jsonAbs: join(dirAbs, 'REGISTRY.json'),
            lockAbs: join(dirAbs, 'REGISTRY.lock'),
      };
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

async function writeJsonAtomic(absPath, obj) {
      await writeTextAtomic(absPath, JSON.stringify(obj, null, 2) + '\n');
}

async function acquireAdvisoryLock({
      lockAbs,
      timeoutMs = 30_000,
      pollMs = 100,
      staleMs = 10 * 60 * 1000,
} = {}) {
      const abs = resolve(String(lockAbs || ''));
      await mkdir(dirname(abs), { recursive: true });
      const started = Date.now();
      // Advisory lock using O_EXCL create.
      while (true) {
            try {
                  // eslint-disable-next-line no-await-in-loop
                  const fh = await open(abs, 'wx');
                  try {
                        // best-effort metadata
                        // eslint-disable-next-line no-await-in-loop
                        await fh.writeFile(
                              JSON.stringify({
                                    pid: process.pid,
                                    acquired_at: nowISO(),
                              }) + '\n',
                              'utf8',
                        );
                  } catch {
                        // ignore
                  }
                  return { ok: true, handle: fh, path: abs };
            } catch (err) {
                  const code = err && typeof err === 'object' ? err.code : null;
                  if (code !== 'EEXIST')
                        return {
                              ok: false,
                              reason: 'open_failed',
                              message:
                                    err instanceof Error
                                          ? err.message
                                          : String(err),
                        };
                  // Stale lock recovery (best-effort): if the lock file is older than staleMs, remove and retry.
                  try {
                        // eslint-disable-next-line no-await-in-loop
                        const st = await stat(abs);
                        const age = Date.now() - Number(st.mtimeMs || 0);
                        if (Number.isFinite(age) && age > staleMs) {
                              // eslint-disable-next-line no-await-in-loop
                              await unlink(abs);
                              continue;
                        }
                  } catch {
                        // ignore and fall back to waiting
                  }
                  if (Date.now() - started > timeoutMs)
                        return {
                              ok: false,
                              reason: 'locked',
                              message: `Registry lock already held: ${abs}`,
                        };
                  // eslint-disable-next-line no-await-in-loop
                  await new Promise((r) => setTimeout(r, pollMs));
            }
      }
}

async function releaseAdvisoryLock(lock) {
      if (!lock || !lock.handle) return;
      try {
            // eslint-disable-next-line no-await-in-loop
            await lock.handle.close();
      } catch {
            // ignore
      }
      try {
            // eslint-disable-next-line no-await-in-loop
            await unlink(lock.path);
      } catch {
            // ignore
      }
}

export async function withRegistryLock(
      fn,
      { toolRepoRoot = null, timeoutMs = 30_000 } = {},
) {
      const p = registryPaths({ toolRepoRoot });
      const lock = await acquireAdvisoryLock({ lockAbs: p.lockAbs, timeoutMs });
      if (!lock.ok)
            throw new Error(
                  lock.message || 'Registry lock acquisition failed.',
            );
      try {
            return await fn();
      } finally {
            await releaseAdvisoryLock(lock);
      }
}

export async function loadRegistry({
      toolRepoRoot = null,
      createIfMissing = true,
} = {}) {
      const p = registryPaths({ toolRepoRoot });
      await mkdir(p.dirAbs, { recursive: true });
      if (!existsSync(p.jsonAbs)) {
            if (!createIfMissing)
                  return {
                        ok: true,
                        exists: false,
                        registry: null,
                        path: p.jsonAbs,
                        dir: p.dirAbs,
                  };
            const reg = defaultRegistry({ hostId: hostname() });
            await writeJsonAtomic(p.jsonAbs, reg);
            return {
                  ok: true,
                  exists: false,
                  registry: reg,
                  path: p.jsonAbs,
                  dir: p.dirAbs,
            };
      }
      const raw = await readFile(p.jsonAbs, 'utf8');
      const parsed = JSON.parse(String(raw || ''));
      // If the tracked file uses placeholders, normalize deterministically on first load.
      const hostId = normStr(parsed?.host_id) || hostname();
      const reg = {
            ...parsed,
            host_id: hostId,
            created_at:
                  normStr(parsed?.created_at) &&
                  parsed.created_at !== '1970-01-01T00:00:00.000Z'
                        ? parsed.created_at
                        : nowISO(),
            updated_at: nowISO(),
      };
      validateProjectRegistry(reg);
      // Do not auto-write here; callers must use withRegistryLock for writes.
      return {
            ok: true,
            exists: true,
            registry: reg,
            path: p.jsonAbs,
            dir: p.dirAbs,
      };
}

export function listProjects(registry) {
      const projects = Array.isArray(registry?.projects)
            ? registry.projects
            : [];
      return projects
            .map((p) => ({
                  project_code: normStr(p.project_code),
                  root_dir: normStr(p.root_dir),
                  status: normStr(p.status),
                  ports: p.ports || null,
                  updated_at: p.updated_at || null,
            }))
            .sort((a, b) => a.project_code.localeCompare(b.project_code));
}

export function getProject(registry, projectCode) {
      const code = normStr(projectCode);
      const projects = Array.isArray(registry?.projects)
            ? registry.projects
            : [];
      return projects.find((p) => normStr(p.project_code) === code) || null;
}

export function allocatePorts(registry) {
      const r = registry;
      if (!r || !r.ports)
            throw new Error('allocatePorts: registry.ports missing.');
      const webui_port = Number(r.ports.webui_next);
      const websvc_port = Number(r.ports.websvc_next);
      if (!Number.isFinite(webui_port) || !Number.isFinite(websvc_port))
            throw new Error('allocatePorts: invalid registry next ports.');
      r.ports.webui_next = webui_port + 1;
      r.ports.websvc_next = websvc_port + 1;
      return { webui_port, websvc_port };
}

export function upsertProject(registry, projectRecord) {
      const rec = projectRecord;
      if (!isPlainObject(rec))
            throw new Error('upsertProject: projectRecord must be an object.');
      const code = normStr(rec.project_code);
      if (!code) throw new Error('upsertProject: project_code required.');
      const next = Array.isArray(registry.projects)
            ? registry.projects.slice()
            : [];
      const idx = next.findIndex((p) => normStr(p.project_code) === code);
      if (idx >= 0) next[idx] = rec;
      else next.push(rec);
      registry.projects = next;
      return registry;
}

export function markProjectRemoved(registry, projectCode) {
      const code = normStr(projectCode);
      const next = Array.isArray(registry.projects)
            ? registry.projects.slice()
            : [];
      const idx = next.findIndex((p) => normStr(p.project_code) === code);
      if (idx < 0) return { ok: false, message: `Project not found: ${code}` };
      next[idx] = { ...next[idx], status: 'removed', updated_at: nowISO() };
      registry.projects = next;
      return { ok: true, project: next[idx] };
}

export async function writeRegistry(registry, { toolRepoRoot = null } = {}) {
      const p = registryPaths({ toolRepoRoot });
      const reg = { ...registry, updated_at: nowISO() };
      validateProjectRegistry(reg);
      await writeJsonAtomic(p.jsonAbs, reg);
      return { ok: true, path: p.jsonAbs };
}

export function resolveProjectPathsByCode(registry, projectCode) {
      const p = getProject(registry, projectCode);
      if (!p || normStr(p.status) !== 'active') return null;
      return {
            ops_dir: normStr(p.ops_dir),
            repos_dir: normStr(p.repos_dir),
            knowledge_dir: normStr(p.knowledge?.abs_path),
            root_dir: normStr(p.root_dir),
      };
}

function removeCronBlockFromText(text, projectCode) {
      const begin = `# AI-TEAM ${projectCode} BEGIN`;
      const end = `# AI-TEAM ${projectCode} END`;
      const lines = String(text || '').split('\n');
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
      while (out.length && out[out.length - 1] === '') out.pop();
      return out.join('\n') + '\n';
}

function hasCmd(cmd) {
      const r = spawnSync(
            'bash',
            ['-lc', `command -v ${cmd} >/dev/null 2>&1`],
            { encoding: 'utf8' },
      );
      return r.status === 0;
}

function pm2StopDelete(app) {
      if (!hasCmd('pm2')) return { ok: false, message: 'pm2 not installed' };
      const name = normStr(app);
      if (!name) return { ok: true };
      const del = spawnSync('pm2', ['delete', name], { encoding: 'utf8' });
      if (del.status === 0) return { ok: true };
      const stop = spawnSync('pm2', ['stop', name], { encoding: 'utf8' });
      return {
            ok: stop.status === 0,
            message: String(
                  del.stderr || del.stdout || stop.stderr || stop.stdout || '',
            ).trim(),
      };
}

function crontabList() {
      if (!hasCmd('crontab'))
            return { ok: false, text: '', message: 'crontab not installed' };
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

async function deleteProjectHomeBestEffort({
      rootDirAbs,
      projectCode,
      dryRun,
}) {
      const root = resolve(String(rootDirAbs || ''));
      if (!root.startsWith('/opt/AI-Projects/'))
            return {
                  ok: false,
                  message: `Refuse to delete: not under /opt/AI-Projects (got ${root}).`,
            };
      const expectedTail = `/${projectCode}`;
      if (!root.endsWith(expectedTail))
            return {
                  ok: false,
                  message: `Refuse to delete: root_dir does not end with ${expectedTail} (got ${root}).`,
            };
      const ops = join(root, 'ops');
      const repos = join(root, 'repos');
      const knowledge = join(root, 'knowledge');
      if (!existsSync(ops) || !existsSync(repos) || !existsSync(knowledge))
            return {
                  ok: false,
                  message: `Refuse to delete: expected layout missing under ${root}.`,
            };
      if (dryRun) return { ok: true, deleted: false, dry_run: true };
      await rm(root, { recursive: true, force: true });
      return { ok: true, deleted: true };
}

export async function removeProject(
      projectCode,
      { toolRepoRoot = null, keepFiles = true, dryRun = false } = {},
) {
      const code = normStr(projectCode);
      if (!code) throw new Error('removeProject: project_code is required.');

      const result = await withRegistryLock(
            async () => {
                  const regRes = await loadRegistry({
                        toolRepoRoot,
                        createIfMissing: true,
                  });
                  const reg = regRes.registry;
                  const p = getProject(reg, code);
                  if (!p)
                        return {
                              ok: false,
                              message: `Project not found: ${code}`,
                        };
                  const marked = markProjectRemoved(reg, code);
                  if (!marked.ok) return { ok: false, message: marked.message };
                  if (!dryRun) await writeRegistry(reg, { toolRepoRoot });
                  return { ok: true, project: p, marked: marked.project };
            },
            { toolRepoRoot },
      );
      if (!result.ok) return result;

      const warnings = [];
      if (!keepFiles) {
            const apps = Array.isArray(result.project?.pm2?.apps)
                  ? result.project.pm2.apps
                  : [];
            for (const app of apps) {
                  if (dryRun) continue;
                  const r = pm2StopDelete(app);
                  if (!r.ok && r.message) warnings.push(`pm2: ${r.message}`);
            }
            if (!dryRun) {
                  const c = crontabList();
                  if (c.ok) {
                        const next = removeCronBlockFromText(c.text, code);
                        const w = crontabWrite(next);
                        if (!w.ok) warnings.push(`cron: ${w.message}`);
                  } else {
                        warnings.push(`cron: ${c.message}`);
                  }
            }
            const del = await deleteProjectHomeBestEffort({
                  rootDirAbs: result.project.root_dir,
                  projectCode: code,
                  dryRun,
            });
            if (!del.ok) warnings.push(`delete: ${del.message}`);
      }

      return {
            ok: true,
            project_code: code,
            keep_files: keepFiles,
            dry_run: dryRun,
            warnings,
            marked: result.marked,
      };
}
