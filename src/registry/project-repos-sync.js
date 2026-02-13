import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { isAbsolute, join, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";
import { loadRegistry, withRegistryLock, getProject, upsertProject, writeRegistry } from "./project-registry.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function requireAbsOpsRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

function gitHeadSha(repoAbs) {
  const cwd = normStr(repoAbs);
  if (!cwd || !existsSync(cwd)) return null;
  const res = spawnSync("git", ["-C", cwd, "rev-parse", "HEAD"], { encoding: "utf8", timeout: 10_000 });
  if (res.status !== 0) return null;
  const sha = normStr(res.stdout).split("\n")[0];
  return sha || null;
}

async function loadReposConfig(paths) {
  const abs = join(paths.opsConfigAbs, "REPOS.json");
  if (!existsSync(abs)) return { ok: false, message: `Missing ${abs}` };
  const raw = await readFile(abs, "utf8");
  const j = JSON.parse(String(raw || ""));
  const repos = Array.isArray(j?.repos) ? j.repos : [];
  return { ok: true, repos };
}

function normalizeRepoEntry(paths, repo) {
  const repo_id = normStr(repo?.repo_id);
  if (!repo_id) return null;
  const relPath = normStr(repo?.path) || repo_id;
  const abs_path = resolve(paths.reposRootAbs, relPath);
  const owner_repo = normStr(repo?.owner_repo) || normStr(repo?.ownerRepo) || "(unknown)";
  const active_branch = normStr(repo?.active_branch) || "main";
  const default_branch = normStr(repo?.default_branch) || active_branch || "main";
  const last_seen_head_sha = existsSync(abs_path) ? gitHeadSha(abs_path) : null;
  const active = String(repo?.status || "").trim().toLowerCase() === "active";
  return {
    repo_id,
    owner_repo,
    abs_path,
    default_branch,
    active_branch,
    last_seen_head_sha: last_seen_head_sha || null,
    active,
  };
}

export async function runProjectReposSync({ projectRoot, toolRepoRoot = null, dryRun = false } = {}) {
  const opsRootAbs = requireAbsOpsRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  const cfgCode = normStr(paths.cfg?.project_code);
  if (!cfgCode) return { ok: false, message: "config/PROJECT.json missing project_code." };

  const reposRes = await loadReposConfig(paths);
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const outRepos = reposRes.repos.map((r) => normalizeRepoEntry(paths, r)).filter(Boolean).sort((a, b) => a.repo_id.localeCompare(b.repo_id));

  const res = await withRegistryLock(async () => {
    const regRes = await loadRegistry({ toolRepoRoot, createIfMissing: true });
    const reg = regRes.registry;
    const p = getProject(reg, cfgCode);
    if (!p) return { ok: false, message: `Project not found in registry: ${cfgCode}` };
    const next = { ...p, repos: outRepos, updated_at: new Date().toISOString() };
    upsertProject(reg, next);
    if (!dryRun) await writeRegistry(reg, { toolRepoRoot });
    return { ok: true, project_code: cfgCode, updated_repos: outRepos.length };
  }, { toolRepoRoot });

  return { ...res, dry_run: !!dryRun, ops_root_abs: opsRootAbs };
}
