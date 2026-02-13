import { mkdir } from "node:fs/promises";
import { join } from "node:path";

import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { runRepoIndex } from "./repo-indexer.js";
import { runIntegrationMapBuild } from "./integration-map.js";
import { runDependencyGraphBuild } from "./dependency-graph-build.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

export async function runKnowledgeIndex({ projectRoot = null, limit = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message, indexed: [], failed: [] };

  const registry = reposRes.registry;
  const activeRepoIds = listActiveRepoIds(registry);
  if (!activeRepoIds.length) return { ok: false, message: "No active repos found in config/REPOS.json.", indexed: [], failed: [] };

  const max = typeof limit === "number" && Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : null;
  const repoIds = max == null ? activeRepoIds : activeRepoIds.slice(0, max);

  const outRoot = paths.knowledge.evidenceIndexReposAbs;
  if (!dryRun) await mkdir(outRoot, { recursive: true });

  const indexed = [];
  const failed = [];

  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const byId = new Map(repos.map((r) => [normStr(r?.repo_id), r]));

  for (const repoId of repoIds) {
    let repoAbs;
    let repoConfig;
    try {
      repoConfig = byId.get(repoId) || null;
      if (!repoConfig) throw new Error(`Unknown repo_id: ${repoId}`);
      repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repoConfig.path });
      if (!repoAbs) throw new Error(`Repo ${repoId} missing path.`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      failed.push({ repo_id: repoId, ok: false, message: msg, error_file: null });
      if (max == null) break;
      continue;
    }

    const outputDir = join(outRoot, repoId);
    // eslint-disable-next-line no-await-in-loop
    const res = await runRepoIndex({
      repo_id: repoId,
      repo_path: repoAbs,
      output_dir: outputDir,
      error_dir_abs: paths.laneA.logsAbs,
      repo_config: repoConfig,
      dry_run: dryRun,
    });
    if (res.ok) indexed.push({ repo_id: repoId, ok: true, paths: res.paths, head_sha: res.head_sha, scanned_at: res.scanned_at });
    else failed.push({ repo_id: repoId, ok: false, message: res.message, error_file: res.error_file });

    if (!res.ok && max == null) break;
  }

  if (!dryRun && failed.length === 0 && max == null) {
    // Derived system view: integration_map.json (deterministic, from repo index outputs).
    const im = await runIntegrationMapBuild({ projectRoot: paths.opsRootAbs, dryRun: false });
    if (!im.ok) return { ok: false, message: im.message, indexed, failed: [{ repo_id: "(system)", ok: false, message: im.message, error_file: null }], dry_run: dryRun, limit: max };

    // Derived system view: dependency graph (deterministic, from repo index outputs + registry).
    const dg = await runDependencyGraphBuild({ projectRoot: paths.opsRootAbs, dryRun: false, toolRepoRoot: process.cwd() });
    if (!dg.ok) return { ok: false, message: dg.message, indexed, failed: [{ repo_id: "(deps)", ok: false, message: dg.message, error_file: null }], dry_run: dryRun, limit: max };
  }

  return { ok: failed.length === 0, projectRoot: paths.opsRootAbs, indexed, failed, dry_run: dryRun, limit: max, evidence_index_root: outRoot };
}
