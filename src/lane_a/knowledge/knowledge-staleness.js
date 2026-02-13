import { mkdir, rename, writeFile } from "node:fs/promises";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve, isAbsolute } from "node:path";

import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry } from "../../utils/repo-registry.js";
import { validateStaleness } from "../../contracts/validators/index.js";
import { computeSystemStaleness, evaluateRepoStaleness } from "../lane-a-staleness-policy.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) return null;
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

let atomicCounter = 0;
async function writeJsonAtomic(absPath, obj) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, JSON.stringify(obj, null, 2) + "\n", "utf8");
  await rename(tmp, abs);
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

export async function runKnowledgeStaleness({ projectRoot, json = false, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const registry = reposRes.registry;
  const activeRepoIds = listActiveRepoIds(registry);

  const registryForStale = isPlainObject(registry) ? { ...registry, base_dir: paths.reposRootAbs } : { base_dir: paths.reposRootAbs, repos: [] };

  const repos = {};
  for (const repoId of activeRepoIds) {
    // eslint-disable-next-line no-await-in-loop
    const s = await evaluateRepoStaleness({ paths, registry: registryForStale, repoId });
    repos[repoId] = { stale: s.stale === true, reasons: Array.isArray(s.stale_reasons) ? s.stale_reasons : [] };
  }

  const sys = computeSystemStaleness({ repoStaleness: Object.entries(repos).map(([repo_id, v]) => ({ ok: true, repo_id, stale: v.stale })), knowledgeRootAbs: paths.knowledge.rootAbs });

  const out = {
    version: 1,
    generated_at: nowISO(),
    repos,
    system: { stale: sys.stale, reasons: sys.reasons },
  };
  validateStaleness(out);

  const outAbs = join(paths.laneA.rootAbs, "staleness.json");
  if (!dryRun) {
    await writeJsonAtomic(outAbs, out);
  }

  return { ok: true, projectRoot: paths.opsRootAbs, staleness_path: outAbs, staleness: out, json_requested: !!json };
}

export function readStalenessOptional({ projectRootAbs }) {
  const abs = join(resolve(String(projectRootAbs || "")), "ai", "lane_a", "staleness.json");
  if (!existsSync(abs)) return null;
  try {
    const text = readFileSync(abs, "utf8");
    const j = JSON.parse(String(text || ""));
    validateStaleness(j);
    return j;
  } catch {
    return null;
  }
}
