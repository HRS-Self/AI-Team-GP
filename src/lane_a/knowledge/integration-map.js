import { existsSync, readFileSync } from "node:fs";
import { mkdir, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { resolveGitRefForBranch, gitShowFileAtRef } from "../../utils/git-files.js";
import { validateRepoIndex } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

function uniqSorted(arr) {
  return Array.from(new Set((Array.isArray(arr) ? arr : []).map((x) => String(x || "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function parseEvidenceRefsJsonl(text) {
  const lines = String(text || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  const map = new Map(); // file_path -> evidence_ids[]
  for (const l of lines) {
    const obj = JSON.parse(l);
    const fp = normStr(obj?.file_path);
    const eid = normStr(obj?.evidence_id);
    if (!fp || !eid) continue;
    if (!map.has(fp)) map.set(fp, []);
    map.get(fp).push(eid);
  }
  for (const [k, ids] of map.entries()) map.set(k, uniqSorted(ids));
  return map;
}

function parseOpenApiEndpointsFromYaml(text) {
  const lines = String(text || "").split("\n");
  const endpoints = [];
  let inPaths = false;
  let currentPath = null;
  for (const raw of lines) {
    const line = String(raw || "");
    if (!inPaths) {
      if (/^paths:\s*$/.test(line.trim())) {
        inPaths = true;
        currentPath = null;
      }
      continue;
    }
    const pathMatch = line.match(/^\s{2,}(\/[^:]+):\s*$/);
    if (pathMatch) {
      currentPath = pathMatch[1].trim();
      continue;
    }
    const methodMatch = currentPath ? line.match(/^\s{4,}(get|post|put|delete|patch|options|head):\s*$/i) : null;
    if (methodMatch) {
      endpoints.push(`${methodMatch[1].toUpperCase()} ${currentPath}`);
      continue;
    }
    // Stop if paths section clearly ended (top-level key)
    if (/^[A-Za-z0-9_]+:\s*$/.test(line) && !/^\s+/.test(line)) break;
  }
  return uniqSorted(endpoints).slice(0, 200);
}

function parseOpenApiEndpointsFromJson(text) {
  try {
    const j = JSON.parse(String(text || ""));
    const paths = isPlainObject(j?.paths) ? j.paths : null;
    if (!paths) return [];
    const endpoints = [];
    for (const [p, methods] of Object.entries(paths)) {
      if (!p || typeof p !== "string") continue;
      const mm = isPlainObject(methods) ? methods : null;
      if (!mm) continue;
      for (const m of Object.keys(mm)) {
        const lower = String(m || "").toLowerCase();
        if (!["get", "post", "put", "delete", "patch", "options", "head"].includes(lower)) continue;
        endpoints.push(`${lower.toUpperCase()} ${p}`);
      }
    }
    return uniqSorted(endpoints).slice(0, 200);
  } catch {
    return [];
  }
}

function inferEdgesFromRepoIndex({ repoId, repoIndex, allRepoIds, evidenceByFilePath }) {
  const edges = [];
  const deps = Array.isArray(repoIndex?.cross_repo_dependencies) ? repoIndex.cross_repo_dependencies : [];

  for (const d of deps) {
    if (!d || typeof d !== "object") continue;
    const type = normStr(d.type);
    const target = normStr(d.target);
    const evPaths = Array.isArray(d.evidence_refs) ? d.evidence_refs.map((x) => normStr(x)).filter(Boolean) : [];
    const evidence_refs = [];
    for (const p of evPaths) {
      const ids = evidenceByFilePath.get(p);
      if (Array.isArray(ids) && ids.length) evidence_refs.push(ids[0]);
    }
    const ev = uniqSorted(evidence_refs);

    const candidates = [];
    for (const other of allRepoIds) {
      if (other === repoId) continue;
      if (target.toLowerCase().includes(other.toLowerCase())) candidates.push(other);
    }
    const toList = candidates.length ? candidates.map((r) => `repo:${r}`) : ["external"];

    const edgeType = type === "http" ? "http" : "sharedlib";
    const confidence = type === "http" ? 0.7 : type === "git" ? 0.6 : 0.55;
    for (const to of toList.slice(0, 5)) {
      edges.push({
        from: `repo:${repoId}`,
        to,
        type: edgeType,
        contract: target,
        confidence,
        evidence_refs: ev,
      });
    }
  }

  edges.sort((a, b) => `${a.from}::${a.to}::${a.type}::${a.contract}`.localeCompare(`${b.from}::${b.to}::${b.type}::${b.contract}`));
  return edges.slice(0, 200);
}

export async function runIntegrationMapBuild({ projectRoot, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const registry = reposRes.registry;
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const activeRepoIds = repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  const byId = new Map(repos.map((r) => [normStr(r?.repo_id), r]));

  const reposOut = [];
  const apiSurfaceOut = {};
  const depsOut = {};
  const edgesOut = [];

  for (const repoId of activeRepoIds) {
    const idxAbs = join(paths.knowledge.evidenceIndexReposAbs, repoId, "repo_index.json");
    if (!existsSync(idxAbs)) return { ok: false, message: `Missing repo_index.json for ${repoId} (run --knowledge-index).` };
    // eslint-disable-next-line no-await-in-loop
    const repoIndex = JSON.parse(readFileSync(idxAbs, "utf8"));
    validateRepoIndex(repoIndex);

    const refsAbs = join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl");
    const evidenceByFilePath = existsSync(refsAbs) ? parseEvidenceRefsJsonl(readFileSync(refsAbs, "utf8")) : new Map();

    reposOut.push({
      repo_id: repoId,
      languages: Array.isArray(repoIndex.languages) ? repoIndex.languages.slice() : [],
      build_commands: repoIndex.build_commands,
      entrypoints: Array.isArray(repoIndex.entrypoints) ? repoIndex.entrypoints.slice() : [],
    });

    const api = repoIndex.api_surface;
    const openapi_files = Array.isArray(api?.openapi_files) ? api.openapi_files.slice() : [];
    const routes_controllers = Array.isArray(api?.routes_controllers) ? api.routes_controllers.slice() : [];
    const events_topics = Array.isArray(api?.events_topics) ? api.events_topics.slice() : [];

    // Optional endpoint extraction from OpenAPI files only (targeted, bounded).
    const endpoints = [];
    const repoCfg = byId.get(repoId) || null;
    const repoAbs = repoCfg ? resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repoCfg.path }) : null;
    const activeBranch = repoCfg && typeof repoCfg.active_branch === "string" && repoCfg.active_branch.trim() ? repoCfg.active_branch.trim() : null;
    const ref = repoAbs && activeBranch ? resolveGitRefForBranch(repoAbs, activeBranch) : null;
    const gitRef = ref || "HEAD";

    if (repoAbs && existsSync(repoAbs)) {
      for (const p of openapi_files.slice(0, 20)) {
        if (!/\.(ya?ml|json)$/i.test(p)) continue;
        const shown = gitShowFileAtRef(repoAbs, gitRef, p);
        if (!shown.ok) continue;
        const content = String(shown.content || "");
        if (content.length > 1_000_000) continue;
        const found = p.toLowerCase().endsWith(".json") ? parseOpenApiEndpointsFromJson(content) : parseOpenApiEndpointsFromYaml(content);
        for (const ep of found) endpoints.push(ep);
      }
    }

    apiSurfaceOut[repoId] = {
      openapi_files: uniqSorted(openapi_files),
      endpoints: uniqSorted(endpoints).slice(0, 500),
      routes_controllers: uniqSorted(routes_controllers),
      events_topics: uniqSorted(events_topics),
    };

    depsOut[repoId] = Array.isArray(repoIndex.cross_repo_dependencies) ? repoIndex.cross_repo_dependencies.slice() : [];

    const inferred = inferEdgesFromRepoIndex({ repoId, repoIndex, allRepoIds: activeRepoIds, evidenceByFilePath });
    for (const e of inferred) edgesOut.push(e);
  }

  reposOut.sort((a, b) => a.repo_id.localeCompare(b.repo_id));
  for (const r of reposOut) {
    r.languages = uniqSorted(r.languages);
    r.entrypoints = uniqSorted(r.entrypoints);
  }
  const edgeMap = new Map();
  for (const e of edgesOut) {
    const k = `${e.from}::${e.to}::${e.type}::${e.contract}`;
    if (!edgeMap.has(k)) edgeMap.set(k, { ...e, evidence_refs: uniqSorted(e.evidence_refs) });
    else {
      const cur = edgeMap.get(k);
      cur.evidence_refs = uniqSorted([...cur.evidence_refs, ...e.evidence_refs]);
    }
  }
  const integration_edges = Array.from(edgeMap.values()).sort((a, b) => `${a.from}::${a.to}::${a.type}::${a.contract}`.localeCompare(`${b.from}::${b.to}::${b.type}::${b.contract}`));

  const integration_map = {
    version: 1,
    generated_at: nowISO(),
    repos: reposOut,
    api_surface: apiSurfaceOut,
    dependencies: depsOut,
    integration_edges,
  };

  const outAbs = join(paths.knowledge.viewsAbs, "integration_map.json");
  if (!dryRun) {
    await mkdir(paths.knowledge.viewsAbs, { recursive: true });
    await writeTextAtomic(outAbs, JSON.stringify(integration_map, null, 2) + "\n");
  }

  return { ok: true, dry_run: dryRun, out: "views/integration_map.json", repos: reposOut.length, edges: integration_edges.length };
}
