import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve, basename } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { resolveGitRefForBranch, gitShowFileAtRef } from "../../utils/git-files.js";
import { loadRegistry } from "../../registry/project-registry.js";
import { validateDependencyGraph, validateRepoIndex } from "../../contracts/validators/index.js";

import { dependencyGraphPaths, ensureDependencyOverrideExists } from "./dependency-graph.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function maxIso(a, b) {
  const aa = normStr(a);
  const bb = normStr(b);
  if (!aa) return bb || null;
  if (!bb) return aa || null;
  const ams = Date.parse(aa);
  const bms = Date.parse(bb);
  if (Number.isFinite(ams) && Number.isFinite(bms)) return ams >= bms ? aa : bb;
  return aa >= bb ? aa : bb;
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeToken(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function stableSort(arr, keyFn) {
  return (Array.isArray(arr) ? arr : []).slice().sort((a, b) => String(keyFn(a)).localeCompare(String(keyFn(b))));
}

function uniq(arr) {
  return Array.from(new Set((Array.isArray(arr) ? arr : []).filter(Boolean)));
}

function safeReadJson(content, fallback) {
  try {
    return JSON.parse(String(content || ""));
  } catch {
    return fallback;
  }
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

async function readJsonAbs(pathAbs) {
  const raw = await readFile(resolve(pathAbs), "utf8");
  return JSON.parse(String(raw || ""));
}

function projectRepoIndex(globalRegistry) {
  const projects = Array.isArray(globalRegistry?.projects) ? globalRegistry.projects : [];
  const active = projects.filter((p) => normStr(p?.status) === "active").slice().sort((a, b) => normStr(a.project_code).localeCompare(normStr(b.project_code)));
  const repos = [];
  for (const p of active) {
    const project_code = normStr(p.project_code);
    const knowledge_abs_path = normStr(p?.knowledge?.abs_path);
    const knowledge_git_remote = normStr(p?.knowledge?.git_remote);
    const knowledge_active_branch = normStr(p?.knowledge?.active_branch) || normStr(p?.knowledge?.default_branch) || "main";
    for (const r of Array.isArray(p?.repos) ? p.repos : []) {
      if (r?.active !== true) continue;
      repos.push({
        project_code,
        repo_id: normStr(r.repo_id),
        owner_repo: normStr(r.owner_repo),
        abs_path: normStr(r.abs_path),
        active_branch: normStr(r.active_branch) || normStr(r.default_branch) || "main",
        knowledge_abs_path,
        knowledge_git_remote,
        knowledge_active_branch,
      });
    }
  }
  // Deterministic: sort by repo_id then project_code.
  repos.sort((a, b) => `${a.repo_id}::${a.project_code}`.localeCompare(`${b.repo_id}::${b.project_code}`));
  return repos;
}

function matchTargetRepo({ token, candidates }) {
  const t = normalizeToken(token);
  if (!t || t.length < 3) return [];
  const hits = [];
  for (const c of candidates) {
    const rid = normalizeToken(c.repo_id);
    if (!rid) continue;
    const slug = normalizeToken(basename(c.owner_repo || c.abs_path || ""));
    if (t === rid || t === slug || (rid.length >= 4 && t.includes(rid)) || (slug.length >= 4 && t.includes(slug))) hits.push(c);
  }
  return hits;
}

function pushEdge(edgesByKey, { from_repo_id, to_repo_id, reason, evidence }) {
  const from = normStr(from_repo_id);
  const to = normStr(to_repo_id);
  if (!from || !to || from === to) return;
  const k = `${from}::${to}`;
  const cur = edgesByKey.get(k) || { from_repo_id: from, to_repo_id: to, reason: normStr(reason) || "detected dependency", evidence: [] };
  const nextEvidence = uniq([...(cur.evidence || []), ...(Array.isArray(evidence) ? evidence : [])])
    .filter((e) => isPlainObject(e) && normStr(e.type) && normStr(e.path) && normStr(e.note))
    .map((e) => ({ type: "file", path: normStr(e.path), note: normStr(e.note) }))
    .sort((a, b) => `${a.path}::${a.type}`.localeCompare(`${b.path}::${b.type}`));
  edgesByKey.set(k, { ...cur, evidence: nextEvidence });
}

function listDependencyCandidatesFromPackageJson(pkgJson) {
  const pkg = isPlainObject(pkgJson) ? pkgJson : null;
  if (!pkg) return [];
  const out = [];
  const add = (obj, label) => {
    if (!isPlainObject(obj)) return;
    const keys = Object.keys(obj).sort((a, b) => a.localeCompare(b));
    for (const k of keys) {
      out.push({ token: k, reason: `package.json ${label}: ${k}`, evidence: [{ type: "file", path: "package.json", note: `${label}: ${k}` }] });
      const v = normStr(obj[k]);
      if (v) out.push({ token: v, reason: `package.json ${label} value for ${k}`, evidence: [{ type: "file", path: "package.json", note: `${label} value for ${k}` }] });
    }
  };
  add(pkg.dependencies, "dependency");
  add(pkg.devDependencies, "devDependency");
  add(pkg.peerDependencies, "peerDependency");
  add(pkg.optionalDependencies, "optionalDependency");
  return out;
}

function listDependencyCandidatesFromTsconfig(tsJson, relPath) {
  const j = isPlainObject(tsJson) ? tsJson : null;
  if (!j) return [];
  const paths = j?.compilerOptions?.paths;
  if (!isPlainObject(paths)) return [];
  const out = [];
  for (const [k, v] of Object.entries(paths)) {
    out.push({ token: k, reason: `tsconfig paths key: ${k}`, evidence: [{ type: "file", path: relPath, note: `paths key: ${k}` }] });
    if (Array.isArray(v)) {
      for (const it of v) out.push({ token: String(it || ""), reason: `tsconfig paths value for ${k}`, evidence: [{ type: "file", path: relPath, note: `paths value for ${k}` }] });
    }
  }
  return out;
}

function listDependencyCandidatesFromReadme(text, knownRepoIds) {
  const t = String(text || "");
  if (!t) return [];
  const out = [];
  for (const id of knownRepoIds) {
    if (!id) continue;
    if (t.toLowerCase().includes(id.toLowerCase())) {
      out.push({ token: id, reason: `README mentions ${id}`, evidence: [{ type: "file", path: "README.md", note: `mentions ${id}` }] });
    }
  }
  return out;
}

function asExternalProjects({ edges, repoToProject, currentProjectCode }) {
  const byProject = new Map(); // project_code -> { project_code, knowledge_repo_dir, repos:Set, fromRepos:Set }
  for (const e of edges) {
    const toProj = repoToProject.get(normStr(e.to_repo_id)) || null;
    if (!toProj || !toProj.project_code) continue;
    if (normStr(toProj.project_code) === normStr(currentProjectCode)) continue;
    const pCode = toProj.project_code;
    const cur =
      byProject.get(pCode) || {
        project_code: pCode,
        knowledge_repo_dir: toProj.knowledge_abs_path,
        repos: new Set(),
        from_repos: new Set(),
      };
    cur.repos.add(normStr(e.to_repo_id));
    cur.from_repos.add(normStr(e.from_repo_id));
    byProject.set(pCode, cur);
  }
  const out = [];
  for (const cur of byProject.values()) {
    out.push({
      project_code: cur.project_code,
      knowledge_repo_dir: cur.knowledge_repo_dir,
      repos: Array.from(cur.repos).sort((a, b) => a.localeCompare(b)),
      reason: `Detected external dependencies referenced by: ${Array.from(cur.from_repos).sort((a, b) => a.localeCompare(b)).join(", ") || "(unknown)"}`,
    });
  }
  out.sort((a, b) => a.project_code.localeCompare(b.project_code));
  return out;
}

export async function runDependencyGraphBuild({ projectRoot, dryRun = false, toolRepoRoot = null } = {}) {
  const paths = await loadProjectPaths({ projectRoot });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const registry = reposRes.registry;
  const project_code = normStr(paths.cfg?.project_code) || "(unknown)";

  const toolRoot = toolRepoRoot || process.cwd();
  const globalRegRes = await loadRegistry({ toolRepoRoot: toolRoot, createIfMissing: true });
  const globalReg = globalRegRes.registry;
  const knownExternalRepos = projectRepoIndex(globalReg);

  const repoToProject = new Map();
  for (const r of knownExternalRepos) {
    // If repo_id is duplicated across projects, choose first by deterministic sort order but preserve reason in depends_on.
    if (!repoToProject.has(r.repo_id)) repoToProject.set(r.repo_id, r);
  }

  const activeRepos = (Array.isArray(registry?.repos) ? registry.repos : [])
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .slice()
    .sort((a, b) => normStr(a.repo_id).localeCompare(normStr(b.repo_id)));

  const nodes = activeRepos.map((r) => ({ repo_id: normStr(r.repo_id), team_id: normStr(r.team_id) || "unknown", type: "repo" }));

  const edgesByKey = new Map();
  let generatedAt = null;

  const evidenceIndexDir = paths.knowledge.evidenceIndexReposAbs;
  for (const r of activeRepos) {
    const fromRepoId = normStr(r.repo_id);
    if (!fromRepoId) continue;
    const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: r.path });
    if (!repoAbs || !existsSync(repoAbs)) continue;

    const activeBranch = normStr(r.active_branch) || null;
    const ref = activeBranch ? resolveGitRefForBranch(repoAbs, activeBranch) : null;
    const gitRef = ref || "HEAD";

    // Read existing repo_index (for cross_repo_dependencies evidence pointers).
    const idxAbs = join(evidenceIndexDir, fromRepoId, "repo_index.json");
    let repoIndex = null;
    if (existsSync(idxAbs)) {
      // eslint-disable-next-line no-await-in-loop
      repoIndex = await readJsonAbs(idxAbs);
      try {
        validateRepoIndex(repoIndex);
        generatedAt = maxIso(generatedAt, repoIndex.scanned_at);
      } catch {
        repoIndex = null;
      }
    }

    const candidates = [];

    if (repoIndex && Array.isArray(repoIndex.cross_repo_dependencies)) {
      for (const d of repoIndex.cross_repo_dependencies) {
        if (!isPlainObject(d)) continue;
        const target = normStr(d.target);
        const refs = Array.isArray(d.evidence_refs) ? d.evidence_refs : [];
        const evidence = refs.map((p) => ({ type: "file", path: normStr(p), note: `cross_repo_dependency: ${target}` }));
        candidates.push({ token: target, reason: `cross_repo_dependency: ${target}`, evidence });
      }
    }

    const pkgShown = gitShowFileAtRef(repoAbs, gitRef, "package.json");
    if (pkgShown.ok) {
      const pkgJson = safeReadJson(pkgShown.content, null);
      candidates.push(...listDependencyCandidatesFromPackageJson(pkgJson));
    }

    for (const cfgPath of ["tsconfig.json", "jsconfig.json"]) {
      const shown = gitShowFileAtRef(repoAbs, gitRef, cfgPath);
      if (!shown.ok) continue;
      const j = safeReadJson(shown.content, null);
      candidates.push(...listDependencyCandidatesFromTsconfig(j, cfgPath));
    }

    const readmeShown = gitShowFileAtRef(repoAbs, gitRef, "README.md");
    if (readmeShown.ok) {
      const knownRepoIds = uniq(knownExternalRepos.map((x) => x.repo_id)).sort((a, b) => a.localeCompare(b));
      candidates.push(...listDependencyCandidatesFromReadme(readmeShown.content, knownRepoIds));
    }

    // Match candidates to known repos from registry (external and internal).
    for (const c of candidates) {
      const hits = matchTargetRepo({ token: c.token, candidates: knownExternalRepos });
      for (const hit of hits) {
        pushEdge(edgesByKey, { from_repo_id: fromRepoId, to_repo_id: hit.repo_id, reason: normStr(c.reason) || "detected dependency", evidence: c.evidence });
      }
    }
  }

  const edgesAll = Array.from(edgesByKey.values())
    .map((e) => ({
      from_repo_id: normStr(e.from_repo_id),
      to_repo_id: normStr(e.to_repo_id),
      reason: normStr(e.reason) || "detected dependency",
      evidence: stableSort(e.evidence, (x) => `${normStr(x.path)}::${normStr(x.type)}`).map((x) => ({ type: "file", path: normStr(x.path), note: normStr(x.note) })),
    }))
    .filter((e) => e.from_repo_id && e.to_repo_id && e.from_repo_id !== e.to_repo_id)
    .sort((a, b) => `${a.from_repo_id}::${a.to_repo_id}`.localeCompare(`${b.from_repo_id}::${b.to_repo_id}`));

  const external_projects = asExternalProjects({ edges: edgesAll, repoToProject, currentProjectCode: project_code });

  const graph = {
    version: 1,
    generated_at: generatedAt || nowISO(),
    project: { code: project_code },
    nodes: nodes.slice().sort((a, b) => a.repo_id.localeCompare(b.repo_id)),
    edges: edgesAll,
    external_projects,
  };
  validateDependencyGraph(graph);

  const dp = dependencyGraphPaths(paths);
  const approvedWhenEmpty = graph.edges.length === 0 && graph.external_projects.length === 0;
  const overrideRes = await ensureDependencyOverrideExists({ paths, approvedWhenEmpty, dryRun: !!dryRun });

  if (!dryRun) {
    await mkdir(dirname(dp.graphAbs), { recursive: true });
    await writeTextAtomic(dp.graphAbs, JSON.stringify(graph, null, 2) + "\n");
  }

  // Update per-repo repo_index dependencies section to include external (cross-project) depends_on details.
  for (const r of activeRepos) {
    const repoId = normStr(r.repo_id);
    if (!repoId) continue;
    const idxAbs = join(evidenceIndexDir, repoId, "repo_index.json");
    if (!existsSync(idxAbs)) continue;
    // eslint-disable-next-line no-await-in-loop
    const idx = await readJsonAbs(idxAbs);
    // eslint-disable-next-line no-continue
    if (!isPlainObject(idx)) continue;

    const depsFrom = edgesAll.filter((e) => e.from_repo_id === repoId);
    const depends_on = [];
    for (const e of depsFrom) {
      const target = repoToProject.get(e.to_repo_id) || null;
      if (!target) continue;
      // Only include cross-project for reuse.
      const isSelf = normStr(target.project_code) === normStr(project_code);
      if (isSelf) continue;
      depends_on.push({
        kind: "project_repo",
        repo_id: target.repo_id,
        owner_repo: target.owner_repo || target.repo_id,
        project_code: target.project_code,
        abs_path: target.abs_path,
        active_branch: target.active_branch,
        knowledge_abs_path: target.knowledge_abs_path,
        knowledge_git_remote: target.knowledge_git_remote || "",
        knowledge_active_branch: target.knowledge_active_branch,
        reason: e.reason,
        evidence: stableSort(e.evidence, (x) => `${normStr(x.path)}::${normStr(x.type)}`).map((x) => ({ type: "file", path: normStr(x.path), note: normStr(x.note) })),
      });
    }

    depends_on.sort((a, b) => `${a.repo_id}::${a.project_code}`.localeCompare(`${b.repo_id}::${b.project_code}`));

    const next = {
      ...idx,
      dependencies: {
        version: 1,
        detected_at: normStr(idx.scanned_at) || nowISO(),
        mode: "detected",
        depends_on,
      },
    };
    validateRepoIndex(next);
    if (!dryRun) await writeTextAtomic(idxAbs, JSON.stringify(next, null, 2) + "\n");
  }

  return {
    ok: true,
    projectRoot: paths.opsRootAbs,
    graph_path: dp.graphAbs,
    override_path: dp.overrideAbs,
    override_status: overrideRes.status || null,
    dry_run: !!dryRun,
    edges: graph.edges.length,
    external_projects: graph.external_projects.length,
  };
}
