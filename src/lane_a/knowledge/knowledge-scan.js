import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { sha256Hex } from "../../utils/fs-hash.js";
import { formatFsSafeUtcTimestamp } from "../../utils/naming.js";
import { resolveGitRefForBranch, gitShowFileAtRef } from "../../utils/git-files.js";
import { ContractValidationError, validateKnowledgeScan, validateRepoIndex } from "../../contracts/validators/index.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { collectEvidenceFilePaths, generateEvidenceRefs, mapFactsToEvidence } from "./evidence-builder.js";
import { loadEffectiveDependencyGraph } from "./dependency-graph.js";
import { refreshPhasePrereqs } from "../phase-state.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function stableSort(arr, keyFn) {
  return (Array.isArray(arr) ? arr : []).slice().sort((a, b) => String(keyFn(a)).localeCompare(String(keyFn(b))));
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
  const t = await readFile(resolve(pathAbs), "utf8");
  return JSON.parse(String(t || ""));
}

async function writeJsonAtomic(absPath, obj) {
  await writeTextAtomic(absPath, JSON.stringify(obj, null, 2) + "\n");
}

function git(repoAbs, args, { timeoutMs = 30_000 } = {}) {
  const res = spawnSync("git", ["-C", repoAbs, ...args], { encoding: "utf8", timeout: timeoutMs });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function stableScanVersionInt({ repoId, repoIndexVersion, evidenceIds }) {
  const base = [repoId, String(repoIndexVersion || 1), ...(Array.isArray(evidenceIds) ? evidenceIds.slice().sort((a, b) => a.localeCompare(b)) : [])].join("\n");
  const hex8 = sha256Hex(base).slice(0, 8);
  const n = Number.parseInt(hex8, 16);
  return Math.max(1, Number.isFinite(n) ? n : 1);
}

async function ensureDepsApprovedOrBlock({ paths, forceWithoutApproval, dryRun }) {
  const loaded = await loadEffectiveDependencyGraph({ paths });
  if (forceWithoutApproval) return { ok: true, forced: true, deps: loaded.ok ? loaded : null };

  const status = loaded.ok ? String(loaded.override_status || "").trim().toLowerCase() : "pending";
  const approved = status === "approved";
  if (approved) return { ok: true, forced: false, deps: loaded };

  const blockerAbs = join(paths.laneA.blockersAbs, "DEPS_NOT_APPROVED.json");
  const payload = {
    version: 1,
    reason: "deps_not_approved",
    suggested_action: "Run --knowledge-index, review dependency_graph.override.json, then run --knowledge-deps-approve.",
    paths: loaded.ok ? [loaded.graph_path, loaded.override_path] : [join(paths.knowledge.ssotSystemAbs, "dependency_graph.json"), join(paths.knowledge.ssotSystemAbs, "dependency_graph.override.json")],
  };
  if (!dryRun) await writeJsonAtomic(blockerAbs, payload);
  return { ok: false, message: "Dependencies not approved. Refusing to scan until dependency graph is human-approved.", blocker_path: blockerAbs, details: payload };
}

function stableExternalBundleId({ parts }) {
  const base = (Array.isArray(parts) ? parts : []).join("\n");
  return `sha256-${sha256Hex(base)}`;
}

async function loadExternalKnowledgeBundleSummary(dep) {
  const project_code = normStr(dep?.project_code);
  const repo_id = normStr(dep?.repo_id);
  const knowledge_abs_path = normStr(dep?.knowledge_abs_path);
  if (!project_code || !repo_id || !knowledge_abs_path) throw new Error("external_dependency_bundle_missing: invalid dependency entry (missing project_code/repo_id/knowledge_abs_path).");

  const scanAbs = join(knowledge_abs_path, "ssot", "repos", repo_id, "scan.json");
  const refsAbs = join(knowledge_abs_path, "evidence", "repos", repo_id, "evidence_refs.jsonl");
  const idxAbs = join(knowledge_abs_path, "evidence", "index", "repos", repo_id, "repo_index.json");
  const fpAbs = join(knowledge_abs_path, "evidence", "index", "repos", repo_id, "repo_fingerprints.json");

  const missing = [scanAbs, refsAbs, idxAbs, fpAbs].filter((p) => !existsSync(p));
  if (missing.length) {
    throw new Error(
      `external_dependency_bundle_missing: project=${project_code} repo=${repo_id} missing=${missing.map((p) => p.replace(knowledge_abs_path, "<K_ROOT>")).join(", ")}\n` +
        `Suggestion: run Lane A in project ${project_code}: --knowledge-index, --knowledge-scan, and optionally --knowledge-bundle --scope repo:${repo_id}.`,
    );
  }

  const scanText = await readFile(scanAbs, "utf8");
  const refsText = await readFile(refsAbs, "utf8");
  const idxText = await readFile(idxAbs, "utf8");
  const fpText = await readFile(fpAbs, "utf8");
  const idxJson = JSON.parse(String(idxText || ""));
  const loaded_at = typeof idxJson?.scanned_at === "string" ? idxJson.scanned_at : "";
  const loadedAtMs = Date.parse(loaded_at);
  if (!loaded_at || !Number.isFinite(loadedAtMs)) {
    throw new Error(`external_dependency_bundle_missing: project=${project_code} repo=${repo_id} repo_index.scanned_at missing/invalid (expected ISO date-time).`);
  }
  const bundle_id = stableExternalBundleId({ parts: [sha256Hex(scanText), sha256Hex(refsText), sha256Hex(idxText), sha256Hex(fpText)] });

  return { project_code, repo_id, bundle_id, path: scanAbs, loaded_at };
}

function factId(prefix, parts) {
  const base = [prefix, ...(Array.isArray(parts) ? parts : [])].join("\n");
  return `${prefix}_${sha256Hex(base).slice(0, 10)}`;
}

function buildFactsFromIndex({ repoId, repoIndex, evidenceByPath }) {
  const factsById = new Map();

  const addFact = (claim, filePath) => {
    const p = normStr(filePath);
    const evid = evidenceByPath.get(p) || null;
    if (!evid) return;
    const id = factId("F", [repoId, claim, p]);
    if (!factsById.has(id)) factsById.set(id, { fact_id: id, claim, evidence_ids: [evid.evidence_id] });
  };

  const entrypoints = Array.isArray(repoIndex?.entrypoints) ? repoIndex.entrypoints : [];
  for (const p of stableSort(entrypoints, (x) => String(x || ""))) addFact(`Entrypoint: ${String(p)}`, String(p));

  const api = isPlainObject(repoIndex?.api_surface) ? repoIndex.api_surface : {};
  const openapi = Array.isArray(api.openapi_files) ? api.openapi_files : [];
  const routes = Array.isArray(api.routes_controllers) ? api.routes_controllers : [];
  const events = Array.isArray(api.events_topics) ? api.events_topics : [];
  for (const p of stableSort(openapi, (x) => String(x || ""))) addFact(`API contract file: ${String(p)}`, String(p));
  for (const p of stableSort(routes.slice(0, 50), (x) => String(x || ""))) addFact(`Route/controller: ${String(p)}`, String(p));
  for (const p of stableSort(events.slice(0, 50), (x) => String(x || ""))) addFact(`Event/messaging artifact: ${String(p)}`, String(p));

  const migrations = Array.isArray(repoIndex?.migrations_schema) ? repoIndex.migrations_schema : [];
  for (const p of stableSort(migrations.slice(0, 50), (x) => String(x || ""))) addFact(`Migration/schema: ${String(p)}`, String(p));

  const build = isPlainObject(repoIndex?.build_commands) ? repoIndex.build_commands : null;
  if (build) {
    const evidenceFiles = Array.isArray(build.evidence_files) ? build.evidence_files : [];
    const buildEvidPath = evidenceFiles.find((p) => evidenceByPath.has(String(p))) || (evidenceByPath.has("package.json") ? "package.json" : null);
    const addBuild = (kind, cmds) => {
      if (!buildEvidPath) return;
      for (const c of stableSort(cmds, (x) => String(x || ""))) addFact(`Build command (${kind}): ${String(c)}`, buildEvidPath);
    };
    addBuild("install", Array.isArray(build.install) ? build.install : []);
    addBuild("lint", Array.isArray(build.lint) ? build.lint : []);
    addBuild("build", Array.isArray(build.build) ? build.build : []);
    addBuild("test", Array.isArray(build.test) ? build.test : []);
  }

  const cross = Array.isArray(repoIndex?.cross_repo_dependencies) ? repoIndex.cross_repo_dependencies : [];
  for (const d of cross) {
    if (!isPlainObject(d)) continue;
    const type = normStr(d.type);
    const target = normStr(d.target);
    const refs = Array.isArray(d.evidence_refs) ? d.evidence_refs : [];
    const evid = refs.find((p) => evidenceByPath.has(String(p))) || null;
    if (type && target && evid) addFact(`Cross-repo dependency (${type}): ${target}`, evid);
  }

  const hotspots = Array.isArray(repoIndex?.hotspots) ? repoIndex.hotspots : [];
  for (const h of hotspots) {
    if (!isPlainObject(h)) continue;
    const p = normStr(h.file_path);
    const reason = normStr(h.reason);
    if (p && evidenceByPath.has(p)) addFact(`Hotspot (${reason}): ${p}`, p);
  }

  const fp = isPlainObject(repoIndex?.fingerprints) ? repoIndex.fingerprints : {};
  for (const p of Object.keys(fp).sort((a, b) => a.localeCompare(b))) {
    if (!evidenceByPath.has(p)) continue;
    const lower = p.toLowerCase();
    if (lower === "package.json") addFact("Dependency manifest: package.json", p);
    if (lower.endsWith("yarn.lock") || lower.endsWith("package-lock.json") || lower.endsWith("pnpm-lock.yaml")) addFact(`Dependency lockfile: ${p}`, p);
    if (/(^|\/)(openapi|swagger)\.(ya?ml|json)$/i.test(p)) addFact(`API contract file: ${p}`, p);
    if (lower.endsWith(".graphql") || lower.endsWith(".gql")) addFact(`API contract file: ${p}`, p);
    if (lower.endsWith(".proto")) addFact(`API contract file: ${p}`, p);
    if (lower === "dockerfile" || lower.startsWith(".github/workflows/") || lower.startsWith("helm/") || lower.startsWith("k8s/") || lower.startsWith("kubernetes/")) {
      addFact(`Infra file: ${p}`, p);
    }
    if (lower.startsWith("migrations/") || lower.startsWith("db/migrations/") || lower.startsWith("prisma/migrations/")) addFact(`Migration file: ${p}`, p);
    if (lower.startsWith("src/main/resources/db/migration/") || lower.startsWith("src/main/resources/db/migrations/")) addFact(`Migration file: ${p}`, p);
    if (lower.startsWith("src/main/resources/db/changelog/") || lower.startsWith("liquibase/") || lower.startsWith("flyway/")) addFact(`Migration file: ${p}`, p);
  }

  return Array.from(factsById.values()).sort((a, b) => a.fact_id.localeCompare(b.fact_id));
}

function renderScanReportMd({ scan, evidenceRefs }) {
  const lines = [];
  lines.push(`# SCAN_REPORT: ${scan.repo_id}`);
  lines.push("");
  lines.push(`scanned_at: ${scan.scanned_at}`);
  lines.push(`scan_version: ${scan.scan_version}`);
  lines.push("");
  lines.push("## Coverage");
  lines.push("");
  lines.push(`- files_seen: ${scan.coverage.files_seen}`);
  lines.push(`- files_indexed: ${scan.coverage.files_indexed}`);
  lines.push("");
  lines.push("## Facts");
  lines.push("");
  if (!scan.facts.length) lines.push("- (none)");
  for (const f of scan.facts) lines.push(`- ${f.fact_id}: ${f.claim} [${f.evidence_ids.join(", ")}]`);
  lines.push("");
  lines.push("## Unknowns");
  lines.push("");
  if (!scan.unknowns.length) lines.push("- (none)");
  for (const u of scan.unknowns) lines.push(`- ${u}`);
  lines.push("");
  lines.push("## Contradictions");
  lines.push("");
  if (!scan.contradictions.length) lines.push("- (none)");
  for (const c of scan.contradictions) lines.push(`- ${c.a_fact_id} vs ${c.b_fact_id}: ${c.reason}`);
  lines.push("");
  lines.push("## Evidence Refs");
  lines.push("");
  lines.push(`count: ${Array.isArray(evidenceRefs) ? evidenceRefs.length : 0}`);
  lines.push("");
  return lines.join("\n") + "\n";
}

async function writeScanError({ laneALogsAbs, repoId, kind, message, errors = null }) {
  const file = `knowledge-scan__${repoId}.error.json`;
  const p = join(resolve(String(laneALogsAbs || "")), file);
  const obj = {
    ok: false,
    repo_id: repoId,
    captured_at: new Date().toISOString(),
    kind,
    message: String(message || "").trim() || "(unknown error)",
    errors: Array.isArray(errors) ? errors.map((e) => String(e)) : null,
  };
  await writeTextAtomic(p, JSON.stringify(obj, null, 2) + "\n");
  return p;
}

async function scanOneRepo({ paths, registry, repoId, dryRun }) {
  const repoIndexDirAbs = join(paths.knowledge.evidenceIndexReposAbs, repoId);
  const repoIndexAbs = join(repoIndexDirAbs, "repo_index.json");
  const repoFingerprintsAbs = join(repoIndexDirAbs, "repo_fingerprints.json");
  if (!existsSync(repoIndexAbs)) throw new Error(`Missing repo index. Run --knowledge-index first: ${repoIndexAbs}`);
  if (!existsSync(repoFingerprintsAbs)) throw new Error(`Missing repo fingerprints. Run --knowledge-index first: ${repoFingerprintsAbs}`);

  const repoIndex = await readJsonAbs(repoIndexAbs);
  validateRepoIndex(repoIndex);

  const repoFingerprints = await readJsonAbs(repoFingerprintsAbs);
  if (!isPlainObject(repoFingerprints) || normStr(repoFingerprints.repo_id) !== repoId) throw new Error("Invalid repo_fingerprints.json: repo_id mismatch.");
  if (!Array.isArray(repoFingerprints.files)) throw new Error("Invalid repo_fingerprints.json: files must be an array.");

  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const repoConfig = repos.find((r) => normStr(r?.repo_id) === repoId) || null;
  if (!repoConfig) throw new Error(`Unknown repo_id: ${repoId}`);
  const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repoConfig.path });
  if (!repoAbs) throw new Error(`Repo ${repoId} missing path.`);
  if (!existsSync(repoAbs)) throw new Error(`Repo path missing: ${repoAbs}`);

  // Determine git ref to scan (prefer active_branch).
  const activeBranch = typeof repoConfig.active_branch === "string" && repoConfig.active_branch.trim() ? repoConfig.active_branch.trim() : null;
  const ref = activeBranch ? resolveGitRefForBranch(repoAbs, activeBranch) : null;
  const gitRef = ref || "HEAD";
  if (activeBranch && !ref) throw new Error(`active_branch not found locally: ${activeBranch}`);

  const headShaRes = git(repoAbs, ["rev-list", "-1", gitRef]);
  if (!headShaRes.ok) throw new Error(`git rev-list failed for ${gitRef}: ${headShaRes.stderr || headShaRes.stdout}`);
  const commitSha = headShaRes.stdout.trim();

  // Enforce index/fingerprint freshness: fingerprinted files at current ref must match stored sha256.
  for (const f of repoFingerprints.files.slice().sort((a, b) => String(a.path).localeCompare(String(b.path)))) {
    if (!isPlainObject(f)) continue;
    const p = normStr(f.path);
    const expected = normStr(f.sha256);
    if (!p || !expected) continue;
    const shown = gitShowFileAtRef(repoAbs, gitRef, p);
    if (!shown.ok) throw new Error(`Fingerprint file missing at ${gitRef}:${p} (${shown.error})`);
    const actual = sha256Hex(shown.content || "");
    if (actual !== expected) throw new Error(`Repo index out of date: fingerprint mismatch for ${p}. Run --knowledge-index again.`);
  }

  const scannedAtIso = normStr(repoIndex.scanned_at);
  const scannedAt = formatFsSafeUtcTimestamp(scannedAtIso);

  const evidenceFilePaths = collectEvidenceFilePaths({ repoIndex, repoFingerprints });
  const evidence = generateEvidenceRefs({
    repo_id: repoId,
    repo_abs: repoAbs,
    git_ref: gitRef,
    commit_sha: commitSha,
    captured_at_iso: scannedAtIso,
    file_paths: evidenceFilePaths,
    extractor: "knowledge_scan_index",
  });

  const evidenceByPath = new Map(evidence.refs.map((r) => [r.file_path, r]));
  const rawFacts = buildFactsFromIndex({ repoId, repoIndex, evidenceByPath });
  const facts = mapFactsToEvidence({ facts: rawFacts, evidenceRefs: evidence.refs });

  const unknowns = [];
  const hasContract = evidenceFilePaths.some(
    (p) => /(^|\/)(openapi|swagger)\.(ya?ml|json)$/i.test(p) || p.toLowerCase().endsWith(".graphql") || p.toLowerCase().endsWith(".proto"),
  );
  if (!hasContract && facts.length) {
    const any = facts[0];
    unknowns.push(`No API contract file detected in indexed evidence bundle. Evidence: ${any.evidence_ids[0]}`);
  }

  const scan = {
    repo_id: repoId,
    scanned_at: scannedAt,
    scan_version: stableScanVersionInt({ repoId, repoIndexVersion: repoIndex.version, evidenceIds: evidence.refs.map((r) => r.evidence_id) }),
    external_knowledge: [],
    facts,
    unknowns: unknowns.sort((a, b) => a.localeCompare(b)),
    contradictions: [],
    coverage: { files_seen: evidenceFilePaths.length, files_indexed: evidence.refs.length },
  };

  // External knowledge bundles (cross-project) referenced by dependency graph.
  const dependsOn = Array.isArray(repoIndex?.dependencies?.depends_on) ? repoIndex.dependencies.depends_on : [];
  if (dependsOn.length) {
    const loaded = [];
    for (const d of dependsOn) {
      // eslint-disable-next-line no-await-in-loop
      loaded.push(await loadExternalKnowledgeBundleSummary(d));
    }
    scan.external_knowledge = loaded
      .slice()
      .sort((a, b) => `${a.project_code}::${a.repo_id}`.localeCompare(`${b.project_code}::${b.repo_id}`));
  }

  validateKnowledgeScan(scan);

  const outScanAbs = join(paths.knowledge.ssotReposAbs, repoId, "scan.json");
  const outRefsAbs = join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl");
  const outReportAbs = join(paths.knowledge.viewsReposAbs, repoId, "SCAN_REPORT.md");

  const written = { scan_json: outScanAbs, evidence_refs_jsonl: outRefsAbs, report_md: outReportAbs };
  if (!dryRun) {
    await mkdir(dirname(outScanAbs), { recursive: true });
    await mkdir(dirname(outRefsAbs), { recursive: true });
    await mkdir(dirname(outReportAbs), { recursive: true });
    await writeTextAtomic(outRefsAbs, evidence.jsonl);
    await writeTextAtomic(outScanAbs, JSON.stringify(scan, null, 2) + "\n");
    await writeTextAtomic(outReportAbs, renderScanReportMd({ scan, evidenceRefs: evidence.refs }));
  }

  return { ok: true, repo_id: repoId, paths: written, scanned_at: scannedAt, scan_version: scan.scan_version, facts_count: scan.facts.length, evidence_count: evidence.refs.length };
}

function clampConcurrency(n) {
  const x = Number.parseInt(String(n ?? ""), 10);
  if (!Number.isFinite(x)) return 4;
  return Math.max(1, Math.min(16, x));
}

export async function runKnowledgeScan({ projectRoot = null, repoId = null, limit = null, concurrency = 4, dryRun = false, forceWithoutDepsApproval = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message, scanned: [], failed: [] };
  const registry = reposRes.registry;

  const activeRepoIds = listActiveRepoIds(registry);
  if (!activeRepoIds.length) return { ok: false, message: "No active repos found in config/REPOS.json.", scanned: [], failed: [] };

  const depsGate = await ensureDepsApprovedOrBlock({ paths, forceWithoutApproval: !!forceWithoutDepsApproval, dryRun: !!dryRun });
  if (!depsGate.ok) {
    return { ok: false, message: depsGate.message, blocker: depsGate.blocker_path, scanned: [], failed: [], dry_run: !!dryRun, deps_gate: depsGate.details || null };
  }

  const repoIds = repoId ? [String(repoId).trim()] : activeRepoIds;
  const max = typeof limit === "number" && Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : null;
  const todo = max == null ? repoIds : repoIds.slice(0, max);

  const scanned = [];
  const failed = [];

  const pool = clampConcurrency(concurrency);
  let cursor = 0;

  async function worker() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const i = cursor;
      cursor += 1;
      if (i >= todo.length) return;
      const id = todo[i];
      try {
        // eslint-disable-next-line no-await-in-loop
        const res = await scanOneRepo({ paths, registry, repoId: id, dryRun });
        scanned.push(res);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (!dryRun) {
          const kind = err instanceof ContractValidationError ? "failure" : "error";
          // eslint-disable-next-line no-await-in-loop
          await writeScanError({ laneALogsAbs: paths.laneA.logsAbs, repoId: id, kind, message: msg });
        }
        failed.push({ ok: false, repo_id: id, message: msg });
      }
    }
  }

  const workers = Array.from({ length: Math.min(pool, todo.length) }, () => worker());
  await Promise.all(workers);

  scanned.sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));
  failed.sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

  const warnings = [];
  try {
    // Keep phase prereqs in sync for UI gating/visibility.
    await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: !!dryRun });
  } catch (err) {
    warnings.push(err instanceof Error ? err.message : String(err));
  }

  return { ok: failed.length === 0, projectRoot: paths.opsRootAbs, scanned, failed, dry_run: dryRun, limit: max, concurrency: pool, warnings: warnings.length ? warnings : null };
}
