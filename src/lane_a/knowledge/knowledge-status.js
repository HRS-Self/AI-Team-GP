import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { validateEvidenceRef, validateKnowledgeScan, validateRepoIndex } from "../../contracts/validators/index.js";
import { validateKnowledgeGapsFile } from "../../validators/knowledge-gap-validator.js";
import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { evaluateRepoStaleness } from "../lane-a-staleness-policy.js";
import { loadEffectiveDependencyGraph } from "./dependency-graph.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
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

async function readJsonOptional(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return { ok: true, exists: false, json: null };
  try {
    const t = await readFile(abs, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(t || "")) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function git(cmdArgs, { cwd }) {
  const res = spawnSync("git", cmdArgs, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function tryParseEvidenceRefsJsonl(text, { repoId }) {
  const ids = new Set();
  const lines = String(text || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  for (const line of lines) {
    const obj = JSON.parse(line);
    validateEvidenceRef(obj);
    if (normStr(obj.repo_id) !== repoId) throw new Error(`evidence_refs.jsonl repo_id mismatch (expected ${repoId}, got ${normStr(obj.repo_id) || "(missing)"})`);
    ids.add(String(obj.evidence_id));
  }
  return ids;
}

function readDecisionIdSample(openDecisionIds, limit) {
  const uniq = Array.from(new Set(openDecisionIds)).sort((a, b) => a.localeCompare(b));
  const cap = Math.max(0, Math.floor(Number.isFinite(limit) ? limit : 0));
  return uniq.slice(0, cap);
}

async function findOpenDecisions({ decisionsDirAbs }) {
  if (!existsSync(decisionsDirAbs)) return { ok: true, byScope: new Map(), totalOpen: 0 };
  const entries = await readdir(decisionsDirAbs, { withFileTypes: true });
  const jsonFiles = entries.filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json")).map((e) => e.name).sort((a, b) => a.localeCompare(b));

  const byScope = new Map(); // scope -> [decision_id]
  let totalOpen = 0;
  for (const f of jsonFiles) {
    const abs = join(decisionsDirAbs, f);
    // eslint-disable-next-line no-await-in-loop
    const res = await readJsonOptional(abs);
    if (!res.ok || !res.exists) continue;
    const j = res.json;
    const status = normStr(j?.status).toLowerCase();
    if (status !== "open") continue;
    const decision_id = normStr(j?.decision_id) || f.replace(/\.json$/, "");
    const scope = normStr(j?.scope) || "system";
    totalOpen += 1;
    const cur = byScope.get(scope) || [];
    cur.push(decision_id);
    byScope.set(scope, cur);
  }
  for (const [k, v] of byScope.entries()) byScope.set(k, Array.from(new Set(v)).sort((a, b) => a.localeCompare(b)));
  return { ok: true, byScope, totalOpen };
}

function segmentKeyFromFile(fileName) {
  const m = /^events-(\d{8}-\d{2})\.jsonl$/.exec(String(fileName || ""));
  return m ? m[1] : null;
}

function segmentFileForKey(key) {
  const k = normStr(key);
  if (!k) return null;
  return `events-${k}.jsonl`;
}

async function listSegmentFiles(segmentsDirAbs) {
  if (!existsSync(segmentsDirAbs)) return [];
  const entries = await readdir(segmentsDirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && /^events-\d{8}-\d{2}\.jsonl$/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

async function findLastMergeSeenAt({ segmentsDirAbs, checkpoint, repoId }) {
  const anchorSegKey = normStr(checkpoint?.last_processed_segment) || null;
  const anchorEvId = normStr(checkpoint?.last_processed_event_id) || null;
  if (!anchorSegKey || !anchorEvId) return null;

  const files = await listSegmentFiles(segmentsDirAbs);
  const anchorFile = segmentFileForKey(anchorSegKey);
  if (!anchorFile) return null;

  const upto = [];
  for (const f of files) {
    upto.push(f);
    if (f === anchorFile) break;
  }
  if (!upto.length || upto[upto.length - 1] !== anchorFile) return null;

  for (let idx = upto.length - 1; idx >= 0; idx -= 1) {
    const f = upto[idx];
    const abs = join(segmentsDirAbs, f);
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(abs, "utf8");
    const linesAll = String(text || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);

    let lines = linesAll;
    if (f === anchorFile) {
      const anchorIdx = linesAll.findIndex((l) => {
        try {
          const obj = JSON.parse(l);
          return String(obj.event_id) === anchorEvId;
        } catch {
          return false;
        }
      });
      if (anchorIdx >= 0) lines = linesAll.slice(0, anchorIdx + 1);
    }

    for (let j = lines.length - 1; j >= 0; j -= 1) {
      const obj = JSON.parse(lines[j]);
      if (normStr(obj.type) !== "merge") continue;
      if (normStr(obj.repo_id) !== repoId) continue;
      const ts = normStr(obj.timestamp);
      if (ts) return ts;
    }
  }
  return null;
}

async function computeRepoHeadInfo(repoAbs) {
  const rev = git(["rev-parse", "HEAD"], { cwd: repoAbs });
  if (!rev.ok) return { ok: false, repo_head_sha: null, repo_head_time: null, error: `git rev-parse HEAD failed: ${rev.stderr.trim() || rev.stdout.trim()}` };
  const sha = normStr(rev.stdout).split("\n")[0];
  const t = git(["show", "-s", "--format=%cI", "HEAD"], { cwd: repoAbs });
  if (!t.ok) return { ok: false, repo_head_sha: sha || null, repo_head_time: null, error: `git show HEAD time failed: ${t.stderr.trim() || t.stdout.trim()}` };
  const iso = normStr(t.stdout).split("\n")[0];
  return { ok: true, repo_head_sha: sha || null, repo_head_time: iso || null, error: null };
}

function msOrNull(iso) {
  const s = normStr(iso);
  if (!s) return null;
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? ms : null;
}

function computeOverall({ systemOpenDecisions, anyScanIncomplete, depsApproved, anyOrphans, anyStale, anyFailures }) {
  if (!depsApproved || anyScanIncomplete || systemOpenDecisions > 0) return "BLOCKED";
  if (anyOrphans || anyStale || anyFailures) return "DEGRADED";
  return "OK";
}

function renderStatusMd({ generatedAt, overall, deps, repos }) {
  const lines = [];
  lines.push("KNOWLEDGE STATUS");
  lines.push("");
  lines.push(`generated_at: ${generatedAt}`);
  lines.push(`overall: ${overall}`);
  if (deps && typeof deps === "object") {
    const status = normStr(deps.override_status) || "missing";
    const hash = normStr(deps.effective_graph_hash) || null;
    lines.push(`deps: status=${status}${hash ? ` hash=${hash.slice(0, 12)}…` : ""} edges=${Number(deps.edges || 0)} external_projects=${Number(deps.external_projects || 0)}`);
  }
  lines.push("");
  lines.push("REPOS");
  lines.push("");
  const sorted = Array.isArray(repos) ? repos.slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id))) : [];
  if (!sorted.length) lines.push("- (none)");
  for (const r of sorted) {
    const id = normStr(r.repo_id) || "(unknown)";
    const complete = r.scan && r.scan.complete === true ? "complete" : "incomplete";
    const stale = r.freshness && r.freshness.stale === true ? "stale" : "fresh";
    const orphans = r.evidence && typeof r.evidence.orphan_claims === "number" ? r.evidence.orphan_claims : 0;
    const openDec = r.decisions && typeof r.decisions.open_count === "number" ? r.decisions.open_count : 0;
    lines.push(`- ${id}: scan=${complete} ${stale} orphan_claims=${orphans} open_decisions=${openDec}`);
  }
  lines.push("");
  return lines.join("\n");
}

export async function runKnowledgeStatus({ projectRoot, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot: projectRoot ? resolve(String(projectRoot)) : null });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, message: reposRes.message };
  const registry = reposRes.registry;
  const activeRepoIds = listActiveRepoIds(registry);

  const generated_at = nowISO();

  const depsRes = await loadEffectiveDependencyGraph({ paths });
  const depsApproved = depsRes.ok && String(depsRes.override_status || "").trim().toLowerCase() === "approved";
  const deps = depsRes.ok
    ? {
        override_status: depsRes.override_status,
        effective_graph_hash: depsRes.effective_hash,
        edges: Array.isArray(depsRes.effective?.edges) ? depsRes.effective.edges.length : 0,
        external_projects: Array.isArray(depsRes.effective?.external_projects) ? depsRes.effective.external_projects.length : 0,
        graph_path: depsRes.graph_path,
        override_path: depsRes.override_path,
      }
    : {
        override_status: "missing",
        effective_graph_hash: null,
        edges: 0,
        external_projects: 0,
        graph_path: join(paths.knowledge.ssotSystemAbs, "dependency_graph.json"),
        override_path: join(paths.knowledge.ssotSystemAbs, "dependency_graph.override.json"),
      };

  const decisions = await findOpenDecisions({ decisionsDirAbs: paths.knowledge.decisionsAbs });
  if (!decisions.ok) return { ok: false, message: "Failed to load decisions." };

  const checkpointAbs = join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json");
  const cpRes = await readJsonOptional(checkpointAbs);
  if (!cpRes.ok) return { ok: false, message: `Invalid ${checkpointAbs}: ${cpRes.message}` };
  const checkpoint = cpRes.exists && isPlainObject(cpRes.json) ? cpRes.json : { version: 1, last_processed_event_id: null, last_processed_segment: null, updated_at: null };
  const last_refresh_at = normStr(checkpoint.updated_at) || null;

  const sysGapsAbs = join(paths.knowledge.ssotSystemAbs, "gaps.json");
  const sysGapsRes = await readJsonOptional(sysGapsAbs);
  let systemGaps = [];
  if (sysGapsRes.ok && sysGapsRes.exists) {
    const v = validateKnowledgeGapsFile(sysGapsRes.json);
    if (!v.ok) return { ok: false, message: `Invalid ${sysGapsAbs}: ${v.errors.join(" | ")}` };
    systemGaps = v.normalized.gaps;
  }

  const repos = [];
  let anyScanIncomplete = false;
  let anyOrphans = false;
  let anyStale = false;
  let anyFailures = false;

  for (const repoId of activeRepoIds) {
    const scanAbs = join(paths.knowledge.ssotReposAbs, repoId, "scan.json");
    const refsAbs = join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl");
    const scanErrAbs = join(paths.laneA.logsAbs, `knowledge-scan__${repoId}.error.json`);

    const scanErrRes = await readJsonOptional(scanErrAbs);
    const errCapturedAt = scanErrRes.ok && scanErrRes.exists ? normStr(scanErrRes.json?.captured_at) : null;
    const errCapturedMs = msOrNull(errCapturedAt);

    let scan = null;
    let scanExists = false;
    let scanOk = false;
    let scanAt = null;
    let scanAtMs = null;
    let scanFailures = 0;
    let scannedPathsCount = 0;
    let coveragePct = null;

    if (existsSync(scanAbs)) {
      const scanRes = await readJsonOptional(scanAbs);
      scanExists = scanRes.ok && scanRes.exists;
      if (scanExists) {
        try {
          validateKnowledgeScan(scanRes.json);
          scanOk = true;
          scan = scanRes.json;
          scanAt = normStr(scan.scanned_at) || null;
          scanAtMs = msOrNull(scanAt);
          scannedPathsCount = Number.isFinite(Number(scan?.coverage?.files_seen)) ? Number(scan.coverage.files_seen) : 0;
          const seen = Number.isFinite(Number(scan?.coverage?.files_seen)) ? Number(scan.coverage.files_seen) : 0;
          const idxd = Number.isFinite(Number(scan?.coverage?.files_indexed)) ? Number(scan.coverage.files_indexed) : 0;
          if (seen > 0) coveragePct = Math.round((idxd / seen) * 100);
          else coveragePct = 0;
        } catch {
          scanOk = false;
        }
      }
    }

    if (!scanOk) {
      scanFailures = scanErrRes.exists ? 1 : 0;
    } else {
      const errNewer = Number.isFinite(errCapturedMs) && (!Number.isFinite(scanAtMs) || errCapturedMs > scanAtMs);
      scanFailures = errNewer ? 1 : 0;
    }
    if (scanFailures) anyFailures = true;

    let evidenceIds = new Set();
    let evidenceOk = false;
    if (existsSync(refsAbs)) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const txt = await readFile(refsAbs, "utf8");
        evidenceIds = tryParseEvidenceRefsJsonl(txt, { repoId });
        evidenceOk = true;
      } catch {
        evidenceOk = false;
      }
    }

    const facts = Array.isArray(scan?.facts) ? scan.facts : [];
    let orphan = 0;
    for (const f of facts) {
      const eids = Array.isArray(f?.evidence_ids) ? f.evidence_ids : [];
      if (!eids.length) {
        orphan += 1;
        continue;
      }
      const missing = eids.some((id) => !evidenceIds.has(String(id)));
      if (missing) orphan += 1;
    }
    const backed = Math.max(0, facts.length - orphan);
    if (orphan > 0) anyOrphans = true;

    const complete = scanOk && evidenceOk;
    if (!complete) anyScanIncomplete = true;

    const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: (registry.repos || []).find((r) => normStr(r?.repo_id) === repoId)?.path });
    let head = { ok: false, repo_head_sha: null, repo_head_time: null, error: "repo path not found" };
    if (repoAbs && existsSync(repoAbs)) head = await computeRepoHeadInfo(repoAbs);

    // Freshness is based on Lane A staleness policy (scan head vs repo head, merge-after-scan), not on refresh checkpoints.
    // eslint-disable-next-line no-await-in-loop
    const staleInfo = await evaluateRepoStaleness({ paths, registry, repoId });
    const stale = staleInfo.stale === true;
    const stale_reason = staleInfo.stale_reason || null;
    if (stale) anyStale = true;

    const openScopeKey = `repo:${repoId}`;
    const openIds = decisions.byScope.get(openScopeKey) || [];
    const openIdsSample = readDecisionIdSample(openIds, 25);

    const last_merge_seen_at = await findLastMergeSeenAt({ segmentsDirAbs: paths.laneA.eventsSegmentsAbs, checkpoint, repoId });
    const last_event_checkpoint =
      normStr(checkpoint?.last_processed_segment) && normStr(checkpoint?.last_processed_event_id)
        ? `${normStr(checkpoint.last_processed_segment)}:${normStr(checkpoint.last_processed_event_id)}`
        : null;

    const repoGaps = systemGaps.filter((g) => normStr(g?.suggested_intake?.repo_id) === repoId);

    // Repo dependencies (cross-project) + external bundle availability.
    const repoIndexAbs = join(paths.knowledge.evidenceIndexReposAbs, repoId, "repo_index.json");
    let depends_on = [];
    if (existsSync(repoIndexAbs)) {
      const riRes = await readJsonOptional(repoIndexAbs);
      if (riRes.ok && riRes.exists) {
        try {
          validateRepoIndex(riRes.json);
          depends_on = Array.isArray(riRes.json?.dependencies?.depends_on) ? riRes.json.dependencies.depends_on : [];
        } catch {
          depends_on = [];
        }
      }
    }
    const external_deps = depends_on
      .map((d) => {
        const pc = normStr(d?.project_code);
        const rid = normStr(d?.repo_id);
        const k = normStr(d?.knowledge_abs_path);
        const scanAbs = pc && rid && k ? join(k, "ssot", "repos", rid, "scan.json") : null;
        const ok = !!(scanAbs && existsSync(scanAbs));
        return { project_code: pc, repo_id: rid, bundle_ok: ok, bundle_path: scanAbs };
      })
      .filter((d) => d.project_code && d.repo_id)
      .sort((a, b) => `${a.project_code}::${a.repo_id}`.localeCompare(`${b.project_code}::${b.repo_id}`));

    repos.push({
      repo_id: repoId,
      scan: {
        complete,
        last_scan_at: scanAt,
        failures: scanFailures,
        scanned_paths_count: scannedPathsCount,
        coverage_pct: coveragePct,
      },
      events: {
        last_merge_seen_at,
        last_event_checkpoint,
      },
      freshness: {
        repo_head_sha: head.repo_head_sha,
        repo_head_time: head.repo_head_time,
        last_refresh_at,
        stale,
        stale_reason,
      },
      decisions: {
        open_count: openIds.length,
        open_ids_sample: openIdsSample,
      },
      gaps: {
        repo_gaps_open_count: repoGaps.length,
        integration_gap_refs: [],
      },
      evidence: {
        backed_facts: backed,
        orphan_claims: orphan,
        orphan_samples: orphan > 0 ? facts.filter((f) => Array.isArray(f?.evidence_ids) && f.evidence_ids.some((id) => !evidenceIds.has(String(id)))).slice(0, 10).map((f) => f.fact_id) : [],
      },
      deps: {
        depends_on: external_deps,
      },
    });
  }

  const openSystemIds = decisions.byScope.get("system") || [];

  const system = {
    scan_complete_all_repos: !anyScanIncomplete && activeRepoIds.length > 0,
    open_decisions_count: decisions.totalOpen,
    integration_gaps_unresolved_count: systemGaps.length,
    evidence: {
      backed_facts: repos.reduce((acc, r) => acc + Number(r?.evidence?.backed_facts || 0), 0),
      orphan_claims: repos.reduce((acc, r) => acc + Number(r?.evidence?.orphan_claims || 0), 0),
    },
    open_decision_ids_sample: readDecisionIdSample(openSystemIds, 25),
  };

  const overall = computeOverall({
    systemOpenDecisions: system.open_decisions_count,
    anyScanIncomplete,
    depsApproved,
    anyOrphans,
    anyStale,
    anyFailures,
  });

  const out = {
    ok: true,
    projectRoot: paths.opsRootAbs,
    generated_at,
    knowledge_repo: paths.knowledge.rootAbs,
    repos_root: paths.reposRootAbs,
    overall,
    deps,
    repos: repos.sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id))),
    system,
  };

  const statusMdAbs = join(paths.laneA.rootAbs, "STATUS.md");
  if (!dryRun) await writeTextAtomic(statusMdAbs, renderStatusMd({ generatedAt: generated_at, overall, deps: out.deps, repos: out.repos }));

  return out;
}
