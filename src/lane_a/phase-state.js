import { existsSync, readFileSync } from "node:fs";
import { mkdir, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";
import { loadRepoRegistry } from "../utils/repo-registry.js";
import { readKnowledgeVersionOrDefault } from "./knowledge/knowledge-version.js";
import { validateKnowledgeScan, validateRepoIndex, validatePhaseState, validateSufficiency } from "../contracts/validators/index.js";

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

async function writeJsonAtomic(absPath, obj) {
  await writeTextAtomic(absPath, JSON.stringify(obj, null, 2) + "\n");
}

function readJsonOptional(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return { ok: true, exists: false, json: null };
  try {
    const t = String(readFileSync(abs, "utf8") || "");
    return { ok: true, exists: true, json: JSON.parse(t) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

export function phasePaths(paths) {
  const dirAbs = join(paths.laneA.rootAbs, "phases");
  return {
    dirAbs,
    jsonAbs: join(dirAbs, "PHASE.json"),
    mdAbs: join(dirAbs, "PHASE.md"),
    forwardBlockedAbs: join(dirAbs, "FORWARD_BLOCKED.json"),
  };
}

export function defaultPhaseState({ projectRootAbs }) {
  const obj = {
    version: 1,
    projectRoot: projectRootAbs,
    current_phase: "reverse",
    reverse: { status: "not_started", session_id: null, started_at: null, closed_at: null, closed_by: null, notes: null },
    forward: { status: "not_started", session_id: null, started_at: null, closed_at: null, closed_by: null, notes: null },
    prereqs: {
      scan_complete: false,
      sufficiency: "unknown",
      human_confirmed_v1: false,
      human_confirmed_at: null,
      human_confirmed_by: null,
      human_notes: null,
    },
  };
  validatePhaseState(obj);
  return obj;
}

export function renderPhaseMd(phase) {
  const p = phase;
  const lines = [];
  lines.push("LANE A PHASE STATE");
  lines.push("");
  lines.push(`projectRoot: ${p.projectRoot}`);
  lines.push(`current_phase: ${p.current_phase}`);
  lines.push("");
  lines.push("REVERSE");
  lines.push(`- status: ${p.reverse.status}`);
  lines.push(`- session_id: ${p.reverse.session_id ?? ""}`);
  lines.push(`- started_at: ${p.reverse.started_at ?? ""}`);
  lines.push(`- closed_at: ${p.reverse.closed_at ?? ""}`);
  lines.push(`- closed_by: ${p.reverse.closed_by ?? ""}`);
  if (p.reverse.notes) lines.push(`- notes: ${p.reverse.notes}`);
  lines.push("");
  lines.push("FORWARD");
  lines.push(`- status: ${p.forward.status}`);
  lines.push(`- session_id: ${p.forward.session_id ?? ""}`);
  lines.push(`- started_at: ${p.forward.started_at ?? ""}`);
  lines.push(`- closed_at: ${p.forward.closed_at ?? ""}`);
  lines.push(`- closed_by: ${p.forward.closed_by ?? ""}`);
  if (p.forward.notes) lines.push(`- notes: ${p.forward.notes}`);
  lines.push("");
  lines.push("PREREQS");
  lines.push(`- scan_complete: ${p.prereqs.scan_complete}`);
  lines.push(`- sufficiency: ${p.prereqs.sufficiency}`);
  lines.push(`- human_confirmed_v1: ${p.prereqs.human_confirmed_v1}`);
  lines.push(`- human_confirmed_at: ${p.prereqs.human_confirmed_at ?? ""}`);
  lines.push(`- human_confirmed_by: ${p.prereqs.human_confirmed_by ?? ""}`);
  if (p.prereqs.human_notes) lines.push(`- human_notes: ${p.prereqs.human_notes}`);
  lines.push("");
  return lines.join("\n");
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function coverageCompleteForRepo({ knowledgeRootAbs, repoId }) {
  const idxAbs = join(knowledgeRootAbs, "evidence", "index", "repos", repoId, "repo_index.json");
  const scanAbs = join(knowledgeRootAbs, "ssot", "repos", repoId, "scan.json");
  if (!existsSync(idxAbs) || !existsSync(scanAbs)) return false;
  try {
    const idx = JSON.parse(String(readFileSync(idxAbs, "utf8") || ""));
    validateRepoIndex(idx);
    const scan = JSON.parse(String(readFileSync(scanAbs, "utf8") || ""));
    validateKnowledgeScan(scan);
    return true;
  } catch {
    return false;
  }
}

function readSystemSufficiencyToken({ paths, knowledgeVersion }) {
  // Read from knowledge git repo decision index (git-worthy source) to avoid coupling to Lane A ops pointers.
  const latestAbs = join(paths.knowledge.decisionsAbs, "sufficiency", "LATEST.json");
  if (!existsSync(latestAbs)) return "unknown";
  try {
    const j = JSON.parse(String(readFileSync(latestAbs, "utf8") || ""));
    const entry = j && typeof j.latest_by_scope === "object" && j.latest_by_scope ? j.latest_by_scope.system : null;
    const rec = entry && typeof entry.record === "object" && entry.record ? entry.record : null;
    if (!rec) return "unknown";
    validateSufficiency(rec);
    if (normStr(rec.knowledge_version) !== normStr(knowledgeVersion)) return "unknown";
    const st = normStr(rec.status).toLowerCase();
    if (st === "sufficient") return "sufficient";
    if (st === "insufficient" || st === "proposed_sufficient") return "insufficient";
    return "unknown";
  } catch {
    return "unknown";
  }
}

export async function readPhaseStateOrDefault({ projectRoot } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const pp = phasePaths(paths);
  const r = readJsonOptional(pp.jsonAbs);
  if (!r.ok) throw new Error(`Invalid PHASE.json (${pp.jsonAbs}): ${r.message}`);
  if (!r.exists) return { ok: true, exists: false, phase: defaultPhaseState({ projectRootAbs: paths.opsRootAbs }), paths, phasePaths: pp };
  validatePhaseState(r.json);
  return { ok: true, exists: true, phase: r.json, paths, phasePaths: pp };
}

export async function writePhaseState({ paths, phase, dryRun = false } = {}) {
  const pp = phasePaths(paths);
  validatePhaseState(phase);
  if (!dryRun) {
    await mkdir(pp.dirAbs, { recursive: true });
    await writeJsonAtomic(pp.jsonAbs, phase);
    await writeTextAtomic(pp.mdAbs, renderPhaseMd(phase));
  }
  return { ok: true, paths: { json: pp.jsonAbs, md: pp.mdAbs }, dry_run: !!dryRun };
}

export async function refreshPhasePrereqs({ projectRoot, dryRun = false } = {}) {
  const { phase, paths } = await readPhaseStateOrDefault({ projectRoot }).then((r) => ({ phase: r.phase, paths: r.paths }));
  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  const registry = reposRes.ok ? reposRes.registry : { version: 1, repos: [], base_dir: paths.reposRootAbs };
  const repoIds = listActiveRepoIds(registry);
  const scan_complete = repoIds.length > 0 && repoIds.every((rid) => coverageCompleteForRepo({ knowledgeRootAbs: paths.knowledge.rootAbs, repoId: rid }));

  const { version } = await readKnowledgeVersionOrDefault({ projectRoot: paths.opsRootAbs });
  const suff = readSystemSufficiencyToken({ paths, knowledgeVersion: version.current });

  const next = {
    ...phase,
    prereqs: {
      ...phase.prereqs,
      scan_complete,
      sufficiency: suff,
    },
  };
  validatePhaseState(next);
  await writePhaseState({ paths, phase: next, dryRun });
  return { ok: true, phase: next };
}

export function computeForwardBlockReasons(phase) {
  const p = phase;
  const reasons = [];
  if (normStr(p.reverse.status) !== "closed") reasons.push("reverse_not_closed");
  if (p.prereqs.scan_complete !== true) reasons.push("scan_incomplete");
  if (normStr(p.prereqs.sufficiency) !== "sufficient") reasons.push("sufficiency_not_sufficient");
  if (p.prereqs.human_confirmed_v1 !== true) reasons.push("human_confirmed_v1_missing");
  return reasons;
}

