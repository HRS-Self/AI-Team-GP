import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import { dirname, join, resolve } from "node:path";

import { nowFsSafeUtcTimestamp } from "../utils/naming.js";
import {
  validateLaneAState,
  validateCommitteeStatus,
  validateDecisionPacket,
  validateEvidenceRef,
  validateIntegrationStatus,
  validateKnowledgeScan,
  validateMeeting,
  validateRepoIndex,
  validateKnowledgeChangeEvent,
} from "../contracts/validators/index.js";
import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../paths/project-paths.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../utils/repo-registry.js";
import { runRepoIndex } from "./knowledge/repo-indexer.js";
import { runKnowledgeScan } from "./knowledge/knowledge-scan.js";
import { runRefreshFromEvents } from "./knowledge/knowledge-refresh-from-events.js";
import { runQaMergeFollowups } from "./events/qa-merge-followups.js";
import { assertKickoffLatestShape } from "./knowledge/kickoff-utils.js";
import { readSufficiencyOrDefault } from "./knowledge/knowledge-sufficiency.js";
import { evaluateScopeStaleness } from "./lane-a-staleness-policy.js";
import { acquireOpsLock, releaseOpsLock } from "../utils/ops-lock.js";
import { handleSoftStaleEscalation } from "./staleness/soft-stale-escalation.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
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

async function readJsonAbs(absPath) {
  const t = await readFile(resolve(String(absPath || "")), "utf8");
  return JSON.parse(String(t || ""));
}

async function loadJsonOptional(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return { ok: true, exists: false, json: null };
  try {
    const json = await readJsonAbs(abs);
    return { ok: true, exists: true, json };
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

async function listDecisionPackets({ decisionsDirAbs }) {
  if (!existsSync(decisionsDirAbs)) return { ok: true, packets: [], open: [], answered: [] };
  const entries = await readdir(decisionsDirAbs, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"))
    .map((e) => join(decisionsDirAbs, e.name))
    .sort((a, b) => a.localeCompare(b));

  const packets = [];
  const open = [];
  const answered = [];
  for (const p of files) {
    // eslint-disable-next-line no-await-in-loop
    const json = await readJsonAbs(p);
    validateDecisionPacket(json);
    packets.push({ path: p, packet: json });
    if (json.status === "open") open.push({ path: p, decision_id: String(json.decision_id) });
    if (json.status === "answered") answered.push({ path: p, decision_id: String(json.decision_id) });
  }
  return { ok: true, packets, open, answered };
}

function parseEvidenceRefsJsonl(text) {
  const lines = String(text || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  const ids = [];
  for (const l of lines) {
    const obj = JSON.parse(l);
    validateEvidenceRef(obj);
    ids.push(String(obj.evidence_id));
  }
  ids.sort((a, b) => a.localeCompare(b));
  return ids;
}

async function computeLowCodeEvidence({ repoIds, evidenceIndexReposAbs }) {
  const codeCats = new Set(["source", "api_contract", "schema", "migration"]);
  let count = 0;
  for (const repoId of repoIds) {
    const fpAbs = join(evidenceIndexReposAbs, repoId, "repo_fingerprints.json");
    if (!existsSync(fpAbs)) continue;
    // eslint-disable-next-line no-await-in-loop
    const fp = await readJsonAbs(fpAbs);
    const files = Array.isArray(fp?.files) ? fp.files : [];
    for (const f of files) {
      const cat = typeof f?.category === "string" ? f.category.trim() : "";
      if (codeCats.has(cat)) count += 1;
    }
  }
  return count < 3;
}

function computeMinimumSufficient({ minimumJson, scansByRepoId }) {
  if (minimumJson == null) return { ok: true, minimum_sufficient: true, missing: [] };
  if (!isPlainObject(minimumJson)) return { ok: false, message: "ssot/system/minimum.json must be an object." };
  const allowed = new Set(["version", "required_facts"]);
  for (const k of Object.keys(minimumJson)) if (!allowed.has(k)) return { ok: false, message: `ssot/system/minimum.json unknown field '${k}'.` };
  if (minimumJson.version !== 1) return { ok: false, message: "ssot/system/minimum.json.version must be 1." };
  if (!Array.isArray(minimumJson.required_facts)) return { ok: false, message: "ssot/system/minimum.json.required_facts must be an array." };

  const missing = [];
  for (const req of minimumJson.required_facts) {
    if (!isPlainObject(req)) return { ok: false, message: "ssot/system/minimum.json.required_facts items must be objects." };
    const a = new Set(["repo_id", "fact_contains"]);
    for (const k of Object.keys(req)) if (!a.has(k)) return { ok: false, message: `ssot/system/minimum.json.required_facts unknown field '${k}'.` };
    const repo_id = normStr(req.repo_id);
    const fact_contains = normStr(req.fact_contains);
    if (!repo_id) return { ok: false, message: "ssot/system/minimum.json.required_facts.repo_id is required." };
    if (!fact_contains) return { ok: false, message: "ssot/system/minimum.json.required_facts.fact_contains is required." };
    const scan = scansByRepoId.get(repo_id);
    const facts = Array.isArray(scan?.facts) ? scan.facts : [];
    const ok = facts.some((f) => typeof f?.claim === "string" && f.claim.includes(fact_contains));
    if (!ok) missing.push({ repo_id, fact_contains });
  }
  return { ok: true, minimum_sufficient: missing.length === 0, missing };
}

async function countPendingEvents({ segmentsDirAbs, checkpointAbs, maxCount = 1 }) {
  if (!existsSync(segmentsDirAbs)) return { ok: true, pending: 0 };
  const cpRes = await loadJsonOptional(checkpointAbs);
  let checkpoint = { last_processed_event_id: null, last_processed_segment: null };
  if (cpRes.ok && cpRes.exists && isPlainObject(cpRes.json)) {
    const j = cpRes.json;
    if (j.version === 1) {
      checkpoint = {
        last_processed_event_id: normStr(j.last_processed_event_id) || null,
        last_processed_segment: normStr(j.last_processed_segment) || null,
      };
    }
  }

  const files = (await readdir(segmentsDirAbs, { withFileTypes: true }))
    .filter((e) => e.isFile() && /^events-\d{8}-\d{2}\.jsonl$/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));

  const anchorFile = checkpoint.last_processed_segment ? `events-${checkpoint.last_processed_segment}.jsonl` : null;
  let started = anchorFile == null;
  let anchorFound = checkpoint.last_processed_event_id == null;
  let pending = 0;

  for (const f of files) {
    if (!started) {
      if (f !== anchorFile) continue;
      started = true;
    }
    const abs = join(segmentsDirAbs, f);
    // eslint-disable-next-line no-await-in-loop
    const text = await readFile(abs, "utf8");
    const lines = String(text || "").split("\n");
    for (const raw of lines) {
      const line = String(raw || "").trim();
      if (!line) continue;
      const obj = JSON.parse(line);
      validateKnowledgeChangeEvent(obj);
      if (!anchorFound) {
        if (String(obj.event_id) === checkpoint.last_processed_event_id) anchorFound = true;
        continue;
      }
      pending += 1;
      if (pending >= maxCount) return { ok: true, pending };
    }
  }

  if (anchorFile && started && !anchorFound) return { ok: false, message: `events checkpoint anchor not found in ${anchorFile}` };
  if (anchorFile && !started) return { ok: false, message: `events checkpoint segment not found: ${anchorFile}` };
  return { ok: true, pending };
}

function renderStateMd(state) {
  const lines = [];
  lines.push("LANE A STATE");
  lines.push("");
  lines.push(`stage: ${state.stage}`);
  lines.push(`evidence_level: ${state.evidence_state.evidence_level}`);
  lines.push(`scan_coverage_complete: ${state.evidence_state.scan_coverage_complete}`);
  lines.push(`minimum_sufficient: ${state.evidence_state.minimum_sufficient}`);
  lines.push(`pending_events: ${state.evidence_state.pending_events}`);
  lines.push(`last_index_at: ${state.evidence_state.last_index_at ?? "-"}`);
  lines.push(`last_scan_at: ${state.evidence_state.last_scan_at ?? "-"}`);
  lines.push(`last_synth_at: ${state.evidence_state.last_synth_at ?? "-"}`);
  lines.push("");
  lines.push("NEXT ACTION");
  lines.push("");
  lines.push(`type: ${state.next_action.type}`);
  lines.push(`target_repos: ${(Array.isArray(state.next_action.target_repos) ? state.next_action.target_repos : []).join(", ") || "-"}`);
  lines.push(`reason: ${state.next_action.reason}`);
  lines.push("");
  return lines.join("\n") + "\n";
}

async function loadCommitteeState({ paths, repoIds }) {
  const missing = [];
  const failed = [];
  const passed = [];
  const stale = [];

  for (const repoId of repoIds) {
    const dirAbs = join(paths.knowledge.ssotReposAbs, repoId, "committee");
    const staleAbs = join(dirAbs, "STALE.json");
    const stAbs = join(dirAbs, "committee_status.json");
    if (existsSync(staleAbs)) {
      stale.push(repoId);
      missing.push(repoId);
      continue;
    }
    if (!existsSync(stAbs)) {
      missing.push(repoId);
      continue;
    }
    // eslint-disable-next-line no-await-in-loop
    const j = await readJsonAbs(stAbs);
    validateCommitteeStatus(j);
    if (String(j.repo_id) !== repoId) throw new Error(`committee_status.json repo_id mismatch for ${repoId}`);
    if (j.evidence_valid === true) passed.push(repoId);
    else failed.push(repoId);
  }

  const integDirAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration");
  const integStaleAbs = join(integDirAbs, "STALE.json");
  const integAbs = join(integDirAbs, "integration_status.json");
  let integration = { exists: false, stale: false, status: null };
  if (existsSync(integStaleAbs)) integration = { exists: false, stale: true, status: null };
  else if (existsSync(integAbs)) {
    const j = await readJsonAbs(integAbs);
    validateIntegrationStatus(j);
    integration = { exists: true, stale: false, status: j };
  }

  return { missing, failed, passed, stale, integration };
}

function scopeSlugForHint(scope) {
  const s = normStr(scope);
  if (!s) return "system";
  if (s === "system") return "system";
  if (s.startsWith("repo:")) {
    const id = normStr(s.slice("repo:".length));
    const slug = id.replace(/[^A-Za-z0-9_.-]/g, "_");
    return `repo-${slug || "unknown"}`;
  }
  return s.replace(/[^A-Za-z0-9_.-]/g, "_") || "system";
}

async function hasOpenUpdateMeeting({ meetingsAbs, scopeSlug }) {
  const dirAbs = resolve(String(meetingsAbs || ""));
  if (!dirAbs || !existsSync(dirAbs)) return false;
  const entries = await readdir(dirAbs, { withFileTypes: true });
  const dirs = entries
    .filter((e) => e.isDirectory() && /^UM-\d{8}_\d{6}__/.test(e.name) && e.name.includes(`__${scopeSlug}`))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
  for (const d of dirs) {
    const jsonAbs = join(dirAbs, d, "MEETING.json");
    if (!existsSync(jsonAbs)) continue;
    try {
      // eslint-disable-next-line no-await-in-loop
      const j = await readJsonAbs(jsonAbs);
      validateMeeting(j);
      if (String(j.status) !== "closed") return true;
    } catch {
      // ignore invalid sessions
    }
  }
  return false;
}

async function writeRefreshHintIfNeeded({ paths, registry }) {
  const st = await evaluateScopeStaleness({ paths, registry, scope: "system" });
  const escalation = await handleSoftStaleEscalation({
    paths,
    scope: "system",
    stalenessSnapshot: st,
  });
  if (!st.ok || !st.stale) return { ok: true, wrote: false, escalation };

  const targetScope = Array.isArray(st.stale_repos) && st.stale_repos.length ? `repo:${st.stale_repos[0]}` : "system";
  const scopeSlug = scopeSlugForHint(targetScope);
  const meetingsAbs = join(paths.laneA.rootAbs, "meetings");
  const hasMeeting = await hasOpenUpdateMeeting({ meetingsAbs, scopeSlug });
  if (hasMeeting) return { ok: true, wrote: false, escalation };

  const ts = nowFsSafeUtcTimestamp();
  const outAbs = join(paths.laneA.refreshHintsAbs, `RH-${ts}__${scopeSlug}.json`);
  const hint = {
    version: 1,
    scope: targetScope,
    reason: `stale:${(st.reasons || []).join(",") || "unknown"}`,
    recommended_action: "knowledge-refresh",
  };

  await mkdir(paths.laneA.refreshHintsAbs, { recursive: true });
  await writeTextAtomic(outAbs, JSON.stringify(hint, null, 2) + "\n");
  // Cap runaway: keep last N hint files.
  const maxKeep = 50;
  try {
    const ents = await readdir(paths.laneA.refreshHintsAbs, { withFileTypes: true });
    const files = ents
      .filter((e) => e.isFile() && e.name.startsWith("RH-") && e.name.endsWith(".json"))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b));
    const overflow = files.length - maxKeep;
    if (overflow > 0) {
      const del = files.slice(0, overflow);
      for (const f of del) {
        // eslint-disable-next-line no-await-in-loop
        await rm(join(paths.laneA.refreshHintsAbs, f), { force: true });
      }
    }
  } catch {
    // ignore cap errors
  }

  return { ok: true, wrote: true, hint_abs: outAbs, scope: targetScope, escalation };
}

function resolveLaneALockTtlMs() {
  const raw = normStr(process.env.LANE_A_LOCK_TTL_MS);
  if (!raw) return 8 * 60 * 1000;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 8 * 60 * 1000;
  return parsed;
}

async function writeLockStatusSnapshot({ lockStatusDirAbs, snapshot }) {
  await mkdir(lockStatusDirAbs, { recursive: true });
  const ts = nowFsSafeUtcTimestamp();
  const outAbs = join(lockStatusDirAbs, `LOCK_STATUS-${ts}.json`);
  await writeTextAtomic(outAbs, JSON.stringify(snapshot, null, 2) + "\n");

  const entries = await readdir(lockStatusDirAbs, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && /^LOCK_STATUS-\d{8}_\d{9}\.json$/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));

  const maxKeep = 50;
  const overflow = files.length - maxKeep;
  if (overflow > 0) {
    for (const f of files.slice(0, overflow)) {
      // eslint-disable-next-line no-await-in-loop
      await rm(join(lockStatusDirAbs, f), { force: true });
    }
  }
  return outAbs;
}

export async function runLaneAOrchestrate({ projectRoot, limit = null, dryRun = false } = {}) {
  const max = typeof limit === "number" && Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : null;
  const capRepos = (arr) => (max == null ? arr.slice() : arr.slice(0, max));

  let paths;
  try {
    paths = await loadProjectPaths({ projectRoot });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: msg };
  }
  const lockPath = paths.laneA.lockPathAbs;
  const lockStatusDirAbs = paths.laneA.locksStatusAbs;
  const lockOwner = {
    pid: process.pid,
    uid: process.getuid?.() ?? null,
    user: process.env.USER || process.env.LOGNAME || null,
    host: os.hostname(),
    cwd: process.cwd(),
    command: process.argv.join(" "),
    project_root: paths.opsRootAbs,
    ai_project_root: paths.opsRootAbs,
  };
  const ttlMs = resolveLaneALockTtlMs();
  const lockRes = await acquireOpsLock({ lockPath, ttlMs, owner: lockOwner });

  if (!lockRes.ok) {
    try {
      await writeLockStatusSnapshot({
        lockStatusDirAbs,
        snapshot: {
          version: 1,
          ts: new Date().toISOString(),
          projectRoot: paths.opsRootAbs,
          lockPath,
          acquired: false,
          reason: "error",
          lock: null,
          note: lockRes.error,
        },
      });
    } catch {
      // best effort only
    }
    return { ok: false, message: lockRes.error || "Failed to acquire Lane A lock." };
  }

  try {
    await writeLockStatusSnapshot({
      lockStatusDirAbs,
      snapshot: {
        version: 1,
        ts: new Date().toISOString(),
        projectRoot: paths.opsRootAbs,
        lockPath,
        acquired: !!lockRes.acquired,
        reason: lockRes.acquired ? (lockRes.broke_stale ? "broke_stale" : "acquired") : "lock_held",
        lock: lockRes.lock || null,
      },
    });
  } catch {
    // best effort only
  }

  if (!lockRes.acquired) {
    return { ok: true, skipped: true, reason: "lock_held", lock: lockRes.lock || null };
  }

  const stateAbs = join(paths.laneA.checkpointsAbs, "state.json");
  const stateMdAbs = join(paths.laneA.checkpointsAbs, "STATE.md");
  const hintAbs = join(paths.laneA.checkpointsAbs, "next_action_hint.json");
  const errorAbs = join(paths.laneA.checkpointsAbs, "state.error.json");

  try {
    if (!dryRun) {
      await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
      await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });
    }

    const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
    if (!reposRes.ok) return { ok: false, message: reposRes.message };
    const registry = reposRes.registry;

    const activeRepoIds = listActiveRepoIds(registry);
    if (!activeRepoIds.length) return { ok: false, message: "No active repos found in config/REPOS.json." };

    const openDecisionsRes = await listDecisionPackets({ decisionsDirAbs: paths.knowledge.decisionsAbs });
    const openDecisions = openDecisionsRes.open;
    const answeredDecisions = openDecisionsRes.answered;

    const missingIndex = [];
    const missingScan = [];
    const lastIndexAt = [];
    const lastScanAt = [];
    const scansByRepoId = new Map();

    for (const repoId of activeRepoIds) {
      const idxDirAbs = join(paths.knowledge.evidenceIndexReposAbs, repoId);
      const idxAbs = join(idxDirAbs, "repo_index.json");
      const fpAbs = join(idxDirAbs, "repo_fingerprints.json");
      if (!existsSync(idxAbs) || !existsSync(fpAbs)) {
        missingIndex.push(repoId);
      } else {
        // eslint-disable-next-line no-await-in-loop
        const idx = await readJsonAbs(idxAbs);
        validateRepoIndex(idx);
        if (typeof idx.scanned_at === "string" && idx.scanned_at.trim()) lastIndexAt.push(idx.scanned_at.trim());
      }

      const scanAbs = join(paths.knowledge.ssotReposAbs, repoId, "scan.json");
      const refsAbs = join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl");
      if (!existsSync(scanAbs) || !existsSync(refsAbs)) {
        missingScan.push(repoId);
      } else {
        // eslint-disable-next-line no-await-in-loop
        const scan = await readJsonAbs(scanAbs);
        validateKnowledgeScan(scan);
        if (typeof scan.scanned_at === "string" && scan.scanned_at.trim()) lastScanAt.push(scan.scanned_at.trim());
        scansByRepoId.set(repoId, scan);

        // Validate evidence refs JSONL quickly (ids only); validation errors are fatal.
        // eslint-disable-next-line no-await-in-loop
        const refsText = await readFile(refsAbs, "utf8");
        parseEvidenceRefsJsonl(refsText);
      }
    }

    const evidence_level = missingIndex.length ? "none" : missingScan.length ? "partial" : "complete";
    const scan_coverage_complete = missingIndex.length === 0 && missingScan.length === 0;

    const kickoffLatestAbs = join(paths.knowledge.sessionsAbs, "kickoff", "LATEST.json");
    let kickoffExists = false;
    let kickoffSuff = null;
    if (existsSync(kickoffLatestAbs)) {
      const ko = assertKickoffLatestShape(await readJsonAbs(kickoffLatestAbs));
      const sys = isPlainObject(ko.latest_by_scope) ? ko.latest_by_scope.system : null;
      if (sys) {
        kickoffExists = true;
        kickoffSuff = isPlainObject(sys.sufficiency) ? normStr(sys.sufficiency.status) : null;
      }
    }

    const lowEvidence = await computeLowCodeEvidence({ repoIds: activeRepoIds, evidenceIndexReposAbs: paths.knowledge.evidenceIndexReposAbs });

    const pendingEventsRes = await countPendingEvents({
      segmentsDirAbs: paths.laneA.eventsSegmentsAbs,
      checkpointAbs: join(paths.laneA.eventsCheckpointsAbs, "last_refresh.json"),
      maxCount: 1000,
    });
    if (!pendingEventsRes.ok) throw new Error(pendingEventsRes.message);
    const pending_events = pendingEventsRes.pending;

    const minAbs = join(paths.knowledge.ssotSystemAbs, "minimum.json");
    const minRes = await loadJsonOptional(minAbs);
    if (!minRes.ok) throw new Error(`Invalid ${minAbs}: ${minRes.message}`);
    const minEval = computeMinimumSufficient({ minimumJson: minRes.exists ? minRes.json : null, scansByRepoId });
    if (!minEval.ok) throw new Error(minEval.message);

    const integAbs = join(paths.knowledge.ssotSystemAbs, "integration.json");
    let last_synth_at = null;
    if (existsSync(integAbs)) {
      const j = await readJsonAbs(integAbs);
      if (isPlainObject(j) && typeof j.captured_at === "string") last_synth_at = j.captured_at;
    }

    const evidenceState = {
      evidence_level,
      scan_coverage_complete,
      minimum_sufficient: minEval.minimum_sufficient,
      milestone_status: {},
      last_scan_at: lastScanAt.length ? lastScanAt.slice().sort((a, b) => a.localeCompare(b)).at(-1) : null,
      last_index_at: lastIndexAt.length ? lastIndexAt.slice().sort((a, b) => a.localeCompare(b)).at(-1) : null,
      last_synth_at,
      pending_events,
    };

    const committeeState = scan_coverage_complete ? await loadCommitteeState({ paths, repoIds: activeRepoIds }) : null;

    let stage = null;
    let next_action = null;

    if (openDecisions.length) {
      stage = "DECISION_NEEDED";
      next_action = { type: "question", target_repos: [], reason: `open decisions: ${openDecisions.map((d) => d.decision_id).join(", ")}` };
    } else if (evidence_level === "none") {
      stage = "NEEDS_INDEX";
      next_action = { type: "index", target_repos: capRepos(missingIndex), reason: `missing repo index for ${missingIndex.length} repo(s)` };
    } else if (evidence_level === "partial") {
      stage = "NEEDS_SCAN";
      next_action = { type: "scan", target_repos: capRepos(missingScan), reason: `missing scan outputs for ${missingScan.length} repo(s)` };
    } else if ((!kickoffExists || kickoffSuff !== "sufficient") && lowEvidence) {
      stage = "NEEDS_KICKOFF";
      const why = kickoffExists ? `kickoff sufficiency is '${kickoffSuff || "(missing)"}'` : "kickoff is missing and code evidence is low";
      next_action = { type: "kickoff", target_repos: [], reason: `NEEDS_KICKOFF: ${why}` };
    } else if (pending_events > 0) {
      stage = "REFRESH_NEEDED";
      next_action = { type: "refresh", target_repos: [], reason: "REFRESH_NEEDED: new Lane B events pending" };
    } else if (!minEval.minimum_sufficient) {
      stage = "COMMITTEE_PENDING";
      next_action = { type: "committee", target_repos: capRepos(activeRepoIds), reason: "COMMITTEE_PENDING: minimum knowledge requirements not satisfied" };
    } else if (committeeState && committeeState.failed.length) {
      stage = "COMMITTEE_REPO_FAILED";
      next_action = { type: "question", target_repos: capRepos(committeeState.failed), reason: "COMMITTEE_REPO_FAILED: resolve decision packets and/or rescan" };
    } else if (committeeState && committeeState.missing.length) {
      stage = "COMMITTEE_PENDING";
      next_action = { type: "committee", target_repos: capRepos(committeeState.missing), reason: "COMMITTEE_PENDING: committee outputs missing/stale" };
    } else if (committeeState && committeeState.passed.length === activeRepoIds.length && !committeeState.integration.exists) {
      stage = "COMMITTEE_REPO_PASSED";
      next_action = { type: "committee", target_repos: [], reason: "COMMITTEE_REPO_PASSED: run integration chair" };
    } else if (committeeState && committeeState.integration.exists && committeeState.integration.status.evidence_valid === false) {
      stage = "COMMITTEE_INTEGRATION_FAILED";
      next_action = { type: "question", target_repos: [], reason: "COMMITTEE_INTEGRATION_FAILED: resolve integration decisions" };
    } else if (committeeState && committeeState.integration.exists && committeeState.integration.status.evidence_valid === true) {
      stage = "COMMITTEE_PASSED";
      next_action = { type: "ready", target_repos: [], reason: "COMMITTEE_PASSED: ready for writer" };
    } else {
      stage = "READY_FOR_WRITER";
      next_action = { type: "ready", target_repos: [], reason: "READY_FOR_WRITER: evidence satisfied" };
    }

    // If knowledge sufficiency is not confirmed, recommend (but do not auto-start) a review meeting once committee passes.
    try {
      const suff = await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
      const suffStatus = typeof suff?.sufficiency?.status === "string" ? suff.sufficiency.status : "insufficient";
      if (suffStatus !== "sufficient" && (stage === "COMMITTEE_PASSED" || stage === "READY_FOR_WRITER") && !openDecisions.length) {
        next_action = {
          ...next_action,
          reason: `${next_action.reason} | SUFFICIENCY_RECOMMENDED: run --knowledge-review-meeting --projectRoot ${paths.opsRootAbs} --scope system --start (then declare sufficiency via --knowledge-sufficiency ...)`,
        };
      }
    } catch {
      // ignore (recommendation is optional)
    }

    // Surface a deterministic "resume" state once after decisions are cleared.
    if (!openDecisions.length) {
      let prevStage = null;
      try {
        if (existsSync(stateAbs)) {
          const prev = JSON.parse(String(await readFile(stateAbs, "utf8")) || "");
          prevStage = typeof prev?.stage === "string" ? prev.stage : null;
        }
      } catch {
        prevStage = null;
      }
      if (prevStage === "DECISION_NEEDED" && answeredDecisions.length) {
        stage = "DECISION_ANSWERED";
        next_action = { ...next_action, reason: `DECISION_ANSWERED: ${next_action.reason}` };
      }
    }

    const state = { version: 1, stage, evidence_state: evidenceState, next_action };
    validateLaneAState(state);

    if (!dryRun) {
      await mkdir(paths.laneA.checkpointsAbs, { recursive: true });
      await writeTextAtomic(stateAbs, JSON.stringify(state, null, 2) + "\n");
      await writeTextAtomic(stateMdAbs, renderStateMd(state));
      await writeTextAtomic(hintAbs, JSON.stringify({ version: 1, next_action }, null, 2) + "\n");
      try {
        await rm(errorAbs, { force: true });
      } catch {
        // ignore
      }
    }

    const logs = [];

    // Optional operational hint: if stale and no active update meeting, write one refresh hint file (capped).
    if (!dryRun) {
      try {
        const hintRes = await writeRefreshHintIfNeeded({ paths, registry });
        const escalated = Array.isArray(hintRes?.escalation?.escalated) ? hintRes.escalation.escalated : [];
        if (escalated.length) {
          logs.push({
            executed: "soft_stale_escalation",
            ok: true,
            created_count: escalated.length,
            artifacts: escalated.map((e) => e.artifact).filter(Boolean),
          });
        }
      } catch {
        // ignore hint errors
      }
    }

    // Execute exactly one action when possible (index/scan/refresh). Other actions are scheduled only.
    if (!dryRun) {
      if (next_action.type === "index") {
        const repos = Array.isArray(registry?.repos) ? registry.repos : [];
        const byId = new Map(repos.map((r) => [normStr(r?.repo_id), r]));
        for (const repoId of next_action.target_repos) {
          const cfg = byId.get(repoId);
          if (!cfg) throw new Error(`Unknown repo_id: ${repoId}`);
          const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: cfg.path });
          if (!repoAbs) throw new Error(`Repo ${repoId} missing path.`);
          // eslint-disable-next-line no-await-in-loop
          const res = await runRepoIndex({
            repo_id: repoId,
            repo_path: repoAbs,
            output_dir: join(paths.knowledge.evidenceIndexReposAbs, repoId),
            error_dir_abs: paths.laneA.logsAbs,
            repo_config: cfg,
            dry_run: false,
          });
          logs.push({ executed: "index", repo_id: repoId, ok: res.ok });
          if (!res.ok) return { ok: false, nextAction: next_action, evidenceState, logs, message: "index failed" };
        }
      } else if (next_action.type === "scan") {
        for (const repoId of next_action.target_repos) {
          // eslint-disable-next-line no-await-in-loop
          const res = await runKnowledgeScan({ projectRoot: paths.opsRootAbs, repoId, limit: 1, concurrency: 1, dryRun: false });
          logs.push({ executed: "scan", repo_id: repoId, ok: res.ok });
          if (!res.ok) return { ok: false, nextAction: next_action, evidenceState, logs, message: "scan failed" };
        }
      } else if (next_action.type === "refresh") {
        const res = await runRefreshFromEvents(paths.opsRootAbs, { dryRun: false, maxEvents: max, stopOnError: true });
        logs.push({ executed: "refresh_from_events", ok: res.ok, processed_events: res.report?.processed_events ?? null });
        if (!res.ok) return { ok: false, nextAction: next_action, evidenceState, logs, message: "refresh-from-events failed" };
      }

      try {
        const qaFollowups = await runQaMergeFollowups({ paths, dryRun: false, maxEvents: max });
        logs.push({
          executed: "qa_merge_followups",
          ok: qaFollowups.ok,
          created_count: qaFollowups.created_count,
          merge_events_seen: qaFollowups.merge_events_seen,
        });
      } catch (err) {
        logs.push({
          executed: "qa_merge_followups",
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    return { ok: true, nextAction: next_action, evidenceState, logs };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const errObj = { ok: false, message: msg, stack: err instanceof Error ? String(err.stack || "") : null };
    if (!dryRun) await writeTextAtomic(errorAbs, JSON.stringify(errObj, null, 2) + "\n");
    return { ok: false, message: msg, nextAction: null, evidenceState: null, logs: [] };
  } finally {
    const releaseRes = await releaseOpsLock({
      lockPath,
      owner: { owner_token: lockRes.lock?.owner_token || null },
    });
    if (!releaseRes.ok) {
      // eslint-disable-next-line no-console
      console.error(releaseRes.error || "Failed to release Lane A orchestrator lock.");
    }
  }
}
