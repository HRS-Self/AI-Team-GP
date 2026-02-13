import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";

import { buildLaneAHealthPayload } from "./lane-a-health.js";
import { getProject as getRegistryProject, listProjects, loadRegistry } from "../registry/project-registry.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { computeForwardBlockReasons, readPhaseStateOrDefault } from "../lane_a/phase-state.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function toIsoOrNull(value) {
  const s = normStr(value);
  if (!s) return null;
  const direct = Date.parse(s);
  if (Number.isFinite(direct)) return new Date(direct).toISOString();
  const fsSafe = /^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(\d{3})$/.exec(s);
  if (!fsSafe) return null;
  const ms = Date.UTC(
    Number(fsSafe[1]),
    Number(fsSafe[2]) - 1,
    Number(fsSafe[3]),
    Number(fsSafe[4]),
    Number(fsSafe[5]),
    Number(fsSafe[6]),
    Number(fsSafe[7]),
  );
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

async function readJsonOptional(absPath) {
  if (!absPath || !existsSync(absPath)) return null;
  try {
    const raw = await readFile(absPath, "utf8");
    const parsed = JSON.parse(String(raw || ""));
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function listNames(dirAbs, { filesOnly = true, pattern = null, dirsOnly = false } = {}) {
  if (!dirAbs || !existsSync(dirAbs)) return [];
  try {
    const entries = await readdir(dirAbs, { withFileTypes: true });
    return entries
      .filter((entry) => (filesOnly ? entry.isFile() : true))
      .filter((entry) => (dirsOnly ? entry.isDirectory() : true))
      .map((entry) => entry.name)
      .filter((name) => (pattern ? pattern.test(name) : true))
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

function defaultOverviewPayload() {
  return {
    version: 1,
    generated_at: nowISO(),
    laneA: {
      health: {
        hard_stale: false,
        stale: false,
        degraded: false,
        last_scan: null,
        last_merge_event: null,
      },
      phases: {
        reverse: { status: "pending", message: "Phase state unavailable." },
        sufficiency: { status: "pending", message: "Sufficiency state unavailable." },
        forward: { status: "pending", message: "Phase state unavailable." },
      },
      repos: [],
    },
    laneB: {
      inbox_count: 0,
      triage_count: 0,
      active_work: [],
      watchdog_status: {
        last_action: null,
        last_event_at: null,
        last_started_at: null,
        last_finished_at: null,
        last_failed_at: null,
        last_work_id: null,
      },
    },
  };
}

function phaseStatus(status, message) {
  const s = normStr(status);
  const normalized = s === "ok" || s === "blocked" ? s : "pending";
  return { status: normalized, message: normStr(message) || "" };
}

function normalizeArtifact(item) {
  if (!item || typeof item !== "object") return null;
  const name = normStr(item.name);
  const url = normStr(item.url);
  if (!name || !url) return null;
  return { name, url };
}

function normalizeCommitteeStatus(raw, repoId) {
  const src = isPlainObject(raw) ? raw : {};
  const staleness = isPlainObject(src.staleness) ? src.staleness : {};
  const nextAction = isPlainObject(src.next_action) ? src.next_action : {};
  return {
    repo_id: normStr(src.repo_id) || repoId,
    evidence_valid: src.evidence_valid === true,
    stale: src.stale === true || staleness.stale === true,
    hard_stale: src.hard_stale === true || staleness.hard_stale === true,
    degraded: src.degraded === true || normStr(src.degraded_reason) === "soft_stale",
    degraded_reason: normStr(src.degraded_reason) || null,
    next_action: {
      type: normStr(nextAction.type || nextAction.action) || null,
      reason: normStr(nextAction.reason || nextAction.message) || null,
    },
  };
}

function toCoveragePercent({ hasIndex, hasScan }) {
  if (hasIndex && hasScan) return "100%";
  if (hasIndex || hasScan) return "50%";
  return "0%";
}

async function latestScanIso(paths, repoIds) {
  const times = [];
  for (const repoId of repoIds) {
    const scanAbs = join(paths.knowledge.ssotReposAbs, repoId, "scan.json");
    // eslint-disable-next-line no-await-in-loop
    const json = await readJsonOptional(scanAbs);
    const iso = toIsoOrNull(json?.scanned_at);
    if (iso) times.push(iso);
  }
  if (!times.length) return null;
  return times
    .slice()
    .sort((a, b) => (Date.parse(b) || 0) - (Date.parse(a) || 0))[0];
}

async function latestMergeEventIso(paths) {
  const files = await listNames(paths.laneA.eventsSegmentsAbs, {
    pattern: /^(\d{8}-\d{6}|events-\d{8}-\d{2})\.jsonl$/,
  });
  const recent = files.slice(Math.max(0, files.length - 48));
  let latest = null;

  for (let idx = recent.length - 1; idx >= 0; idx -= 1) {
    const abs = join(paths.laneA.eventsSegmentsAbs, recent[idx]);
    // eslint-disable-next-line no-await-in-loop
    const text = existsSync(abs) ? await readFile(abs, "utf8").catch(() => "") : "";
    const lines = String(text || "")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);

    for (let li = lines.length - 1; li >= 0; li -= 1) {
      try {
        const parsed = JSON.parse(lines[li]);
        if (normStr(parsed.type) !== "merge") continue;
        const iso = toIsoOrNull(parsed.timestamp);
        if (!iso) continue;
        if (!latest || Date.parse(iso) > Date.parse(latest)) latest = iso;
      } catch {
        // ignore malformed line
      }
    }
  }
  return latest;
}

function findProjectByOpsRoot(registry, projectRootHint) {
  const hint = normStr(projectRootHint);
  if (!hint) return null;
  const hintAbs = resolve(hint);
  const all = listProjects(registry)
    .filter((project) => normStr(project.status).toLowerCase() === "active")
    .map((project) => getRegistryProject(registry, project.project_code))
    .filter(Boolean);
  for (const project of all) {
    const ops = normStr(project.ops_dir) || (normStr(project.root_dir) ? resolve(project.root_dir, "ops") : "");
    if (ops && resolve(ops) === hintAbs) return project;
  }
  return null;
}

async function resolveProjectContext({ engineRoot, projectCode, projectRootHint }) {
  const regRes = await loadRegistry({ toolRepoRoot: engineRoot, createIfMissing: true });
  const registry = regRes.registry;
  const requestedCode = normStr(projectCode);

  let project = null;
  if (requestedCode) {
    const p = getRegistryProject(registry, requestedCode);
    if (p && normStr(p.status).toLowerCase() === "active") project = p;
  } else {
    project = findProjectByOpsRoot(registry, projectRootHint);
    if (!project) {
      project = listProjects(registry)
        .filter((p) => normStr(p.status).toLowerCase() === "active")
        .map((p) => getRegistryProject(registry, p.project_code))
        .filter(Boolean)
        .sort((a, b) => normStr(a.project_code).localeCompare(normStr(b.project_code)))[0] || null;
    }
  }

  if (!project) return { project: null, opsRoot: null, projectCode: null };
  const opsRoot = normStr(project.ops_dir) || (normStr(project.root_dir) ? resolve(project.root_dir, "ops") : "");
  if (!opsRoot || !isAbsolute(opsRoot)) return { project: null, opsRoot: null, projectCode: null };
  return { project, opsRoot: resolve(opsRoot), projectCode: normStr(project.project_code) };
}

async function collectRepoIds({ project, paths, laneAProject }) {
  const ids = new Set();
  const fromRegistry = Array.isArray(project?.repos) ? project.repos : [];
  for (const repo of fromRegistry) {
    const repoId = normStr(repo?.repo_id);
    if (repoId) ids.add(repoId);
  }

  const reposCfg = await readJsonOptional(join(paths.opsConfigAbs, "REPOS.json"));
  const repos = Array.isArray(reposCfg?.repos) ? reposCfg.repos : [];
  for (const repo of repos) {
    const repoId = normStr(repo?.repo_id);
    if (repoId) ids.add(repoId);
  }

  const scopes = Array.isArray(laneAProject?.scopes) ? laneAProject.scopes : [];
  for (const scope of scopes) {
    const s = normStr(scope?.scope);
    if (s.startsWith("repo:")) {
      const repoId = normStr(s.slice("repo:".length));
      if (repoId) ids.add(repoId);
    }
  }
  return Array.from(ids).sort((a, b) => a.localeCompare(b));
}

async function collectLaneAPhases(paths) {
  let phase = null;
  try {
    const state = await readPhaseStateOrDefault({ projectRoot: paths.opsRootAbs });
    phase = isPlainObject(state?.phase) ? state.phase : null;
  } catch {
    phase = null;
  }
  if (!phase) {
    return {
      reverse: phaseStatus("pending", "Reverse phase has not started."),
      sufficiency: phaseStatus("pending", "Sufficiency is unknown."),
      forward: phaseStatus("pending", "Forward phase has not started."),
    };
  }

  const reverseToken = normStr(phase?.reverse?.status);
  const reverse =
    reverseToken === "closed"
      ? phaseStatus("ok", "Reverse phase closed.")
      : reverseToken === "in_progress" || reverseToken === "started"
        ? phaseStatus("pending", "Reverse phase is in progress.")
        : phaseStatus("pending", "Reverse phase is not closed.");

  const suffToken = normStr(phase?.prereqs?.sufficiency);
  const scanComplete = phase?.prereqs?.scan_complete === true;
  const sufficiency =
    suffToken === "sufficient"
      ? phaseStatus("ok", "Sufficiency is sufficient.")
      : scanComplete !== true
        ? phaseStatus("blocked", "Sufficiency is blocked until scan coverage is complete.")
        : phaseStatus("pending", "Sufficiency is not sufficient.");

  const forwardToken = normStr(phase?.forward?.status);
  let forward = null;
  if (forwardToken === "closed") {
    forward = phaseStatus("ok", "Forward phase closed.");
  } else {
    const reasons = computeForwardBlockReasons(phase);
    forward =
      reasons.length > 0
        ? phaseStatus("blocked", `Forward blocked: ${reasons.join(", ")}.`)
        : phaseStatus("pending", "Forward phase is not closed.");
  }

  return { reverse, sufficiency, forward };
}

function buildLatestArtifacts(scope) {
  const artifacts = isPlainObject(scope?.artifacts) ? scope.artifacts : {};
  return {
    refresh_hint: normalizeArtifact(artifacts.latest_refresh_hint),
    decision_packet: normalizeArtifact(artifacts.latest_decision_packet),
    update_meeting: normalizeArtifact(artifacts.latest_update_meeting),
    review_meeting: normalizeArtifact(artifacts.latest_review_meeting),
    committee_report: normalizeArtifact(artifacts.latest_committee_status),
    writer_report: normalizeArtifact(artifacts.latest_writer_status),
  };
}

async function collectLaneBOverview(paths) {
  const inbox_count = (await listNames(paths.laneB.inboxAbs, { pattern: /^I-.*\.(md|json)$/ })).length;
  const triage_count = (await listNames(paths.laneB.triageAbs, { pattern: /^T-.*\.json$/ })).length;

  const workIds = await listNames(paths.laneB.workAbs, { filesOnly: false, dirsOnly: true });
  const active_work = [];
  for (const workId of workIds) {
    const statusJson = await readJsonOptional(join(paths.laneB.workAbs, workId, "status.json"));
    const stage = normStr(statusJson?.stage);
    const terminal = new Set(["DONE", "MERGED", "COMPLETED"]);
    if (!stage || terminal.has(stage)) continue;
    active_work.push({
      work_id: workId,
      current_stage: stage || "UNKNOWN",
      blocked: statusJson?.blocked === true,
      updated_at: toIsoOrNull(statusJson?.updated_at) || null,
    });
  }
  active_work.sort((a, b) => (Date.parse(String(b.updated_at || "")) || 0) - (Date.parse(String(a.updated_at || "")) || 0));

  const watchdog_status = {
    last_action: null,
    last_event_at: null,
    last_started_at: null,
    last_finished_at: null,
    last_failed_at: null,
    last_work_id: null,
  };

  if (existsSync(paths.laneB.ledgerAbs)) {
    const text = await readFile(paths.laneB.ledgerAbs, "utf8").catch(() => "");
    const lines = String(text || "")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
    for (let idx = Math.max(0, lines.length - 1500); idx < lines.length; idx += 1) {
      try {
        const parsed = JSON.parse(lines[idx]);
        const action = normStr(parsed.action);
        if (!action.startsWith("watchdog_")) continue;
        const ts = toIsoOrNull(parsed.timestamp) || watchdog_status.last_event_at;
        watchdog_status.last_action = action;
        watchdog_status.last_event_at = ts;
        if (normStr(parsed.workId)) watchdog_status.last_work_id = normStr(parsed.workId);
        if (action === "watchdog_started") watchdog_status.last_started_at = ts;
        if (action === "watchdog_finished") watchdog_status.last_finished_at = ts;
        if (action === "watchdog_failed") watchdog_status.last_failed_at = ts;
      } catch {
        // ignore malformed ledger line
      }
    }
  }

  return { inbox_count, triage_count, active_work, watchdog_status };
}

function assertPhaseObject(value, name) {
  if (!isPlainObject(value)) throw new Error(`Invalid ${name} object.`);
  const status = normStr(value.status);
  if (!(status === "ok" || status === "pending" || status === "blocked")) throw new Error(`Invalid ${name}.status.`);
  if (typeof value.message !== "string") throw new Error(`Invalid ${name}.message.`);
}

function validateStatusOverviewPayload(payload) {
  if (!isPlainObject(payload)) throw new Error("Invalid payload object.");
  if (payload.version !== 1) throw new Error("Invalid payload.version.");
  if (!normStr(payload.generated_at)) throw new Error("Missing payload.generated_at.");
  if (!isPlainObject(payload.laneA)) throw new Error("Missing payload.laneA.");
  if (!isPlainObject(payload.laneB)) throw new Error("Missing payload.laneB.");

  const health = payload.laneA.health;
  if (!isPlainObject(health)) throw new Error("Missing laneA.health.");
  for (const key of ["hard_stale", "stale", "degraded"]) {
    if (typeof health[key] !== "boolean") throw new Error(`Invalid laneA.health.${key}.`);
  }
  for (const key of ["last_scan", "last_merge_event"]) {
    if (!(health[key] === null || typeof health[key] === "string")) throw new Error(`Invalid laneA.health.${key}.`);
  }

  if (!isPlainObject(payload.laneA.phases)) throw new Error("Missing laneA.phases.");
  assertPhaseObject(payload.laneA.phases.reverse, "laneA.phases.reverse");
  assertPhaseObject(payload.laneA.phases.sufficiency, "laneA.phases.sufficiency");
  assertPhaseObject(payload.laneA.phases.forward, "laneA.phases.forward");

  if (!Array.isArray(payload.laneA.repos)) throw new Error("Invalid laneA.repos.");
  for (const repo of payload.laneA.repos) {
    if (!isPlainObject(repo)) throw new Error("Invalid laneA.repos[] entry.");
    if (!normStr(repo.repo_id)) throw new Error("Missing laneA.repos[].repo_id.");
    if (typeof repo.coverage !== "string") throw new Error("Invalid laneA.repos[].coverage.");
    for (const key of ["stale", "hard_stale", "degraded"]) {
      if (typeof repo[key] !== "boolean") throw new Error(`Invalid laneA.repos[].${key}.`);
    }
    if (!isPlainObject(repo.committee_status)) throw new Error("Invalid laneA.repos[].committee_status.");
    if (!isPlainObject(repo.latest_artifacts)) throw new Error("Invalid laneA.repos[].latest_artifacts.");
    for (const key of ["refresh_hint", "decision_packet", "update_meeting", "review_meeting", "committee_report", "writer_report"]) {
      const art = repo.latest_artifacts[key];
      if (!(art === null || (isPlainObject(art) && normStr(art.name) && normStr(art.url)))) {
        throw new Error(`Invalid laneA.repos[].latest_artifacts.${key}.`);
      }
    }
  }

  if (!Number.isInteger(payload.laneB.inbox_count) || payload.laneB.inbox_count < 0) throw new Error("Invalid laneB.inbox_count.");
  if (!Number.isInteger(payload.laneB.triage_count) || payload.laneB.triage_count < 0) throw new Error("Invalid laneB.triage_count.");
  if (!Array.isArray(payload.laneB.active_work)) throw new Error("Invalid laneB.active_work.");
  if (!isPlainObject(payload.laneB.watchdog_status)) throw new Error("Invalid laneB.watchdog_status.");
}

export async function buildStatusOverviewPayload({
  engineRoot,
  projectCode = "",
  projectRootHint = "",
} = {}) {
  const root = resolve(String(engineRoot || process.cwd()));
  const payload = defaultOverviewPayload();

  const context = await resolveProjectContext({ engineRoot: root, projectCode, projectRootHint });
  if (!context.project || !context.opsRoot || !context.projectCode) {
    validateStatusOverviewPayload(payload);
    return payload;
  }

  let paths = null;
  try {
    paths = await loadProjectPaths({ projectRoot: context.opsRoot });
  } catch {
    validateStatusOverviewPayload(payload);
    return payload;
  }

  const laneAHealthPayload = await buildLaneAHealthPayload({ engineRoot: root, projectCode: context.projectCode });
  const laneAProject = (Array.isArray(laneAHealthPayload?.projects) ? laneAHealthPayload.projects : [])[0] || null;
  const phases = await collectLaneAPhases(paths);
  const repoIds = await collectRepoIds({ project: context.project, paths, laneAProject });

  const laneAScopesByRepo = new Map();
  const scopes = Array.isArray(laneAProject?.scopes) ? laneAProject.scopes : [];
  for (const scope of scopes) {
    const label = normStr(scope?.scope);
    if (label.startsWith("repo:")) laneAScopesByRepo.set(label.slice("repo:".length), scope);
  }

  const repos = [];
  for (const repoId of repoIds) {
    const scope = laneAScopesByRepo.get(repoId) || {};
    const hasIndex = existsSync(join(paths.knowledge.evidenceIndexReposAbs, repoId, "repo_index.json"));
    const hasScan = existsSync(join(paths.knowledge.ssotReposAbs, repoId, "scan.json"));
    // eslint-disable-next-line no-await-in-loop
    const committeeRaw = await readJsonOptional(join(paths.knowledge.ssotReposAbs, repoId, "committee", "committee_status.json"));
    repos.push({
      repo_id: repoId,
      coverage: toCoveragePercent({ hasIndex, hasScan }),
      stale: scope?.stale === true,
      hard_stale: scope?.hard_stale === true,
      degraded: scope?.degraded === true,
      committee_status: normalizeCommitteeStatus(committeeRaw, repoId),
      latest_artifacts: buildLatestArtifacts(scope),
    });
  }

  payload.laneA = {
    health: {
      hard_stale: laneAProject?.summary?.hard_stale === true,
      stale: laneAProject?.summary?.stale === true,
      degraded: laneAProject?.summary?.degraded === true,
      last_scan: await latestScanIso(paths, repoIds),
      last_merge_event: await latestMergeEventIso(paths),
    },
    phases,
    repos,
  };

  payload.laneB = await collectLaneBOverview(paths);
  payload.generated_at = nowISO();

  validateStatusOverviewPayload(payload);
  return payload;
}

export function registerStatusOverviewRoutes(app, { engineRoot, authMiddleware = null, projectRootHint = "" } = {}) {
  const middleware = typeof authMiddleware === "function" ? authMiddleware : (_req, _res, next) => next();
  app.get("/api/status-overview", middleware, async (req, res) => {
    const projectCode = typeof req.query?.project === "string" ? req.query.project.trim() : "";
    try {
      const payload = await buildStatusOverviewPayload({ engineRoot, projectCode, projectRootHint });
      return res.status(200).json(payload);
    } catch (err) {
      return res.status(500).json({ ok: false, message: err instanceof Error ? err.message : "status overview failed" });
    }
  });
}
