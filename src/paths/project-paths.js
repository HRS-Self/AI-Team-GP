import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

import { readProjectConfig } from "../project/project-config.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function requireBasename(absPath, expectedBase, nameForError) {
  const b = basename(String(absPath || ""));
  if (b !== expectedBase) {
    throw new Error(`${nameForError} must end with '/${expectedBase}' (got: ${absPath}).`);
  }
}

function requireAbsolutePath(p, name) {
  const s = normStr(p);
  if (!s) throw new Error(`Missing ${name}.`);
  if (!isAbsolute(s)) throw new Error(`${name} must be an absolute path.`);
  return resolve(s);
}

export function resolveOpsRootAbs({ projectRoot = null, required = true } = {}) {
  const fromArg = normStr(projectRoot);
  const fromEnv = normStr(process.env.AI_PROJECT_ROOT);
  const raw = fromArg || fromEnv;
  if (!raw) {
    if (!required) return null;
    throw new Error("Missing AI_PROJECT_ROOT. Set env var AI_PROJECT_ROOT to OPS_ROOT (absolute path, e.g. /path/to/projects/<code>/ops).");
  }
  if (!isAbsolute(raw)) throw new Error(`AI_PROJECT_ROOT must be an absolute path (got: ${raw}).`);
  const abs = resolve(raw);
  // Reject common misconfiguration: pointing at OPS_ROOT/ai instead of OPS_ROOT.
  if (abs.endsWith("/ops/ai") || abs.endsWith("\\ops\\ai")) {
    throw new Error(
      `Invalid AI_PROJECT_ROOT=${abs}.\n` +
        "AI_PROJECT_ROOT must point to OPS_ROOT (the folder that contains 'ai/' and 'config/'), not to OPS_ROOT/ai.",
    );
  }
  requireBasename(abs, "ops", "AI_PROJECT_ROOT");
  return abs;
}

export async function loadProjectPaths({ projectRoot = null } = {}) {
  const opsRootAbs = resolveOpsRootAbs({ projectRoot, required: true });

  const cfgRes = await readProjectConfig({ projectRoot: opsRootAbs });
  if (!cfgRes.ok) throw new Error(cfgRes.message);
  const cfg = cfgRes.config;

  const reposRootAbs = requireAbsolutePath(cfg.repos_root_abs, "config/PROJECT.json.repos_root_abs");
  const opsRootFromCfg = requireAbsolutePath(cfg.ops_root_abs, "config/PROJECT.json.ops_root_abs");
  const knowledgeRootAbs = requireAbsolutePath(cfg.knowledge_repo_dir, "config/PROJECT.json.knowledge_repo_dir");
  requireBasename(reposRootAbs, "repos", "config/PROJECT.json.repos_root_abs");
  requireBasename(opsRootFromCfg, "ops", "config/PROJECT.json.ops_root_abs");
  requireBasename(knowledgeRootAbs, "knowledge", "config/PROJECT.json.knowledge_repo_dir");

  if (opsRootFromCfg !== opsRootAbs) {
    throw new Error(`OPS root mismatch.\nAI_PROJECT_ROOT=${opsRootAbs}\nconfig/PROJECT.json.ops_root_abs=${opsRootFromCfg}\nFix config/PROJECT.json or AI_PROJECT_ROOT.`);
  }

  const projectHomeAbs = dirname(opsRootAbs);

  const opsConfigAbs = join(opsRootAbs, "config");
  const opsAiAbs = join(opsRootAbs, "ai");
  const laneAAbs = join(opsAiAbs, "lane_a");
  const laneBAbs = join(opsAiAbs, "lane_b");

  const laneAMeetingsAbs = join(laneAAbs, "meetings");
  const laneAMeetingsUpdateAbs = join(laneAMeetingsAbs, "update");

  const paths = {
    version: 1,
    cfg,
    projectHomeAbs,
    opsRootAbs,
    opsConfigAbs,
    reposRootAbs,
    knowledgeRootAbs,
    opsAiAbs,
    laneA: {
      rootAbs: laneAAbs,
      locksAbs: join(laneAAbs, "locks"),
      locksStatusAbs: join(laneAAbs, "locks", "status"),
      lockPathAbs: join(laneAAbs, "locks", "lane-a-orchestrate.lock.json"),
      stalenessAbs: join(laneAAbs, "staleness"),
      softStaleTrackerAbs: join(laneAAbs, "staleness", "soft_stale_tracker.json"),
      decisionPacketsAbs: join(laneAAbs, "decision_packets"),
      logsAbs: join(laneAAbs, "logs"),
      scansRawAbs: join(laneAAbs, "scans_raw"),
      evidenceRawAbs: join(laneAAbs, "evidence_raw"),
      synthRawAbs: join(laneAAbs, "synth_raw"),
      decisionsNeededAbs: join(laneAAbs, "decisions_needed"),
      blockersAbs: join(laneAAbs, "blockers"),
      checkpointsAbs: join(laneAAbs, "checkpoints"),
      stateAbs: join(laneAAbs, "state"),
      phasesAbs: join(laneAAbs, "phases"),
      meetingsAbs: laneAMeetingsAbs,
      meetingsUpdateAbs: laneAMeetingsUpdateAbs,
      sufficiencyAbs: join(laneAAbs, "sufficiency"),
      sufficiencyHistoryAbs: join(laneAAbs, "sufficiency", "history"),
      refreshHintsAbs: join(laneAAbs, "refresh_hints"),
      eventsAbs: join(laneAAbs, "events"),
      eventsSegmentsAbs: join(laneAAbs, "events", "segments"),
      eventsCheckpointsAbs: join(laneAAbs, "events", "checkpoints"),
      eventsSummaryAbs: join(laneAAbs, "events", "summary"),
      ledgerAbs: join(laneAAbs, "ledger.jsonl"),
    },
    laneB: {
      rootAbs: laneBAbs,
      logsAbs: join(laneBAbs, "logs"),
      workAbs: join(laneBAbs, "work"),
      inboxAbs: join(laneBAbs, "inbox"),
      triageAbs: join(laneBAbs, "triage"),
      approvalsAbs: join(laneBAbs, "approvals"),
      scheduleAbs: join(laneBAbs, "schedule"),
      cacheAbs: join(laneBAbs, "cache"),
      ledgerAbs: join(laneBAbs, "ledger.jsonl"),
    },
    knowledge: {
      rootAbs: knowledgeRootAbs,
      ssotAbs: join(knowledgeRootAbs, "ssot"),
      ssotSystemAbs: join(knowledgeRootAbs, "ssot", "system"),
      ssotReposAbs: join(knowledgeRootAbs, "ssot", "repos"),
      evidenceAbs: join(knowledgeRootAbs, "evidence"),
      evidenceSystemAbs: join(knowledgeRootAbs, "evidence", "system"),
      evidenceReposAbs: join(knowledgeRootAbs, "evidence", "repos"),
      evidenceIndexAbs: join(knowledgeRootAbs, "evidence", "index"),
      evidenceIndexReposAbs: join(knowledgeRootAbs, "evidence", "index", "repos"),
      viewsAbs: join(knowledgeRootAbs, "views"),
      viewsTeamsAbs: join(knowledgeRootAbs, "views", "teams"),
      viewsReposAbs: join(knowledgeRootAbs, "views", "repos"),
      docsAbs: join(knowledgeRootAbs, "docs"),
      sessionsAbs: join(knowledgeRootAbs, "sessions"),
      decisionsAbs: join(knowledgeRootAbs, "decisions"),
      eventsAbs: join(knowledgeRootAbs, "events"),
      eventsSummaryAbs: join(knowledgeRootAbs, "events", "summary.json"),
    },
  };

  assertNoCrossLanePathOverlap(paths.laneA.rootAbs, paths.laneB.rootAbs);
  assertNoCrossLanePathOverlap(paths.laneA.logsAbs, paths.laneB.logsAbs);
  assertNoCrossLanePathOverlap(paths.laneA.locksAbs, paths.laneB.rootAbs);

  return paths;
}

export async function ensureLaneADirs({ projectRoot = null } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  const dirs = [
    p.laneA.locksAbs,
    p.laneA.locksStatusAbs,
    p.laneA.stalenessAbs,
    p.laneA.decisionPacketsAbs,
    p.laneA.logsAbs,
    p.laneA.scansRawAbs,
    p.laneA.evidenceRawAbs,
    p.laneA.synthRawAbs,
    p.laneA.decisionsNeededAbs,
    p.laneA.blockersAbs,
    p.laneA.checkpointsAbs,
    p.laneA.stateAbs,
    p.laneA.phasesAbs,
    p.laneA.meetingsAbs,
    p.laneA.meetingsUpdateAbs,
    p.laneA.sufficiencyAbs,
    p.laneA.sufficiencyHistoryAbs,
    p.laneA.refreshHintsAbs,
    p.laneA.eventsSegmentsAbs,
    p.laneA.eventsCheckpointsAbs,
    p.laneA.eventsSummaryAbs,
  ];
  for (const d of dirs) {
    // eslint-disable-next-line no-await-in-loop
    await mkdir(d, { recursive: true });
  }
  return { ok: true };
}

export async function laneALockDir({ projectRoot = null } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  return p.laneA.locksAbs;
}

export async function laneALockPath({ projectRoot = null } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  return p.laneA.lockPathAbs;
}

export async function laneALockStatusDir({ projectRoot = null } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  return p.laneA.locksStatusAbs;
}

export async function ensureLaneBDirs({ projectRoot = null } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  const dirs = [
    p.laneB.logsAbs,
    p.laneB.workAbs,
    p.laneB.inboxAbs,
    p.laneB.triageAbs,
    p.laneB.approvalsAbs,
    p.laneB.scheduleAbs,
    p.laneB.cacheAbs,
  ];
  for (const d of dirs) {
    // eslint-disable-next-line no-await-in-loop
    await mkdir(d, { recursive: true });
  }
  return { ok: true };
}

export async function ensureKnowledgeDirs({ projectRoot = null } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  const dirs = [
    p.knowledge.ssotSystemAbs,
    p.knowledge.ssotReposAbs,
    p.knowledge.evidenceIndexReposAbs,
    p.knowledge.evidenceSystemAbs,
    p.knowledge.evidenceReposAbs,
    p.knowledge.viewsTeamsAbs,
    p.knowledge.viewsReposAbs,
    join(p.knowledge.viewsAbs, "system"),
    p.knowledge.docsAbs,
    p.knowledge.sessionsAbs,
    p.knowledge.decisionsAbs,
    p.knowledge.eventsAbs,
  ];
  for (const d of dirs) {
    // eslint-disable-next-line no-await-in-loop
    await mkdir(d, { recursive: true });
  }
  // Ensure the compact summary path parent exists; file is optional.
  if (!existsSync(p.knowledge.eventsAbs)) await mkdir(p.knowledge.eventsAbs, { recursive: true });
  return { ok: true };
}

export function assertNoCrossLanePathOverlap(aAbs, bAbs) {
  const a = resolve(String(aAbs || ""));
  const b = resolve(String(bAbs || ""));
  if (!a || !b) return;
  if (a === b) throw new Error(`Ops isolation violation: same path used by both lanes: ${a}`);
  const isPrefix = (p, x) => x === p || x.startsWith(`${p}/`) || x.startsWith(`${p}\\`);
  if (isPrefix(a, b) || isPrefix(b, a)) throw new Error(`Ops isolation violation: overlapping paths: ${a} vs ${b}`);
}
