import { basename, dirname, resolve } from "node:path";
import { existsSync } from "node:fs";
import { mkdir, rename, writeFile } from "node:fs/promises";

import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../paths/project-paths.js";
import { validatePhaseState } from "../contracts/validators/index.js";
import { readPhaseStateOrDefault, writePhaseState, refreshPhasePrereqs, computeForwardBlockReasons, phasePaths } from "./phase-state.js";
import { runKnowledgeKickoff } from "./knowledge/kickoff-runner.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
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

export async function runKnowledgePhaseStatus({ projectRoot, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const refreshed = await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun });
  return { ok: true, projectRoot: paths.opsRootAbs, phase: refreshed.phase, paths: phasePaths(paths) };
}

export async function runKnowledgeConfirmV1({ projectRoot, by, notes = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const byName = normStr(by);
  if (!byName) return { ok: false, message: "Missing --by \"<name>\"." };

  const refreshed = await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: true });
  const cur = refreshed.phase;
  if (normStr(cur.prereqs.sufficiency) !== "sufficient") {
    return { ok: false, message: "Cannot confirm v1: prereqs.sufficiency is not sufficient.", prereqs: cur.prereqs };
  }

  const next = {
    ...cur,
    prereqs: {
      ...cur.prereqs,
      human_confirmed_v1: true,
      human_confirmed_at: nowISO(),
      human_confirmed_by: byName,
      human_notes: notes != null ? String(notes) : cur.prereqs.human_notes ?? null,
    },
  };
  validatePhaseState(next);
  await writePhaseState({ paths, phase: next, dryRun });
  return { ok: true, projectRoot: paths.opsRootAbs, phase: next, dry_run: !!dryRun };
}

export async function runKnowledgePhaseClose({ projectRoot, phase, by, notes = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const who = normStr(by);
  if (!who) return { ok: false, message: "Missing --by \"<name>\"." };
  const which = normStr(phase).toLowerCase();
  if (!(which === "reverse" || which === "forward")) return { ok: false, message: "Invalid --phase (expected reverse|forward)." };

  const r = await readPhaseStateOrDefault({ projectRoot: paths.opsRootAbs });
  const cur = r.phase;
  const blk = cur[which];
  const next = {
    ...cur,
    current_phase: which,
    [which]: {
      ...blk,
      status: "closed",
      closed_at: nowISO(),
      closed_by: who,
      notes: notes != null ? String(notes) : blk.notes ?? null,
    },
  };
  validatePhaseState(next);
  await writePhaseState({ paths, phase: next, dryRun });
  return { ok: true, projectRoot: paths.opsRootAbs, phase: next, dry_run: !!dryRun };
}

async function writeForwardBlocked({ paths, reasons, message, dryRun }) {
  const pp = phasePaths(paths);
  const payload = { version: 1, reasons: reasons.slice().sort((a, b) => a.localeCompare(b)), message };
  if (!dryRun) await writeJsonAtomic(pp.forwardBlockedAbs, payload);
  return { ok: false, message, blocker: pp.forwardBlockedAbs, reasons: payload.reasons };
}

function extractSessionIdFromKickoffResult(res) {
  if (!res || typeof res !== "object") return null;
  const md = res.latest_files && typeof res.latest_files.md === "string" ? res.latest_files.md : null;
  if (!md) return null;
  const base = basename(md).replace(/\.md$/i, "");
  return base || null;
}

export async function runKnowledgeKickoffReverse({ projectRoot, scope = "system", start = false, cont = false, nonInteractive = false, inputFileAbs = null, sessionText = null, maxQuestions = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const refreshed = await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: true });
  const cur = refreshed.phase;
  const nextPhase = {
    ...cur,
    current_phase: "reverse",
    reverse: {
      ...cur.reverse,
      status: cur.reverse.status === "closed" ? "closed" : "in_progress",
      started_at: cur.reverse.started_at ?? nowISO(),
    },
  };
  validatePhaseState(nextPhase);
  await writePhaseState({ paths, phase: nextPhase, dryRun });

  const kickoff = await runKnowledgeKickoff({
    projectRoot: paths.opsRootAbs,
    scope,
    start,
    cont,
    nonInteractive,
    inputFile: inputFileAbs,
    sessionText,
    maxQuestions,
    dryRun,
    kickoffDirName: "kickoff",
  });

  if (kickoff.ok && (start || !nextPhase.reverse.session_id)) {
    const sid = extractSessionIdFromKickoffResult(kickoff);
    if (sid) {
      const after = { ...nextPhase, reverse: { ...nextPhase.reverse, session_id: nextPhase.reverse.session_id ?? sid } };
      validatePhaseState(after);
      await writePhaseState({ paths, phase: after, dryRun });
      return { ...kickoff, phase: after };
    }
  }

  return { ...kickoff, phase: nextPhase };
}

export async function runKnowledgeKickoffForward({ projectRoot, scope = "system", start = false, cont = false, nonInteractive = false, inputFileAbs = null, sessionText = null, maxQuestions = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const refreshed = await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: true });
  const cur = refreshed.phase;

  const reasons = computeForwardBlockReasons(cur);
  if (reasons.length) {
    return await writeForwardBlocked({
      paths,
      reasons,
      message:
        "Forward kickoff blocked. Prereqs required: reverse.status=closed, prereqs.scan_complete=true, prereqs.sufficiency=sufficient, prereqs.human_confirmed_v1=true.",
      dryRun,
    });
  }

  const nextPhase = {
    ...cur,
    current_phase: "forward",
    forward: {
      ...cur.forward,
      status: cur.forward.status === "closed" ? "closed" : "in_progress",
      started_at: cur.forward.started_at ?? nowISO(),
    },
  };
  validatePhaseState(nextPhase);
  await writePhaseState({ paths, phase: nextPhase, dryRun });

  const kickoff = await runKnowledgeKickoff({
    projectRoot: paths.opsRootAbs,
    scope,
    start,
    cont,
    nonInteractive,
    inputFile: inputFileAbs,
    sessionText,
    maxQuestions,
    dryRun,
    kickoffDirName: "kickoff_forward",
  });

  if (kickoff.ok && (start || !nextPhase.forward.session_id)) {
    const sid = extractSessionIdFromKickoffResult(kickoff);
    if (sid) {
      const after = { ...nextPhase, forward: { ...nextPhase.forward, session_id: nextPhase.forward.session_id ?? sid } };
      validatePhaseState(after);
      await writePhaseState({ paths, phase: after, dryRun });
      return { ...kickoff, phase: after };
    }
  }

  return { ...kickoff, phase: nextPhase };
}
