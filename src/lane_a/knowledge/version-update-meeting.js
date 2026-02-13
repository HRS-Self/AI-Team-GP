import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, rename, writeFile, readFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { validateUpdateMeeting } from "../../contracts/validators/index.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { evaluateScopeStaleness } from "../lane-a-staleness-policy.js";
import { loadRepoRegistry } from "../../utils/repo-registry.js";
import { runKnowledgeStatus } from "./knowledge-status.js";
import { runKnowledgeCommittee } from "./committee-runner.js";
import { runKnowledgeSufficiencyApprove, runKnowledgeSufficiencyReject } from "./knowledge-sufficiency.js";
import { bumpVersion, readKnowledgeVersionOrDefault, setKnowledgeVersionExplicit } from "./knowledge-version.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function requireAbsOpsRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

function requireScope(scope) {
  const s = normStr(scope);
  if (!s) throw new Error("Missing --scope.");
  if (s === "system") return "system";
  if (/^repo:[A-Za-z0-9._-]+$/.test(s)) return s;
  throw new Error("Invalid --scope. Expected system|repo:<id>.");
}

function requireVersionLike(v, name) {
  const s = normStr(v);
  if (!s) throw new Error(`Missing --${name}.`);
  if (!/^v\d+(\.\d+)*$/.test(s)) throw new Error(`Invalid --${name} (expected v<major>[.<minor>[.<patch>...]]).`);
  return s;
}

function scopeSlug(scope) {
  if (scope === "system") return "system";
  const id = scope.slice("repo:".length);
  return `repo-${id.replace(/[^A-Za-z0-9_.-]/g, "_")}`;
}

function meetingId({ scope, fromVersion, toVersion }) {
  const ts = nowFsSafeUtcTimestamp();
  const slug = scopeSlug(scope);
  const h8 = sha256Hex(`${scope}\n${fromVersion}\n${toVersion}\n${ts}`).slice(0, 8);
  return `UM-${ts}__${slug}__${fromVersion}__${toVersion}__${h8}`;
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

async function readJsonAbs(absPath) {
  const t = await readFile(resolve(String(absPath || "")), "utf8");
  return JSON.parse(String(t || ""));
}

function meetingDirs(paths) {
  const updateAbs = resolve(paths.laneA.meetingsUpdateAbs);
  const knowledgeUpdateAbs = resolve(paths.knowledge.decisionsAbs, "meetings", "update");
  return { updateAbs, knowledgeUpdateAbs };
}

function meetingPaths({ baseAbs, meeting_id }) {
  const dirAbs = resolve(baseAbs, meeting_id);
  return {
    dirAbs,
    jsonAbs: join(dirAbs, `UPDATE_MEETING-${meeting_id}.json`),
    transcriptAbs: join(dirAbs, "transcript"),
    questionsJsonlAbs: join(dirAbs, "transcript", "QUESTIONS.jsonl"),
    answersJsonlAbs: join(dirAbs, "transcript", "ANSWERS.jsonl"),
    statusAbs: join(baseAbs, "status.json"),
  };
}

async function readStatusIndexOptional({ statusAbs }) {
  if (!existsSync(statusAbs)) return { ok: true, exists: false, index: { version: 1, updated_at: null, latest_by_scope: {} } };
  const j = JSON.parse(String(readFileSync(statusAbs, "utf8") || ""));
  if (!j || typeof j !== "object" || j.version !== 1 || !j.latest_by_scope || typeof j.latest_by_scope !== "object") {
    return { ok: false, message: `Invalid status.json shape: ${statusAbs}` };
  }
  return { ok: true, exists: true, index: j };
}

async function writeStatusIndex({ statusAbs, index, dryRun }) {
  const next = { version: 1, updated_at: nowISO(), latest_by_scope: index.latest_by_scope || {} };
  if (!dryRun) await writeJsonAtomic(statusAbs, next);
  return { ok: true, wrote: !dryRun, index: next };
}

function listUnanswered(meeting) {
  const asked = Array.isArray(meeting.asked) ? meeting.asked : [];
  const answers = Array.isArray(meeting.answers) ? meeting.answers : [];
  return asked.filter((q) => !answers.some((a) => a.qid === q.qid));
}

async function openDecisionCountForScope({ decisionsDirAbs, scope }) {
  const dirAbs = resolve(String(decisionsDirAbs || ""));
  if (!existsSync(dirAbs)) return 0;
  const entries = await readdir(dirAbs, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"))
    .map((e) => join(dirAbs, e.name))
    .sort((a, b) => a.localeCompare(b));
  let count = 0;
  for (const abs of files) {
    try {
      // eslint-disable-next-line no-await-in-loop
      const j = JSON.parse(String(await readFile(abs, "utf8") || ""));
      const s = normStr(j?.scope);
      const st = normStr(j?.status);
      if (st !== "open") continue;
      if (scope === "system") {
        if (s === "system") count += 1;
      } else if (s === scope) {
        count += 1;
      }
    } catch {
      // ignore invalid packets
    }
  }
  return count;
}

async function coverageCompleteForScope({ projectRootAbs, scope }) {
  const st = await runKnowledgeStatus({ projectRoot: projectRootAbs, dryRun: true });
  if (!st.ok) return { ok: false, complete: false, message: st.message || "knowledge-status failed" };
  if (scope === "system") {
    return { ok: true, complete: st.system?.scan_complete_all_repos === true };
  }
  const repoId = scope.slice("repo:".length);
  const r = (Array.isArray(st.repos) ? st.repos : []).find((x) => normStr(x?.repo_id) === repoId) || null;
  return { ok: true, complete: r?.scan?.complete === true };
}

async function writeCompactDecisionRecord({ paths, meeting, dryRun }) {
  const { knowledgeUpdateAbs } = meetingDirs(paths);
  const latestAbs = join(knowledgeUpdateAbs, "LATEST.json");
  const recordAbs = join(knowledgeUpdateAbs, `UPDATE_MEETING-${meeting.meeting_id}.json`);
  const compact = {
    version: 1,
    meeting_id: meeting.meeting_id,
    scope: meeting.scope,
    from_version: meeting.from_version,
    to_version: meeting.to_version,
    status: meeting.status,
    decision: meeting.decision,
    notes: meeting.notes || null,
    resulting_actions: meeting.resulting_actions || [],
    created_at: meeting.created_at || null,
    closed_at: meeting.closed_at || null,
    closed_by: meeting.closed_by || null,
  };
  validateUpdateMeeting({ ...meeting, asked: meeting.asked || [], answers: meeting.answers || [], resulting_actions: meeting.resulting_actions || [] });

  if (!dryRun) {
    await mkdir(knowledgeUpdateAbs, { recursive: true });
    await writeJsonAtomic(recordAbs, compact);
    let latest = { version: 1, updated_at: null, latest_by_scope: {} };
    if (existsSync(latestAbs)) {
      try {
        latest = JSON.parse(String(readFileSync(latestAbs, "utf8") || "")) || latest;
      } catch {
        latest = { version: 1, updated_at: null, latest_by_scope: {} };
      }
    }
    const byScope = latest && typeof latest.latest_by_scope === "object" ? { ...latest.latest_by_scope } : {};
    byScope[String(meeting.scope)] = { meeting_id: meeting.meeting_id, decision: meeting.decision, to_version: meeting.to_version, closed_at: meeting.closed_at, record: basename(recordAbs) };
    await writeJsonAtomic(latestAbs, { version: 1, updated_at: nowISO(), latest_by_scope: byScope });
  }
  return { ok: true, wrote: !dryRun, record: recordAbs, latest: latestAbs };
}

export async function runVersionedKnowledgeUpdateMeeting({
  projectRoot,
  mode,
  scope,
  fromVersion,
  toVersion,
  session = null,
  maxQuestions = 1,
  dryRun = false,
  decision = null,
  by = null,
  notes = null,
} = {}) {
  const projectRootAbs = requireAbsOpsRoot(projectRoot);
  const m = normStr(mode);
  if (!m) throw new Error("Missing meeting mode.");
  const sc = requireScope(scope);

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const { updateAbs } = meetingDirs(paths);
  const statusIndex = meetingPaths({ baseAbs: updateAbs, meeting_id: "x" }).statusAbs;

  if (m === "status") {
    const idx = await readStatusIndexOptional({ statusAbs: statusIndex });
    if (!idx.ok) return { ok: false, message: idx.message };
    const entry = idx.index.latest_by_scope ? idx.index.latest_by_scope[sc] : null;
    return { ok: true, project_root: paths.opsRootAbs, update_meetings_root: updateAbs, latest_for_scope: entry || null, status_path: statusIndex };
  }

  const fromV = requireVersionLike(fromVersion, "from");
  const toV = requireVersionLike(toVersion, "to");
  const maxQ = Math.max(1, Math.min(25, Number.isFinite(Number(maxQuestions)) ? Math.floor(Number(maxQuestions)) : 1));

  if (m === "start") {
    const mid = meetingId({ scope: sc, fromVersion: fromV, toVersion: toV });
    const mp = meetingPaths({ baseAbs: updateAbs, meeting_id: mid });
    if (!dryRun) await mkdir(mp.transcriptAbs, { recursive: true });

    const meeting = validateUpdateMeeting({
      version: 1,
      meeting_id: mid,
      scope: sc,
      from_version: fromV,
      to_version: toV,
      status: "open",
      next_question: null,
      asked: [],
      answers: [],
      decision: null,
      notes: null,
      resulting_actions: [],
      created_at: nowISO(),
      updated_at: nowISO(),
      closed_at: null,
      closed_by: null,
    });

    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, meeting);
      const idxRes = await readStatusIndexOptional({ statusAbs: mp.statusAbs });
      if (!idxRes.ok) return { ok: false, message: idxRes.message };
      const latest_by_scope = { ...(idxRes.index.latest_by_scope || {}) };
      latest_by_scope[sc] = { meeting_id: mid, status: "open", from_version: fromV, to_version: toV, updated_at: nowISO() };
      await writeStatusIndex({ statusAbs: mp.statusAbs, index: { ...idxRes.index, latest_by_scope }, dryRun: false });
    }

    return { ok: true, meeting_id: mid, scope: sc, status: "open", dir: mp.dirAbs, wrote: !dryRun };
  }

  const meetingIdArg = normStr(session);
  if (!meetingIdArg) return { ok: false, message: "Missing --session." };
  const mp = meetingPaths({ baseAbs: updateAbs, meeting_id: meetingIdArg });
  if (!existsSync(mp.jsonAbs)) return { ok: false, message: `Missing meeting: ${mp.jsonAbs}` };

  const prev = validateUpdateMeeting(await readJsonAbs(mp.jsonAbs));
  if (prev.status === "closed") return { ok: false, message: "Meeting is closed." };

  if (m === "continue") {
    const unanswered = listUnanswered(prev);
    if (unanswered.length) return { ok: true, meeting_id: prev.meeting_id, scope: prev.scope, status: prev.status, waiting_for_answer: true, next_question: prev.next_question };
    if (prev.asked.length >= maxQ) return { ok: true, meeting_id: prev.meeting_id, scope: prev.scope, status: prev.status, ready_to_close: true };

    const qRes = await runKnowledgeCommittee({ projectRoot: paths.opsRootAbs, scope: prev.scope, mode: "challenge", maxQuestions: 1, dryRun });
    if (!qRes.ok) return { ok: false, message: qRes.message || "committee challenge failed" };
    const q = qRes.question;
    const askedEntry = { qid: q.id, question: q.question, asked_at: nowISO() };
    const next = validateUpdateMeeting({
      ...prev,
      next_question: { qid: askedEntry.qid, question: askedEntry.question },
      asked: prev.asked.concat([askedEntry]),
      updated_at: nowISO(),
    });
    if (!dryRun) await writeJsonAtomic(mp.jsonAbs, next);
    return { ok: true, meeting_id: next.meeting_id, scope: next.scope, status: next.status, question: askedEntry, wrote: !dryRun };
  }

  if (m === "close") {
    const dec = normStr(decision);
    if (!dec || !["approve", "reject", "defer"].includes(dec)) return { ok: false, message: "Missing/invalid --decision (approve|reject|defer)." };
    const who = normStr(by);
    if (!who) return { ok: false, message: "Missing --by." };
    const note = normStr(notes) || null;

    const unanswered = listUnanswered(prev);
    if (unanswered.length) return { ok: false, message: "Cannot close meeting while waiting for an answer." };

    const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
    if (!reposRes.ok) return { ok: false, message: reposRes.message };
    const staleInfo = await evaluateScopeStaleness({ paths, registry: reposRes.registry, scope: prev.scope });
    const coverage = await coverageCompleteForScope({ projectRootAbs: paths.opsRootAbs, scope: prev.scope });
    const openDecisions = await openDecisionCountForScope({ decisionsDirAbs: paths.knowledge.decisionsAbs, scope: prev.scope });

    if (dec === "approve") {
      if (staleInfo.hard_stale === true) return { ok: false, message: "Cannot approve: scope is hard-stale." };
      if (!coverage.ok || coverage.complete !== true) return { ok: false, message: "Cannot approve: scan coverage incomplete." };
      if (openDecisions > 0) return { ok: false, message: "Cannot approve: open decision packets exist for scope." };
    }

    const closed = validateUpdateMeeting({
      ...prev,
      status: "closed",
      decision: dec,
      notes: note,
      resulting_actions: dec === "approve" ? ["proceed_to_lane_b_intake"] : dec === "reject" ? ["run_scan", "open_decision_packet"] : [],
      updated_at: nowISO(),
      closed_at: nowISO(),
      closed_by: who,
      next_question: null,
    });

    if (dec === "approve") {
      if (!dryRun) {
        await setKnowledgeVersionExplicit({ projectRoot: paths.opsRootAbs, fromVersion: prev.from_version, toVersion: prev.to_version, scope: prev.scope, reason: "update_meeting", notes: note || "", dryRun: false });
      }
      await runKnowledgeSufficiencyApprove({ projectRoot: paths.opsRootAbs, scope: prev.scope, knowledgeVersion: prev.to_version, by: who, notes: note || null, dryRun });
    } else if (dec === "reject") {
      await runKnowledgeSufficiencyReject({ projectRoot: paths.opsRootAbs, scope: prev.scope, knowledgeVersion: prev.to_version, by: who, notes: note || "insufficient", dryRun });
    }

    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, closed);
      const idxRes = await readStatusIndexOptional({ statusAbs: mp.statusAbs });
      if (idxRes.ok) {
        const latest_by_scope = { ...(idxRes.index.latest_by_scope || {}) };
        latest_by_scope[sc] = { meeting_id: closed.meeting_id, status: closed.status, from_version: closed.from_version, to_version: closed.to_version, decision: closed.decision, updated_at: nowISO() };
        await writeStatusIndex({ statusAbs: mp.statusAbs, index: { ...idxRes.index, latest_by_scope }, dryRun: false });
      }
      await writeCompactDecisionRecord({ paths, meeting: closed, dryRun: false });
    }

    return { ok: dec !== "approve" ? true : true, meeting_id: closed.meeting_id, status: closed.status, decision: closed.decision, wrote: !dryRun };
  }

  return { ok: false, message: "Invalid --mode (start|continue|status|close)." };
}

export async function runVersionedKnowledgeUpdateAnswer({ projectRoot, session, inputPath, dryRun = false } = {}) {
  const projectRootAbs = requireAbsOpsRoot(projectRoot);
  const meetingIdArg = normStr(session);
  if (!meetingIdArg) throw new Error("Missing --session.");
  const inputAbs = resolve(String(inputPath || ""));
  if (!existsSync(inputAbs)) throw new Error(`Missing --input file (${inputAbs}).`);

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const { updateAbs } = meetingDirs(paths);
  const mp = meetingPaths({ baseAbs: updateAbs, meeting_id: meetingIdArg });
  if (!existsSync(mp.jsonAbs)) throw new Error(`Missing meeting: ${mp.jsonAbs}`);

  const prev = validateUpdateMeeting(await readJsonAbs(mp.jsonAbs));
  if (prev.status === "closed") return { ok: false, message: "Meeting is closed." };
  const unanswered = listUnanswered(prev);
  if (!unanswered.length) return { ok: false, message: "No pending question to answer." };
  const q = unanswered[0];
  const content = String(readFileSync(inputAbs, "utf8") || "").trim();
  if (!content) return { ok: false, message: "Answer input is empty." };

  const answerFileAbs = join(mp.transcriptAbs, `ANSWER-${q.qid.replace(/[^A-Za-z0-9_.-]/g, "_")}.md`);
  if (!dryRun) {
    await mkdir(mp.transcriptAbs, { recursive: true });
    await writeTextAtomic(answerFileAbs, content + "\n");
  }

  const ansEntry = { qid: q.qid, answer_ref: answerFileAbs, answered_at: nowISO() };
  const next = validateUpdateMeeting({
    ...prev,
    answers: prev.answers.concat([ansEntry]),
    updated_at: nowISO(),
    next_question: null,
  });
  if (!dryRun) await writeJsonAtomic(mp.jsonAbs, next);
  return { ok: true, meeting_id: next.meeting_id, status: next.status, answered: ansEntry, wrote: !dryRun };
}
