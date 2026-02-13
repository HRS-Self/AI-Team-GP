import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, readFile, rename, rm, writeFile, appendFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";
import { validateIntegrationStatus, validateMeeting } from "../../contracts/validators/index.js";
import { runKnowledgeStatus } from "./knowledge-status.js";
import { runKnowledgeCommittee, runKnowledgeCommitteeStatus } from "./committee-runner.js";
import { readSufficiencyOrDefault } from "./knowledge-sufficiency.js";
import { bumpKnowledgeVersion, readKnowledgeVersionOrDefault } from "./knowledge-version.js";
import { runKnowledgeBundle } from "./knowledge-bundle.js";
import { writeRefreshRequiredDecisionPacketIfNeeded } from "../lane-a-staleness-policy.js";
import {
  bindOldestOpenChangeRequestsToMeeting,
  loadAllChangeRequests,
  markChangeRequestsProcessed,
} from "./change-requests.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function fsSafeUtcTimestamp14() {
  const d = new Date();
  const y = String(d.getUTCFullYear()).padStart(4, "0");
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${y}${m}${day}_${hh}${mm}${ss}`;
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function parseScope(scope) {
  const s = normStr(scope) || "system";
  if (s === "system") return { kind: "system", scope: "system", repo_id: null, scopeSlug: "system" };
  if (s.startsWith("repo:")) {
    const id = normStr(s.slice("repo:".length));
    if (!id) throw new Error("Invalid --scope. Expected repo:<id>.");
    const slug = `repo-${id.replace(/[^A-Za-z0-9_.-]/g, "_")}`;
    return { kind: "repo", scope: `repo:${id}`, repo_id: id, scopeSlug: slug };
  }
  throw new Error("Invalid --scope. Expected system or repo:<id>.");
}

function meetingDirName({ scopeSlug, ts }) {
  return `UM-${ts}__${scopeSlug}`;
}

function meetingPaths({ laneAMeetingsAbs, meetingDir }) {
  const dirAbs = resolve(laneAMeetingsAbs, meetingDir);
  return {
    dirAbs,
    jsonAbs: join(dirAbs, "MEETING.json"),
    mdAbs: join(dirAbs, "MEETING.md"),
    questionsAbs: join(dirAbs, "QUESTIONS.jsonl"),
    answersAbs: join(dirAbs, "ANSWERS.jsonl"),
    decisionsAbs: join(dirAbs, "DECISIONS.jsonl"),
    errorAbs: join(dirAbs, "ERROR.json"),
  };
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

async function appendJsonl(absPath, obj) {
  await mkdir(dirname(resolve(absPath)), { recursive: true });
  await appendFile(resolve(absPath), JSON.stringify(obj) + "\n", "utf8");
}

async function readJsonAbs(absPath) {
  const t = await readFile(resolve(String(absPath || "")), "utf8");
  return JSON.parse(String(t || ""));
}

async function readJsonlLines(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return [];
  const t = await readFile(abs, "utf8");
  return String(t || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => JSON.parse(l));
}

function computeAnsweredTiers(answers) {
  const out = new Set();
  for (const a of Array.isArray(answers) ? answers : []) {
    const tier = normStr(a?.tier);
    if (tier) out.add(tier);
  }
  return out;
}

function ladderQuestions({ parsedScope, cr }) {
  const repoId = parsedScope.kind === "repo" ? parsedScope.repo_id : null;
  const crId = cr ? cr.id : null;
  const crTitle = cr ? cr.title : null;
  const prefix = crId ? `Change request ${crId}${crTitle ? ` (${crTitle})` : ""}: ` : "";
  return [
    { tier: "VISION", text: `${prefix}${repoId ? `What is the intent/goal of this change for repo '${repoId}'?` : "What is the intent/goal of this change for the system?"}` },
    { tier: "REQUIREMENTS", text: `${prefix}Describe the affected business flow(s) + actor(s) + expected outcome.` },
    { tier: "DOMAIN_DATA", text: `${prefix}Which domain entities/invariants are impacted (names only; 1-2 bullets)?` },
    { tier: "DATA", text: `${prefix}Any data model/storage assumptions or migrations required?` },
    { tier: "API", text: `${prefix}Which API contracts/events are affected (names/paths/topics only)?` },
    { tier: "INFRA", text: `${prefix}Any runtime config/env/secrets changes needed?` },
    { tier: "OPS", text: `${prefix}Any quality gates/deploy constraints for this change?` },
  ];
}

function tierOrderIndex(tier) {
  const order = ["REFRESH", "VISION", "REQUIREMENTS", "DOMAIN_DATA", "DATA", "API", "INFRA", "OPS"];
  const idx = order.indexOf(String(tier || ""));
  return idx >= 0 ? idx : 999;
}

function pickOldestInMeetingChangeRequest({ changeRequests, meetingId, parsedScope }) {
  const items = Array.isArray(changeRequests) ? changeRequests : [];
  const candidates = items.filter((c) => c.status === "in_meeting" && c.linked_meeting_id === meetingId && c.scope === parsedScope.scope);
  if (!candidates.length) return null;
  return candidates[0];
}

function computeStalenessFromKnowledgeStatus({ parsedScope, status }) {
  const repos = Array.isArray(status?.repos) ? status.repos : [];
  const reasons = [];
  let stale = false;
  if (parsedScope.kind === "repo") {
    const r = repos.find((x) => normStr(x?.repo_id) === parsedScope.repo_id) || null;
    stale = r?.freshness?.stale === true;
    if (stale) reasons.push(normStr(r?.freshness?.stale_reason) || "repo_head_moved");
  } else {
    const staleRepos = repos.filter((r) => r?.freshness?.stale === true);
    stale = staleRepos.length > 0;
    for (const r of staleRepos) reasons.push(`${normStr(r?.repo_id) || "repo"}:${normStr(r?.freshness?.stale_reason) || "stale"}`);
  }
  return { stale, reasons: Array.from(new Set(reasons)).sort((a, b) => a.localeCompare(b)) };
}

async function computeMeetingInputs({ paths, parsedScope }) {
  const knowledgeStatus = await runKnowledgeStatus({ projectRoot: paths.opsRootAbs, dryRun: true });
  if (!knowledgeStatus.ok) throw new Error(knowledgeStatus.message || "Failed to load knowledge status.");

  const staleness = computeStalenessFromKnowledgeStatus({ parsedScope, status: knowledgeStatus });

  const openDecisionIds = [];
  const byScope = knowledgeStatus.decisions && knowledgeStatus.decisions.by_scope ? knowledgeStatus.decisions.by_scope : null;
  if (byScope && typeof byScope === "object") {
    const sys = Array.isArray(byScope.system) ? byScope.system : [];
    for (const id of sys) openDecisionIds.push(id);
    if (parsedScope.kind === "repo") {
      const s = `repo:${parsedScope.repo_id}`;
      const rs = Array.isArray(byScope[s]) ? byScope[s] : [];
      for (const id of rs) openDecisionIds.push(id);
    }
  }

  let integrationGapIds = [];
  const integAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json");
  if (existsSync(integAbs)) {
    try {
      const integ = JSON.parse(String(readFileSync(integAbs, "utf8") || ""));
      validateIntegrationStatus(integ);
      const gaps = Array.isArray(integ.integration_gaps) ? integ.integration_gaps : [];
      integrationGapIds = gaps
        .map((g) => normStr(g?.id))
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
    } catch {
      integrationGapIds = [];
    }
  }

  const committeeStatusPath =
    parsedScope.kind === "repo"
      ? join(paths.knowledge.ssotReposAbs, parsedScope.repo_id, "committee", "committee_status.json")
      : join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json");

  const coveragePath = join(paths.opsRootAbs, "ai", "lane_a", "STATUS.md");
  const suffPaths = resolve(paths.laneA.sufficiencyAbs, "SUFFICIENCY.json");

  return {
    knowledgeStatus,
    inputs: {
      coverage_path: coveragePath,
      sufficiency_path: suffPaths,
      committee_status_path: committeeStatusPath,
      open_decisions: Array.from(new Set(openDecisionIds)).sort((a, b) => a.localeCompare(b)),
      integration_gaps: integrationGapIds,
      staleness,
    },
  };
}

async function findMeetingDirs({ laneAMeetingsAbs }) {
  if (!existsSync(laneAMeetingsAbs)) return [];
  const entries = await readdir(laneAMeetingsAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isDirectory() && /^UM-\d{8}_\d{6}__/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

function pickLatestMeetingDir({ meetingDirs, parsedScope }) {
  const filtered = meetingDirs.filter((d) => d.includes(`__${parsedScope.scopeSlug}`));
  if (!filtered.length) return null;
  return filtered[filtered.length - 1];
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

function readLatestSystemBundleId({ laneARootAbs }) {
  const abs = resolve(String(laneARootAbs || ""), "bundles", "LATEST.json");
  if (!existsSync(abs)) return null;
  try {
    const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    const entry = j && j.latest_by_scope && typeof j.latest_by_scope === "object" ? j.latest_by_scope.system : null;
    const bid = entry && typeof entry.bundle_id === "string" ? entry.bundle_id.trim() : "";
    return bid || null;
  } catch {
    return null;
  }
}

async function ensureSystemBundleForIntegrationChair({ projectRootAbs, paths, dryRun }) {
  const existing = readLatestSystemBundleId({ laneARootAbs: paths.laneA.rootAbs });
  if (existing) return { ok: true, bundle_id: existing, created: false };
  const b = await runKnowledgeBundle({ projectRoot: projectRootAbs, scope: "system", out: null, dryRun });
  if (!b.ok) return { ok: false, message: b.message || "Failed to build system knowledge bundle." };
  const createdId = typeof b.bundle_id === "string" ? b.bundle_id : null;
  return { ok: true, bundle_id: createdId, created: true };
}

async function committeeIsReady({ projectRootAbs, scope }) {
  const st = await runKnowledgeCommitteeStatus({ projectRoot: projectRootAbs });
  if (!st.ok) return { ok: false, ready: false, summary: null, message: st.message };
  if (scope === "system") {
    const repos = Array.isArray(st.repos) ? st.repos : [];
    const anyMissing = repos.some((r) => !r.exists);
    const anyFailed = repos.some((r) => r.exists && r.evidence_valid === false);
    const integOk = st.integration && st.integration.evidence_valid === true;
    const ready = !anyMissing && !anyFailed && integOk;
    return { ok: true, ready, summary: st };
  }
  if (scope.startsWith("repo:")) {
    const repoId = scope.slice("repo:".length).trim();
    const r = (Array.isArray(st.repos) ? st.repos : []).find((x) => normStr(x.repo_id) === repoId) || null;
    const ready = !!(r && r.exists && r.evidence_valid === true);
    return { ok: true, ready, summary: { scope, repo: r } };
  }
  return { ok: true, ready: false, summary: st };
}

async function advanceCommitteeOneStep({ projectRootAbs, scope, dryRun }) {
  return runKnowledgeCommittee({ projectRoot: projectRootAbs, scope, limit: 1, dryRun });
}

function renderMeetingMd({ meeting, knowledgeStatus, changeRequests, versionCurrent }) {
  const m = meeting;
  const ks = knowledgeStatus;
  const lines = [];
  lines.push("KNOWLEDGE UPDATE MEETING");
  lines.push("");
  lines.push(`meeting_id: ${m.meeting_id}`);
  lines.push(`scope: ${m.scope}`);
  lines.push(`status: ${m.status}`);
  lines.push(`knowledge_version_target: ${m.knowledge_version_target}`);
  lines.push(`knowledge_version_current: ${versionCurrent || "(unknown)"}`);
  lines.push("");
  lines.push("INPUTS");
  lines.push("");
  lines.push(`open_decisions: ${m.inputs.open_decisions.length}`);
  lines.push(`integration_gaps: ${m.inputs.integration_gaps.length}`);
  lines.push(`stale: ${m.inputs.staleness.stale ? "true" : "false"}`);
  if (m.inputs.staleness.reasons.length) lines.push(`stale_reasons: ${m.inputs.staleness.reasons.join(", ")}`);
  lines.push("");
  lines.push("CHANGE REQUESTS (in_meeting)");
  lines.push("");
  const inMeeting = (Array.isArray(changeRequests) ? changeRequests : []).filter((c) => c.status === "in_meeting" && c.linked_meeting_id === m.meeting_id);
  if (!inMeeting.length) lines.push("- (none)");
  for (const c of inMeeting) lines.push(`- ${c.id}: ${c.type} severity=${c.severity} title=${c.title}`);
  lines.push("");
  lines.push("SCAN/COMMITTEE SUMMARY");
  lines.push("");
  lines.push(`scan_complete_all_repos: ${ks?.system?.scan_complete_all_repos ? "true" : "false"}`);
  lines.push(`open_decisions_count: ${ks?.system?.open_decisions_count ?? "(unknown)"}`);
  lines.push("");
  lines.push("PROGRESS");
  lines.push("");
  lines.push(`question_cursor: ${m.question_cursor}`);
  lines.push(`asked_count: ${m.asked_count}`);
  lines.push(`answered_count: ${m.answered_count}`);
  lines.push("");
  lines.push("RECOMMENDED NEXT ACTION");
  lines.push("");
  if (m.status === "waiting_for_answer") lines.push("- Provide an answer via --knowledge-review-answer.");
  else if (m.status === "ready_to_close") lines.push("- Close via --knowledge-update-meeting --close ...");
  else if (m.status === "closed") lines.push("- (closed)");
  else lines.push("- Continue via --knowledge-update-meeting --continue");
  lines.push("");
  return lines.join("\n") + "\n";
}

function nextQuestionForUpdateMeeting({ parsedScope, meeting, answers, changeRequests, staleness }) {
  if (staleness && staleness.stale === true && meeting.asked_count === 0) {
    return { ok: true, done: false, question: { tier: "REFRESH", text: "Refresh required: repo(s) appear stale relative to scans/merge events. Should we run revise_scans/refresh before proceeding? (yes/no)" } };
  }

  const cr = pickOldestInMeetingChangeRequest({ changeRequests, meetingId: meeting.meeting_id, parsedScope });
  const qs = ladderQuestions({ parsedScope, cr });
  const answeredTiers = computeAnsweredTiers(answers);
  for (const q of qs) {
    if (!answeredTiers.has(q.tier)) return { ok: true, done: false, question: q, cr_id: cr ? cr.id : null };
  }
  return { ok: true, done: true, question: null };
}

export async function runKnowledgeUpdateMeeting({
  projectRoot,
  mode,
  scope = "system",
  session = null,
  maxQuestions = 12,
  dryRun = false,
  closeDecision = null,
  closeNotes = null,
  forceStaleOverride = false,
  by = null,
  reason = null,
} = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });

  const parsedScope = parseScope(scope);
  const laneAMeetingsAbs = resolve(paths.laneA.rootAbs, "meetings");
  await mkdir(laneAMeetingsAbs, { recursive: true });

  if (mode === "status") {
    const dirs = await findMeetingDirs({ laneAMeetingsAbs });
    const sessionsOut = [];
    for (const d of dirs) {
      const mp = meetingPaths({ laneAMeetingsAbs, meetingDir: d });
      if (!existsSync(mp.jsonAbs)) continue;
      // eslint-disable-next-line no-await-in-loop
      const j = await readJsonAbs(mp.jsonAbs);
      validateMeeting(j);
      sessionsOut.push({ meeting_id: j.meeting_id, scope: j.scope, status: j.status, dir: d, created_at: j.created_at, updated_at: j.updated_at });
    }
    return { ok: true, project_root: paths.opsRootAbs, meetings_root: laneAMeetingsAbs, sessions: sessionsOut };
  }

  const maxQ = Math.max(1, Math.min(25, Number.isFinite(Number(maxQuestions)) ? Math.floor(Number(maxQuestions)) : 12));

  if (mode === "start") {
    const ts = fsSafeUtcTimestamp14();
    const meetingDir = meetingDirName({ scopeSlug: parsedScope.scopeSlug, ts });
    const mp = meetingPaths({ laneAMeetingsAbs, meetingDir });
    if (existsSync(mp.dirAbs)) return { ok: false, message: `Meeting already exists: ${mp.dirAbs}` };
    if (!dryRun) await mkdir(mp.dirAbs, { recursive: true });

    const suff = await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
    const knowledge_version_target = normStr(suff?.sufficiency?.knowledge_version) || "v1";

    const { knowledgeStatus, inputs } = await computeMeetingInputs({ paths, parsedScope });
    const meeting = {
      version: 1,
      meeting_id: meetingDir,
      project_root: paths.opsRootAbs,
      scope: parsedScope.scope,
      status: "open",
      knowledge_version_target,
      inputs,
      question_cursor: 0,
      asked_count: 0,
      answered_count: 0,
      created_at: nowISO(),
      updated_at: nowISO(),
      closed_at: null,
      closed_decision: null,
    };
    validateMeeting(meeting);

    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, meeting);
      // Bind oldest open change requests within scope immediately (contract: move to processed/ when bound).
      await bindOldestOpenChangeRequestsToMeeting({ projectRoot: paths.opsRootAbs, meetingId: meeting.meeting_id, scope: parsedScope.scope, maxBind: 10, dryRun: false });
      const crs = await loadAllChangeRequests({ changeRequestsAbs: resolve(paths.laneA.rootAbs, "change_requests") });
      const kv = null;
      await writeTextAtomic(mp.mdAbs, renderMeetingMd({ meeting, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, changeRequests: crs, versionCurrent: kv }));
    }
    return { ok: true, meeting_id: meeting.meeting_id, scope: meeting.scope, status: meeting.status, dir: mp.dirAbs, wrote: !dryRun };
  }

  // continue/close require a session
  const dirs = await findMeetingDirs({ laneAMeetingsAbs });
  const meetingDir = session ? normStr(session) : pickLatestMeetingDir({ meetingDirs: dirs, parsedScope });
  if (!meetingDir) return { ok: false, message: "No update meeting session found. Use --start to create one." };
  const mp = meetingPaths({ laneAMeetingsAbs, meetingDir });
  if (!existsSync(mp.jsonAbs)) return { ok: false, message: `Missing ${mp.jsonAbs}` };

  const meetingPrev = await readJsonAbs(mp.jsonAbs);
  validateMeeting(meetingPrev);

  const changeRequestsAbs = resolve(paths.laneA.rootAbs, "change_requests");
  const crsAll = await loadAllChangeRequests({ changeRequestsAbs });

  if (mode === "close") {
    if (meetingPrev.status === "closed") return { ok: false, message: "Meeting already closed." };
    const decision = normStr(closeDecision);
    if (!decision) return { ok: false, message: "Missing --decision." };
    const notes = normStr(closeNotes) || "";

    const { knowledgeStatus, inputs } = await computeMeetingInputs({ paths, parsedScope });
    const suff = await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
    const committeeReady = await committeeIsReady({ projectRootAbs: paths.opsRootAbs, scope: parsedScope.scope });

    const staleness = inputs.staleness;
    const inMeetingCrs = crsAll.filter((c) => c.status === "in_meeting" && c.linked_meeting_id === meetingPrev.meeting_id);
    const approvedCrIds = inMeetingCrs.map((c) => c.id).sort((a, b) => a.localeCompare(b));

    if (decision === "approve_intake") {
      const stale = staleness.stale === true;
      if (stale && !forceStaleOverride) {
        await writeRefreshRequiredDecisionPacketIfNeeded({
          paths,
          repoId: parsedScope.kind === "repo" ? parsedScope.repo_id : null,
          blockingState: "APPROVE_INTAKE",
          staleInfo: { stale_reason: (staleness.reasons && staleness.reasons[0]) || "stale", stale_reasons: staleness.reasons || [] },
          producer: "meeting",
          dryRun,
        });
        return { ok: false, error: "knowledge_stale", scope: parsedScope.scope, reasons: staleness.reasons || [] };
      }
      if (stale && forceStaleOverride) {
        const who = normStr(by) || normStr(process.env.USER || "") || null;
        const why = normStr(reason) || normStr(closeNotes) || null;
        await appendFile(paths.laneA.ledgerAbs, JSON.stringify({ timestamp: nowISO(), type: "stale_override", command: "knowledge-update-meeting.approve_intake", scope: parsedScope.scope, by: who, reason: why }) + "\n");
      }
      const scanCompleteAll = knowledgeStatus.system?.scan_complete_all_repos === true;
      if (!scanCompleteAll) return { ok: false, message: "Cannot close with approve_intake: scan coverage is incomplete." };
      if (!committeeReady.ok || !committeeReady.ready) return { ok: false, message: "Cannot close with approve_intake: committee is not evidence-valid." };
      const suffStatus = normStr(suff?.sufficiency?.status) || "insufficient";
      const suffOk = suffStatus === "sufficient" || notes.toLowerCase().includes("override_sufficiency");
      if (!suffOk) return { ok: false, message: "Cannot close with approve_intake: sufficiency is not sufficient (add override_sufficiency to notes to override)." };

      const kvRes = await readKnowledgeVersionOrDefault({ projectRoot: paths.opsRootAbs });
      const kv = kvRes.ok ? kvRes.version : { version: 1, current: "v0", history: [] };

      const iaDirAbs = resolve(paths.laneA.rootAbs, "intake_approvals");
      const iaProcessedAbs = join(iaDirAbs, "processed");
      const ts = fsSafeUtcTimestamp14();
      const hash8 = sha256Hex(`${meetingPrev.meeting_id}\n${parsedScope.scope}\n${approvedCrIds.join(",")}\n${notes}`).slice(0, 8);
      const iaId = `IA-${ts}__${parsedScope.scopeSlug}__${hash8}`;
      const iaAbs = join(iaDirAbs, `${iaId}.json`);
      const iaObj = {
        version: 1,
        id: iaId,
        scope: parsedScope.scope,
        approved_items: approvedCrIds,
        knowledge_version: kv.current,
        sufficiency_status: suffStatus,
        sufficiency_override: suffStatus !== "sufficient",
        created_at: nowISO(),
        notes,
      };
      if (!dryRun) {
        await mkdir(iaDirAbs, { recursive: true });
        await mkdir(iaProcessedAbs, { recursive: true });
        await writeJsonAtomic(iaAbs, iaObj);
        await markChangeRequestsProcessed({ projectRoot: paths.opsRootAbs, ids: approvedCrIds, meetingId: meetingPrev.meeting_id, dryRun: false });
        await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "approve_intake", notes, intake_approval: `intake_approvals/${iaId}.json`, approved_items: approvedCrIds });
      }
    } else if (decision === "revise_scans") {
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "revise_scans", notes });
    } else if (decision === "open_decisions") {
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "open_decisions", notes, related_change_requests: approvedCrIds });
    } else if (decision === "abort") {
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "abort", notes });
    } else if (decision === "bump_patch" || decision === "bump_minor" || decision === "bump_major" || decision === "no_bump") {
      const bumpKind = decision;
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: bumpKind, notes });
      const bumpRes = await bumpKnowledgeVersion({ projectRoot: paths.opsRootAbs, kind: bumpKind, reason: "update_meeting", scope: parsedScope.scope, notes, dryRun });
      if (!bumpRes.ok) return { ok: false, message: bumpRes.message || "Failed to bump knowledge version." };
    } else {
      return { ok: false, message: `Invalid --decision: ${decision}` };
    }

    const closed = {
      ...meetingPrev,
      status: "closed",
      updated_at: nowISO(),
      closed_at: nowISO(),
      closed_decision: decision,
      inputs,
    };
    validateMeeting(closed);
    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, closed);
      await writeTextAtomic(mp.mdAbs, renderMeetingMd({ meeting: closed, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, changeRequests: crsAll, versionCurrent: null }));
      await rm(mp.errorAbs, { force: true });
    }
    return { ok: true, meeting_id: closed.meeting_id, status: closed.status, closed_decision: closed.closed_decision, wrote: !dryRun };
  }

  if (mode !== "continue") return { ok: false, message: "Invalid meeting mode." };

  if (meetingPrev.status === "closed") return { ok: false, message: "Meeting is closed." };
  if (meetingPrev.status === "waiting_for_answer") {
    return { ok: true, meeting_id: meetingPrev.meeting_id, status: meetingPrev.status, waiting_for_answer: true, message: "Waiting for answer to the last question." };
  }

  const { knowledgeStatus, inputs } = await computeMeetingInputs({ paths, parsedScope });
  const suff = await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });

  // Bind change requests deterministically once per meeting.
  const hasBound = crsAll.some((c) => c.status === "in_meeting" && c.linked_meeting_id === meetingPrev.meeting_id);
  if (!hasBound) {
    await bindOldestOpenChangeRequestsToMeeting({ projectRoot: paths.opsRootAbs, meetingId: meetingPrev.meeting_id, scope: parsedScope.scope, maxBind: 10, dryRun });
  }

  const crs = await loadAllChangeRequests({ changeRequestsAbs });

  // Ensure system bundle for integration chair before running system committee.
  if (parsedScope.kind === "system") {
    const sb = await ensureSystemBundleForIntegrationChair({ projectRootAbs: paths.opsRootAbs, paths, dryRun });
    if (!sb.ok) return { ok: false, message: sb.message };
  }

  // Advance committee at most one step per run.
  const committeeReady = await committeeIsReady({ projectRootAbs: paths.opsRootAbs, scope: parsedScope.scope });
  if (!committeeReady.ok) return { ok: false, message: committeeReady.message || "Failed to load committee status." };
  if (!committeeReady.ready) {
    const progressed = await advanceCommitteeOneStep({ projectRootAbs: paths.opsRootAbs, scope: parsedScope.scope, dryRun });
    if (!progressed.ok) {
      const errObj = { ok: false, message: progressed.message || "committee failed", executed: progressed.executed || [] };
      if (!dryRun) await writeJsonAtomic(mp.errorAbs, errObj);
      return { ok: false, message: progressed.message || "Committee failed." };
    }
    const next = { ...meetingPrev, updated_at: nowISO(), inputs };
    validateMeeting(next);
    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, next);
      await writeTextAtomic(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, changeRequests: crs, versionCurrent: null }));
      await rm(mp.errorAbs, { force: true });
    }
    return { ok: true, meeting_id: next.meeting_id, status: next.status, progressed: "committee", wrote: !dryRun };
  }

  const answers = await readJsonlLines(mp.answersAbs);
  const nq = nextQuestionForUpdateMeeting({ parsedScope, meeting: meetingPrev, answers, changeRequests: crs, staleness: inputs.staleness });
  if (!nq.ok) return { ok: false, message: "Failed to compute next question." };
  if (meetingPrev.asked_count >= maxQ || nq.done) {
    const next = { ...meetingPrev, status: "ready_to_close", updated_at: nowISO(), inputs };
    validateMeeting(next);
    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, next);
      await writeTextAtomic(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, changeRequests: crs, versionCurrent: null }));
      await rm(mp.errorAbs, { force: true });
    }
    return { ok: true, meeting_id: next.meeting_id, status: next.status, ready_to_close: true, wrote: !dryRun };
  }

  const q = nq.question;
  const questionText = normStr(q.text);
  const tier = q.tier;
  const qId = `Q-${sha256Hex(`${meetingPrev.meeting_id}\n${meetingPrev.question_cursor}\n${tier}\n${questionText}`).slice(0, 16)}`;
  const qLine = {
    version: 1,
    id: qId,
    meeting_id: meetingPrev.meeting_id,
    cursor: meetingPrev.question_cursor,
    tier,
    question: questionText,
    created_at: nowISO(),
    related_change_request_id: nq.cr_id || null,
  };

  const next = {
    ...meetingPrev,
    status: "waiting_for_answer",
    updated_at: nowISO(),
    inputs,
    question_cursor: meetingPrev.question_cursor + 1,
    asked_count: meetingPrev.asked_count + 1,
  };
  validateMeeting(next);

  if (!dryRun) {
    await appendJsonl(mp.questionsAbs, qLine);
    await writeJsonAtomic(mp.jsonAbs, next);
    await writeTextAtomic(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, changeRequests: crs, versionCurrent: null }));
    await rm(mp.errorAbs, { force: true });
  }

  return { ok: true, meeting_id: next.meeting_id, status: next.status, question: qLine, wrote: !dryRun };
}
