import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, readdir, rename, writeFile, appendFile, rm } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";
import { validateDecisionPacket, validateIntegrationStatus, validateMeeting } from "../../contracts/validators/index.js";
import { runKnowledgeStatus } from "./knowledge-status.js";
import { runKnowledgeCommittee, runKnowledgeCommitteeStatus } from "./committee-runner.js";
import { runKnowledgeSufficiencyPropose, readSufficiencyOrDefault } from "./knowledge-sufficiency.js";
import { buildDecisionPacket, renderDecisionPacketMd, stableId, writeTextAtomic } from "./committee-utils.js";
import { runKnowledgeBundle } from "./knowledge-bundle.js";
import { writeRefreshRequiredDecisionPacketIfNeeded } from "../lane-a-staleness-policy.js";

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
  return `M-${ts}__${scopeSlug}`;
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

async function readJsonAbs(absPath) {
  const t = await readFile(resolve(String(absPath || "")), "utf8");
  return JSON.parse(String(t || ""));
}

async function loadMeeting({ meetingJsonAbs }) {
  const j = await readJsonAbs(meetingJsonAbs);
  validateMeeting(j);
  return j;
}

async function writeJsonAtomic(absPath, obj) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  const tmp = `${abs}.tmp.${process.pid}.${sha256Hex(abs).slice(0, 8)}`;
  await writeFile(tmp, JSON.stringify(obj, null, 2) + "\n", "utf8");
  await rename(tmp, abs);
}

async function writeTextAtomicLocal(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  const tmp = `${abs}.tmp.${process.pid}.${sha256Hex(abs).slice(0, 8)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

async function appendJsonl(absPath, obj) {
  await mkdir(dirname(resolve(absPath)), { recursive: true });
  await appendFile(resolve(absPath), JSON.stringify(obj) + "\n", "utf8");
}

function renderMeetingMd({ meeting, knowledgeStatus, committeeSummary }) {
  const m = meeting;
  const ks = knowledgeStatus;
  const lines = [];
  lines.push("KNOWLEDGE REVIEW MEETING");
  lines.push("");
  lines.push(`meeting_id: ${m.meeting_id}`);
  lines.push(`scope: ${m.scope}`);
  lines.push(`status: ${m.status}`);
  lines.push(`knowledge_version_target: ${m.knowledge_version_target}`);
  lines.push("");
  lines.push("INPUTS");
  lines.push("");
  lines.push(`sufficiency_path: ${m.inputs.sufficiency_path}`);
  lines.push(`committee_status_path: ${m.inputs.committee_status_path}`);
  lines.push(`coverage_path: ${m.inputs.coverage_path}`);
  lines.push(`open_decisions: ${m.inputs.open_decisions.length}`);
  lines.push(`integration_gaps: ${m.inputs.integration_gaps.length}`);
  lines.push(`stale: ${m.inputs.staleness.stale ? "true" : "false"}`);
  if (m.inputs.staleness.reasons.length) lines.push(`stale_reasons: ${m.inputs.staleness.reasons.join(", ")}`);
  lines.push("");
  lines.push("SUFFICIENCY");
  lines.push("");
  if (ks && ks.sufficiency) {
    lines.push(`status: ${ks.sufficiency.status}`);
    lines.push(`confidence: ${ks.sufficiency.confidence}%`);
  } else {
    lines.push("- (unknown)");
  }
  lines.push("");
  lines.push("SCAN COVERAGE");
  lines.push("");
  if (ks && ks.system) {
    lines.push(`scan_complete_all_repos: ${ks.system.scan_complete_all_repos ? "true" : "false"}`);
  } else {
    lines.push("- (unknown)");
  }
  lines.push("");
  lines.push("COMMITTEE");
  lines.push("");
  if (committeeSummary) {
    if (committeeSummary.scope === "system") {
      const repos = Array.isArray(committeeSummary.repos) ? committeeSummary.repos : [];
      const passed = repos.filter((r) => r.exists && r.evidence_valid === true).length;
      const failed = repos.filter((r) => r.exists && r.evidence_valid === false).length;
      const missing = repos.filter((r) => !r.exists).length;
      lines.push(`repos_passed: ${passed}`);
      lines.push(`repos_failed: ${failed}`);
      lines.push(`repos_missing: ${missing}`);
      lines.push(`integration_exists: ${committeeSummary.integration ? "true" : "false"}`);
      if (committeeSummary.integration) lines.push(`integration_evidence_valid: ${committeeSummary.integration.evidence_valid ? "true" : "false"}`);
    } else {
      lines.push(JSON.stringify(committeeSummary, null, 2));
    }
  } else {
    lines.push("- (unknown)");
  }
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
  else if (m.status === "ready_to_close") lines.push("- Close via --knowledge-review-meeting --close ...");
  else if (m.status === "closed") lines.push("- (closed)");
  else lines.push("- Continue via --knowledge-review-meeting --continue");
  lines.push("");
  return lines.join("\n") + "\n";
}

function ladderQuestions({ parsedScope }) {
  const repoId = parsedScope.kind === "repo" ? parsedScope.repo_id : null;
  return [
    { tier: "VISION", text: repoId ? `What is the mission of repo '${repoId}' in the overall system?` : "What is the vision and primary goal of this system?" },
    { tier: "REQUIREMENTS", text: repoId ? `List the top 3 business flows this repo supports (actors + outcomes).` : "List the top 3 business flows/actors the system must support (actors + outcomes)." },
    { tier: "DOMAIN_DATA", text: "Name the top 5 domain entities and 1 key invariant each (brief bullets)." },
    { tier: "DATA", text: "What are the primary data stores and persistence assumptions (DBs/queues/storage; any constraints)?" },
    { tier: "API", text: "List the primary external API contracts/events exposed and consumed (names/paths only; no payload details yet)." },
    { tier: "INFRA", text: "Describe runtime/deploy topology + key environment variables/secrets categories (high level)." },
    { tier: "OPS", text: "What are the quality gates and release constraints (CI requirements, environments, deploy rules)?" },
  ];
}

function tierOrderIndex(tier) {
  const order = ["VISION", "REQUIREMENTS", "DOMAIN_DATA", "DATA", "API", "INFRA", "OPS"];
  const idx = order.indexOf(String(tier || ""));
  return idx >= 0 ? idx : 999;
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

function nextQuestion({ parsedScope, answers, maxQuestions, askedCount }) {
  const qs = ladderQuestions({ parsedScope });
  const answeredTiers = computeAnsweredTiers(answers);

  if (askedCount >= maxQuestions) return { ok: true, done: true, question: null, reason: "max_questions_reached" };

  for (const q of qs) {
    if (!answeredTiers.has(q.tier)) return { ok: true, done: false, question: q, reason: "next_tier" };
  }
  return { ok: true, done: true, question: null, reason: "ladder_complete" };
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

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

async function findMeetingDirs({ laneAMeetingsAbs }) {
  if (!existsSync(laneAMeetingsAbs)) return [];
  const entries = await readdir(laneAMeetingsAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isDirectory() && /^M-\d{8}_\d{6}__/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

function pickLatestMeetingDir({ meetingDirs, parsedScope, onlyOpen = false }) {
  const filtered = meetingDirs.filter((d) => d.includes(`__${parsedScope.scopeSlug}`));
  if (!filtered.length) return null;
  if (!onlyOpen) return filtered[filtered.length - 1];
  return filtered[filtered.length - 1];
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
  const res = await runKnowledgeCommittee({ projectRoot: projectRootAbs, scope, limit: 1, dryRun });
  return res;
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

export async function runKnowledgeReviewMeeting({
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
      // eslint-disable-next-line no-await-in-loop
      const j = existsSync(mp.jsonAbs) ? await loadMeeting({ meetingJsonAbs: mp.jsonAbs }) : null;
      if (!j) continue;
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
      await writeTextAtomicLocal(mp.mdAbs, renderMeetingMd({ meeting, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, committeeSummary: null }));
    }
    return { ok: true, meeting_id: meeting.meeting_id, scope: meeting.scope, status: meeting.status, dir: mp.dirAbs, wrote: !dryRun };
  }

  // continue/close require a session
  const dirs = await findMeetingDirs({ laneAMeetingsAbs });
  const meetingDir = session ? normStr(session) : pickLatestMeetingDir({ meetingDirs: dirs, parsedScope, onlyOpen: false });
  if (!meetingDir) return { ok: false, message: "No meeting session found. Use --start to create one." };
  const mp = meetingPaths({ laneAMeetingsAbs, meetingDir });
  if (!existsSync(mp.jsonAbs)) return { ok: false, message: `Missing ${mp.jsonAbs}` };

  const meetingPrev = await loadMeeting({ meetingJsonAbs: mp.jsonAbs });

  if (mode === "close") {
    if (meetingPrev.status === "closed") return { ok: false, message: "Meeting already closed." };
    const decision = normStr(closeDecision);
    if (!decision) return { ok: false, message: "Missing --decision (confirm_sufficiency|revise_scans|open_decisions|abort)." };
    const notes = normStr(closeNotes) || "";

    const { knowledgeStatus, inputs } = await computeMeetingInputs({ paths, parsedScope });
    const suff = await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
    const committeeReady = await committeeIsReady({ projectRootAbs: paths.opsRootAbs, scope: parsedScope.scope });

    if (decision === "confirm_sufficiency") {
      const scanCompleteAll = knowledgeStatus.system?.scan_complete_all_repos === true;
      const stale = inputs.staleness.stale === true;
      const openDecCount = Number.isFinite(Number(knowledgeStatus.system?.open_decisions_count)) ? Number(knowledgeStatus.system.open_decisions_count) : 0;
      const openOk = openDecCount === 0 || notes.toLowerCase().includes("override_open_decisions");
      if (!scanCompleteAll) return { ok: false, message: "Cannot close with confirm_sufficiency: scan coverage is incomplete." };
      if (stale && !forceStaleOverride) {
        await writeRefreshRequiredDecisionPacketIfNeeded({
          paths,
          repoId: parsedScope.kind === "repo" ? parsedScope.repo_id : null,
          blockingState: "SUFFICIENCY",
          staleInfo: { stale_reason: (inputs.staleness.reasons && inputs.staleness.reasons[0]) || "stale", stale_reasons: inputs.staleness.reasons || [] },
          producer: "meeting",
          dryRun,
        });
        return {
          ok: false,
          error: "knowledge_stale",
          message: "Cannot close with confirm_sufficiency: staleness.stale is true.",
          scope: parsedScope.scope,
          reasons: inputs.staleness.reasons || [],
        };
      }
      if (stale && forceStaleOverride) {
        const who = normStr(by) || normStr(process.env.USER || "") || null;
        const why = normStr(reason) || normStr(closeNotes) || null;
        await appendFile(paths.laneA.ledgerAbs, JSON.stringify({ timestamp: nowISO(), type: "stale_override", command: "knowledge-review-meeting.confirm_sufficiency", scope: parsedScope.scope, by: who, reason: why }) + "\n");
      }
      if (!committeeReady.ok || !committeeReady.ready) return { ok: false, message: "Cannot close with confirm_sufficiency: committee is not evidence-valid." };
      if (!openOk) return { ok: false, message: "Cannot close with confirm_sufficiency: open decisions exist (add override_open_decisions to notes to override)." };

      const proposed = await runKnowledgeSufficiencyPropose({ projectRoot: paths.opsRootAbs, dryRun });
      if (!proposed.ok) return { ok: false, message: proposed.message || "knowledge-sufficiency-propose failed." };
      if (!dryRun) {
        await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "confirm_sufficiency", notes, sufficiency_proposed: true });
      }
    } else if (decision === "revise_scans") {
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "revise_scans", notes });
    } else if (decision === "open_decisions") {
      // Turn any unanswered meeting ladder tiers into decision packets (in knowledge repo).
      const answers = await readJsonlLines(mp.answersAbs);
      const answeredTiers = computeAnsweredTiers(answers);
      const missing = ladderQuestions({ parsedScope }).filter((q) => !answeredTiers.has(q.tier)).sort((a, b) => tierOrderIndex(a.tier) - tierOrderIndex(b.tier));
      const created = [];
      const maxPackets = Math.min(5, missing.length);
      for (let i = 0; i < maxPackets; i += 1) {
        const q = missing[i];
        const packet = buildDecisionPacket({
          scope: parsedScope.scope,
          trigger: "state_machine",
          blocking_state: "DECISION_NEEDED",
          context_summary: "Knowledge review meeting missing required SDLC-ordered inputs.",
          why_automation_failed: "Meeting was closed with decision=open_decisions to solicit missing prerequisite information from a human.",
          what_is_known: [],
          question: q.text,
          expected_answer_type: "text",
          constraints: "",
          blocks: ["COMMITTEE_PENDING"],
          assumptions_if_unanswered: "If unanswered, knowledge sufficiency cannot be proposed and delivery remains blocked.",
          created_at: nowISO(),
        });
        validateDecisionPacket(packet);
        const decisionsDirAbs = paths.knowledge.decisionsAbs;
        const jsonAbs = join(decisionsDirAbs, `DECISION-${packet.decision_id}.json`);
        const mdAbs = join(decisionsDirAbs, `DECISION-${packet.decision_id}.md`);
        if (!dryRun) {
          await mkdir(decisionsDirAbs, { recursive: true });
          await writeTextAtomic(jsonAbs, JSON.stringify(packet, null, 2) + "\n");
          await writeTextAtomic(mdAbs, renderDecisionPacketMd(packet));
        }
        created.push(packet.decision_id);
      }
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "open_decisions", notes, created_decisions: created });
    } else if (decision === "abort") {
      if (!dryRun) await appendJsonl(mp.decisionsAbs, { version: 1, decided_at: nowISO(), decision: "abort", notes });
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
      await writeTextAtomicLocal(mp.mdAbs, renderMeetingMd({ meeting: closed, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, committeeSummary: committeeReady.summary }));
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

  // Advance committee at most one step per run (deterministic).
  const committeeReady = await committeeIsReady({ projectRootAbs: paths.opsRootAbs, scope: parsedScope.scope });
  if (!committeeReady.ok) return { ok: false, message: committeeReady.message || "Failed to load committee status." };
  if (!committeeReady.ready) {
    if (parsedScope.kind === "system") {
      const sb = await ensureSystemBundleForIntegrationChair({ projectRootAbs: paths.opsRootAbs, paths, dryRun });
      if (!sb.ok) return { ok: false, message: sb.message };
    }
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
      await writeTextAtomicLocal(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, committeeSummary: committeeReady.summary }));
      await rm(mp.errorAbs, { force: true });
    }
    return { ok: true, meeting_id: next.meeting_id, status: next.status, progressed: "committee", wrote: !dryRun };
  }

  // Ask next single question (tier-ordered).
  const answers = await readJsonlLines(mp.answersAbs);
  const nq = nextQuestion({ parsedScope, answers, maxQuestions: maxQ, askedCount: meetingPrev.asked_count });
  if (!nq.ok) return { ok: false, message: "Failed to compute next question." };
  if (nq.done) {
    const next = { ...meetingPrev, status: "ready_to_close", updated_at: nowISO(), inputs };
    validateMeeting(next);
    if (!dryRun) {
      await writeJsonAtomic(mp.jsonAbs, next);
      await writeTextAtomicLocal(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, committeeSummary: committeeReady.summary }));
      await rm(mp.errorAbs, { force: true });
    }
    return { ok: true, meeting_id: next.meeting_id, status: next.status, ready_to_close: true, reason: nq.reason, wrote: !dryRun };
  }

  const q = nq.question;
  const questionText = normStr(q.text);
  const tier = q.tier;
  const qId = stableId("Q", [meetingPrev.meeting_id, String(meetingPrev.question_cursor), tier, questionText]).slice("Q_".length);
  const qLine = { version: 1, id: qId, meeting_id: meetingPrev.meeting_id, cursor: meetingPrev.question_cursor, tier, question: questionText, created_at: nowISO() };

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
    await writeTextAtomicLocal(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, committeeSummary: committeeReady.summary }));
    await rm(mp.errorAbs, { force: true });
  }

  return { ok: true, meeting_id: next.meeting_id, status: next.status, question: qLine, wrote: !dryRun };
}

export async function runKnowledgeReviewAnswer({ projectRoot, session, inputPath, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  if (!session) return { ok: false, message: "Missing --session \"<id>\"." };
  const inputAbs = resolve(String(inputPath || ""));
  if (!existsSync(inputAbs)) return { ok: false, message: `Missing --input file (${inputAbs}).` };

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const laneAMeetingsAbs = resolve(paths.laneA.rootAbs, "meetings");
  const mp = meetingPaths({ laneAMeetingsAbs, meetingDir: normStr(session) });
  if (!existsSync(mp.jsonAbs)) return { ok: false, message: `Meeting not found: ${mp.jsonAbs}` };

  const meeting = await loadMeeting({ meetingJsonAbs: mp.jsonAbs });
  if (meeting.status !== "waiting_for_answer") return { ok: false, message: `Meeting is not waiting_for_answer (status=${meeting.status}).` };

  const questions = await readJsonlLines(mp.questionsAbs);
  if (!questions.length) return { ok: false, message: "No questions have been asked." };
  const lastQ = questions[questions.length - 1];

  const rawAnswer = String(await readFile(inputAbs, "utf8") || "").trim();
  if (!rawAnswer) return { ok: false, message: "Answer is empty." };

  const answers = await readJsonlLines(mp.answersAbs);
  const alreadyAnswered = answers.some((a) => normStr(a?.question_id) === normStr(lastQ?.id));
  if (alreadyAnswered) return { ok: false, message: "Last question already has an answer." };

  const aLine = { version: 1, meeting_id: meeting.meeting_id, question_id: lastQ.id, tier: lastQ.tier, answered_at: nowISO(), answer: rawAnswer };

  const next = {
    ...meeting,
    status: "open",
    updated_at: nowISO(),
    answered_count: meeting.answered_count + 1,
  };
  validateMeeting(next);

  if (!dryRun) {
    await appendJsonl(mp.answersAbs, aLine);
    await writeJsonAtomic(mp.jsonAbs, next);
    const knowledgeStatus = await runKnowledgeStatus({ projectRoot: paths.opsRootAbs, dryRun: true });
    const suff = await readSufficiencyOrDefault({ projectRoot: paths.opsRootAbs });
    await writeTextAtomicLocal(mp.mdAbs, renderMeetingMd({ meeting: next, knowledgeStatus: { ...knowledgeStatus, sufficiency: suff.sufficiency }, committeeSummary: null }));
    await rm(mp.errorAbs, { force: true });
  }

  return { ok: true, meeting_id: next.meeting_id, status: next.status, answered: { question_id: lastQ.id, tier: lastQ.tier }, wrote: !dryRun };
}
