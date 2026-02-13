import { randomBytes } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { jsonStableStringify } from "../../utils/json.js";
import { runKnowledgeUpdateMeeting } from "../knowledge/knowledge-update-meeting.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function nowISO(now = null) {
  if (now instanceof Date) return now.toISOString();
  if (typeof now === "string" && now.trim()) {
    const d = new Date(now);
    if (!Number.isNaN(d.getTime())) return d.toISOString();
  }
  return new Date().toISOString();
}

function isoMs(iso) {
  const ms = Date.parse(String(iso || "").trim());
  return Number.isFinite(ms) ? ms : null;
}

function boolEnv(name, def = true) {
  const raw = normStr(process.env[name]);
  if (!raw) return !!def;
  const low = raw.toLowerCase();
  if (low === "0" || low === "false" || low === "no" || low === "off") return false;
  if (low === "1" || low === "true" || low === "yes" || low === "on") return true;
  return !!def;
}

function intEnv(name, def, min, max) {
  const raw = normStr(process.env[name]);
  const parsed = raw ? Number.parseInt(raw, 10) : Number.parseInt(String(def), 10);
  if (!Number.isFinite(parsed)) return def;
  return Math.max(min, Math.min(max, parsed));
}

function enumEnv(name, def, allowed) {
  const raw = normStr(process.env[name]).toLowerCase();
  if (!raw) return def;
  return allowed.has(raw) ? raw : def;
}

function readSoftStalePolicy() {
  return {
    bannerEnabled: boolEnv("LANE_A_SOFT_STALE_BANNER", true),
    escalateAfterMinutes: intEnv("LANE_A_SOFT_STALE_ESCALATE_AFTER_MINUTES", 180, 1, 24 * 60 * 30),
    escalateMode: enumEnv("LANE_A_SOFT_STALE_ESCALATE_MODE", "update_meeting", new Set(["update_meeting", "decision_packet"])),
    escalateCapPerDay: intEnv("LANE_A_SOFT_STALE_ESCALATE_CAP_PER_DAY", 3, 0, 1000),
  };
}

function toRelOpsPath(paths, absPath) {
  const rel = relative(paths.opsRootAbs, resolve(String(absPath || "")));
  return rel.split(sep).join("/");
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

function snapshotReasonCodes(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return [];
  const src = Array.isArray(snapshot.stale_reasons) ? snapshot.stale_reasons : Array.isArray(snapshot.reasons) ? snapshot.reasons : [];
  return Array.from(new Set(src.map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function snapshotSoftStale(snapshot) {
  const stale = snapshot?.stale === true;
  const hard = snapshot?.hard_stale === true;
  return stale && !hard;
}

function repoIdFromScope(scope) {
  const m = /^repo:([A-Za-z0-9._-]+)$/.exec(normStr(scope));
  return m ? m[1] : null;
}

function staleRepoIdsFromSnapshot({ scope, stalenessSnapshot }) {
  const out = new Set();
  const scopedRepo = repoIdFromScope(scope);
  if (scopedRepo) {
    if (stalenessSnapshot?.stale === true) out.add(scopedRepo);
    return Array.from(out).sort((a, b) => a.localeCompare(b));
  }
  const fromList = Array.isArray(stalenessSnapshot?.stale_repos) ? stalenessSnapshot.stale_repos : [];
  for (const id of fromList) {
    const s = normStr(id);
    if (s) out.add(s);
  }
  if (!out.size && normStr(stalenessSnapshot?.repo_id) && stalenessSnapshot?.stale === true) out.add(normStr(stalenessSnapshot.repo_id));
  return Array.from(out).sort((a, b) => a.localeCompare(b));
}

function hardStaleRepoIdsFromSnapshot({ scope, stalenessSnapshot }) {
  const out = new Set();
  const scopedRepo = repoIdFromScope(scope);
  if (scopedRepo) {
    if (stalenessSnapshot?.hard_stale === true) out.add(scopedRepo);
    return Array.from(out).sort((a, b) => a.localeCompare(b));
  }
  const fromList = Array.isArray(stalenessSnapshot?.hard_stale_repos) ? stalenessSnapshot.hard_stale_repos : [];
  for (const id of fromList) {
    const s = normStr(id);
    if (s) out.add(s);
  }
  if (!out.size && normStr(stalenessSnapshot?.repo_id) && stalenessSnapshot?.hard_stale === true) out.add(normStr(stalenessSnapshot.repo_id));
  return Array.from(out).sort((a, b) => a.localeCompare(b));
}

function defaultTracker(projectRoot) {
  return {
    version: 1,
    projectRoot: projectRoot,
    updated_at: nowISO(),
    repos: {},
  };
}

function normalizeTracker(input, projectRoot) {
  const base = isPlainObject(input) ? input : {};
  const reposRaw = isPlainObject(base.repos) ? base.repos : {};
  const repos = {};
  for (const [repoIdRaw, entryRaw] of Object.entries(reposRaw)) {
    const repoId = normStr(repoIdRaw);
    if (!repoId || !isPlainObject(entryRaw)) continue;
    const escalationsIn = Array.isArray(entryRaw.escalations) ? entryRaw.escalations : [];
    const escalations = escalationsIn
      .map((e) => ({
        at: normStr(e?.at),
        mode: normStr(e?.mode),
        artifact: normStr(e?.artifact),
      }))
      .filter((e) => e.at && (e.mode === "update_meeting" || e.mode === "decision_packet") && e.artifact)
      .sort((a, b) => `${a.at}:${a.mode}:${a.artifact}`.localeCompare(`${b.at}:${b.mode}:${b.artifact}`));
    repos[repoId] = {
      first_seen_at: normStr(entryRaw.first_seen_at) || null,
      last_seen_at: normStr(entryRaw.last_seen_at) || null,
      current_reason_codes: Array.from(new Set((Array.isArray(entryRaw.current_reason_codes) ? entryRaw.current_reason_codes : []).map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
      escalations,
    };
  }
  return {
    version: 1,
    projectRoot: normStr(base.projectRoot) || projectRoot,
    updated_at: normStr(base.updated_at) || nowISO(),
    repos,
  };
}

async function readTracker(paths) {
  const abs = paths.laneA.softStaleTrackerAbs;
  if (!existsSync(abs)) return defaultTracker(paths.opsRootAbs);
  try {
    const raw = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    return normalizeTracker(raw, paths.opsRootAbs);
  } catch {
    return defaultTracker(paths.opsRootAbs);
  }
}

async function writeTracker(paths, tracker) {
  const normalized = normalizeTracker(tracker, paths.opsRootAbs);
  normalized.updated_at = nowISO();
  await writeTextAtomic(paths.laneA.softStaleTrackerAbs, jsonStableStringify(normalized, 2));
  return normalized;
}

function dayStampFromIso(iso) {
  return String(iso || "").slice(0, 10).replaceAll("-", "");
}

async function readDailyCounter(paths, dayStamp) {
  const abs = join(paths.laneA.stalenessAbs, `soft_stale_escalations_${dayStamp}.json`);
  if (!existsSync(abs)) return { abs, counter: { version: 1, count: 0, artifacts: [] } };
  try {
    const parsed = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    const count = Number.isFinite(Number(parsed?.count)) ? Math.max(0, Math.floor(Number(parsed.count))) : 0;
    const artifacts = Array.isArray(parsed?.artifacts) ? parsed.artifacts.map((x) => normStr(x)).filter(Boolean) : [];
    return { abs, counter: { version: 1, count, artifacts } };
  } catch {
    return { abs, counter: { version: 1, count: 0, artifacts: [] } };
  }
}

async function writeDailyCounter(abs, counter) {
  const next = {
    version: 1,
    count: Number.isFinite(Number(counter?.count)) ? Math.max(0, Math.floor(Number(counter.count))) : 0,
    artifacts: Array.from(new Set((Array.isArray(counter?.artifacts) ? counter.artifacts : []).map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
  };
  await writeTextAtomic(abs, jsonStableStringify(next, 2));
  return next;
}

async function pruneOldStatusFiles(paths) {
  const dirAbs = paths.laneA.stalenessAbs;
  if (!existsSync(dirAbs)) return;
  const entries = await readdir(dirAbs, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && /^soft_stale_escalations_\d{8}\.json$/.test(e.name))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
  const maxKeep = 30;
  const overflow = files.length - maxKeep;
  if (overflow <= 0) return;
  for (const name of files.slice(0, overflow)) {
    // eslint-disable-next-line no-await-in-loop
    await rm(join(dirAbs, name), { force: true });
  }
}

function firstRepoInfo(repoInfos) {
  const list = Array.isArray(repoInfos) ? repoInfos : [];
  return list
    .slice()
    .sort((a, b) => normStr(a?.repo_id).localeCompare(normStr(b?.repo_id)))[0] || null;
}

function selectBannerFields({ stalenessSnapshot, repoSnapshot = null }) {
  const info = repoSnapshot && typeof repoSnapshot === "object" ? repoSnapshot : stalenessSnapshot && typeof stalenessSnapshot === "object" ? stalenessSnapshot : {};
  const reasons = snapshotReasonCodes(stalenessSnapshot);
  return {
    reasons,
    last_scan: normStr(info?.last_scan_time) || "unknown",
    last_merge_event: normStr(info?.last_merge_event_time) || "unknown",
    repo_head: normStr(info?.repo_head_sha) || "unknown",
    knowledge_head: normStr(info?.last_scanned_head_sha) || "unknown",
  };
}

export function renderSoftStaleBanner({ stalenessSnapshot, repoSnapshot = null } = {}) {
  const fields = selectBannerFields({ stalenessSnapshot, repoSnapshot });
  const reasonText = fields.reasons.length ? fields.reasons.join(", ") : "unknown";
  return [
    "---",
    "⚠️ SOFT-STALE KNOWLEDGE (DEGRADED OUTPUT)",
    "This output is based on knowledge that may be behind the current repo HEAD.",
    `Reason(s): ${reasonText}`,
    `Last scan: ${fields.last_scan}`,
    `Last merge event seen: ${fields.last_merge_event}`,
    `Repo head known: ${fields.repo_head}  Knowledge head: ${fields.knowledge_head}`,
    "Recommended action: run --knowledge-refresh-from-events OR start a knowledge update meeting.",
    "---",
    "",
  ].join("\n");
}

export function maybePrependSoftStaleBanner({ markdown, stalenessSnapshot, repoSnapshot = null } = {}) {
  const text = String(markdown || "");
  if (!boolEnv("LANE_A_SOFT_STALE_BANNER", true)) return text;
  if (!snapshotSoftStale(stalenessSnapshot)) return text;
  return `${renderSoftStaleBanner({ stalenessSnapshot, repoSnapshot })}${text}`;
}

async function findOpenMeetingForScope({ projectRoot, scope }) {
  const status = await runKnowledgeUpdateMeeting({
    projectRoot,
    mode: "status",
    scope,
    dryRun: false,
  });
  if (!status.ok) return null;
  const sessions = Array.isArray(status.sessions) ? status.sessions : [];
  const open = sessions
    .filter((s) => normStr(s?.scope) === scope && normStr(s?.status) !== "closed")
    .sort((a, b) => normStr(a?.created_at).localeCompare(normStr(b?.created_at)));
  return open.length ? open[0] : null;
}

async function openUpdateMeetingForScope({ paths, scope, nowIso }) {
  const existing = await findOpenMeetingForScope({ projectRoot: paths.opsRootAbs, scope });
  if (existing) {
    return {
      created: false,
      meeting_id: normStr(existing.meeting_id) || normStr(existing.dir) || null,
      artifact: `ai/lane_a/meetings/${normStr(existing.dir) || normStr(existing.meeting_id)}/MEETING.json`,
    };
  }

  const started = await runKnowledgeUpdateMeeting({
    projectRoot: paths.opsRootAbs,
    mode: "start",
    scope,
    dryRun: false,
  });
  if (!started.ok) {
    return { created: false, error: started.message || "Failed to open update meeting." };
  }

  const meetingId = normStr(started.meeting_id);
  const noticeAbs = join(paths.laneA.meetingsAbs, meetingId, "SOFT_STALE_NOTICE.md");
  const note = [
    "# Soft-stale persistence escalation",
    "",
    `Scope: ${scope}`,
    `CreatedAt: ${nowIso}`,
    "",
    "Knowledge is persistently soft-stale and requires human attention.",
    "Required action: run --knowledge-refresh-from-events and/or start a knowledge update cycle.",
    "",
  ].join("\n");
  await writeTextAtomic(noticeAbs, `${note}\n`);

  return {
    created: true,
    meeting_id: meetingId,
    artifact: toRelOpsPath(paths, noticeAbs),
  };
}

async function writeDecisionPacketMarkdown({ paths, repoId, reasonCodes, firstSeenAt, lastSeenAt, nowIso }) {
  await mkdir(paths.laneA.decisionPacketsAbs, { recursive: true });
  const day = dayStampFromIso(nowIso);
  const rand = randomBytes(4).toString("hex");
  const fileName = `DP-SOFT-STALE-${day}_${rand}.md`;
  const abs = join(paths.laneA.decisionPacketsAbs, fileName);
  const lines = [];
  lines.push("# Soft-stale escalation decision");
  lines.push("");
  lines.push(`repo_id: ${repoId}`);
  lines.push(`first_seen_at: ${firstSeenAt || "unknown"}`);
  lines.push(`last_seen_at: ${lastSeenAt || "unknown"}`);
  lines.push(`escalated_at: ${nowIso}`);
  lines.push(`reasons: ${(Array.isArray(reasonCodes) ? reasonCodes : []).join(", ") || "unknown"}`);
  lines.push("");
  lines.push("Required action:");
  lines.push("- Run `node src/cli.js --knowledge-refresh-from-events --projectRoot <abs>` OR");
  lines.push("- Run `node src/cli.js --knowledge-index --projectRoot <abs>` and `--knowledge-scan`, then re-run committee/writer.");
  lines.push("");
  await writeTextAtomic(abs, `${lines.join("\n")}\n`);
  return { artifact: toRelOpsPath(paths, abs), abs };
}

function withSortedRepoEntries(obj) {
  const repos = isPlainObject(obj?.repos) ? obj.repos : {};
  const sorted = {};
  for (const key of Object.keys(repos).sort((a, b) => a.localeCompare(b))) sorted[key] = repos[key];
  return { ...obj, repos: sorted };
}

function makeSystemSnapshotFromRepoInfos(repoInfos) {
  const list = Array.isArray(repoInfos) ? repoInfos : [];
  const staleRepos = list.filter((r) => r?.stale === true).map((r) => normStr(r?.repo_id)).filter(Boolean).sort((a, b) => a.localeCompare(b));
  const hardRepos = list.filter((r) => r?.hard_stale === true).map((r) => normStr(r?.repo_id)).filter(Boolean).sort((a, b) => a.localeCompare(b));
  return {
    scope: "system",
    stale: staleRepos.length > 0,
    hard_stale: hardRepos.length > 0,
    reasons: staleRepos.length ? ["repo_stale"] : [],
    stale_repos: staleRepos,
    hard_stale_repos: hardRepos,
  };
}

export async function handleSoftStaleEscalation({
  projectRoot = null,
  paths = null,
  stalenessSnapshot,
  scope,
  now = null,
  allowEscalation = true,
} = {}) {
  const resolvedPaths = paths || (await loadProjectPaths({ projectRoot }));
  const nowIso = nowISO(now);
  const policy = readSoftStalePolicy();
  const scopeNorm = normStr(scope) || normStr(stalenessSnapshot?.scope) || "system";
  const snapshot = isPlainObject(stalenessSnapshot) ? { ...stalenessSnapshot } : {};

  await mkdir(resolvedPaths.laneA.stalenessAbs, { recursive: true });
  await mkdir(resolvedPaths.laneA.decisionPacketsAbs, { recursive: true });

  const tracker = await readTracker(resolvedPaths);
  tracker.projectRoot = resolvedPaths.opsRootAbs;
  const staleRepoIds = staleRepoIdsFromSnapshot({ scope: scopeNorm, stalenessSnapshot: snapshot });
  const hardRepoIds = new Set(hardStaleRepoIdsFromSnapshot({ scope: scopeNorm, stalenessSnapshot: snapshot }));
  const soft = snapshotSoftStale(snapshot);
  const reasonCodes = snapshotReasonCodes(snapshot);

  if (soft) {
    for (const repoId of staleRepoIds) {
      const current = isPlainObject(tracker.repos[repoId]) ? tracker.repos[repoId] : null;
      tracker.repos[repoId] = {
        first_seen_at: normStr(current?.first_seen_at) || nowIso,
        last_seen_at: nowIso,
        current_reason_codes: reasonCodes.slice(),
        escalations: Array.isArray(current?.escalations) ? current.escalations.slice() : [],
      };
    }
  }

  // Resolve tracker entries when repos are no longer soft-stale.
  const scopedRepo = repoIdFromScope(scopeNorm);
  if (!soft && scopedRepo) delete tracker.repos[scopedRepo];
  if (scopeNorm === "system") {
    const keep = new Set(staleRepoIds.filter((id) => !hardRepoIds.has(id)));
    for (const existingRepoId of Object.keys(tracker.repos)) {
      if (!keep.has(existingRepoId)) delete tracker.repos[existingRepoId];
    }
  }
  for (const id of staleRepoIds) {
    if (hardRepoIds.has(id)) delete tracker.repos[id];
  }

  const normalizedTracker = await writeTracker(resolvedPaths, withSortedRepoEntries(tracker));
  const result = {
    ok: true,
    soft_stale: soft,
    scope: scopeNorm,
    stale_repos: staleRepoIds,
    tracker_path: toRelOpsPath(resolvedPaths, resolvedPaths.laneA.softStaleTrackerAbs),
    escalated: [],
  };

  if (!soft || !allowEscalation) return result;

  const thresholdMs = policy.escalateAfterMinutes * 60_000;
  const today = dayStampFromIso(nowIso);
  const counterRes = await readDailyCounter(resolvedPaths, today);
  let counter = counterRes.counter;

  const candidateRepoIds = staleRepoIds
    .filter((id) => !hardRepoIds.has(id))
    .sort((a, b) => a.localeCompare(b));

  for (const repoId of candidateRepoIds) {
    if (counter.count >= policy.escalateCapPerDay) break;
    const entry = normalizedTracker.repos[repoId];
    if (!entry) continue;
    const firstSeenMs = isoMs(entry.first_seen_at);
    const nowMs = isoMs(nowIso);
    if (!Number.isFinite(firstSeenMs) || !Number.isFinite(nowMs)) continue;
    if (nowMs - firstSeenMs < thresholdMs) continue;

    const alreadyEscalatedToday = (Array.isArray(entry.escalations) ? entry.escalations : []).some((e) => {
      const atDay = dayStampFromIso(normStr(e?.at));
      return atDay === today && normStr(e?.mode) === policy.escalateMode;
    });
    if (alreadyEscalatedToday) continue;

    let escalation = null;
    if (policy.escalateMode === "update_meeting") {
      // Use repo scope for persistent repo-level soft-stale visibility.
      const escalateScope = `repo:${repoId}`;
      const opened = await openUpdateMeetingForScope({
        paths: resolvedPaths,
        scope: escalateScope,
        nowIso,
      });
      if (!opened.error && opened.artifact && opened.created === true) {
        escalation = {
          at: nowIso,
          mode: "update_meeting",
          artifact: opened.artifact,
          repo_id: repoId,
          scope: escalateScope,
          created: opened.created === true,
        };
      }
    } else {
      const packet = await writeDecisionPacketMarkdown({
        paths: resolvedPaths,
        repoId,
        reasonCodes: entry.current_reason_codes || reasonCodes,
        firstSeenAt: entry.first_seen_at,
        lastSeenAt: entry.last_seen_at,
        nowIso,
      });
      escalation = {
        at: nowIso,
        mode: "decision_packet",
        artifact: packet.artifact,
        repo_id: repoId,
        scope: `repo:${repoId}`,
        created: true,
      };
    }

    if (!escalation) continue;
    entry.escalations = (Array.isArray(entry.escalations) ? entry.escalations : []).concat([
      { at: escalation.at, mode: escalation.mode, artifact: escalation.artifact },
    ]);
    counter.count += 1;
    counter.artifacts = Array.from(new Set([...(Array.isArray(counter.artifacts) ? counter.artifacts : []), escalation.artifact])).sort((a, b) => a.localeCompare(b));
    result.escalated.push(escalation);
  }

  await writeTracker(resolvedPaths, withSortedRepoEntries(normalizedTracker));
  await writeDailyCounter(counterRes.abs, counter);
  await pruneOldStatusFiles(resolvedPaths);

  return result;
}

export async function recordSoftStaleObservation({
  projectRoot = null,
  paths = null,
  stalenessSnapshot,
  scope,
  now = null,
} = {}) {
  return await handleSoftStaleEscalation({
    projectRoot,
    paths,
    stalenessSnapshot,
    scope,
    now,
    allowEscalation: false,
  });
}

export function buildSystemSoftStaleSnapshotFromRepoInfos(repoInfos) {
  return makeSystemSnapshotFromRepoInfos(repoInfos);
}

export function selectSoftStaleBannerRepoSnapshot(repoInfos) {
  return firstRepoInfo(repoInfos);
}

export function softStaleBannerEnabled() {
  return readSoftStalePolicy().bannerEnabled;
}
