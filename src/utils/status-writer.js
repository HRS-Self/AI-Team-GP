import { readTextIfExists, writeText } from "./fs.js";
import { resolveStatePath } from "../project/state-paths.js";
import { readdir } from "node:fs/promises";
import { appendStatusHistory } from "./status-json-history.js";
import { jsonStableStringify } from "./json.js";
import { nowFsSafeUtcTimestamp } from "./naming.js";

export const WORK_STAGES = [
  "INTAKE_RECEIVED",
  "ROUTED",
  "TASKS_CREATED",
  "SWEEP_READY",
  "PROPOSED",
  "BUNDLED",
  "PATCH_PLANNED",
  "QA_PLANNED",
  "APPLY_APPROVAL_PENDING",
  "APPLY_APPROVAL_APPROVED",
  "APPLYING",
  "APPLIED",
  "CI_PENDING",
  "CI_FAILED",
  "CI_FIXING",
  "CI_GREEN",
  "MERGE_APPROVAL_PENDING",
  "MERGE_APPROVAL_APPROVED",
  "MERGED",
  "DONE",
  // Legacy stages kept for existing works / backward-compat aliasing.
  "GATE_A_PENDING",
  "GATE_A_APPROVED",
  "GATE_B_PENDING",
  "APPROVED_TO_MERGE",
  "APPROVAL_REQUESTED",
  "APPROVAL_REQUIRED",
  "APPROVED",
  "REJECTED",
  // Renamed plan-approval stages (new).
  "PLAN_APPROVAL_REQUESTED",
  "PLAN_APPROVAL_REQUIRED",
  "PLAN_APPROVED",
  "PR_OPENED",
  "CI_RUNNING",
  "FAILED",
  "BLOCKED",
  "COMPLETED",
];

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function sortObjectKeys(obj) {
  const out = {};
  for (const k of Object.keys(obj || {}).sort((a, b) => a.localeCompare(b))) out[k] = obj[k];
  return out;
}

function normalizeSnapshot(existing) {
  const e = isPlainObject(existing) ? existing : {};
  const history = Array.isArray(e.history) ? e.history : [];
  const artifacts = isPlainObject(e.artifacts) ? e.artifacts : {};
  const repos = isPlainObject(e.repos) ? e.repos : {};

  return {
    work_id: typeof e.work_id === "string" ? e.work_id : null,
    current_stage: typeof e.current_stage === "string" ? e.current_stage : null,
    last_updated: typeof e.last_updated === "string" ? e.last_updated : null,
    blocked: !!e.blocked,
    blocking_reason: typeof e.blocking_reason === "string" ? e.blocking_reason : null,
    artifacts: artifacts,
    repos,
    history: history.filter((h) => isPlainObject(h) && typeof h.timestamp === "string" && typeof h.stage === "string"),
  };
}

function parseSnapshotFromStatusMd(md) {
  const text = String(md || "");
  const m = text.match(/<!--\s*STATUS_SNAPSHOT_BEGIN\s*-->\s*```json\s*([\s\S]*?)\s*```\s*<!--\s*STATUS_SNAPSHOT_END\s*-->/);
  if (!m) return null;
  try {
    return JSON.parse(m[1]);
  } catch {
    return null;
  }
}

export async function readWorkStatusSnapshot(workId) {
  const path = `ai/lane_b/work/${workId}/STATUS.md`;
  const text = await readTextIfExists(path);
  if (!text) return { ok: false, missing: true, path, snapshot: null };
  const parsed = parseSnapshotFromStatusMd(text);
  if (!parsed) return { ok: false, missing: false, path, snapshot: null };
  return { ok: true, missing: false, path, snapshot: normalizeSnapshot(parsed) };
}

function renderArtifactsMd(artifacts) {
  const keys = Object.keys(artifacts || {}).sort((a, b) => a.localeCompare(b));
  if (!keys.length) return ["- (none)"];

  const out = [];
  for (const k of keys) {
    const v = artifacts[k];
    if (typeof v === "string" && v.trim()) out.push(`- ${k}: \`${v.trim()}\``);
    else if (Array.isArray(v) && v.length && v.every((x) => typeof x === "string")) out.push(`- ${k}: ${v.map((x) => `\`${x}\``).join(", ")}`);
    else if (v === null) out.push(`- ${k}: null`);
    else out.push(`- ${k}: (unrecognized)`);
  }
  return out;
}

function renderHistoryMd(history) {
  const h = Array.isArray(history) ? history : [];
  if (!h.length) return ["- (none)"];
  return h
    .slice()
    .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)))
    .map((x) => `- ${x.timestamp} - ${x.stage}${x.note ? ` - ${x.note}` : ""}`);
}

function renderReposMd(repos) {
  const keys = Object.keys(repos || {}).sort((a, b) => a.localeCompare(b));
  if (!keys.length) return ["- (none)"];
  const out = [];
  for (const repoId of keys) {
    const r = repos[repoId];
    if (!isPlainObject(r)) {
      out.push(`- ${repoId}: (unrecognized)`);
      continue;
    }
    const applied = r.applied === true ? "yes" : r.applied === false ? "no" : "(unknown)";
    const lint = typeof r.local_lint === "string" ? r.local_lint : "(unknown)";
    const pr = r.pr && isPlainObject(r.pr) ? (r.pr.url ? `yes (${r.pr.url})` : "yes") : r.pr_created === true ? "yes" : "no";
    const ci = typeof r.ci_status === "string" ? r.ci_status : "(unknown)";
    const ready = r.ready_to_merge === true ? "yes" : r.ready_to_merge === false ? "no" : "(unknown)";
    out.push(`- ${repoId}: applied=${applied}, local_lint=${lint}, pr_created=${pr}, ci_status=${ci}, ready_to_merge=${ready}`);
  }
  return out;
}

export function renderWorkStatusMd(snapshot) {
  const s = normalizeSnapshot(snapshot);
  const snapForJson = sortObjectKeys({
    work_id: s.work_id,
    current_stage: s.current_stage,
    last_updated: s.last_updated,
    blocked: s.blocked,
    blocking_reason: s.blocking_reason,
    artifacts: sortObjectKeys(s.artifacts || {}),
    repos: sortObjectKeys(s.repos || {}),
    history: s.history || [],
  });

  return [
    "# STATUS",
    "",
    "<!-- STATUS_SNAPSHOT_BEGIN -->",
    "```json",
    JSON.stringify(snapForJson, null, 2),
    "```",
    "<!-- STATUS_SNAPSHOT_END -->",
    "",
    "## Current",
    "",
    `- current_stage: \`${s.current_stage || "(missing)"}\``,
    `- last_updated: \`${s.last_updated || "(missing)"}\``,
    `- blocked: \`${s.blocked ? "yes" : "no"}\``,
    ...(s.blocking_reason ? [`- blocking_reason: ${s.blocking_reason}`] : []),
    "",
    "## Artifacts",
    "",
    ...renderArtifactsMd(s.artifacts || {}),
    "",
    "## Repos",
    "",
    ...renderReposMd(s.repos || {}),
    "",
    "## Stage history",
    "",
    ...renderHistoryMd(s.history || []),
    "",
  ].join("\n");
}

export async function updateWorkStatus({ workId, stage, blocked = null, blockingReason = null, artifacts = null, repos = null, note = null, appendHistory = false }) {
  const workDir = `ai/lane_b/work/${workId}`;
  const path = `${workDir}/STATUS.md`;

  const existingRes = await readWorkStatusSnapshot(workId);
  const prev = existingRes.ok ? existingRes.snapshot : normalizeSnapshot({ work_id: workId, current_stage: null, last_updated: null, blocked: false, blocking_reason: null, artifacts: {}, history: [] });

  const ts = nowISO();
  const next = normalizeSnapshot(prev);
  next.work_id = workId;
  next.last_updated = ts;
  next.current_stage = stage;

  if (typeof blocked === "boolean") next.blocked = blocked;
  else next.blocked = stage === "BLOCKED";

  next.blocking_reason = next.blocked ? String(blockingReason || prev.blocking_reason || "").trim() || null : null;

  if (isPlainObject(artifacts)) {
    next.artifacts = { ...(isPlainObject(prev.artifacts) ? prev.artifacts : {}), ...artifacts };
  } else {
    next.artifacts = isPlainObject(prev.artifacts) ? prev.artifacts : {};
  }

  if (isPlainObject(repos)) {
    const prevRepos = isPlainObject(prev.repos) ? prev.repos : {};
    const nextRepos = { ...prevRepos };
    for (const [repoId, update] of Object.entries(repos)) {
      if (!repoId) continue;
      if (isPlainObject(update)) nextRepos[repoId] = { ...(isPlainObject(prevRepos[repoId]) ? prevRepos[repoId] : {}), ...update };
      else nextRepos[repoId] = update;
    }
    next.repos = nextRepos;
  } else {
    next.repos = isPlainObject(prev.repos) ? prev.repos : {};
  }

  const lastStage = prev.current_stage;
  if (stage && (appendHistory || stage !== lastStage)) {
    const history = Array.isArray(prev.history) ? prev.history.slice() : [];
    const nextNote = note ? String(note) : null;
    const last = history.length ? history[history.length - 1] : null;
    const isDup = last && last.stage === stage && String(last.note || "") === String(nextNote || "");
    if (!isDup) history.push({ timestamp: ts, stage, ...(nextNote ? { note: nextNote } : {}) });
    next.history = history;
  }

  await writeText(path, renderWorkStatusMd(next));

  // Maintain deterministic JSON checkpoint under the work folder.
  // This file is used by apply resume/troubleshooting and should reflect stage transitions too.
  {
    const statusJsonPath = `${workDir}/status.json`;
    const statusHistoryPath = `${workDir}/status-history.json`;
    const statusText = await readTextIfExists(statusJsonPath);
    let statusJson = null;
    let hasExisting = false;
    try {
      statusJson = statusText ? JSON.parse(statusText) : null;
      hasExisting = !!statusText;
    } catch {
      statusJson = null;
      hasExisting = !!statusText;
    }
    if (!statusJson || typeof statusJson !== "object") statusJson = { workId, repos: {} };
    const prevJsonStage = typeof statusJson.stage === "string" ? statusJson.stage.trim() : null;
    const nextJsonStage = typeof stage === "string" ? stage.trim() : "";
    if (nextJsonStage && nextJsonStage !== prevJsonStage) {
      if (hasExisting) await appendStatusHistory({ statusPath: statusJsonPath, historyPath: statusHistoryPath });
      statusJson.workId = workId;
      statusJson.stage = nextJsonStage;
      statusJson.updated_at = nowFsSafeUtcTimestamp();
      await writeText(statusJsonPath, jsonStableStringify(statusJson, 2));
    }
  }
  return { ok: true, workId, path, snapshot: next };
}

function extractWorkIdsFromPortfolioSectionLines(lines) {
  const ids = [];
  for (const line of lines) {
    const m = String(line).match(/^\s*-\s*(W-[^\s]+)\s+\|/);
    if (m && !ids.includes(m[1])) ids.push(m[1]);
  }
  return ids;
}

export function workIdsFromPortfolio(portfolioMd) {
  const lines = String(portfolioMd || "").split("\n");
  const sections = [];
  let current = null;
  for (const l of lines) {
    const m = l.match(/^##\s+(.+?)\s*$/);
    if (m) {
      if (current) sections.push(current);
      current = { heading: m[1].trim(), lines: [] };
      continue;
    }
    if (current) current.lines.push(l);
  }
  if (current) sections.push(current);

  const out = [];
  for (const s of sections) {
    const ids = extractWorkIdsFromPortfolioSectionLines(s.lines);
    for (const id of ids) out.push({ workId: id, section: s.heading });
  }
  return out;
}

export async function writeGlobalStatusFromPortfolio() {
  const portfolioText = await readTextIfExists("ai/lane_b/PORTFOLIO.md");
  if (!portfolioText) return { ok: false, message: "ai/lane_b/PORTFOLIO.md missing; cannot render ai/lane_b/STATUS.md." };

  const entries = workIdsFromPortfolio(portfolioText);
  const unique = [];
  const seen = new Set();
  for (const e of entries) {
    if (seen.has(e.workId)) continue;
    seen.add(e.workId);
    unique.push(e);
  }

  const rows = [];
  for (const e of unique) {
    const status = await readWorkStatusSnapshot(e.workId);
    const snap = status.ok ? status.snapshot : null;
    rows.push({
      workId: e.workId,
      stage: snap?.current_stage || "(missing)",
      last_updated: snap?.last_updated || "(missing)",
      blocked: snap?.blocked ? "yes" : "no",
      blocking_reason: snap?.blocking_reason || "",
      path: `ai/lane_b/work/${e.workId}/`,
      section: e.section,
    });
  }

  const lines = [];
  lines.push("# STATUS");
  lines.push("");
  lines.push(`Last updated: ${nowISO()}`);
  lines.push("");
  if (!rows.length) {
    lines.push("No work items found in ai/lane_b/PORTFOLIO.md.");
    lines.push("");
    await writeText("ai/lane_b/STATUS.md", lines.join("\n"));
    return { ok: true, path: "ai/lane_b/STATUS.md", rows: 0 };
  }

  lines.push("| workId | current_stage | last_updated | blocked | reason | path |");
  lines.push("|---|---|---|---|---|---|");
  for (const r of rows) {
    const reason = String(r.blocking_reason || "").replace(/\|/g, "\\|");
    lines.push(`| ${r.workId} | ${r.stage} | ${r.last_updated} | ${r.blocked} | ${reason} | \`${r.path}\` |`);
  }
  lines.push("");

  await writeText("ai/lane_b/STATUS.md", lines.join("\n"));
  return { ok: true, path: "ai/lane_b/STATUS.md", rows: rows.length };
}

export async function listWorkIdsDesc() {
  try {
    const entries = await readdir(resolveStatePath("ai/lane_b/work"), { withFileTypes: true });
    return entries
      .filter((e) => e.isDirectory() && e.name.startsWith("W-"))
      .map((e) => e.name)
      .sort((a, b) => b.localeCompare(a));
  } catch (err) {
    if (err && err.code === "ENOENT") return [];
    throw err;
  }
}
