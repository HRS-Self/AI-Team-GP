import { createHash } from "node:crypto";
import { readdir, unlink } from "node:fs/promises";
import { join as joinPath } from "node:path";

import { assertGhReady, ghJson } from "../../github/gh.js";
import { resolveStatePath } from "../../project/state-paths.js";
import { ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { jsonStableStringify, sortKeysDeep } from "../../utils/json.js";
import { formatFsSafeUtcTimestamp } from "../../utils/naming.js";

export const DEFAULT_CI_FEEDBACK_PAIRS_TO_KEEP = 5;
export const DEFAULT_CI_STATUS_HISTORY_TO_KEEP = 20;

function nowISO() {
  return new Date().toISOString();
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function safeJsonParse(text, label) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid JSON in ${label} (${msg}).` };
  }
}

function repoFullNameFromPrJson(prJson) {
  const owner = typeof prJson?.owner === "string" ? prJson.owner.trim() : "";
  const repo = typeof prJson?.repo === "string" ? prJson.repo.trim() : "";
  if (!owner || !repo) return null;
  return `${owner}/${repo}`;
}

function normalizeCheckStatus(value) {
  const v = String(value || "").trim().toLowerCase();
  if (!v) return null;
  if (v === "queued" || v === "in_progress" || v === "completed") return v;
  if (v === "in progress") return "in_progress";
  return v;
}

function normalizeCheckConclusion(value) {
  const v = String(value || "").trim().toLowerCase();
  return v || null;
}

function checkIsFailing(conclusion) {
  const c = String(conclusion || "").trim().toLowerCase();
  return c === "failure" || c === "timed_out" || c === "cancelled" || c === "canceled" || c === "action_required";
}

function computeOverallFromChecks(checks) {
  const arr = Array.isArray(checks) ? checks : [];
  if (!arr.length) return "pending";
  const anyPending = arr.some((c) => {
    const st = String(c?.status || "").trim().toLowerCase();
    return st === "queued" || st === "in_progress";
  });
  if (anyPending) return "pending";
  const anyFail = arr.some((c) => checkIsFailing(c?.conclusion));
  if (anyFail) return "failed";
  return "success";
}

function extractTopErrorLinesFromSummary(summary, maxLines = 8) {
  const s = String(summary || "").trim();
  if (!s) return [];
  return s
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .slice(0, Math.max(0, Number(maxLines) || 8));
}

function capTotalErrorLines({ checks, maxTotalLines = 80, perCheckMax = 12 }) {
  let remaining = Math.max(0, Number(maxTotalLines) || 80);
  const out = [];
  for (const c of Array.isArray(checks) ? checks : []) {
    const take = Math.min(Math.max(0, Number(perCheckMax) || 12), remaining);
    const kept = Array.isArray(c.top_error_lines) ? c.top_error_lines.slice(0, take) : [];
    remaining -= kept.length;
    out.push({ ...c, top_error_lines: kept });
  }
  return out;
}

function statusMaterialPayload(statusJson) {
  const s = statusJson && typeof statusJson === "object" ? { ...statusJson } : {};
  delete s.captured_at;
  delete s.latest_feedback;
  return sortKeysDeep(s);
}

export function computeCiSnapshotHash(statusJson) {
  return sha256Hex(jsonStableStringify(statusMaterialPayload(statusJson), 0));
}

async function readJsonIfExists(path) {
  const text = await readTextIfExists(path);
  if (!text) return { ok: true, exists: false, json: null, text: null };
  const parsed = safeJsonParse(text, path);
  if (!parsed.ok) return { ok: false, exists: true, message: parsed.message };
  return { ok: true, exists: true, json: parsed.json, text };
}

export async function capCiStatusHistory({ historyPath, maxEntries }) {
  const res = await readJsonIfExists(historyPath);
  if (!res.ok) return;
  const arr = Array.isArray(res.json) ? res.json : [];
  const cap = Math.max(0, Number(maxEntries) || 50);
  const next = arr.length > cap ? arr.slice(arr.length - cap) : arr;
  await writeText(historyPath, JSON.stringify(next, null, 2) + "\n");
}

export async function capCiFeedbackFiles({ ciDir, maxPairs }) {
  const cap = Math.max(0, Number(maxPairs) || 10);
  const dirAbs = resolveStatePath(ciDir, { requiredRoot: true });
  let entries;
  try {
    entries = await readdir(dirAbs, { withFileTypes: true });
  } catch (err) {
    if (err && typeof err === "object" && "code" in err && err.code === "ENOENT") return;
    throw err;
  }
  const jsons = entries.filter((e) => e.isFile() && e.name.startsWith("feedback_") && e.name.endsWith(".json")).map((e) => e.name);
  const bases = jsons
    .map((n) => n.replace(/\.json$/i, ""))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  if (bases.length <= cap) return;
  const remove = bases.slice(0, Math.max(0, bases.length - cap));
  for (const b of remove) {
    await unlink(joinPath(dirAbs, `${b}.json`)).catch(() => {});
    await unlink(joinPath(dirAbs, `${b}.md`)).catch(() => {});
  }
}

function renderFeedbackMd({ status, feedback }) {
  const lines = [];
  lines.push(`# CI feedback`);
  lines.push("");
  lines.push(`Work item: \`${feedback.workId}\``);
  lines.push(`PR: ${feedback.pr_url ? feedback.pr_url : `#${feedback.pr_number}`}`);
  lines.push(`Snapshot: \`${feedback.snapshot_id}\``);
  lines.push(`Overall: \`${status.overall}\``);
  lines.push("");
  const failing = (feedback.checks || []).filter((c) => checkIsFailing(c.conclusion));
  lines.push("## Failing checks");
  lines.push("");
  if (!failing.length) lines.push("- (none)");
  for (const c of failing) {
    const url = c.url ? ` (${c.url})` : "";
    lines.push(`- \`${c.name}\`: \`${c.conclusion || "unknown"}\`${url}`);
  }
  lines.push("");
  lines.push("## Top error lines");
  lines.push("");
  const capped = capTotalErrorLines({ checks: failing, maxTotalLines: 80, perCheckMax: 12 });
  const anyLines = capped.some((c) => Array.isArray(c.top_error_lines) && c.top_error_lines.length);
  if (!anyLines) {
    lines.push("(none)");
    lines.push("");
  } else {
    for (const c of capped) {
      const lns = Array.isArray(c.top_error_lines) ? c.top_error_lines : [];
      if (!lns.length) continue;
      lines.push(`### ${c.name}`);
      lines.push("");
      lines.push("```");
      for (const l of lns) lines.push(l);
      lines.push("```");
      lines.push("");
    }
  }
  lines.push("## Next action");
  lines.push("");
  lines.push(String(feedback.suggested_next_action || "").trim() || "(none)");
  lines.push("");
  return lines.join("\n");
}

function suggestedNextAction(overall) {
  if (overall === "pending") return "Wait for CI to finish and poll again.";
  if (overall === "failed") return "Review failing checks and push fix commits to the SAME PR branch (no new work items).";
  if (overall === "success") return "CI is green. You may request merge-approval (merge permission).";
  return "Poll CI again.";
}

function parseChecksFromRollup(rollup) {
  const out = [];
  for (const item of Array.isArray(rollup) ? rollup : []) {
    const name = typeof item?.name === "string" ? item.name.trim() : "";
    if (!name) continue;
    const status = normalizeCheckStatus(item?.status || item?.state);
    const conclusion = normalizeCheckConclusion(item?.conclusion || item?.state);
    const url = typeof item?.detailsUrl === "string" ? item.detailsUrl : typeof item?.link === "string" ? item.link : null;
    const requiredRaw =
      typeof item?.isRequired === "boolean"
        ? item.isRequired
        : typeof item?.required === "boolean"
          ? item.required
          : typeof item?.is_required === "boolean"
            ? item.is_required
            : null;
    const required = typeof requiredRaw === "boolean" ? requiredRaw : null;
    const summary = typeof item?.description === "string" ? item.description : typeof item?.summary === "string" ? item.summary : null;
    const top_error_lines = extractTopErrorLinesFromSummary(summary);
    out.push({ name, status, conclusion, url, required, summary: summary ? String(summary) : null, top_error_lines });
  }
  return out.sort((a, b) => a.name.localeCompare(b.name));
}

export async function pollCiForWork({ workId, maxFeedbackPairs = DEFAULT_CI_FEEDBACK_PAIRS_TO_KEEP, maxStatusHistory = DEFAULT_CI_STATUS_HISTORY_TO_KEEP } = {}) {
  assertGhReady();

  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };

  const prPath = `ai/lane_b/work/${wid}/PR.json`;
  const prText = await readTextIfExists(prPath);
  if (!prText) return { ok: false, message: `Missing ${prPath}.` };
  const prParsed = safeJsonParse(prText, prPath);
  if (!prParsed.ok) return { ok: false, message: prParsed.message };
  const prJson = prParsed.json;

  const repoFullName = repoFullNameFromPrJson(prJson);
  const prNumber = typeof prJson?.pr_number === "number" ? prJson.pr_number : Number.parseInt(String(prJson?.pr_number || "").trim(), 10);
  if (!repoFullName) return { ok: false, message: `PR.json missing owner/repo (${prPath}).` };
  if (!Number.isFinite(prNumber) || prNumber <= 0) return { ok: false, message: `PR.json missing/invalid pr_number (${prPath}).` };

  const prView = ghJson(
    ["pr", "view", String(prNumber), "--repo", repoFullName, "--json", "number,url,baseRefName,headRefName,headRefOid,state,statusCheckRollup"],
    { label: "gh pr view --json statusCheckRollup" },
  );

  const headBranch = typeof prView?.headRefName === "string" ? prView.headRefName : (typeof prJson?.head_branch === "string" ? prJson.head_branch : null);
  const baseBranch = typeof prView?.baseRefName === "string" ? prView.baseRefName : (typeof prJson?.base_branch === "string" ? prJson.base_branch : null);
  const headSha = typeof prView?.headRefOid === "string" ? prView.headRefOid : null;
  const prUrl = typeof prView?.url === "string" ? prView.url : (typeof prJson?.url === "string" ? prJson.url : null);
  if (!headBranch || !baseBranch) return { ok: false, message: "Unable to resolve head/base branch from gh pr view." };

  const checks = parseChecksFromRollup(prView?.statusCheckRollup);
  const overall = computeOverallFromChecks(checks);
  const failingChecks = checks.filter((c) => checkIsFailing(c.conclusion));

  const snapshotId = formatFsSafeUtcTimestamp(new Date());
  const feedbackBase = `feedback_${snapshotId}`;

  const ciDir = `ai/lane_b/work/${wid}/CI`;
  await ensureDir(ciDir);
  const statusPath = `${ciDir}/CI_Status.json`;
  const historyPath = `${ciDir}/CI_Status_History.json`;
  const legacyStatusPath = `${ciDir}/status.json`;
  const legacyHistoryPath = `${ciDir}/status_history.json`;

  const nextStatus = {
    version: 1,
    workId: wid,
    pr_number: prNumber,
    head_sha: headSha,
    captured_at: nowISO(),
    overall,
    checks: checks.map((c) => ({ name: c.name, status: c.status, conclusion: c.conclusion, url: c.url || null, required: typeof c.required === "boolean" ? c.required : null })),
    latest_feedback: null,
  };

  const existingNewRes = await readJsonIfExists(statusPath);
  if (!existingNewRes.ok) return { ok: false, message: existingNewRes.message };
  const existingLegacyRes = existingNewRes.exists ? { ok: true, exists: false, json: null } : await readJsonIfExists(legacyStatusPath);
  if (!existingLegacyRes.ok) return { ok: false, message: existingLegacyRes.message };

  const hasNew = existingNewRes.exists;
  const hasLegacy = existingLegacyRes.exists;
  const existingStatus = hasNew ? existingNewRes.json : hasLegacy ? existingLegacyRes.json : null;

  const nextHash = computeCiSnapshotHash(nextStatus);
  const existingHash = existingStatus ? computeCiSnapshotHash(existingStatus) : null;

  const shouldWriteNewSnapshot = !existingHash || nextHash !== existingHash || (!hasNew && hasLegacy);

  const feedbackJson = {
    workId: wid,
    owner: prJson.owner || null,
    repo: prJson.repo || null,
    pr_number: prNumber,
    pr_url: prUrl,
    head_branch: headBranch,
    base_branch: baseBranch,
    snapshot_id: snapshotId,
    conclusion: overall,
    checks: failingChecks.map((c) => ({
      name: c.name,
      status: c.status,
      conclusion: c.conclusion,
      url: c.url,
      required: typeof c.required === "boolean" ? c.required : null,
      summary: c.summary || null,
      top_error_lines: Array.isArray(c.top_error_lines) ? c.top_error_lines : [],
    })),
    suggested_next_action: suggestedNextAction(overall),
  };

  if (!shouldWriteNewSnapshot) {
    return {
      ok: true,
      workId: wid,
      overall,
      wrote_new_snapshot: false,
      status_path: statusPath,
      snapshot_hash: nextHash,
      latest_feedback: existingStatus?.latest_feedback || null,
    };
  }

  // Append previous status snapshot to history before overwriting, and cap.
  if (existingStatus) {
    // Migrate legacy history file on first run after renaming.
    if (!hasNew && hasLegacy) {
      const legacyHistText = await readTextIfExists(legacyHistoryPath);
      const newHistText = await readTextIfExists(historyPath);
      if (!newHistText && legacyHistText) await writeText(historyPath, legacyHistText);
    }

    const histRes = await readJsonIfExists(historyPath);
    const arr = histRes.ok && Array.isArray(histRes.json) ? histRes.json.slice() : [];
    arr.push(existingStatus);
    await writeText(historyPath, JSON.stringify(arr, null, 2) + "\n");
    await capCiStatusHistory({ historyPath, maxEntries: maxStatusHistory });
  } else {
    const histRes = await readJsonIfExists(historyPath);
    if (!histRes.exists) await writeText(historyPath, "[]\n");
  }

  let feedbackJsonPath = null;
  let feedbackMdPath = null;
  const statusToWrite = { ...nextStatus };

  if (overall === "failed") {
    statusToWrite.latest_feedback = feedbackBase;
    feedbackJsonPath = `${ciDir}/${feedbackBase}.json`;
    feedbackMdPath = `${ciDir}/${feedbackBase}.md`;
    await writeText(feedbackJsonPath, JSON.stringify(feedbackJson, null, 2) + "\n");
    await writeText(feedbackMdPath, renderFeedbackMd({ status: statusToWrite, feedback: feedbackJson }));
    await capCiFeedbackFiles({ ciDir, maxPairs: maxFeedbackPairs });
  }

  await writeText(statusPath, JSON.stringify(statusToWrite, null, 2) + "\n");

  // Cleanup legacy CI snapshot filenames to reduce ambiguity.
  if (!hasNew && hasLegacy) {
    try {
      await unlink(resolveStatePath(legacyStatusPath));
    } catch {
      // ignore
    }
    try {
      const legacyHistText = await readTextIfExists(legacyHistoryPath);
      if (legacyHistText) await unlink(resolveStatePath(legacyHistoryPath));
    } catch {
      // ignore
    }
  }

  return {
    ok: true,
    workId: wid,
    pr_number: prNumber,
    repo_full_name: repoFullName,
    overall,
    wrote_new_snapshot: true,
    status_path: statusPath,
    snapshot_hash: nextHash,
    status_history_path: historyPath,
    ...(feedbackJsonPath ? { feedback_json: feedbackJsonPath } : {}),
    ...(feedbackMdPath ? { feedback_md: feedbackMdPath } : {}),
    latest_feedback: statusToWrite.latest_feedback,
  };
}
