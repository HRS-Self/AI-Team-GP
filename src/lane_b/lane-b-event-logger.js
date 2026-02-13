import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdir, appendFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";

function pad2(n) {
  return String(n).padStart(2, "0");
}

function utcSegmentName(d) {
  const dt = d instanceof Date ? d : new Date();
  const y = dt.getUTCFullYear();
  const mo = pad2(dt.getUTCMonth() + 1);
  const da = pad2(dt.getUTCDate());
  const hh = pad2(dt.getUTCHours());
  const mm = pad2(dt.getUTCMinutes());
  const ss = pad2(dt.getUTCSeconds());
  return `${y}${mo}${da}-${hh}${mm}${ss}.jsonl`;
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function ensureInt(n) {
  const x = typeof n === "number" ? n : Number.parseInt(String(n ?? ""), 10);
  return Number.isFinite(x) ? x : null;
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function uniqSortedStrings(xs) {
  const out = Array.isArray(xs) ? xs.map((x) => normStr(x)).filter(Boolean) : [];
  out.sort((a, b) => a.localeCompare(b));
  return out.filter((v, i) => i === 0 || v !== out[i - 1]);
}

function randomHex8() {
  return randomBytes(4).toString("hex");
}

function validateMergeEventShape(ev) {
  if (!ev || typeof ev !== "object") throw new Error("Invalid merge event: must be an object.");
  if (ev.version !== 1) throw new Error("Invalid merge event: version must be 1.");
  if (normStr(ev.type) !== "merge") throw new Error("Invalid merge event: type must be 'merge'.");
  if (!normStr(ev.id)) throw new Error("Invalid merge event: id is required.");
  if (!normStr(ev.repo_id)) throw new Error("Invalid merge event: repo_id is required.");
  if (ensureInt(ev.pr_number) == null || ensureInt(ev.pr_number) <= 0) throw new Error("Invalid merge event: pr_number must be a positive integer.");
  if (!normStr(ev.merge_commit_sha) || normStr(ev.merge_commit_sha).length < 7) throw new Error("Invalid merge event: merge_commit_sha is required.");
  if (!normStr(ev.base_branch)) throw new Error("Invalid merge event: base_branch is required.");
  if (!Array.isArray(ev.affected_paths)) throw new Error("Invalid merge event: affected_paths must be an array.");
  if (typeof ev.timestamp !== "string" || !ev.timestamp.trim()) throw new Error("Invalid merge event: timestamp is required.");
  if (ev.work_id !== undefined && !normStr(ev.work_id)) throw new Error("Invalid merge event: work_id must be a non-empty string when provided.");
  if (ev.pr !== undefined) {
    if (!isPlainObject(ev.pr)) throw new Error("Invalid merge event: pr must be an object when provided.");
    if (ev.pr.number !== undefined) {
      const prn = ensureInt(ev.pr.number);
      if (prn == null || prn <= 0) throw new Error("Invalid merge event: pr.number must be a positive integer.");
    }
  }
  if (ev.merge_sha !== undefined && !normStr(ev.merge_sha)) throw new Error("Invalid merge event: merge_sha must be a non-empty string when provided.");
  if (ev.changed_paths !== undefined && !Array.isArray(ev.changed_paths)) throw new Error("Invalid merge event: changed_paths must be an array when provided.");
  if (ev.obligations !== undefined && !isPlainObject(ev.obligations)) throw new Error("Invalid merge event: obligations must be an object when provided.");
  if (ev.risk_level !== undefined && !normStr(ev.risk_level)) throw new Error("Invalid merge event: risk_level must be a non-empty string when provided.");
  if (ev.qa_waiver !== undefined) {
    if (!isPlainObject(ev.qa_waiver)) throw new Error("Invalid merge event: qa_waiver must be an object when provided.");
    if (ev.qa_waiver.explicit !== undefined && typeof ev.qa_waiver.explicit !== "boolean") throw new Error("Invalid merge event: qa_waiver.explicit must be boolean.");
    if (ev.qa_waiver.waived_obligations !== undefined && !Array.isArray(ev.qa_waiver.waived_obligations)) throw new Error("Invalid merge event: qa_waiver.waived_obligations must be an array.");
  }
}

function ghListChangedFiles({ repoFullName, prNumber, timeoutMs = 20_000 } = {}) {
  const r = normStr(repoFullName);
  const pr = ensureInt(prNumber);
  if (!r || !pr) return { ok: false, paths: [], reason: "missing_repo_or_pr" };
  const args = ["api", `repos/${r}/pulls/${pr}/files`, "--paginate", "-q", ".[].filename"];
  const res = spawnSync("gh", args, { encoding: "utf8", timeout: timeoutMs, env: { ...process.env, GH_PAGER: "cat", PAGER: "cat" } });
  if (res.status !== 0) return { ok: false, paths: [], reason: "gh_failed" };
  const lines = String(res.stdout || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  return { ok: true, paths: uniqSortedStrings(lines), reason: null };
}

function gitDiffTree({ repoAbs, mergeCommitSha, timeoutMs = 20_000 } = {}) {
  const cwd = normStr(repoAbs);
  const sha = normStr(mergeCommitSha);
  if (!cwd || !sha) return { ok: false, paths: [], reason: "missing_repo_or_sha" };
  const res = spawnSync("git", ["-C", cwd, "diff-tree", "--name-only", `${sha}^`, sha], { encoding: "utf8", timeout: timeoutMs });
  if (res.status !== 0) return { ok: false, paths: [], reason: "git_failed" };
  const lines = String(res.stdout || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  return { ok: true, paths: uniqSortedStrings(lines), reason: null };
}

export async function bestEffortAffectedPaths({
  repoFullName = null,
  pr_number = null,
  merge_commit_sha = null,
  repoAbs = null,
} = {}) {
  const ghRes = ghListChangedFiles({ repoFullName, prNumber: pr_number });
  if (ghRes.ok) return { ok: true, paths: ghRes.paths, source: "gh_api" };
  const gitRes = gitDiffTree({ repoAbs, mergeCommitSha: merge_commit_sha });
  if (gitRes.ok) return { ok: true, paths: gitRes.paths, source: "git_diff_tree" };
  return { ok: true, paths: [], source: "none" };
}

export async function logMergeEvent(
  { repo_id, pr_number, merge_commit_sha, base_branch, affected_paths, work_id = null, pr = null, merge_sha = null, changed_paths = null, obligations = null, risk_level = null, qa_waiver = null } = {},
  { projectRoot = null, now = null, dryRun = false } = {},
) {
  const rid = normStr(repo_id);
  const prn = ensureInt(pr_number);
  const sha = normStr(merge_commit_sha);
  const base = normStr(base_branch);
  if (!rid) throw new Error("logMergeEvent: repo_id is required.");
  if (!prn || prn <= 0) throw new Error("logMergeEvent: pr_number is required.");
  if (!sha) throw new Error("logMergeEvent: merge_commit_sha is required.");
  if (!base) throw new Error("logMergeEvent: base_branch is required.");

  const ts = now instanceof Date ? now : new Date();
  const segmentFile = utcSegmentName(ts);

  const paths = await loadProjectPaths({ projectRoot: projectRoot ? resolve(String(projectRoot)) : null });
  const segmentsDirAbs = paths.laneA.eventsSegmentsAbs;
  const segAbs = join(segmentsDirAbs, segmentFile);

  const idTs = segmentFile.replace(/\.jsonl$/u, "");
  const id = `EV-${rid}-${idTs}-${randomHex8()}`;

  const line = {
    version: 1,
    id,
    type: "merge",
    repo_id: rid,
    pr_number: prn,
    merge_commit_sha: sha,
    base_branch: base,
    affected_paths: uniqSortedStrings(Array.isArray(affected_paths) ? affected_paths : []),
    timestamp: ts.toISOString(),
  };
  const workIdNorm = normStr(work_id);
  if (workIdNorm) line.work_id = workIdNorm;
  if (isPlainObject(pr)) {
    const prObj = {};
    const prNumber = ensureInt(pr.number);
    if (prNumber != null && prNumber > 0) prObj.number = prNumber;
    for (const key of ["owner", "repo", "url", "base_branch", "head_branch"]) {
      const value = normStr(pr[key]);
      if (value) prObj[key] = value;
    }
    if (Object.keys(prObj).length) line.pr = prObj;
  }
  const mergeShaNorm = normStr(merge_sha);
  if (mergeShaNorm) line.merge_sha = mergeShaNorm;
  const changedPaths = uniqSortedStrings(Array.isArray(changed_paths) ? changed_paths : []);
  if (changedPaths.length) line.changed_paths = changedPaths;
  if (isPlainObject(obligations)) line.obligations = obligations;
  const risk = normStr(risk_level);
  if (risk) line.risk_level = risk;
  if (isPlainObject(qa_waiver)) {
    const waiver = {};
    if (typeof qa_waiver.explicit === "boolean") waiver.explicit = qa_waiver.explicit;
    if (Array.isArray(qa_waiver.waived_obligations)) waiver.waived_obligations = uniqSortedStrings(qa_waiver.waived_obligations);
    if (typeof qa_waiver.notes === "string" && qa_waiver.notes.trim()) waiver.notes = qa_waiver.notes.trim();
    if (typeof qa_waiver.by === "string" && qa_waiver.by.trim()) waiver.by = qa_waiver.by.trim();
    if (typeof qa_waiver.updated_at === "string" && qa_waiver.updated_at.trim()) waiver.updated_at = qa_waiver.updated_at.trim();
    if (Object.keys(waiver).length) line.qa_waiver = waiver;
  }
  if (!line.changed_paths) line.changed_paths = line.affected_paths.slice();
  if (!line.merge_sha) line.merge_sha = line.merge_commit_sha;
  validateMergeEventShape(line);

  if (!dryRun) {
    await mkdir(segmentsDirAbs, { recursive: true });
    await appendFile(segAbs, JSON.stringify(line) + "\n", "utf8");
  }

  return { ok: true, segment_file: segmentFile, segment_path: segAbs, event: line };
}
