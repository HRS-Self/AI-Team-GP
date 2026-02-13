import { readTextIfExists } from "../utils/fs.js";
import { assertGhReady, getPrChecks } from "./gh.js";

function nowISO() {
  return new Date().toISOString();
}

function safeJsonParse(text) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    return { ok: false, message: err instanceof Error ? err.message : String(err) };
  }
}

function summarizeStatusCheckRollup(rollup) {
  const counts = { success: 0, failure: 0, pending: 0, skipped: 0, cancelled: 0, other: 0 };
  for (const c of Array.isArray(rollup) ? rollup : []) {
    const st = String(c?.state || "").toUpperCase();
    if (st === "SUCCESS") counts.success += 1;
    else if (st === "FAILURE") counts.failure += 1;
    else if (st === "PENDING" || st === "IN_PROGRESS" || st === "QUEUED") counts.pending += 1;
    else if (st === "SKIPPED") counts.skipped += 1;
    else if (st === "CANCELLED" || st === "CANCELED") counts.cancelled += 1;
    else counts.other += 1;
  }
  return counts;
}

export async function runPrStatus({ workId }) {
  assertGhReady();

  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing --workId <id>." };

  const prPath = `ai/lane_b/work/${wid}/PR.json`;
  const text = await readTextIfExists(prPath);
  if (!text) return { ok: false, message: `Missing ${prPath}.` };
  const parsed = safeJsonParse(text);
  if (!parsed.ok) return { ok: false, message: `Invalid JSON in ${prPath} (${parsed.message}).` };

  const prDoc = parsed.json;
  const owner = typeof prDoc?.owner === "string" ? prDoc.owner.trim() : "";
  const name = typeof prDoc?.repo === "string" ? prDoc.repo.trim() : "";
  const repo = owner && name ? `${owner}/${name}` : "";
  const num = typeof prDoc?.pr_number === "number" ? prDoc.pr_number : Number.parseInt(String(prDoc?.pr_number || "").trim(), 10);
  if (!repo) return { ok: false, message: `PR.json missing owner/repo (${prPath}).` };
  if (!Number.isFinite(num) || num <= 0) return { ok: false, message: `PR.json missing/invalid pr_number (${prPath}).` };

  const checks = getPrChecks({ repo, prNumber: num });
  const rollup = checks.checks;
  const summary = summarizeStatusCheckRollup(rollup);
  const anyFailure = summary.failure > 0;
  const allComplete = summary.pending === 0;
  const ok = !anyFailure && allComplete && summary.success > 0;

  return {
    ok,
    work_id: wid,
    checked_at: nowISO(),
    pr: {
      repo_full_name: repo,
      pr_number: num,
      pr_url: typeof checks.pr?.url === "string" ? checks.pr.url : (typeof prDoc?.url === "string" ? prDoc.url : null),
      base_branch: typeof checks.pr?.baseRefName === "string" ? checks.pr.baseRefName : (typeof prDoc?.base_branch === "string" ? prDoc.base_branch : null),
      head_branch: typeof checks.pr?.headRefName === "string" ? checks.pr.headRefName : (typeof prDoc?.head_branch === "string" ? prDoc.head_branch : null),
      head_sha: typeof checks.pr?.headRefOid === "string" ? checks.pr.headRefOid : null,
      state: typeof checks.pr?.state === "string" ? checks.pr.state : null,
    },
    summary,
    checks: rollup
      .map((c) => ({
        name: typeof c?.name === "string" ? c.name : null,
        state: typeof c?.state === "string" ? c.state : null,
        description: typeof c?.description === "string" ? c.description : null,
        details_url: typeof c?.detailsUrl === "string" ? c.detailsUrl : null,
      }))
      .filter((c) => c.name),
  };
}
