import { resolve } from "node:path";

import { appendFile, readTextIfExists } from "../../utils/fs.js";
import { runRepoPatchPlans } from "../agents/repo-patch-plan-runner.js";
import { runApplyPatchPlans } from "../agents/apply-runner.js";
import { updateBundlePatchPlanForRepo } from "./ci-bundle.js";
import { readProjectConfig } from "../../project/project-config.js";
import { appendEvent as appendKnowledgeChangeEvent } from "../../lane_a/knowledge/knowledge-events-store.js";

function safeJsonParse(text, label) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid JSON in ${label} (${msg}).` };
  }
}

function repoIdFromHeadBranch({ workId, headBranch }) {
  const wid = String(workId || "").trim();
  const hb = String(headBranch || "").trim();
  if (!wid || !hb) return null;
  const prefix = `ai/${wid}/`;
  if (hb.startsWith(prefix)) {
    const rest = hb.slice(prefix.length);
    const seg = rest.split("/").filter(Boolean)[0] || null;
    return seg;
  }
  const parts = hb.split("/").filter(Boolean);
  return parts.length ? parts[parts.length - 1] : null;
}

export async function runCiFixOnce({ orchestrator, workId, attemptNumber, latestFeedbackBase = null } = {}) {
  const wid = String(workId || "").trim();
  const attempt = Number.isFinite(Number(attemptNumber)) ? Math.max(1, Math.floor(Number(attemptNumber))) : 1;
  if (!wid) return { ok: false, message: "Missing workId." };
  if (!orchestrator || typeof orchestrator.repoRoot !== "string" || !orchestrator.repoRoot.trim()) return { ok: false, message: "Missing orchestrator.repoRoot." };

  const workDir = `ai/lane_b/work/${wid}`;
  const prText = await readTextIfExists(`${workDir}/PR.json`);
  if (!prText) return { ok: false, message: `Missing ${workDir}/PR.json.` };
  const prParsed = safeJsonParse(prText, `${workDir}/PR.json`);
  if (!prParsed.ok) return { ok: false, message: prParsed.message };
  const pr = prParsed.json;

  const headBranch = typeof pr?.head_branch === "string" ? pr.head_branch.trim() : "";
  if (!headBranch) return { ok: false, message: `PR.json missing head_branch (${workDir}/PR.json).` };

  const repoId = repoIdFromHeadBranch({ workId: wid, headBranch });
  if (!repoId) return { ok: false, message: `Unable to infer repoId from PR head_branch='${headBranch}'.` };

  const outputSuffix = `.ci-fix.${attempt}`;
  const extraContextPath =
    typeof latestFeedbackBase === "string" && latestFeedbackBase.trim() ? `${workDir}/CI/${latestFeedbackBase.trim()}.md` : null;

  const planRes = await runRepoPatchPlans({
    repoRoot: orchestrator.repoRoot,
    workId: wid,
    repoIds: [repoId],
    outputSuffix,
    branchNameOverride: headBranch,
    extraContextPath,
  });
  if (!planRes.ok) return { ok: false, message: String(planRes.message || "ci fix planner failed"), ...(planRes.errors ? { errors: planRes.errors } : {}) };

  const patchPlanJsonPath = `${workDir}/patch-plans/${repoId}${outputSuffix}.json`;
  const patchPlanMdPath = `${workDir}/patch-plans/${repoId}${outputSuffix}.md`;

  const bundleRes = await updateBundlePatchPlanForRepo({ workId: wid, repoId, patchPlanJsonPath, patchPlanMdPath });
  if (!bundleRes.ok) return { ok: false, message: bundleRes.message, ...(bundleRes.errors ? { errors: bundleRes.errors } : {}) };

  const applyRes = await runApplyPatchPlans({ repoRoot: orchestrator.repoRoot, workId: wid, onlyRepoId: repoId, mode: "ci_fix" });
  if (!applyRes.ok) return { ok: false, message: String(applyRes.message || "ci fix apply failed") };

  // Lane B -> Lane A feedback event (best-effort; never fails ci-fix).
  try {
    const projectRoot = typeof process.env.AI_PROJECT_ROOT === "string" ? process.env.AI_PROJECT_ROOT : null;
    if (!projectRoot) throw new Error("Missing AI_PROJECT_ROOT.");
    const cfgRes = await readProjectConfig({ projectRoot });
    if (!cfgRes.ok) throw new Error(cfgRes.message);
    const cfg = cfgRes.config;

    const appliedOk = Array.isArray(applyRes?.results) ? applyRes.results.find((r) => r && r.ok === true) : null;
    const commit = appliedOk && typeof appliedOk.commit === "string" ? appliedOk.commit.trim() : "";
    if (!commit) throw new Error("Missing commit SHA from ci_fix apply results.");

    const prNumber = typeof pr?.pr_number === "number" ? pr.pr_number : null;
    await appendKnowledgeChangeEvent(
      {
        type: "ci_fix",
        scope: `repo:${repoId}`,
        repo_id: repoId,
        work_id: wid,
        pr_number: prNumber,
        commit,
        artifacts: {
          paths: [patchPlanJsonPath, patchPlanMdPath, `${workDir}/PR.json`],
          fingerprints: [bundleRes && typeof bundleRes.bundle_hash === "string" ? `bundle:${bundleRes.bundle_hash}` : `work:${wid}`],
        },
        summary: `CI fix patch applied for work ${wid} repo ${repoId}.`,
        timestamp: new Date().toISOString(),
      },
      { opsLaneAAbs: resolve(String(cfg.ops_root_abs || ""), "ai", "lane_a"), dryRun: false },
    );
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: new Date().toISOString(), action: "knowledge_event_emitted", workId: wid, type: "ci_fix", repo_id: repoId, commit }) + "\n");
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: new Date().toISOString(), action: "knowledge_event_emit_failed", workId: wid, type: "ci_fix", repo_id: repoId, error: msg }) + "\n");
  }

  return { ok: true, workId: wid, repoId, patch_plan_json: patchPlanJsonPath, bundle_hash: bundleRes.bundle_hash, applied: applyRes.results || [] };
}
