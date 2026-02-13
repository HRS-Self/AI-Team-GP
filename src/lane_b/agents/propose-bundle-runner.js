import { appendFile } from "../../utils/fs.js";
import { nowTs } from "../../utils/id.js";
import { runProposals } from "./agent-runner.js";
import { runRepoPatchPlans } from "./repo-patch-plan-runner.js";
import { updateWorkStatus } from "../../utils/status-writer.js";
import { writeWorkBundle } from "../../bundle/bundle-builder.js";
import { assertLaneAGovernanceForWorkId } from "../lane-a-governance.js";

export async function runProposeBundle({ repoRoot, workId, teamsCsv }) {
  const gov = await assertLaneAGovernanceForWorkId({ workId, phase: "propose_bundle" });
  if (!gov.ok) return gov;

  const proposals = await runProposals({ repoRoot, workId, teamsCsv });
  if (!proposals.ok) return proposals;

  const patchPlans = await runRepoPatchPlans({ repoRoot, workId });
  if (!patchPlans.ok) return patchPlans;

  const createdPlans = Array.isArray(patchPlans.created) ? patchPlans.created : [];
  const failedPlans = createdPlans.filter((p) => !p.ok);
  if (failedPlans.length) {
    return {
      ok: false,
      message: `Some repo patch plans failed validation; cannot create bundle (${failedPlans.map((p) => p.repo_id).join(", ")}).`,
      failed: failedPlans,
    };
  }

  await updateWorkStatus({
    workId,
    stage: "PATCH_PLANNED",
    blocked: false,
    artifacts: { patch_plans_dir: `ai/lane_b/work/${workId}/patch-plans/` },
    note: "repo patch plans created",
  });

  // QA Strategist stage must run after proposals + patch plans and before bundling.
  {
    const { runQaInspector } = await import("./qa-inspector-runner.js");
    const qaRes = await runQaInspector({ repoRoot, workId, teamsCsv: teamsCsv || null });
    if (!qaRes.ok) return qaRes;
  }

  const bundleRes = await writeWorkBundle({ workId });
  if (!bundleRes.ok) return bundleRes;

  await appendFile(
    "ai/lane_b/ledger.jsonl",
    JSON.stringify({ timestamp: nowTs(), action: "propose_bundle_created", workId, bundle_path: bundleRes.bundle_path, bundle_hash: bundleRes.bundle_hash }) + "\n",
  );

  await updateWorkStatus({
    workId,
    stage: "APPLY_APPROVAL_PENDING",
    blocked: true,
    blockingReason: "Apply approval required before PR creation.",
    artifacts: { bundle_json: `ai/lane_b/work/${workId}/BUNDLE.json` },
    note: "propose complete; awaiting apply-approval",
  });

  return { ok: true, workId, bundle_path: bundleRes.bundle_path, bundle_hash: bundleRes.bundle_hash, proposals, patch_plans: patchPlans };
}
