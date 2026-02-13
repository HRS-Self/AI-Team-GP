import { createHash } from "node:crypto";
import { readdir } from "node:fs/promises";

import { readTextIfExists, ensureDir, writeText } from "../utils/fs.js";
import { resolveStatePath } from "../project/state-paths.js";
import { jsonStableStringify } from "../utils/json.js";
import { nowTs } from "../utils/id.js";
import { loadRepoRegistry } from "../utils/repo-registry.js";
import { loadPolicies } from "../policy/resolve.js";
import { validatePatchPlan } from "../validators/patch-plan-validator.js";
import { validateQaPlan } from "../validators/qa-plan-validator.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../utils/status-writer.js";
import { writeWorkPlan } from "../utils/plan-writer.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function computeBundleHashFromPins(pins) {
  const uniq = new Map();
  for (const it of pins || []) {
    if (!it || typeof it.path !== "string" || !it.path.trim()) continue;
    if (typeof it.sha256 !== "string" || !it.sha256.trim()) continue;
    uniq.set(it.path.trim(), it.sha256.trim());
  }
  const sorted = Array.from(uniq.entries())
    .map(([path, sha]) => ({ path, sha256: sha }))
    .sort((a, b) => a.path.localeCompare(b.path));

  const h = createHash("sha256");
  for (const it of sorted) {
    h.update(`${it.path}\n`);
    h.update(it.sha256);
    h.update("\n---\n");
  }
  return h.digest("hex");
}

async function listProposalFilesForTeam({ workId, teamId }) {
  const dir = `ai/lane_b/work/${workId}/proposals`;
  try {
    const entries = await readdir(resolveStatePath(dir), { withFileTypes: true });
    return entries
      .filter((e) => e.isFile() && e.name.endsWith(".json") && e.name.startsWith(`${teamId}__`))
      .map((e) => `${dir}/${e.name}`)
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

async function listAllProposalJsonFiles({ workId }) {
  const dir = `ai/lane_b/work/${workId}/proposals`;
  try {
    const entries = await readdir(resolveStatePath(dir), { withFileTypes: true });
    return entries
      .filter((e) => e.isFile() && e.name.endsWith(".json"))
      .map((e) => `${dir}/${e.name}`)
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

export async function buildWorkBundle({ workId, requireQa = true } = {}) {
  const reposLoaded = await loadRepoRegistry();
  if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message };
  const registry = reposLoaded.registry;
  const byId = new Map((registry.repos || []).map((r) => [String(r.repo_id), r]));

  const policiesLoaded = await loadPolicies();
  if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };
  const policies = policiesLoaded.policies;

  const workDir = `ai/lane_b/work/${workId}`;
  const patchPlansDir = `${workDir}/patch-plans`;
  const qaDir = `${workDir}/qa`;

  // Work-level SSOT bundle (reference-only) is mandatory for delivery (Lane B) traceability.
  const workSsotBundlePath = `${workDir}/SSOT_BUNDLE.json`;
  const workSsotBundleText = await readTextIfExists(workSsotBundlePath);
  if (!workSsotBundleText) return { ok: false, message: `Missing ${workSsotBundlePath}. Re-run: node src/cli.js --propose --with-patch-plans (or run planner/reviewer/qa to generate SSOT bundle).` };
  const workSsotBundleSha = sha256Hex(workSsotBundleText);

  const allProposalFiles = await listAllProposalJsonFiles({ workId });
  if (!allProposalFiles.length) return { ok: false, message: `Missing proposals directory content (expected ${workDir}/proposals/*.json).` };

  const proposalPins = [];
  const proposalShaByPath = new Map();
  for (const p of allProposalFiles) {
    const t = await readTextIfExists(p);
    if (!t) return { ok: false, message: `Missing proposal input file: ${p}` };
    const sha = sha256Hex(t);
    proposalPins.push({ path: p, sha256: sha });
    proposalShaByPath.set(p, sha);

    const mdPath = p.replace(/\.json$/i, ".md");
    const mdText = await readTextIfExists(mdPath);
    if (mdText) proposalPins.push({ path: mdPath, sha256: sha256Hex(mdText) });
  }

  const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
  let routing = null;
  try {
    routing = routingText ? JSON.parse(routingText) : null;
  } catch {
    routing = null;
  }
  const selectedRepos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.map((x) => String(x)).filter(Boolean) : [];
  if (!selectedRepos.length) return { ok: false, message: `Missing/invalid selected_repos in ${workDir}/ROUTING.json.` };

  const repos = [];
  const patchPlanPins = [];
  const qaPins = [];
  const ssotPins = [];
  const ssotPinnedTeams = new Set();

  // Pin the work-level SSOT bundle into the bundle hash.
  ssotPins.push({ path: workSsotBundlePath, sha256: workSsotBundleSha });

  for (const repoId of selectedRepos.slice().sort((a, b) => a.localeCompare(b))) {
    const repo = byId.get(repoId) || null;
    if (!repo) return { ok: false, message: `Repo not found in registry: ${repoId}` };
    const teamId = String(repo.team_id || "").trim();
    if (!teamId) return { ok: false, message: `Repo team_id missing in registry: ${repoId}` };

    const proposalPaths = await listProposalFilesForTeam({ workId, teamId });
    if (!proposalPaths.length) return { ok: false, message: `Missing proposal for team ${teamId} (expected ${workDir}/proposals/${teamId}__*.json).` };
    const proposalPath = proposalPaths[0];
    const proposalText = await readTextIfExists(proposalPath);
    if (!proposalText) return { ok: false, message: `Missing proposal JSON: ${proposalPath}` };
    let proposalJson;
    try {
      proposalJson = JSON.parse(proposalText);
    } catch {
      return { ok: false, message: `Invalid JSON in proposal: ${proposalPath}` };
    }
    if (proposalJson?.status !== "SUCCESS") return { ok: false, message: `Proposal is not SUCCESS for team ${teamId} (${proposalPath}).` };
    if (!Array.isArray(proposalJson?.ssot_references) || proposalJson.ssot_references.length < 1) {
      return { ok: false, message: `Proposal missing ssot_references (required) for team ${teamId} (${proposalPath}).` };
    }
    const proposalAgentId = typeof proposalJson?.agent_id === "string" ? proposalJson.agent_id.trim() : "";
    if (!proposalAgentId) return { ok: false, message: `Proposal agent_id missing for team ${teamId} (${proposalPath}).` };

    const ssotPath = `${workDir}/ssot/SSOT_BUNDLE.team-${teamId}.json`;
    if (!ssotPinnedTeams.has(teamId)) {
      const ssotText = await readTextIfExists(ssotPath);
      if (!ssotText) return { ok: false, message: `Missing SSOT bundle for team ${teamId} (expected ${ssotPath}).` };
      ssotPins.push({ path: ssotPath, sha256: sha256Hex(ssotText) });
      ssotPinnedTeams.add(teamId);
    }

    const planJsonPath = `${patchPlansDir}/${repoId}.json`;
    const planMdPath = `${patchPlansDir}/${repoId}.md`;
    const planText = await readTextIfExists(planJsonPath);
    if (!planText) return { ok: false, message: `Missing patch plan JSON: ${planJsonPath}` };
    let planJson;
    try {
      planJson = JSON.parse(planText);
    } catch {
      return { ok: false, message: `Invalid JSON in patch plan: ${planJsonPath}` };
    }
    const v = validatePatchPlan(planJson, { policy: policies, expected_proposal_hash: proposalShaByPath.get(proposalPath) || null, expected_proposal_agent_id: proposalAgentId });
    if (!v.ok) return { ok: false, message: `Invalid patch plan (validator): ${repoId}`, errors: v.errors };
    const planSha = sha256Hex(planText);
    patchPlanPins.push({ path: planJsonPath, sha256: planSha });

    // QA pins (required for bundling; approval gates the bundle).
    const qaJsonPath = `${qaDir}/qa-plan.${repoId}.json`;
    const qaMdPath = `${qaDir}/qa-plan.${repoId}.md`;
    const qaText = await readTextIfExists(qaJsonPath);
    if (requireQa) {
      if (!qaText) return { ok: false, message: `Missing QA plan JSON: ${qaJsonPath}. Run: node src/cli.js --qa --workId ${workId}` };
      let qaJson;
      try {
        qaJson = JSON.parse(qaText);
      } catch {
        return { ok: false, message: `Invalid JSON in QA plan: ${qaJsonPath}` };
      }
      const qaV = validateQaPlan(qaJson, { expectedWorkId: workId, expectedRepoId: repoId });
      if (!qaV.ok) return { ok: false, message: `Invalid QA plan (validator): ${repoId}`, errors: qaV.errors };
      if (qaJson?.derived_from?.patch_plan_sha256 && qaJson.derived_from.patch_plan_sha256 !== planSha) {
        return { ok: false, message: `QA plan derived_from.patch_plan_sha256 mismatch for ${repoId}.` };
      }
      const expectedProposalSha = proposalShaByPath.get(proposalPath) || null;
      if (expectedProposalSha && qaJson?.derived_from?.proposal_sha256 && qaJson.derived_from.proposal_sha256 !== expectedProposalSha) {
        return { ok: false, message: `QA plan derived_from.proposal_sha256 mismatch for ${repoId}.` };
      }
      const qaTestsCount = Array.isArray(qaJson?.tests) ? qaJson.tests.length : 0;
      const qaGapsCount = Array.isArray(qaJson?.gaps) ? qaJson.gaps.length : 0;
      qaPins.push({ path: qaJsonPath, sha256: sha256Hex(qaText) });
      const qaMdText = await readTextIfExists(qaMdPath);
      if (qaMdText) qaPins.push({ path: qaMdPath, sha256: sha256Hex(qaMdText) });

      repos.push({
        repo_id: repoId,
        proposal_path: proposalPath,
        proposal_sha256: proposalShaByPath.get(proposalPath) || null,
        proposal_md_path: proposalPath.replace(/\.json$/i, ".md"),
        ssot_bundle_json_path: ssotPath,
        patch_plan_json_path: planJsonPath,
        patch_plan_json_sha256: planSha,
        patch_plan_md_path: planMdPath,
        qa_plan_json_path: qaJsonPath,
        qa_plan_md_path: qaMdPath,
        qa_tests: qaTestsCount,
        qa_gaps: qaGapsCount,
      });
      continue;
    }
    repos.push({
      repo_id: repoId,
      proposal_path: proposalPath,
      proposal_sha256: proposalShaByPath.get(proposalPath) || null,
      proposal_md_path: proposalPath.replace(/\.json$/i, ".md"),
      ssot_bundle_json_path: ssotPath,
      patch_plan_json_path: planJsonPath,
      patch_plan_json_sha256: planSha,
      patch_plan_md_path: planMdPath,
    });
  }

  const pins = [...proposalPins, ...patchPlanPins, ...qaPins, ...ssotPins];
  const bundleHash = computeBundleHashFromPins(pins);
  const bundle = {
    version: 1,
    work_id: workId,
    created_at: nowTs(),
    ssot_bundle_path: workSsotBundlePath,
    ssot_bundle_sha256: workSsotBundleSha,
    repos,
    inputs: {
      proposals: proposalPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
      patch_plan_jsons: patchPlanPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
      qa_plan_jsons: qaPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
      ssot_bundle_jsons: ssotPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
    },
    bundle_hash: bundleHash,
  };

  return { ok: true, workId, bundle, bundle_hash: bundleHash };
}

export async function writeWorkBundle({ workId }) {
  const built = await buildWorkBundle({ workId, requireQa: true });
  if (!built.ok) return built;

  const workDir = `ai/lane_b/work/${workId}`;
  await ensureDir(workDir);
  const bundlePath = `${workDir}/BUNDLE.json`;
  await writeText(bundlePath, jsonStableStringify(built.bundle));

  await updateWorkStatus({
    workId,
    stage: "BUNDLED",
    blocked: false,
    artifacts: { bundle_json: bundlePath, bundle_hash: built.bundle_hash },
    note: "bundle created",
  });
  {
    const intakeMd = (await readTextIfExists(`${workDir}/INTAKE.md`)) || "";
    const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
    let routing = null;
    try {
      routing = routingText ? JSON.parse(routingText) : null;
    } catch {
      routing = null;
    }
    await writeWorkPlan({ workId, intakeMd, routing, bundle: built.bundle });
  }
  await writeGlobalStatusFromPortfolio();
  return { ok: true, workId, bundle_path: bundlePath, bundle_hash: built.bundle_hash, bundle: built.bundle };
}
