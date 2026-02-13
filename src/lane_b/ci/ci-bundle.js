import { createHash } from "node:crypto";

import { readTextIfExists, writeText } from "../../utils/fs.js";
import { loadPolicies } from "../../policy/resolve.js";
import { validatePatchPlan } from "../../validators/patch-plan-validator.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";

function nowISO() {
  return new Date().toISOString();
}

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

function safeJsonParse(text, label) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid JSON in ${label} (${msg}).` };
  }
}

export async function updateBundlePatchPlanForRepo({ workId, repoId, patchPlanJsonPath, patchPlanMdPath = null } = {}) {
  const wid = String(workId || "").trim();
  const rid = String(repoId || "").trim();
  const jsonPath = String(patchPlanJsonPath || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };
  if (!rid) return { ok: false, message: "Missing repoId." };
  if (!jsonPath) return { ok: false, message: "Missing patchPlanJsonPath." };

  const workDir = `ai/lane_b/work/${wid}`;
  const bundlePath = `${workDir}/BUNDLE.json`;
  const bundleText = await readTextIfExists(bundlePath);
  if (!bundleText) return { ok: false, message: `Missing ${bundlePath}.` };
  const bundleParsed = safeJsonParse(bundleText, bundlePath);
  if (!bundleParsed.ok) return { ok: false, message: bundleParsed.message };
  const bundle = bundleParsed.json;
  if (!bundle || bundle.version !== 1 || String(bundle.work_id || "") !== wid || !Array.isArray(bundle.repos)) return { ok: false, message: "Invalid bundle format." };

  const planText = await readTextIfExists(jsonPath);
  if (!planText) return { ok: false, message: `Missing patch plan JSON: ${jsonPath}` };
  const planParsed = safeJsonParse(planText, jsonPath);
  if (!planParsed.ok) return { ok: false, message: planParsed.message };

  const policiesLoaded = await loadPolicies();
  if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };
  const policies = policiesLoaded.policies;

  const repos = bundle.repos.map((r) => ({ ...r }));
  const idx = repos.findIndex((r) => String(r?.repo_id || "").trim() === rid);
  if (idx < 0) return { ok: false, message: `Repo not found in bundle: ${rid}` };

  const expectedProposalHash = typeof repos[idx]?.proposal_sha256 === "string" && repos[idx].proposal_sha256.trim() ? repos[idx].proposal_sha256.trim() : null;
  const proposalPath = typeof repos[idx]?.proposal_path === "string" && repos[idx].proposal_path.trim() ? repos[idx].proposal_path.trim() : null;
  let expectedProposalAgentId = null;
  if (proposalPath) {
    const proposalText = await readTextIfExists(proposalPath);
    const proposalParsed = proposalText ? safeJsonParse(proposalText, proposalPath) : { ok: false };
    if (proposalParsed.ok) expectedProposalAgentId = typeof proposalParsed.json?.agent_id === "string" ? proposalParsed.json.agent_id.trim() : null;
  }

  const v = validatePatchPlan(planParsed.json, { policy: policies, expected_proposal_hash: expectedProposalHash, expected_proposal_agent_id: expectedProposalAgentId });
  if (!v.ok) return { ok: false, message: `Invalid patch plan (validator): ${rid}`, errors: v.errors };

  const planSha = sha256Hex(planText);
  repos[idx] = {
    ...repos[idx],
    patch_plan_json_path: jsonPath,
    patch_plan_json_sha256: planSha,
    ...(patchPlanMdPath ? { patch_plan_md_path: patchPlanMdPath } : {}),
  };

  const proposalPins = Array.isArray(bundle.inputs?.proposals) ? bundle.inputs.proposals : [];
  const ssotPins = Array.isArray(bundle.inputs?.ssot_bundle_jsons) ? bundle.inputs.ssot_bundle_jsons : [];
  const qaPins = Array.isArray(bundle.inputs?.qa_plan_jsons) ? bundle.inputs.qa_plan_jsons : [];
  const patchPins = repos.map((r) => ({ path: r.patch_plan_json_path, sha256: r.patch_plan_json_sha256 })).filter((x) => x.path && x.sha256);

  const next = {
    ...bundle,
    repos,
    inputs: {
      proposals: proposalPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
      patch_plan_jsons: patchPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
      qa_plan_jsons: qaPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
      ssot_bundle_jsons: ssotPins.slice().sort((a, b) => a.path.localeCompare(b.path)),
    },
    bundle_hash: computeBundleHashFromPins([...proposalPins, ...patchPins, ...qaPins, ...ssotPins]),
    updated_at: nowISO(),
  };

  const tsSafe = nowFsSafeUtcTimestamp();
  const archivePath = `${workDir}/BUNDLE.ci-fix-${tsSafe}.json`;
  await writeText(archivePath, JSON.stringify(next, null, 2) + "\n");
  await writeText(bundlePath, JSON.stringify(next, null, 2) + "\n");
  return { ok: true, workId: wid, repoId: rid, bundle_path: bundlePath, archive_path: archivePath, bundle_hash: next.bundle_hash };
}
