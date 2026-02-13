import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { rename } from "node:fs/promises";

import { appendFile, ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { resolveStatePath } from "../../project/state-paths.js";
import { loadPolicies } from "../../policy/resolve.js";
import { validatePatchPlan } from "../../validators/patch-plan-validator.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";

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

function riskBucketFromPatchPlanRiskLevel(levelRaw) {
  const v = String(levelRaw || "").trim().toLowerCase();
  if (v === "low") return "low";
  if (v === "normal") return "medium";
  if (v === "high") return "high";
  return "unknown";
}

function renderApplyApprovalMarkdown(doc, { errors = [] } = {}) {
  const lines = [];
  lines.push(`# Apply Approval (PR creation permission)`);
  lines.push("");
  lines.push(`Work item: \`${doc.workId}\``);
  lines.push("");
  lines.push("## Status");
  lines.push("");
  lines.push(`- status: \`${doc.status}\``);
  lines.push(`- mode: \`${doc.mode}\``);
  lines.push(`- bundle_hash: \`${doc.bundle_hash || "(missing)"}\``);
  lines.push(`- approved_at: \`${doc.approved_at || "(null)"}\``);
  lines.push(`- approved_by: \`${doc.approved_by || "(null)"}\``);
  lines.push("");
  lines.push("## Scope");
  lines.push("");
  lines.push(`- teams: ${(doc.scope?.teams || []).map((t) => `\`${t}\``).join(", ") || "(none)"}`);
  lines.push(`- repos: ${(doc.scope?.repos || []).map((r) => `\`${r}\``).join(", ") || "(none)"}`);
  lines.push("");
  if (Array.isArray(doc.reason_codes) && doc.reason_codes.length) {
    lines.push("## Reason codes");
    lines.push("");
    for (const c of doc.reason_codes) lines.push(`- \`${c}\``);
    lines.push("");
  }
  if (doc.notes) {
    lines.push("## Notes");
    lines.push("");
    lines.push(String(doc.notes).trim());
    lines.push("");
  }
  if (errors.length) {
    lines.push("## Validation errors");
    lines.push("");
    for (const e of errors) lines.push(`- ${e}`);
    lines.push("");
  }
  return lines.join("\n");
}

async function appendLedger(event) {
  await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify(event) + "\n");
}

async function migrateLegacyGateAArtifactsIfNeeded(workId) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: true, migrated: false };
  const workDir = `ai/lane_b/work/${wid}`;
  const legacyJson = `${workDir}/GATE_A.json`;
  const legacyMd = `${workDir}/GATE_A.md`;
  const nextJson = `${workDir}/APPLY_APPROVAL.json`;
  const nextMd = `${workDir}/APPLY_APPROVAL.md`;

  const legacyJsonAbs = resolveStatePath(legacyJson, { requiredRoot: true });
  const legacyMdAbs = resolveStatePath(legacyMd, { requiredRoot: true });
  const nextJsonAbs = resolveStatePath(nextJson, { requiredRoot: true });
  const nextMdAbs = resolveStatePath(nextMd, { requiredRoot: true });

  let migrated = false;
  if (existsSync(legacyJsonAbs) && !existsSync(nextJsonAbs)) {
    await rename(legacyJsonAbs, nextJsonAbs);
    migrated = true;
  }
  if (existsSync(legacyMdAbs) && !existsSync(nextMdAbs)) {
    await rename(legacyMdAbs, nextMdAbs);
    migrated = true;
  }
  return { ok: true, migrated };
}

export async function requestApplyApproval({ workId, dryRun = false } = {}) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };

  await migrateLegacyGateAArtifactsIfNeeded(wid);

  const workDir = `ai/lane_b/work/${wid}`;
  const metaText = await readTextIfExists(`${workDir}/META.json`);
  if (!metaText) return { ok: false, message: `Work item not found: missing ${workDir}/META.json.` };
  const approvalJsonPath = `${workDir}/APPLY_APPROVAL.json`;
  const approvalMdPath = `${workDir}/APPLY_APPROVAL.md`;

  const bundlePath = `${workDir}/BUNDLE.json`;
  const bundleText = await readTextIfExists(bundlePath);
  if (!bundleText) {
    await updateWorkStatus({ workId: wid, stage: "APPLY_APPROVAL_PENDING", blocked: true, blockingReason: "bundle_missing" });
    await writeGlobalStatusFromPortfolio();
    return { ok: false, message: `Missing ${bundlePath}. Run propose with patch plans first.` };
  }
  const bundleParsed = safeJsonParse(bundleText, bundlePath);
  if (!bundleParsed.ok) return { ok: false, message: bundleParsed.message };
  const bundle = bundleParsed.json;
  const bundleHash = typeof bundle?.bundle_hash === "string" ? bundle.bundle_hash.trim() : "";
  if (!bundleHash) return { ok: false, message: `BUNDLE.json missing bundle_hash (${bundlePath}).` };

  const repos = Array.isArray(bundle?.repos) ? bundle.repos : [];
  const scopeTeams = Array.from(new Set(repos.map((r) => String(r?.team_id || "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  const scopeRepos = Array.from(new Set(repos.map((r) => String(r?.repo_id || "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));

  const policiesLoaded = await loadPolicies();
  if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };
  const policies = policiesLoaded.policies;

  const errors = [];
  const reasonCodes = [];

  // Patch plans must exist and validate.
  for (const r of repos) {
    const repoId = String(r?.repo_id || "").trim();
    const planPath = String(r?.patch_plan_json_path || "").trim();
    const proposalPath = String(r?.proposal_path || "").trim();
    if (!repoId) continue;
    if (!planPath) {
      errors.push(`Missing patch_plan_json_path for ${repoId} in BUNDLE.json.`);
      reasonCodes.push("patch_plan_missing");
      continue;
    }
    const planText = await readTextIfExists(planPath);
    if (!planText) {
      errors.push(`Missing patch plan JSON: ${planPath}`);
      reasonCodes.push("patch_plan_missing");
      continue;
    }
    let planJson = null;
    try {
      planJson = JSON.parse(planText);
    } catch {
      errors.push(`Invalid JSON in patch plan: ${planPath}`);
      reasonCodes.push("patch_plan_invalid");
      continue;
    }
    const expectedProposalText = proposalPath ? await readTextIfExists(proposalPath) : null;
    const expectedProposalSha = expectedProposalText ? sha256Hex(expectedProposalText) : null;
    const expectedAgentId = expectedProposalText
      ? (() => {
          try {
            const j = JSON.parse(expectedProposalText);
            return typeof j?.agent_id === "string" ? j.agent_id.trim() : null;
          } catch {
            return null;
          }
        })()
      : null;
    const v = validatePatchPlan(planJson, { policy: policies, expected_proposal_hash: expectedProposalSha, expected_proposal_agent_id: expectedAgentId });
    if (!v.ok) {
      errors.push(`Patch plan validator failed for ${repoId}: ${v.errors.join(" | ")}`);
      reasonCodes.push("patch_plan_invalid");
    }
  }

  // SSOT invariants: proposals must have ssot_references and no hard drift.
  for (const r of repos) {
    const repoId = String(r?.repo_id || "").trim();
    const proposalPath = String(r?.proposal_path || "").trim();
    if (!repoId || !proposalPath) continue;
    const proposalText = await readTextIfExists(proposalPath);
    if (!proposalText) {
      errors.push(`Missing proposal JSON: ${proposalPath}`);
      reasonCodes.push("proposal_missing");
      continue;
    }
    try {
      const j = JSON.parse(proposalText);
      if (!Array.isArray(j?.ssot_references)) {
        errors.push(`Proposal JSON missing ssot_references array (${proposalPath}).`);
        reasonCodes.push("ssot_references_missing");
      }
    } catch {
      errors.push(`Invalid JSON in proposal: ${proposalPath}`);
      reasonCodes.push("proposal_invalid");
    }
  }

  const driftPath = `${workDir}/SSOT_DRIFT.json`;
  const driftText = await readTextIfExists(driftPath);
  if (driftText) {
    const driftParsed = safeJsonParse(driftText, driftPath);
    if (!driftParsed.ok) {
      errors.push(driftParsed.message);
      reasonCodes.push("ssot_drift_invalid");
    } else {
      const hard = Array.isArray(driftParsed.json?.hard_violations) ? driftParsed.json.hard_violations : [];
      if (hard.length) reasonCodes.push("ssot_hard_violation");
    }
  }

  // Risk gating: auto-approve only if no high risk.
  let highestRisk = "unknown";
  for (const r of repos) {
    const planPath = String(r?.patch_plan_json_path || "").trim();
    const planText = planPath ? await readTextIfExists(planPath) : null;
    if (!planText) continue;
    try {
      const plan = JSON.parse(planText);
      const bucket = riskBucketFromPatchPlanRiskLevel(plan?.risk?.level);
      if (bucket === "high") highestRisk = "high";
      else if (bucket === "medium" && highestRisk !== "high") highestRisk = "medium";
      else if (bucket === "low" && highestRisk === "unknown") highestRisk = "low";
    } catch {
      // ignore; already counted in errors.
    }
  }
  const riskBlocksAuto = highestRisk === "high";

  const canAutoApprove = errors.length === 0 && !riskBlocksAuto && !reasonCodes.includes("ssot_hard_violation");
  const status = canAutoApprove ? "approved" : errors.length ? "rejected" : "pending";
  const mode = canAutoApprove ? "auto" : "manual";

  const approval = {
    version: 1,
    workId: wid,
    status,
    mode,
    bundle_hash: bundleHash,
    approved_at: status === "approved" ? nowISO() : null,
    approved_by: status === "approved" ? "auto" : null,
    reason_codes: Array.from(new Set(reasonCodes)).sort((a, b) => a.localeCompare(b)),
    notes: null,
    scope: { teams: scopeTeams, repos: scopeRepos },
  };

  if (!dryRun) {
    await ensureDir(workDir);
    await writeText(approvalJsonPath, JSON.stringify(approval, null, 2) + "\n");
    await writeText(approvalMdPath, renderApplyApprovalMarkdown(approval, { errors }));
  }

  await appendLedger({ timestamp: nowISO(), action: "apply_approval_requested", workId: wid, status, mode, bundle_hash: bundleHash });
  if (status === "approved") await appendLedger({ timestamp: nowISO(), action: "apply_approval_auto_approved", workId: wid, bundle_hash: bundleHash });

  await updateWorkStatus({
    workId: wid,
    stage: status === "approved" ? "APPLY_APPROVAL_APPROVED" : "APPLY_APPROVAL_PENDING",
    blocked: status !== "approved",
    blockingReason: status === "approved" ? null : "Apply approval not approved.",
    artifacts: { apply_approval_json: `ai/lane_b/work/${wid}/APPLY_APPROVAL.json`, apply_approval_md: `ai/lane_b/work/${wid}/APPLY_APPROVAL.md` },
    note: `apply_approval=${status} mode=${mode}`,
  });
  await writeGlobalStatusFromPortfolio();

  return {
    ok: true,
    workId: wid,
    apply_approval_json: approvalJsonPath,
    apply_approval_md: approvalMdPath,
    status,
    mode,
    bundle_hash: bundleHash,
    highest_risk: highestRisk,
    errors,
    dry_run: !!dryRun,
  };
}

async function setApplyApprovalStatus({ workId, status, approvedBy, notes }) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };

  await migrateLegacyGateAArtifactsIfNeeded(wid);

  const workDir = `ai/lane_b/work/${wid}`;
  const metaText = await readTextIfExists(`${workDir}/META.json`);
  if (!metaText) return { ok: false, message: `Work item not found: missing ${workDir}/META.json.` };
  const approvalJsonPath = `${workDir}/APPLY_APPROVAL.json`;
  const approvalMdPath = `${workDir}/APPLY_APPROVAL.md`;

  const existingText = await readTextIfExists(approvalJsonPath);
  if (!existingText) return { ok: false, message: `Missing ${approvalJsonPath}. Run: node src/cli.js --apply-approval --workId ${wid}` };
  const parsed = safeJsonParse(existingText, approvalJsonPath);
  if (!parsed.ok) return { ok: false, message: parsed.message };

  const next = { ...parsed.json };
  next.status = status;
  next.mode = "manual";
  next.approved_at = status === "approved" ? nowISO() : null;
  next.approved_by = String(approvedBy || "human").trim() || "human";
  next.notes = notes ? String(notes).trim() : null;

  await writeText(approvalJsonPath, JSON.stringify(next, null, 2) + "\n");
  await writeText(approvalMdPath, renderApplyApprovalMarkdown(next, { errors: [] }));

  await appendLedger({ timestamp: nowISO(), action: status === "approved" ? "apply_approval_approved" : "apply_approval_rejected", workId: wid, approved_by: next.approved_by, notes: next.notes });
  await updateWorkStatus({
    workId: wid,
    stage: status === "approved" ? "APPLY_APPROVAL_APPROVED" : "APPLY_APPROVAL_PENDING",
    blocked: status !== "approved",
    blockingReason: status === "approved" ? null : "Apply approval rejected.",
    artifacts: { apply_approval_json: `ai/lane_b/work/${wid}/APPLY_APPROVAL.json`, apply_approval_md: `ai/lane_b/work/${wid}/APPLY_APPROVAL.md` },
    note: `apply_approval=${status} manual`,
  });
  await writeGlobalStatusFromPortfolio();

  return { ok: true, workId: wid, status, apply_approval_json: approvalJsonPath, apply_approval_md: approvalMdPath };
}

export async function approveApplyApproval({ workId, approvedBy = "human", notes = null } = {}) {
  return await setApplyApprovalStatus({ workId, status: "approved", approvedBy, notes });
}

export async function rejectApplyApproval({ workId, approvedBy = "human", notes = null } = {}) {
  return await setApplyApprovalStatus({ workId, status: "rejected", approvedBy, notes });
}

