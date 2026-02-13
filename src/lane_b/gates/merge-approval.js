import { existsSync } from "node:fs";
import { mkdir, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createHash } from "node:crypto";

import { appendFile, ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { readWorkStatusSnapshot, updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";
import { appendEvent as appendKnowledgeChangeEvent } from "../../lane_a/knowledge/knowledge-events-store.js";
import { bestEffortAffectedPaths, logMergeEvent } from "../lane-b-event-logger.js";
import { loadProjectPaths } from "../../paths/project-paths.js";
import { resolveStatePath } from "../../project/state-paths.js";
import { auditQaObligationsAgainstEditPaths, parseWaivedQaObligationsFromNotes } from "../qa/qa-obligations-audit.js";
import { validateDecisionPacket } from "../../contracts/validators/index.js";
import { renderDecisionPacketMd } from "../../lane_a/knowledge/committee-utils.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function normalizeRiskLevel(raw) {
  const s = normStr(raw).toLowerCase();
  if (s === "high") return "high";
  if (s === "normal") return "normal";
  if (s === "low") return "low";
  return "unknown";
}

function dualSignoffRequired({ obligations }) {
  const risk = normalizeRiskLevel(obligations?.risk_level);
  return risk === "high";
}

function collectWaivedObligations(audit) {
  const required = isPlainObject(audit?.required) ? audit.required : {};
  const waived = isPlainObject(audit?.waived) ? audit.waived : {};
  const out = [];
  if (required.unit === true && waived.unit === true) out.push("unit");
  if (required.integration === true && waived.integration === true) out.push("integration");
  if (required.e2e === true && waived.e2e === true) out.push("e2e");
  return out;
}

function renderInvariantWaiverDecisionMd(packet) {
  return renderDecisionPacketMd(packet);
}

async function writeInvariantWaiverDecisionPacket({
  paths,
  workId,
  repoId,
  audit,
  mergeCommitSha,
  mergeApprovalBy,
  dryRun = false,
}) {
  const waived = collectWaivedObligations(audit);
  if (!waived.length) return { ok: true, wrote: false, waived };

  const scope = repoId ? `repo:${repoId}` : "system";
  const decisionSeed = `${workId}\n${scope}\n${waived.join(",")}\n${normStr(mergeCommitSha)}`;
  const decisionId = `DEC_invariant_waiver_${sha256Hex(decisionSeed).slice(0, 16)}`;
  const questionId = `Q_invariant_waiver_${sha256Hex(`${decisionSeed}\nquestion`).slice(0, 16)}`;
  const createdAt = nowISO();
  const notes = normStr(audit?.qa_approval?.notes);
  const waiverByNotes = parseWaivedQaObligationsFromNotes(notes);
  const obligations = isPlainObject(audit?.obligations) ? audit.obligations : {};

  const packet = {
    version: 1,
    type: "INVARIANT_WAIVER",
    decision_id: decisionId,
    scope,
    trigger: "state_machine",
    blocking_state: "MERGE_APPROVAL_APPROVED",
    context: {
      summary: `QA obligations were explicitly waived for work ${workId}.`,
      why_automation_failed: "Merge completed with explicit QA waiver; human confirmation is required for invariant policy tracking.",
      what_is_known: [
        `work_id:${workId}`,
        `waived_obligations:${waived.join(",")}`,
        `qa_approved_by:${normStr(audit?.qa_approval?.by) || "unknown"}`,
        `merge_approved_by:${normStr(mergeApprovalBy) || "unknown"}`,
        `risk_level:${normalizeRiskLevel(obligations.risk_level)}`,
        notes ? `qa_notes:${notes}` : "qa_notes:(none)",
        `waiver_markers:unit=${waiverByNotes.unit ? "true" : "false"},integration=${waiverByNotes.integration ? "true" : "false"},e2e=${waiverByNotes.e2e ? "true" : "false"}`,
      ],
    },
    questions: [
      {
        id: questionId,
        question: `Should the INVARIANT_WAIVER decision for work ${workId} be accepted?`,
        expected_answer_type: "choice",
        constraints: "Choose one: confirm|reject",
        blocks: ["MERGE_APPROVAL_APPROVED"],
      },
    ],
    assumptions_if_unanswered: "Waiver remains under review and should be tracked as unresolved policy debt.",
    created_at: createdAt,
    status: "open",
  };
  validateDecisionPacket(packet);

  const jsonAbs = join(paths.knowledge.decisionsAbs, `DECISION-${decisionId}.json`);
  const mdAbs = join(paths.knowledge.decisionsAbs, `DECISION-${decisionId}.md`);
  if (existsSync(jsonAbs)) return { ok: true, wrote: false, skipped: "exists", decision_id: decisionId, waived };
  if (dryRun) return { ok: true, wrote: false, dry_run: true, decision_id: decisionId, waived, json_abs: jsonAbs, md_abs: mdAbs };

  await mkdir(paths.knowledge.decisionsAbs, { recursive: true });
  await writeFile(jsonAbs, JSON.stringify(packet, null, 2) + "\n", "utf8");
  await writeFile(mdAbs, renderInvariantWaiverDecisionMd(packet), "utf8");
  return { ok: true, wrote: true, decision_id: decisionId, waived, json_abs: jsonAbs, md_abs: mdAbs };
}

function safeJsonParse(text, label) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid JSON in ${label} (${msg}).` };
  }
}

function renderMergeApprovalMarkdown(doc, { errors = [] } = {}) {
  const lines = [];
  lines.push(`# Merge Approval (merge permission)`);
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
  const dualRequired = doc?.dual_signoff_required === true;
  lines.push(`- dual_signoff_required: \`${dualRequired ? "true" : "false"}\``);
  if (dualRequired) {
    const ownerBy = normStr(doc?.owner_signoff?.by) || "(null)";
    const ownerAt = normStr(doc?.owner_signoff?.at) || "(null)";
    const qaBy = normStr(doc?.qa_signoff?.by) || "(null)";
    const qaStatus = normStr(doc?.qa_signoff?.status) || "(null)";
    const qaAt = normStr(doc?.qa_signoff?.updated_at) || "(null)";
    lines.push(`- owner_signoff.by: \`${ownerBy}\``);
    lines.push(`- owner_signoff.at: \`${ownerAt}\``);
    lines.push(`- qa_signoff.status: \`${qaStatus}\``);
    lines.push(`- qa_signoff.by: \`${qaBy}\``);
    lines.push(`- qa_signoff.updated_at: \`${qaAt}\``);
  }
  lines.push("");
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

function ciIsGreen(ciStatusJson) {
  if (!ciStatusJson || typeof ciStatusJson !== "object") return false;
  const overall = String(ciStatusJson.overall || "").trim().toLowerCase();
  if (overall !== "success") return false;
  const checks = Array.isArray(ciStatusJson.checks) ? ciStatusJson.checks : [];
  const failing = checks.filter((c) => String(c?.conclusion || "").trim().toLowerCase() === "failure");
  return failing.length === 0;
}

async function auditQaObligationsOrError({ workId, workDir, bundle }) {
  const obligationsPath = `${workDir}/QA/obligations.json`;
  const obligationsText = await readTextIfExists(obligationsPath);
  if (!obligationsText) {
    return { ok: false, message: `Missing ${obligationsPath}. Run: node src/cli.js --qa-obligations --workId ${workId}` };
  }
  const oParsed = safeJsonParse(obligationsText, obligationsPath);
  if (!oParsed.ok) return { ok: false, message: oParsed.message };
  const obligations = oParsed.json;

  const qaApprovalPath = `${workDir}/QA_APPROVAL.json`;
  const qaText = await readTextIfExists(qaApprovalPath);
  let approval = { status: "pending", by: null, notes: null, updated_at: null };
  if (qaText) {
    const ap = safeJsonParse(qaText, qaApprovalPath);
    if (!ap.ok) return { ok: false, message: ap.message };
    approval = ap.json || approval;
  }
  const approvalStatus = String(approval?.status || "pending").trim().toLowerCase() || "pending";
  const editPaths = [];
  const repos = Array.isArray(bundle?.repos) ? bundle.repos : [];
  for (const r of repos) {
    const planPath = String(r?.patch_plan_json_path || "").trim();
    if (!planPath) continue;
    // eslint-disable-next-line no-await-in-loop
    const planText = await readTextIfExists(planPath);
    if (!planText) continue;
    try {
      const plan = JSON.parse(planText);
      const edits = Array.isArray(plan?.edits) ? plan.edits : [];
      for (const e of edits) editPaths.push(e?.path);
    } catch {
      // ignore
    }
  }

  const audit = auditQaObligationsAgainstEditPaths({ obligations, editPaths, qaApprovalStatus: approvalStatus, qaApprovalNotes: approval?.notes });
  if (!audit.ok) {
    if (audit.missing && audit.missing.includes("qa_rejected")) return { ok: false, message: `Merge approval blocked: QA status is rejected (${qaApprovalPath}).` };
    const missing = Array.isArray(audit.missing) ? audit.missing : ["unit", "integration", "e2e"];
    return { ok: false, message: `Merge approval blocked: QA obligations require ${missing.join(", ")} tests, but no corresponding test edits were found in patch plans. Add tests or explicitly waive via: node src/cli.js --qa-approve --workId ${workId} --by \"<name>\" --notes \"waive: ${missing.join(",")}\"` };
  }
  return {
    ok: true,
    ...audit,
    obligations,
    obligations_path: obligationsPath,
    qa_approval_path: qaApprovalPath,
    qa_approval: {
      status: approvalStatus,
      by: normStr(approval?.by) || null,
      notes: typeof approval?.notes === "string" ? approval.notes : null,
      updated_at: normStr(approval?.updated_at) || null,
    },
  };
}

async function migrateLegacyGateBArtifactsIfNeeded(workId) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: true, migrated: false };
  const workDir = `ai/lane_b/work/${wid}`;
  const legacyJson = `${workDir}/GATE_B.json`;
  const legacyMd = `${workDir}/GATE_B.md`;
  const nextJson = `${workDir}/MERGE_APPROVAL.json`;
  const nextMd = `${workDir}/MERGE_APPROVAL.md`;

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

function enforceDualSignoffOrError({ audit, workId }) {
  if (!dualSignoffRequired({ obligations: audit?.obligations })) {
    return { ok: true, required: false };
  }
  const qaStatus = normStr(audit?.qa_approval?.status).toLowerCase() || "pending";
  const qaBy = normStr(audit?.qa_approval?.by);
  if (qaStatus !== "approved" || !qaBy) {
    return {
      ok: false,
      required: true,
      message: `Merge approval blocked: high-risk QA obligations require dual signoff (owner + QA). Missing QA signoff. Run: node src/cli.js --qa-approve --workId ${workId} --by "<qa-name>" [--notes "..."]`,
    };
  }
  return { ok: true, required: true };
}

export async function requestMergeApproval({ workId, dryRun = false } = {}) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };

  await migrateLegacyGateBArtifactsIfNeeded(wid);

  const workDir = `ai/lane_b/work/${wid}`;
  const metaText = await readTextIfExists(`${workDir}/META.json`);
  if (!metaText) return { ok: false, message: `Work item not found: missing ${workDir}/META.json.` };

  const statusRes = await readWorkStatusSnapshot(wid);
  const stage = statusRes.ok ? String(statusRes.snapshot?.current_stage || "").trim() : "";
  if (stage !== "CI_GREEN") {
    return { ok: false, message: `Merge approval cannot be requested unless stage is CI_GREEN (current_stage=${stage || "(missing)"}).` };
  }
  const approvalJsonPath = `${workDir}/MERGE_APPROVAL.json`;
  const approvalMdPath = `${workDir}/MERGE_APPROVAL.md`;

  const bundlePath = `${workDir}/BUNDLE.json`;
  const bundleText = await readTextIfExists(bundlePath);
  if (!bundleText) return { ok: false, message: `Missing ${bundlePath}.` };
  const bundleParsed = safeJsonParse(bundleText, bundlePath);
  if (!bundleParsed.ok) return { ok: false, message: bundleParsed.message };
  const bundleHash = typeof bundleParsed.json?.bundle_hash === "string" ? bundleParsed.json.bundle_hash.trim() : "";
  if (!bundleHash) return { ok: false, message: `BUNDLE.json missing bundle_hash (${bundlePath}).` };

  const qaAudit = await auditQaObligationsOrError({ workId: wid, workDir, bundle: bundleParsed.json });
  if (!qaAudit.ok) return { ok: false, message: qaAudit.message };

  const prPath = `${workDir}/PR.json`;
  const prText = await readTextIfExists(prPath);
  if (!prText) return { ok: false, message: `Missing ${prPath}.` };

  const ciPath = `${workDir}/CI/CI_Status.json`;
  const ciText = await readTextIfExists(ciPath);
  if (!ciText) return { ok: false, message: `Missing ${ciPath}. Poll CI first (node src/cli.js --ci-update --workId ${wid}).` };
  const ciParsed = safeJsonParse(ciText, ciPath);
  if (!ciParsed.ok) return { ok: false, message: ciParsed.message };
  if (!ciIsGreen(ciParsed.json)) {
    await updateWorkStatus({ workId: wid, stage: "CI_FAILED", blocked: false, blockingReason: "CI is not green; merge approval cannot be requested." });
    await writeGlobalStatusFromPortfolio();
    return { ok: false, message: "CI is not green; cannot request merge approval (stage drift detected)." };
  }

  const approval = {
    version: 1,
    workId: wid,
    status: "pending",
    mode: "manual",
    bundle_hash: bundleHash,
    approved_at: null,
    approved_by: null,
    reason_codes: [],
    notes: null,
    dual_signoff_required: dualSignoffRequired({ obligations: qaAudit.obligations }),
    owner_signoff: null,
    qa_signoff: {
      status: qaAudit.qa_approval?.status || "pending",
      by: qaAudit.qa_approval?.by || null,
      notes: qaAudit.qa_approval?.notes || null,
      updated_at: qaAudit.qa_approval?.updated_at || null,
    },
  };

  if (!dryRun) {
    await ensureDir(workDir);
    await writeText(approvalJsonPath, JSON.stringify(approval, null, 2) + "\n");
    await writeText(approvalMdPath, renderMergeApprovalMarkdown(approval));
  }

  await appendLedger({ timestamp: nowISO(), action: "merge_approval_requested", workId: wid, bundle_hash: bundleHash });
  await updateWorkStatus({
    workId: wid,
    stage: "MERGE_APPROVAL_PENDING",
    blocked: true,
    blockingReason: "Merge approval requires human approval.",
    artifacts: {
      merge_approval_json: `ai/lane_b/work/${wid}/MERGE_APPROVAL.json`,
      merge_approval_md: `ai/lane_b/work/${wid}/MERGE_APPROVAL.md`,
      pr_json: `ai/lane_b/work/${wid}/PR.json`,
      ci_status: `ai/lane_b/work/${wid}/CI/CI_Status.json`,
    },
    note: "merge_approval=pending manual",
  });
  await writeGlobalStatusFromPortfolio();

  return { ok: true, workId: wid, merge_approval_json: approvalJsonPath, merge_approval_md: approvalMdPath, status: "pending", dry_run: !!dryRun };
}

async function setMergeApprovalStatus({ workId, status, approvedBy, notes }) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing workId." };

  await migrateLegacyGateBArtifactsIfNeeded(wid);

  const workDir = `ai/lane_b/work/${wid}`;
  const metaText = await readTextIfExists(`${workDir}/META.json`);
  if (!metaText) return { ok: false, message: `Work item not found: missing ${workDir}/META.json.` };
  const approvalJsonPath = `${workDir}/MERGE_APPROVAL.json`;
  const approvalMdPath = `${workDir}/MERGE_APPROVAL.md`;
  const approvalText = await readTextIfExists(approvalJsonPath);
  if (!approvalText) return { ok: false, message: `Missing ${approvalJsonPath}. Run: node src/cli.js --merge-approval --workId ${wid}` };
  const approvalParsed = safeJsonParse(approvalText, approvalJsonPath);
  if (!approvalParsed.ok) return { ok: false, message: approvalParsed.message };

  let qaAudit = null;

  // Merge approval requires CI to still be green at approval time.
  if (status === "approved") {
    const ciPath = `${workDir}/CI/CI_Status.json`;
    const ciText = await readTextIfExists(ciPath);
    if (!ciText) return { ok: false, message: `Missing ${ciPath}.` };
    const ciParsed = safeJsonParse(ciText, ciPath);
    if (!ciParsed.ok) return { ok: false, message: ciParsed.message };
    if (!ciIsGreen(ciParsed.json)) return { ok: false, message: "Cannot approve merge approval: CI is not green." };

    const bundlePath = `${workDir}/BUNDLE.json`;
    const bundleText = await readTextIfExists(bundlePath);
    if (!bundleText) return { ok: false, message: `Missing ${bundlePath}.` };
    const bundleParsed = safeJsonParse(bundleText, bundlePath);
    if (!bundleParsed.ok) return { ok: false, message: bundleParsed.message };
    qaAudit = await auditQaObligationsOrError({ workId: wid, workDir, bundle: bundleParsed.json });
    if (!qaAudit.ok) return { ok: false, message: qaAudit.message };
    const dual = enforceDualSignoffOrError({ audit: qaAudit, workId: wid });
    if (!dual.ok) return { ok: false, message: dual.message };
  }

  const next = { ...approvalParsed.json };
  next.status = status;
  next.mode = "manual";
  next.approved_at = status === "approved" ? nowISO() : null;
  next.approved_by = String(approvedBy || "human").trim() || "human";
  next.notes = notes ? String(notes).trim() : null;
  if (status === "approved") {
    next.dual_signoff_required = dualSignoffRequired({ obligations: qaAudit?.obligations });
    next.owner_signoff = { by: next.approved_by, at: next.approved_at };
    next.qa_signoff = {
      status: qaAudit?.qa_approval?.status || "pending",
      by: qaAudit?.qa_approval?.by || null,
      notes: qaAudit?.qa_approval?.notes || null,
      updated_at: qaAudit?.qa_approval?.updated_at || null,
    };
  } else if (next.dual_signoff_required === true) {
    next.owner_signoff = null;
  }

  await writeText(approvalJsonPath, JSON.stringify(next, null, 2) + "\n");
  await writeText(approvalMdPath, renderMergeApprovalMarkdown(next));

  await appendLedger({ timestamp: nowISO(), action: status === "approved" ? "merge_approval_approved" : "merge_approval_rejected", workId: wid, approved_by: next.approved_by, notes: next.notes });
  await updateWorkStatus({
    workId: wid,
    stage: status === "approved" ? "MERGE_APPROVAL_APPROVED" : "MERGE_APPROVAL_PENDING",
    blocked: status !== "approved",
    blockingReason: status === "approved" ? null : "Merge approval rejected.",
    artifacts: { merge_approval_json: `ai/lane_b/work/${wid}/MERGE_APPROVAL.json`, merge_approval_md: `ai/lane_b/work/${wid}/MERGE_APPROVAL.md` },
    note: `merge_approval=${status} manual`,
  });
  await writeGlobalStatusFromPortfolio();

  // Lane B -> Lane A feedback event (best-effort; never fails merge approval).
  if (status === "approved") {
    try {
      const projectRoot = typeof process.env.AI_PROJECT_ROOT === "string" ? process.env.AI_PROJECT_ROOT : null;
      if (!projectRoot) throw new Error("Missing AI_PROJECT_ROOT.");
      const paths = await loadProjectPaths({ projectRoot });

      const prPath = `${workDir}/PR.json`;
      const prText = await readTextIfExists(prPath);
      const prParsed = prText ? safeJsonParse(prText, prPath) : { ok: false, message: "missing PR.json" };
      const pr = prParsed.ok ? prParsed.json : null;
      const headBranch = typeof pr?.head_branch === "string" ? pr.head_branch.trim() : "";
      const repoId = repoIdFromHeadBranch({ workId: wid, headBranch });

      const ciPath = `${workDir}/CI/CI_Status.json`;
      const ciText = await readTextIfExists(ciPath);
      const ciParsed = ciText ? safeJsonParse(ciText, ciPath) : { ok: false, message: "missing CI status" };
      const headSha = ciParsed.ok && typeof ciParsed.json?.head_sha === "string" ? ciParsed.json.head_sha.trim() : "";
      if (!repoId) throw new Error("Unable to infer repo_id from PR head branch.");
      if (!headSha) throw new Error("Missing CI head_sha; cannot emit merge event.");
      const qaObligations = isPlainObject(qaAudit?.obligations) ? qaAudit.obligations : {};
      const waivedObligations = collectWaivedObligations(qaAudit);
      const qaWaiver = {
        explicit: waivedObligations.length > 0,
        waived_obligations: waivedObligations,
        by: qaAudit?.qa_approval?.by || null,
        notes: qaAudit?.qa_approval?.notes || null,
        updated_at: qaAudit?.qa_approval?.updated_at || null,
      };

      if (qaWaiver.explicit) {
        try {
          const waiverDecision = await writeInvariantWaiverDecisionPacket({
            paths,
            workId: wid,
            repoId,
            audit: qaAudit,
            mergeCommitSha: headSha,
            mergeApprovalBy: next.approved_by,
          });
          if (waiverDecision.wrote) {
            await appendLedger({
              timestamp: nowISO(),
              action: "invariant_waiver_decision_created",
              workId: wid,
              repo_id: repoId,
              decision_id: waiverDecision.decision_id,
              waived_obligations: waiverDecision.waived,
            });
          }
        } catch (waiverErr) {
          const waiverMsg = waiverErr instanceof Error ? waiverErr.message : String(waiverErr);
          await appendLedger({
            timestamp: nowISO(),
            action: "invariant_waiver_decision_failed",
            workId: wid,
            repo_id: repoId,
            error: waiverMsg,
          });
        }
      }

      await appendKnowledgeChangeEvent(
        {
          type: "merge",
          scope: `repo:${repoId}`,
          repo_id: repoId,
          work_id: wid,
          pr_number: pr && typeof pr.pr_number === "number" ? pr.pr_number : null,
          commit: headSha,
          artifacts: {
            paths: [`ai/lane_b/work/${wid}/PR.json`, `ai/lane_b/work/${wid}/CI/CI_Status.json`, `ai/lane_b/work/${wid}/MERGE_APPROVAL.json`],
            fingerprints: [typeof next.bundle_hash === "string" && next.bundle_hash.trim() ? `bundle:${next.bundle_hash.trim()}` : `work:${wid}`],
          },
          summary: `Merge approval approved for work ${wid}; treated as merge signal for repo ${repoId}.`,
          timestamp: nowISO(),
        },
        { opsLaneAAbs: paths.laneA.rootAbs, dryRun: false },
      );
      await appendLedger({ timestamp: nowISO(), action: "knowledge_event_emitted", workId: wid, type: "merge", repo_id: repoId, commit: headSha });

      // Lane B merge event logging (ops-only, segmented). This is an additional, merge-friendly contract.
      try {
        const owner = pr && typeof pr.owner === "string" ? pr.owner.trim() : "";
        const repo = pr && typeof pr.repo === "string" ? pr.repo.trim() : "";
        const repoFullName = owner && repo ? `${owner}/${repo}` : null;
        const repoAbsGuess = repoId ? join(paths.reposRootAbs, repoId) : null;
        const affected = await bestEffortAffectedPaths({
          repoFullName,
          pr_number: pr && typeof pr.pr_number === "number" ? pr.pr_number : null,
          merge_commit_sha: headSha,
          repoAbs: repoAbsGuess && existsSync(repoAbsGuess) ? repoAbsGuess : null,
        });

        const mergeRes = await logMergeEvent(
          {
            repo_id: repoId,
            pr_number: pr && typeof pr.pr_number === "number" ? pr.pr_number : null,
            merge_commit_sha: headSha,
            base_branch: pr && typeof pr.base_branch === "string" ? pr.base_branch.trim() : "",
            affected_paths: affected.paths,
            work_id: wid,
            pr: {
              number: pr && typeof pr.pr_number === "number" ? pr.pr_number : null,
              owner: pr && typeof pr.owner === "string" ? pr.owner.trim() : null,
              repo: pr && typeof pr.repo === "string" ? pr.repo.trim() : null,
              url: pr && typeof pr.url === "string" ? pr.url.trim() : null,
              base_branch: pr && typeof pr.base_branch === "string" ? pr.base_branch.trim() : null,
              head_branch: pr && typeof pr.head_branch === "string" ? pr.head_branch.trim() : null,
            },
            merge_sha: headSha,
            changed_paths: affected.paths,
            obligations: qaObligations,
            risk_level: normalizeRiskLevel(qaObligations.risk_level),
            qa_waiver: qaWaiver,
          },
          { projectRoot: paths.opsRootAbs, dryRun: false },
        );
        await appendLedger({ timestamp: nowISO(), action: "merge_event_logged", workId: wid, repo_id: repoId, segment_file: mergeRes.segment_file });
      } catch (err2) {
        const msg2 = err2 instanceof Error ? err2.message : String(err2);
        await appendLedger({ timestamp: nowISO(), action: "merge_event_log_failed", workId: wid, repo_id: repoId, error: msg2 });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      await appendLedger({ timestamp: nowISO(), action: "knowledge_event_emit_failed", workId: wid, type: "merge", error: msg });
    }
  }

  return { ok: true, workId: wid, status, merge_approval_json: approvalJsonPath, merge_approval_md: approvalMdPath };
}

export async function approveMergeApproval({ workId, approvedBy = "human", notes = null } = {}) {
  return await setMergeApprovalStatus({ workId, status: "approved", approvedBy, notes });
}

export async function rejectMergeApproval({ workId, approvedBy = "human", notes = null } = {}) {
  return await setMergeApprovalStatus({ workId, status: "rejected", approvedBy, notes });
}
