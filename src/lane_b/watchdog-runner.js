import { readdir, unlink } from "node:fs/promises";

import { appendFile, ensureDir, readTextIfExists, writeText } from "../utils/fs.js";
import { resolveStatePath } from "../project/state-paths.js";
import { computeSchedule } from "../scheduler/scheduler.js";
import { readWorkStatusSnapshot, updateWorkStatus, writeGlobalStatusFromPortfolio } from "../utils/status-writer.js";
import { writeWorkPlan, readBundleIfExists } from "../utils/plan-writer.js";
import { acquireLock, releaseLock } from "../utils/lockfile.js";

function nowISO() {
  return new Date().toISOString();
}

function stageOrder() {
  // Approval model:
  // - apply-approval: permission to create PR
  // - merge-approval: permission to merge (manual, only when CI is green)
  return [
    "INTAKE_RECEIVED",
    "ROUTED",
    "TASKS_CREATED",
    "SWEEP_READY",
    "PROPOSED",
    "PATCH_PLANNED",
    "QA_PLANNED",
    "BUNDLED",
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
    "DONE",
  ];
}

function normalizeStageForOrder(stage) {
  const s = String(stage || "").trim();
  if (s === "GATE_A_PENDING") return "APPLY_APPROVAL_PENDING";
  if (s === "GATE_A_APPROVED") return "APPLY_APPROVAL_APPROVED";
  if (s === "GATE_B_PENDING") return "MERGE_APPROVAL_PENDING";
  if (s === "APPROVED_TO_MERGE") return "MERGE_APPROVAL_APPROVED";
  return s;
}

function stageIndex(stage) {
  return stageOrder().indexOf(normalizeStageForOrder(stage));
}

async function listTeamTaskFiles(workId) {
  const dir = resolveStatePath(`ai/lane_b/work/${workId}/tasks`);
  try {
    const entries = await readdir(dir, { withFileTypes: true });
    return entries.filter((e) => e.isFile() && e.name.endsWith(".md")).map((e) => e.name);
  } catch {
    return [];
  }
}

async function writeWatchdogFailureReport({ workId, title, bodyLines }) {
  const dir = `ai/lane_b/work/${workId}/failure-reports`;
  await ensureDir(dir);
  const path = `${dir}/watchdog.md`;
  const lines = [];
  lines.push(`# Watchdog failure: ${workId}`);
  lines.push("");
  lines.push(`Timestamp: ${nowISO()}`);
  lines.push("");
  lines.push(`## ${title}`);
  lines.push("");
  for (const l of bodyLines || []) lines.push(String(l));
  lines.push("");
  await writeText(path, lines.join("\n"));
  return path;
}

async function writeProposeFailedReport({ workId, bodyLines }) {
  const dir = `ai/lane_b/work/${workId}/errors`;
  await ensureDir(dir);
  const path = `${dir}/PROPOSE_FAILED.md`;
  const lines = [];
  lines.push(`# PROPOSE_FAILED: ${workId}`);
  lines.push("");
  lines.push(`Timestamp: ${nowISO()}`);
  lines.push("");
  for (const l of bodyLines || []) lines.push(String(l));
  lines.push("");
  await writeText(path, lines.join("\n"));
  return path;
}

async function readQueueIfExists() {
  const raw = await readTextIfExists("ai/lane_b/schedule/QUEUE.json");
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed.map((x) => String(x)).filter(Boolean);
    if (Array.isArray(parsed?.work_ids)) return parsed.work_ids.map((x) => String(x)).filter(Boolean);
    if (Array.isArray(parsed?.queue)) return parsed.queue.map((x) => String(x)).filter(Boolean);
  } catch {
    return null;
  }
  return null;
}

async function appendWatchdogLog(line) {
  const stamp = nowISO().slice(0, 10).replace(/-/g, "");
  const path = `ai/lane_b/logs/watchdog-${stamp}.log`;
  await appendFile(path, `${nowISO()} ${line}\n`);
  return path;
}

export async function runWatchdog({
  orchestrator,
  limit = null,
  dryRun = false,
  workId = null,
  stopAt = "APPLY_APPROVAL_PENDING",
  maxMinutes = 8,
  watchdogCi = true,
  watchdogPrepr = true,
  ciPoller = null,
  ciFixer = null,
  ciUpdater = null,
} = {}) {
  const started = nowISO();
  const normalizedStopAt = normalizeStageForOrder(stopAt);
  const targetStage = stageOrder().includes(normalizedStopAt) ? normalizedStopAt : "APPLY_APPROVAL_PENDING";
  const targetIndex = stageIndex(targetStage);
  const deadline = Date.now() + Math.max(1, Number(maxMinutes) || 8) * 60 * 1000;

  const globalLock = await acquireLock({ path: "ai/lane_b/.watchdog.lock", staleMs: 30 * 60 * 1000, metadata: { scope: "watchdog" } });
  if (!globalLock.ok) {
    const message = globalLock.reason === "locked" ? "watchdog lock already held" : "watchdog lock acquisition failed";
    await appendWatchdogLog(`global_lock_failed reason=${globalLock.reason || "unknown"}`);
    return { ok: false, message };
  }

  const advanced = [];
  const failed = [];
  const skipped = [];
  let sched = null;

  try {
    if (globalLock.stale_replaced) {
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "work_lock_stale_replaced", scope: "global", path: "ai/lane_b/.watchdog.lock" }) + "\n",
      );
    }
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: started, action: "watchdog_started", limit: limit ?? null, stop_at: targetStage }) + "\n");

    const queue = workId ? [workId] : await readQueueIfExists();
    const allowlist = queue && queue.length ? queue : workId ? [workId] : null;
    const orderBy = queue && queue.length ? "queue" : "created_at";
    sched = await computeSchedule({ limit, orderBy, workIdAllowlist: allowlist, dryRun });
    if (!sched.ok) return { ok: false, message: "Failed to compute schedule." };

    const selectedBase = Array.isArray(sched.schedule?.selected) ? sched.schedule.selected : [];
    const selected = selectedBase.slice();
    // CI-enabled watchdog must be able to process APPLIED-stage work items (post-PR),
    // even if the scheduler marks them as beyond_watchdog.
    if (watchdogCi) {
      const skippedByScheduler = Array.isArray(sched.schedule?.skipped) ? sched.schedule.skipped : [];
      const extra = skippedByScheduler.filter((x) => String(x?.reason || "") === "beyond_watchdog:APPLIED");
      if (extra.length) {
        const seen = new Set(selected.map((x) => String(x?.work_id || "").trim()).filter(Boolean));
        for (const it of extra) {
          const wid = String(it?.work_id || "").trim();
          if (!wid || seen.has(wid)) continue;
          selected.unshift({ work_id: wid, reason: "eligible:watchdog_ci_postpr", score: null, repos: [] });
          seen.add(wid);
        }
      }
    }

    for (const item of selected) {
      if (Date.now() > deadline) {
        skipped.push({ work_id: item.work_id, reason: "time_budget" });
        break;
      }
      const currentWorkId = String(item.work_id || "").trim();
      if (!currentWorkId) continue;
      if (workId && currentWorkId !== workId) continue;
      const workDir = `ai/lane_b/work/${currentWorkId}`;

      if (dryRun) {
        await appendWatchdogLog(`dry_run workId=${currentWorkId} skip=1`);
        skipped.push({ work_id: currentWorkId, reason: "dry_run" });
        continue;
      }

      const workLock = await acquireLock({ path: `${workDir}/.lock`, staleMs: 30 * 60 * 1000, metadata: { workId: currentWorkId } });
      if (!workLock.ok) {
        skipped.push({ work_id: currentWorkId, reason: `locked:${workLock.reason || "locked"}` });
        await appendWatchdogLog(`work_locked workId=${currentWorkId} reason=${workLock.reason || "locked"}`);
        continue;
      }
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "work_locked", workId: currentWorkId, path: `${workDir}/.lock` }) + "\n",
      );
      if (workLock.stale_replaced) {
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "work_lock_stale_replaced", workId: currentWorkId, path: `${workDir}/.lock` }) + "\n",
        );
      }

      try {
        const statusRes = await readWorkStatusSnapshot(currentWorkId);
        let currentStage = String(statusRes.ok ? statusRes.snapshot?.current_stage : "");
        if (statusRes.ok && statusRes.snapshot?.blocked) {
          skipped.push({ work_id: currentWorkId, reason: `blocked:${statusRes.snapshot.blocking_reason || "unknown"}` });
          continue;
        }
        const isPostPrStage =
          currentStage === "APPLIED" ||
          String(currentStage || "").startsWith("CI_") ||
          currentStage === "MERGE_APPROVAL_PENDING" ||
          currentStage === "MERGE_APPROVAL_APPROVED" ||
          currentStage === "GATE_B_PENDING" ||
          currentStage === "APPROVED_TO_MERGE" ||
          currentStage === "MERGED" ||
          currentStage === "DONE";
        if (!isPostPrStage && stageIndex(currentStage) >= targetIndex) {
          skipped.push({ work_id: currentWorkId, reason: `already_at:${currentStage}` });
          continue;
        }

        // Post-PR CI dispatcher (single-work folder; no new intake/work).
        if (watchdogCi) {
          const lc = (s) => String(s || "").trim().toLowerCase();
          let stageFromStatusJson = null;
          try {
            const txt = await readTextIfExists(`${workDir}/status.json`);
            if (txt) {
              const parsed = JSON.parse(txt);
              if (parsed && typeof parsed === "object" && typeof parsed.stage === "string" && parsed.stage.trim()) stageFromStatusJson = parsed.stage.trim();
            }
          } catch {
            stageFromStatusJson = null;
          }

          const stageForCi = stageFromStatusJson || currentStage;
          const eligibleStage = stageForCi === "APPLIED" || stageForCi === "CI_PENDING";
          if (!eligibleStage) {
            skipped.push({ work_id: currentWorkId, reason: "ci_not_eligible_stage" });
          } else {
            try {
              const updater =
                typeof ciUpdater === "function"
                  ? ciUpdater
                  : async ({ workId }) => {
                      const { runCiUpdate } = await import("./ci/ci-update.js");
                      return await runCiUpdate({ workId });
                    };
              const updateRes = await updater({ workId: currentWorkId });
              if (!updateRes || updateRes.ok !== true) throw new Error(String(updateRes?.message || "ci-update failed"));
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              const report = await writeWatchdogFailureReport({ workId: currentWorkId, title: "CI update failed", bodyLines: [msg] });
              await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "ci_update_failed", report }) + "\n");
              failed.push({ work_id: currentWorkId, reason: "ci_update_failed", report });
              continue;
            }

            const ciPath = `${workDir}/CI/CI_Status.json`;
            const ciText = await readTextIfExists(ciPath);
            if (!ciText) {
              skipped.push({ work_id: currentWorkId, reason: "ci_status_missing" });
              continue;
            }

            let ci = null;
            try {
              ci = JSON.parse(ciText);
            } catch {
              skipped.push({ work_id: currentWorkId, reason: "ci_status_invalid_json" });
              continue;
            }

            const overall = lc(ci?.overall);
            const checks = Array.isArray(ci?.checks) ? ci.checks : [];
            const allCompleted = checks.length > 0 && checks.every((c) => lc(c?.status) === "completed");
            const allSuccess = checks.length > 0 && checks.every((c) => lc(c?.conclusion) === "success");
            const canPromote =
              overall === "success" && allCompleted && allSuccess && stageIndex(stageForCi) >= 0 && stageIndex(stageForCi) < stageIndex("CI_GREEN");

            if (canPromote) {
              await updateWorkStatus({
                workId: currentWorkId,
                stage: "CI_GREEN",
                blocked: false,
                artifacts: { ci_status: `ai/lane_b/work/${currentWorkId}/CI/CI_Status.json` },
                note: "ci_green",
              });
              await appendFile(
                "ai/lane_b/ledger.jsonl",
                JSON.stringify({ timestamp: nowISO(), action: "watchdog_advanced", workId: currentWorkId, from_stage: stageForCi || null, to_stage: "CI_GREEN" }) + "\n",
              );
              await writeGlobalStatusFromPortfolio();
              advanced.push({ work_id: currentWorkId, result: "ci_green" });
              break;
            }

            skipped.push({ work_id: currentWorkId, reason: `ci_no_promotion:${overall || "(missing)"}` });
            continue;
          }
        }

        if (!watchdogPrepr) {
          skipped.push({ work_id: currentWorkId, reason: "watchdog_prepr_disabled" });
          continue;
        }

      const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
      if (!routingText) {
        const report = await writeWatchdogFailureReport({
          workId: currentWorkId,
          title: "Missing routing",
          bodyLines: [`Missing ${workDir}/ROUTING.json.`, "Run routing/sweep before watchdog."],
        });
        await updateWorkStatus({
          workId: currentWorkId,
          stage: "FAILED",
          blocked: true,
          blockingReason: "watchdog_missing_routing",
          artifacts: { watchdog_report: report },
        });
        await writeGlobalStatusFromPortfolio();
        failed.push({ work_id: currentWorkId, reason: "missing_routing", report });
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "missing_routing", report }) + "\n");
        continue;
      }

      let routing;
      try {
        routing = JSON.parse(routingText);
      } catch {
        const report = await writeWatchdogFailureReport({
          workId: currentWorkId,
          title: "Invalid routing",
          bodyLines: [`Invalid JSON in ${workDir}/ROUTING.json.`],
        });
        await updateWorkStatus({
          workId: currentWorkId,
          stage: "FAILED",
          blocked: true,
          blockingReason: "watchdog_invalid_routing",
          artifacts: { watchdog_report: report },
        });
        await writeGlobalStatusFromPortfolio();
        failed.push({ work_id: currentWorkId, reason: "invalid_routing", report });
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "invalid_routing", report }) + "\n");
        continue;
      }

      // Stage: ROUTED (ensure status reflects routing availability)
      if (stageIndex(currentStage) < stageIndex("ROUTED")) {
        await updateWorkStatus({ workId: currentWorkId, stage: "ROUTED", blocked: false, artifacts: { routing_json: `${workDir}/ROUTING.json` } });
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "watchdog_advanced", workId: currentWorkId, from_stage: currentStage || null, to_stage: "ROUTED" }) + "\n",
        );
        currentStage = "ROUTED";
        advanced.push({ work_id: currentWorkId, result: "advanced", stage: "ROUTED" });
        continue;
      }
      if (targetIndex <= stageIndex("ROUTED")) {
        advanced.push({ work_id: currentWorkId, result: "stopped_at", stage: "ROUTED" });
        continue;
      }

      const selectedTeams = Array.isArray(routing?.selected_teams) ? routing.selected_teams.filter(Boolean) : [];
      if (!selectedTeams.length) {
        const report = await writeWatchdogFailureReport({
          workId: currentWorkId,
          title: "Missing selected teams",
          bodyLines: [`${workDir}/ROUTING.json has no selected_teams.`],
        });
        await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_missing_teams", artifacts: { watchdog_report: report } });
        await writeGlobalStatusFromPortfolio();
        failed.push({ work_id: currentWorkId, reason: "missing_teams", report });
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "missing_teams", report }) + "\n");
        continue;
      }

      const taskFiles = await listTeamTaskFiles(currentWorkId);
      const taskSet = new Set(taskFiles);
      const missingTasks = selectedTeams.filter((t) => !taskSet.has(`${t}.md`));
      if (missingTasks.length) {
        const tasksRes = await orchestrator.createTeamTasksForWorkId({ workId: currentWorkId, ignorePendingDecisionCheck: true });
        if (!tasksRes.ok) {
          const report = await writeWatchdogFailureReport({
            workId: currentWorkId,
            title: "Task creation failed",
            bodyLines: [String(tasksRes.message || "create-tasks failed")],
          });
          await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_tasks_failed", artifacts: { watchdog_report: report } });
          await writeGlobalStatusFromPortfolio();
          failed.push({ work_id: currentWorkId, reason: "tasks_failed", report });
          await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "tasks_failed", report }) + "\n");
          continue;
        }
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "tasks_created", workId: currentWorkId, teams: selectedTeams }) + "\n");
        // One major action per work per run.
        advanced.push({ work_id: currentWorkId, result: "tasks_created" });
        continue;
      }

      if (stageIndex(currentStage) < stageIndex("SWEEP_READY")) {
        await updateWorkStatus({ workId: currentWorkId, stage: "SWEEP_READY", blocked: false });
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "watchdog_advanced", workId: currentWorkId, from_stage: currentStage || null, to_stage: "SWEEP_READY" }) + "\n",
        );
        currentStage = "SWEEP_READY";
        advanced.push({ work_id: currentWorkId, result: "advanced", stage: "SWEEP_READY" });
        continue;
      }
      if (targetIndex <= stageIndex("SWEEP_READY")) {
        advanced.push({ work_id: currentWorkId, result: "stopped_at", stage: "SWEEP_READY" });
        continue;
      }

      const bundleBefore = await readBundleIfExists(currentWorkId);
      if (!bundleBefore.ok) {
        const proposeRes = await orchestrator.propose({ workId: currentWorkId, teams: null, withPatchPlans: true });
        if (!proposeRes.ok) {
          const proposalFailedJson = await readTextIfExists(`${workDir}/PROPOSAL_FAILED.json`);
          const reportPath = await writeProposeFailedReport({
            workId: currentWorkId,
            bodyLines: [
              String(proposeRes.message || "propose failed"),
              ...(proposalFailedJson ? ["", `See ${workDir}/PROPOSAL_FAILED.json`] : []),
            ],
          });
          const watchdogReport = await writeWatchdogFailureReport({
            workId: currentWorkId,
            title: "Proposal failed",
            bodyLines: [String(proposeRes.message || "propose failed"), `See ${reportPath}`],
          });
          await updateWorkStatus({
            workId: currentWorkId,
            stage: "FAILED",
            blocked: true,
            blockingReason: "PROPOSAL_FAILED",
            artifacts: { watchdog_report: watchdogReport, propose_failed: reportPath },
          });
          await writeGlobalStatusFromPortfolio();
          failed.push({ work_id: currentWorkId, reason: "propose_failed", report: watchdogReport });
          await appendFile(
            "ai/lane_b/ledger.jsonl",
            JSON.stringify({ timestamp: nowISO(), action: "propose_failed", workId: currentWorkId, reason: proposeRes.message || "propose failed", report: reportPath }) + "\n",
          );
          const bundlePath = `${workDir}/BUNDLE.json`;
          try {
            await unlink(resolveStatePath(bundlePath));
          } catch {
            // ignore
          }
          continue;
        }
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "bundle_created", workId: currentWorkId }) + "\n");
      }

      if (stageIndex(currentStage) < stageIndex("PROPOSED")) {
        await updateWorkStatus({ workId: currentWorkId, stage: "PROPOSED", blocked: false });
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "watchdog_advanced", workId: currentWorkId, from_stage: currentStage || null, to_stage: "PROPOSED" }) + "\n",
        );
        currentStage = "PROPOSED";
        advanced.push({ work_id: currentWorkId, result: "advanced", stage: "PROPOSED" });
        continue;
      }
      if (targetIndex <= stageIndex("PROPOSED")) {
        advanced.push({ work_id: currentWorkId, result: "stopped_at", stage: "PROPOSED" });
        continue;
      }

      // Ensure QA exists (in case the work was created before QA stage existed or was partially executed).
      {
        const selectedRepos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.map((x) => String(x)).filter(Boolean) : [];
        const missingQa = [];
        for (const repoId of selectedRepos) {
          const qaPath = `${workDir}/qa/qa-plan.${repoId}.json`;
          const qaText = await readTextIfExists(qaPath);
          if (!qaText) missingQa.push(repoId);
        }
        if (missingQa.length) {
          const qaRes = await orchestrator.qa({ workId: currentWorkId, teams: null, limit: null });
          if (!qaRes.ok) {
            const report = await writeWatchdogFailureReport({
              workId: currentWorkId,
              title: "QA planning failed",
              bodyLines: [String(qaRes.message || "qa failed")],
            });
            await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_qa_failed", artifacts: { watchdog_report: report } });
            await writeGlobalStatusFromPortfolio();
            failed.push({ work_id: currentWorkId, reason: "qa_failed", report });
            await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "qa_failed", report }) + "\n");
            continue;
          }
          await updateWorkStatus({ workId: currentWorkId, stage: "QA_PLANNED", blocked: false, artifacts: { qa_dir: `${workDir}/qa/` }, note: `qa_missing_repos=${missingQa.length}` });
          await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_advanced", workId: currentWorkId, from_stage: currentStage || null, to_stage: "QA_PLANNED" }) + "\n");
          await writeGlobalStatusFromPortfolio();
          currentStage = "QA_PLANNED";
          advanced.push({ work_id: currentWorkId, result: "advanced", stage: "QA_PLANNED" });
          continue;
        }
      }

      const bundleRes = await readBundleIfExists(currentWorkId);
      if (!bundleRes.ok) {
        const report = await writeWatchdogFailureReport({
          workId: currentWorkId,
          title: "Bundle missing after propose/qa",
          bodyLines: [`Missing ${workDir}/BUNDLE.json.`, "Run: --propose --with-patch-plans (or --qa) to produce a bundle."],
        });
        await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_bundle_missing", artifacts: { watchdog_report: report } });
        await writeGlobalStatusFromPortfolio();
        failed.push({ work_id: currentWorkId, reason: "bundle_missing", report });
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "bundle_missing", report }) + "\n");
        continue;
      }

      if (stageIndex(currentStage) < stageIndex("BUNDLED")) {
        await updateWorkStatus({ workId: currentWorkId, stage: "BUNDLED", blocked: false });
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({ timestamp: nowISO(), action: "watchdog_advanced", workId: currentWorkId, from_stage: currentStage || null, to_stage: "BUNDLED" }) + "\n",
        );
        currentStage = "BUNDLED";
        advanced.push({ work_id: currentWorkId, result: "advanced", stage: "BUNDLED" });
        continue;
      }
      if (targetIndex <= stageIndex("BUNDLED")) {
        advanced.push({ work_id: currentWorkId, result: "stopped_at", stage: "BUNDLED" });
        continue;
      }

      // Apply approval: request + (optional) auto-approve based on deterministic checks.
      {
        const { requestApplyApproval } = await import("./gates/apply-approval.js");
        const approvalRes = await requestApplyApproval({ workId: currentWorkId, dryRun });
        if (!approvalRes.ok) {
          const report = await writeWatchdogFailureReport({
            workId: currentWorkId,
            title: "Apply approval failed",
            bodyLines: [String(approvalRes.message || "apply-approval failed")],
          });
          await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_apply_approval_failed", artifacts: { watchdog_report: report } });
          await writeGlobalStatusFromPortfolio();
          failed.push({ work_id: currentWorkId, reason: "apply_approval_failed", report });
          await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "apply_approval_failed", report }) + "\n");
          continue;
        }
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({
            timestamp: nowISO(),
            action: "watchdog_advanced",
            workId: currentWorkId,
            from_stage: currentStage || null,
            to_stage: approvalRes.status === "approved" ? "APPLY_APPROVAL_APPROVED" : "APPLY_APPROVAL_PENDING",
          }) + "\n",
        );
        currentStage = approvalRes.status === "approved" ? "APPLY_APPROVAL_APPROVED" : "APPLY_APPROVAL_PENDING";
        advanced.push({ work_id: currentWorkId, apply_approval_status: approvalRes.status, apply_approval_json: approvalRes.apply_approval_json || null });
        continue;
      }

      if (targetIndex <= stageIndex("APPLY_APPROVAL_PENDING")) continue;

      if (normalizeStageForOrder(currentStage) !== "APPLY_APPROVAL_APPROVED") {
        skipped.push({ work_id: currentWorkId, reason: "apply_approval_not_approved" });
        continue;
      }

      if (targetIndex >= stageIndex("APPLIED")) {
        const applyRes = await orchestrator.applyPatchPlans({ workId: currentWorkId });
        if (!applyRes.ok) {
          const report = await writeWatchdogFailureReport({
            workId: currentWorkId,
            title: "Apply/PR creation failed",
            bodyLines: [String(applyRes.message || "apply failed")],
          });
          await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_apply_failed", artifacts: { watchdog_report: report } });
          await writeGlobalStatusFromPortfolio();
          failed.push({ work_id: currentWorkId, reason: "apply_failed", report });
          await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "apply_failed", report }) + "\n");
          continue;
        }
        currentStage = "CI_PENDING";
        advanced.push({ work_id: currentWorkId, result: "applied" });
        continue;
      }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const report = await writeWatchdogFailureReport({ workId: currentWorkId, title: "Unhandled error", bodyLines: [msg] });
        await updateWorkStatus({ workId: currentWorkId, stage: "FAILED", blocked: true, blockingReason: "watchdog_unhandled", artifacts: { watchdog_report: report } });
        await writeGlobalStatusFromPortfolio();
        failed.push({ work_id: currentWorkId, reason: "unhandled", report });
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "watchdog_failed", workId: currentWorkId, reason: "unhandled", report }) + "\n");
      } finally {
        await releaseLock(`${workDir}/.lock`);
      }
    }

    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({
        timestamp: nowISO(),
        action: "watchdog_finished",
        selected: selected.length,
        advanced: advanced.length,
        failed: failed.length,
        schedule_path: sched.path,
      }) + "\n",
    );

    return { ok: failed.length === 0, schedule_path: sched.path, selected: selected.map((x) => x.work_id), advanced, failed, skipped };
  } finally {
    await releaseLock("ai/.watchdog.lock");
  }
}
