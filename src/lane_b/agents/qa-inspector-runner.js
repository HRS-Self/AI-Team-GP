import { existsSync, readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";

import { agentsForTeam } from "./agent-registry.js";
import { ensureDir, readTextIfExists, writeText, appendFile } from "../../utils/fs.js";
import { jsonStableStringify } from "../../utils/json.js";
import { validateQaPlan } from "../../validators/qa-plan-validator.js";
import { nowTs } from "../../utils/id.js";
import { loadRepoRegistry } from "../../utils/repo-registry.js";
import { resolveStatePath } from "../../project/state-paths.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";
import { ensureWorkSsotBundle, renderSsotExcerptsForLlm } from "../../ssot/work-ssot-bundle.js";
import { normalizeLlmContentToText } from "../../llm/content.js";
import { maybeAugmentLlmMessagesWithSkills } from "../../llm/prompt-augment.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

async function listProposalFilesForTeam({ workId, teamId }) {
  const dir = `ai/lane_b/work/${workId}/proposals`;
  try {
    const { readdir } = await import("node:fs/promises");
    const entries = await readdir(resolveStatePath(dir), { withFileTypes: true });
    return entries
      .filter((e) => e.isFile() && e.name.endsWith(".json") && e.name.startsWith(`${teamId}__`))
      .map((e) => `${dir}/${e.name}`)
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

function failReasonToReportLines({ title, workId, repoId, message, errors }) {
  const lines = [];
  lines.push(`# QA failed: ${workId}${repoId ? ` / ${repoId}` : ""}`);
  lines.push("");
  lines.push(`Timestamp: ${nowTs()}`);
  lines.push("");
  lines.push(`## ${title}`);
  lines.push("");
  lines.push(String(message || "(no message)"));
  if (Array.isArray(errors) && errors.length) {
    lines.push("");
    lines.push("## Errors");
    lines.push("");
    for (const e of errors) lines.push(`- ${String(e)}`);
  }
  lines.push("");
  return lines;
}

async function writeQaFailureReport({ workId, repoId, title, message, errors }) {
  const dir = `ai/lane_b/work/${workId}/failure-reports`;
  await ensureDir(dir);
  const path = `${dir}/qa.md`;
  await writeText(path, failReasonToReportLines({ title, workId, repoId, message, errors }).join("\n"));
  return path;
}

function llmProfileResolverOrThrow({ profilesLoaded, profileKey }) {
  const key = String(profileKey || "").trim();
  if (!key) throw new Error("QA inspector agent missing llm_profile.");
  const p = profilesLoaded && isPlainObject(profilesLoaded.profiles) ? profilesLoaded.profiles : null;
  if (!p || !Object.prototype.hasOwnProperty.call(p, key)) throw new Error(`Unknown llm_profile '${key}'. Add it to config/LLM_PROFILES.json.`);
  const prof = p[key];
  const provider = String(prof?.provider || "").trim();
  const model = String(prof?.model || "").trim();
  if (!provider) throw new Error(`llm_profile '${key}' missing provider.`);
  if (!model) throw new Error(`llm_profile '${key}' missing model.`);
  return { profile_key: key, profile: { ...prof, provider, model } };
}

export async function runQaInspector({ repoRoot, workId, teamsCsv = null, limit = null } = {}) {
  const workDir = `ai/lane_b/work/${workId}`;
  const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
  const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
  if (!intakeMd) return { ok: false, message: `Cannot QA: missing ${workDir}/INTAKE.md.` };
  if (!routingText) return { ok: false, message: `Cannot QA: missing ${workDir}/ROUTING.json.` };

  let routing;
  try {
    routing = JSON.parse(routingText);
  } catch {
    return { ok: false, message: `Cannot QA: invalid JSON in ${workDir}/ROUTING.json.` };
  }

  const selectedRepos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.map((x) => String(x)).filter(Boolean) : [];
  if (!selectedRepos.length) return { ok: false, message: "Cannot QA: ROUTING.json selected_repos empty." };

  const selectedTeams = Array.isArray(routing?.selected_teams) ? routing.selected_teams.map((x) => String(x)).filter(Boolean) : [];
  const requestedTeams =
    typeof teamsCsv === "string" && teamsCsv.trim()
      ? teamsCsv
          .split(",")
          .map((t) => t.trim())
          .filter(Boolean)
      : null;
  const teamsFilter = (requestedTeams || selectedTeams).slice().filter(Boolean);

  const reposLoaded = await loadRepoRegistry();
  if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message };
  const registry = reposLoaded.registry;
  const byRepoId = new Map((registry.repos || []).map((r) => [String(r.repo_id || "").trim(), r]));

  // Preconditions: proposal + patch plan + SSOT bundle must exist per repo/team.
  const qaAgent = agentsForTeam("QA", { role: "qa_inspector", implementation: "llm" })[0] || null;
  if (!qaAgent) {
    const report = await writeQaFailureReport({
      workId,
      repoId: null,
      title: "Missing QA inspector agent",
      message: "No qa_inspector LLM agent registered for team QA. Run --agents-generate (ensure TEAMS.json includes QA).",
      errors: [],
    });
    await updateWorkStatus({ workId, stage: "FAILED", blocked: true, blockingReason: "QA_AGENT_MISSING", artifacts: { qa_failure_report: report } });
    await writeGlobalStatusFromPortfolio();
    return { ok: false, message: "Cannot QA: missing qa_inspector agent (team QA).", report };
  }

  const { createLlmClient } = await import("../../llm/client.js");
  const { loadLlmProfiles } = await import("../../llm/llm-profiles.js");
  const profilesLoaded = await loadLlmProfiles();
  if (!profilesLoaded.ok) {
    const report = await writeQaFailureReport({ workId, repoId: null, title: "LLM profiles invalid", message: profilesLoaded.message, errors: profilesLoaded.errors || [] });
    await updateWorkStatus({ workId, stage: "FAILED", blocked: true, blockingReason: "QA_LLM_PROFILES_INVALID", artifacts: { qa_failure_report: report } });
    await writeGlobalStatusFromPortfolio();
    return { ok: false, message: profilesLoaded.message, report, errors: profilesLoaded.errors || [] };
  }

  let llm;
  try {
    const resolved = llmProfileResolverOrThrow({ profilesLoaded, profileKey: qaAgent.llm_profile });
    const client = createLlmClient({ ...resolved.profile });
    if (!client.ok) throw new Error(client.message || "LLM client unavailable.");
    llm = client.llm;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const report = await writeQaFailureReport({ workId, repoId: null, title: "LLM initialization failed", message: msg, errors: [] });
    await updateWorkStatus({ workId, stage: "FAILED", blocked: true, blockingReason: "QA_LLM_INIT_FAILED", artifacts: { qa_failure_report: report } });
    await writeGlobalStatusFromPortfolio();
    return { ok: false, message: msg, report };
  }

  if (!existsSync(resolve(repoRoot, "src/llm/prompts/qa_inspector.system.txt"))) {
    throw new Error("Missing QA inspector prompt. Expected src/llm/prompts/qa_inspector.system.txt.");
  }
  const systemPrompt = readFileSync(resolve(repoRoot, "src/llm/prompts/qa_inspector.system.txt"), "utf8");
  await ensureDir(`${workDir}/qa`);

  const created = [];
  const errors = [];
  let totalTests = 0;
  let totalGaps = 0;

  const targetBranch = typeof routing?.target_branch?.name === "string" && routing.target_branch.name.trim() ? routing.target_branch.name.trim() : null;

  const effectiveRepos = selectedRepos.slice().sort((a, b) => a.localeCompare(b));
  const boundedRepos = Number.isFinite(Number(limit)) && Number(limit) > 0 ? effectiveRepos.slice(0, Number(limit)) : effectiveRepos;

  for (const repoId of boundedRepos) {
    const repo = byRepoId.get(repoId);
    if (!repo) {
      errors.push({ repo_id: repoId, message: `Repo not found in registry: ${repoId}` });
      continue;
    }
    const teamId = String(repo.team_id || "").trim();
    if (teamsFilter.length && !teamsFilter.includes(teamId)) continue;

    const proposalPaths = await listProposalFilesForTeam({ workId, teamId });
    if (!proposalPaths.length) {
      errors.push({ repo_id: repoId, message: `Missing proposal for team ${teamId} (expected ${workDir}/proposals/${teamId}__*.json).` });
      continue;
    }
    const proposalPath = proposalPaths[0];
    const proposalText = await readTextIfExists(proposalPath);
    if (!proposalText) {
      errors.push({ repo_id: repoId, message: `Missing proposal JSON: ${proposalPath}` });
      continue;
    }
    let proposalJson;
    try {
      proposalJson = JSON.parse(proposalText);
    } catch {
      errors.push({ repo_id: repoId, message: `Invalid JSON in proposal: ${proposalPath}` });
      continue;
    }
    if (proposalJson?.status !== "SUCCESS") {
      errors.push({ repo_id: repoId, message: `Proposal is not SUCCESS for team ${teamId} (${proposalPath}).` });
      continue;
    }

    const patchPlanPath = `${workDir}/patch-plans/${repoId}.json`;
    const patchPlanText = await readTextIfExists(patchPlanPath);
    if (!patchPlanText) {
      errors.push({ repo_id: repoId, message: `Missing patch plan JSON: ${patchPlanPath}` });
      continue;
    }
    let patchPlanJson;
    try {
      patchPlanJson = JSON.parse(patchPlanText);
    } catch {
      errors.push({ repo_id: repoId, message: `Invalid JSON in patch plan: ${patchPlanPath}` });
      continue;
    }

    const ssotPath = `${workDir}/ssot/SSOT_BUNDLE.team-${teamId}.json`;
    const ssotText = await readTextIfExists(ssotPath);
    if (!ssotText) {
      errors.push({ repo_id: repoId, message: `Missing SSOT bundle for team ${teamId} (expected ${ssotPath}).` });
      continue;
    }
    let ssotJson;
    try {
      ssotJson = JSON.parse(ssotText);
    } catch {
      errors.push({ repo_id: repoId, message: `Invalid SSOT bundle JSON: ${ssotPath}` });
      continue;
    }

    const workSsotRes = await ensureWorkSsotBundle({ workId, teamId, workDir, teamBundlePath: ssotPath, allowOverwriteTeamMismatch: true });
    if (!workSsotRes.ok) {
      errors.push({ repo_id: repoId, message: `SSOT_BUNDLE.json creation failed: ${workSsotRes.message}` });
      continue;
    }
    const workSsotText = await readTextIfExists(workSsotRes.outPath);
    if (!workSsotText) {
      errors.push({ repo_id: repoId, message: `Missing SSOT_BUNDLE.json after creation: ${workSsotRes.outPath}` });
      continue;
    }
    const ssotExcerpts = renderSsotExcerptsForLlm({ teamBundleText: ssotText });

    const userPrompt = [
      `Work: ${workId}`,
      `Repo: ${repoId}`,
      `Team: ${teamId}`,
      `Target branch: ${targetBranch || "(unknown)"}`,
      "",
      "=== SSOT_BUNDLE.json (authoritative; reference-only) ===",
      workSsotText.trim(),
      "",
      "=== SSOT SECTION EXCERPTS (clipped; cite using SSOT:<section_id>@<sha256>) ===",
      ssotExcerpts.trim(),
      "",
      "=== ROUTING.json ===",
      routingText.trim(),
      "",
      "=== INTAKE.md (raw) ===",
      String(intakeMd || "").trim(),
      "",
      "=== PROPOSAL.json (authoritative) ===",
      proposalText.trim(),
      "",
      "=== PATCH_PLAN.json (authoritative) ===",
      patchPlanText.trim(),
      "",
      "Return JSON only.",
    ].join("\n");

    let content;
    try {
      const augmented = await maybeAugmentLlmMessagesWithSkills({
        baseMessages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        projectRoot: process.env.AI_PROJECT_ROOT || null,
        input: {
          scope: `repo:${repoId}`,
          base_system: String(systemPrompt || ""),
          base_prompt: String(userPrompt || ""),
          context: { role: "lane_b.qa_inspector", workId, repo_id: repoId, team_id: teamId },
          constraints: { output: "json_only" },
          knowledge_snippets: [],
        },
      });
      const res = await llm.invoke(augmented.messages);
      const norm = normalizeLlmContentToText(res?.content);
      content = norm.text;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push({ repo_id: repoId, message: `LLM invocation failed: ${msg}` });
      continue;
    }

    let qaJson;
    try {
      qaJson = JSON.parse(String(content || ""));
    } catch {
      errors.push({ repo_id: repoId, message: "QA LLM output is not valid JSON." });
      continue;
    }

    // Enforce canonical fields from orchestrator context (deterministic).
    qaJson.version = 1;
    qaJson.work_id = workId;
    qaJson.repo_id = repoId;
    qaJson.team_id = teamId;
    qaJson.target_branch = targetBranch || String(qaJson.target_branch || "").trim() || "unknown";
    qaJson.created_at = typeof qaJson.created_at === "string" && qaJson.created_at.trim() ? qaJson.created_at.trim() : nowTs();
    qaJson.ssot = {
      bundle_path: workSsotRes.outPath,
      bundle_hash: sha256Hex(workSsotText),
      snapshot_sha256: typeof ssotJson?.snapshot?.sha256 === "string" ? ssotJson.snapshot.sha256 : sha256Hex(JSON.stringify(ssotJson?.snapshot || {})),
    };
    qaJson.derived_from = {
      proposal_path: proposalPath,
      proposal_sha256: sha256Hex(proposalText),
      patch_plan_path: patchPlanPath,
      patch_plan_sha256: sha256Hex(patchPlanText),
      timestamp: nowTs(),
    };

    const v = validateQaPlan(qaJson, { expectedWorkId: workId, expectedRepoId: repoId });
    if (!v.ok) {
      errors.push({ repo_id: repoId, message: "qa-plan validation failed", errors: v.errors });
      continue;
    }

    const outJsonPath = `${workDir}/qa/qa-plan.${repoId}.json`;
    const outMdPath = `${workDir}/qa/qa-plan.${repoId}.md`;
    await writeText(outJsonPath, jsonStableStringify(qaJson) + "\n");

    const tests = Array.isArray(qaJson.tests) ? qaJson.tests : [];
    const gaps = Array.isArray(qaJson.gaps) ? qaJson.gaps : [];
    totalTests += tests.length;
    totalGaps += gaps.length;

    const mdLines = [];
    mdLines.push(`# QA Plan: ${repoId}`);
    mdLines.push("");
    mdLines.push(`Work: ${workId}`);
    mdLines.push(`Team: ${teamId}`);
    mdLines.push(`Target branch: ${qaJson.target_branch}`);
    mdLines.push("");
    mdLines.push("## Summary");
    mdLines.push("");
    mdLines.push(`- tests: ${tests.length}`);
    mdLines.push(`- gaps: ${gaps.length}`);
    mdLines.push("");
    mdLines.push("## QA Plan JSON (authoritative)");
    mdLines.push("");
    mdLines.push("```json");
    mdLines.push(jsonStableStringify(qaJson));
    mdLines.push("```");
    mdLines.push("");
    await writeText(outMdPath, mdLines.join("\n"));

    const sha = sha256Hex(jsonStableStringify(qaJson) + "\n");
    created.push({ repo_id: repoId, ok: true, qa_plan_json_path: outJsonPath, qa_plan_md_path: outMdPath, sha256: sha, tests: tests.length, gaps: gaps.length });
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowTs(), action: "qa_planned", workId, repo_id: repoId, team_id: teamId, agent_id: qaAgent.agent_id, qa_plan_json: outJsonPath, qa_plan_md: outMdPath, tests: tests.length, gaps: gaps.length, sha256: sha }) + "\n");
  }

  if (errors.length) {
    const report = await writeQaFailureReport({
      workId,
      repoId: null,
      title: "One or more repos failed QA planning",
      message: "QA_PLANNED did not complete for all repos.",
      errors: errors.flatMap((e) => {
        if (e && typeof e === "object" && Array.isArray(e.errors)) return [`${e.repo_id}: ${e.message}`, ...e.errors.map((x) => `  - ${x}`)];
        return [`${e.repo_id || "(unknown)"}: ${e.message || "error"}`];
      }),
    });
    await updateWorkStatus({ workId, stage: "FAILED", blocked: true, blockingReason: "QA_FAILED", artifacts: { qa_failure_report: report } });
    await writeGlobalStatusFromPortfolio();
    return { ok: false, message: "QA planning failed for one or more repos.", created, errors, report };
  }

  await updateWorkStatus({
    workId,
    stage: "QA_PLANNED",
    blocked: false,
    artifacts: {
      qa_dir: `${workDir}/qa/`,
      qa_plan_jsons: created.map((c) => c.qa_plan_json_path).sort((a, b) => a.localeCompare(b)),
    },
    repos: Object.fromEntries(created.map((c) => [c.repo_id, { qa_planned: true, tests: c.tests, gaps: c.gaps }])),
    note: `qa planned repos=${created.length} tests=${totalTests} gaps=${totalGaps}`,
  });
  await writeGlobalStatusFromPortfolio();

  return { ok: true, workId, created };
}

// Back-compat alias: older callers still import runQaStrategist.
export async function runQaStrategist(args = {}) {
  return await runQaInspector(args);
}
