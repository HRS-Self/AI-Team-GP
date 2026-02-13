import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { readdir } from "node:fs/promises";
import { appendFile, ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { nowTs } from "../../utils/id.js";
import { agentsForTeam } from "./agent-registry.js";
import { getRepoPathsForWork } from "../../utils/repo-registry.js";
import { hasRg, scanWithRgInRoots, scanFallbackInRoots, discoverPackageScriptsInRoots } from "../../utils/repo-scan.js";
import { resolveStatePath } from "../../project/state-paths.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";
import { writeWorkPlan, readBundleIfExists } from "../../utils/plan-writer.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function parseTeamsCsv(csv) {
  if (!csv) return null;
  const out = [];
  for (const part of String(csv).split(",")) {
    const t = part.trim();
    if (!t) continue;
    if (!out.includes(t)) out.push(t);
  }
  return out.length ? out : null;
}

function boolFromEnv(name, defaultValue = false) {
  const raw = process.env[name];
  if (typeof raw !== "string") return defaultValue;
  const v = raw.trim().toLowerCase();
  return v === "true" || v === "1" || v === "yes";
}

function tokenizeTerms(text) {
  const allowShort = new Set(["rn", "ui", "api", "jwt", "oidc", "oauth"]);
  const stop = new Set([
    "the",
    "and",
    "or",
    "to",
    "for",
    "of",
    "in",
    "on",
    "a",
    "an",
    "is",
    "are",
    "be",
    "with",
    "by",
    "from",
    "this",
    "that",
    "it",
    "we",
    "you",
    "work",
    "item",
    "team",
    "scope",
    "tasks",
    "task",
    "stub",
    "notes",
    "none",
    "open",
    "questions",
    "question",
    "confirm",
    "dependencies",
    "acceptance",
    "criteria",
    "review",
    "draft",
    "identify",
    "scoped",
    "responsibility",
    "boundaries",
    "captured",
    "declared",
    "parallel",
    "run",
    "runs",
    "update",
    "change",
    "add",
    "create",
  ]);

  const cleaned = String(text || "")
    .toLowerCase()
    .replace(/[`"'.,:;()[\]{}<>!?/\\|+=_*~-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const tokens = cleaned.split(" ").filter(Boolean);
  const counts = new Map();
  for (const t of tokens) {
    if (stop.has(t)) continue;
    if (t.length < 4 && !allowShort.has(t)) continue;
    counts.set(t, (counts.get(t) || 0) + 1);
  }
  return counts;
}

function deriveSearchTerms({ teamId, intakeMd, teamTaskMd, proposalMds }) {
  const counts = tokenizeTerms(`${intakeMd}\n${teamTaskMd}\n${proposalMds.join("\n")}`);

  const teamSeeds = {
    Portal: ["portal", "dashboard", "copy", "text", "next"],
    Mobile: ["mobile", "rn", "react", "android", "screen"],
    IdentitySecurity: ["auth", "token", "jwt", "oidc", "oauth"],
    BackendCore: ["endpoint", "controller", "service", "health", "api"],
    DevOps: ["ci", "github", "actions", "deploy", "docker"],
    QA: ["test", "e2e", "integration", "unit", "coverage"],
  };

  const base = Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([t]) => t)
    .slice(0, 12);

  const seeds = (teamSeeds[teamId] || []).slice();
  const out = [];
  for (const t of [...seeds, ...base]) {
    const v = String(t).trim().toLowerCase();
    if (!v) continue;
    if (!out.includes(v)) out.push(v);
    if (out.length >= 14) break;
  }
  return out;
}

function scanRepoWithRg({ repoRoots, terms }) {
  return scanWithRgInRoots({ repoRoots, terms });
}

async function scanRepoFallback({ repoRoots, terms }) {
  return await scanFallbackInRoots({ repoRoots, terms });
}

async function discoverPackageScripts({ repoRoots }) {
  return await discoverPackageScriptsInRoots({ repoRoots });
}

function pickValidationCommands({ scripts }) {
  const preferred = ["lint", "test", "unit", "e2e", "build", "typecheck"];
  const out = [];
  for (const name of preferred) {
    const hit = scripts.find((s) => s.script === name);
    if (hit && !out.includes(hit.command)) out.push(hit.command);
  }
  if (!out.length) {
    for (const s of scripts.slice(0, 5)) {
      if (!out.includes(s.command)) out.push(s.command);
    }
  }
  return out;
}

function validatePatchPlanJson(obj) {
  const errors = [];
  const reqStr = (k) => {
    if (typeof obj?.[k] !== "string" || !obj[k].trim()) errors.push(`Missing/invalid ${k} (expected non-empty string).`);
  };
  const reqStrArr = (k) => {
    if (!Array.isArray(obj?.[k]) || !obj[k].every((v) => typeof v === "string")) errors.push(`Missing/invalid ${k} (expected string[]).`);
  };
  reqStr("summary");
  reqStrArr("assumptions");
  if (!Array.isArray(obj?.files_to_change)) errors.push("Missing/invalid files_to_change (expected array).");
  else {
    for (const f of obj.files_to_change) {
      if (typeof f?.path !== "string" || !f.path.trim()) errors.push("files_to_change[].path must be a non-empty string.");
      if (typeof f?.why !== "string" || !f.why.trim()) errors.push("files_to_change[].why must be a non-empty string.");
      if (typeof f?.evidence !== "undefined" && (!Array.isArray(f.evidence) || !f.evidence.every((x) => typeof x === "string")))
        errors.push("files_to_change[].evidence must be string[] if provided.");
    }
  }
  reqStrArr("step_by_step_patch_plan");
  reqStrArr("tests_and_validation_commands");
  reqStrArr("risk_notes_and_rollback_plan");
  reqStrArr("open_questions_or_blockers");
  return { ok: errors.length === 0, errors };
}

async function generateJsonWithRetries({ llm, systemPrompt, userPrompt }) {
  const attempts = [];
  if (!llm) {
    return { ok: false, attempts: [{ attempt: 0, raw: "LLM unavailable.", error: "LLM unavailable." }] };
  }
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    let response;
    try {
      response = await llm.invoke([
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ]);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      attempts.push({ attempt, raw: "", error: `LLM invocation failed: ${msg}` });
      continue;
    }
    const raw = typeof response?.content === "string" ? response.content : String(response?.content ?? "");
    let parsed = null;
    let parseError = null;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      parseError = err instanceof Error ? err.message : String(err);
    }
    if (parsed) {
      const v = validatePatchPlanJson(parsed);
      if (v.ok) return { ok: true, json: parsed, raw };
      attempts.push({ attempt, raw, error: `Schema validation failed: ${v.errors.join(" ")}` });
    } else {
      attempts.push({ attempt, raw, error: `JSON parse failed: ${parseError}` });
    }
  }
  return { ok: false, attempts };
}

function fallbackPatchPlan({ teamId, scan, searchTerms, validationCommands, llmError }) {
  const hasHits = Array.isArray(scan?.hits) && scan.hits.length > 0;
  const topPaths = hasHits ? scan.hits.map((h) => h.path).filter(Boolean).slice(0, 5) : [];

  const files =
    topPaths.length >= 3
      ? topPaths.slice(0, 5).map((p) => ({
          path: p,
          why: "Matched derived search terms during repo scan.",
          evidence: (scan.hits.find((h) => h.path === p)?.lines || []).slice(0, 2),
        }))
      : [
          {
            path: "unknown (no matches found)",
            why: `Repo scan found no matches for terms: ${searchTerms.join(", ") || "(none)"}.`,
            evidence: [`Matches (captured): ${Number.isFinite(scan?.total_matches) ? scan.total_matches : 0}`],
          },
        ];

  const tests =
    validationCommands && validationCommands.length
      ? validationCommands.slice()
      : ["scripts not discovered; run CI defaults", "npm test (if configured)", "npm run build (if configured)"];

  const blockers = [];
  if (!hasHits) blockers.push("Repo scan produced no concrete file candidates; need the exact repo paths/components for this team scope.");
  if (llmError) blockers.push(`Planner LLM unavailable; generated deterministic fallback plan. Error: ${llmError}`);

  return {
    summary: `Patch plan (fallback) for ${teamId}: define concrete file targets and apply scoped changes per approved intake.`,
    assumptions: ["Some required details may be unknown due to missing repo scan hits; see blockers."],
    files_to_change: files,
    step_by_step_patch_plan: [
      "Confirm scope using ai/lane_b/work/<workId>/INTAKE.md and team task stub",
      "Identify exact file(s) to change using repo search + manual inspection",
      "Prepare minimal edits scoped to the team responsibility",
      "Run validation commands and capture results",
      "Document decisions, risks, and outcomes back into ai/lane_b/work/<workId>/ artifacts",
    ],
    tests_and_validation_commands: tests,
    risk_notes_and_rollback_plan: [
      "Keep changes minimal and reversible",
      "Rollback plan: revert the applied change set (git revert) and restore prior copy/behavior",
    ],
    open_questions_or_blockers: blockers,
  };
}

function renderPatchPlanMarkdown({ workId, teamId, agentId, evidence, approval, reviewPath, patchPlan, inputHash }) {
  const bullets = (arr) => {
    if (!Array.isArray(arr) || arr.length === 0) return ["- (none)"];
    return arr.map((x) => `- ${String(x)}`);
  };

  const filesLines = [];
  const files = Array.isArray(patchPlan?.files_to_change) ? patchPlan.files_to_change : [];
  if (!files.length) {
    filesLines.push("- (none)");
  } else {
    for (const f of files) {
      const p = String(f.path || "").trim();
      const why = String(f.why || "").trim();
      const ev = Array.isArray(f.evidence) ? f.evidence : [];
      filesLines.push(`- \`${p}\` — ${why || "(no rationale)"}`);
      for (const ln of ev.slice(0, 2)) filesLines.push(`  - ${String(ln)}`);
    }
  }

  const scan = evidence?.scan || { total_matches: 0, hits: [] };
  const searchTerms = Array.isArray(evidence?.search_terms) ? evidence.search_terms : [];
  const repoRoots = Array.isArray(evidence?.repo_roots) ? evidence.repo_roots : [];
  const repoRegistryNote = typeof evidence?.repo_registry_note === "string" ? evidence.repo_registry_note.trim() : "";
  const scanHits = Array.isArray(scan?.hits) ? scan.hits : [];

  const evidenceLines = [];
  evidenceLines.push(`- Repo registry: ${repoRegistryNote || "Repo registry not configured; scanning current repo only."}`);
  if (repoRoots.length) {
    evidenceLines.push("- Repo roots scanned:");
    for (const r of repoRoots) {
      const id = String(r?.repo_id || "").trim() || "unknown";
      const p = String(r?.abs_path || "").trim() || "(unknown path)";
      const exists = typeof r?.exists === "boolean" ? (r.exists ? "exists" : "missing") : "unknown";
      evidenceLines.push(`  - \`${id}\`: \`${p}\` (${exists})`);
    }
  } else {
    evidenceLines.push("- Repo roots scanned: (none)");
  }
  evidenceLines.push(`- Search terms: ${searchTerms.length ? searchTerms.map((t) => `\`${t}\``).join(", ") : "(none)"}`);
  evidenceLines.push(`- Matches (captured): ${Number.isFinite(scan?.total_matches) ? scan.total_matches : 0}`);
  if (!scanHits.length) {
    evidenceLines.push("- File hits: No matches found.");
  } else {
    evidenceLines.push("- File hits:");
    for (const h of scanHits) {
      evidenceLines.push(`  - \`${h.path}\``);
      for (const ln of (h.lines || []).slice(0, 2)) evidenceLines.push(`    - ${String(ln)}`);
    }
  }

  const reviewLine = reviewPath ? `- Review: \`${reviewPath}\`` : "- Review: (none)";
  const inputHashLine = inputHash ? `- Input hash: ${inputHash}` : null;

  return [
    `# Patch plan: ${teamId} / ${agentId}`,
    "",
    `Work item: ${workId}`,
    "",
    "## Summary",
    "",
    String(patchPlan?.summary || "").trim() || "(unavailable)",
    "",
    "## Assumptions",
    "",
    ...bullets(patchPlan?.assumptions),
    "",
    "## Files to change",
    "",
    "Evidence:",
    ...evidenceLines,
    "",
    "Ranked list:",
    ...filesLines,
    "",
    "## Step-by-step patch plan",
    "",
    ...bullets(patchPlan?.step_by_step_patch_plan),
    "",
    "## Tests / validation commands",
    "",
    ...bullets(patchPlan?.tests_and_validation_commands),
    "",
    "## Risk notes + rollback plan",
    "",
    ...bullets(patchPlan?.risk_notes_and_rollback_plan),
    "",
    "## Open questions / blockers",
    "",
    ...bullets(patchPlan?.open_questions_or_blockers),
    "",
    "## Approval context",
    "",
    `- Approval status: ${approval?.status || "unknown"}`,
    `- Approved by: ${approval?.approved_by || "unknown"}`,
    `- Approved at: ${approval?.approved_at || "(none)"}`,
    reviewLine,
    ...(inputHashLine ? [inputHashLine] : []),
    "",
  ].join("\n");
}

function extractInputHash(markdown) {
  const lines = String(markdown || "").split("\n");
  for (const line of lines) {
    const m = line.trim().match(/^\-\s*Input hash:\s*([a-f0-9]{64})\s*$/i);
    if (m) return m[1].toLowerCase();
  }
  return null;
}

function computeTeamInputHash({
  workId,
  teamId,
  agentId,
  repoRoots,
  repoRegistryNote,
  approvalJson,
  approvalMd,
  intakeMd,
  intakeTaggedMd,
  routingJson,
  teamTaskMd,
  proposalTexts,
  reviewMd,
  validationCommands,
}) {
  const parts = [
    `workId=${workId}`,
    `teamId=${teamId}`,
    `agentId=${agentId}`,
    `repoRegistryNote=${String(repoRegistryNote || "")}`,
    `repoRoots=${JSON.stringify((repoRoots || []).map((r) => ({ repo_id: r.repo_id, abs_path: r.abs_path, exists: r.exists })))}`,
    `approvalJson=${String(approvalJson || "")}`,
    `approvalMd=${String(approvalMd || "")}`,
    `intakeMd=${String(intakeMd || "")}`,
    `intakeTaggedMd=${String(intakeTaggedMd || "")}`,
    `routingJson=${String(routingJson || "")}`,
    `teamTaskMd=${String(teamTaskMd || "")}`,
    `reviewMd=${String(reviewMd || "")}`,
    `validationCommands=${JSON.stringify(validationCommands || [])}`,
    ...proposalTexts.map((p) => `proposal:${p.path}\n${p.text}`),
  ];
  return sha256Hex(parts.join("\n---\n"));
}

function updateStatusForPatchPlans(statusText, { workId, timestamp, count }) {
  const lines = String(statusText || "").split("\n");
  const out = [];
  let has = false;

  for (const line of lines) {
    if (line.startsWith("- Work item: ")) {
      out.push(`- Work item: ${workId}`);
      continue;
    }
    if (line.startsWith("- Timestamp: ")) {
      out.push(`- Timestamp: ${timestamp}`);
      continue;
    }
    if (line.startsWith("- Summary: ")) {
      out.push(`- Summary: Created ${count} patch plan(s) for ${workId}.`);
      continue;
    }
    if (line.startsWith("- Patch plans created: ")) {
      has = true;
      out.push(`- Patch plans created: ${count}`);
      continue;
    }
    out.push(line);
  }

  if (!has) {
    const idx = out.findIndex((l) => l.startsWith("- Summary: "));
    if (idx !== -1) out.splice(idx + 1, 0, `- Patch plans created: ${count}`);
  }

  return out.join("\n");
}

function updatePlanNextForStep6(planText) {
  const step6 = "- Step 6: Apply patch plan to a branch (guarded) — NOT IMPLEMENTED YET";
  const lines = String(planText || "").split("\n");
  const nextIdx = lines.findIndex((l) => l.trim() === "## Next");
  if (nextIdx === -1) return planText;

  let end = lines.length;
  for (let i = nextIdx + 1; i < lines.length; i += 1) {
    if (lines[i].startsWith("## ") && lines[i].trim() !== "## Next") {
      end = i;
      break;
    }
  }
  const has = lines.slice(nextIdx, end).some((l) => l.trim() === step6);
  if (has) return planText;
  return [...lines.slice(0, end), step6, ...lines.slice(end)].join("\n");
}

async function readApproval(workId) {
  const planPath = `ai/lane_b/work/${workId}/PLAN_APPROVAL.json`;
  const legacyPath = `ai/lane_b/work/${workId}/APPROVAL.json`;
  const text = (await readTextIfExists(planPath)) || (await readTextIfExists(legacyPath));
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { _invalid: true, raw: text };
  }
}

async function readRoutingSelectedTeams(workId) {
  const routingText = await readTextIfExists(`ai/lane_b/work/${workId}/ROUTING.json`);
  if (!routingText) return [];
  try {
    const routing = JSON.parse(routingText);
    return Array.isArray(routing?.selected_teams) ? routing.selected_teams.slice().filter(Boolean) : [];
  } catch {
    return [];
  }
}

async function listTeamProposals({ workId, teamId }) {
  const dir = `ai/lane_b/work/${workId}/proposals`;
  try {
    const entries = await readdir(resolveStatePath(dir), { withFileTypes: true });
    return entries
      .filter((e) => e.isFile() && e.name.endsWith(".md") && e.name.startsWith(`${teamId}__`))
      .map((e) => `${dir}/${e.name}`)
      .sort((a, b) => a.localeCompare(b));
  } catch (err) {
    if (err && err.code === "ENOENT") return [];
    throw err;
  }
}

export async function runPatchPlans({ repoRoot, workId, teamsCsv }) {
  const approvalRequired = boolFromEnv("PLAN_APPROVAL_REQUIRED", boolFromEnv("APPROVAL_REQUIRED", true));
  const approval = await readApproval(workId);
  if (approvalRequired) {
    if (!approval || approval._invalid || approval.status !== "approved") {
      return { ok: false, message: `Cannot proceed: plan-approval required. Run --plan-approve --workId ${workId}.` };
    }
  }
  if (approval && approval._invalid) return { ok: false, message: `Invalid ai/lane_b/work/${workId}/PLAN_APPROVAL.json (or legacy APPROVAL.json).` };

  const approvedTeams =
    approval && Array.isArray(approval?.scope?.teams) && approval.scope.teams.length
      ? approval.scope.teams.slice().filter(Boolean)
      : await readRoutingSelectedTeams(workId);

  const requestedTeams = parseTeamsCsv(teamsCsv);
  const selected = requestedTeams ? approvedTeams.filter((t) => requestedTeams.includes(t)) : approvedTeams.slice();
  const teams = selected.filter(Boolean).sort((a, b) => a.localeCompare(b));
  if (!teams.length) return { ok: false, message: "No teams selected for patch plans (after intersecting with approved teams)." };

  // proposals must exist for all selected teams
  const proposalPathsByTeam = new Map();
  for (const t of teams) {
    const paths = await listTeamProposals({ workId, teamId: t });
    proposalPathsByTeam.set(t, paths);
    if (!paths.length) return { ok: false, message: `Cannot create patch plans: missing proposal for team ${t}.` };
  }

  const workDir = `ai/lane_b/work/${workId}`;
  const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
  const routingJson = await readTextIfExists(`${workDir}/ROUTING.json`);
  const approvalJson = (await readTextIfExists(`${workDir}/PLAN_APPROVAL.json`)) || (await readTextIfExists(`${workDir}/APPROVAL.json`));
  const approvalMd = (await readTextIfExists(`${workDir}/PLAN_APPROVAL.md`)) || (await readTextIfExists(`${workDir}/APPROVAL.md`));
  if (!intakeMd) return { ok: false, message: `Missing ${workDir}/INTAKE.md.` };
  if (!routingJson) return { ok: false, message: `Missing ${workDir}/ROUTING.json.` };
  if (approvalRequired && (!approvalJson || !approvalMd)) return { ok: false, message: `Missing approval artifacts under ${workDir}/.` };

  const intakeTaggedMd = await readTextIfExists(`${workDir}/INTAKE_TAGGED.md`);
  const reviewPath = `${workDir}/reviews/architect-review.md`;
  const reviewMd = await readTextIfExists(reviewPath);

  const repoContext = await getRepoPathsForWork({ workId });
  const repoRegistryConfigured = repoContext.ok && repoContext.configured;
  const repoRegistryNote = repoRegistryConfigured
    ? "Repo registry configured; scanning selected repo(s) for this work item."
    : "Repo registry not configured; scanning current repo only.";
  const rgAvailable = hasRg();

  const systemPrompt = readFileSync(resolve(repoRoot, "src/llm/prompts/patch-plan.system.txt"), "utf8");
  const schemaJson = readFileSync(resolve(repoRoot, "src/llm/schemas/patch-plan.llm-output.schema.json"), "utf8");

  const agentsText = await readTextIfExists("config/AGENTS.json");
  if (!agentsText) return { ok: false, message: "Missing config/AGENTS.json (required). Run: node src/cli.js --agents-generate" };
  try {
    const cfg = JSON.parse(agentsText);
    if (!cfg || cfg.version !== 3 || !Array.isArray(cfg.agents)) {
      return { ok: false, message: "Invalid config/AGENTS.json (expected {version:3, agents:[...]}). Run: node src/cli.js --agents-migrate" };
    }
    const hasLegacyModel = cfg.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"));
    if (hasLegacyModel) return { ok: false, message: "AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate" };
  } catch {
    return { ok: false, message: "Invalid config/AGENTS.json (must be valid JSON)." };
  }

  let createLlmClient = null;
  let llmUnavailableMsg = null;
  try {
    const mod = await import("../../llm/client.js");
    createLlmClient = mod.createLlmClient;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    createLlmClient = null;
    llmUnavailableMsg = `LLM client unavailable (${msg}).`;
  }

  const { loadLlmProfiles, resolveLlmProfileOrError } = await import("../../llm/llm-profiles.js");
  const profilesLoaded = await loadLlmProfiles();
  if (!profilesLoaded.ok) return { ok: false, message: profilesLoaded.message, ...(profilesLoaded.errors ? { errors: profilesLoaded.errors } : {}) };

  const llmCache = new Map();
  function llmForPlannerAgent(agent) {
    if (!createLlmClient) return { llm: null, reason: llmUnavailableMsg || "LLM unavailable." };
    const profileKey = agent && typeof agent.llm_profile === "string" ? agent.llm_profile.trim() : "";
    const resolved = resolveLlmProfileOrError({ profiles: profilesLoaded.profiles, profileKey });
    if (!resolved.ok) return { llm: null, model: null, reason: resolved.message };
    const cacheKey = `profile:${resolved.profile_key}`;
    if (llmCache.has(cacheKey)) return llmCache.get(cacheKey);
    const client = createLlmClient({ ...resolved.profile });
    const v = client && client.ok ? { llm: client.llm, model: client.model, reason: null } : { llm: null, model: null, reason: client?.message || "LLM unavailable." };
    llmCache.set(cacheKey, v);
    return v;
  }

  await ensureDir(`${workDir}/patch-plans`);

  const created = [];
  for (const teamId of teams) {
    const teamRepos = repoRegistryConfigured
      ? (repoContext.repos || []).filter((r) => r.team_id === teamId)
      : [{ repo_id: "ai-team", abs_path: repoRoot, exists: true }];

    const planners = agentsForTeam(teamId, { role: "planner", implementation: "llm" });
    if (!planners.length)
      return { ok: false, message: `No planner agent available for team ${teamId}. Run: node src/cli.js --agents-generate.` };
    const planner = planners[0];
    const agentId = planner.agent_id;
    const llmInfo = llmForPlannerAgent(planner);
    const llm = llmInfo.llm;

    const teamTaskMd = await readTextIfExists(`${workDir}/tasks/${teamId}.md`);
    if (!teamTaskMd) return { ok: false, message: `Missing ${workDir}/tasks/${teamId}.md.` };

    const proposalPaths = proposalPathsByTeam.get(teamId) || [];
    const proposalTexts = [];
    for (const p of proposalPaths.slice(0, 3)) {
      const t = await readTextIfExists(p);
      if (t) proposalTexts.push({ path: p, text: t });
    }

    const scripts = await discoverPackageScripts({ repoRoots: teamRepos });
    const validationCommands = pickValidationCommands({ scripts });
    const validationHint = validationCommands.length
      ? `Discovered npm scripts; use concrete commands:\n${validationCommands.map((c) => `- ${c}`).join("\n")}`
      : "Scripts not discovered; run CI defaults and generic safe commands (documented).";

    const outputPath = `${workDir}/patch-plans/${teamId}__planner-01.md`;
    const existingText = await readTextIfExists(outputPath);
    const existingInputHash = existingText ? extractInputHash(existingText) : null;
    const existingHash = existingText ? sha256Hex(existingText) : null;

    const inputHash = computeTeamInputHash({
      workId,
      teamId,
      agentId,
      repoRoots: teamRepos,
      repoRegistryNote,
      approvalJson,
      approvalMd,
      intakeMd,
      intakeTaggedMd,
      routingJson,
      teamTaskMd,
      proposalTexts,
      reviewMd,
      validationCommands,
    });

    if (existingText && existingInputHash && existingInputHash === inputHash) {
      const ts = nowTs();
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({
          timestamp: ts,
          action: "patch_plan_skipped",
          workId,
          team_id: teamId,
          agent_id: agentId,
          output_path: outputPath,
          hash: existingHash,
          reason: "unchanged",
        }) + "\n",
      );
      created.push({ team_id: teamId, agent_id: agentId, output_path: outputPath, action: "patch_plan_skipped", hash: existingHash });
      continue;
    }

    const searchTerms = deriveSearchTerms({ teamId, intakeMd, teamTaskMd, proposalMds: proposalTexts.map((p) => p.text) });
    const scan = rgAvailable
      ? scanRepoWithRg({ repoRoots: teamRepos, terms: searchTerms })
      : await scanRepoFallback({ repoRoots: teamRepos, terms: searchTerms });

    const evidenceBlock = [
      "=== EVIDENCE (read-only repo scan) ===",
      `Repo registry: ${repoRegistryNote}`,
      ...(teamRepos.length
        ? ["Repo roots:", ...teamRepos.map((r) => `- ${String(r.repo_id)}: ${String(r.abs_path || "(unknown path)")} (${r.exists ? "exists" : "missing"})`)]
        : ["Repo roots: (none)"]),
      `Search terms: ${searchTerms.join(", ") || "(none)"}`,
      `Matches (captured): ${scan.total_matches}`,
      ...scan.hits.flatMap((h) => [`- ${h.path}`, ...(h.lines || []).map((l) => `  - ${l}`)]),
      "",
      "If scan hits are empty, do NOT invent file paths. Use 'unknown (no matches found)' and list search terms + count.",
    ].join("\n");

    const userPrompt = [
      `WorkId: ${workId}`,
      `TeamId: ${teamId}`,
      `AgentId: ${agentId}`,
      "",
      "You must output JSON that matches this schema exactly:",
      schemaJson.trim(),
      "",
      "Inputs (read-only):",
      "",
      "=== PLAN_APPROVAL.json ===",
      String(approvalJson || "(missing)").trim(),
      "",
      "=== PLAN_APPROVAL.md ===",
      String(approvalMd || "(missing)").trim(),
      "",
      "=== INTAKE.md ===",
      intakeMd.trim(),
      "",
      intakeTaggedMd
        ? [
            "=== INTAKE_TAGGED.md (optional) ===",
            intakeTaggedMd.trim(),
            "",
          ].join("\n")
        : "",
      "=== ROUTING.json ===",
      routingJson.trim(),
      "",
      `=== tasks/${teamId}.md ===`,
      teamTaskMd.trim(),
      "",
      `=== proposals for ${teamId} ===`,
      ...(proposalTexts.length
        ? proposalTexts.flatMap((p) => [`--- ${p.path} ---`, p.text.trim(), ""])
        : ["(missing proposals text)"]),
      reviewMd
        ? [
            "=== architect-review.md (optional guidance) ===",
            reviewMd.trim(),
            "",
          ].join("\n")
        : "",
      evidenceBlock,
      "",
      "=== VALIDATION COMMANDS ===",
      validationHint,
      "",
      "Output requirements:",
      "- Files to change MUST be grounded in evidence or be 'unknown (no matches found)'.",
      "- Include rollback guidance.",
      "- No meetings/sync calls; use artifact updates in ai/lane_b/work/<workId>/ instead.",
    ]
      .filter(Boolean)
      .join("\n");

    const generated = await generateJsonWithRetries({ llm, systemPrompt, userPrompt });
    let planJson;
    if (!generated.ok) {
      const last = generated.attempts[generated.attempts.length - 1];
      const llmError = String(last?.error || llmInfo.reason || "unknown");
      planJson = fallbackPatchPlan({
        teamId,
        scan,
        searchTerms,
        validationCommands,
        llmError,
      });
    } else {
      planJson = generated.json;
    }

    // Enforce concrete commands if discovered
    if (validationCommands.length) {
      const existing = Array.isArray(planJson.tests_and_validation_commands) ? planJson.tests_and_validation_commands.slice() : [];
      const normalized = existing.map((x) => String(x).trim());
      const prepend = [];
      for (const cmd of validationCommands) {
        if (normalized.some((x) => x === cmd)) continue;
        prepend.push(cmd);
      }
      planJson.tests_and_validation_commands = [...prepend, ...existing].slice(0, 15);
    } else if (!Array.isArray(planJson.tests_and_validation_commands) || !planJson.tests_and_validation_commands.length) {
      planJson.tests_and_validation_commands = ["scripts not discovered; run CI defaults", "npm test (if configured)", "npm run build (if configured)"];
    }

    const missingRepos = teamRepos.filter((r) => r && r.exists === false);
    if (missingRepos.length) {
      const existing = Array.isArray(planJson.open_questions_or_blockers) ? planJson.open_questions_or_blockers.slice() : [];
      const prepend = missingRepos.map((r) => `Repo missing locally: ${String(r.abs_path || "(unknown path)")} (repo_id=${String(r.repo_id)})`);
      planJson.open_questions_or_blockers = [...prepend, ...existing].slice(0, 30);
    }

    const markdown = renderPatchPlanMarkdown({
      workId,
      teamId,
      agentId,
      evidence: { search_terms: searchTerms, scan, repo_roots: teamRepos, repo_registry_note: repoRegistryNote },
      approval,
      reviewPath: reviewMd ? reviewPath : null,
      patchPlan: planJson,
      inputHash,
    });

    const newHash = sha256Hex(markdown);

    let action = "patch_plan_created";
    let reason = null;
    if (existingHash) {
      if (existingHash === newHash) {
        action = "patch_plan_skipped";
        reason = "unchanged";
      } else {
        action = "patch_plan_updated";
      }
    }

    if (action !== "patch_plan_skipped") {
      await writeText(outputPath, markdown);
    }

    const ts = nowTs();
    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({ timestamp: ts, action, workId, team_id: teamId, agent_id: agentId, output_path: outputPath, hash: newHash, ...(reason ? { reason } : {}) }) +
        "\n",
    );

    created.push({ team_id: teamId, agent_id: agentId, output_path: outputPath, action, hash: newHash });
  }

  const ts = nowTs();
  await updateWorkStatus({
    workId,
    stage: "PATCH_PLANNED",
    blocked: false,
    artifacts: {
      patch_plans_dir: `ai/lane_b/work/${workId}/patch-plans/`,
      patch_plans: created.map((c) => c.output_path).slice().sort((a, b) => a.localeCompare(b)),
    },
    note: `patch_plans=${created.length} @ ${ts}`,
  });
  {
    const workDir = `ai/lane_b/work/${workId}`;
    const intakeMd = (await readTextIfExists(`${workDir}/INTAKE.md`)) || "";
    const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
    let routing = null;
    try {
      routing = routingText ? JSON.parse(routingText) : null;
    } catch {
      routing = null;
    }
    const bundleRes = await readBundleIfExists(workId);
    await writeWorkPlan({ workId, intakeMd, routing, bundle: bundleRes.ok ? bundleRes.bundle : null });
  }
  await writeGlobalStatusFromPortfolio();

  return { ok: true, workId, teams, patch_plans_created: created.length, created };
}
