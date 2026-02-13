import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { appendFile, ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { nowTs } from "../../utils/id.js";
import { agentsForTeam } from "./agent-registry.js";
import { generateProposalWithRetries, proposalJsonToMarkdown } from "./proposal-agent.js";
import { getRepoPathsForWork } from "../../utils/repo-registry.js";
import { hasRg, scanWithRgInRoots, scanFallbackInRoots, discoverPackageScriptsInRoots } from "../../utils/repo-scan.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";
import { writeWorkPlan, readBundleIfExists } from "../../utils/plan-writer.js";
import { getAIProjectRoot, resolveStatePath } from "../../project/state-paths.js";
import { resolveSsotBundle } from "../../ssot/ssot-resolver.js";
import { ensureWorkSsotBundle, renderSsotExcerptsForLlm } from "../../ssot/work-ssot-bundle.js";
import { resolveGitRefForBranch } from "../../utils/git-files.js";
import { buildWorkScopedPrCiContextPack } from "../ci/ci-context-pack.js";

export async function buildPlannerCiContextSection({ workDir }) {
  return await buildWorkScopedPrCiContextPack({ workDir });
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function classifyLlmFailure(message) {
  const msg = String(message || "").trim();
  const lower = msg.toLowerCase();
  if (!msg) return { error_type: "unknown", retryable: true };
  if (lower.includes("timed out") || lower.includes("timeout")) return { error_type: "timeout", retryable: true };
  if (lower.includes("missing openai_api_key") || lower.includes("authentication") || lower.includes("unauthorized") || lower.includes(" 401"))
    return { error_type: "auth", retryable: false };
  if (lower.includes("model") && (lower.includes("not found") || lower.includes("does not exist") || lower.includes("unknown model")))
    return { error_type: "model", retryable: false };
  if (lower.includes("json parse failed") || lower.includes("schema validation failed") || lower.includes("failed to validate json"))
    return { error_type: "malformed", retryable: true };
  return { error_type: "unknown", retryable: true };
}

function pad2(n) {
  const s = String(n);
  return s.length >= 2 ? s : `0${s}`;
}

function plannerLabelForIndex(idx1) {
  const n = Number.isFinite(Number(idx1)) ? Number(idx1) : 1;
  return `planner-${pad2(Math.max(1, n))}`;
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

function deriveSearchTerms({ teamId, intakeMd, teamTaskMd }) {
  const counts = tokenizeTerms(`${intakeMd}\n${teamTaskMd}`);

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
    .slice(0, 10);

  const seeds = (teamSeeds[teamId] || []).slice();

  const out = [];
  for (const t of [...seeds, ...base]) {
    const v = String(t).trim().toLowerCase();
    if (!v) continue;
    if (!out.includes(v)) out.push(v);
    if (out.length >= 12) break;
  }
  return out;
}

function pickValidationCommands({ scripts }) {
  const preferred = ["lint", "test", "unit", "e2e", "build", "typecheck"];
  const out = [];
  for (const name of preferred) {
    const hit = scripts.find((s) => s.script === name);
    if (hit && !out.includes(hit.command)) out.push(hit.command);
  }
  if (!out.length) {
    // fallback to first few scripts
    for (const s of scripts.slice(0, 5)) {
      if (!out.includes(s.command)) out.push(s.command);
    }
  }
  return out;
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

function firstReadyWorkIdFromPortfolio(portfolioMd) {
  const lines = String(portfolioMd || "").split("\n");
  const start = lines.findIndex((l) => l.trim() === "## READY/PLANNED");
  if (start === -1) return null;

  for (let i = start + 1; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (line.startsWith("## ")) break;
    const m = line.match(/^\-\s+(W-[^\s]+)\s+\|/);
    if (m) return m[1];
  }
  return null;
}

function portfolioHasWorkIdInSection(portfolioMd, sectionHeading, workId) {
  const lines = String(portfolioMd || "").split("\n");
  const start = lines.findIndex((l) => l.trim() === `## ${sectionHeading}`);
  if (start === -1) return false;

  for (let i = start + 1; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (line.startsWith("## ")) break;
    if (line.includes(workId)) return true;
  }
  return false;
}

function decisionsHasWorkId(decisionsMd, workId) {
  return String(decisionsMd || "").split("\n").some((l) => l.trim() === `Work item: ${workId}`);
}

function updateGlobalStatusForProposals(statusMd, { workId, timestamp, proposalsCreated }) {
  const lines = String(statusMd || "").split("\n");
  const out = [];
  let inserted = false;
  let hasProposalsLine = false;

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
      out.push(`- Summary: Created ${proposalsCreated} proposal(s) for ${workId}.`);
      continue;
    }
    if (line.startsWith("- Proposals created: ")) {
      hasProposalsLine = true;
      out.push(`- Proposals created: ${proposalsCreated}`);
      continue;
    }
    out.push(line);
  }

  if (!hasProposalsLine) {
    const idx = out.findIndex((l) => l.startsWith("- Summary: "));
    if (idx !== -1) {
      out.splice(idx + 1, 0, `- Proposals created: ${proposalsCreated}`);
      inserted = true;
    }
  }

  if (!inserted && !hasProposalsLine) {
    out.push(`- Proposals created: ${proposalsCreated}`);
  }

  return out.join("\n");
}

function updateGlobalPlanNextForStep5(planMd) {
  const step5 = "- Step 5: patch-plan generation (no code writes)";
  const optionalReview = "- Optional: run --review (disabled by default)";
  const lines = String(planMd || "").split("\n");
  const nextIdx = lines.findIndex((l) => l.trim() === "## Next");

  if (nextIdx === -1) {
    return String(planMd || "").trimEnd() + `\n\n## Next\n\n${step5}\n${optionalReview}\n`;
  }

  let end = lines.length;
  for (let i = nextIdx + 1; i < lines.length; i += 1) {
    if (lines[i].startsWith("## ") && lines[i].trim() !== "## Next") {
      end = i;
      break;
    }
  }

  const has = lines.slice(nextIdx, end).some((l) => l.trim() === step5);
  const hasOptional = lines.slice(nextIdx, end).some((l) => l.trim() === optionalReview);
  if (has && hasOptional) return lines.join("\n");

  const insert = [];
  if (!has) insert.push(step5);
  if (!hasOptional) insert.push(optionalReview);

  return [...lines.slice(0, end), ...insert, ...lines.slice(end)].join("\n");
}

function buildUserPrompt({ workId, teamId, agentId, intakeMd, intakeTaggedMd, routingJson, teamTaskMd, schemaJson, ssotBundleText, ssotExcerpts }) {
  return [
    "Context:",
    `- WorkId: ${workId}`,
    `- TeamId: ${teamId}`,
    `- AgentId: ${agentId}`,
    "",
    "You must produce JSON that matches this schema exactly:",
    schemaJson.trim(),
    "",
    "Inputs (read-only):",
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
    "=== SSOT_BUNDLE.json (authoritative; reference-only; do not invent) ===",
    String(ssotBundleText || "").trim(),
    "",
    "=== SSOT SECTION EXCERPTS (clipped; cite using SSOT:<section_id>@<sha256>) ===",
    String(ssotExcerpts || "").trim(),
    "",
    "Output:",
    "- Return ONLY a JSON object (no markdown).",
  ]
    .filter(Boolean)
    .join("\n");
}

export async function runProposals({ repoRoot, workId, teamsCsv }) {
  let projectRoot;
  try {
    projectRoot = getAIProjectRoot({ required: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Cannot propose: ${msg}` };
  }

  function runGit(repoAbs, args) {
    const res = spawnSync("git", ["-C", String(repoAbs || ""), ...(Array.isArray(args) ? args : [])], { encoding: "utf8" });
    return {
      ok: res.status === 0,
      status: typeof res.status === "number" ? res.status : null,
      stdout: String(res.stdout || ""),
      stderr: String(res.stderr || ""),
    };
  }

  function hasOriginRemote(repoAbs) {
    const res = runGit(repoAbs, ["remote", "get-url", "origin"]);
    return res.ok;
  }

  function gitFetchPruneIfPossible(repoAbs) {
    if (!hasOriginRemote(repoAbs)) return { ok: true, skipped: true, stdout: "", stderr: "" };
    const res = runGit(repoAbs, ["fetch", "--prune", "origin"]);
    return { ok: res.ok, skipped: false, stdout: res.stdout, stderr: res.stderr, status: res.status };
  }

  function worktreeRegistered(repoAbs, worktreeAbs) {
    const list = runGit(repoAbs, ["worktree", "list", "--porcelain"]);
    if (!list.ok) return false;
    const needle = String(worktreeAbs || "").trim();
    return list.stdout
      .split("\n")
      .some((l) => l.trim() === `worktree ${needle}`);
  }

  async function ensureDetachedWorktreeAtRef({ repoAbs, worktreeAbs, gitRef }) {
    const wtAbs = String(worktreeAbs || "").trim();
    const ref = String(gitRef || "").trim();
    if (!wtAbs || !ref) return { ok: false, reason: "missing_worktree_or_ref" };

    if (worktreeRegistered(repoAbs, wtAbs)) {
      const resolved = runGit(repoAbs, ["rev-parse", ref]);
      if (!resolved.ok) return { ok: false, reason: "ref_unresolvable", details: resolved.stderr.trim() || resolved.stdout.trim() };
      const sha = resolved.stdout.trim();
      const co = runGit(wtAbs, ["checkout", "--detach", sha]);
      if (!co.ok) return { ok: false, reason: "worktree_checkout_failed", details: co.stderr.trim() || co.stdout.trim() };
      runGit(wtAbs, ["reset", "--hard", sha]);
      runGit(wtAbs, ["clean", "-fd"]);
      return { ok: true, reused: true, sha };
    }

    await ensureDir(wtAbs);
    const add = runGit(repoAbs, ["worktree", "add", "--detach", wtAbs, ref]);
    if (!add.ok) return { ok: false, reason: "worktree_add_failed", details: add.stderr.trim() || add.stdout.trim() };
    return { ok: true, reused: false };
  }

  const portfolioMd = (await readTextIfExists("ai/lane_b/PORTFOLIO.md")) || "";
  const decisionsMd = (await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md")) || "";

  const targetWorkId = workId || firstReadyWorkIdFromPortfolio(portfolioMd);
  if (!targetWorkId) return { ok: false, message: "Cannot propose: no --workId provided and no READY/PLANNED work item found in ai/lane_b/PORTFOLIO.md." };

  const isBlocked = decisionsHasWorkId(decisionsMd, targetWorkId) || portfolioHasWorkIdInSection(portfolioMd, "BLOCKED", targetWorkId);
  if (isBlocked) {
    return { ok: false, message: "Cannot propose: work item is BLOCKED; resolve decision first." };
  }

  const workDir = `ai/lane_b/work/${targetWorkId}`;
  const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
  const routingJson = await readTextIfExists(`${workDir}/ROUTING.json`);
  if (!intakeMd) return { ok: false, message: `Cannot propose: missing ${workDir}/INTAKE.md.` };
  if (!routingJson) return { ok: false, message: `Cannot propose: missing ${workDir}/ROUTING.json.` };

  let routing;
  try {
    routing = JSON.parse(routingJson);
  } catch {
    return { ok: false, message: `Cannot propose: invalid JSON in ${workDir}/ROUTING.json.` };
  }

  const selectedTeams = Array.isArray(routing?.selected_teams) ? routing.selected_teams.slice().filter(Boolean) : [];
  const requestedTeams = parseTeamsCsv(teamsCsv);
  const teams = (requestedTeams || selectedTeams).filter(Boolean);
  if (!teams.length) return { ok: false, message: "Cannot propose: no teams selected (ROUTING.json selected_teams empty and --teams not provided)." };

  const agentsText = await readTextIfExists("config/AGENTS.json");
  if (!agentsText) {
    return {
      ok: false,
      message: "Cannot propose: missing config/AGENTS.json. Run: node src/cli.js --agents-generate (or rerun --initial-project).",
    };
  }
  try {
    const cfg = JSON.parse(agentsText);
    if (!cfg || cfg.version !== 3 || !Array.isArray(cfg.agents)) {
      return { ok: false, message: "Cannot propose: invalid config/AGENTS.json (expected {version:3, agents:[...]}). Run: node src/cli.js --agents-migrate" };
    }
    const hasLegacyModel = cfg.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"));
    if (hasLegacyModel) return { ok: false, message: "Cannot propose: AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate" };
  } catch {
    return { ok: false, message: "Cannot propose: invalid config/AGENTS.json (must be valid JSON)." };
  }

  const intakeTaggedMd = await readTextIfExists(`${workDir}/INTAKE_TAGGED.md`);
  const systemPrompt = readFileSync(resolve(repoRoot, "src/llm/prompts/proposal.system.txt"), "utf8");
  const schemaJson = readFileSync(resolve(repoRoot, "src/llm/schemas/proposal.llm-output.schema.json"), "utf8");
  const repoContext = await getRepoPathsForWork({ workId: targetWorkId });
  const repoRegistryConfigured = repoContext.ok && repoContext.configured;
  const repoRegistryNote = repoRegistryConfigured
    ? "Repo registry configured; scanning selected repo(s) for this work item."
    : "Repo registry not configured; scanning current repo only.";

  const targetBranchName = typeof routing?.target_branch?.name === "string" && routing.target_branch.name.trim() ? routing.target_branch.name.trim() : null;
  const targetBranchValid = routing?.target_branch?.valid !== false;
  if (!targetBranchName || !targetBranchValid) return { ok: false, message: `Cannot propose: routing.target_branch is missing/invalid for ${targetWorkId}.` };

  let createLlmClient = null;
  let llmUnavailableReason = null;
  try {
    const mod = await import("../../llm/client.js");
    createLlmClient = mod.createLlmClient;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    createLlmClient = null;
    llmUnavailableReason = `LLM client unavailable (${msg}).`;
  }

  const { loadLlmProfiles, resolveLlmProfileOrError } = await import("../../llm/llm-profiles.js");
  const profilesLoaded = await loadLlmProfiles();
  if (!profilesLoaded.ok) return { ok: false, message: profilesLoaded.message, ...(profilesLoaded.errors ? { errors: profilesLoaded.errors } : {}) };

  const llmCache = new Map();
  function llmForPlannerAgent(agent) {
    if (!createLlmClient) return { llm: null, reason: llmUnavailableReason || "LLM unavailable." };
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

  await ensureDir(`${workDir}/proposals`);
  await ensureDir(`${workDir}/ssot`);

  const rgAvailable = hasRg();

  const created = [];
  for (const teamId of teams) {
    const teamReposCanonical = repoRegistryConfigured
      ? (repoContext.repos || []).filter((r) => r.team_id === teamId)
      : [{ repo_id: "ai-team", abs_path: repoRoot, exists: true }];

    // Concurrency-safe, branch-correct proposal evidence:
    // Create per-work detached worktrees at ROUTING target_branch and scan those trees.
    const teamRepos = [];
    for (const r of teamReposCanonical) {
      const repoId = String(r?.repo_id || "").trim();
      const repoAbs = String(r?.abs_path || "").trim();
      const exists = !!r?.exists;
      if (!repoId || !repoAbs || !exists) {
        teamRepos.push({ repo_id: repoId || "unknown", abs_path: repoAbs || null, exists: false });
        continue;
      }

      if (repoId === "ai-team") {
        teamRepos.push({ repo_id: repoId, abs_path: repoAbs, exists: true });
        continue;
      }

      const fetched = gitFetchPruneIfPossible(repoAbs);
      if (!fetched.ok) {
        return { ok: false, message: `Cannot propose: git fetch failed for ${repoId}: ${(fetched.stderr || fetched.stdout || "").trim() || "fetch_failed"}` };
      }

      const gitRef = resolveGitRefForBranch(repoAbs, targetBranchName);
      if (!gitRef) {
        return {
          ok: false,
          message: `Cannot propose: target branch '${targetBranchName}' not found in repo ${repoId}. Ensure canonical clone has origin/${targetBranchName} (run --checkout-active-branch).`,
        };
      }

      const baseRel = `${workDir}/worktrees/patch-plan/${repoId}/base`;
      const baseAbs = resolveStatePath(baseRel, { requiredRoot: true });
      const ensured = await ensureDetachedWorktreeAtRef({ repoAbs, worktreeAbs: baseAbs, gitRef });
      if (!ensured.ok) {
        return { ok: false, message: `Cannot propose: failed to prepare per-work worktree for ${repoId} at ${gitRef}: ${ensured.reason}${ensured.details ? ` (${ensured.details})` : ""}` };
      }

      teamRepos.push({ repo_id: repoId, abs_path: baseAbs, exists: true });
    }

    const planners = agentsForTeam(teamId, { role: "planner", implementation: "llm" });
    if (!planners.length)
      return {
        ok: false,
        message: `Cannot propose: no planner agent registered for team ${teamId}. Run: node src/cli.js --agents-generate (or rerun --initial-project).`,
      };
    const agent = planners[0];
    const plannerLabel = plannerLabelForIndex(1);
    const llmInfo = llmForPlannerAgent(agent);
    const llm = llmInfo.llm;

    const teamTaskMd = await readTextIfExists(`${workDir}/tasks/${teamId}.md`);
    if (!teamTaskMd) return { ok: false, message: `Cannot propose: missing ${workDir}/tasks/${teamId}.md.` };

    const scripts = await discoverPackageScriptsInRoots({ repoRoots: teamRepos });
    const validationCommands = pickValidationCommands({ scripts });

    const searchTerms = deriveSearchTerms({ teamId, intakeMd, teamTaskMd });
    const scan = rgAvailable
      ? scanWithRgInRoots({ repoRoots: teamRepos, terms: searchTerms })
      : await scanFallbackInRoots({ repoRoots: teamRepos, terms: searchTerms });
    const scanFinal = scan;
    const evidence = {
      artifacts: [
        `${workDir}/INTAKE.md`,
        `${workDir}/ROUTING.json`,
        `${workDir}/tasks/${teamId}.md`,
        ...(intakeTaggedMd ? [`${workDir}/INTAKE_TAGGED.md`] : []),
      ],
      repo_registry_note: repoRegistryNote,
      repo_roots: teamRepos.map((r) => ({ repo_id: r.repo_id, abs_path: r.abs_path, exists: r.exists })),
      search_terms: searchTerms,
      total_matches: scanFinal.total_matches,
      hits: scanFinal.hits,
      validation_commands: validationCommands,
    };

    const validationHint = validationCommands.length
      ? `Discovered npm scripts; prefer concrete commands:\n${validationCommands.map((c) => `- ${c}`).join("\n")}`
      : "No npm scripts discovered; suggest generic safe commands and CI defaults.";

    const ssotOut = `${workDir}/ssot/SSOT_BUNDLE.team-${teamId}.json`;
    const ssotRes = await resolveSsotBundle({ projectRoot, view: `team:${teamId}`, outPath: ssotOut, dryRun: false });
    if (!ssotRes.ok) return { ok: false, message: `Cannot propose: SSOT resolution failed for team ${teamId}: ${ssotRes.message}` };
    const ssotText = await readTextIfExists(ssotOut);
    if (!ssotText) return { ok: false, message: `Cannot propose: missing SSOT bundle after resolve: ${ssotOut}` };

    const workSsotRes = await ensureWorkSsotBundle({ workId: targetWorkId, teamId, workDir, teamBundlePath: ssotOut, allowOverwriteTeamMismatch: true });
    if (!workSsotRes.ok) return { ok: false, message: `Cannot propose: SSOT_BUNDLE.json creation failed: ${workSsotRes.message}` };
    const workSsotText = await readTextIfExists(workSsotRes.outPath);
    if (!workSsotText) return { ok: false, message: `Cannot propose: missing work SSOT_BUNDLE.json after creation: ${workSsotRes.outPath}` };
    const ssotExcerpts = renderSsotExcerptsForLlm({ teamBundleText: ssotText });

    const userPrompt = buildUserPrompt({
      workId: targetWorkId,
      teamId,
      agentId: agent.agent_id,
      intakeMd,
      intakeTaggedMd,
      routingJson,
      teamTaskMd,
      schemaJson,
      ssotBundleText: workSsotText,
      ssotExcerpts,
    });

    const ciContext = await buildPlannerCiContextSection({ workDir });
    const enrichedPrompt = [
      userPrompt,
      "",
      ...(ciContext ? [ciContext, ""] : []),
      "=== REPO SCAN EVIDENCE (read-only) ===",
      `Search terms: ${searchTerms.join(", ") || "(none)"}`,
      `Matches (captured): ${scanFinal.total_matches}`,
      ...scanFinal.hits.flatMap((h) => [`- ${h.path}`, ...(h.lines || []).map((l) => `  - ${l}`)]),
      "",
      "=== VALIDATION COMMANDS ===",
      validationHint,
      "",
      "If repo scan hits are empty, avoid guessing file paths; say 'unknown (no matches found)'.",
      "Requirement: Suggested validation MUST include concrete commands when provided above.",
    ].join("\n");

    const generated =
      llm
        ? await (async () => {
            try {
              return await generateProposalWithRetries({ llm, systemPrompt, userPrompt: enrichedPrompt });
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              return { ok: false, attempts: [{ attempt: 0, raw: `LLM invocation failed: ${msg}`, error: "LLM invocation failed." }] };
            }
          })()
        : { ok: false, attempts: [{ attempt: 0, raw: llmInfo.reason || "LLM unavailable.", error: "LLM unavailable." }] };

    let markdown;
    let usedRaw = null;
    if (generated.ok) {
      const proposedValidation = Array.isArray(generated.proposal?.suggested_validation) ? generated.proposal.suggested_validation.slice() : [];
      if (validationCommands.length) {
        const normalized = proposedValidation.map((s) => String(s).trim());
        const prepend = [];
        for (const cmd of validationCommands) {
          if (normalized.some((x) => x === cmd)) continue;
          prepend.push(cmd);
        }
        generated.proposal.suggested_validation = [...prepend, ...proposedValidation].slice(0, 12);
      } else {
        generated.proposal.suggested_validation = proposedValidation;
      }

      const likely = Array.isArray(generated.proposal?.likely_files_or_areas_impacted)
        ? generated.proposal.likely_files_or_areas_impacted.slice()
        : [];
      const specific = likely.filter((s) => String(s).includes("/") || String(s).includes("."));
      if (scan.hits.length) {
        const candidates = scan.hits.map((h) => h.path).filter(Boolean);
        const merged = [];
        for (const p of candidates) {
          if (!merged.includes(p)) merged.push(p);
          if (merged.length >= 5) break;
        }
        if (specific.length < 3) {
          generated.proposal.likely_files_or_areas_impacted = [...merged, ...likely].slice(0, 12);
        }
      } else if (!specific.length) {
        generated.proposal.likely_files_or_areas_impacted = ["unknown (repo scan found no matches for derived terms)"];
      }

      markdown = proposalJsonToMarkdown({
        workId: targetWorkId,
        teamId,
        agentId: agent.agent_id,
        proposal: generated.proposal,
        evidence,
      });
    } else {
      const last = generated.attempts[generated.attempts.length - 1];
      usedRaw = String(last?.raw || last?.error || "").trim();
      const failure = classifyLlmFailure(usedRaw);

      const proposalFailedPath = `${workDir}/PROPOSAL_FAILED.json`;
      const proposalFailedReportDir = `${workDir}/failure-reports`;
      const proposalFailedReportPath = `${proposalFailedReportDir}/proposal-failed.md`;
      const proposalFailed = {
        work_id: targetWorkId,
        stage: "PROPOSE",
        status: "FAILED",
        error_type: failure.error_type,
        error_message: usedRaw || "LLM proposal generation failed.",
        retryable: failure.retryable,
        agent_id: agent.agent_id,
        timestamp: nowTs(),
      };

      await ensureDir(proposalFailedReportDir);
      await writeText(proposalFailedPath, JSON.stringify(proposalFailed, null, 2) + "\n");
      await writeText(
        proposalFailedReportPath,
        [
          `# Proposal failed: ${targetWorkId}`,
          "",
          `Timestamp: ${proposalFailed.timestamp}`,
          "",
          `- team_id: \`${teamId}\``,
          `- agent_id: \`${agent.agent_id}\``,
          `- error_type: \`${proposalFailed.error_type}\``,
          `- retryable: \`${proposalFailed.retryable ? "yes" : "no"}\``,
          "",
          "## Error",
          "",
          "```",
          proposalFailed.error_message,
          "```",
          "",
          "## How to fix",
          "",
          "- Verify `OPENAI_API_KEY` is available to the process (cron shells often miss `.env`).",
          "- Verify outbound network access from this host to the OpenAI API endpoint.",
          "- If this is a timeout, re-run `--propose` or `--watchdog` (retryable).",
          "",
        ].join("\n"),
      );
      await writeText(
        `${workDir}/status.json`,
        JSON.stringify(
          {
            workId: targetWorkId,
            status: "failed",
            failure_stage: "PROPOSE",
            blocked: true,
            blocking_reason: "PROPOSAL_FAILED",
            repos: {},
          },
          null,
          2,
        ) + "\n",
      );

      await updateWorkStatus({
        workId: targetWorkId,
        stage: "FAILED",
        blocked: true,
        blockingReason: "PROPOSAL_FAILED",
        artifacts: {
          proposal_failed_json: proposalFailedPath,
          proposal_failed_report: proposalFailedReportPath,
          work_status_json: `${workDir}/status.json`,
        },
        note: `proposal_failed team=${teamId} agent=${agent.agent_id} type=${failure.error_type}`,
      });
      await writeGlobalStatusFromPortfolio();

      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({
          timestamp: nowTs(),
          action: "proposal_failed",
          workId: targetWorkId,
          team_id: teamId,
          agent_id: agent.agent_id,
          error_type: failure.error_type,
          retryable: failure.retryable,
          error_message: proposalFailed.error_message,
          proposal_failed_json: proposalFailedPath,
          proposal_failed_report: proposalFailedReportPath,
        }) + "\n",
      );

      return {
        ok: false,
        message: `Proposal generation failed for team ${teamId} (agent ${agent.agent_id}): ${proposalFailed.error_message}`,
        workId: targetWorkId,
        proposal_failed_json: proposalFailedPath,
        error_type: failure.error_type,
        retryable: failure.retryable,
      };
    }

    const outputJsonPath = `${workDir}/proposals/${teamId}__${plannerLabel}.json`;
    const outputMdPath = `${workDir}/proposals/${teamId}__${plannerLabel}.md`;

    const proposalJson = {
      version: 1,
      work_id: targetWorkId,
      team_id: teamId,
      agent_id: agent.agent_id,
      status: "SUCCESS",
      created_at: nowTs(),
      // Gap-2 hardening: authoritative proposal JSON must include SSOT citations at the top level.
      // The nested `proposal` object still contains the full proposal content.
      ssot_references: Array.isArray(generated?.proposal?.ssot_references) ? generated.proposal.ssot_references : [],
      proposal: generated.proposal,
      evidence,
    };
    const proposalJsonText = JSON.stringify(proposalJson, null, 2) + "\n";
    const proposalJsonSha = sha256Hex(proposalJsonText);

    const newHash = sha256Hex(markdown);

    const existing = await readTextIfExists(outputMdPath);
    const existingHash = existing ? sha256Hex(existing) : null;

    let action = "proposal_created";
    let reason = null;

    if (existingHash) {
      if (existingHash === newHash) {
        action = "proposal_skipped";
        reason = "unchanged";
      } else {
        action = "proposal_updated";
      }
    }

    if (action !== "proposal_skipped") {
      await writeText(outputJsonPath, proposalJsonText);
      await writeText(outputMdPath, markdown);
    }
    created.push({
      team_id: teamId,
      agent_id: agent.agent_id,
      proposal_json_path: outputJsonPath,
      proposal_json_sha256: proposalJsonSha,
      proposal_md_path: outputMdPath,
    });

    const ts = nowTs();
    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({
        timestamp: ts,
        action,
        workId: targetWorkId,
        team_id: teamId,
        agent_id: agent.agent_id,
        output_path: outputMdPath,
        hash: newHash,
        proposal_json_path: outputJsonPath,
        proposal_json_sha256: proposalJsonSha,
        ...(reason ? { reason } : {}),
      }) + "\n",
    );
  }

  const ts = nowTs();
  await updateWorkStatus({
    workId: targetWorkId,
    stage: "PROPOSED",
    blocked: false,
    artifacts: {
      proposals_dir: `ai/lane_b/work/${targetWorkId}/proposals/`,
      proposals: created.map((c) => c.proposal_md_path).slice().sort((a, b) => a.localeCompare(b)),
      proposal_jsons: created.map((c) => c.proposal_json_path).slice().sort((a, b) => a.localeCompare(b)),
    },
    note: `proposals=${created.length} @ ${ts}`,
  });
  {
    const workDir = `ai/lane_b/work/${targetWorkId}`;
    const intakeMd = (await readTextIfExists(`${workDir}/INTAKE.md`)) || "";
    const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
    let routing = null;
    try {
      routing = routingText ? JSON.parse(routingText) : null;
    } catch {
      routing = null;
    }
    const bundleRes = await readBundleIfExists(targetWorkId);
    await writeWorkPlan({ workId: targetWorkId, intakeMd, routing, bundle: bundleRes.ok ? bundleRes.bundle : null });
  }
  await writeGlobalStatusFromPortfolio();

  return { ok: true, workId: targetWorkId, proposals_created: created.length, created };
}
