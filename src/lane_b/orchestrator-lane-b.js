import { readdir, rename, stat } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { resolve as pathResolve, relative as pathRelative, sep as pathSep, posix as pathPosix, isAbsolute as pathIsAbsolute } from "node:path";
import { appendFile, ensureDir, readTextIfExists, writeText } from "../utils/fs.js";
import { intakeId, nowTs, workId as makeWorkId, todayUtcYyyyMmDd } from "../utils/id.js";
import { createHash } from "node:crypto";
import { loadRepoRegistry, selectReposForRouting, resolveRepoAbsPath, findExplicitRepoReferences } from "../utils/repo-registry.js";
import { resolveStatePath } from "../project/state-paths.js";
import { validatePatchPlan } from "../validators/patch-plan-validator.js";
import { validateQaPlan } from "../validators/qa-plan-validator.js";
import { validateTriagedBatch, validateTriagedRepoItem } from "../validators/triaged-repo-item-validator.js";
import { validateLlmProfiles } from "../validators/llm-profiles-validator.js";
import { validateSsotSnapshot } from "../validators/ssot-snapshot-validator.js";
import { validateSsotView } from "../validators/ssot-view-validator.js";
import { validateBacklogSeeds } from "../validators/backlog-seeds-validator.js";
import { validateGaps } from "../validators/gaps-validator.js";
import { loadPolicies } from "../policy/resolve.js";
import { validateAgentsConfigCoversTeams } from "../project/agents-generator.js";
import { extractExplicitTargetBranchFromIntake } from "../utils/branch-intent.js";
import { readWorkStatusSnapshot, updateWorkStatus, writeGlobalStatusFromPortfolio } from "../utils/status-writer.js";
import { writeWorkPlan, readBundleIfExists } from "../utils/plan-writer.js";
import { readProjectConfig } from "../project/project-config.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { probeGitWorkTree } from "../lane_a/knowledge/git-checks.js";

const REQUIRED_PROJECT_DIRS = ["ai", "config"];
const REQUIRED_PROJECT_FILES = ["config/PROJECT.json", "config/REPOS.json", "config/TEAMS.json", "config/AGENTS.json", "config/LLM_PROFILES.json", "config/POLICIES.json"];
const OPTIONAL_PROJECT_FILES = ["config/teams.json"];

const ROUTING_CONFIDENCE_THRESHOLD = 0.6;
const HIGH_RISK_KEYWORDS = ["idp", "auth", "oauth", "oidc", "token", "jwt", "encryption", "crypto", "key", "compiler", "packager", "openapi", "swagger", "contract", "cross-service"];

// NOTE: Naming must be symbol-safe for filesystem + git. We keep these helper names for minimal diffs,
// but they return canonical fs-safe UTC timestamps (not ISO strings).
function nowISO() {
  return nowTs();
}

function todayISO() {
  return todayUtcYyyyMmDd();
}

function normalizeRepoRelPathForCompare(p) {
  const s = String(p || "").trim();
  if (!s) return "";
  const replaced = s.replaceAll("\\", "/");
  const norm = pathPosix.normalize(replaced);
  if (norm === "." || norm === "./") return ".";
  return norm.startsWith("./") ? norm.slice(2) : norm;
}

function registryRepoPathForPlanCompare({ baseDir, repoPath, repoAbs }) {
  const raw = String(repoPath || "").trim();
  if (!raw) return "";
  if (pathIsAbsolute(raw)) {
    try {
      const baseAbs = pathResolve(String(baseDir || "").trim());
      const rel = pathRelative(baseAbs, repoAbs).split(pathSep).join("/");
      return normalizeRepoRelPathForCompare(rel);
    } catch {
      return normalizeRepoRelPathForCompare(raw);
    }
  }
  return normalizeRepoRelPathForCompare(raw);
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function oneLineSummary(text, maxLen = 120) {
  const first = String(text || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((l) => l.trim())
    .find((l) => l.length > 0);
  const line = first || "";
  if (line.length <= maxLen) return line;
  return line.slice(0, maxLen - 1) + "…";
}

function detectHighRisk(intakeText) {
  const t = intakeText.toLowerCase();
  return HIGH_RISK_KEYWORDS.some((kw) => t.includes(kw));
}

function explicitTagsPresent(intakeText) {
  const t = intakeText.toLowerCase();
  return t.includes("boundedcontext:") && t.includes("securityboundary:");
}

function intakeAllowsArchivedRepos(intakeText) {
  const t = String(intakeText || "").toLowerCase();
  return (
    t.includes("include archived") ||
    t.includes("include inactive") ||
    t.includes("include archived repos") ||
    t.includes("include inactive repos")
  );
}

function defaultWorkMeta({ workId, createdAtIso = null } = {}) {
  const created_at = createdAtIso && String(createdAtIso).trim() ? String(createdAtIso).trim() : nowISO();
  return {
    version: 1,
    work_id: workId,
    created_at,
    // Lineage (triage batch -> per-repo work). These may remain null for legacy/non-triaged works.
    raw_intake_id: null,
    batch_id: null,
    triaged_id: null,
    repo_id: null,
    team_id: null,
    target_branch: null,
    priority: 50,
    depends_on: [],
    blocks: [],
    labels: [],
    repo_scopes: [],
  };
}

async function ensureWorkMeta({ workId, createdAtIso = null }) {
  const path = `ai/lane_b/work/${workId}/META.json`;
  const existing = await readTextIfExists(path);
  if (existing) {
    try {
      const parsed = JSON.parse(existing);
      if (parsed && parsed.version === 1) return { ok: true, path, meta: parsed, created: false };
    } catch {
      // fall through to rewrite
    }
  }
  const meta = defaultWorkMeta({ workId, createdAtIso });
  await writeText(path, JSON.stringify(meta, null, 2) + "\n");
  // Deterministic work-scoped status checkpoint (for apply resume + troubleshooting).
  // Always written under the work folder; do not rely on STATUS.md for machine state.
  {
    const statusPath = `ai/lane_b/work/${workId}/status.json`;
    const historyPath = `ai/lane_b/work/${workId}/status-history.json`;
    const statusText = await readTextIfExists(statusPath);
    if (!statusText) await writeText(statusPath, JSON.stringify({ workId, repos: {} }, null, 2) + "\n");
    const historyText = await readTextIfExists(historyPath);
    if (!historyText) await writeText(historyPath, "[]\n");
  }
  return { ok: true, path, meta, created: true };
}

async function updateWorkMetaFromRouting({ workId, routing }) {
  const path = `ai/lane_b/work/${workId}/META.json`;
  const text = await readTextIfExists(path);
  let meta;
  try {
    meta = text ? JSON.parse(text) : null;
  } catch {
    meta = null;
  }
  const base = meta && typeof meta === "object" && meta.version === 1 ? meta : defaultWorkMeta({ workId, createdAtIso: routing?.timestamp || null });

  const next = { ...base };
  if (!Array.isArray(next.depends_on)) next.depends_on = [];
  if (!Array.isArray(next.repo_scopes)) next.repo_scopes = [];

  const explicitRepo = routing?.routing_mode === "repo_explicit";
  const explicitBranch = routing?.target_branch?.source === "explicit";

  if (explicitRepo && (!Array.isArray(next.repo_scopes) || next.repo_scopes.length === 0)) {
    const repos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : [];
    next.repo_scopes = repos;
  }
  if (explicitBranch && (next.target_branch === null || next.target_branch === "")) {
    const b = typeof routing?.target_branch?.name === "string" ? routing.target_branch.name.trim() : "";
    if (b) next.target_branch = b;
  }

  await writeText(path, JSON.stringify(next, null, 2) + "\n");
  return { ok: true, path, meta: next };
}

function git(cwd, args) {
  const res = spawnSync("git", ["-C", cwd, ...args], { encoding: "utf8" });
  return {
    ok: res.status === 0,
    status: res.status,
    stdout: String(res.stdout || "").trim(),
    stderr: String(res.stderr || "").trim(),
  };
}

function branchExists(repoAbs, branchName) {
  const b = String(branchName || "").trim();
  if (!b) return false;
  if (git(repoAbs, ["show-ref", "--verify", "--quiet", `refs/heads/${b}`]).ok) return true;
  if (git(repoAbs, ["show-ref", "--verify", "--quiet", `refs/remotes/origin/${b}`]).ok) return true;
  return false;
}

function repoDefaultBranchFromGit(repoAbs) {
  const res = git(repoAbs, ["symbolic-ref", "refs/remotes/origin/HEAD"]);
  if (!res.ok) return { ok: false, name: null, method: "origin_head", error: res.stderr || res.stdout || "origin/HEAD not available" };
  const ref = String(res.stdout || "").trim();
  const m = ref.match(/^refs\/remotes\/origin\/(.+?)$/);
  if (!m) return { ok: false, name: null, method: "origin_head", error: `Unexpected origin/HEAD ref: ${ref}` };
  return { ok: true, name: m[1], method: "origin_head", error: null };
}

function repoDefaultBranchFallback(repoAbs) {
  const candidates = ["develop", "main", "master"];
  for (const c of candidates) {
    if (branchExists(repoAbs, c)) return { ok: true, name: c, method: "fallback", error: null };
  }
  return { ok: true, name: "main", method: "fallback", error: null };
}

function scoreTeams(intakeText, teams) {
  const t = intakeText.toLowerCase();

  const scored = (teams || []).map((team) => {
    const hints = Array.isArray(team?.scope_hints) ? team.scope_hints : [];
    const matched = [];
    for (const rawHint of hints) {
      const hint = String(rawHint || "").trim().toLowerCase();
      if (!hint) continue;
      if (t.includes(hint)) matched.push(rawHint);
    }
    return { team_id: team.team_id, score: matched.length, matched_hints: matched };
  });

  const totalMatches = scored.reduce((sum, x) => sum + x.score, 0);
  const bestScore = scored.reduce((m, x) => (x.score > m ? x.score : m), 0);

  const matches = scored
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score || String(a.team_id).localeCompare(String(b.team_id)))
    .map((x) => ({ team_id: x.team_id, matched_hints: x.matched_hints }));

  const selectedTeams = scored
    .filter((x) => x.score === bestScore && x.score > 0)
    .map((x) => x.team_id)
    .sort((a, b) => String(a).localeCompare(String(b)));

  const routingConfidence = bestScore / Math.max(1, totalMatches);

  return { matches, selectedTeams, routingConfidence, totalMatches, bestScore };
}

function routeTeams({ workId, timestamp, intakeText, intakeSource, teams }) {
  const { matches, selectedTeams, routingConfidence, totalMatches } = scoreTeams(intakeText, teams);
  const highRiskDetected = detectHighRisk(intakeText);

  let needsConfirmation = totalMatches === 0;
  if (highRiskDetected) needsConfirmation = true;

  let reason;
  if (totalMatches === 0) {
    reason = "No scope_hints matched intake.";
  } else if (highRiskDetected) {
    reason = "High-risk keyword detected; confirmation required.";
  } else {
    reason = `Matched teams: ${selectedTeams.join(", ") || "(none)"}.`;
  }

  const routing = {
    workId,
    timestamp,
    intake: {
      source: intakeSource,
      summary: oneLineSummary(intakeText),
    },
    matches,
    selected_teams: selectedTeams,
    routing_confidence: routingConfidence,
    high_risk_detected: highRiskDetected,
    needs_confirmation: needsConfirmation,
    reason,
  };

  if (needsConfirmation) {
    if (highRiskDetected) {
      routing.proposed_question = `Routing confirmation required for ${workId} (high-risk).`;
      routing.proposed_options = explicitTagsPresent(intakeText)
        ? ["A: Confirm routing for this high-risk change.", "B: Escalate to Architect."]
        : ["A: Add `BoundedContext:` and `SecurityBoundary:` to intake and rerun.", "B: Escalate to Architect."];
    } else {
      routing.proposed_question = `Routing confirmation required for ${workId}: no team matches found.`;
      routing.proposed_options = ["A: Route to BackendCore.", "B: Escalate to Architect."];
    }
  }

  return routing;
}

async function augmentRoutingWithRepos({ routing, intakeText }) {
  const loaded = await loadRepoRegistry();
  if (!loaded.ok) {
    routing.selected_repos = [];
    routing.repo_scores = {};
    routing.repo_matches = [];
    routing.repo_registry_configured = false;
    routing.repo_registry_message = loaded.message;
    routing.routing_mode = "keyword_fallback";
    routing.repo_match = null;
    routing.target_branch = {
      name: null,
      source: "default",
      method: "unavailable",
      confidence: 0.6,
      valid: false,
    };
    routing.needs_confirmation = true;
    routing.reason = `${routing.reason} Repo registry missing; cannot resolve target branch.`;
    if (!routing.proposed_question) {
      routing.proposed_question = `Routing confirmation required for ${routing.workId} (repo registry missing).`;
      routing.proposed_options = ["A: Create config/REPOS.json and rerun.", "B: Escalate to Architect."];
    }
    return routing;
  }

  const registry = loaded.registry;
  const baseDir = String(registry.base_dir || "").trim();

  routing.repo_registry_configured = true;
  routing.repo_registry_message = `Loaded ${registry.repos.length} repo(s) from config/REPOS.json.`;

  // 1) Repo explicit override (deterministic): if intake names exactly one repo, route to its team_id and repo_id.
  const explicitMatches = findExplicitRepoReferences({ intakeText, registry });
  routing.repo_match = null;
  routing.routing_mode = "keyword_fallback";

  // Keep keyword team matches for visibility.
  const keywordSelectedTeams = Array.isArray(routing?.selected_teams) ? routing.selected_teams.slice() : [];

  if (explicitMatches.length > 1) {
    routing.routing_mode = "repo_ambiguous";
    routing.selected_repos = [];
    routing.repo_scores = {};
    routing.repo_matches = [];
    routing.needs_confirmation = true;
    routing.reason = `${routing.reason} Explicit repo reference is ambiguous (${explicitMatches.map((m) => m.repo_id).join(", ")}).`;
    routing.proposed_question = `Repo selection ambiguous for ${routing.workId}. Candidates: ${explicitMatches.map((m) => m.repo_id).join(", ")}.`;
    routing.proposed_options = [
      `A: Update intake to name exactly one repo_id (${explicitMatches.map((m) => m.repo_id).join(", ")}), then rerun.`,
      "B: Escalate to Architect.",
    ];
  } else if (explicitMatches.length === 1) {
    const m = explicitMatches[0];
    routing.routing_mode = "repo_explicit";
    routing.selected_repos = [m.repo_id];
    routing.selected_teams = m.team_id ? [m.team_id] : [];
    routing.repo_scores = { [m.repo_id]: 0 };
    routing.repo_matches = [{ repo_id: m.repo_id, matched_keywords: [] }];
    routing.repo_match = { repo_id: m.repo_id, match_type: m.match_type, confidence: 1.0, matched_token: m.matched_token };

    if (Array.isArray(routing.matches)) {
      routing.matches = routing.matches.map((x) => ({ ...x, ignored_due_to_repo_match: true }));
    }

    // Archived/inactive explicit mention: refuse unless intake explicitly allows.
    const status = String(m.status || "").trim().toLowerCase();
    const allowArchived = intakeAllowsArchivedRepos(intakeText);
    if (!m.team_id) {
      routing.needs_confirmation = true;
      routing.reason = `${routing.reason} Explicit repo match '${m.repo_id}' is missing team_id in config/REPOS.json.`;
      routing.proposed_question = `Repo ${m.repo_id} has no team_id in config/REPOS.json. Assign a team_id, then rerun.`;
      routing.proposed_options = ["A: Add team_id for this repo in config/REPOS.json and rerun.", "B: Escalate to Architect."];
    } else if (status && status !== "active" && !allowArchived) {
      routing.needs_confirmation = true;
      routing.high_risk_detected = true;
      routing.reason = `${routing.reason} Explicit repo match is ${status}; refused without explicit allow.`;
      routing.proposed_question = `Repo ${m.repo_id} is ${status}. Confirm whether to include archived/inactive repos.`;
      routing.proposed_options = ["A: Update intake to include 'include archived repos' (if intended) and rerun.", "B: Escalate to Architect."];
    }
  } else {
    // 2) Keyword routing (fallback): select repos for the already-selected team(s).
    const { selected_repos, repo_scores, repo_matches, archived_explicit_mentions } = selectReposForRouting({
      intakeText,
      selectedTeams: keywordSelectedTeams,
      registry,
      topNPerTeam: 1,
    });

    routing.selected_repos = selected_repos;
    routing.repo_scores = repo_scores;
    routing.repo_matches = repo_matches;
    routing.routing_mode = "keyword_fallback";

    if (archived_explicit_mentions.length) {
      routing.high_risk_detected = true;
      routing.needs_confirmation = true;
      routing.reason = `${routing.reason} Archived repo mentioned in intake: ${archived_explicit_mentions.join(", ")}.`;

      if (!routing.proposed_question) {
        routing.proposed_question = `Routing confirmation required for ${routing.workId} (archived repo mentioned).`;
        routing.proposed_options = explicitTagsPresent(intakeText)
          ? ["A: Confirm routing for this high-risk change.", "B: Escalate to Architect."]
          : ["A: Add `BoundedContext:` and `SecurityBoundary:` to intake and rerun.", "B: Escalate to Architect."];
      }
    }
  }

  // 3) Target branch extraction (always).
  const explicitBranch = extractExplicitTargetBranchFromIntake(intakeText);
  const selectedRepos = Array.isArray(routing.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : [];
  const byRepoId = new Map((registry.repos || []).map((r) => [String(r.repo_id || "").trim(), r]));

  // Determine which repo to use for default-branch resolution.
  const defaultRepoId = selectedRepos.length ? selectedRepos[0] : null;
  const defaultRepo = defaultRepoId ? byRepoId.get(defaultRepoId) || null : null;
  const defaultRepoAbs = defaultRepo ? resolveRepoAbsPath({ baseDir, repoPath: defaultRepo.path }) : null;

  let targetBranch = null;
  if (explicitBranch) {
    let name = explicitBranch.name;
    const repoDefault = defaultRepo && typeof defaultRepo.default_branch === "string" ? defaultRepo.default_branch.trim() : null;
    if (name === "main" && repoDefault === "master" && /main\s+branch/i.test(String(explicitBranch.matched_token || ""))) {
      name = "master";
    }
    targetBranch = { name, source: "explicit", matched_token: explicitBranch.matched_token, confidence: 1.0, method: "explicit", valid: true };
  } else {
    if (defaultRepo && typeof defaultRepo.default_branch === "string" && defaultRepo.default_branch.trim()) {
      targetBranch = { name: defaultRepo.default_branch.trim(), source: "default", method: "repos_json", confidence: 0.6, valid: true };
    } else if (defaultRepoAbs) {
      const originHead = repoDefaultBranchFromGit(defaultRepoAbs);
      if (originHead.ok && originHead.name) {
        targetBranch = { name: originHead.name, source: "default", method: "origin_head", confidence: 0.6, valid: true };
      } else {
        const fb = repoDefaultBranchFallback(defaultRepoAbs);
        targetBranch = { name: fb.name, source: "default", method: fb.method, confidence: 0.6, valid: true };
      }
    } else {
      targetBranch = { name: null, source: "default", method: "unresolved", confidence: 0.6, valid: false };
    }
  }

  routing.target_branch = targetBranch;

  // Branch validity checks.
  if (targetBranch && targetBranch.name && selectedRepos.length) {
    const missing = [];
    for (const repoId of selectedRepos) {
      const repo = byRepoId.get(String(repoId)) || null;
      if (!repo) continue;
      const repoAbs = resolveRepoAbsPath({ baseDir, repoPath: repo.path });
      if (!repoAbs) continue;
      if (!branchExists(repoAbs, targetBranch.name)) missing.push(repoId);
    }

    if (explicitBranch && missing.length) {
      routing.target_branch.valid = false;
      routing.needs_confirmation = true;
      routing.reason = `${routing.reason} Explicit branch '${targetBranch.name}' missing in repo(s): ${missing.join(", ")}.`;
      routing.proposed_question = `Branch '${targetBranch.name}' was explicitly requested but does not exist for repo(s): ${missing.join(", ")}. Confirm the intended branch.`;
      routing.proposed_options = ["A: Update intake with the correct branch name and rerun.", "B: Escalate to Architect."];
    } else if (!explicitBranch && missing.length) {
      routing.target_branch.valid = false;
      routing.needs_confirmation = true;
      routing.reason = `${routing.reason} Default branch '${targetBranch.name}' not found in repo(s): ${missing.join(", ")}.`;
      routing.proposed_question = `Could not validate default branch '${targetBranch.name}' for repo(s): ${missing.join(", ")}. Confirm the intended branch.`;
      routing.proposed_options = ["A: Specify a target branch explicitly in intake and rerun.", "B: Escalate to Architect."];
    }
  }

  if (!routing.target_branch || !routing.target_branch.name || routing.target_branch.valid === false) {
    routing.needs_confirmation = true;
    if (!routing.proposed_question) {
      routing.proposed_question = `Routing confirmation required for ${routing.workId} (target branch unresolved).`;
      routing.proposed_options = ["A: Specify a target branch explicitly in intake and rerun.", "B: Escalate to Architect."];
    }
  }

  return routing;
}

function renderGlobalStatus({ workId, timestamp, outcome, summary }) {
  return [
    "# STATUS",
    "",
    `Last updated: ${todayISO()}`,
    "",
    "## Current",
    "",
    `- Work item: ${workId}`,
    `- State: orchestrator_cycle_complete`,
    `- Outcome: ${outcome}`,
    `- Timestamp: ${timestamp}`,
    `- Summary: ${summary}`,
    "",
  ].join("\n");
}

function renderGlobalPlan({ workId, timestamp, intakePath, routing }) {
  const routedTeams = routing.selected_teams.length ? routing.selected_teams.join(", ") : "(none)";
  const confidenceLine = `${routing.routing_confidence.toFixed(2)}`;

  const stubs =
    routing.selected_teams.length > 0
      ? routing.selected_teams.map((t) => `- ${t}: (task stubs placeholder)`).join("\n")
      : "- (none)";

  return [
    "# PLAN",
    "",
    `Last updated: ${todayISO()}`,
    "",
    "## Current",
    "",
    `- Work item: ${workId}`,
    `- Timestamp: ${timestamp}`,
    `- Intake: ${intakePath}`,
    `- Routed teams: ${routedTeams}`,
    `- Routing confidence: ${confidenceLine}`,
    `- Needs confirmation: ${routing.needs_confirmation ? "yes" : "no"}`,
    "",
    "## Next",
    "",
    "- triage intake into teams",
    "- produce routing decision + confidence",
    "- produce per-team task stubs (no execution)",
    "",
    "## Per-team task stubs",
    "",
    stubs,
    "",
  ].join("\n");
}

function renderGlobalRisks({ workId }) {
  return [
    "# RISKS",
    "",
    `Last updated: ${todayISO()}`,
    "",
    "## Active",
    "",
    "- (none)",
    "",
    `<!-- current_work: ${workId} -->`,
    "",
  ].join("\n");
}

function renderGlobalDecisions({ workId, routing }) {
  if (!routing.needs_confirmation) {
    return ["# DECISIONS_NEEDED", "", `Last updated: ${todayISO()}`, "", "No pending decisions.", ""].join("\n");
  }

  const intakeLine = routing?.intake?.summary ? `Intake: ${routing.intake.summary}` : null;

  return [
    "# DECISIONS_NEEDED",
    "",
    `Last updated: ${todayISO()}`,
    "",
    `Work item: ${workId}`,
    "",
    ...(intakeLine ? [intakeLine, ""] : []),
    routing.proposed_question || `Routing confirmation required for ${workId}.`,
    "",
    ...(Array.isArray(routing.proposed_options) ? routing.proposed_options.map((o) => `- ${o}`) : []),
    "",
  ].join("\n");
}

function renderWorkIntake({ intakeText }) {
  return ["# INTAKE", "", intakeText.trim(), ""].join("\n");
}

function renderWorkPlan({ workId }) {
  return [
    "# PLAN",
    "",
    `Work item: ${workId}`,
    "",
    "1) Intake",
    "2) Plan",
    "3) Assign",
    "4) Execute",
    "5) Validate",
    "6) Report",
    "",
    "(placeholder content)",
    "",
  ].join("\n");
}

function renderWorkStatus({ workId, timestamp }) {
  return [
    "# WORK STATUS",
    "",
    `Work item: ${workId}`,
    `Created: ${timestamp}`,
    "",
    "State: started",
    "",
  ].join("\n");
}

function renderWorkRouting({ routing }) {
  return JSON.stringify(routing, null, 2) + "\n";
}

function parsePendingDecisions(markdown) {
  const text = String(markdown || "");
  if (text.includes("No pending decisions.")) return [];

  const lines = text.split("\n").map((l) => l.trimEnd());
  const decisions = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line.startsWith("Work item: ")) continue;

    const workId = line.slice("Work item: ".length).trim();
    if (!workId) continue;

    let question = null;
    let intakeSummary = null;
    const options = {};

    for (let j = i + 1; j < lines.length; j += 1) {
      const l = lines[j].trim();
      if (l.startsWith("Work item: ")) break;
      if (!l) continue;

      const intake = l.match(/^Intake:\s*(.+?)\s*$/);
      if (intake) {
        intakeSummary = intake[1].trim();
        continue;
      }

      const opt = l.match(/^\s*-\s*([AB])\s*:\s*(.+?)\s*$/);
      if (opt) {
        options[opt[1]] = opt[2];
        continue;
      }

      if (!question && !l.startsWith("- ")) {
        question = l;
      }
    }

    if (question && options.A && options.B) {
      decisions.push({ workId, intakeSummary: intakeSummary || null, question, options });
    }
  }

  return decisions;
}

function parsePendingDecision(markdown) {
  const text = String(markdown || "");
  if (text.includes("No pending decisions.")) return null;

  const decisions = parsePendingDecisions(text);
  if (!decisions.length) return { error: "No parseable pending decision found in ai/lane_b/DECISIONS_NEEDED.md." };
  return decisions[0];
}

function renderNoPendingDecisions() {
  return ["# DECISIONS_NEEDED", "", `Last updated: ${todayISO()}`, "", "No pending decisions.", ""].join("\n");
}

function renderPendingDecisions(decisions) {
  if (!decisions.length) return renderNoPendingDecisions();

  const blocks = decisions.flatMap((d) => [
    `Work item: ${d.workId}`,
    "",
    ...(d.intakeSummary ? [`Intake: ${d.intakeSummary}`, ""] : []),
    d.question,
    "",
    `- A: ${d.options.A}`,
    `- B: ${d.options.B}`,
    "",
  ]);

  return ["# DECISIONS_NEEDED", "", `Last updated: ${todayISO()}`, "", ...blocks].join("\n");
}

function autoApproveDisqualifiersFromPlan(plan) {
  const textParts = [];
  textParts.push(String(plan?.intent_summary || ""));
  textParts.push(String(plan?.risk?.notes || ""));
  for (const e of Array.isArray(plan?.edits) ? plan.edits : []) {
    textParts.push(`${String(e?.path || "")}\n${String(e?.rationale || "")}`);
  }
  const t = textParts.join("\n").toLowerCase();

  const keywords = [
    // auth/identity
    "auth",
    "oauth",
    "oidc",
    "jwt",
    "token",
    "password",
    "idp",
    // data/schema migration
    "migration",
    "migrate",
    "schema",
    "database",
    "db",
    "sql",
    "prisma",
    "typeorm",
    "sequelize",
    "knex",
    "flyway",
    "liquibase",
    "alembic",
  ];

  const wordBoundary = (w) => new RegExp(`\\b${w.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}\\b`, "i");
  const shortWords = new Set(["db", "sql", "idp", "jwt"]);
  const hits = keywords.filter((k) => {
    if (shortWords.has(k)) return wordBoundary(k).test(t);
    return t.includes(k);
  });
  const reasons = [];
  if (hits.length) reasons.push(`Contains disallowed auto-approve keywords: ${Array.from(new Set(hits)).join(", ")}.`);

  const paths = (Array.isArray(plan?.edits) ? plan.edits : []).map((e) => String(e?.path || ""));
  const pathHits = paths.filter((p) => /(^|\/)(migrations?|db)(\/|$)/i.test(p) || /prisma\/migrations/i.test(p));
  if (pathHits.length) reasons.push(`Touches migration/db paths: ${pathHits.slice(0, 6).join(", ")}.`);

  return reasons;
}

async function loadIntakeSummaryForWorkId(workId) {
  const routingText = await readTextIfExists(`ai/lane_b/work/${workId}/ROUTING.json`);
  if (routingText) {
    try {
      const routing = JSON.parse(routingText);
      const summary = routing?.intake?.summary ? String(routing.intake.summary).trim() : null;
      if (summary) return oneLineSummary(summary);
    } catch {
      // fall through
    }
  }

  const intakeMd = await readTextIfExists(`ai/lane_b/work/${workId}/INTAKE.md`);
  if (intakeMd) {
    const stripped = stripIntakeMarkdown(intakeMd);
    const summary = oneLineSummary(stripped);
    if (summary) return summary;
  }

  return null;
}

async function ensureDecisionIntakeSummaries(decisions) {
  const out = [];
  for (const d of decisions) {
    const existing = d?.intakeSummary ? String(d.intakeSummary).trim() : "";
    if (existing) {
      out.push({ ...d, intakeSummary: existing });
      continue;
    }
    const loaded = await loadIntakeSummaryForWorkId(d.workId);
    out.push({ ...d, intakeSummary: loaded || null });
  }
  return out;
}

function stripIntakeMarkdown(intakeMd) {
  const lines = String(intakeMd || "").split("\n");
  if (lines[0]?.trim() === "# INTAKE") {
    while (lines.length && lines[0].trim() === "# INTAKE") lines.shift();
    while (lines.length && lines[0].trim() === "") lines.shift();
  }
  return lines.join("\n").trim();
}

function updatePlanNextForRerun(planText, intakeTaggedPath) {
  const lines = String(planText || "").split("\n");
  const idx = lines.findIndex((l) => l.trim() === "## Next");
  if (idx === -1) {
    return (
      String(planText || "").trimEnd() +
      `\n\n## Next\n\n- rerun orchestrator with: \`node src/cli.js --intake ${intakeTaggedPath}\`\n`
    );
  }

  let end = lines.length;
  for (let i = idx + 1; i < lines.length; i += 1) {
    if (lines[i].startsWith("## ")) {
      end = i;
      break;
    }
  }

  const nextSection = [
    "## Next",
    "",
    `- rerun orchestrator with: \`node src/cli.js --intake ${intakeTaggedPath}\``,
    "",
  ];
  return [...lines.slice(0, idx), ...nextSection, ...lines.slice(end)].join("\n");
}

function updateStatusForEscalation(statusText, { workId, timestamp }) {
  const lines = String(statusText || "").split("\n");
  const out = [];
  let inCurrent = false;
  let hasEscalationLine = false;

  for (const line of lines) {
    if (line.trim() === "## Current") inCurrent = true;
    if (inCurrent && line.startsWith("## ") && line.trim() !== "## Current") inCurrent = false;

    if (line.startsWith("- Work item: ")) {
      out.push(`- Work item: ${workId}`);
      continue;
    }
    if (line.startsWith("- Timestamp: ")) {
      out.push(`- Timestamp: ${timestamp}`);
      continue;
    }
    if (line.startsWith("- Summary: ")) {
      out.push(`- Summary: Escalation pending for ${workId}.`);
      continue;
    }
    if (line.trim() === "- Escalation: pending") {
      hasEscalationLine = true;
    }
    out.push(line);
  }

  if (!hasEscalationLine) {
    const insertAt = out.findIndex((l) => l.startsWith("- Summary: "));
    if (insertAt !== -1) {
      out.splice(insertAt + 1, 0, "- Escalation: pending");
    } else {
      out.push("- Escalation: pending");
    }
  }

  return out.join("\n");
}

function parseWorkIdFromStatus(statusText) {
  const lines = String(statusText || "").split("\n");
  for (const line of lines) {
    const m = line.match(/^\s*-\s*Work item:\s*(W-[^\s]+)\s*$/);
    if (m) return m[1];
  }
  return null;
}

async function listWorkIdsDesc() {
  try {
    const entries = await readdir(resolveStatePath("ai/lane_b/work"), { withFileTypes: true });
    return entries
      .filter((e) => e.isDirectory() && e.name.startsWith("W-"))
      .map((e) => e.name)
      .sort((a, b) => b.localeCompare(a));
  } catch (err) {
    if (err && err.code === "ENOENT") return [];
    throw err;
  }
}

function updatePlanWithTaskPaths(planText, taskPaths) {
  const lines = String(planText || "").split("\n");
  const currentIdx = lines.findIndex((l) => l.trim() === "## Current");
  if (currentIdx === -1) return planText;

  let currentEnd = lines.length;
  for (let i = currentIdx + 1; i < lines.length; i += 1) {
    if (lines[i].startsWith("## ") && lines[i].trim() !== "## Current") {
      currentEnd = i;
      break;
    }
  }

  const currentBlock = lines.slice(currentIdx, currentEnd);
  const existing = new Set(currentBlock.map((l) => l.trim()));

  const insert = [];
  if (!existing.has("Task files:")) {
    insert.push("", "Task files:");
  }
  for (const p of taskPaths) {
    const bullet = `- ${p}`;
    if (!existing.has(bullet)) insert.push(bullet);
  }

  const updated = [...lines.slice(0, currentEnd), ...insert, ...lines.slice(currentEnd)];

  const nextIdx = updated.findIndex((l) => l.trim() === "## Next");
  if (nextIdx !== -1) {
    let nextEnd = updated.length;
    for (let i = nextIdx + 1; i < updated.length; i += 1) {
      if (updated[i].startsWith("## ") && updated[i].trim() !== "## Next") {
        nextEnd = i;
        break;
      }
    }

    const step4Line = "- Step 4: generate proposals (team planners)";
    const hasStep4 = updated.slice(nextIdx, nextEnd).some((l) => l.trim() === step4Line);
    if (!hasStep4) {
      updated.splice(nextEnd, 0, step4Line);
    }
  }

  return updated.join("\n");
}

function computeDependencyDefaults({ selectedTeams, intakeText }) {
  const lower = String(intakeText || "").toLowerCase();
  const multiTeam = selectedTeams.length > 1;
  const contractKeywords = ["openapi", "swagger", "contract", "auth", "token", "idp", "oauth", "oidc"];
  const contractFirst = multiTeam && contractKeywords.some((k) => lower.includes(k));

  const has = (t) => selectedTeams.includes(t);
  const implementationTeams = selectedTeams.filter((t) => !["QA", "DevOps"].includes(t));

  const deps = {};
  for (const team of selectedTeams) {
    deps[team] = { must_run_after: [], can_run_in_parallel_with: [] };
  }

  if (contractFirst && has("IdentitySecurity")) {
    if (has("Portal")) deps.Portal.must_run_after.push("IdentitySecurity");
    if (has("Mobile")) deps.Mobile.must_run_after.push("IdentitySecurity");
  }

  if (has("QA")) {
    deps.QA.must_run_after = implementationTeams.slice().sort((a, b) => a.localeCompare(b));
    deps.QA.can_run_in_parallel_with = selectedTeams.filter((t) => t !== "QA").slice().sort((a, b) => a.localeCompare(b));
  }

  if (has("DevOps")) {
    const deployRequested = ["deploy", "deployment", "release"].some((k) => lower.includes(k));
    if (deployRequested) {
      deps.DevOps.must_run_after = implementationTeams.slice().sort((a, b) => a.localeCompare(b));
    } else {
      deps.DevOps.can_run_in_parallel_with = selectedTeams.filter((t) => t !== "DevOps").slice().sort((a, b) => a.localeCompare(b));
    }
  }

  for (const team of selectedTeams) {
    if (team === "QA" || team === "DevOps") continue;
    deps[team].can_run_in_parallel_with = selectedTeams
      .filter((t) => t !== team && !deps[team].must_run_after.includes(t))
      .slice()
      .sort((a, b) => a.localeCompare(b));
  }

  return deps;
}

function expectedActions({ workId, timestamp }) {
  const workDir = `ai/lane_b/work/${workId}`;
  return {
    workId,
    timestamp,
    ensureDirs: REQUIRED_PROJECT_DIRS,
    writeFiles: [
      { path: `${workDir}/INTAKE.md` },
      { path: `${workDir}/PLAN.md` },
      { path: `${workDir}/STATUS.md` },
      { path: `${workDir}/ROUTING.json` },
      { path: "ai/lane_b/PORTFOLIO.md" },
      { path: "ai/lane_b/STATUS.md" },
      { path: "ai/lane_b/DECISIONS_NEEDED.md" },
    ],
    appendLedger: { path: "ai/lane_b/ledger.jsonl", json: { timestamp, workId, action: "orchestrator_cycle_start" } },
  };
}

async function pathExists(path) {
  const p = resolveStatePath(path);
  try {
    await stat(p);
    return true;
  } catch (err) {
    if (err && err.code === "ENOENT") return false;
    throw err;
  }
}

async function isDirectory(path) {
  const p = resolveStatePath(path);
  try {
    const s = await stat(p);
    return s.isDirectory();
  } catch (err) {
    if (err && err.code === "ENOENT") return false;
    throw err;
  }
}

async function readTeamsConfigText() {
  return (await readTextIfExists("config/TEAMS.json")) || (await readTextIfExists("config/teams.json"));
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

async function computeBundleHashFromPaths(paths) {
  const uniq = Array.from(new Set((paths || []).slice().filter(Boolean)));
  const sorted = uniq.sort((a, b) => a.localeCompare(b));
  const h = createHash("sha256");
  for (const p of sorted) {
    const text = await readTextIfExists(p);
    h.update(`${p}\n`);
    h.update(String(text || ""));
    h.update("\n---\n");
  }
  return h.digest("hex");
}

function computeBundleHashFromPins(pins) {
  const uniq = new Map();
  for (const it of pins || []) {
    if (!it?.path || !it?.sha256) continue;
    uniq.set(String(it.path), String(it.sha256));
  }
  const sorted = Array.from(uniq.entries())
    .map(([path, sha256]) => ({ path, sha256 }))
    .sort((a, b) => a.path.localeCompare(b.path));
  const h = createHash("sha256");
  for (const it of sorted) {
    h.update(`${it.path}\n`);
    h.update(it.sha256);
    h.update("\n---\n");
  }
  return h.digest("hex");
}

function bundleInputPinsOrNull(bundle) {
  const inputs = bundle && typeof bundle === "object" ? bundle.inputs : null;
  const proposals = inputs && Array.isArray(inputs.proposals) ? inputs.proposals : null;
  const plans = inputs && Array.isArray(inputs.patch_plan_jsons) ? inputs.patch_plan_jsons : null;
  const qa = inputs && Array.isArray(inputs.qa_plan_jsons) ? inputs.qa_plan_jsons : null;
  const ssot = inputs && Array.isArray(inputs.ssot_bundle_jsons) ? inputs.ssot_bundle_jsons : null;
  if (!proposals || !plans || !qa || !ssot) return null;

  const pins = [];
  for (const it of [...proposals, ...plans, ...qa, ...ssot]) {
    const path = typeof it?.path === "string" ? it.path.trim() : "";
    const sha256 = typeof it?.sha256 === "string" ? it.sha256.trim() : "";
    if (!path || !sha256) return null;
    pins.push({ path, sha256 });
  }
  return pins;
}

async function validateBundleInputsAndHash(bundle) {
  const pins = bundleInputPinsOrNull(bundle);
  if (pins) {
    const errors = [];
    for (const it of pins) {
      const text = await readTextIfExists(it.path);
      if (!text) {
        errors.push(`Missing bundle input file: ${it.path}`);
        continue;
      }
      const actual = sha256Hex(text);
      if (actual !== it.sha256) errors.push(`Bundle pin sha mismatch for ${it.path} (expected ${it.sha256}, computed ${actual}).`);
    }
    const computedHash = computeBundleHashFromPins(pins);
    if (computedHash !== bundle.bundle_hash) errors.push(`Bundle hash mismatch (expected ${bundle.bundle_hash}, computed ${computedHash}).`);
    return { ok: errors.length === 0, computedHash, errors };
  }
  return {
    ok: false,
    computedHash: null,
    errors: [
      "BUNDLE.json is missing pinned inputs (inputs.proposals + inputs.patch_plan_jsons + inputs.qa_plan_jsons + inputs.ssot_bundle_jsons). Re-run: --propose --with-patch-plans (or run --qa then re-bundle).",
    ],
  };
}

function bundleInputPaths(bundle) {
  const proposalPaths = [];
  const planJsonPaths = [];
  const ssotBundlePaths = [];

  const inputs = bundle && typeof bundle === "object" ? bundle.inputs : null;
  if (inputs && typeof inputs === "object") {
    if (Array.isArray(inputs.proposals)) {
      for (const it of inputs.proposals) {
        const p = typeof it?.path === "string" ? it.path.trim() : "";
        if (p) proposalPaths.push(p);
      }
    }
    if (Array.isArray(inputs.patch_plan_jsons)) {
      for (const it of inputs.patch_plan_jsons) {
        const p = typeof it?.path === "string" ? it.path.trim() : "";
        if (p) planJsonPaths.push(p);
      }
    }
    if (Array.isArray(inputs.ssot_bundle_jsons)) {
      for (const it of inputs.ssot_bundle_jsons) {
        const p = typeof it?.path === "string" ? it.path.trim() : "";
        if (p) ssotBundlePaths.push(p);
      }
    }
    if (Array.isArray(inputs.proposal_paths)) {
      for (const p of inputs.proposal_paths) {
        if (typeof p === "string" && p.trim()) proposalPaths.push(p.trim());
      }
    }
    if (Array.isArray(inputs.patch_plan_json_paths)) {
      for (const p of inputs.patch_plan_json_paths) {
        if (typeof p === "string" && p.trim()) planJsonPaths.push(p.trim());
      }
    }
  }

  // Back-compat: if bundle.inputs not present, fall back to per-repo pointers.
  if (!proposalPaths.length || !planJsonPaths.length) {
    for (const r of Array.isArray(bundle?.repos) ? bundle.repos : []) {
      if (!proposalPaths.length && typeof r?.proposal_path === "string" && r.proposal_path.trim()) proposalPaths.push(r.proposal_path.trim());
      if (!planJsonPaths.length && typeof r?.patch_plan_json_path === "string" && r.patch_plan_json_path.trim()) planJsonPaths.push(r.patch_plan_json_path.trim());
      if (typeof r?.ssot_bundle_json_path === "string" && r.ssot_bundle_json_path.trim()) ssotBundlePaths.push(r.ssot_bundle_json_path.trim());
    }
  }

  const uniq = Array.from(new Set([...proposalPaths, ...planJsonPaths, ...ssotBundlePaths]));
  return uniq.sort((a, b) => a.localeCompare(b));
}

async function readBundleForWork(workId) {
  const p = `ai/lane_b/work/${workId}/BUNDLE.json`;
  const text = await readTextIfExists(p);
  if (!text) return { ok: false, message: `Missing ${p}.`, path: p };
  let bundle;
  try {
    bundle = JSON.parse(text);
  } catch {
    return { ok: false, message: `Invalid JSON in ${p}.`, path: p };
  }
  if (bundle?.version !== 1) return { ok: false, message: `Invalid ${p}: expected version=1.`, path: p };
  if (bundle?.work_id !== workId) return { ok: false, message: `Invalid ${p}: work_id mismatch.`, path: p };
  if (!Array.isArray(bundle?.repos) || !bundle.repos.length) return { ok: false, message: `Invalid ${p}: repos[] missing/empty.`, path: p };
  if (typeof bundle?.bundle_hash !== "string" || !bundle.bundle_hash.trim()) return { ok: false, message: `Invalid ${p}: bundle_hash missing.`, path: p };
  return { ok: true, path: p, bundle };
}

function cleanPatchPlanRequiredByPolicy(policies) {
  const approval = policies && typeof policies === "object" ? policies.approval : null;
  if (approval && typeof approval.require_clean_patch_plan === "boolean") return approval.require_clean_patch_plan;
  return false;
}

function getAutoApprovePolicy(policies) {
  const approval = policies && typeof policies === "object" ? policies.approval : null;
  const aa = approval && typeof approval === "object" ? approval.auto_approve : null;
  const enabled = !!(aa && aa.enabled);
  return {
    enabled,
    allowed_teams: Array.isArray(aa?.allowed_teams) ? aa.allowed_teams.slice() : [],
    allowed_kinds: Array.isArray(aa?.allowed_kinds) ? aa.allowed_kinds.slice() : [],
    disallowed_risk_levels: Array.isArray(aa?.disallowed_risk_levels) ? aa.disallowed_risk_levels.slice() : [],
    require_clean_patch_plan: aa?.require_clean_patch_plan !== false,
  };
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

async function approvalJsonPath(workId) {
  return `ai/lane_b/work/${workId}/PLAN_APPROVAL.json`;
}

async function approvalMdPath(workId) {
  return `ai/lane_b/work/${workId}/PLAN_APPROVAL.md`;
}

async function migrateLegacyApprovalArtifactsIfNeeded(workId) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: true, migrated: false };
  const workDir = `ai/lane_b/work/${wid}`;
  const legacyJson = `${workDir}/APPROVAL.json`;
  const legacyMd = `${workDir}/APPROVAL.md`;
  const nextJson = `${workDir}/PLAN_APPROVAL.json`;
  const nextMd = `${workDir}/PLAN_APPROVAL.md`;

  const legacyJsonAbs = resolveStatePath(legacyJson, { requiredRoot: true });
  const legacyMdAbs = resolveStatePath(legacyMd, { requiredRoot: true });
  const nextJsonAbs = resolveStatePath(nextJson, { requiredRoot: true });
  const nextMdAbs = resolveStatePath(nextMd, { requiredRoot: true });

  let migrated = false;
  try {
    await stat(legacyJsonAbs);
    try {
      await stat(nextJsonAbs);
    } catch (err) {
      if (err && err.code === "ENOENT") {
        await rename(legacyJsonAbs, nextJsonAbs);
        migrated = true;
      } else throw err;
    }
  } catch (err) {
    if (!(err && err.code === "ENOENT")) throw err;
  }
  try {
    await stat(legacyMdAbs);
    try {
      await stat(nextMdAbs);
    } catch (err) {
      if (err && err.code === "ENOENT") {
        await rename(legacyMdAbs, nextMdAbs);
        migrated = true;
      } else throw err;
    }
  } catch (err) {
    if (!(err && err.code === "ENOENT")) throw err;
  }
  return { ok: true, migrated };
}

async function readApprovalIfExists(workId) {
  await migrateLegacyApprovalArtifactsIfNeeded(workId);
  const p = await approvalJsonPath(workId);
  const text = await readTextIfExists(p);
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { _invalid: true, raw: text };
  }
}

function renderApprovalMarkdown({ approval, proposalPathsByTeam, reviewPath }) {
  const teams = Array.isArray(approval?.scope?.teams) ? approval.scope.teams : [];
  const status = approval?.status || "pending";
  const approvedAt = approval?.approved_at || null;
  const approvedBy = approval?.approved_by || "human";
  const notes = approval?.notes || "";
  const mode = approval?.mode || "manual";
  const bundleHash = approval?.bundle_hash || null;

  const lines = [];
  lines.push("# PLAN_APPROVAL");
  lines.push("");
  lines.push(`Work item: ${approval.workId}`);
  lines.push("");
  lines.push("## Current");
  lines.push("");
  lines.push(`- Status: ${status}`);
  lines.push(`- Mode: ${mode}`);
  lines.push(`- Bundle hash: ${bundleHash || "(none)"}`);
  lines.push(`- Approved at: ${approvedAt || "(none)"}`);
  lines.push(`- Approved by: ${approvedBy}`);
  lines.push(`- Teams covered: ${teams.length ? teams.join(", ") : "(none)"}`);
  lines.push(`- Review required: ${approval?.review_required ? "true" : "false"}`);
  lines.push(`- Review seen: ${approval?.review_seen ? "true" : "false"}`);
  lines.push("");
  lines.push("## Notes");
  lines.push("");
  lines.push(notes ? String(notes) : "(none)");
  lines.push("");
  lines.push("## Artifacts");
  lines.push("");
  lines.push("Proposals:");
  if (!teams.length) {
    lines.push("- (none)");
  } else {
    for (const t of teams) {
      const paths = proposalPathsByTeam.get(t) || [];
      if (!paths.length) {
        lines.push(`- ${t}: (missing)`);
        continue;
      }
      for (const p of paths) lines.push(`- ${t}: \`${p}\``);
    }
  }
  lines.push("");
  lines.push("Review:");
  lines.push(reviewPath ? `- \`${reviewPath}\`` : "- (none)");
  lines.push("");
  return lines.join("\n");
}

function renderBundleSummaryMarkdown(bundle) {
  const repos = Array.isArray(bundle?.repos) ? bundle.repos : [];
  const lines = [];
  lines.push("## Bundle");
  lines.push("");
  lines.push(`- bundle_hash: \`${bundle?.bundle_hash || "(missing)"}\``);
  lines.push("");
  lines.push("Repos:");
  lines.push("");
  for (const r of repos.slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)))) {
    lines.push(`- ${r.repo_id}`);
    lines.push(`  - proposal: \`${r.proposal_path || "(missing)"}\``);
    lines.push(`  - patch plan (json): \`${r.patch_plan_json_path || "(missing)"}\``);
    lines.push(`  - patch plan (md): \`${r.patch_plan_md_path || "(missing)"}\``);
    lines.push(`  - qa plan (json): \`${r.qa_plan_json_path || "(missing)"}\``);
    lines.push(`  - qa plan (md): \`${r.qa_plan_md_path || "(missing)"}\``);
    if (typeof r.qa_tests === "number" || typeof r.qa_gaps === "number") {
      lines.push(`  - qa counts: tests=${typeof r.qa_tests === "number" ? r.qa_tests : "?"}, gaps=${typeof r.qa_gaps === "number" ? r.qa_gaps : "?"}`);
    }
  }
  lines.push("");
  return lines.join("\n");
}

export class Orchestrator {
  constructor({ repoRoot, projectRoot }) {
    this.repoRoot = repoRoot;
    this.projectRoot = projectRoot;
  }

  async reposValidate() {
    const loaded = await loadRepoRegistry();
    if (!loaded.ok) return { ok: false, message: loaded.message };

    const registry = loaded.registry;
    const baseDir = String(registry.base_dir || "").trim();
    const repos = Array.isArray(registry.repos) ? registry.repos.slice() : [];

    const active = repos
      .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
      .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));
    const archived = repos
      .filter((r) => String(r?.status || "").trim().toLowerCase() === "archived")
      .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

    const found = [];
    const missing = [];

    for (const r of active) {
      const repoId = String(r.repo_id || "").trim();
      const absPath = resolveRepoAbsPath({ baseDir, repoPath: r.path });
      const exists = absPath ? await isDirectory(absPath) : false;
      const entry = { repo_id: repoId, team_id: String(r.team_id || "").trim() || null, abs_path: absPath, exists };
      if (exists) found.push(entry);
      else missing.push(entry);
    }

    return {
      ok: true,
      base_dir: baseDir,
      active_total: active.length,
      active_found: found,
      active_missing: missing,
      archived_total: archived.length,
      note: "Only status=active repos are validated for existence by default.",
    };
  }

  async reposList() {
    const loaded = await loadRepoRegistry();
    if (!loaded.ok) return { ok: false, message: loaded.message };

    const repos = Array.isArray(loaded.registry.repos) ? loaded.registry.repos.slice() : [];
    const active = repos
      .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
      .map((r) => ({
        repo_id: String(r.repo_id || "").trim(),
        name: String(r.name || "").trim() || null,
        team_id: String(r.team_id || "").trim() || null,
        status: "active",
      }))
      .sort((a, b) => a.repo_id.localeCompare(b.repo_id));

    return { ok: true, active };
  }

  async validate({ workId = null } = {}) {
    const errors = [];

    for (const dir of REQUIRED_PROJECT_DIRS) {
      const ok = await isDirectory(dir);
      if (!ok) errors.push(`Missing required directory: ${dir}`);
    }

    for (const file of REQUIRED_PROJECT_FILES) {
      const ok = await pathExists(file);
      if (!ok) {
        if (file === "config/LLM_PROFILES.json") {
          errors.push("LLM_PROFILES.json missing at config/LLM_PROFILES.json. Create it from template or rerun --initial-project.");
        } else {
          errors.push(`Missing required file: ${file}`);
        }
      }
    }

    for (const file of OPTIONAL_PROJECT_FILES) {
      // Optional: validate presence and well-formedness where applicable, but don't fail if missing.
      // This keeps `--validate` usable for freshly onboarded projects that haven't written registries yet.
      await pathExists(file);
    }

    // Project determinism anchors (mandatory): config/PROJECT.json and K_ROOT layout.
    {
      let paths = null;
      try {
        paths = await loadProjectPaths({ projectRoot: this.projectRoot });
      } catch (err) {
        errors.push(err instanceof Error ? err.message : String(err));
        paths = null;
      }
      if (paths) {
        const kRoot = paths.knowledge.rootAbs;
        const repoOk = kRoot ? await isDirectory(kRoot) : false;
        if (!repoOk) {
          errors.push(`Knowledge repo directory missing: ${kRoot} (from config/PROJECT.json).`);
        } else {
          const probe = probeGitWorkTree({ cwd: kRoot });
          if (!probe.ok) {
            errors.push(probe.message || `Knowledge repo is not a git worktree: ${kRoot}`);
          } else {
            const requiredDirs = [
              paths.knowledge.ssotAbs,
              paths.knowledge.evidenceAbs,
              paths.knowledge.evidenceIndexAbs,
              paths.knowledge.viewsAbs,
              paths.knowledge.docsAbs,
              paths.knowledge.sessionsAbs,
              paths.knowledge.decisionsAbs,
              paths.knowledge.eventsAbs,
            ];
            for (const d of requiredDirs) {
              const ok = await isDirectory(d);
              if (!ok) errors.push(`Missing required knowledge directory: ${d}`);
            }

            // SSOT snapshot + views must exist and be valid (when present).
            const snapshotAbs = pathResolve(paths.knowledge.ssotAbs, "PROJECT_SNAPSHOT.json");
            const snapshotText = await readTextIfExists(snapshotAbs);
            if (snapshotText) {
              try {
                const parsed = JSON.parse(snapshotText);
                const v = validateSsotSnapshot(parsed);
                if (!v.ok) errors.push(`Invalid SSOT snapshot (${snapshotAbs}): ${v.errors.join(" | ")}`);
                else if (v.normalized.project_code !== paths.cfg.project_code) {
                  errors.push(`SSOT snapshot project_code mismatch: PROJECT.json=${paths.cfg.project_code} snapshot=${v.normalized.project_code}`);
                } else {
                  const viewsDir = paths.knowledge.viewsAbs;
                  const packs = Array.isArray(paths.cfg.ssot_bundle_policy?.global_packs) ? paths.cfg.ssot_bundle_policy.global_packs : [];
                  const wantViews = [{ id: "global", filename: "global.json" }, ...packs.map((p) => ({ id: `pack:${p}`, filename: `pack-${p}.json` }))];
                  for (const vw of wantViews) {
                    const p = pathResolve(viewsDir, vw.filename);
                    const txt = await readTextIfExists(p);
                    if (!txt) {
                      errors.push(`Missing SSOT view file for ${vw.id}: ${p}`);
                      continue;
                    }
                    try {
                      const obj = JSON.parse(txt);
                      const vv = validateSsotView(obj);
                      if (!vv.ok) errors.push(`Invalid SSOT view (${p}): ${vv.errors.join(" | ")}`);
                      else if (vv.normalized.view_id !== vw.id) errors.push(`SSOT view_id mismatch in ${p} (expected ${vw.id}).`);
                    } catch {
                      errors.push(`Invalid SSOT view (${p}): must be valid JSON.`);
                    }
                  }
                }
              } catch {
                errors.push(`Invalid SSOT snapshot (${snapshotAbs}): must be valid JSON.`);
              }
            }
          }
        }
      }
    }

    // Validate required JSON files parse.
    const policiesText = await readTextIfExists("config/POLICIES.json");
    if (policiesText) {
      try {
        JSON.parse(policiesText);
      } catch {
        errors.push("Invalid config/POLICIES.json: must be valid JSON.");
      }
    }

    const reposText = await readTextIfExists("config/REPOS.json");
    let reposCfg = null;
    if (reposText) {
      try {
        reposCfg = JSON.parse(reposText);
      } catch {
        errors.push("Invalid config/REPOS.json: must be valid JSON.");
      }
    }

    const teamsText = await readTextIfExists("config/TEAMS.json");
    let teamsCfg = null;
    if (teamsText) {
      try {
        teamsCfg = JSON.parse(teamsText);
        const teams = Array.isArray(teamsCfg?.teams) ? teamsCfg.teams : null;
        if (!teams) errors.push("Invalid config/TEAMS.json: expected `{ \"teams\": [...] }`.");
        if (teams && teams.some((t) => !t?.team_id || !Array.isArray(t.scope_hints) || (t.risk_level !== "normal" && t.risk_level !== "high"))) {
          errors.push("Invalid config/TEAMS.json: each team must include `team_id`, `scope_hints[]`, and `risk_level` (normal|high).");
        }
      } catch {
        errors.push("Invalid config/TEAMS.json: must be valid JSON.");
      }
    }

    const teamsById = new Set((Array.isArray(teamsCfg?.teams) ? teamsCfg.teams : []).map((t) => String(t?.team_id || "").trim()).filter(Boolean));
    const reposById = new Set((Array.isArray(reposCfg?.repos) ? reposCfg.repos : []).map((r) => String(r?.repo_id || "").trim()).filter(Boolean));

    const llmProfilesText = await readTextIfExists("config/LLM_PROFILES.json");
    let llmProfiles = null;
    if (llmProfilesText) {
      try {
        const parsed = JSON.parse(llmProfilesText);
        const v = validateLlmProfiles(parsed);
        if (!v.ok) {
          errors.push(`Invalid config/LLM_PROFILES.json: ${v.errors.join(" | ")}`);
        } else {
          llmProfiles = v.normalized.profiles;
        }
      } catch {
        errors.push("Invalid config/LLM_PROFILES.json: must be valid JSON.");
      }
    }

    const agentsText = await readTextIfExists("config/AGENTS.json");
    if (agentsText) {
      try {
        const cfg = JSON.parse(agentsText);
        if (!cfg || cfg.version !== 3 || !Array.isArray(cfg.agents)) {
          errors.push("Invalid config/AGENTS.json: expected `{ \"version\": 3, \"agents\": [...] }`. Run: node src/cli.js --agents-migrate");
        } else {
          const hasLegacyModel = cfg.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"));
          if (hasLegacyModel) errors.push("AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate");

          for (const a of cfg.agents) {
            const agentId = String(a?.agent_id || "").trim() || "<unknown>";
            const impl = String(a?.implementation || "").trim();
            if (impl === "llm") {
              const prof = typeof a?.llm_profile === "string" ? a.llm_profile.trim() : "";
              if (!prof) errors.push(`Agent ${agentId} missing llm_profile (implementation=llm).`);
              if (prof && llmProfiles && !Object.prototype.hasOwnProperty.call(llmProfiles, prof)) errors.push(`Agent ${agentId} references unknown llm_profile '${prof}'.`);
            }
          }

          if (teamsCfg) {
            const cover = validateAgentsConfigCoversTeams({ teamsConfig: teamsCfg, agentsConfig: cfg });
            if (!cover.ok) errors.push(...cover.errors);
          }
        }
      } catch {
        errors.push("Invalid config/AGENTS.json: must be valid JSON.");
      }
    }

    // Knowledge exports validation (optional): BACKLOG_SEEDS.json and GAPS.json under ssot/system when present.
    {
      let paths = null;
      try {
        paths = await loadProjectPaths({ projectRoot: this.projectRoot });
      } catch {
        paths = null;
      }
      if (paths) {
        const seedsAbs = pathResolve(paths.knowledge.ssotSystemAbs, "BACKLOG_SEEDS.json");
        const gapsAbs = pathResolve(paths.knowledge.ssotSystemAbs, "GAPS.json");

        const seedsText = await readTextIfExists(seedsAbs);
        if (seedsText) {
          try {
            const parsed = JSON.parse(seedsText);
            const v = validateBacklogSeeds(parsed, { teamsById, reposById, expectedProjectCode: paths.cfg.project_code, expectedProjectMode: null });
            if (!v.ok) errors.push(`Invalid BACKLOG_SEEDS.json (${seedsAbs}): ${v.errors.join(" | ")}`);
          } catch {
            errors.push(`Invalid BACKLOG_SEEDS.json (${seedsAbs}): must be valid JSON.`);
          }
        }

        const gapsText = await readTextIfExists(gapsAbs);
        if (gapsText) {
          try {
            const parsed = JSON.parse(gapsText);
            const v = validateGaps(parsed, { teamsById, reposById, expectedProjectCode: paths.cfg.project_code, expectedProjectMode: null });
            if (!v.ok) errors.push(`Invalid GAPS.json (${gapsAbs}): ${v.errors.join(" | ")}`);
          } catch {
            errors.push(`Invalid GAPS.json (${gapsAbs}): must be valid JSON.`);
          }
        }
      }
    }

	    // Optional bundle validation.
	    if (workId) {
	      const bundleRes = await readBundleForWork(String(workId));
	      if (!bundleRes.ok) {
	        errors.push(bundleRes.message);
      } else {
        const bundle = bundleRes.bundle;
        const v = await validateBundleInputsAndHash(bundle);
        if (!v.ok) errors.push(...v.errors);
        if (typeof bundle?.ssot_bundle_path === "string" && bundle.ssot_bundle_path.trim()) {
          const p = bundle.ssot_bundle_path.trim();
          const txt = await readTextIfExists(p);
          if (!txt) errors.push(`Missing bundle ssot_bundle_path file: ${p}`);
          else {
            const sha = sha256Hex(txt);
            if (typeof bundle?.ssot_bundle_sha256 === "string" && bundle.ssot_bundle_sha256.trim() && sha !== bundle.ssot_bundle_sha256.trim()) {
              errors.push(`Bundle ssot_bundle_sha256 mismatch for ${p} (expected ${bundle.ssot_bundle_sha256}, computed ${sha}).`);
            }
          }
        }

        const policiesLoaded = await loadPolicies();
        if (!policiesLoaded.ok) errors.push(policiesLoaded.message);
        const reposLoaded = await loadRepoRegistry();
        if (!reposLoaded.ok) errors.push(reposLoaded.message);

        if (policiesLoaded.ok && reposLoaded.ok) {
          const policies = policiesLoaded.policies;
          const registry = reposLoaded.registry;
          const byRepoId = new Map((registry.repos || []).map((x) => [String(x.repo_id || "").trim(), x]));
          for (const br of bundle.repos || []) {
            const repoId = String(br?.repo_id || "").trim();
            const planPath = String(br?.patch_plan_json_path || "").trim();
            const qaPath = String(br?.qa_plan_json_path || "").trim();
            const proposalPath = String(br?.proposal_path || "").trim();
            if (proposalPath) {
              const proposalText = await readTextIfExists(proposalPath);
              if (!proposalText) errors.push(`Missing proposal JSON: ${proposalPath}`);
              else {
                try {
                  const proposalJson = JSON.parse(proposalText);
                  if (!Array.isArray(proposalJson?.ssot_references)) errors.push(`Proposal missing ssot_references: ${proposalPath}`);
                  else if (proposalJson.ssot_references.length < 1) errors.push(`Proposal ssot_references empty: ${proposalPath}`);
                } catch {
                  errors.push(`Invalid JSON in proposal: ${proposalPath}`);
                }
              }
            }
            const planText = await readTextIfExists(planPath);
            if (!planText) {
              errors.push(`Missing patch plan JSON: ${planPath}`);
              continue;
            }
            let planJson;
            try {
              planJson = JSON.parse(planText);
            } catch {
              errors.push(`Invalid JSON in patch plan: ${planPath}`);
              continue;
            }
            const v = validatePatchPlan(planJson, { policy: policies });
            if (!v.ok) errors.push(`Invalid patch plan (${repoId}): ${v.errors.join(" | ")}`);
            const repo = byRepoId.get(repoId);
            if (!repo) errors.push(`Repo not found in registry for bundle: ${repoId}`);

            if (!qaPath) {
              errors.push(`Missing qa_plan_json_path for bundle repo: ${repoId}`);
              continue;
            }
            const qaText = await readTextIfExists(qaPath);
            if (!qaText) {
              errors.push(`Missing QA plan JSON: ${qaPath}`);
              continue;
            }
            let qaJson;
            try {
              qaJson = JSON.parse(qaText);
            } catch {
              errors.push(`Invalid JSON in QA plan: ${qaPath}`);
              continue;
            }
            const qv = validateQaPlan(qaJson, { expectedWorkId: String(workId), expectedRepoId: repoId });
            if (!qv.ok) errors.push(`Invalid QA plan (${repoId}): ${qv.errors.join(" | ")}`);
          }
	        }
	      }
	    }

	    // QA artifacts validation (work-scoped, even if bundle missing).
	    if (workId) {
	      const workDir = `ai/lane_b/work/${String(workId)}`;
	      const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
	      let routing = null;
	      try {
	        routing = routingText ? JSON.parse(routingText) : null;
	      } catch {
	        routing = null;
	      }
	      const repos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.map((x) => String(x)).filter(Boolean) : [];
	      for (const repoId of repos) {
	        const qaPath = `${workDir}/qa/qa-plan.${repoId}.json`;
	        const qaText = await readTextIfExists(qaPath);
	        if (!qaText) continue; // only validate when present; bundling enforces requiredness
	        let qaJson;
	        try {
	          qaJson = JSON.parse(qaText);
	        } catch {
	          errors.push(`Invalid JSON in QA plan: ${qaPath}`);
	          continue;
	        }
	        const qv = validateQaPlan(qaJson, { expectedWorkId: String(workId), expectedRepoId: repoId });
	        if (!qv.ok) errors.push(`Invalid QA plan (${repoId}): ${qv.errors.join(" | ")}`);
	      }
	    }

    // Reviewer JSON artifact validation (if present; authoritative JSON only).
    if (workId) {
      const reviewJsonPath = `ai/lane_b/work/${String(workId)}/reviews/architect-review.json`;
      const txt = await readTextIfExists(reviewJsonPath);
      if (txt) {
        try {
          const obj = JSON.parse(txt);
          if (!Array.isArray(obj?.ssot_references) || obj.ssot_references.length < 1) errors.push(`Invalid review JSON (missing ssot_references): ${reviewJsonPath}`);
        } catch {
          errors.push(`Invalid JSON in review: ${reviewJsonPath}`);
        }
      }
    }

    // Two-gate PR/CI workflow artifacts validation (work-scoped only; no new work/intake from CI).
    if (workId) {
      const wid = String(workId);
      const statusRes = await readWorkStatusSnapshot(wid);
      const currentStage = statusRes.ok ? String(statusRes.snapshot?.current_stage || "").trim() : "";

      const stagesRequiringApplyApproval = new Set([
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
        "MERGED",
        "DONE",
        // legacy
        "GATE_A_PENDING",
        "GATE_A_APPROVED",
        "GATE_B_PENDING",
        "APPROVED_TO_MERGE",
      ]);
      const stagesRequiringApplyApprovalApproved = new Set([
        "APPLY_APPROVAL_APPROVED",
        "APPLYING",
        "APPLIED",
        "CI_PENDING",
        "CI_FAILED",
        "CI_FIXING",
        "CI_GREEN",
        "MERGE_APPROVAL_PENDING",
        "MERGE_APPROVAL_APPROVED",
        "MERGED",
        "DONE",
        // legacy
        "GATE_A_APPROVED",
        "GATE_B_PENDING",
        "APPROVED_TO_MERGE",
      ]);
      const stagesRequiringPr = new Set([
        "APPLIED",
        "CI_PENDING",
        "CI_FAILED",
        "CI_FIXING",
        "CI_GREEN",
        "MERGE_APPROVAL_PENDING",
        "MERGE_APPROVAL_APPROVED",
        "MERGED",
        "DONE",
        // legacy
        "GATE_B_PENDING",
        "APPROVED_TO_MERGE",
      ]);
      const stagesRequiringCi = new Set([
        "CI_PENDING",
        "CI_FAILED",
        "CI_FIXING",
        "CI_GREEN",
        "MERGE_APPROVAL_PENDING",
        "MERGE_APPROVAL_APPROVED",
        "MERGED",
        "DONE",
        // legacy
        "GATE_B_PENDING",
        "APPROVED_TO_MERGE",
      ]);
      const stagesRequiringMergeApproval = new Set(["MERGE_APPROVAL_PENDING", "MERGE_APPROVAL_APPROVED", "MERGED", "DONE", "GATE_B_PENDING", "APPROVED_TO_MERGE"]);
      const stagesRequiringMergeApprovalApproved = new Set(["MERGE_APPROVAL_APPROVED", "MERGED", "DONE", "APPROVED_TO_MERGE"]);

      // Apply approval
      const applyApprovalPath = `ai/lane_b/work/${wid}/APPLY_APPROVAL.json`;
      const legacyGateAPath = `ai/lane_b/work/${wid}/GATE_A.json`;
      const applyApprovalText = (await readTextIfExists(applyApprovalPath)) || (await readTextIfExists(legacyGateAPath));
      const applyApprovalLabel = (await readTextIfExists(applyApprovalPath)) ? applyApprovalPath : legacyGateAPath;
      if (!applyApprovalText && stagesRequiringApplyApproval.has(currentStage)) {
        errors.push(`Missing apply-approval artifact: ${applyApprovalPath} (run: node src/cli.js --apply-approval --workId ${wid})`);
      }
      if (applyApprovalText) {
        try {
          const aa = JSON.parse(applyApprovalText);
          if (aa?.version !== 1) errors.push(`Invalid apply-approval JSON (expected version=1): ${applyApprovalLabel}`);
          if (String(aa?.workId || "").trim() !== wid) errors.push(`apply-approval workId mismatch: ${applyApprovalLabel}`);
          const st = String(aa?.status || "").trim();
          if (!["approved", "rejected", "pending"].includes(st)) errors.push(`Invalid apply-approval status '${st}' (${applyApprovalLabel}).`);
          const mode = String(aa?.mode || "").trim();
          if (!["auto", "manual"].includes(mode)) errors.push(`Invalid apply-approval mode '${mode}' (${applyApprovalLabel}).`);
          const bh = typeof aa?.bundle_hash === "string" ? aa.bundle_hash.trim() : "";
          if (!bh) errors.push(`apply-approval missing bundle_hash (${applyApprovalLabel}).`);
          if (stagesRequiringApplyApprovalApproved.has(currentStage) && st !== "approved") {
            errors.push(`Stage ${currentStage} requires apply-approval approved; got status='${st}' (${applyApprovalLabel}).`);
          }
        } catch {
          errors.push(`Invalid JSON in ${applyApprovalLabel}`);
        }
      }

      // PR.json (must exist once PR is open)
      const prPath = `ai/lane_b/work/${wid}/PR.json`;
      const prText = await readTextIfExists(prPath);
      if (!prText && stagesRequiringPr.has(currentStage)) errors.push(`Missing ${prPath} (PR must be created before stage ${currentStage}).`);
      let pr = null;
      if (prText) {
        try {
          pr = JSON.parse(prText);
        } catch {
          errors.push(`Invalid JSON in ${prPath}`);
          pr = null;
        }
        if (pr) {
          if (pr?.version !== 1) errors.push(`Invalid PR.json (expected version=1): ${prPath}`);
          if (String(pr?.workId || "").trim() !== wid) errors.push(`PR.json workId mismatch: ${prPath}`);
          const owner = typeof pr?.owner === "string" ? pr.owner.trim() : "";
          const repo = typeof pr?.repo === "string" ? pr.repo.trim() : "";
          const num = typeof pr?.pr_number === "number" ? pr.pr_number : Number.parseInt(String(pr?.pr_number || "").trim(), 10);
          const url = typeof pr?.url === "string" ? pr.url.trim() : "";
          if (!owner || !repo) errors.push(`PR.json missing owner/repo (${prPath}).`);
          if (!Number.isFinite(num) || num <= 0) errors.push(`PR.json missing/invalid pr_number (${prPath}).`);
          if (!url) errors.push(`PR.json missing url (${prPath}).`);
          if (typeof pr?.head_branch !== "string" || !pr.head_branch.trim()) errors.push(`PR.json missing head_branch (${prPath}).`);
          if (typeof pr?.base_branch !== "string" || !pr.base_branch.trim()) errors.push(`PR.json missing base_branch (${prPath}).`);
        }
      }

      // CI artifacts (work-scoped only)
      const ciStatusPath = `ai/lane_b/work/${wid}/CI/CI_Status.json`;
      const ciText = await readTextIfExists(ciStatusPath);
      if (!ciText && stagesRequiringCi.has(currentStage)) errors.push(`Missing CI status snapshot: ${ciStatusPath} (run: node src/cli.js --ci-update --workId ${wid}).`);
      if (ciText) {
        let ci = null;
        try {
          ci = JSON.parse(ciText);
        } catch {
          errors.push(`Invalid JSON in ${ciStatusPath}`);
          ci = null;
        }
        if (ci) {
          if (ci?.version !== 1) errors.push(`Invalid CI/CI_Status.json (expected version=1): ${ciStatusPath}`);
          if (String(ci?.workId || "").trim() !== wid) errors.push(`CI/CI_Status.json workId mismatch: ${ciStatusPath}`);
          const overall = String(ci?.overall || "").trim().toLowerCase();
          if (!["pending", "failed", "success"].includes(overall)) errors.push(`CI/CI_Status.json invalid overall='${overall}' (${ciStatusPath}).`);
          const captured = typeof ci?.captured_at === "string" ? ci.captured_at.trim() : "";
          if (!captured) errors.push(`CI/CI_Status.json missing captured_at (${ciStatusPath}).`);
          const prn = typeof ci?.pr_number === "number" ? ci.pr_number : Number.parseInt(String(ci?.pr_number || "").trim(), 10);
          if (!Number.isFinite(prn) || prn <= 0) errors.push(`CI/CI_Status.json missing/invalid pr_number (${ciStatusPath}).`);
          if (pr && Number.isFinite(prn) && Number.isFinite(pr?.pr_number) && prn !== pr.pr_number) errors.push(`CI/CI_Status.json pr_number mismatch vs PR.json (${ciStatusPath}).`);

          const checks = Array.isArray(ci?.checks) ? ci.checks : null;
          if (!checks) errors.push(`CI/CI_Status.json checks must be an array (${ciStatusPath}).`);
          const latest = typeof ci?.latest_feedback === "string" ? ci.latest_feedback.trim() : "";
          if (overall === "failed" && !latest) errors.push(`CI/CI_Status.json missing latest_feedback for overall=failed (${ciStatusPath}).`);
          if (overall !== "failed" && latest) errors.push(`CI/CI_Status.json latest_feedback must be null/empty unless overall=failed (${ciStatusPath}).`);
          if (overall === "failed" && latest) {
            const fjPath = `ai/lane_b/work/${wid}/CI/${latest}.json`;
            const fmPath = `ai/lane_b/work/${wid}/CI/${latest}.md`;
            const fj = await readTextIfExists(fjPath);
            const fm = await readTextIfExists(fmPath);
            if (!fj) errors.push(`Missing CI feedback json: ${fjPath}`);
            else {
              try {
                const fobj = JSON.parse(fj);
                const fsid = typeof fobj?.snapshot_id === "string" ? fobj.snapshot_id.trim() : "";
                if (!fsid) errors.push(`CI feedback missing snapshot_id (${fjPath}).`);
              } catch {
                errors.push(`Invalid JSON in ${fjPath}`);
              }
            }
            if (!fm) errors.push(`Missing CI feedback markdown: ${fmPath}`);
          }

          const failing = (Array.isArray(checks) ? checks : []).filter((c) => String(c?.conclusion || "").trim().toLowerCase() === "failure");
          if ((currentStage === "CI_GREEN" || stagesRequiringMergeApproval.has(currentStage)) && (overall !== "success" || failing.length > 0)) {
            errors.push(`Stage ${currentStage} requires CI green; got overall='${overall}', failing_checks=${failing.length} (${ciStatusPath}).`);
          }
        }

        const histPath = `ai/lane_b/work/${wid}/CI/CI_Status_History.json`;
        const histText = await readTextIfExists(histPath);
        if (histText) {
          try {
            const arr = JSON.parse(histText);
            if (!Array.isArray(arr)) errors.push(`CI/CI_Status_History.json must be a JSON array (${histPath}).`);
          } catch {
            errors.push(`Invalid JSON in ${histPath}`);
          }
        }
      }

      // Merge approval
      const mergeApprovalPath = `ai/lane_b/work/${wid}/MERGE_APPROVAL.json`;
      const legacyGateBPath = `ai/lane_b/work/${wid}/GATE_B.json`;
      const mergeApprovalText = (await readTextIfExists(mergeApprovalPath)) || (await readTextIfExists(legacyGateBPath));
      const mergeApprovalLabel = (await readTextIfExists(mergeApprovalPath)) ? mergeApprovalPath : legacyGateBPath;
      if (!mergeApprovalText && stagesRequiringMergeApproval.has(currentStage)) {
        errors.push(`Missing merge-approval artifact: ${mergeApprovalPath} (run: node src/cli.js --merge-approval --workId ${wid})`);
      }
      if (mergeApprovalText) {
        try {
          const ma = JSON.parse(mergeApprovalText);
          if (ma?.version !== 1) errors.push(`Invalid merge-approval JSON (expected version=1): ${mergeApprovalLabel}`);
          if (String(ma?.workId || "").trim() !== wid) errors.push(`merge-approval workId mismatch: ${mergeApprovalLabel}`);
          const st = String(ma?.status || "").trim();
          if (!["approved", "rejected", "pending"].includes(st)) errors.push(`Invalid merge-approval status '${st}' (${mergeApprovalLabel}).`);
          const mode = String(ma?.mode || "").trim();
          if (!["manual"].includes(mode)) errors.push(`Invalid merge-approval mode '${mode}' (${mergeApprovalLabel}).`);
          const bh = typeof ma?.bundle_hash === "string" ? ma.bundle_hash.trim() : "";
          if (!bh) errors.push(`merge-approval missing bundle_hash (${mergeApprovalLabel}).`);
          if (stagesRequiringMergeApprovalApproved.has(currentStage) && st !== "approved") errors.push(`Stage ${currentStage} requires merge-approval approved; got status='${st}' (${mergeApprovalLabel}).`);
        } catch {
          errors.push(`Invalid JSON in ${mergeApprovalLabel}`);
        }
      }
    }

	    return { ok: errors.length === 0, errors };
	  }

  async dryRun({ intakeText, intakeSource }) {
    const timestamp = nowTs();
    const workId = makeWorkId({ timestamp, seed: String(intakeText || "") });
    const workDir = `ai/lane_b/work/${workId}`;
    const intakePath = `${workDir}/INTAKE.md`;

    let routing = {
      workId,
      timestamp,
      intake: { source: intakeSource, summary: oneLineSummary(intakeText) },
      matches: [],
      selected_teams: [],
      routing_confidence: 0,
      high_risk_detected: detectHighRisk(intakeText),
      needs_confirmation: true,
      reason: "Missing or invalid teams config (config/TEAMS.json preferred, else config/teams.json).",
      proposed_question: `Routing confirmation required for ${workId}: teams config missing or invalid.`,
      proposed_options: ["A: Fix config/TEAMS.json (or config/teams.json) and rerun.", "B: Escalate to Architect."],
    };

    const teamsText = await readTeamsConfigText();
    if (teamsText) {
      try {
        const cfg = JSON.parse(teamsText);
        const teams = Array.isArray(cfg?.teams) ? cfg.teams : [];
        routing = routeTeams({ workId, timestamp, intakeText, intakeSource, teams });
        routing = await augmentRoutingWithRepos({ routing, intakeText });
      } catch {
        // keep default
      }
    }

    return {
      mode: "dry-run",
      actions: expectedActions({ workId, timestamp }),
      preview: {
        "ai/lane_b/work/<id>/INTAKE.md": renderWorkIntake({ intakeText }),
        "ai/lane_b/work/<id>/PLAN.md": "(generated normalized intent PLAN.md)",
        "ai/lane_b/work/<id>/STATUS.md": "(generated lifecycle STATUS.md)",
        "ai/lane_b/work/<id>/ROUTING.json": renderWorkRouting({ routing }),
        "ai/lane_b/PORTFOLIO.md": "(generated portfolio index)",
        "ai/lane_b/STATUS.md": "(generated global status summary from PORTFOLIO.md + per-work STATUS.md)",
        "ai/lane_b/DECISIONS_NEEDED.md": renderGlobalDecisions({ workId, routing }),
      },
    };
  }

  async run({ intakeText, intakeSource, decisionsMode = "replace" }) {
    const timestamp = nowTs();
    const workId = makeWorkId({ timestamp, seed: String(intakeText || "") });
    const workDir = `ai/lane_b/work/${workId}`;
    const intakePath = `${workDir}/INTAKE.md`;

    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, workId, action: "orchestrator_cycle_start" }) + "\n");

    try {
      for (const dir of REQUIRED_PROJECT_DIRS) {
        await ensureDir(dir);
      }
		      await ensureDir(workDir);
		      await ensureWorkMeta({ workId, createdAtIso: timestamp });

	      await writeText(intakePath, renderWorkIntake({ intakeText }));
	      await writeWorkPlan({ workId, intakeMd: await readTextIfExists(intakePath), routing: null, bundle: null });
      await updateWorkStatus({
        workId,
        stage: "INTAKE_RECEIVED",
        blocked: false,
        artifacts: {
          intake_md: intakePath,
          plan_md: `${workDir}/PLAN.md`,
          routing_json: `${workDir}/ROUTING.json`,
          tasks_dir: `${workDir}/tasks/`,
          proposals_dir: `${workDir}/proposals/`,
          patch_plans_dir: `${workDir}/patch-plans/`,
          bundle_json: `${workDir}/BUNDLE.json`,
          plan_approval_json: `${workDir}/PLAN_APPROVAL.json`,
          plan_approval_md: `${workDir}/PLAN_APPROVAL.md`,
          apply_status_json: `${workDir}/status.json`,
          decisions_md: "ai/lane_b/DECISIONS_NEEDED.md",
        },
        note: `Intake source: ${intakeSource}`,
      });

      const teamsText = await readTeamsConfigText();
      if (!teamsText) throw new Error("Missing teams config (config/TEAMS.json preferred, else config/teams.json)");
      const cfg = JSON.parse(teamsText);
      const teams = Array.isArray(cfg?.teams) ? cfg.teams : [];
	      let routing = routeTeams({ workId, timestamp, intakeText, intakeSource, teams });
	      routing = await augmentRoutingWithRepos({ routing, intakeText });
	      await updateWorkMetaFromRouting({ workId, routing });
	      await writeText(`${workDir}/ROUTING.json`, renderWorkRouting({ routing }));
	      await writeWorkPlan({ workId, intakeMd: await readTextIfExists(intakePath), routing, bundle: null });
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({
          timestamp: nowISO(),
          action: "routing_resolved",
          workId,
          mode: routing.routing_mode || "keyword_fallback",
          selected_teams: Array.isArray(routing.selected_teams) ? routing.selected_teams : [],
          selected_repos: Array.isArray(routing.selected_repos) ? routing.selected_repos : [],
          target_branch: routing.target_branch || null,
        }) + "\n",
      );

      const stage = routing.needs_confirmation ? "BLOCKED" : "ROUTED";
      await updateWorkStatus({
        workId,
        stage,
        blocked: routing.needs_confirmation,
        blockingReason: routing.needs_confirmation ? routing.reason || routing.proposed_question || "Blocked: routing confirmation required." : null,
        artifacts: {
          routing_json: `${workDir}/ROUTING.json`,
          decisions_md: "ai/lane_b/DECISIONS_NEEDED.md",
        },
        note: routing.routing_mode ? `routing_mode=${routing.routing_mode}` : null,
      });
      if (decisionsMode === "replace") {
        await writeText("ai/lane_b/DECISIONS_NEEDED.md", renderGlobalDecisions({ workId, routing }));
      }
      await this.writePortfolio();
      await writeGlobalStatusFromPortfolio();

      const updated = [
        `${workDir}/INTAKE.md`,
        `${workDir}/PLAN.md`,
        `${workDir}/STATUS.md`,
        `${workDir}/ROUTING.json`,
        "ai/lane_b/STATUS.md",
        "ai/lane_b/PORTFOLIO.md",
        "ai/lane_b/ledger.jsonl",
      ];
      if (decisionsMode === "replace") updated.push("ai/lane_b/DECISIONS_NEEDED.md");

      return {
        mode: "run",
        workId,
        workDir,
        updated,
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      try {
        await updateWorkStatus({
          workId,
          stage: "FAILED",
          blocked: false,
          blockingReason: null,
          artifacts: { errors: [`Failed during orchestration cycle: ${message}`] },
          note: message,
        });
        await this.writePortfolio();
        await writeGlobalStatusFromPortfolio();
      } catch {
        // Fall back to prior global status writer (best-effort).
        await writeText("ai/lane_b/STATUS.md", renderGlobalStatus({ workId, timestamp, outcome: "failed", summary: `Failed during orchestration cycle (${message}).` }));
      }
      throw err;
    }
  }

  async resolveDecision({ choice, workId }) {
    const timestamp = nowISO();
    const decisionText = await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md");
    if (!decisionText) return { ok: false, message: "ai/lane_b/DECISIONS_NEEDED.md not found." };

    const allPending = parsePendingDecisions(decisionText);
    if (!allPending.length) return { ok: false, message: "No pending decision to resolve." };

    const trimmedWorkId = typeof workId === "string" && workId.trim() ? workId.trim() : null;

    let targetWorkId = trimmedWorkId;
    if (!targetWorkId) {
      if (allPending.length === 1) {
        targetWorkId = allPending[0].workId;
      } else {
        const pendingIds = allPending.map((d) => d.workId).join(", ");
        return { ok: false, message: `Multiple pending decisions; specify --workId. Pending workIds: ${pendingIds}` };
      }
    }

    const idx = allPending.findIndex((d) => d.workId === targetWorkId);
    if (idx === -1) {
      const pendingIds = allPending.map((d) => d.workId).join(", ");
      return { ok: false, message: `No pending decision found for workId=${targetWorkId}. Pending workIds: ${pendingIds}` };
    }

    const parsed = allPending[idx];
    const remainingRaw = allPending.filter((_, i) => i !== idx);
    const remaining = await ensureDecisionIntakeSummaries(remainingRaw);

    const { workId: decisionWorkId, question, options } = parsed;
    const chosenText = choice === "A" ? options.A : options.B;

    await ensureDir("ai/reports");
    const decisionsReportPath = `ai/reports/decisions-${todayISO()}.md`;
    const existing = await readTextIfExists(decisionsReportPath);
    if (!existing) {
      await writeText(decisionsReportPath, `# Decisions ${todayISO()}\n\n`);
    }

    await appendFile(
      decisionsReportPath,
      [
        `## ${timestamp}`,
        "",
        `- Work item: ${decisionWorkId}`,
        `- Question: ${question}`,
        `- Choice: ${choice}: ${chosenText}`,
        "",
      ].join("\n"),
    );

    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, action: "decision_resolved", workId: decisionWorkId, choice }) + "\n");

    const workDir = `ai/lane_b/work/${decisionWorkId}`;
    const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
    const routingJson = await readTextIfExists(`${workDir}/ROUTING.json`);

    if (choice === "A") {
      const taggedPath = `${workDir}/INTAKE_TAGGED.md`;
      const existingTagged = await readTextIfExists(taggedPath);
      if (!existingTagged) {
        const originalIntake = stripIntakeMarkdown(intakeMd || "");
        await writeText(
          taggedPath,
          [
            "BoundedContext:",
            "SecurityBoundary:",
            "Notes:",
            `OriginalIntake: ${originalIntake}`,
            "",
          ].join("\n"),
        );
      }

      const planText = await readTextIfExists("ai/PLAN.md");
      if (planText) {
        await writeText("ai/PLAN.md", updatePlanNextForRerun(planText, taggedPath));
      }

      await writeText("ai/lane_b/DECISIONS_NEEDED.md", renderPendingDecisions(remaining));

      return {
        ok: true,
        workId: decisionWorkId,
        choice,
        decisionReport: decisionsReportPath,
        created: existingTagged ? [] : [taggedPath],
      };
    }

    // choice === "B"
    const escalationPath = `${workDir}/ESCALATION.md`;
    let routing = null;
    if (routingJson) {
      try {
        routing = JSON.parse(routingJson);
      } catch {
        routing = null;
      }
    }

    const originalIntake = stripIntakeMarkdown(intakeMd || "");
    const matchedTeams = routing?.matches ? routing.matches.map((m) => `- ${m.team_id}: ${m.matched_hints.join(", ")}`).join("\n") : "- (unavailable)";
    const whyHighRisk = routing?.high_risk_detected ? "high_risk_detected=true" : "high_risk_detected=false";
    const decisionNeeded = [question, "", `- A: ${options.A}`, `- B: ${options.B}`].join("\n");

    await writeText(
      escalationPath,
      [
        "# ESCALATION",
        "",
        `Work item: ${decisionWorkId}`,
        `Timestamp: ${timestamp}`,
        "",
        "## Original intake",
        "",
        originalIntake || "(missing INTAKE.md)",
        "",
        "## Matched teams",
        "",
        matchedTeams,
        "",
        "## High-risk rationale",
        "",
        whyHighRisk,
        routing?.reason ? `Reason: ${routing.reason}` : "",
        "",
        "## Decision needed",
        "",
        decisionNeeded,
        "",
      ]
        .join("\n"),
    );

    await updateWorkStatus({
      workId: decisionWorkId,
      stage: "BLOCKED",
      blocked: true,
      blockingReason: "Escalated to Architect.",
      artifacts: { escalation_md: escalationPath, decisions_md: "ai/lane_b/DECISIONS_NEEDED.md" },
      note: "escalation requested",
    });
    await this.writePortfolio();
    await writeGlobalStatusFromPortfolio();

    await writeText("ai/lane_b/DECISIONS_NEEDED.md", renderPendingDecisions(remaining));

    return {
      ok: true,
      workId: decisionWorkId,
      choice,
      decisionReport: decisionsReportPath,
      created: [escalationPath],
    };
  }

  async propose({ workId, teams, withPatchPlans = false }) {
    if (withPatchPlans) {
      const { runProposeBundle } = await import("./agents/propose-bundle-runner.js");
      return await runProposeBundle({ repoRoot: this.repoRoot, workId, teamsCsv: teams });
    }
    const { runProposals } = await import("./agents/agent-runner.js");
    return await runProposals({ repoRoot: this.repoRoot, workId, teamsCsv: teams });
  }

  async qa({ workId, teams = null, limit = null }) {
    const workDir = `ai/lane_b/work/${workId}`;
    const ok = await isDirectory(workDir);
    if (!ok) return { ok: false, message: `Work item not found: ${workDir}` };

    const { runQaInspector } = await import("./agents/qa-inspector-runner.js");
    const qaRes = await runQaInspector({ repoRoot: this.repoRoot, workId, teamsCsv: teams, limit });
    if (!qaRes.ok) return qaRes;

    // Rebuild bundle so QA artifacts are pinned by hash (approval gates the updated bundle).
    const { writeWorkBundle } = await import("../bundle/bundle-builder.js");
    const bundleRes = await writeWorkBundle({ workId });
    if (!bundleRes.ok) return bundleRes;

    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({ timestamp: nowISO(), action: "qa_bundled", workId, bundle_path: bundleRes.bundle_path, bundle_hash: bundleRes.bundle_hash }) + "\n",
    );
    return { ok: true, workId, qa: qaRes, bundle: { path: bundleRes.bundle_path, hash: bundleRes.bundle_hash } };
  }

  async review({ workId, teams }) {
    const { runReview } = await import("./review-runner.js");
    return await runReview({ repoRoot: this.repoRoot, workId, teamsCsv: teams });
  }

  async approvalStatus({ workId }) {
    const workDir = `ai/lane_b/work/${workId}`;
    const ok = await isDirectory(workDir);
    if (!ok) return { ok: false, message: `Work item not found: ${workDir}` };

    const approval = await readApprovalIfExists(workId);
    const jsonPath = await approvalJsonPath(workId);
    const mdPath = await approvalMdPath(workId);

    if (!approval) {
      return { ok: true, workId, status: "missing", plan_approval_json: jsonPath, plan_approval_md: mdPath };
    }
    if (approval._invalid) {
      return { ok: true, workId, status: "invalid", plan_approval_json: jsonPath, plan_approval_md: mdPath };
    }

    return {
      ok: true,
      workId,
      status: approval.status,
      mode: approval.mode || null,
      bundle_hash: approval.bundle_hash || null,
      approved_at: approval.approved_at || null,
      approved_by: approval.approved_by || null,
      teams: approval?.scope?.teams || null,
      repos: approval?.scope?.repos || null,
      review_required: !!approval.review_required,
      review_seen: !!approval.review_seen,
      plan_approval_json: jsonPath,
      plan_approval_md: mdPath,
    };
  }

  async approvalGate({ workId }) {
    const workDir = `ai/lane_b/work/${workId}`;
    const ok = await isDirectory(workDir);
    if (!ok) return { ok: false, message: `Work item not found: ${workDir}` };
    const proposalFailed = await readTextIfExists(`${workDir}/PROPOSAL_FAILED.json`);
    if (proposalFailed) return { ok: false, message: `Cannot approve: proposal phase FAILED (see ${workDir}/PROPOSAL_FAILED.json).` };
    const statusText = await readTextIfExists(`${workDir}/STATUS.md`);
    if (statusText && statusText.includes('"blocking_reason": "PATCH_PLAN_INVALID"')) {
      return { ok: false, message: `Cannot approve: patch plan validation FAILED (see ${workDir}/failure-reports/patch-plan-validation.md).` };
    }

    const bundleRes = await readBundleForWork(workId);
    if (!bundleRes.ok) return { ok: false, message: bundleRes.message, bundle_path: bundleRes.path };
    const bundle = bundleRes.bundle;

    const bundleValidation = await validateBundleInputsAndHash(bundle);
    if (!bundleValidation.ok) {
      return { ok: false, message: "Cannot approve: bundle inputs/hash invalid.", errors: bundleValidation.errors, bundle_path: bundleRes.path };
    }
	    {
	      const inputs = bundle && typeof bundle === "object" ? bundle.inputs : null;
	      const pinnedProposals = new Set(
	        (inputs && Array.isArray(inputs.proposals) ? inputs.proposals : [])
	          .map((it) => (typeof it?.path === "string" ? it.path.trim() : ""))
	          .filter(Boolean),
	      );
	      const pinnedPlans = new Set(
	        (inputs && Array.isArray(inputs.patch_plan_jsons) ? inputs.patch_plan_jsons : [])
	          .map((it) => (typeof it?.path === "string" ? it.path.trim() : ""))
	          .filter(Boolean),
	      );
	      const pinnedQa = new Set(
	        (inputs && Array.isArray(inputs.qa_plan_jsons) ? inputs.qa_plan_jsons : [])
	          .map((it) => (typeof it?.path === "string" ? it.path.trim() : ""))
	          .filter(Boolean),
	      );

	      for (const r of Array.isArray(bundle?.repos) ? bundle.repos : []) {
	        const pp = typeof r?.proposal_path === "string" ? r.proposal_path.trim() : "";
	        const plan = typeof r?.patch_plan_json_path === "string" ? r.patch_plan_json_path.trim() : "";
	        const qa = typeof r?.qa_plan_json_path === "string" ? r.qa_plan_json_path.trim() : "";
	        if (pinnedProposals.size && pp && !pinnedProposals.has(pp)) {
	          return { ok: false, message: `Cannot approve: bundle proposal_path not pinned in inputs (${pp}).`, bundle_path: bundleRes.path };
	        }
	        if (pinnedPlans.size && plan && !pinnedPlans.has(plan)) {
	          return { ok: false, message: `Cannot approve: bundle patch_plan_json_path not pinned in inputs (${plan}).`, bundle_path: bundleRes.path };
	        }
	        if (pinnedQa.size && qa && !pinnedQa.has(qa)) {
	          return { ok: false, message: `Cannot approve: bundle qa_plan_json_path not pinned in inputs (${qa}).`, bundle_path: bundleRes.path };
	        }
	      }
	    }

    const policiesLoaded = await loadPolicies();
    if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };
    const policies = policiesLoaded.policies;

    const reposLoaded = await loadRepoRegistry();
    if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message };
    const registry = reposLoaded.registry;
    const byRepoId = new Map((registry.repos || []).map((r) => [String(r.repo_id || "").trim(), r]));

    let teamsCfg = null;
    {
      const teamsText = await readTeamsConfigText();
      if (teamsText) {
        try {
          teamsCfg = JSON.parse(teamsText);
        } catch {
          teamsCfg = null;
        }
      }
    }
    const teamRisk = new Map((teamsCfg?.teams || []).map((t) => [String(t?.team_id || ""), String(t?.risk_level || "")]));

    const applyPolicy = isPlainObject(policies?.apply) ? policies.apply : {};
    const allowInstructionMode = !!applyPolicy.allow_instruction_mode;
    const requireClean = cleanPatchPlanRequiredByPolicy(policies);

	    const bundleRepos = [];
	    const patchPlanErrors = [];
	    for (const r of bundle.repos.slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)))) {
	      const repoId = String(r.repo_id || "").trim();
	      const planPath = String(r.patch_plan_json_path || "").trim();
	      const qaPath = String(r.qa_plan_json_path || "").trim();
	      const proposalPath = String(r.proposal_path || "").trim();
	      const proposalSha = typeof r.proposal_sha256 === "string" ? r.proposal_sha256.trim() : "";
	      if (!repoId || !planPath) {
	        patchPlanErrors.push(`Invalid bundle entry: repo_id/patch_plan_json_path missing.`);
	        continue;
	      }
	      if (!proposalPath || !proposalSha) {
	        patchPlanErrors.push(`Invalid bundle entry: proposal_path/proposal_sha256 missing for ${repoId}.`);
	        continue;
	      }

	      // Proposal must exist and be SUCCESS; patch plan provenance must match proposal pins.
	      const proposalText = await readTextIfExists(proposalPath);
	      if (!proposalText) {
	        patchPlanErrors.push(`Missing proposal JSON: ${proposalPath}`);
	        continue;
	      }
	      const computedProposalSha = sha256Hex(proposalText);
	      if (computedProposalSha !== proposalSha) {
	        patchPlanErrors.push(`Proposal sha mismatch for ${proposalPath} (expected ${proposalSha}, computed ${computedProposalSha}).`);
	        continue;
	      }
	      let proposalJson = null;
	      try {
	        proposalJson = JSON.parse(proposalText);
	      } catch {
	        patchPlanErrors.push(`Invalid JSON in proposal: ${proposalPath}`);
	        continue;
	      }
	      if (proposalJson?.status !== "SUCCESS") {
	        patchPlanErrors.push(`Proposal is not SUCCESS for ${repoId} (${proposalPath}).`);
	        continue;
	      }
	      const proposalAgentId = typeof proposalJson?.agent_id === "string" ? proposalJson.agent_id.trim() : "";
	      if (!proposalAgentId) {
	        patchPlanErrors.push(`Proposal agent_id missing for ${repoId} (${proposalPath}).`);
	        continue;
	      }
	      const planText = await readTextIfExists(planPath);
	      if (!planText) {
	        patchPlanErrors.push(`Missing patch plan JSON: ${planPath}`);
	        continue;
      }
      let planJson;
      try {
        planJson = JSON.parse(planText);
      } catch {
        patchPlanErrors.push(`Invalid JSON in patch plan: ${planPath}`);
        continue;
      }

	      const v = validatePatchPlan(planJson, { policy: policies, expected_proposal_hash: proposalSha, expected_proposal_agent_id: proposalAgentId });
	      if (!v.ok) {
	        patchPlanErrors.push(`Patch plan validator failed for ${repoId}: ${v.errors.join(" | ")}`);
	        continue;
	      }

	      if (!qaPath) {
	        patchPlanErrors.push(`Missing qa_plan_json_path for ${repoId}.`);
	        continue;
	      }
	      const qaText = await readTextIfExists(qaPath);
	      if (!qaText) {
	        patchPlanErrors.push(`Missing QA plan JSON: ${qaPath}`);
	        continue;
	      }
	      let qaJson = null;
	      try {
	        qaJson = JSON.parse(qaText);
	      } catch {
	        patchPlanErrors.push(`Invalid JSON in QA plan: ${qaPath}`);
	        continue;
	      }
	      const qv = validateQaPlan(qaJson, { expectedWorkId: workId, expectedRepoId: repoId });
	      if (!qv.ok) {
	        patchPlanErrors.push(`QA plan validator failed for ${repoId}: ${qv.errors.join(" | ")}`);
	        continue;
	      }
	      if (qaJson?.derived_from?.patch_plan_sha256 && qaJson.derived_from.patch_plan_sha256 !== sha256Hex(planText)) {
	        patchPlanErrors.push(`QA plan derived_from.patch_plan_sha256 mismatch for ${repoId}.`);
	        continue;
	      }

	      const repo = byRepoId.get(repoId) || null;
	      if (!repo) {
	        patchPlanErrors.push(`Repo not found in registry: ${repoId}`);
	        continue;
	      }
	      const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repo.path });
	      if (repoAbs) {
	        const expectedRepoPath = registryRepoPathForPlanCompare({ baseDir: registry.base_dir, repoPath: repo.path, repoAbs });
	        const planRepoPath = normalizeRepoRelPathForCompare(v.normalized.repo_path);
	        if (planRepoPath !== expectedRepoPath) {
	          patchPlanErrors.push(`Patch plan repo_path mismatch for ${repoId} (plan=${v.normalized.repo_path}, registry_path=${String(repo.path || "")}).`);
	        }
	      }
      if (String(v.normalized.team_id || "") !== String(repo.team_id || "")) patchPlanErrors.push(`Patch plan team_id mismatch for ${repoId}.`);
      if (String(v.normalized.kind || "") !== String(repo.Kind || repo.kind || "")) patchPlanErrors.push(`Patch plan kind mismatch for ${repoId}.`);
      if (!!v.normalized.is_hexa !== !!repo.IsHexa) patchPlanErrors.push(`Patch plan is_hexa mismatch for ${repoId}.`);

      const cleanOk =
        Array.isArray(v.normalized?.scope?.allowed_paths) &&
        v.normalized.scope.allowed_paths.length > 0 &&
        Array.isArray(v.normalized?.edits) &&
        v.normalized.edits.length > 0 &&
        isPlainObject(v.normalized?.risk) &&
        typeof v.normalized.risk.level === "string";

      if (requireClean && !cleanOk) {
        patchPlanErrors.push(`Patch plan is not clean for ${repoId} (requires non-empty allowed_paths + edits + risk.level).`);
      }

      const riskLevel = teamRisk.get(String(repo.team_id || "")) || "unknown";
      bundleRepos.push({
        repo_id: repoId,
        team_id: String(repo.team_id || "") || null,
        kind: String(repo.Kind || repo.kind || "") || null,
        is_hexa: !!repo.IsHexa,
        team_risk_level: riskLevel,
        patch_plan_clean: cleanOk,
        auto_disqualifiers: autoApproveDisqualifiersFromPlan(v.normalized),
      });
    }

    if (patchPlanErrors.length) {
      return { ok: false, message: "Bundle patch plans failed validation.", errors: patchPlanErrors, bundle_path: bundleRes.path };
    }

    // If approval already exists, enforce hash pinning and return status without rewriting.
    const existing = await readApprovalIfExists(workId);
    if (existing && !existing._invalid) {
      if (existing.bundle_hash && existing.bundle_hash !== bundle.bundle_hash) {
        return { ok: false, message: "Existing PLAN_APPROVAL.json bundle_hash does not match current BUNDLE.json; run --plan-reset-approval.", bundle_hash: existing.bundle_hash, bundle_path: bundleRes.path };
      }
      return {
        ok: true,
        workId,
        status: existing.status,
        mode: existing.mode || null,
        bundle_hash: existing.bundle_hash || null,
        plan_approval_json: await approvalJsonPath(workId),
        plan_approval_md: await approvalMdPath(workId),
      };
    }

    // SSOT drift check is mandatory before approval request generation (deterministic, code-only).
    const driftPath = `${workDir}/SSOT_DRIFT.json`;
    let drift = null;
    if (!(await pathExists(driftPath))) {
      const { runSsotDriftCheck } = await import("../ssot/ssot-drift-check.js");
      const driftRes = await runSsotDriftCheck({ workId });
      if (!driftRes.ok) return { ok: false, message: `Cannot approve: ssot-drift-check failed: ${driftRes.message}` };
    }
    {
      const driftText = await readTextIfExists(driftPath);
      if (!driftText) return { ok: false, message: `Cannot approve: missing ${driftPath} after ssot-drift-check.` };
      try {
        drift = JSON.parse(driftText);
      } catch {
        return { ok: false, message: `Cannot approve: invalid JSON in ${driftPath}.` };
      }
    }
    const hardViolations = Array.isArray(drift?.hard_violations) ? drift.hard_violations : [];
    const softDeviations = Array.isArray(drift?.soft_deviations) ? drift.soft_deviations : [];

    const auto = getAutoApprovePolicy(policies);
    const disallowedTeams = new Set(["IdentitySecurity", "Tooling"]);
    const autoErrors = [];

    if (!auto.enabled) autoErrors.push("Auto-approve disabled by policy.");
    if (hardViolations.length) autoErrors.push(`SSOT drift hard_violations present: ${hardViolations.length} (see ${driftPath}).`);
    for (const r of bundleRepos) {
      if (disallowedTeams.has(String(r.team_id))) autoErrors.push(`Disallowed team in bundle: ${r.team_id} (${r.repo_id}).`);
      if (auto.allowed_teams.length && !auto.allowed_teams.includes(String(r.team_id))) autoErrors.push(`Team not allowed by policy: ${r.team_id} (${r.repo_id}).`);
      if (auto.allowed_kinds.length && !auto.allowed_kinds.includes(String(r.kind))) autoErrors.push(`Kind not allowed by policy: ${r.kind} (${r.repo_id}).`);
      if (!r.team_risk_level || r.team_risk_level === "unknown") autoErrors.push(`Team risk_level unknown: ${r.team_id} (${r.repo_id}).`);
      if (auto.disallowed_risk_levels.includes(String(r.team_risk_level))) autoErrors.push(`Team risk_level disallowed: ${r.team_id}=${r.team_risk_level} (${r.repo_id}).`);
      if (auto.require_clean_patch_plan && !r.patch_plan_clean) autoErrors.push(`Patch plan not clean: ${r.repo_id}.`);
      for (const d of Array.isArray(r.auto_disqualifiers) ? r.auto_disqualifiers : []) {
        autoErrors.push(`Auto-approve disqualified (${r.repo_id}): ${d}`);
      }
    }

    const mode = auto.enabled && autoErrors.length === 0 ? "auto" : "manual";
    const status = mode === "auto" ? "approved" : "pending";

    const approvedAt = status === "approved" ? nowISO() : null;
    const approvedBy = status === "approved" ? "auto" : String(process.env.APPROVER_LABEL || "human").trim() || "human";

    const reviewRequired = boolFromEnv("REVIEW_BEFORE_APPROVAL", false) || softDeviations.length > 0 || hardViolations.length > 0;
    const reviewPath = `ai/lane_b/work/${workId}/reviews/architect-review.md`;
    const reviewExists = !!(await readTextIfExists(reviewPath));

    const scopeTeams = Array.from(new Set(bundleRepos.map((r) => String(r.team_id)).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    const scopeRepoIds = bundleRepos.map((r) => r.repo_id).slice().sort((a, b) => a.localeCompare(b));

    const approval = {
      workId,
      status,
      mode,
      bundle_hash: bundle.bundle_hash,
      approved_at: approvedAt,
      approved_by: approvedBy,
      scope: { teams: scopeTeams, repos: scopeRepoIds },
      notes:
        status === "approved"
          ? "Auto-approved bundle."
          : softDeviations.length || hardViolations.length
            ? [
                `SSOT drift detected (hard=${hardViolations.length}, soft=${softDeviations.length}). See ${driftPath}.`,
                ...(hardViolations.length
                  ? [
                      "",
                      "Hard violations:",
                      ...hardViolations
                        .slice()
                        .sort((a, b) => `${String(a?.rule_id || "")}:${String(a?.evidence || "")}`.localeCompare(`${String(b?.rule_id || "")}:${String(b?.evidence || "")}`))
                        .slice(0, 20)
                        .map((v) => `- ${String(v?.rule_id || "")}: ${String(v?.evidence || "")}`),
                    ]
                  : []),
                ...(softDeviations.length
                  ? [
                      "",
                      "Soft deviations:",
                      ...softDeviations
                        .slice()
                        .sort((a, b) => `${String(a?.rule_id || "")}:${String(a?.evidence || "")}`.localeCompare(`${String(b?.rule_id || "")}:${String(b?.evidence || "")}`))
                        .slice(0, 20)
                        .map((v) => `- ${String(v?.rule_id || "")}: ${String(v?.evidence || "")}`),
                    ]
                  : []),
              ].join("\n")
            : "",
      review_required: reviewRequired,
      review_seen: reviewExists,
    };

    await writeText(await approvalJsonPath(workId), JSON.stringify(approval, null, 2) + "\n");

    // Include proposals pointers for covered teams (best-effort).
    const proposalPathsByTeam = new Map();
    for (const t of scopeTeams) proposalPathsByTeam.set(t, await listTeamProposals({ workId, teamId: t }));
    const mdText = `${renderApprovalMarkdown({ approval, proposalPathsByTeam, reviewPath: reviewExists ? reviewPath : null })}\n${renderBundleSummaryMarkdown(bundle)}`;
    await writeText(await approvalMdPath(workId), mdText);

    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "plan_approval_requested", workId, bundle_hash: bundle.bundle_hash }) + "\n");

    if (status === "approved") {
      await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "plan_approval_auto_granted", workId, bundle_hash: bundle.bundle_hash }) + "\n");
    } else {
      await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "wait_for_manual_approval", workId, bundle_hash: bundle.bundle_hash, reasons: autoErrors }) + "\n");
      // Write a decision note (append-only, best effort).
      const decisionsPath = "ai/lane_b/DECISIONS_NEEDED.md";
      const existingDecisions = (await readTextIfExists(decisionsPath)) || "";
      if (!existingDecisions.includes(`Auto-approve refused for ${workId}`)) {
        const block = [
          "",
          `## Auto-approve refused for ${workId}`,
          "",
          `- bundle_hash: \`${bundle.bundle_hash}\``,
          "",
          "Reasons:",
          "",
          ...autoErrors.map((e) => `- ${e}`),
          "",
        ].join("\n");
        await writeText(decisionsPath, existingDecisions.trimEnd() + "\n" + block);
      }
    }

    await updateWorkStatus({
      workId,
      stage: status === "approved" ? "PLAN_APPROVED" : "PLAN_APPROVAL_REQUESTED",
      blocked: status !== "approved",
      blockingReason: status !== "approved" ? "Plan approval required (manual)." : null,
      artifacts: {
        plan_approval_json: await approvalJsonPath(workId),
        plan_approval_md: await approvalMdPath(workId),
        bundle_json: `ai/lane_b/work/${workId}/BUNDLE.json`,
      },
      note: `plan_approval_mode=${mode}`,
    });
    await this.writePortfolio();
    await writeGlobalStatusFromPortfolio();

    return {
      ok: true,
      workId,
      status,
      mode,
      bundle_hash: bundle.bundle_hash,
      plan_approval_json: await approvalJsonPath(workId),
      plan_approval_md: await approvalMdPath(workId),
    };
  }

  async approve({ workId, teams, notes }) {
    const workDir = `ai/lane_b/work/${workId}`;
    const ok = await isDirectory(workDir);
    if (!ok) return { ok: false, message: `Work item not found: ${workDir}` };
    const proposalFailed = await readTextIfExists(`${workDir}/PROPOSAL_FAILED.json`);
    if (proposalFailed) return { ok: false, message: `Cannot approve: proposal phase FAILED (see ${workDir}/PROPOSAL_FAILED.json).` };
    const statusText = await readTextIfExists(`${workDir}/STATUS.md`);
    if (statusText && statusText.includes('"blocking_reason": "PATCH_PLAN_INVALID"')) {
      return { ok: false, message: `Cannot approve: patch plan validation FAILED (see ${workDir}/failure-reports/patch-plan-validation.md).` };
    }

    const bundleRes = await readBundleForWork(workId);
    if (!bundleRes.ok) return { ok: false, message: bundleRes.message, bundle_path: bundleRes.path };
    const bundle = bundleRes.bundle;

    const policiesLoaded = await loadPolicies();
    if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };
    const policies = policiesLoaded.policies;

    const bundleValidation = await validateBundleInputsAndHash(bundle);
    if (!bundleValidation.ok) {
      return { ok: false, message: "Cannot approve: bundle inputs/hash invalid.", errors: bundleValidation.errors, bundle_path: bundleRes.path };
    }
    {
      const inputs = bundle && typeof bundle === "object" ? bundle.inputs : null;
      const pinnedProposals = new Set(
        (inputs && Array.isArray(inputs.proposals) ? inputs.proposals : [])
          .map((it) => (typeof it?.path === "string" ? it.path.trim() : ""))
          .filter(Boolean),
      );
      const pinnedPlans = new Set(
        (inputs && Array.isArray(inputs.patch_plan_jsons) ? inputs.patch_plan_jsons : [])
          .map((it) => (typeof it?.path === "string" ? it.path.trim() : ""))
          .filter(Boolean),
      );

      for (const r of Array.isArray(bundle?.repos) ? bundle.repos : []) {
        const pp = typeof r?.proposal_path === "string" ? r.proposal_path.trim() : "";
        const plan = typeof r?.patch_plan_json_path === "string" ? r.patch_plan_json_path.trim() : "";
        if (pinnedProposals.size && pp && !pinnedProposals.has(pp)) {
          return { ok: false, message: `Cannot approve: bundle proposal_path not pinned in inputs (${pp}).`, bundle_path: bundleRes.path };
        }
        if (pinnedPlans.size && plan && !pinnedPlans.has(plan)) {
          return { ok: false, message: `Cannot approve: bundle patch_plan_json_path not pinned in inputs (${plan}).`, bundle_path: bundleRes.path };
        }
      }
    }

    const requireClean = cleanPatchPlanRequiredByPolicy(policies);

    const requestedTeams = parseTeamsCsv(teams);
    const defaultTeams = await readRoutingSelectedTeams(workId);
    const scopeTeams = (requestedTeams || defaultTeams).slice().filter(Boolean);
    if (!scopeTeams.length) return { ok: false, message: "Cannot approve: no teams provided and ROUTING.json has no selected_teams." };

    // Gate: proposals must exist for teams being approved
    const proposalPathsByTeam = new Map();
    for (const t of scopeTeams) {
      const paths = await listTeamProposals({ workId, teamId: t });
      proposalPathsByTeam.set(t, paths);
      if (!paths.length) return { ok: false, message: `Cannot approve: missing proposal for team ${t} (expected ai/lane_b/work/${workId}/proposals/${t}__*.md).` };
    }

    // Gate: bundle patch plans must be valid (and clean if required by policy).
    const policiesForValidator = policies;
    for (const r of bundle.repos || []) {
      const planPath = String(r?.patch_plan_json_path || "").trim();
      const planText = await readTextIfExists(planPath);
      if (!planText) return { ok: false, message: `Cannot approve: missing patch plan JSON ${planPath}.` };
      let planJson;
      try {
        planJson = JSON.parse(planText);
      } catch {
        return { ok: false, message: `Cannot approve: invalid JSON in ${planPath}.` };
      }
      const v = validatePatchPlan(planJson, { policy: policiesForValidator });
      if (!v.ok) return { ok: false, message: `Cannot approve: invalid patch plan for repo ${r.repo_id}.`, errors: v.errors };
      if (requireClean) {
        const cleanOk =
          Array.isArray(v.normalized?.scope?.allowed_paths) &&
          v.normalized.scope.allowed_paths.length > 0 &&
          Array.isArray(v.normalized?.edits) &&
          v.normalized.edits.length > 0;
        if (!cleanOk) return { ok: false, message: `Cannot approve: patch plan not clean for repo ${r.repo_id}.` };
      }
    }

    const reviewRequired = boolFromEnv("REVIEW_BEFORE_APPROVAL", false);
    const reviewPath = `ai/lane_b/work/${workId}/reviews/architect-review.md`;
    const reviewExists = !!(await readTextIfExists(reviewPath));
    if (reviewRequired && !reviewExists) return { ok: false, message: `Cannot approve: review required but missing ${reviewPath}.` };

    const approvedAt = nowISO();
    const approvedBy = String(process.env.APPROVER_LABEL || "human").trim() || "human";

    const approval = {
      workId,
      status: "approved",
      mode: "manual",
      bundle_hash: bundle.bundle_hash,
      approved_at: approvedAt,
      approved_by: approvedBy,
      scope: {
        teams: scopeTeams.slice().sort((a, b) => a.localeCompare(b)),
        repos: (bundle.repos || []).map((x) => String(x.repo_id)).filter(Boolean).sort((a, b) => a.localeCompare(b)),
      },
      notes: notes ? String(notes) : "",
      review_required: reviewRequired,
      review_seen: reviewExists,
    };

    const jsonText = JSON.stringify(approval, null, 2) + "\n";
    const hash = sha256Hex(jsonText);
    await writeText(await approvalJsonPath(workId), jsonText);

    const mdText = renderApprovalMarkdown({ approval, proposalPathsByTeam, reviewPath: reviewExists ? reviewPath : null });
    await writeText(await approvalMdPath(workId), `${mdText}\n${renderBundleSummaryMarkdown(bundle)}`);

    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({
        timestamp: approvedAt,
        action: "plan_approval_granted",
        workId,
        mode: "manual",
        teams: approval.scope.teams,
        repos: approval.scope.repos,
        bundle_hash: bundle.bundle_hash,
        approved_by: approvedBy,
        approved_at: approvedAt,
        hash,
      }) + "\n",
    );

    await updateWorkStatus({
      workId,
      stage: "PLAN_APPROVED",
      blocked: false,
      artifacts: {
        plan_approval_json: await approvalJsonPath(workId),
        plan_approval_md: await approvalMdPath(workId),
      },
      note: "approved (manual)",
    });
    await this.writePortfolio();
    await writeGlobalStatusFromPortfolio();

    return {
      ok: true,
      workId,
      status: "approved",
      mode: "manual",
      bundle_hash: bundle.bundle_hash,
      approved_at: approvedAt,
      approved_by: approvedBy,
      teams: approval.scope.teams,
      repos: approval.scope.repos,
      plan_approval_json: await approvalJsonPath(workId),
      plan_approval_md: await approvalMdPath(workId),
      hash,
    };
  }

  async reject({ workId, teams, notes }) {
    const workDir = `ai/lane_b/work/${workId}`;
    const ok = await isDirectory(workDir);
    if (!ok) return { ok: false, message: `Work item not found: ${workDir}` };

    const requestedTeams = parseTeamsCsv(teams);
    const defaultTeams = await readRoutingSelectedTeams(workId);
    const scopeTeams = (requestedTeams || defaultTeams).slice().filter(Boolean);

    const approvedBy = String(process.env.APPROVER_LABEL || "human").trim() || "human";
    const ts = nowISO();

    const reviewPath = `ai/lane_b/work/${workId}/reviews/architect-review.md`;
    const reviewExists = !!(await readTextIfExists(reviewPath));
    const reviewRequired = boolFromEnv("REVIEW_BEFORE_APPROVAL", false);

    const approval = {
      workId,
      status: "rejected",
      approved_at: null,
      approved_by: approvedBy,
      scope: scopeTeams.length ? { teams: scopeTeams.slice().sort((a, b) => a.localeCompare(b)) } : { teams: [] },
      notes: notes ? String(notes) : "",
      review_required: reviewRequired,
      review_seen: reviewExists,
    };

    const jsonText = JSON.stringify(approval, null, 2) + "\n";
    const hash = sha256Hex(jsonText);
    await writeText(await approvalJsonPath(workId), jsonText);

    // Best-effort artifact pointers
    const proposalPathsByTeam = new Map();
    for (const t of approval.scope.teams) {
      proposalPathsByTeam.set(t, await listTeamProposals({ workId, teamId: t }));
    }
    const mdText = renderApprovalMarkdown({ approval, proposalPathsByTeam, reviewPath: reviewExists ? reviewPath : null });
    await writeText(await approvalMdPath(workId), mdText);

    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({ timestamp: ts, action: "plan_approval_rejected", workId, teams: approval.scope.teams, approved_by: approvedBy, notes: approval.notes, hash }) + "\n",
    );

    await updateWorkStatus({
      workId,
      stage: "REJECTED",
      blocked: false,
      artifacts: { plan_approval_json: await approvalJsonPath(workId), plan_approval_md: await approvalMdPath(workId) },
      note: "plan-approval rejected",
    });
    await this.writePortfolio();
    await writeGlobalStatusFromPortfolio();

    return { ok: true, workId, status: "rejected", teams: approval.scope.teams, plan_approval_json: await approvalJsonPath(workId), plan_approval_md: await approvalMdPath(workId), hash };
  }

  async resetApproval({ workId }) {
    const workDir = `ai/lane_b/work/${workId}`;
    const ok = await isDirectory(workDir);
    if (!ok) return { ok: false, message: `Work item not found: ${workDir}` };

    const existing = await readApprovalIfExists(workId);
    const existingTeams = existing && !existing._invalid && Array.isArray(existing?.scope?.teams) ? existing.scope.teams : null;
    const defaultTeams = await readRoutingSelectedTeams(workId);
    const scopeTeams = (existingTeams || defaultTeams).slice().filter(Boolean).sort((a, b) => a.localeCompare(b));

    const approvedBy = String(process.env.APPROVER_LABEL || "human").trim() || "human";
    const ts = nowISO();

    const reviewPath = `ai/lane_b/work/${workId}/reviews/architect-review.md`;
    const reviewExists = !!(await readTextIfExists(reviewPath));
    const reviewRequired = boolFromEnv("REVIEW_BEFORE_APPROVAL", false);

    const approval = {
      workId,
      status: "pending",
      approved_at: null,
      approved_by: approvedBy,
      scope: { teams: scopeTeams },
      notes: "",
      review_required: reviewRequired,
      review_seen: false,
    };

    const jsonText = JSON.stringify(approval, null, 2) + "\n";
    const hash = sha256Hex(jsonText);
    await writeText(await approvalJsonPath(workId), jsonText);

    const proposalPathsByTeam = new Map();
    for (const t of scopeTeams) {
      proposalPathsByTeam.set(t, await listTeamProposals({ workId, teamId: t }));
    }
    const mdText = renderApprovalMarkdown({ approval, proposalPathsByTeam, reviewPath: reviewExists ? reviewPath : null });
    await writeText(await approvalMdPath(workId), mdText);

    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: ts, action: "plan_approval_reset", workId, teams: scopeTeams, approved_by: approvedBy, hash }) + "\n");

    await updateWorkStatus({
      workId,
      stage: "PLAN_APPROVAL_REQUIRED",
      blocked: true,
      blockingReason: "Plan approval required (reset).",
      artifacts: { plan_approval_json: await approvalJsonPath(workId), plan_approval_md: await approvalMdPath(workId) },
      note: "plan-approval reset",
    });
    await this.writePortfolio();
    await writeGlobalStatusFromPortfolio();

    return { ok: true, workId, status: "pending", plan_approval_json: await approvalJsonPath(workId), plan_approval_md: await approvalMdPath(workId), hash };
  }

  async patchPlan({ workId, teams }) {
    const { runPatchPlans } = await import("./agents/patch-plan-runner.js");
    const res = await runPatchPlans({ repoRoot: this.repoRoot, workId, teamsCsv: teams });
    if (res && res.ok) {
      await updateWorkStatus({
        workId,
        stage: "PATCH_PLANNED",
        blocked: false,
        artifacts: { patch_plans_dir: `ai/lane_b/work/${workId}/patch-plans/` },
        note: "team patch plans created",
      });
      await this.writePortfolio();
      await writeGlobalStatusFromPortfolio();
    }
    return res;
  }

  async applyPatchPlans({ workId }) {
    const { runApplyPatchPlans } = await import("./agents/apply-runner.js");
    return await runApplyPatchPlans({ repoRoot: this.repoRoot, workId });
  }

  async createTeamTasksForWorkId({ workId, ignorePendingDecisionCheck }) {
    if (!ignorePendingDecisionCheck) {
      const decisionsText = await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md");
      if (!decisionsText || !decisionsText.includes("No pending decisions.")) {
        return { ok: false, message: "Cannot create tasks: pending decision exists." };
      }
    }

    const workDir = `ai/lane_b/work/${workId}`;
    const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
    if (!routingText) return { ok: false, message: `Cannot create tasks: missing ${workDir}/ROUTING.json.` };

    let routing;
    try {
      routing = JSON.parse(routingText);
    } catch {
      return { ok: false, message: `Cannot create tasks: invalid JSON in ${workDir}/ROUTING.json.` };
    }

    const selectedTeams = Array.isArray(routing?.selected_teams)
      ? routing.selected_teams.slice().filter(Boolean).sort((a, b) => String(a).localeCompare(String(b)))
      : [];
    if (!selectedTeams.length) return { ok: false, message: `Cannot create tasks: ${workDir}/ROUTING.json has no selected_teams.` };

    const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
    const intakeSummary = routing?.intake?.summary || oneLineSummary(stripIntakeMarkdown(intakeMd || ""));

    const teamsConfigText = await readTeamsConfigText();
    if (!teamsConfigText) return { ok: false, message: "Cannot create tasks: missing config/TEAMS.json (preferred) or config/teams.json." };

    let teamsConfig;
    try {
      teamsConfig = JSON.parse(teamsConfigText);
    } catch {
      return { ok: false, message: "Cannot create tasks: invalid teams config JSON." };
    }

    const teamsById = new Map((teamsConfig?.teams || []).map((t) => [t.team_id, t]));
    const deps = computeDependencyDefaults({ selectedTeams, intakeText: stripIntakeMarkdown(intakeMd || intakeSummary) });

    const tasksDir = `${workDir}/tasks`;
    await ensureDir(tasksDir);

    const created = [];
    for (const teamId of selectedTeams) {
      const team = teamsById.get(teamId);
      const scope = team?.description || "(scope unknown)";
      const dep = deps[teamId] || { must_run_after: [], can_run_in_parallel_with: [] };
      const taskPath = `${tasksDir}/${teamId}.md`;

      const content = [
        `# ${teamId}`,
        "",
        "## Context",
        "",
        `Work item: ${workId}`,
        `Intake: ${intakeSummary}`,
        "",
        "## Scope",
        "",
        scope,
        "",
        "## Assumptions",
        "",
        "- (none)",
        "",
        "## Open Questions",
        "",
        "- (none)",
        "",
        "## Tasks",
        "",
        "- Review intake and confirm scoped responsibility/boundaries",
        "- Identify impacted components within team scope",
        "- Draft task breakdown (no code changes yet)",
        "- Identify risks and open questions for supervision",
        "- Confirm dependencies and sequencing",
        "",
        "## Dependencies",
        "",
        `- must_run_after: ${JSON.stringify(dep.must_run_after)}`,
        `- can_run_in_parallel_with: ${JSON.stringify(dep.can_run_in_parallel_with)}`,
        "",
        "## Acceptance Criteria",
        "",
        "- Task stub exists and is scoped to team responsibility",
        "- Dependencies declared",
        "- Open questions captured (or explicitly none)",
        "",
      ].join("\n");

      await writeText(taskPath, content);
      created.push(taskPath);
    }

    const planText = await readTextIfExists("ai/PLAN.md");
    if (planText) {
      await writeText("ai/PLAN.md", updatePlanWithTaskPaths(planText, created));
    }

    const timestamp = nowISO();
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, workId, action: "team_tasks_created", teams: selectedTeams }) + "\n");

    await updateWorkStatus({
      workId,
      stage: "SWEEP_READY",
      blocked: false,
      artifacts: {
        tasks_dir: `${tasksDir}/`,
      },
      note: `teams=${selectedTeams.join(",") || "none"}`,
    });
    await this.writePortfolio();
    await writeGlobalStatusFromPortfolio();

    return { ok: true, workId, teams: selectedTeams, created };
  }

  async createTeamTasksForLatestWork() {
    const workIds = await listWorkIdsDesc();
    const latestWorkId = workIds[0] || null;

    const workId = latestWorkId;
    if (!workId) return { ok: false, message: "Cannot create tasks: no work cycles found under ai/lane_b/work/." };

    return await this.createTeamTasksForWorkId({ workId, ignorePendingDecisionCheck: false });
  }

  async enqueue({ text, source = null, sourcePath = null, origin = null, scope = null } = {}) {
    const timestamp = nowISO();
    const id = intakeId({ timestamp, text: String(text || "") });
    await ensureDir("ai/lane_b/inbox");
    const path = `ai/lane_b/inbox/${id}.md`;
    const lines = [];
    lines.push(`Intake: ${String(text || "").trimEnd()}`);
    lines.push(`CreatedAt: ${timestamp}`);
    if (typeof origin === "string" && origin.trim()) lines.push(`Origin: ${origin.trim()}`);
    if (typeof scope === "string" && scope.trim()) lines.push(`Scope: ${scope.trim()}`);
    if (typeof source === "string" && source.trim()) lines.push(`Source: ${source.trim()}`);
    if (typeof sourcePath === "string" && sourcePath.trim()) lines.push(`SourcePath: ${sourcePath.trim()}`);
    lines.push("");
    await writeText(path, lines.join("\n"));
    return { ok: true, intake_file: path, createdAt: timestamp, origin: typeof origin === "string" ? origin.trim() : null, scope: typeof scope === "string" ? scope.trim() : null };
  }

  async sweep({ limit }) {
    await ensureDir("ai/lane_b/inbox");
    await ensureDir("ai/lane_b/inbox/.processed");
    await ensureDir("ai/lane_b/inbox/triaged");
    await ensureDir("ai/lane_b/inbox/triaged/.processed");
    await ensureDir("ai/lane_b/inbox/triaged/archive");
    await ensureDir("ai/archive");

    const inboxEntries = await readdir(resolveStatePath("ai/lane_b/inbox"), { withFileTypes: true });

    let triagedEntries = [];
    try {
      triagedEntries = await readdir(resolveStatePath("ai/lane_b/inbox/triaged"), { withFileTypes: true });
    } catch {
      triagedEntries = [];
    }
    const triagedFilesAll = triagedEntries
      .filter((e) => e.isFile() && e.name.startsWith("T-") && e.name.endsWith(".json"))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b));
    const triagedFiles = [];
    for (const f of triagedFilesAll) {
      const triagedId = f.replace(/\.json$/i, "");
      if (await readTextIfExists(`ai/lane_b/inbox/triaged/.processed/${triagedId}.json`)) continue;
      triagedFiles.push(f);
    }

    const rawInboxFilesAll = inboxEntries
      .filter((e) => e.isFile() && e.name.startsWith("I-") && e.name.endsWith(".md"))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b));

    const rawInboxFiles = [];
    for (const f of rawInboxFilesAll) {
      const rawId = f.replace(/\.md$/i, "");
      const marker = await readTextIfExists(`ai/lane_b/inbox/.processed/${rawId}.json`);
      if (marker) continue;
      rawInboxFiles.push(f);
    }

    const mode = triagedFiles.length ? "triaged" : "raw";
    const inboxFiles = mode === "triaged" ? triagedFiles : rawInboxFiles;
    const toProcess = typeof limit === "number" ? inboxFiles.slice(0, limit) : inboxFiles;

    const existingDecisionsText = (await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md")) || renderNoPendingDecisions();
    const pendingDecisions = await ensureDecisionIntakeSummaries(parsePendingDecisions(existingDecisionsText));

    let processedCount = 0;
    let blockedCount = 0;
    let readyCount = 0;
    let escalatedCount = 0;

    const processed = [];

    for (const filename of toProcess) {
      const inboxFilePath = `ai/lane_b/inbox/${filename}`;
      const timestamp = nowISO();

      if (mode === "triaged") {
        const triagedFilePath = `ai/lane_b/inbox/triaged/${filename}`;
        const triagedText = await readTextIfExists(triagedFilePath);
        if (!triagedText) {
          escalatedCount += 1;
          processedCount += 1;
          await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, action: "triaged_item_missing", triaged_file: filename }) + "\n");
          processed.push({ intake_file: filename, workId: null, result: "escalated" });
          continue;
        }

        let triagedJson = null;
        try {
          triagedJson = JSON.parse(triagedText);
        } catch {
          triagedJson = null;
        }
        const triagedId = (triagedJson && typeof triagedJson === "object" ? String(triagedJson.triaged_id || "").trim() : "") || filename.replace(/\.json$/i, "");
        const validated = validateTriagedRepoItem(triagedJson || {}, { triagedId });
        if (!validated.ok) {
          escalatedCount += 1;
          processedCount += 1;
          await appendFile(
            "ai/lane_b/ledger.jsonl",
            JSON.stringify({ timestamp, action: "triaged_item_invalid", triaged_file: filename, errors: validated.errors.slice(0, 5) }) + "\n",
          );
          processed.push({ intake_file: filename, workId: null, result: "escalated" });
          continue;
        }
        const item = validated.normalized;

        const workId = makeWorkId({ timestamp, seed: triagedId });
        const workDir = `ai/lane_b/work/${workId}`;
        await ensureDir(workDir);
        await ensureWorkMeta({ workId, createdAtIso: timestamp });

        // META lineage fields (explicit; do not rely on folder naming).
        {
          const metaPath = `${workDir}/META.json`;
          const metaText = await readTextIfExists(metaPath);
          let meta = null;
          try {
            meta = metaText ? JSON.parse(metaText) : null;
          } catch {
            meta = null;
          }
          const next = meta && meta.version === 1 ? { ...meta } : defaultWorkMeta({ workId, createdAtIso: timestamp });
          next.raw_intake_id = item.raw_intake_id;
          next.batch_id = `BATCH-${item.raw_intake_id}`;
          next.triaged_id = item.triaged_id;
          next.repo_id = item.repo_id;
          next.team_id = item.team_id;
          next.target_branch = item.target_branch;
          next.repo_scopes = Array.isArray(next.repo_scopes) && next.repo_scopes.length ? next.repo_scopes : [item.repo_id];
          if (item.origin === "lane_a") {
            next.origin = "lane_a";
            if (typeof item.intake_approval_id === "string" && item.intake_approval_id.trim()) next.intake_approval_id = item.intake_approval_id.trim();
            if (typeof item.knowledge_version === "string" && item.knowledge_version.trim()) next.knowledge_version = item.knowledge_version.trim();
            if (typeof item.lane_a_scope === "string" && item.lane_a_scope.trim()) next.lane_a_scope = item.lane_a_scope.trim();
            next.sufficiency_override = item.sufficiency_override === true;
          }
          if (!Array.isArray(next.depends_on)) next.depends_on = [];
          await writeText(metaPath, JSON.stringify(next, null, 2) + "\n");
        }

        // INTAKE.md pointers + repo-scoped instructions
        {
          const rawIntakePath = `ai/lane_b/inbox/${item.raw_intake_id}.md`;
          const batchPath = `ai/lane_b/inbox/triaged/BATCH-${item.raw_intake_id}.json`;
          const lines = [];
          lines.push("# INTAKE");
          lines.push("");
          lines.push(`raw_intake_id: ${item.raw_intake_id}`);
          lines.push(`raw_intake_path: ${rawIntakePath}`);
          lines.push(`batch_id: BATCH-${item.raw_intake_id}`);
          lines.push(`batch_path: ${batchPath}`);
          lines.push(`triaged_id: ${item.triaged_id}`);
          lines.push(`triaged_path: ${triagedFilePath}`);
          lines.push(`repo_id: ${item.repo_id}`);
          lines.push(`team_id: ${item.team_id}`);
          lines.push(`target_branch: ${item.target_branch}`);
          if (item.origin === "lane_a") {
            lines.push(`origin: lane_a`);
            if (item.intake_approval_id) lines.push(`intake_approval_id: ${item.intake_approval_id}`);
            if (item.knowledge_version) lines.push(`knowledge_version: ${item.knowledge_version}`);
            if (item.lane_a_scope) lines.push(`lane_a_scope: ${item.lane_a_scope}`);
            if (item.sufficiency_override === true) lines.push("sufficiency_override: true");
          }
          lines.push("");
          lines.push("## Summary");
          lines.push("");
          lines.push(item.summary);
          lines.push("");
          lines.push("## Instructions");
          lines.push("");
          lines.push(item.instructions);
          lines.push("");
          await writeText(`${workDir}/INTAKE.md`, lines.join("\n"));
        }

        // Trivial deterministic routing for repo-scoped work
        {
          const routing = {
            workId,
            timestamp,
            intake: { source: "triaged", summary: item.summary },
            matches: [],
            selected_teams: [item.team_id],
            selected_repos: [item.repo_id],
            routing_confidence: 1.0,
            high_risk_detected: false,
            needs_confirmation: false,
            reason: "triaged_repo_scoped",
            routing_mode: "triaged_repo_scoped",
            repo_match: { repo_id: item.repo_id, match_type: "triaged_repo_item", confidence: 1.0, matched_token: item.repo_id },
            target_branch: { name: item.target_branch, source: "triage", method: "triaged_item", confidence: 1.0, valid: true },
          };
          await writeText(`${workDir}/ROUTING.json`, renderWorkRouting({ routing }));
          await writeWorkPlan({ workId, intakeMd: await readTextIfExists(`${workDir}/INTAKE.md`), routing, bundle: null });
          await updateWorkStatus({ workId, stage: "ROUTED", blocked: false, artifacts: { routing_json: `${workDir}/ROUTING.json`, meta_json: `${workDir}/META.json` } });
        }

        await this.createTeamTasksForWorkId({ workId, ignorePendingDecisionCheck: true });
        await updateWorkStatus({ workId, stage: "SWEEP_READY", blocked: false, note: "tasks created (triaged repo work)" });

        await writeText(
          `ai/lane_b/inbox/triaged/.processed/${item.triaged_id}.json`,
          JSON.stringify({ version: 1, triaged_id: item.triaged_id, processed_at: timestamp, work_id: workId }, null, 2) + "\n",
        );
        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({ timestamp, action: "work_created_from_triage", workId, triaged_id: item.triaged_id, raw_intake_id: item.raw_intake_id, repo_id: item.repo_id, team_id: item.team_id }) + "\n",
        );

        // Best-effort: flip batch status to swept when all children are processed.
        {
          const batchPath = `ai/lane_b/inbox/triaged/BATCH-${item.raw_intake_id}.json`;
          const batchText = await readTextIfExists(batchPath);
          if (batchText) {
            try {
              const bRaw = JSON.parse(batchText);
              const vb = validateTriagedBatch(bRaw, { batchId: `BATCH-${item.raw_intake_id}`, rawIntakeId: item.raw_intake_id });
              if (vb.ok) {
                const b = vb.normalized;
                if (b.status === "triaged") {
                  let allDone = true;
                  for (const tid of b.triaged_ids) {
                    if (!(await readTextIfExists(`ai/lane_b/inbox/triaged/.processed/${tid}.json`))) {
                      allDone = false;
                      break;
                    }
                  }
                  if (allDone) {
                    b.status = "swept";
                    await writeText(batchPath, JSON.stringify(b, null, 2) + "\n");
                  }
                }
              }
            } catch {
              // ignore
            }
          }
        }

        readyCount += 1;
        processedCount += 1;
        processed.push({ intake_file: filename, workId, result: "ready" });
        continue;
      }

      // Raw inbox mode (triage not used): process unprocessed I-*.md entries as before.
      const raw = await readTextIfExists(inboxFilePath);
      if (!raw) {
        escalatedCount += 1;
        processedCount += 1;
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, action: "inbox_item_processed", intake_file: filename, workId: null, result: "escalated" }) + "\n");
        processed.push({ intake_file: filename, workId: null, result: "escalated" });
        continue;
      }

      const intakeLine = raw
        .split("\n")
        .map((l) => l.trim())
        .find((l) => l.toLowerCase().startsWith("intake:"));
      const intakeText = intakeLine ? intakeLine.slice("intake:".length).trim() : raw;

      let runResult;
      try {
        runResult = await this.run({ intakeText, intakeSource: "file", decisionsMode: "sweep" });
      } catch {
        escalatedCount += 1;
        processedCount += 1;
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, action: "inbox_item_processed", intake_file: filename, workId: null, result: "escalated" }) + "\n");
        processed.push({ intake_file: filename, workId: null, result: "escalated" });
        continue;
      }

      const workId = runResult.workId;
      const workDir = runResult.workDir;

      const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
      let routing = null;
      if (routingText) {
        try {
          routing = JSON.parse(routingText);
        } catch {
          routing = null;
        }
      }

      let result = "ready";
      if (routing?.needs_confirmation) {
        result = "blocked";
        blockedCount += 1;

        const opt = { A: "Confirm routing.", B: "Escalate to Architect." };
        if (Array.isArray(routing.proposed_options)) {
          for (const o of routing.proposed_options) {
            const m = String(o).match(/^\s*([AB])\s*:\s*(.+?)\s*$/);
            if (m) opt[m[1]] = m[2];
          }
        }

        pendingDecisions.push({
          workId,
          intakeSummary: routing?.intake?.summary ? oneLineSummary(routing.intake.summary) : null,
          question: routing.proposed_question || `Routing confirmation required for ${workId}.`,
          options: opt,
        });
      } else {
        readyCount += 1;
        await this.createTeamTasksForWorkId({ workId, ignorePendingDecisionCheck: true });
      }

      processedCount += 1;
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({ timestamp, action: "inbox_item_processed", intake_file: filename, workId, result }) + "\n",
      );
      processed.push({ intake_file: filename, workId, result });

      const destBase = `ai/archive/${filename}`;
      let dest = destBase;
      let n = 1;
      while (await pathExists(dest)) {
        dest = destBase.replace(/\.md$/, `.${n}.md`);
        n += 1;
      }
      await rename(resolveStatePath(inboxFilePath), resolveStatePath(dest));
    }

    await writeText("ai/lane_b/DECISIONS_NEEDED.md", renderPendingDecisions(await ensureDecisionIntakeSummaries(pendingDecisions)));

    await this.writePortfolio();

    const sweepTs = nowISO();
    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({
        timestamp: sweepTs,
        action: "sweep_complete",
        processed_count: processedCount,
        blocked_count: blockedCount,
        ready_count: readyCount,
        escalated_count: escalatedCount,
      }) + "\n",
    );

    return { ok: true, processed_count: processedCount, blocked_count: blockedCount, ready_count: readyCount, escalated_count: escalatedCount, processed };
  }

  async writePortfolio() {
    const decisionsText = (await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md")) || renderNoPendingDecisions();
    const pending = parsePendingDecisions(decisionsText);
    const blockedSet = new Set(pending.map((d) => d.workId));

    const workIds = await listWorkIdsDesc();
    const ready = [];
    const blocked = [];
    const escalated = [];
    const byBatchId = new Map();

    const batchApprovalCache = new Map();
    const getBatchApprovalStatus = async (rawIntakeId) => {
      const k = String(rawIntakeId || "").trim();
      if (!k) return null;
      if (batchApprovalCache.has(k)) return batchApprovalCache.get(k);
      try {
        const { readBatchApproval } = await import("../project/batch-approval.js");
        const res = await readBatchApproval(k);
        const st = res.ok && res.exists && res.approval && typeof res.approval === "object" ? String(res.approval.status || "").trim() : null;
        batchApprovalCache.set(k, st || null);
        return st || null;
      } catch {
        batchApprovalCache.set(k, null);
        return null;
      }
    };

    for (const workId of workIds) {
      const workDir = `ai/lane_b/work/${workId}`;
      const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
      if (!routingText) continue;
      let routing;
      try {
        routing = JSON.parse(routingText);
      } catch {
        continue;
      }

      const metaText = await readTextIfExists(`${workDir}/META.json`);
      let meta = null;
      try {
        meta = metaText ? JSON.parse(metaText) : null;
      } catch {
        meta = null;
      }
      const batchId = meta && typeof meta === "object" && typeof meta.batch_id === "string" && meta.batch_id.trim() ? meta.batch_id.trim() : null;
      const repoId = meta && typeof meta === "object" && typeof meta.repo_id === "string" && meta.repo_id.trim() ? meta.repo_id.trim() : null;
      const teamId = meta && typeof meta === "object" && typeof meta.team_id === "string" && meta.team_id.trim() ? meta.team_id.trim() : null;
      const rawIntakeId = meta && typeof meta === "object" && typeof meta.raw_intake_id === "string" && meta.raw_intake_id.trim() ? meta.raw_intake_id.trim() : null;

      const statusRes = await readWorkStatusSnapshot(workId);
      const stage = statusRes.ok ? String(statusRes.snapshot.current_stage || "") : "";

      // Effective approval status: per-work overrides batch. (Used for batch summary only.)
      let effectiveApproval = "pending";
      const approvalText = (await readTextIfExists(`${workDir}/PLAN_APPROVAL.json`)) || (await readTextIfExists(`${workDir}/APPROVAL.json`));
      if (approvalText) {
        try {
          const a = JSON.parse(approvalText);
          if (a && typeof a === "object" && typeof a.status === "string" && a.status.trim()) effectiveApproval = a.status.trim();
        } catch {
          // ignore
        }
      } else {
        const batchStatus = rawIntakeId ? await getBatchApprovalStatus(rawIntakeId) : null;
        if (batchStatus === "approved") effectiveApproval = "approved_by_batch";
        else if (batchStatus === "rejected") effectiveApproval = "rejected_by_batch";
      }

      const date = (routing.timestamp || "").slice(0, 10) || todayISO();
      const summary = routing?.intake?.summary || "(no summary)";
      const teams = Array.isArray(routing.selected_teams) && routing.selected_teams.length ? routing.selected_teams.join(", ") : teamId || "(none)";
      const entry = `- ${workId} | ${date} | ${summary} | repo: ${repoId || "(unknown)"} | teams: ${teams}${batchId ? ` | batch: ${batchId}` : ""}`;

      if (batchId) {
        const b = byBatchId.get(batchId) || { batch_id: batchId, work_ids: [], repos: new Set(), stages: new Map(), approvals: new Map() };
        b.work_ids.push(workId);
        if (repoId) b.repos.add(repoId);
        b.stages.set(workId, stage || "");
        b.approvals.set(workId, effectiveApproval);
        byBatchId.set(batchId, b);
      }

      if (await pathExists(`${workDir}/ESCALATION.md`)) {
        escalated.push(entry);
      } else if (blockedSet.has(workId)) {
        blocked.push(entry);
      } else {
        // Default: include in READY/PLANNED once routing exists.
        ready.push(entry);
      }
    }

    const content = [
      "# PORTFOLIO",
      "",
      `Last updated: ${todayISO()}`,
      "",
      "## BATCH SUMMARY",
      "",
      ...(Array.from(byBatchId.values()).length
        ? Array.from(byBatchId.values())
            .sort((a, b) => String(a.batch_id).localeCompare(String(b.batch_id)))
            .map((b) => {
              const workIds2 = Array.isArray(b.work_ids) ? b.work_ids.slice() : [];
              const total = workIds2.length;
              const done = workIds2.filter((w) => {
                const s = String(b.stages.get(w) || "");
                return s === "COMPLETED" || s === "DONE" || s === "MERGED";
              }).length;
              const failed = workIds2.filter((w) => String(b.stages.get(w) || "") === "FAILED").length;
              const approved = workIds2.filter((w) => {
                const a = String(b.approvals.get(w) || "");
                return a === "approved" || a === "approved_by_batch";
              }).length;
              return `- ${b.batch_id}: total=${total}, approved=${approved}, done=${done}, failed=${failed}, repos=${Array.from(b.repos).sort().join(", ")}`;
            })
        : ["- (none)"]),
      "",
      "## READY/PLANNED",
      "",
      ...(ready.length ? ready : ["- (none)"]),
      "",
      "## BLOCKED",
      "",
      ...(blocked.length ? blocked : ["- (none)"]),
      "",
      "## ESCALATED",
      "",
      ...(escalated.length ? escalated : ["- (none)"]),
      "",
    ].join("\n");

    await writeText("ai/lane_b/PORTFOLIO.md", content);
  }

  async portfolio() {
    const text = await readTextIfExists("ai/lane_b/PORTFOLIO.md");
    if (!text) return { ok: false, message: "ai/lane_b/PORTFOLIO.md not found. Run --sweep first." };
    return { ok: true, output: text.endsWith("\n") ? text : text + "\n" };
  }
}
