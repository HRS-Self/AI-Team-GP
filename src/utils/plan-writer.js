import { readTextIfExists, writeText } from "./fs.js";

function nowISO() {
  return new Date().toISOString();
}

function stripCodeFences(md) {
  const lines = String(md || "").split("\n");
  const out = [];
  let inFence = false;
  for (const l of lines) {
    if (l.trim().startsWith("```")) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    out.push(l);
  }
  return out.join("\n");
}

function normalizeProblemStatementFromIntake(intakeMd) {
  const text = stripCodeFences(intakeMd)
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"))
    .join("\n")
    .trim();

  // Keep first paragraph only to avoid dumping large intake into PLAN.md.
  const parts = text.split(/\n\s*\n/).map((p) => p.trim()).filter(Boolean);
  const first = parts[0] || "";
  return first || "(missing)";
}

function renderList(items, empty = "- (none)") {
  const list = (items || []).map((x) => String(x).trim()).filter(Boolean);
  if (!list.length) return [empty];
  return list.sort((a, b) => a.localeCompare(b)).map((x) => `- \`${x}\``);
}

export async function writeWorkPlan({ workId, intakeMd, routing = null, bundle = null }) {
  const workDir = `ai/lane_b/work/${workId}`;
  const path = `${workDir}/PLAN.md`;

  const problem = normalizeProblemStatementFromIntake(intakeMd);
  const repos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : [];
  const teams = Array.isArray(routing?.selected_teams) ? routing.selected_teams.slice().filter(Boolean) : [];
  const targetBranch = typeof routing?.target_branch?.name === "string" && routing.target_branch.name.trim() ? routing.target_branch.name.trim() : null;

  const hasBundle = !!bundle && typeof bundle === "object" && typeof bundle?.bundle_hash === "string" && bundle.bundle_hash.trim();

  const constraints = [];
  if (targetBranch) constraints.push(`Target branch pinned by routing: \`${targetBranch}\``);
  if (routing?.routing_mode === "repo_explicit" && routing?.repo_match?.repo_id) constraints.push(`Repo-explicit routing: \`${routing.repo_match.repo_id}\` overrides keyword routing.`);
  if (routing?.routing_mode === "repo_ambiguous") constraints.push("Repo selection ambiguous; work is blocked until clarified.");
  if (routing?.target_branch?.valid === false) constraints.push("Target branch invalid/missing; work is blocked until clarified.");
  if (hasBundle) constraints.push("Bundle hash pins proposal + patch plan inputs (do not regenerate without re-approval).");

  const success = [];
  success.push("Scope is correct and agreed (repos + target branch).");
  if (repos.length) success.push(`Only the in-scope repo(s) are changed: ${repos.map((r) => `\`${r}\``).join(", ")}.`);
  if (hasBundle) success.push("Bundle is approved (PLAN_APPROVAL.json status=approved, bundle_hash matches).");
  success.push("Apply completes with no failed repos (or failures are explicitly resolved/approved).");

  const outOfScope = ["- No changes outside the in-scope repos.", "- No changes to archived/inactive repos unless explicitly approved."];

  const nonGoals = ["- No unrelated refactors.", "- No instruction-mode implementation details in this plan.", "- No shell commands documented here."];

  const assumptions = ["- Repo registry (config/REPOS.json) and routing are correct.", "- Patch plans list the complete file scope and are validator-clean."];

  const md = [
    "# PLAN",
    "",
    `Work item: ${workId}`,
    `Last updated: ${nowISO()}`,
    "",
    "## Problem statement",
    "",
    problem,
    "",
    "## In-scope repos",
    "",
    ...renderList(repos),
    "",
    "## In-scope teams",
    "",
    ...renderList(teams),
    "",
    "## Constraints",
    "",
    ...(constraints.length ? constraints.map((c) => `- ${c}`) : ["- (none)"]),
    "",
    "## Out of scope",
    "",
    ...outOfScope,
    "",
    "## Non-goals",
    "",
    ...nonGoals,
    "",
    "## Success criteria",
    "",
    ...success.map((c) => `- ${c}`),
    "",
    "## Assumptions",
    "",
    ...assumptions.map((a) => `- ${a}`),
    "",
    "<!-- NOTE: This file is generated/updated by the engine. Do not put shell commands or implementation steps here. -->",
    "",
  ].join("\n");

  await writeText(path, md);
  return { ok: true, path };
}

export async function readBundleIfExists(workId) {
  const p = `ai/lane_b/work/${workId}/BUNDLE.json`;
  const t = await readTextIfExists(p);
  if (!t) return { ok: false, missing: true, path: p, bundle: null };
  try {
    return { ok: true, missing: false, path: p, bundle: JSON.parse(t) };
  } catch {
    return { ok: false, missing: false, path: p, bundle: null };
  }
}
