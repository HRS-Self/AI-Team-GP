import { readFileSync, existsSync } from "node:fs";
import { join, relative, resolve } from "node:path";

import { appendFile, ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { validateScope, ensureKnowledgeStructure } from "./knowledge-utils.js";
import { runArchitectInterview, renderTranscriptMd } from "./architect-interviewer.js";
import { mergeSessionNotesIntoMerged } from "./notes-merge.js";
import { loadLlmProfiles, resolveLlmProfileOrError } from "../../llm/llm-profiles.js";
import { validateBacklogSeeds } from "../../validators/backlog-seeds-validator.js";
import { validateGaps } from "../../validators/gaps-validator.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { probeGitWorkTree, runGit, getOriginUrl } from "./git-checks.js";
import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function readJsonAbsOptional(absPath) {
  try {
    if (!existsSync(absPath)) return { ok: true, exists: false, json: null };
    const t = readFileSync(absPath, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(t || "")) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

function readSsotSectionContent({ knowledgeRootAbs, sectionFile }) {
  const abs = join(knowledgeRootAbs, "ssot", "system", "sections", sectionFile);
  const r = readJsonAbsOptional(abs);
  if (!r.ok || !r.exists) return "";
  const c = r.json && typeof r.json.content === "string" ? r.json.content.trim() : "";
  return c;
}

function computeAllowedStageFromCompletion(completion) {
  const c = completion && typeof completion === "object" ? completion : {};
  if (!c.vision) return "VISION";
  if (!c.requirements) return "REQUIREMENTS";
  if (!c.domain_data) return "DOMAIN_DATA";
  if (!c.api) return "API";
  if (!c.infra) return "INFRA";
  return "OPS";
}

function buildSdlcContext({ knowledgeRootAbs, scope }) {
  // Deterministic ladder completion signals derived from stable Lane A artifacts (kickoff + SSOT sections + integration_map).
  const kickoffLatestAbs = join(knowledgeRootAbs, "sessions", "kickoff", "LATEST.json");
  const latestRes = readJsonAbsOptional(kickoffLatestAbs);
  let kickoffSystemInputs = null;
  let kickoffRepoInputs = null;
  if (latestRes.ok && latestRes.exists && isPlainObject(latestRes.json) && isPlainObject(latestRes.json.latest_by_scope)) {
    const sys = latestRes.json.latest_by_scope.system || null;
    const rep = typeof scope === "string" && scope.startsWith("repo:") ? latestRes.json.latest_by_scope[scope] || null : null;
    const loadInputs = (entry) => {
      if (!entry || typeof entry.latest_json !== "string" || !entry.latest_json.trim()) return null;
      const abs = join(knowledgeRootAbs, "sessions", "kickoff", entry.latest_json);
      const jr = readJsonAbsOptional(abs);
      if (!jr.ok || !jr.exists) return null;
      return jr.json && typeof jr.json === "object" && jr.json.inputs && typeof jr.json.inputs === "object" ? jr.json.inputs : null;
    };
    kickoffSystemInputs = loadInputs(sys);
    kickoffRepoInputs = loadInputs(rep);
  }

  const visionTxt = readSsotSectionContent({ knowledgeRootAbs, sectionFile: "vision.json" });
  const scopeTxt = readSsotSectionContent({ knowledgeRootAbs, sectionFile: "scope.json" });
  const archTxt = readSsotSectionContent({ knowledgeRootAbs, sectionFile: "architecture.json" });
  const constraintsTxt = readSsotSectionContent({ knowledgeRootAbs, sectionFile: "constraints.json" });
  const risksTxt = readSsotSectionContent({ knowledgeRootAbs, sectionFile: "risks.json" });

  const anyKickoff = (obj, field) => {
    if (!obj || typeof obj !== "object") return false;
    const v = obj[field];
    if (typeof v === "string") return !!v.trim();
    if (Array.isArray(v)) return v.map((x) => String(x || "").trim()).filter(Boolean).length > 0;
    return false;
  };

  const integrationMapAbs = join(knowledgeRootAbs, "views", "integration_map.json");
  const integrationMapExists = existsSync(integrationMapAbs);

  const completion = {
    vision: !!visionTxt || anyKickoff(kickoffSystemInputs, "vision") || anyKickoff(kickoffSystemInputs, "problem_statement") || anyKickoff(kickoffSystemInputs, "title"),
    requirements: !!scopeTxt || anyKickoff(kickoffSystemInputs, "in_scope") || anyKickoff(kickoffSystemInputs, "out_of_scope") || anyKickoff(kickoffSystemInputs, "success_criteria"),
    domain_data: !!archTxt || anyKickoff(kickoffSystemInputs, "glossary") || anyKickoff(kickoffRepoInputs, "glossary"),
    api: integrationMapExists,
    infra: !!constraintsTxt || anyKickoff(kickoffSystemInputs, "constraints") || anyKickoff(kickoffSystemInputs, "nfrs"),
    ops: !!risksTxt,
  };

  return { version: 1, completion, allowed_stage: computeAllowedStageFromCompletion(completion) };
}

function isGitRepo(repoRoot) {
  const probe = probeGitWorkTree({ cwd: repoRoot });
  return probe.ok && probe.is_inside_work_tree === true;
}

async function listActiveRepoIdsFromRuntime() {
  const reposText = await readTextIfExists("config/REPOS.json");
  if (!reposText) return { ok: false, message: "Missing config/REPOS.json (required for knowledge interview gating)." };
  let parsed;
  try {
    parsed = JSON.parse(reposText);
  } catch {
    return { ok: false, message: "Invalid JSON in config/REPOS.json." };
  }
  const repos = Array.isArray(parsed?.repos) ? parsed.repos : [];
  const ids = repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => String(r?.repo_id || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  return { ok: true, repo_ids: ids };
}

async function loadTeamsAndReposSetsForKnowledge() {
  const teamsText = await readTextIfExists("config/TEAMS.json");
  const reposText = await readTextIfExists("config/REPOS.json");
  if (!teamsText) return { ok: false, message: "Missing config/TEAMS.json (required to validate backlog seeds/gaps)." };
  if (!reposText) return { ok: false, message: "Missing config/REPOS.json (required to validate backlog seeds/gaps)." };
  let teamsJson;
  let reposJson;
  try {
    teamsJson = JSON.parse(teamsText);
  } catch {
    return { ok: false, message: "Invalid JSON in config/TEAMS.json." };
  }
  try {
    reposJson = JSON.parse(reposText);
  } catch {
    return { ok: false, message: "Invalid JSON in config/REPOS.json." };
  }
  const teams = Array.isArray(teamsJson?.teams) ? teamsJson.teams : [];
  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];
  return {
    ok: true,
    teamsById: new Set(teams.map((t) => String(t?.team_id || "").trim()).filter(Boolean)),
    reposById: new Set(repos.map((r) => String(r?.repo_id || "").trim()).filter(Boolean)),
  };
}

function priorityWeight(p) {
  if (p === "P0") return 0;
  if (p === "P1") return 1;
  if (p === "P2") return 2;
  if (p === "P3") return 3;
  return 99;
}

function sortSeeds(items) {
  return items
    .slice()
    .sort((a, b) => {
      const pa = Number.isFinite(Number(a?.phase)) ? Number(a.phase) : 0;
      const pb = Number.isFinite(Number(b?.phase)) ? Number(b.phase) : 0;
      if (pa !== pb) return pa - pb;
      const wa = priorityWeight(String(a?.priority || ""));
      const wb = priorityWeight(String(b?.priority || ""));
      if (wa !== wb) return wa - wb;
      return String(a?.seed_id || "").localeCompare(String(b?.seed_id || ""));
    });
}

function impactWeight(x) {
  if (x === "high") return 0;
  if (x === "medium") return 1;
  if (x === "low") return 2;
  return 99;
}

function sortGaps(items) {
  return items
    .slice()
    .sort((a, b) => {
      const ia = impactWeight(String(a?.impact || ""));
      const ib = impactWeight(String(b?.impact || ""));
      if (ia !== ib) return ia - ib;
      const ra = impactWeight(String(a?.risk_level || ""));
      const rb = impactWeight(String(b?.risk_level || ""));
      if (ra !== rb) return ra - rb;
      return String(a?.gap_id || "").localeCompare(String(b?.gap_id || ""));
    });
}

async function buildRegistryContextForInterviewer() {
  // Provide deterministic, low-noise registry context so the interviewer can emit validator-clean
  // backlog_seeds / gaps artifacts (team_id and repo_id must match runtime registries).
  const teamsText = await readTextIfExists("config/TEAMS.json");
  const reposText = await readTextIfExists("config/REPOS.json");
  if (!teamsText || !reposText) return "";

  let teamsJson;
  let reposJson;
  try {
    teamsJson = JSON.parse(teamsText);
  } catch {
    teamsJson = null;
  }
  try {
    reposJson = JSON.parse(reposText);
  } catch {
    reposJson = null;
  }
  const teams = Array.isArray(teamsJson?.teams) ? teamsJson.teams : [];
  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];

  const teamIds = teams
    .map((t) => String(t?.team_id || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  const activeRepos = repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => ({
      repo_id: String(r?.repo_id || "").trim(),
      team_id: typeof r?.team_id === "string" && r.team_id.trim() ? r.team_id.trim() : null,
    }))
    .filter((r) => r.repo_id)
    .sort((a, b) => a.repo_id.localeCompare(b.repo_id));

  if (!teamIds.length && !activeRepos.length) return "";

  const lines = [];
  lines.push("Registry context (STRICT; use exact IDs)");
  lines.push("- Do NOT use placeholders like 'tbd' or 'unknown' for IDs.");
  if (teamIds.length) lines.push(`- Valid team_id values: ${teamIds.join(", ")}`);
  if (activeRepos.length) {
    lines.push("- Active repos (repo_id -> team_id):");
    for (const r of activeRepos) lines.push(`  - ${r.repo_id} -> ${r.team_id || "(missing team_id)"}`);
  }
  return `\n\n${lines.join("\n")}\n`;
}

async function writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message, errors = [] }) {
  const path = `${String(knowledgeRootAbs || "").trim()}/sessions/${sessionStem}.FAILED.md`;
  const lines = [];
  lines.push("# Knowledge Interview FAILED");
  lines.push("");
  lines.push(`RecordedAt: ${nowISO()}`);
  lines.push("");
  lines.push("## Error");
  lines.push("");
  lines.push(String(message || "").trim() || "(unknown)");
  if (errors.length) {
    lines.push("");
    lines.push("## Validation errors");
    lines.push("");
    for (const e of errors) lines.push(`- ${String(e)}`);
  }
  lines.push("");
  await writeText(path, lines.join("\n"));
  return path;
}

function normalizeBacklogSeedsFromInterview({ project_code, interviewBacklogSeeds }) {
  if (!isPlainObject(interviewBacklogSeeds)) {
    return { ok: false, message: "Interview output missing backlog_seeds (expected object)." };
  }
  const itemsRaw = Array.isArray(interviewBacklogSeeds.items) ? interviewBacklogSeeds.items : null;
  if (!itemsRaw) return { ok: false, message: "backlog_seeds.items missing (expected array)." };
  return {
    ok: true,
    raw: {
      version: 1,
      project_code,
      generated_at: nowISO(),
      items: sortSeeds(itemsRaw),
    },
  };
}

function normalizeGapsFromInterview({ project_code, interviewGaps }) {
  if (!isPlainObject(interviewGaps)) {
    return { ok: false, message: "Interview output missing gaps (expected object)." };
  }
  const baseline = typeof interviewGaps.baseline === "string" ? interviewGaps.baseline.trim() : "";
  if (!baseline) return { ok: false, message: "gaps.baseline missing (expected non-empty string)." };
  const itemsRaw = Array.isArray(interviewGaps.items) ? interviewGaps.items : null;
  if (!itemsRaw) return { ok: false, message: "gaps.items missing (expected array)." };
  return {
    ok: true,
    raw: {
      version: 1,
      project_code,
      baseline,
      generated_at: nowISO(),
      items: sortGaps(itemsRaw),
    },
  };
}

function pickInterviewerAgent({ agentsConfig }) {
  const cfg = isPlainObject(agentsConfig) ? agentsConfig : null;
  const agents = cfg && Array.isArray(cfg.agents) ? cfg.agents.filter((a) => isPlainObject(a) && a.enabled !== false) : [];
  const pick = (role) =>
    agents.find((a) => String(a.role || "").trim().toLowerCase() === role && String(a.implementation || "").trim().toLowerCase() === "llm") ||
    null;
  const interviewer = pick("interviewer");
  const planner = pick("planner");
  const chosen = interviewer || planner;
  if (!chosen) return { agent_id: null, llm_profile: null };
  const agent_id = typeof chosen.agent_id === "string" ? chosen.agent_id.trim() : null;
  const llm_profile = typeof chosen.llm_profile === "string" && chosen.llm_profile.trim() ? chosen.llm_profile.trim() : null;
  return { agent_id: agent_id || null, llm_profile };
}

async function appendKnowledgeDecisions({ knowledgeRootAbs, scope, decisions, dryRun }) {
  const items = Array.isArray(decisions) ? decisions.filter((x) => isPlainObject(x)) : [];
  if (!items.length) return { ok: true, appended: 0 };

  const path = `${String(knowledgeRootAbs || "").trim()}/decisions/DECISIONS_NEEDED.md`;
  const existing = (await readTextIfExists(path)) || "# DECISIONS NEEDED\n\nNo pending decisions.\n";
  const header = `## Knowledge/${scope}`;
  const lines = existing.trimEnd().split("\n");

  const out = [];
  let hasSection = false;
  for (const l of lines) {
    out.push(l);
    if (l.trim() === header) hasSection = true;
  }
  if (!hasSection) {
    out.push("");
    out.push(header);
    out.push("");
  } else {
    out.push("");
  }

  let appended = 0;
  for (const d of items) {
    const q = String(d.question || "").trim();
    const A = String(d.A || "").trim();
    const B = String(d.B || "").trim();
    const rec = d.recommended === "A" || d.recommended === "B" ? d.recommended : null;
    if (!q || !A || !B) continue;
    out.push(`- Q: ${q}`);
    out.push(`  - A: ${A}`);
    out.push(`  - B: ${B}`);
    if (rec) out.push(`  - Recommended: ${rec}`);
    appended += 1;
  }
  out.push("");

  if (dryRun) return { ok: true, appended };
  await writeText(path, out.join("\n"));
  return { ok: true, appended };
}

export async function runKnowledgeInterview({
  scope,
  start = false,
  cont = false,
  sessionText = null,
  maxQuestions = 12,
  dryRun = false,
}) {
  const scopeInfo = validateScope(scope);
  const scopeTag = scopeInfo.kind === "system" ? "system" : `repo-${scopeInfo.repo_id}`;
  const tsSafe = nowFsSafeUtcTimestamp();
  let paths;
  try {
    paths = await loadProjectPaths({ projectRoot: null });
  } catch (err) {
    return { ok: false, message: err instanceof Error ? err.message : String(err) };
  }
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const knowledgeRootAbs = paths.knowledge.rootAbs;
  const knowledgeRepo = knowledgeRootAbs;
  const logErrPath = join(paths.laneA.logsAbs, `knowledge-interview__${scopeTag}__${tsSafe}.error.json`);

  if (!dryRun && !isGitRepo(knowledgeRootAbs)) {
    const probe = probeGitWorkTree({ cwd: knowledgeRootAbs });
    const errObj = {
      timestamp: nowISO(),
      scope: scopeInfo.scope,
      action: "knowledge_repo_invalid",
      message: probe?.message || "Knowledge repo is missing or not a git repo.",
      knowledge_repo_dir: knowledgeRootAbs,
      hint: "Initialize it (during --initial-project) or run `git init`, then configure origin.",
    };
    await writeText(logErrPath, JSON.stringify(errObj, null, 2) + "\n");
    await appendKnowledgeDecisions({
      knowledgeRootAbs,
      scope: scopeInfo.scope,
      decisions: [
        {
          question: `Initialize knowledge repo git (${knowledgeRootAbs})`,
          A: "Run `git init` in the knowledge repo and set origin, then rerun knowledge interview.",
          B: "Run knowledge interview with --dry-run (no writes) until the repo is ready.",
          recommended: "A",
        },
      ],
      dryRun: false,
    });
    return {
      ok: false,
      message: `Knowledge repo is missing or not a git repo:\n${probe?.message || `(unknown error)\ncwd: ${knowledgeRootAbs}`}\n\nInitialize it and set origin (see decisions/DECISIONS_NEEDED.md in the knowledge repo).`,
      knowledge_repo_dir: knowledgeRootAbs,
      error_log: logErrPath,
    };
  }

  if (!dryRun) await ensureKnowledgeStructure({ knowledgeRootAbs });

  if (start && cont) return { ok: false, message: "Choose only one: --start or --continue." };
  if (!start && !cont) return { ok: false, message: "Missing mode: use --start or --continue." };

  // Lane A must not write runtime ledger entries.

  const charterMd = ["# Charter", "", `- Scope: ${scopeInfo.scope}`, ""].join("\n");

  // If no session text, only generate questions and record a session stub.
  const runQuestionsOnly = !sessionText || !String(sessionText).trim();

  // Interviews may only write scoped deltas after the scavenger + synthesizer have completed.
  const mergedPath =
    scopeInfo.kind === "system"
      ? join(knowledgeRootAbs, "ssot", "system", "assumptions.json")
      : join(knowledgeRootAbs, "ssot", "repos", scopeInfo.repo_id, "assumptions.json");
  if (!dryRun && scopeInfo.kind !== "system") {
    await ensureDir(join(knowledgeRootAbs, "ssot", "repos", scopeInfo.repo_id));
  }
  const mergedText = await readTextIfExists(mergedPath);
  let mergedJson = null;
  try {
    mergedJson = mergedText ? JSON.parse(mergedText) : null;
  } catch {
    mergedJson = null;
  }

  const prevQuestions = [];

  let agentInfo = { agent_id: null, llm_profile: null };
  try {
    const agentsText = await readTextIfExists("config/AGENTS.json");
    if (agentsText) {
      const parsed = JSON.parse(agentsText);
      if (!parsed || parsed.version !== 3 || !Array.isArray(parsed.agents)) throw new Error("Invalid config/AGENTS.json (expected version 3). Run: node src/cli.js --agents-migrate");
      const hasLegacyModel = parsed.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"));
      if (hasLegacyModel) throw new Error("AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate");
      agentInfo = pickInterviewerAgent({ agentsConfig: parsed });
    }
  } catch {
    agentInfo = { agent_id: null, llm_profile: null };
  }

  const profilesLoaded = await loadLlmProfiles();
  if (!profilesLoaded.ok) {
    const errObj = { timestamp: nowISO(), scope: scopeInfo.scope, action: "knowledge_interview_failed", agent_id: agentInfo.agent_id, error_type: "config", retryable: false, message: profilesLoaded.message };
    await writeText(logErrPath, JSON.stringify(errObj, null, 2) + "\n");
    return { ok: false, message: profilesLoaded.message, error_log: logErrPath };
  }
  const resolvedProfile = resolveLlmProfileOrError({ profiles: profilesLoaded.profiles, profileKey: agentInfo.llm_profile });
  if (!resolvedProfile.ok) {
    const errObj = { timestamp: nowISO(), scope: scopeInfo.scope, action: "knowledge_interview_failed", agent_id: agentInfo.agent_id, error_type: "config", retryable: false, message: resolvedProfile.message };
    await writeText(logErrPath, JSON.stringify(errObj, null, 2) + "\n");
    return { ok: false, message: resolvedProfile.message, error_log: logErrPath };
  }

  const timeoutMsRaw = process.env.KNOWLEDGE_TIMEOUT_MS;
  const timeoutMs = Number.isFinite(Number(timeoutMsRaw)) && Number(timeoutMsRaw) > 0 ? Number(timeoutMsRaw) : 60_000;

  let systemPrompt = null;
  try {
    const baseSys = readFileSync(resolve("src/llm/prompts/knowledge-interviewer.system.txt"), "utf8");
    const registryContext = await buildRegistryContextForInterviewer();
    systemPrompt = `${String(baseSys || "").trimEnd()}\n${registryContext}`;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const errObj = { timestamp: nowISO(), scope: scopeInfo.scope, action: "knowledge_interview_failed", agent_id: agentInfo.agent_id, error_type: "config", retryable: false, message: msg };
    await writeText(logErrPath, JSON.stringify(errObj, null, 2) + "\n");
    return { ok: false, message: msg, error_log: logErrPath };
  }

  const sdlcContext = buildSdlcContext({ knowledgeRootAbs, scope: scopeInfo.scope });

  const interview = await runArchitectInterview({
    scope: scopeInfo.scope,
    charterMd,
    mergedNotesJson: mergedJson,
    previousQuestions: prevQuestions,
    userSessionText: String(sessionText || ""),
    maxQuestions,
    llmConfig: { ...resolvedProfile.profile },
    timeoutMs,
    systemPrompt,
    sdlcContext,
  });

  if (!interview.ok) {
    const errObj = {
      timestamp: nowISO(),
      scope: scopeInfo.scope,
      action: "knowledge_interview_failed",
      agent_id: agentInfo.agent_id,
      ...interview.error,
      model: interview.model || resolvedProfile.profile.model,
      llm_profile: resolvedProfile.profile_key,
      raw: interview.raw || null,
    };
    await writeText(logErrPath, JSON.stringify(errObj, null, 2) + "\n");
    return { ok: false, message: `Knowledge interview failed: ${interview.error?.message || "unknown"}`, error: interview.error, error_log: logErrPath };
  }

  const sessionStem = `SESSION-${tsSafe}`;
  const sessionId = interview.sessionId || sessionStem;
  const sessionFile = join(knowledgeRootAbs, "sessions", `${sessionStem}.md`);

  const transcriptMd = renderTranscriptMd({
    scope: scopeInfo.scope,
    sessionId,
    charterTitle: null,
    questions: interview.questions,
    userSessionText: String(sessionText || ""),
  });

  if (!dryRun) {
    await writeText(sessionFile, transcriptMd);
  }
  // Lane A must not write runtime ledger entries.

  if (dryRun) {
    return {
      ok: true,
      dry_run: true,
      scope: scopeInfo.scope,
      knowledge_repo: knowledgeRepo,
      mode: runQuestionsOnly ? "questions_only" : "session_write",
      would_write: {
        knowledge_repo_files: [sessionFile, ...(runQuestionsOnly ? [] : [mergedPath])],
        runtime_files: [],
      },
      questions: interview.questions.map((q) => q.question),
    };
  }

  // Merge notes (canonical, stored in knowledge repo).
  let merged = null;
  if (!runQuestionsOnly) {
    merged = await mergeSessionNotesIntoMerged({
      mergedPath,
      scope: scopeInfo.scope,
      sessionFile,
      sessionNotes: interview.session_notes,
    });
  }

  // Optional machine-readable exports used by Lane B bridges.
  let wroteSeedsAbs = null;
  let wroteGapsAbs = null;
  if (!runQuestionsOnly) {
    const setsRes = await loadTeamsAndReposSetsForKnowledge();
    if (!setsRes.ok) {
      const failureSession = await writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message: setsRes.message });
      return { ok: false, message: setsRes.message, failure_session: failureSession };
    }

    const project_code = String(paths.cfg?.project_code || "").trim();

    if (!project_code) {
      const failureSession = await writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message: "PROJECT.json missing project_code." });
      return { ok: false, message: "PROJECT.json missing project_code.", failure_session: failureSession };
    }

    const hasSeeds = isPlainObject(interview.backlog_seeds);
    const hasGaps = isPlainObject(interview.gaps);

    if (hasSeeds) {
      const norm = normalizeBacklogSeedsFromInterview({ project_code, interviewBacklogSeeds: interview.backlog_seeds });
      if (!norm.ok) {
        const failureSession = await writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message: norm.message });
        return { ok: false, message: norm.message, failure_session: failureSession };
      }
      const v = validateBacklogSeeds(norm.raw, {
        teamsById: setsRes.teamsById,
        reposById: setsRes.reposById,
        expectedProjectCode: project_code,
      });
      if (!v.ok) {
        const failureSession = await writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message: "BACKLOG_SEEDS.json failed validation.", errors: v.errors });
        return { ok: false, message: "BACKLOG_SEEDS.json failed validation.", errors: v.errors, failure_session: failureSession };
      }
      wroteSeedsAbs = join(knowledgeRootAbs, "ssot", "system", "BACKLOG_SEEDS.json");
      await writeText(wroteSeedsAbs, JSON.stringify(v.normalized, null, 2) + "\n");
    }

    if (hasGaps) {
      const norm = normalizeGapsFromInterview({ project_code, interviewGaps: interview.gaps });
      if (!norm.ok) {
        const failureSession = await writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message: norm.message });
        return { ok: false, message: norm.message, failure_session: failureSession };
      }
      const v = validateGaps(norm.raw, {
        teamsById: setsRes.teamsById,
        reposById: setsRes.reposById,
        expectedProjectCode: project_code,
      });
      if (!v.ok) {
        const failureSession = await writeKnowledgeFailureSession({ knowledgeRootAbs, sessionStem, message: "GAPS.json failed validation.", errors: v.errors });
        return { ok: false, message: "GAPS.json failed validation.", errors: v.errors, failure_session: failureSession };
      }
      wroteGapsAbs = join(knowledgeRootAbs, "ssot", "system", "GAPS.json");
      await writeText(wroteGapsAbs, JSON.stringify(v.normalized, null, 2) + "\n");
    }
  }

  // Decisions needed (knowledge repo only; human-facing).
  const decisionsRes = await appendKnowledgeDecisions({ knowledgeRootAbs, scope: scopeInfo.scope, decisions: interview.session_notes.decisions_needed, dryRun: false });

  // Auto-commit and push in knowledge repo (mandatory unless --dry-run).
  {
    const decisionsMdAbs = join(knowledgeRootAbs, "decisions", "DECISIONS_NEEDED.md");
    const touched = [
      sessionFile,
      ...(runQuestionsOnly ? [] : [mergedPath]),
      ...(wroteSeedsAbs ? [wroteSeedsAbs] : []),
      ...(wroteGapsAbs ? [wroteGapsAbs] : []),
      ...(existsSync(decisionsMdAbs) ? [decisionsMdAbs] : []),
    ]
      .map((p) => resolve(String(p || "")))
      .filter(Boolean)
      .map((p) => relative(knowledgeRepo, p))
      .filter((p) => p && !p.startsWith(".."));

    const addRes = runGit({ cwd: knowledgeRepo, args: ["add", ...touched], label: "git add <touched>" });
    if (!addRes.ok) {
      return {
        ok: false,
        message: `Failed to git add knowledge outputs.\ncwd: ${knowledgeRepo}\nstdout: ${JSON.stringify(addRes.stdout.trim())}\nstderr: ${JSON.stringify(addRes.stderr.trim() || addRes.error || "")}`,
        knowledge_repo: knowledgeRepo,
      };
    }

    const branchRes = runGit({ cwd: knowledgeRepo, args: ["rev-parse", "--abbrev-ref", "HEAD"], label: "git rev-parse --abbrev-ref HEAD" });
    const branch = branchRes.ok ? branchRes.stdout.trim() : null;
    const commitMsg = `knowledge(${scopeTag}): session ${interview.sessionId || sessionStem} [${tsSafe}] (from run ${tsSafe})`;
    const commitRes = runGit({ cwd: knowledgeRepo, args: ["commit", "-m", commitMsg], label: "git commit -m <msg>" });
    if (!commitRes.ok && !String(commitRes.stderr || "").toLowerCase().includes("nothing to commit")) {
      return {
        ok: false,
        message: `Failed to git commit knowledge outputs.\ncwd: ${knowledgeRepo}\nstdout: ${JSON.stringify(commitRes.stdout.trim())}\nstderr: ${JSON.stringify(commitRes.stderr.trim() || commitRes.error || "")}`,
        knowledge_repo: knowledgeRepo,
      };
    }

    const shaRes = runGit({ cwd: knowledgeRepo, args: ["rev-parse", "HEAD"], label: "git rev-parse HEAD" });
    const commit = shaRes.ok ? shaRes.stdout.trim() : null;

    const originRes = getOriginUrl({ cwd: knowledgeRepo });
    if (!originRes.ok) {
      await appendKnowledgeDecisions({
        knowledgeRootAbs,
        scope: scopeInfo.scope,
        decisions: [
          { question: `Set origin for knowledge repo (${knowledgeRepo})`, A: "Configure origin and rerun knowledge interview.", B: "Skip push and manually manage knowledge repo.", recommended: "A" },
        ],
        dryRun: false,
      });
      return {
        ok: true,
        scope: scopeInfo.scope,
        session_file: sessionFile,
        notes_merged: runQuestionsOnly ? null : mergedPath,
        decisions_appended: decisionsRes.appended,
        knowledge_repo: knowledgeRepo,
        warnings: [originRes.warning],
        push_skipped: true,
        commit: commit || null,
        branch: branch || null,
      };
    }

    if (!branch || branch === "HEAD") {
      await appendKnowledgeDecisions({
        knowledgeRootAbs,
        scope: scopeInfo.scope,
        decisions: [
          { question: `Knowledge repo is in detached HEAD (${knowledgeRepo})`, A: "Checkout a branch in the knowledge repo and rerun.", B: "Keep commit locally and manage branches manually.", recommended: "A" },
        ],
        dryRun: false,
      });
      return { ok: false, message: `Knowledge repo is in detached HEAD; cannot push. Checkout a branch and rerun. (${knowledgeRepo})`, knowledge_repo: knowledgeRepo, commit };
    }

    const pushRes = runGit({ cwd: knowledgeRepo, args: ["push", "origin", branch], label: "git push origin <branch>" });
    if (!pushRes.ok) {
      const tail = String(pushRes.stderr || pushRes.stdout || pushRes.error || "").split("\n").slice(-12).join("\n");
      await appendKnowledgeDecisions({
        knowledgeRootAbs,
        scope: scopeInfo.scope,
        decisions: [
          { question: `Knowledge repo push failed (${knowledgeRepo})`, A: `Fix credentials/permissions and push the commit (branch: ${branch}).`, B: "Keep commit locally and push later.", recommended: "A" },
        ],
        dryRun: false,
      });
      return { ok: false, message: `Knowledge repo push failed. Commit kept locally.\n${tail}`, knowledge_repo: knowledgeRepo, commit, branch };
    }
  }

  return {
    ok: true,
    scope: scopeInfo.scope,
    session_file: sessionFile,
    notes_merged: runQuestionsOnly ? null : mergedPath,
    decisions_appended: decisionsRes.appended,
    knowledge_repo: knowledgeRepo,
  };
}
