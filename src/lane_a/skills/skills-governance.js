import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry } from "../../utils/repo-registry.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { jsonStableStringify } from "../../utils/json.js";
import { validateSkillPackage, validateSkillsGovernanceStatus, validateSkillsRegistry } from "../../contracts/validators/index.js";
import { loadGlobalSkillsRegistry, loadProjectSkills } from "../../skills/skills-loader.js";
import { runSkillsDraft } from "./skills-draft.js";
import { runSkillsAuthor } from "./skill-author.js";

function nowIso() {
  return new Date().toISOString();
}

function normStr(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeLf(text) {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function boolFromEnv(name, fallback = false) {
  const raw = normStr(process.env[name]).toLowerCase();
  if (!raw) return fallback;
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

function intFromEnv(name, fallback, min = 0) {
  const raw = normStr(process.env[name]);
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, parsed);
}

function governanceEnv() {
  return {
    enabled: boolFromEnv("ENABLE_SKILLS_GOVERNANCE", false),
    draft_daily_cap: intFromEnv("SKILLS_GOV_DRAFT_DAILY_CAP", 10, 0),
    min_reuse_repos: intFromEnv("SKILLS_GOV_MIN_REUSE_REPOS", 2, 1),
    min_evidence_refs: intFromEnv("SKILLS_GOV_MIN_EVIDENCE_REFS", 3, 1),
    auto_author: boolFromEnv("SKILLS_GOV_AUTO_AUTHOR", false),
    require_approval: boolFromEnv("SKILLS_GOV_REQUIRE_APPROVAL", true),
  };
}

function defaultAiTeamRepoRoot() {
  const envRoot = normStr(process.env.AI_TEAM_REPO);
  if (envRoot) return resolve(envRoot);
  return resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
}

function resolveAiTeamRepoRoot(aiTeamRepoRoot = null) {
  const root = normStr(aiTeamRepoRoot) || defaultAiTeamRepoRoot();
  if (!isAbsolute(root)) throw new Error(`aiTeamRepoRoot must be absolute (got: ${root}).`);
  return resolve(root);
}

function relFromOps(paths, absPath) {
  const rel = relative(paths.opsRootAbs, resolve(absPath)).replaceAll("\\", "/");
  return rel || ".";
}

function slugifyToken(value) {
  return normStr(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function hashShort(text, len = 10) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex").slice(0, len);
}

function parseScopeStaleFlag(statusObj) {
  if (!statusObj || typeof statusObj !== "object") return false;
  return statusObj.stale === true || statusObj.hard_stale === true || statusObj.degraded === true;
}

function tokensFromText(text) {
  const stop = new Set([
    "src",
    "lib",
    "test",
    "tests",
    "docs",
    "doc",
    "dist",
    "build",
    "main",
    "index",
    "file",
    "files",
    "json",
    "yaml",
    "yml",
    "controller",
    "controllers",
    "service",
    "services",
    "module",
    "modules",
    "utils",
    "common",
    "repo",
    "repos",
    "system",
  ]);
  const raw = String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9/._-]+/g, " ");
  const parts = raw
    .split(/[\/._\-\s]+/)
    .map((part) => part.trim())
    .filter(Boolean);
  const out = [];
  for (const part of parts) {
    if (part.length < 3) continue;
    if (/^\d+$/.test(part)) continue;
    if (stop.has(part)) continue;
    out.push(part);
  }
  return out;
}

async function readJsonAbsOptional(absPath) {
  try {
    const raw = await readFile(absPath, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(raw || "")) };
  } catch (err) {
    if (err && err.code === "ENOENT") return { ok: true, exists: false, json: null };
    return { ok: false, exists: false, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

async function readJsonlEvidence(absPath) {
  const text = await readFile(absPath, "utf8").catch(() => "");
  const out = [];
  for (const line of String(text).split("\n").map((part) => part.trim()).filter(Boolean)) {
    try {
      const parsed = JSON.parse(line);
      const evidenceId = normStr(parsed?.evidence_id);
      const filePath = normStr(parsed?.file_path);
      if (!evidenceId || !filePath) continue;
      out.push({ evidence_id: evidenceId, file_path: filePath });
    } catch {
      continue;
    }
  }
  return out;
}

async function loadDraftRecords(paths) {
  const draftsDirAbs = join(paths.laneA.skillsDirAbs, "drafts");
  const entries = await readdir(draftsDirAbs, { withFileTypes: true }).catch(() => []);
  const records = [];
  for (const entry of entries.filter((item) => item.isFile() && item.name.endsWith(".json")).sort((a, b) => a.name.localeCompare(b.name))) {
    const abs = join(draftsDirAbs, entry.name);
    // eslint-disable-next-line no-await-in-loop
    const loaded = await readJsonAbsOptional(abs);
    if (!loaded.ok || !loaded.exists || !loaded.json || typeof loaded.json !== "object") continue;
    const draftId = normStr(loaded.json.draft_id) || entry.name.replace(/\.json$/i, "");
    const status = normStr(loaded.json.status).toLowerCase() || "pending";
    const candidateSkillId = slugifyToken(loaded.json.candidate_skill_id || loaded.json.candidateSkillId || "");
    records.push({
      draft_id: draftId,
      status,
      candidate_skill_id: candidateSkillId,
      is_refresh: draftId.startsWith("DRAFT_REFRESH_"),
      path_abs: abs,
    });
  }
  return records;
}

async function loadApprovalRecords(paths) {
  const approvalsDirAbs = paths.laneA.skillsGovernanceApprovalsAbs;
  const entries = await readdir(approvalsDirAbs, { withFileTypes: true }).catch(() => []);
  const records = [];
  for (const entry of entries.filter((item) => item.isFile() && item.name.startsWith("APPROVAL-") && item.name.endsWith(".json")).sort((a, b) => a.name.localeCompare(b.name))) {
    const abs = join(approvalsDirAbs, entry.name);
    // eslint-disable-next-line no-await-in-loop
    const loaded = await readJsonAbsOptional(abs);
    if (!loaded.ok || !loaded.exists || !loaded.json || typeof loaded.json !== "object") continue;
    const draftId = normStr(loaded.json.draft_id);
    const decision = normStr(loaded.json.decision).toLowerCase();
    if (!draftId || (decision !== "approved" && decision !== "rejected")) continue;
    records.push({ draft_id: draftId, decision, path_abs: abs, created_at: normStr(loaded.json.created_at) });
  }
  return records;
}

function detectTodayPrefix() {
  const ts = nowFsSafeUtcTimestamp();
  return ts.slice(0, 8);
}

function countDraftsCreatedToday(draftRecords) {
  const today = detectTodayPrefix();
  return draftRecords.filter((record) => record.draft_id.startsWith(`DRAFT-${today}_`)).length;
}

async function collectTokenSignals({ paths, activeRepoIds }) {
  const tokenMap = new Map();
  const allEvidenceIds = new Set();
  for (const repoId of activeRepoIds) {
    const evidenceRefs = await readJsonlEvidence(join(paths.knowledge.evidenceReposAbs, repoId, "evidence_refs.jsonl"));
    const repoIndexRes = await readJsonAbsOptional(join(paths.knowledge.evidenceIndexReposAbs, repoId, "repo_index.json"));
    const repoIndex = repoIndexRes.ok && repoIndexRes.exists && repoIndexRes.json && typeof repoIndexRes.json === "object" ? repoIndexRes.json : {};

    const repoTokens = new Set();
    for (const ref of evidenceRefs) {
      allEvidenceIds.add(ref.evidence_id);
      for (const token of tokensFromText(ref.file_path)) repoTokens.add(token);
      for (const token of tokensFromText(`${ref.file_path}:${ref.evidence_id}`)) {
        const cur = tokenMap.get(token) || { repos: new Set(), evidence_refs: new Set() };
        cur.repos.add(repoId);
        cur.evidence_refs.add(ref.evidence_id);
        tokenMap.set(token, cur);
      }
    }

    const indexPaths = [];
    const entrypoints = Array.isArray(repoIndex?.entrypoints) ? repoIndex.entrypoints : [];
    const openapiFiles = Array.isArray(repoIndex?.api_surface?.openapi_files) ? repoIndex.api_surface.openapi_files : [];
    const routeControllers = Array.isArray(repoIndex?.api_surface?.routes_controllers) ? repoIndex.api_surface.routes_controllers : [];
    const eventsTopics = Array.isArray(repoIndex?.api_surface?.events_topics) ? repoIndex.api_surface.events_topics : [];
    const migrations = Array.isArray(repoIndex?.migrations_schema) ? repoIndex.migrations_schema : [];
    indexPaths.push(...entrypoints, ...openapiFiles, ...routeControllers, ...eventsTopics, ...migrations);
    for (const path of indexPaths) {
      for (const token of tokensFromText(path)) repoTokens.add(token);
    }

    for (const token of repoTokens) {
      const cur = tokenMap.get(token) || { repos: new Set(), evidence_refs: new Set() };
      cur.repos.add(repoId);
      tokenMap.set(token, cur);
    }
  }
  return { tokenMap, allEvidenceIds };
}

function buildCandidateSkillId(token) {
  return slugifyToken(`${token}-shared-pattern`);
}

function buildCandidateId({ token, repos, evidenceRefs }) {
  return `${slugifyToken(token)}-${hashShort(`${token}|${repos.join(",")}|${evidenceRefs.join(",")}`, 8)}`;
}

function candidateComparator(a, b) {
  if (a.detected_in_repos.length !== b.detected_in_repos.length) return b.detected_in_repos.length - a.detected_in_repos.length;
  if (a.evidence_refs.length !== b.evidence_refs.length) return b.evidence_refs.length - a.evidence_refs.length;
  return a.candidate_skill_id.localeCompare(b.candidate_skill_id);
}

async function writeCandidateArtifact({ paths, candidate, ts, sequence }) {
  const suffix = String(sequence).padStart(3, "0");
  const fileName = `CAND-${ts}_${suffix}.json`;
  const abs = join(paths.laneA.skillsGovernanceCandidatesAbs, fileName);
  await writeFile(abs, jsonStableStringify(candidate), "utf8");
  return abs;
}

async function evaluateSkillStaleness({ paths, aiTeamRepoRootAbs, registrySkills, knownEvidenceIds }) {
  const stale = [];
  const scopeStaleCache = new Map();
  const staleScope = async (scope) => {
    const s = normStr(scope) || "system";
    if (scopeStaleCache.has(s)) return scopeStaleCache.get(s);
    let isStale = false;
    if (s === "system") {
      const loaded = await readJsonAbsOptional(join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json"));
      isStale = loaded.ok && loaded.exists ? parseScopeStaleFlag(loaded.json) : false;
    } else {
      const match = s.match(/^repo:([a-z0-9._-]+)$/i);
      if (match) {
        const loaded = await readJsonAbsOptional(join(paths.knowledge.ssotReposAbs, match[1], "committee", "committee_status.json"));
        isStale = loaded.ok && loaded.exists ? parseScopeStaleFlag(loaded.json) : false;
      }
    }
    scopeStaleCache.set(s, isStale);
    return isStale;
  };

  const skillIds = Object.keys(registrySkills || {}).sort((a, b) => a.localeCompare(b));
  for (const skillId of skillIds) {
    const skillJsonAbs = join(aiTeamRepoRootAbs, "skills", skillId, "skill.json");
    const skillMdAbs = join(aiTeamRepoRootAbs, "skills", skillId, "skill.md");
    let metadata;
    let skillMd;
    try {
      // eslint-disable-next-line no-await-in-loop
      const jsonText = await readFile(skillJsonAbs, "utf8");
      // eslint-disable-next-line no-await-in-loop
      const mdText = await readFile(skillMdAbs, "utf8");
      metadata = JSON.parse(String(jsonText || ""));
      skillMd = normalizeLf(mdText);
      validateSkillPackage(metadata, { skillMd });
    } catch {
      stale.push(skillId);
      continue;
    }

    const refs = Array.isArray(metadata.evidence_refs) ? metadata.evidence_refs.map((value) => String(value).trim()).filter(Boolean) : [];
    if (refs.some((ref) => !knownEvidenceIds.has(ref))) {
      stale.push(skillId);
      continue;
    }
    // eslint-disable-next-line no-await-in-loop
    const scopeIsStale = await staleScope(metadata.source_scope);
    if (scopeIsStale) stale.push(skillId);
  }

  return stale.sort((a, b) => a.localeCompare(b));
}

function renderStatusMd({ status, paths, runStats }) {
  const lines = [];
  lines.push("# Skills Governance Status");
  lines.push("");
  lines.push("## Current policy (env)");
  lines.push("");
  lines.push(`- enabled: ${status.env.enabled}`);
  lines.push(`- draft_daily_cap: ${status.env.draft_daily_cap}`);
  lines.push(`- min_reuse_repos: ${status.env.min_reuse_repos}`);
  lines.push(`- min_evidence_refs: ${status.env.min_evidence_refs}`);
  lines.push(`- auto_author: ${status.env.auto_author}`);
  lines.push(`- require_approval: ${status.env.require_approval}`);
  lines.push("");
  lines.push("## New candidates/drafts");
  lines.push("");
  lines.push(`- candidates_created_this_run: ${status.candidates_created_this_run}`);
  lines.push(`- drafts_created_this_run: ${status.drafts_created_this_run}`);
  if (Array.isArray(runStats?.new_candidate_paths) && runStats.new_candidate_paths.length) {
    for (const rel of runStats.new_candidate_paths) lines.push(`- candidate: \`${rel}\``);
  } else lines.push("- candidate: (none)");
  if (Array.isArray(runStats?.new_draft_ids) && runStats.new_draft_ids.length) {
    for (const draftId of runStats.new_draft_ids) lines.push(`- draft: \`${draftId}\``);
  } else lines.push("- draft: (none)");
  lines.push("");
  lines.push("## Pending approvals");
  lines.push("");
  lines.push(`- approved: ${(status.approvals.approved || []).join(", ") || "(none)"}`);
  lines.push(`- rejected: ${(status.approvals.rejected || []).join(", ") || "(none)"}`);
  lines.push(`- pending drafts: ${(status.drafts.pending || []).join(", ") || "(none)"}`);
  lines.push("");
  lines.push("## Stale skills and recommended refresh");
  lines.push("");
  lines.push(`- stale skills: ${(status.skills.stale || []).join(", ") || "(none)"}`);
  lines.push(`- refresh pending drafts: ${(status.drafts.refresh_pending || []).join(", ") || "(none)"}`);
  lines.push("");
  lines.push("## Artifact paths");
  lines.push("");
  lines.push(`- status.json: \`${relFromOps(paths, paths.laneA.skillsGovernanceStatusJsonAbs)}\``);
  lines.push(`- candidates/: \`${relFromOps(paths, paths.laneA.skillsGovernanceCandidatesAbs)}\``);
  lines.push(`- approvals/: \`${relFromOps(paths, paths.laneA.skillsGovernanceApprovalsAbs)}\``);
  lines.push(`- runs/: \`${relFromOps(paths, paths.laneA.skillsGovernanceRunsAbs)}\``);
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function ensureGovernanceDirs(paths) {
  const dirs = [paths.laneA.skillsGovernanceAbs, paths.laneA.skillsGovernanceCandidatesAbs, paths.laneA.skillsGovernanceApprovalsAbs, paths.laneA.skillsGovernanceRunsAbs];
  for (const dir of dirs) {
    // eslint-disable-next-line no-await-in-loop
    await mkdir(dir, { recursive: true });
  }
}

function buildStatusObject({
  paths,
  env,
  knownSkills,
  staleSkills,
  draftRecords,
  approvalRecords,
  runStats,
  notes = undefined,
}) {
  const approvalsApproved = approvalRecords.filter((record) => record.decision === "approved").map((record) => record.draft_id).sort((a, b) => a.localeCompare(b));
  const approvalsRejected = approvalRecords.filter((record) => record.decision === "rejected").map((record) => record.draft_id).sort((a, b) => a.localeCompare(b));

  const pendingDrafts = draftRecords.filter((record) => record.status === "pending" && !record.is_refresh).map((record) => record.draft_id).sort((a, b) => a.localeCompare(b));
  const publishedDrafts = draftRecords.filter((record) => record.status === "published").map((record) => record.draft_id).sort((a, b) => a.localeCompare(b));
  const refreshPending = draftRecords.filter((record) => record.status === "pending" && record.is_refresh).map((record) => record.draft_id).sort((a, b) => a.localeCompare(b));

  const status = {
    version: 1,
    projectRoot: paths.opsRootAbs,
    captured_at: nowIso(),
    env,
    skills: {
      total: knownSkills.length,
      known: knownSkills,
      stale: staleSkills,
    },
    drafts: {
      pending: pendingDrafts,
      published: publishedDrafts,
      refresh_pending: refreshPending,
    },
    approvals: {
      approved: approvalsApproved,
      rejected: approvalsRejected,
    },
    candidates_created_this_run: runStats?.candidates_created_this_run || 0,
    drafts_created_this_run: runStats?.drafts_created_this_run || 0,
    ...(typeof notes === "string" && notes.trim() ? { notes: notes.trim() } : {}),
  };
  validateSkillsGovernanceStatus(status);
  return status;
}

async function detectGovernanceCandidates({
  paths,
  env,
  knownSkillsSet,
  draftRecords,
}) {
  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { candidates: [], note: reposRes.message };
  const activeRepoIds = (Array.isArray(reposRes.registry?.repos) ? reposRes.registry.repos : [])
    .filter((repo) => normStr(repo?.status).toLowerCase() === "active")
    .map((repo) => normStr(repo?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  if (!activeRepoIds.length) return { candidates: [], note: "No active repos found." };
  const { tokenMap } = await collectTokenSignals({ paths, activeRepoIds });

  const existingDraftSkillIds = new Set(draftRecords.map((record) => record.candidate_skill_id).filter(Boolean));
  const candidates = [];
  for (const token of Array.from(tokenMap.keys()).sort((a, b) => a.localeCompare(b))) {
    const signal = tokenMap.get(token);
    const repos = Array.from(signal?.repos || []).sort((a, b) => a.localeCompare(b));
    const evidenceRefs = Array.from(signal?.evidence_refs || []).sort((a, b) => a.localeCompare(b));
    if (repos.length < env.min_reuse_repos) continue;
    if (evidenceRefs.length < env.min_evidence_refs) continue;
    const candidateSkillId = buildCandidateSkillId(token);
    if (!candidateSkillId) continue;
    if (knownSkillsSet.has(candidateSkillId)) continue;
    if (existingDraftSkillIds.has(candidateSkillId)) continue;
    const candidate = {
      version: 1,
      candidate_id: buildCandidateId({ token, repos, evidenceRefs }),
      created_at: nowIso(),
      candidate_skill_id: candidateSkillId,
      reason: `token '${token}' detected in ${repos.length} repos with ${evidenceRefs.length} evidence refs`,
      detected_in_repos: repos,
      evidence_refs: evidenceRefs,
      status: "pending",
      recommended_action: "draft",
    };
    candidates.push(candidate);
  }
  candidates.sort(candidateComparator);
  return { candidates, note: null };
}

async function maybeAutoAuthorDrafts({
  env,
  paths,
  aiTeamRepoRootAbs,
  draftRecords,
  approvalRecords,
  knownSkillsSet,
}) {
  const out = {
    attempted: 0,
    created: 0,
    skipped_existing: [],
    errors: [],
  };
  if (!env.auto_author || env.require_approval) return out;

  const approvalByDraftId = new Map();
  for (const record of approvalRecords) approvalByDraftId.set(record.draft_id, record.decision);
  const pending = draftRecords.filter((record) => record.status === "pending" && !record.is_refresh).sort((a, b) => a.draft_id.localeCompare(b.draft_id));
  for (const draft of pending) {
    if (!draft.candidate_skill_id) continue;
    if (knownSkillsSet.has(draft.candidate_skill_id)) {
      out.skipped_existing.push(draft.draft_id);
      continue;
    }
    const decision = approvalByDraftId.get(draft.draft_id);
    if (decision === "rejected") continue;
    out.attempted += 1;
    try {
      // eslint-disable-next-line no-await-in-loop
      const res = await runSkillsAuthor({
        projectRoot: paths.opsRootAbs,
        draftId: draft.draft_id,
        aiTeamRepoRoot: aiTeamRepoRootAbs,
        failIfSkillExists: true,
      });
      if (res?.ok) {
        out.created += 1;
        knownSkillsSet.add(String(res.skill_id || ""));
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("already exists in SKILLS registry")) out.skipped_existing.push(draft.draft_id);
      else out.errors.push({ draft_id: draft.draft_id, message: msg });
    }
  }
  out.skipped_existing.sort((a, b) => a.localeCompare(b));
  return out;
}

async function writeRunArtifact(paths, runObj) {
  const ts = nowFsSafeUtcTimestamp();
  const runAbs = join(paths.laneA.skillsGovernanceRunsAbs, `RUN-${ts}.json`);
  await writeFile(runAbs, jsonStableStringify(runObj), "utf8");
  return runAbs;
}

export async function writeSkillsGovernanceApproval({
  projectRoot,
  draftId,
  decision,
  by,
  notes = "",
  dryRun = false,
} = {}) {
  const dId = normStr(draftId);
  const actor = normStr(by);
  const dec = normStr(decision).toLowerCase();
  if (!dId) throw new Error("Missing --draft <draft_id>.");
  if (!actor) throw new Error("Missing --by <name>.");
  if (dec !== "approved" && dec !== "rejected") throw new Error("decision must be approved|rejected.");

  const paths = await loadProjectPaths({ projectRoot });
  const fileSafeDraftId = dId.replace(/[^A-Za-z0-9._-]+/g, "_");
  const abs = join(paths.laneA.skillsGovernanceApprovalsAbs, `APPROVAL-${fileSafeDraftId}.json`);
  const artifact = {
    version: 1,
    draft_id: dId,
    decision: dec,
    by: actor,
    notes: normStr(notes),
    created_at: nowIso(),
  };

  if (!dryRun) {
    await ensureGovernanceDirs(paths);
    await writeFile(abs, jsonStableStringify(artifact), "utf8");
  }
  return { ok: true, dry_run: !!dryRun, path: abs, approval: artifact };
}

export async function runSkillsGovernance({
  projectRoot,
  run = false,
  status = false,
  aiTeamRepoRoot = null,
  dryRun = false,
} = {}) {
  const shouldRun = !!run;
  const shouldStatus = status || !run;
  const env = governanceEnv();
  const paths = await loadProjectPaths({ projectRoot });

  if (!env.enabled) {
    return {
      ok: true,
      projectRoot: paths.opsRootAbs,
      env,
      notes: "skills governance disabled (ENABLE_SKILLS_GOVERNANCE!=1)",
      wrote: null,
      status: null,
    };
  }

  const aiTeamRepoRootAbs = resolveAiTeamRepoRoot(aiTeamRepoRoot);
  const runStats = {
    candidates_created_this_run: 0,
    drafts_created_this_run: 0,
    new_candidate_paths: [],
    new_draft_ids: [],
    auto_author: null,
    errors: [],
  };

  if (!dryRun && (shouldRun || shouldStatus)) await ensureGovernanceDirs(paths);

  let finalStatus = null;
  let runAbs = null;

  try {
    const globalRegistry = await loadGlobalSkillsRegistry({ aiTeamRepoRoot: aiTeamRepoRootAbs });
    validateSkillsRegistry(globalRegistry);
    const knownSkills = Object.keys(globalRegistry.skills || {}).sort((a, b) => a.localeCompare(b));
    const knownSkillsSet = new Set(knownSkills);

    await loadProjectSkills({ projectRoot: paths.opsRootAbs }).catch(() => null);

    let draftRecords = await loadDraftRecords(paths);
    let approvalRecords = await loadApprovalRecords(paths);

    if (shouldRun) {
      const detection = await detectGovernanceCandidates({
        paths,
        env,
        knownSkillsSet,
        draftRecords,
      });
      if (detection.note) runStats.errors.push({ stage: "detect", message: detection.note });

      const alreadyToday = countDraftsCreatedToday(draftRecords);
      const allowedRemaining = Math.max(0, env.draft_daily_cap - alreadyToday);
      const ts = nowFsSafeUtcTimestamp();
      let created = 0;
      for (const candidate of detection.candidates) {
        if (created >= allowedRemaining) break;
        if (!dryRun) {
          // eslint-disable-next-line no-await-in-loop
          const candAbs = await writeCandidateArtifact({ paths, candidate, ts, sequence: created + 1 });
          runStats.new_candidate_paths.push(relFromOps(paths, candAbs));
        }
        runStats.candidates_created_this_run += 1;

        const draftRes = await runSkillsDraft({
          projectRoot: paths.opsRootAbs,
          scope: "system",
          prefill: {
            candidateSkillId: candidate.candidate_skill_id,
            reason: candidate.reason,
            evidenceRefs: candidate.evidence_refs,
          },
          dryRun,
        });
        if (draftRes?.ok) {
          runStats.drafts_created_this_run += 1;
          runStats.new_draft_ids.push(draftRes.draft_id);
        }
        created += 1;
      }

      if (env.auto_author) {
        draftRecords = await loadDraftRecords(paths);
        approvalRecords = await loadApprovalRecords(paths);
        runStats.auto_author = await maybeAutoAuthorDrafts({
          env,
          paths,
          aiTeamRepoRootAbs,
          draftRecords,
          approvalRecords,
          knownSkillsSet,
        });
      }
    }

    draftRecords = await loadDraftRecords(paths);
    approvalRecords = await loadApprovalRecords(paths);

    const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
    const activeRepoIds = reposRes.ok
      ? (Array.isArray(reposRes.registry?.repos) ? reposRes.registry.repos : [])
          .filter((repo) => normStr(repo?.status).toLowerCase() === "active")
          .map((repo) => normStr(repo?.repo_id))
          .filter(Boolean)
      : [];
    const { allEvidenceIds } = await collectTokenSignals({ paths, activeRepoIds });
    const staleSkills = await evaluateSkillStaleness({
      paths,
      aiTeamRepoRootAbs,
      registrySkills: globalRegistry.skills || {},
      knownEvidenceIds: allEvidenceIds,
    });

    finalStatus = buildStatusObject({
      paths,
      env,
      knownSkills,
      staleSkills,
      draftRecords,
      approvalRecords,
      runStats,
      notes: runStats.errors.length ? runStats.errors.map((item) => `${item.stage || "run"}:${item.message}`).join(" | ") : undefined,
    });

    if (!dryRun && shouldStatus) {
      await writeFile(paths.laneA.skillsGovernanceStatusJsonAbs, jsonStableStringify(finalStatus), "utf8");
      await writeFile(paths.laneA.skillsGovernanceStatusMdAbs, renderStatusMd({ status: finalStatus, paths, runStats }), "utf8");
    }

    if (!dryRun && shouldRun) {
      runAbs = await writeRunArtifact(paths, {
        version: 1,
        created_at: nowIso(),
        ok: true,
        env,
        stats: {
          candidates_created_this_run: runStats.candidates_created_this_run,
          drafts_created_this_run: runStats.drafts_created_this_run,
          auto_author: runStats.auto_author,
          errors: runStats.errors,
        },
      });
    }

    return {
      ok: true,
      projectRoot: paths.opsRootAbs,
      env,
      status: finalStatus,
      wrote: {
        ...(shouldStatus ? { status_json: paths.laneA.skillsGovernanceStatusJsonAbs, status_md: paths.laneA.skillsGovernanceStatusMdAbs } : {}),
        ...(runAbs ? { run_json: runAbs } : {}),
      },
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (!dryRun && shouldRun) {
      await ensureGovernanceDirs(paths);
      runAbs = await writeRunArtifact(paths, {
        version: 1,
        created_at: nowIso(),
        ok: false,
        env,
        error: message,
      });
    }
    return {
      ok: false,
      projectRoot: paths.opsRootAbs,
      env,
      message,
      wrote: runAbs ? { run_json: runAbs } : null,
    };
  }
}

