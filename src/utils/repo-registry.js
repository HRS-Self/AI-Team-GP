import { resolve, isAbsolute, join } from "node:path";
import { stat } from "node:fs/promises";
import { readFile } from "node:fs/promises";
import { readTextIfExists } from "./fs.js";
import { loadProjectPaths } from "../paths/project-paths.js";

export const REPO_REGISTRY_PATH = "config/REPOS.json";
export const AGENT_REGISTRY_PATH = "config/AGENTS.json";

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeRefToken(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/-+/g, "-");
}

function containsTokenExact(haystack, needle) {
  const n = String(needle || "");
  if (!n) return false;
  const re = new RegExp(`(^|[^A-Za-z0-9_\\-/])${escapeRegExp(n)}([^A-Za-z0-9_\\-/]|$)`);
  return re.test(String(haystack || ""));
}

function containsTokenNormalized(haystack, needle) {
  const n = normalizeRefToken(needle);
  if (!n) return false;
  const h = normalizeRefToken(haystack);
  const re = new RegExp(`(^|[^a-z0-9])${escapeRegExp(n)}([^a-z0-9]|$)`);
  return re.test(h);
}

export function findExplicitRepoReferences({ intakeText, registry }) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const intake = String(intakeText || "");

  const priority = new Map([
    ["repo_id_exact", 1],
    ["name_exact", 2],
    ["path_exact", 3],
    ["repo_id_normalized", 4],
    ["name_normalized", 5],
    ["path_normalized", 6],
  ]);

  const bestByRepoId = new Map();
  for (const r of repos) {
    const repo_id = String(r?.repo_id || "").trim();
    if (!repo_id) continue;

    const name = String(r?.name || "").trim();
    const path = String(r?.path || "").trim();

    const candidates = [];

    if (repo_id && containsTokenExact(intake, repo_id)) candidates.push({ match_type: "repo_id_exact", matched_token: repo_id });
    if (name && containsTokenExact(intake, name)) candidates.push({ match_type: "name_exact", matched_token: name });
    if (path && containsTokenExact(intake, path)) candidates.push({ match_type: "path_exact", matched_token: path });

    if (repo_id && containsTokenNormalized(intake, repo_id)) candidates.push({ match_type: "repo_id_normalized", matched_token: normalizeRefToken(repo_id) });
    if (name && containsTokenNormalized(intake, name)) candidates.push({ match_type: "name_normalized", matched_token: normalizeRefToken(name) });
    if (path && containsTokenNormalized(intake, path)) candidates.push({ match_type: "path_normalized", matched_token: normalizeRefToken(path) });

    if (!candidates.length) continue;

    candidates.sort((a, b) => (priority.get(a.match_type) || 999) - (priority.get(b.match_type) || 999));
    const best = candidates[0];
    bestByRepoId.set(repo_id, {
      repo_id,
      team_id: String(r?.team_id || "").trim() || null,
      status: String(r?.status || "").trim().toLowerCase() || null,
      match_type: best.match_type,
      matched_token: best.matched_token,
      confidence: 1.0,
    });
  }

  return Array.from(bestByRepoId.values()).sort((a, b) => a.repo_id.localeCompare(b.repo_id));
}

export async function loadRepoRegistry({ projectRoot = null } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const abs = join(paths.opsConfigAbs, "REPOS.json");
  let text = null;
  try {
    text = await readFile(abs, "utf8");
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) text = null;
    else throw err;
  }
  // Back-compat: allow tests/tools that still rely on resolveStatePath for config reads.
  if (!text) text = await readTextIfExists(REPO_REGISTRY_PATH);
  if (!text) return { ok: false, missing: true, message: `Repo registry not configured (${REPO_REGISTRY_PATH} missing).` };
  try {
    const parsed = JSON.parse(text);
    if (!parsed || parsed.version !== 1) return { ok: false, message: `Invalid ${REPO_REGISTRY_PATH}: expected version=1.` };
    if (typeof parsed.base_dir !== "undefined") return { ok: false, message: `Invalid ${REPO_REGISTRY_PATH}: 'base_dir' is deprecated. Use config/PROJECT.json.repos_root_abs as REPOS_ROOT.` };
    if (!Array.isArray(parsed.repos)) return { ok: false, message: `Invalid ${REPO_REGISTRY_PATH}: repos[] missing.` };
    return { ok: true, registry: { ...parsed, base_dir: paths.reposRootAbs } };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid ${REPO_REGISTRY_PATH}: JSON parse failed (${msg}).` };
  }
}

export function resolveRepoAbsPath({ baseDir, repoPath }) {
  const p = String(repoPath || "").trim();
  if (!p) return null;
  if (isAbsolute(p)) return p;
  return resolve(String(baseDir || "").trim(), p);
}

export async function pathExists(absPath) {
  try {
    const s = await stat(absPath);
    return s.isDirectory();
  } catch (err) {
    if (err && err.code === "ENOENT") return false;
    return false;
  }
}

function matchKeywords(text, keywords) {
  const lower = String(text || "").toLowerCase();
  const matched = [];
  for (const raw of Array.isArray(keywords) ? keywords : []) {
    const kw = String(raw || "").trim().toLowerCase();
    if (!kw) continue;
    if (lower.includes(kw)) matched.push(String(raw));
  }
  return matched;
}

export function selectReposForRouting({ intakeText, selectedTeams, registry, topNPerTeam = 1 }) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const active = repos.filter((r) => String(r?.status || "").trim().toLowerCase() === "active");

  const selected = new Set();
  const repoScores = {};
  const repoMatches = [];

  for (const teamId of (selectedTeams || []).slice()) {
    const candidates = active
      .filter((r) => String(r?.team_id || "").trim() === String(teamId))
      .slice()
      .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

    if (!candidates.length) continue;

    const scored = candidates.map((r) => {
      const matched = matchKeywords(intakeText, r.keywords);
      return { repo_id: r.repo_id, matched, score: matched.length };
    });

    const bestScore = scored.reduce((m, x) => (x.score > m ? x.score : m), 0);
    const best = scored
      .filter((x) => x.score === bestScore)
      .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

    const toSelect = best.slice(0, Math.max(1, Number.isFinite(topNPerTeam) ? topNPerTeam : 1));
    for (const s of toSelect) {
      selected.add(String(s.repo_id));
      repoScores[String(s.repo_id)] = s.score;
      repoMatches.push({ repo_id: String(s.repo_id), matched_keywords: s.matched.slice().sort((a, b) => a.localeCompare(b)) });
    }
  }

  // Archived explicit mention: allow selecting but mark high risk at routing layer (handled by caller).
  const archived = repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "archived")
    .slice()
    .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

  const explicitlyMentionedArchived = [];
  for (const r of archived) {
    const id = String(r.repo_id || "").trim();
    const name = String(r.name || "").trim();
    const lower = String(intakeText || "").toLowerCase();
    if (id && lower.includes(id.toLowerCase())) explicitlyMentionedArchived.push(id);
    else if (name && lower.includes(name.toLowerCase())) explicitlyMentionedArchived.push(id);
  }

  // If mentioned, include deterministically (sorted) even if not active.
  for (const repoId of explicitlyMentionedArchived.sort((a, b) => a.localeCompare(b))) {
    selected.add(repoId);
    if (typeof repoScores[repoId] !== "number") repoScores[repoId] = 0;
    if (!repoMatches.some((m) => m.repo_id === repoId)) repoMatches.push({ repo_id: repoId, matched_keywords: [] });
  }

  const selectedRepos = Array.from(selected).sort((a, b) => a.localeCompare(b));
  const matches = repoMatches
    .slice()
    .sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)));

  return { selected_repos: selectedRepos, repo_scores: repoScores, repo_matches: matches, archived_explicit_mentions: explicitlyMentionedArchived };
}

export async function getRepoPathsForWork({ workId }) {
  const routingText = await readTextIfExists(`ai/lane_b/work/${workId}/ROUTING.json`);
  if (!routingText) return { ok: false, message: `Missing ai/lane_b/work/${workId}/ROUTING.json.` };

  let routing;
  try {
    routing = JSON.parse(routingText);
  } catch {
    return { ok: false, message: `Invalid JSON in ai/lane_b/work/${workId}/ROUTING.json.` };
  }

  const selectedRepos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : null;

  const loaded = await loadRepoRegistry();
  if (!loaded.ok) {
    return {
      ok: true,
      configured: false,
      message: loaded.message,
      selected_repos: selectedRepos || [],
      repos: [],
    };
  }

  const registry = loaded.registry;
  const baseDir = registry.base_dir;
  const byId = new Map((registry.repos || []).map((r) => [String(r.repo_id), r]));
  const repos = [];

  for (const repoId of (selectedRepos || []).slice().sort((a, b) => String(a).localeCompare(String(b)))) {
    const cfg = byId.get(String(repoId));
    if (!cfg) {
      repos.push({ repo_id: String(repoId), team_id: null, abs_path: null, exists: false, status: "unknown", missing_config: true });
      continue;
    }
    const abs = resolveRepoAbsPath({ baseDir, repoPath: cfg.path });
    const exists = abs ? await pathExists(abs) : false;
    repos.push({
      repo_id: String(cfg.repo_id),
      team_id: String(cfg.team_id || "") || null,
      abs_path: abs,
      exists,
      status: String(cfg.status || "") || null,
      missing_config: false,
    });
  }

  return { ok: true, configured: true, base_dir: baseDir, selected_repos: selectedRepos || [], repos };
}
