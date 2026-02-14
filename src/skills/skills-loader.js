import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, isAbsolute, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";
import { validateProjectSkills, validateSkillsRegistry } from "../contracts/validators/index.js";
import { sha256Hex } from "../utils/fs-hash.js";

const MAX_SKILL_BYTES = 80 * 1024;
const SKILLS_REGISTRY_REL = "skills/SKILLS.json";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function normalizeLf(text) {
  return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function defaultAiTeamRepoRoot() {
  const envRoot = normStr(process.env.AI_TEAM_REPO);
  if (envRoot) return resolve(envRoot);
  return resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
}

function resolveAiTeamRepoRoot(aiTeamRepoRoot) {
  const raw = normStr(aiTeamRepoRoot) || defaultAiTeamRepoRoot();
  if (!isAbsolute(raw)) throw new Error(`aiTeamRepoRoot must be an absolute path (got: ${raw}).`);
  return resolve(raw);
}

async function readJsonAbs(pathAbs, nameForError) {
  let raw;
  try {
    raw = await readFile(pathAbs, "utf8");
  } catch (err) {
    if (err && err.code === "ENOENT") throw new Error(`${nameForError} missing at ${pathAbs}.`);
    throw err;
  }
  try {
    return JSON.parse(String(raw || ""));
  } catch {
    throw new Error(`${nameForError} is invalid JSON at ${pathAbs}.`);
  }
}

function validatePinnedSkillIfPresent({ projectSkills, skillId, contentSha, strict }) {
  const pinned = projectSkills && typeof projectSkills.pinned === "object" && projectSkills.pinned ? projectSkills.pinned[skillId] : null;
  if (!pinned || typeof pinned !== "object") return { pinned: false };
  const expected = normStr(pinned.content_sha256);
  if (!expected) return { pinned: true };
  if (expected === contentSha) return { pinned: true };
  if (!strict) return { pinned: true, mismatch: true };
  throw new Error(`Pinned content_sha256 mismatch for skill '${skillId}'. expected=${expected} actual=${contentSha}`);
}

export async function loadGlobalSkillsRegistry({ aiTeamRepoRoot } = {}) {
  const root = resolveAiTeamRepoRoot(aiTeamRepoRoot);
  const abs = resolve(root, SKILLS_REGISTRY_REL);
  const parsed = await readJsonAbs(abs, "Global skills registry");
  validateSkillsRegistry(parsed);
  return parsed;
}

export async function loadSkillContent({ aiTeamRepoRoot, skill_id } = {}) {
  const root = resolveAiTeamRepoRoot(aiTeamRepoRoot);
  const id = normStr(skill_id);
  if (!id) throw new Error("loadSkillContent: skill_id is required.");

  const reg = await loadGlobalSkillsRegistry({ aiTeamRepoRoot: root });
  const entry = reg && reg.skills && typeof reg.skills === "object" ? reg.skills[id] : null;
  if (!entry || typeof entry !== "object") throw new Error(`Unknown skill_id '${id}' in global skills registry.`);

  const relPath = normStr(entry.path);
  const absPath = resolve(root, relPath);
  let raw;
  try {
    raw = await readFile(absPath, "utf8");
  } catch (err) {
    if (err && err.code === "ENOENT") throw new Error(`Skill content missing for '${id}' at ${relPath}.`);
    throw err;
  }

  const normalized = normalizeLf(raw);
  const bytes = Buffer.byteLength(normalized, "utf8");
  if (bytes > MAX_SKILL_BYTES) throw new Error(`Skill '${id}' exceeds max size ${MAX_SKILL_BYTES} bytes.`);
  const sha256 = sha256Hex(normalized);
  return { skill_id: id, path: relPath, content: normalized, sha256 };
}

export async function loadProjectSkills({ projectRoot } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const abs = paths.laneA.projectSkillsAbs;
  let parsed = null;
  try {
    parsed = await readJsonAbs(abs, "PROJECT_SKILLS.json");
  } catch (err) {
    if (String(err?.message || "").startsWith("PROJECT_SKILLS.json missing")) {
      return {
        version: 1,
        project_code: String(paths.cfg?.project_code || "").trim() || "unknown",
        updated_at: nowISO(),
        allowed_skills: [],
        pinned: {},
      };
    }
    throw err;
  }

  validateProjectSkills(parsed);
  return parsed;
}

export async function resolveAllowedSkillContents({ projectRoot, aiTeamRepoRoot, lenient = false } = {}) {
  const [registry, projectSkills] = await Promise.all([
    loadGlobalSkillsRegistry({ aiTeamRepoRoot }),
    loadProjectSkills({ projectRoot }),
  ]);

  const strict = !lenient;
  const ids = Array.from(new Set(Array.isArray(projectSkills.allowed_skills) ? projectSkills.allowed_skills.map((s) => normStr(s)).filter(Boolean) : [])).sort((a, b) =>
    a.localeCompare(b),
  );
  if (!ids.length) return [];

  const out = [];
  for (const id of ids) {
    const entry = registry && registry.skills && typeof registry.skills === "object" ? registry.skills[id] : null;
    if (!entry) {
      if (strict) throw new Error(`Allowed skill '${id}' is missing from global registry.`);
      continue;
    }
    // eslint-disable-next-line no-await-in-loop
    const loaded = await loadSkillContent({ aiTeamRepoRoot, skill_id: id });
    const pinStatus = validatePinnedSkillIfPresent({ projectSkills, skillId: id, contentSha: loaded.sha256, strict });
    if (!strict && pinStatus.mismatch) continue;
    out.push({
      skill_id: id,
      sha256: loaded.sha256,
      content: loaded.content,
      pinned: pinStatus.pinned === true,
    });
  }

  return out;
}
