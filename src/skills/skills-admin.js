import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";
import { validateProjectSkills } from "../contracts/validators/index.js";
import { loadGlobalSkillsRegistry, loadProjectSkills, loadSkillContent } from "./skills-loader.js";
import { nowFsSafeUtcTimestamp } from "../utils/naming.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function parseSkillIdCsv(csv) {
  return Array.from(
    new Set(
      String(csv || "")
        .split(",")
        .map((s) => normStr(s))
        .filter(Boolean),
    ),
  ).sort((a, b) => a.localeCompare(b));
}

function ensureSkillIdsExist(skillIds, registry) {
  const skillsMap = registry && typeof registry.skills === "object" ? registry.skills : {};
  const missing = skillIds.filter((id) => !skillsMap[id]);
  if (missing.length) throw new Error(`Unknown skill_id(s): ${missing.join(", ")}`);
}

function formatPreview(content, maxLines = 80) {
  const lines = String(content || "").split("\n");
  if (lines.length <= maxLines) return { text: lines.join("\n"), truncated: false, total_lines: lines.length };
  return {
    text: lines.slice(0, maxLines).join("\n"),
    truncated: true,
    total_lines: lines.length,
  };
}

export async function listGlobalSkills({ aiTeamRepoRoot, includeDeprecated = false } = {}) {
  const registry = await loadGlobalSkillsRegistry({ aiTeamRepoRoot });
  const rows = Object.values(registry.skills || {})
    .filter((s) => includeDeprecated || String(s?.status || "").trim() === "active")
    .sort((a, b) => String(a.skill_id || "").localeCompare(String(b.skill_id || "")));
  return { ok: true, skills: rows, updated_at: registry.updated_at };
}

export async function showSkill({ aiTeamRepoRoot, skillId, maxLines = 80 } = {}) {
  const id = normStr(skillId);
  if (!id) throw new Error("Missing --skill <skill_id>.");
  const repoRootAbs = aiTeamRepoRoot ? resolve(aiTeamRepoRoot) : null;
  const loaded = await loadSkillContent({ aiTeamRepoRoot: repoRootAbs, skill_id: id });
  const preview = formatPreview(loaded.content, maxLines);
  const bytes = Buffer.byteLength(loaded.content, "utf8");

  let updated_at = null;
  let title = null;
  try {
    const baseRoot = repoRootAbs || (typeof process.env.AI_TEAM_REPO === "string" && process.env.AI_TEAM_REPO.trim() ? process.env.AI_TEAM_REPO.trim() : process.cwd());
    const skillAbs = resolve(baseRoot, loaded.path);
    const metaAbs = resolve(dirname(skillAbs), "skill.json");
    const raw = await readFile(metaAbs, "utf8");
    const parsed = JSON.parse(String(raw || ""));
    updated_at = typeof parsed?.updated_at === "string" ? parsed.updated_at.trim() : null;
    title = typeof parsed?.title === "string" ? parsed.title.trim() : null;
  } catch {
    updated_at = null;
    title = null;
  }

  return {
    ok: true,
    skill_id: loaded.skill_id,
    path: loaded.path,
    sha256: loaded.sha256,
    bytes,
    updated_at,
    title,
    preview: preview.text,
    truncated: preview.truncated,
    total_lines: preview.total_lines,
  };
}

export async function readProjectSkillsStatus({ projectRoot } = {}) {
  const p = await loadProjectPaths({ projectRoot });
  const projectSkills = await loadProjectSkills({ projectRoot: p.opsRootAbs });
  return {
    ok: true,
    projectRoot: p.opsRootAbs,
    path: p.laneA.projectSkillsAbs,
    skills: projectSkills,
  };
}

export async function updateProjectSkillsAllowlist({
  mode,
  projectRoot,
  aiTeamRepoRoot,
  skillsCsv,
  by,
  notes = null,
  dryRun = false,
} = {}) {
  const op = normStr(mode);
  if (op !== "allow" && op !== "deny") throw new Error("mode must be 'allow' or 'deny'.");
  const actor = normStr(by);
  if (!actor) throw new Error("Missing --by <name>.");
  const changes = parseSkillIdCsv(skillsCsv);
  if (!changes.length) throw new Error("Missing --skills \"a,b,c\".");

  const [paths, registry, before] = await Promise.all([
    loadProjectPaths({ projectRoot }),
    loadGlobalSkillsRegistry({ aiTeamRepoRoot }),
    loadProjectSkills({ projectRoot }),
  ]);
  ensureSkillIdsExist(changes, registry);

  const allowed = new Set(Array.isArray(before.allowed_skills) ? before.allowed_skills.map((s) => normStr(s)).filter(Boolean) : []);
  if (op === "allow") {
    for (const id of changes) allowed.add(id);
  } else {
    for (const id of changes) allowed.delete(id);
  }
  const allowedSorted = Array.from(allowed).sort((a, b) => a.localeCompare(b));

  const pinnedIn = before && typeof before.pinned === "object" && before.pinned ? before.pinned : {};
  const pinnedOut = {};
  for (const id of allowedSorted) {
    if (pinnedIn[id] && typeof pinnedIn[id] === "object") pinnedOut[id] = pinnedIn[id];
  }

  const after = {
    version: 1,
    project_code: String(before.project_code || paths.cfg?.project_code || "unknown"),
    updated_at: nowISO(),
    allowed_skills: allowedSorted,
    pinned: pinnedOut,
  };
  validateProjectSkills(after);

  const audit = {
    version: 1,
    by: actor,
    notes: typeof notes === "string" && notes.trim() ? notes.trim() : null,
    before,
    after,
    created_at: nowISO(),
  };

  const ts = nowFsSafeUtcTimestamp();
  const auditPathAbs = resolve(paths.laneA.skillsDirAbs, `skills_change_${ts}.json`);
  const projectSkillsAbs = paths.laneA.projectSkillsAbs;

  if (!dryRun) {
    await mkdir(paths.laneA.skillsDirAbs, { recursive: true });
    await writeFile(projectSkillsAbs, `${JSON.stringify(after, null, 2)}\n`, "utf8");
    await writeFile(auditPathAbs, `${JSON.stringify(audit, null, 2)}\n`, "utf8");
  }

  return {
    ok: true,
    dry_run: dryRun,
    mode: op,
    projectRoot: paths.opsRootAbs,
    changed: changes,
    before,
    after,
    audit_path: auditPathAbs,
    project_skills_path: projectSkillsAbs,
  };
}
