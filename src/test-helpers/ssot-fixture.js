import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

function sha256Hex(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function run(cmd, args, { cwd }) {
  const res = spawnSync(cmd, args, { cwd, encoding: "utf8" });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

export function initKnowledgeRepoWithMinimalSsot({ projectRoot, projectId, activeTeams = [], sharedPacks = [] } = {}) {
  const projectHomeAbs = String(projectRoot || "").trim();
  if (!projectHomeAbs) throw new Error("initKnowledgeRepoWithMinimalSsot: projectRoot is required.");

  const projectCode = String(projectId || "").trim() || "project";
  const opsRootAbs = join(projectHomeAbs, "ops");
  const reposRootAbs = join(projectHomeAbs, "repos");
  const knowledgeRootAbs = join(projectHomeAbs, "knowledge");

  mkdirSync(join(opsRootAbs, "config"), { recursive: true });
  mkdirSync(reposRootAbs, { recursive: true });
  mkdirSync(knowledgeRootAbs, { recursive: true });

  // Must be a git worktree for SSOT resolver validation.
  const gitInit = run("git", ["init", "-q"], { cwd: knowledgeRootAbs });
  if (!gitInit.ok) throw new Error(`Failed to git init knowledge repo: ${gitInit.stderr.trim() || gitInit.stdout.trim()}`);

  const ssotSystemDir = join(knowledgeRootAbs, "ssot", "system");
  const ssotSectionsDir = join(ssotSystemDir, "sections");
  const viewsTeamsDir = join(knowledgeRootAbs, "views", "teams");
  const viewsReposDir = join(knowledgeRootAbs, "views", "repos");
  const ssotReposDir = join(knowledgeRootAbs, "ssot", "repos");
  const evidenceIndexReposDir = join(knowledgeRootAbs, "evidence", "index", "repos");
  const evidenceReposDir = join(knowledgeRootAbs, "evidence", "repos");
  const evidenceSystemDir = join(knowledgeRootAbs, "evidence", "system");
  const sessionsDir = join(knowledgeRootAbs, "sessions");
  const decisionsDir = join(knowledgeRootAbs, "decisions");
  const docsDir = join(knowledgeRootAbs, "docs");
  const eventsDir = join(knowledgeRootAbs, "events");

  mkdirSync(ssotSectionsDir, { recursive: true });
  mkdirSync(ssotReposDir, { recursive: true });
  mkdirSync(viewsTeamsDir, { recursive: true });
  mkdirSync(viewsReposDir, { recursive: true });
  mkdirSync(evidenceIndexReposDir, { recursive: true });
  mkdirSync(evidenceReposDir, { recursive: true });
  mkdirSync(evidenceSystemDir, { recursive: true });
  mkdirSync(sessionsDir, { recursive: true });
  mkdirSync(decisionsDir, { recursive: true });
  mkdirSync(docsDir, { recursive: true });
  mkdirSync(eventsDir, { recursive: true });

  const sectionDefs = [
    { id: "vision", filename: "vision.json" },
    { id: "scope", filename: "scope.json" },
    { id: "constraints", filename: "constraints.json" },
    { id: "architecture", filename: "architecture.json" },
    { id: "nfr", filename: "nfr.json" },
    { id: "risks", filename: "risks.json" },
  ];

  const sections = [];
  for (const def of sectionDefs) {
    const relPath = `ssot/system/sections/${def.filename}`;
    const abs = join(knowledgeRootAbs, relPath);
    const json = { version: 1, id: def.id, content: "" };
    const text = JSON.stringify(json, null, 2) + "\n";
    writeFileSync(abs, text, "utf8");
    const sha = sha256Hex(Buffer.from(text, "utf8"));
    sections.push({ id: def.id, path: relPath, sha256: sha });
  }

  const snapshot = {
    version: 1,
    project_code: projectCode,
    created_at: new Date().toISOString(),
    shared_knowledge_packs: sharedPacks,
    sections,
  };
  writeFileSync(join(knowledgeRootAbs, "ssot", "system", "PROJECT_SNAPSHOT.json"), JSON.stringify(snapshot, null, 2) + "\n", "utf8");

  // Required views: global + team:<TeamID> + pack:<Pack>
  const globalView = { version: 1, view_id: "global", section_ids: sectionDefs.map((s) => s.id) };
  writeFileSync(join(viewsTeamsDir, "global.json"), JSON.stringify(globalView, null, 2) + "\n", "utf8");

  for (const t of activeTeams) {
    const teamId = String(t || "").trim();
    if (!teamId) continue;
    const view = { version: 1, view_id: `team:${teamId}`, section_ids: [] };
    writeFileSync(join(viewsTeamsDir, `team-${teamId}.json`), JSON.stringify(view, null, 2) + "\n", "utf8");
  }

  for (const p of sharedPacks) {
    const pack = String(p || "").trim();
    if (!pack) continue;
    const view = { version: 1, view_id: `pack:${pack}`, section_ids: [] };
    writeFileSync(join(viewsTeamsDir, `pack-${pack}.json`), JSON.stringify(view, null, 2) + "\n", "utf8");
  }

  return { projectHomeAbs, opsRootAbs, reposRootAbs, knowledgeRootAbs, projectCode };
}

export function writeProjectConfig({ projectRoot, projectId, knowledgeRepo, activeTeams = [], sharedPacks = [] } = {}) {
  const projectHomeAbs = String(projectRoot || "").trim();
  if (!projectHomeAbs) throw new Error("writeProjectConfig: projectRoot is required.");
  const projectCode = String(projectId || "").trim() || knowledgeRepo?.projectCode || "project";

  const opsRootAbs = knowledgeRepo?.opsRootAbs || join(projectHomeAbs, "ops");
  const reposRootAbs = knowledgeRepo?.reposRootAbs || join(projectHomeAbs, "repos");
  const knowledgeRootAbs = knowledgeRepo?.knowledgeRootAbs || join(projectHomeAbs, "knowledge");

  mkdirSync(join(opsRootAbs, "config"), { recursive: true });

  const cfg = {
    version: 4,
    project_code: projectCode,
    repos_root_abs: reposRootAbs,
    ops_root_abs: opsRootAbs,
    knowledge_repo_dir: knowledgeRootAbs,
    ssot_bundle_policy: { global_packs: sharedPacks },
  };
  writeFileSync(join(opsRootAbs, "config", "PROJECT.json"), JSON.stringify(cfg, null, 2) + "\n", "utf8");

  // Convenience defaults for many tests (do not overwrite if caller already wrote them).
  const teamsPath = join(opsRootAbs, "config", "TEAMS.json");
  const reposPath = join(opsRootAbs, "config", "REPOS.json");
  if (!existsSync(teamsPath) && activeTeams.length) {
    writeFileSync(
      teamsPath,
      JSON.stringify(
        {
          version: 1,
          teams: activeTeams.map((team_id) => ({ team_id: String(team_id), name: String(team_id) })),
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );
  }
  if (!existsSync(reposPath)) {
    writeFileSync(reposPath, JSON.stringify({ version: 1, repos: [] }, null, 2) + "\n", "utf8");
  }
}
