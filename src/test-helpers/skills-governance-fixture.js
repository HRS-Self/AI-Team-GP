import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "./ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

export function createGovernanceFixture({
  projectId = "proj-skills-gov",
  repos = ["repo-a", "repo-b"],
} = {}) {
  const root = mkdtempSync(join(tmpdir(), "ai-team-skills-gov-"));
  const knowledge = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo: knowledge, activeTeams: [], sharedPacks: [] });

  const reposRootAbs = join(root, "repos");
  for (const repoId of repos) mkdirSync(join(reposRootAbs, repoId), { recursive: true });

  const reposJson = {
    version: 1,
    repos: repos.map((repoId) => ({
      repo_id: repoId,
      name: repoId,
      team_id: "TeamA",
      path: repoId,
      status: "active",
    })),
  };
  writeJson(join(root, "ops", "config", "REPOS.json"), reposJson);

  const aiTeamRepoRoot = mkdtempSync(join(tmpdir(), "ai-team-global-skills-gov-"));
  mkdirSync(join(aiTeamRepoRoot, "skills"), { recursive: true });
  writeJson(join(aiTeamRepoRoot, "skills", "SKILLS.json"), {
    version: 1,
    updated_at: "2026-02-14T00:00:00.000Z",
    skills: {},
  });

  return {
    root,
    opsRoot: join(root, "ops"),
    knowledgeRoot: join(root, "knowledge"),
    aiTeamRepoRoot,
  };
}

export function writeEvidenceRefs({ knowledgeRoot, repoId, refs }) {
  const dir = join(knowledgeRoot, "evidence", "repos", repoId);
  mkdirSync(dir, { recursive: true });
  const lines = refs.map((ref) =>
    JSON.stringify({
      evidence_id: ref.evidence_id,
      repo_id: repoId,
      file_path: ref.file_path,
      commit_sha: ref.commit_sha || "a".repeat(40),
      start_line: ref.start_line || 1,
      end_line: ref.end_line || 20,
      extractor: "test",
      captured_at: "2026-02-14T00:00:00.000Z",
    }),
  );
  writeFileSync(join(dir, "evidence_refs.jsonl"), `${lines.join("\n")}\n`, "utf8");
}

export function writeRepoIndex({ knowledgeRoot, repoId, index = {} }) {
  const dir = join(knowledgeRoot, "evidence", "index", "repos", repoId);
  mkdirSync(dir, { recursive: true });
  writeJson(join(dir, "repo_index.json"), {
    version: 1,
    repo_id: repoId,
    scanned_at: "2026-02-14T00:00:00.000Z",
    entrypoints: [],
    api_surface: {
      openapi_files: [],
      routes_controllers: [],
      events_topics: [],
    },
    migrations_schema: [],
    ...index,
  });
}

export function writeSkill({ aiTeamRepoRoot, skillId, skillMd, skillJson }) {
  const dir = join(aiTeamRepoRoot, "skills", skillId);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "skill.md"), skillMd, "utf8");
  writeJson(join(dir, "skill.json"), skillJson);
}

export function writeGlobalRegistrySkill({ aiTeamRepoRoot, skillId, title = "Skill", description = "Desc" }) {
  const pathAbs = join(aiTeamRepoRoot, "skills", "SKILLS.json");
  const reg = JSON.parse(String(readFileSync(pathAbs, "utf8")));
  reg.skills[skillId] = {
    skill_id: skillId,
    title,
    description,
    tags: ["governance"],
    path: `skills/${skillId}/skill.md`,
    status: "active",
  };
  writeJson(pathAbs, reg);
}

