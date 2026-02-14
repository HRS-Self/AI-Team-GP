import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { loadSkillContent, resolveAllowedSkillContents } from "../src/skills/skills-loader.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("skills loader hashing is deterministic and ordering is stable", async () => {
  const aiTeamRepoRoot = mkdtempSync(join(tmpdir(), "ai-team-skills-reg-"));
  mkdirSync(join(aiTeamRepoRoot, "skills", "a.skill"), { recursive: true });
  mkdirSync(join(aiTeamRepoRoot, "skills", "z.skill"), { recursive: true });
  writeFileSync(join(aiTeamRepoRoot, "skills", "a.skill", "skill.md"), "line1\r\nline2\r\n", "utf8");
  writeFileSync(join(aiTeamRepoRoot, "skills", "z.skill", "skill.md"), "Z\n", "utf8");
  writeJson(join(aiTeamRepoRoot, "skills", "SKILLS.json"), {
    version: 1,
    updated_at: "2026-02-14T00:00:00.000Z",
    skills: {
      "a.skill": {
        skill_id: "a.skill",
        title: "A",
        description: "A",
        tags: ["alpha"],
        path: "skills/a.skill/skill.md",
        status: "active",
      },
      "z.skill": {
        skill_id: "z.skill",
        title: "Z",
        description: "Z",
        tags: ["omega"],
        path: "skills/z.skill/skill.md",
        status: "active",
      },
    },
  });

  const root = mkdtempSync(join(tmpdir(), "ai-team-skills-proj-"));
  const projectId = "proj-skills-det";
  const knowledge = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo: knowledge, activeTeams: [], sharedPacks: [] });

  const projectSkillsDir = join(root, "ops", "ai", "lane_a", "skills");
  mkdirSync(projectSkillsDir, { recursive: true });
  writeJson(join(projectSkillsDir, "PROJECT_SKILLS.json"), {
    version: 1,
    project_code: projectId,
    updated_at: "2026-02-14T00:00:00.000Z",
    allowed_skills: ["z.skill", "a.skill"],
    pinned: {},
  });

  const firstLoad = await loadSkillContent({ aiTeamRepoRoot, skill_id: "a.skill" });
  const secondLoad = await loadSkillContent({ aiTeamRepoRoot, skill_id: "a.skill" });
  assert.equal(firstLoad.sha256, secondLoad.sha256);

  const firstResolved = await resolveAllowedSkillContents({
    projectRoot: join(root, "ops"),
    aiTeamRepoRoot,
  });
  const secondResolved = await resolveAllowedSkillContents({
    projectRoot: join(root, "ops"),
    aiTeamRepoRoot,
  });
  assert.deepEqual(
    firstResolved.map((s) => s.skill_id),
    ["a.skill", "z.skill"],
  );
  assert.deepEqual(firstResolved, secondResolved);
});
