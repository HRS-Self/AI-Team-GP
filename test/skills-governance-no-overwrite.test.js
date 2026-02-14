import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { createGovernanceFixture, writeGlobalRegistrySkill, writeSkill } from "../src/test-helpers/skills-governance-fixture.js";
import { runSkillsGovernance } from "../src/lane_a/skills/skills-governance.js";
import { sha256Hex } from "../src/utils/fs-hash.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("skills governance auto-author never overwrites existing skill_id", async () => {
  const fx = createGovernanceFixture({ repos: [] });
  const skillId = "auth-shared-pattern";
  const skillMd = [
    "# Overview",
    "Existing immutable skill.",
    "",
    "# When to use",
    "- Existing scope.",
    "",
    "# When NOT to use",
    "- Unrelated changes.",
    "",
    "# Constraints",
    "- Keep previous behavior.",
    "",
    "# Known failure modes",
    "- None.",
    "",
  ].join("\n");
  writeSkill({
    aiTeamRepoRoot: fx.aiTeamRepoRoot,
    skillId,
    skillMd: `${skillMd}\n`,
    skillJson: {
      version: 1,
      skill_id: skillId,
      title: "Auth Shared Pattern",
      domain: "auth",
      applies_to: ["system"],
      created_at: "2026-02-14T00:00:00.000Z",
      updated_at: "2026-02-14T00:00:00.000Z",
      hash: sha256Hex(`${skillMd}\n`),
      evidence_refs: ["EVID_auth_001"],
      source_scope: "system",
      dependencies: [],
      author: "skill.author",
    },
  });
  writeGlobalRegistrySkill({ aiTeamRepoRoot: fx.aiTeamRepoRoot, skillId, title: "Auth Shared Pattern", description: "Existing" });

  const draftsDir = join(fx.opsRoot, "ai", "lane_a", "skills", "drafts");
  mkdirSync(draftsDir, { recursive: true });
  const draftId = "DRAFT-20260214_120000000";
  writeJson(join(draftsDir, `${draftId}.json`), {
    version: 1,
    draft_id: draftId,
    scope: "system",
    candidate_skill_id: skillId,
    reason: "existing skill collision",
    evidence_refs: ["EVID_auth_001"],
    status: "pending",
  });

  const before = readFileSync(join(fx.aiTeamRepoRoot, "skills", skillId, "skill.md"), "utf8");

  const prev = {
    ENABLE_SKILLS_GOVERNANCE: process.env.ENABLE_SKILLS_GOVERNANCE,
    SKILLS_GOV_AUTO_AUTHOR: process.env.SKILLS_GOV_AUTO_AUTHOR,
    SKILLS_GOV_REQUIRE_APPROVAL: process.env.SKILLS_GOV_REQUIRE_APPROVAL,
    AI_TEAM_REPO: process.env.AI_TEAM_REPO,
  };
  process.env.ENABLE_SKILLS_GOVERNANCE = "1";
  process.env.SKILLS_GOV_AUTO_AUTHOR = "1";
  process.env.SKILLS_GOV_REQUIRE_APPROVAL = "0";
  process.env.AI_TEAM_REPO = fx.aiTeamRepoRoot;
  try {
    const result = await runSkillsGovernance({
      projectRoot: fx.opsRoot,
      run: true,
      status: true,
    });
    assert.equal(result.ok, true);
    const after = readFileSync(join(fx.aiTeamRepoRoot, "skills", skillId, "skill.md"), "utf8");
    assert.equal(after, before);
  } finally {
    if (typeof prev.ENABLE_SKILLS_GOVERNANCE === "string") process.env.ENABLE_SKILLS_GOVERNANCE = prev.ENABLE_SKILLS_GOVERNANCE;
    else delete process.env.ENABLE_SKILLS_GOVERNANCE;
    if (typeof prev.SKILLS_GOV_AUTO_AUTHOR === "string") process.env.SKILLS_GOV_AUTO_AUTHOR = prev.SKILLS_GOV_AUTO_AUTHOR;
    else delete process.env.SKILLS_GOV_AUTO_AUTHOR;
    if (typeof prev.SKILLS_GOV_REQUIRE_APPROVAL === "string") process.env.SKILLS_GOV_REQUIRE_APPROVAL = prev.SKILLS_GOV_REQUIRE_APPROVAL;
    else delete process.env.SKILLS_GOV_REQUIRE_APPROVAL;
    if (typeof prev.AI_TEAM_REPO === "string") process.env.AI_TEAM_REPO = prev.AI_TEAM_REPO;
    else delete process.env.AI_TEAM_REPO;
  }
});
