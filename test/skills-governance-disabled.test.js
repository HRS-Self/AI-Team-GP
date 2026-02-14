import test from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { join } from "node:path";

import { createGovernanceFixture, writeEvidenceRefs, writeRepoIndex } from "../src/test-helpers/skills-governance-fixture.js";
import { runSkillsGovernance } from "../src/lane_a/skills/skills-governance.js";

test("ENABLE_SKILLS_GOVERNANCE=0 keeps governance fully inactive (no artifacts)", async () => {
  const fx = createGovernanceFixture({ repos: ["repo-a", "repo-b"] });
  writeEvidenceRefs({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-a",
    refs: [{ evidence_id: "EVID_repo_a_001", file_path: "src/auth/service.ts" }],
  });
  writeEvidenceRefs({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-b",
    refs: [{ evidence_id: "EVID_repo_b_001", file_path: "src/auth/controller.ts" }],
  });
  writeRepoIndex({ knowledgeRoot: fx.knowledgeRoot, repoId: "repo-a" });
  writeRepoIndex({ knowledgeRoot: fx.knowledgeRoot, repoId: "repo-b" });

  const prev = {
    ENABLE_SKILLS_GOVERNANCE: process.env.ENABLE_SKILLS_GOVERNANCE,
    AI_TEAM_REPO: process.env.AI_TEAM_REPO,
  };
  process.env.ENABLE_SKILLS_GOVERNANCE = "0";
  process.env.AI_TEAM_REPO = fx.aiTeamRepoRoot;
  try {
    const result = await runSkillsGovernance({
      projectRoot: fx.opsRoot,
      run: true,
      status: true,
    });
    assert.equal(result.ok, true);
    assert.equal(result.env.enabled, false);
    assert.equal(result.wrote, null);
    assert.equal(existsSync(join(fx.opsRoot, "ai", "lane_a", "skills", "governance")), false);
  } finally {
    if (typeof prev.ENABLE_SKILLS_GOVERNANCE === "string") process.env.ENABLE_SKILLS_GOVERNANCE = prev.ENABLE_SKILLS_GOVERNANCE;
    else delete process.env.ENABLE_SKILLS_GOVERNANCE;
    if (typeof prev.AI_TEAM_REPO === "string") process.env.AI_TEAM_REPO = prev.AI_TEAM_REPO;
    else delete process.env.AI_TEAM_REPO;
  }
});
