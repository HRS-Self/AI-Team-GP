import test from "node:test";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { join } from "node:path";

import { createGovernanceFixture } from "../src/test-helpers/skills-governance-fixture.js";
import { runSkillsGovernance, writeSkillsGovernanceApproval } from "../src/lane_a/skills/skills-governance.js";

test("skills governance approve/reject writes artifacts and status reflects decisions", async () => {
  const fx = createGovernanceFixture({ repos: [] });
  const prev = {
    ENABLE_SKILLS_GOVERNANCE: process.env.ENABLE_SKILLS_GOVERNANCE,
    AI_TEAM_REPO: process.env.AI_TEAM_REPO,
  };
  process.env.ENABLE_SKILLS_GOVERNANCE = "1";
  process.env.AI_TEAM_REPO = fx.aiTeamRepoRoot;
  try {
    const appr = await writeSkillsGovernanceApproval({
      projectRoot: fx.opsRoot,
      draftId: "DRAFT-20260214_120000000",
      decision: "approved",
      by: "qa-lead",
      notes: "approved for authoring",
    });
    assert.equal(appr.ok, true);
    assert.equal(existsSync(appr.path), true);

    const rej = await writeSkillsGovernanceApproval({
      projectRoot: fx.opsRoot,
      draftId: "DRAFT-20260214_120000111",
      decision: "rejected",
      by: "qa-lead",
      notes: "insufficient evidence",
    });
    assert.equal(rej.ok, true);
    assert.equal(existsSync(rej.path), true);

    const status = await runSkillsGovernance({
      projectRoot: fx.opsRoot,
      run: false,
      status: true,
    });
    assert.equal(status.ok, true);
    assert.deepEqual(status.status.approvals.approved, ["DRAFT-20260214_120000000"]);
    assert.deepEqual(status.status.approvals.rejected, ["DRAFT-20260214_120000111"]);
    assert.equal(existsSync(join(fx.opsRoot, "ai", "lane_a", "skills", "governance", "status.json")), true);
  } finally {
    if (typeof prev.ENABLE_SKILLS_GOVERNANCE === "string") process.env.ENABLE_SKILLS_GOVERNANCE = prev.ENABLE_SKILLS_GOVERNANCE;
    else delete process.env.ENABLE_SKILLS_GOVERNANCE;
    if (typeof prev.AI_TEAM_REPO === "string") process.env.AI_TEAM_REPO = prev.AI_TEAM_REPO;
    else delete process.env.AI_TEAM_REPO;
  }
});
