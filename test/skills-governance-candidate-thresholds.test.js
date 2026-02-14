import test from "node:test";
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { join } from "node:path";

import { createGovernanceFixture, writeEvidenceRefs, writeRepoIndex } from "../src/test-helpers/skills-governance-fixture.js";
import { runSkillsGovernance } from "../src/lane_a/skills/skills-governance.js";

function applyEnv(overrides) {
  const prev = {};
  for (const [k, v] of Object.entries(overrides)) {
    prev[k] = process.env[k];
    if (typeof v === "string") process.env[k] = v;
    else delete process.env[k];
  }
  return prev;
}

function restoreEnv(prev) {
  for (const [k, v] of Object.entries(prev)) {
    if (typeof v === "string") process.env[k] = v;
    else delete process.env[k];
  }
}

test("skills governance does not create candidate when thresholds are not met", async () => {
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

  const prev = applyEnv({
    ENABLE_SKILLS_GOVERNANCE: "1",
    SKILLS_GOV_DRAFT_DAILY_CAP: "10",
    SKILLS_GOV_MIN_REUSE_REPOS: "2",
    SKILLS_GOV_MIN_EVIDENCE_REFS: "3",
    AI_TEAM_REPO: fx.aiTeamRepoRoot,
  });
  try {
    const result = await runSkillsGovernance({
      projectRoot: fx.opsRoot,
      run: true,
      status: true,
    });
    assert.equal(result.ok, true);
    assert.equal(result.status.candidates_created_this_run, 0);
    assert.equal(result.status.drafts_created_this_run, 0);
  } finally {
    restoreEnv(prev);
  }
});

test("skills governance creates candidate and draft when thresholds are met", async () => {
  const fx = createGovernanceFixture({ repos: ["repo-a", "repo-b"] });
  writeEvidenceRefs({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-a",
    refs: [
      { evidence_id: "EVID_repo_a_001", file_path: "src/auth/service.ts" },
      { evidence_id: "EVID_repo_a_002", file_path: "src/auth/routes.ts" },
    ],
  });
  writeEvidenceRefs({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-b",
    refs: [
      { evidence_id: "EVID_repo_b_001", file_path: "src/auth/controller.ts" },
      { evidence_id: "EVID_repo_b_002", file_path: "src/auth/handler.ts" },
    ],
  });
  writeRepoIndex({ knowledgeRoot: fx.knowledgeRoot, repoId: "repo-a" });
  writeRepoIndex({ knowledgeRoot: fx.knowledgeRoot, repoId: "repo-b" });

  const prev = applyEnv({
    ENABLE_SKILLS_GOVERNANCE: "1",
    SKILLS_GOV_DRAFT_DAILY_CAP: "10",
    SKILLS_GOV_MIN_REUSE_REPOS: "2",
    SKILLS_GOV_MIN_EVIDENCE_REFS: "3",
    AI_TEAM_REPO: fx.aiTeamRepoRoot,
  });
  try {
    const result = await runSkillsGovernance({
      projectRoot: fx.opsRoot,
      run: true,
      status: true,
    });
    assert.equal(result.ok, true);
    assert.equal(result.status.candidates_created_this_run > 0, true);
    assert.equal(result.status.drafts_created_this_run > 0, true);

    const candDir = join(fx.opsRoot, "ai", "lane_a", "skills", "governance", "candidates");
    const candidates = readdirSync(candDir).filter((name) => /^CAND-\d{8}_\d{9}_\d{3}\.json$/.test(name));
    assert.equal(candidates.length > 0, true);
  } finally {
    restoreEnv(prev);
  }
});
