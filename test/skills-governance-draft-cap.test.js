import test from "node:test";
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { join } from "node:path";

import { createGovernanceFixture, writeEvidenceRefs, writeRepoIndex } from "../src/test-helpers/skills-governance-fixture.js";
import { runSkillsGovernance } from "../src/lane_a/skills/skills-governance.js";

test("skills governance respects SKILLS_GOV_DRAFT_DAILY_CAP", async () => {
  const fx = createGovernanceFixture({ repos: ["repo-a", "repo-b"] });
  writeEvidenceRefs({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-a",
    refs: [
      { evidence_id: "EVID_repo_a_001", file_path: "src/auth/service.ts" },
      { evidence_id: "EVID_repo_a_002", file_path: "src/payments/client.ts" },
    ],
  });
  writeEvidenceRefs({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-b",
    refs: [
      { evidence_id: "EVID_repo_b_001", file_path: "src/auth/controller.ts" },
      { evidence_id: "EVID_repo_b_002", file_path: "src/payments/handler.ts" },
    ],
  });
  writeRepoIndex({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-a",
    index: { api_surface: { openapi_files: ["openapi-auth.yaml"], routes_controllers: ["src/payments/routes.ts"], events_topics: [] } },
  });
  writeRepoIndex({
    knowledgeRoot: fx.knowledgeRoot,
    repoId: "repo-b",
    index: { api_surface: { openapi_files: ["openapi-payments.yaml"], routes_controllers: ["src/auth/routes.ts"], events_topics: [] } },
  });

  const prev = {
    ENABLE_SKILLS_GOVERNANCE: process.env.ENABLE_SKILLS_GOVERNANCE,
    SKILLS_GOV_DRAFT_DAILY_CAP: process.env.SKILLS_GOV_DRAFT_DAILY_CAP,
    SKILLS_GOV_MIN_REUSE_REPOS: process.env.SKILLS_GOV_MIN_REUSE_REPOS,
    SKILLS_GOV_MIN_EVIDENCE_REFS: process.env.SKILLS_GOV_MIN_EVIDENCE_REFS,
    AI_TEAM_REPO: process.env.AI_TEAM_REPO,
  };
  process.env.ENABLE_SKILLS_GOVERNANCE = "1";
  process.env.SKILLS_GOV_DRAFT_DAILY_CAP = "1";
  process.env.SKILLS_GOV_MIN_REUSE_REPOS = "2";
  process.env.SKILLS_GOV_MIN_EVIDENCE_REFS = "1";
  process.env.AI_TEAM_REPO = fx.aiTeamRepoRoot;
  try {
    const result = await runSkillsGovernance({
      projectRoot: fx.opsRoot,
      run: true,
      status: true,
    });
    assert.equal(result.ok, true);
    assert.equal(result.status.drafts_created_this_run, 1);
    assert.equal(result.status.candidates_created_this_run, 1);

    const draftsDir = join(fx.opsRoot, "ai", "lane_a", "skills", "drafts");
    const draftFiles = readdirSync(draftsDir).filter((name) => /^DRAFT-\d{8}_\d{9}\.json$/.test(name));
    assert.equal(draftFiles.length, 1);
  } finally {
    for (const [k, v] of Object.entries(prev)) {
      if (typeof v === "string") process.env[k] = v;
      else delete process.env[k];
    }
  }
});
