import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ContractValidationError, validateProjectSkills } from "../src/contracts/validators/index.js";
import { loadProjectSkills } from "../src/skills/skills-loader.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function validProjectSkills() {
  return {
    version: 1,
    project_code: "proj-a",
    updated_at: "2026-02-14T00:00:00.000Z",
    allowed_skills: ["repo.audit"],
    pinned: {
      "repo.audit": {
        content_sha256: "a".repeat(64),
      },
    },
  };
}

test("project skills validator rejects pinned skill not in allowed_skills", () => {
  const bad = validProjectSkills();
  bad.allowed_skills = [];
  assert.throws(
    () => validateProjectSkills(bad),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.pinned['repo.audit']"),
  );
});

test("loadProjectSkills returns default empty when PROJECT_SKILLS.json is missing", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-project-skills-missing-"));
  const projectId = "proj-skills";
  const knowledge = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo: knowledge, activeTeams: [], sharedPacks: [] });

  const loaded = await loadProjectSkills({ projectRoot: join(root, "ops") });
  assert.equal(Array.isArray(loaded.allowed_skills), true);
  assert.equal(loaded.allowed_skills.length, 0);
});
