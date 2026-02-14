import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runSkillsDraft } from "../src/lane_a/skills/skills-draft.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("skills-draft creates deterministic repo-scope draft artifact from knowledge evidence", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-skills-draft-"));
  const projectId = "proj-skills-draft";
  const knowledge = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo: knowledge, activeTeams: [], sharedPacks: [] });

  const repoId = "repo-a";
  const evidenceRepoDir = join(root, "knowledge", "evidence", "repos", repoId);
  const indexRepoDir = join(root, "knowledge", "evidence", "index", "repos", repoId);
  mkdirSync(evidenceRepoDir, { recursive: true });
  mkdirSync(indexRepoDir, { recursive: true });
  writeFileSync(
    join(evidenceRepoDir, "evidence_refs.jsonl"),
    [
      JSON.stringify({
        evidence_id: "EVID_repo_a_001",
        repo_id: repoId,
        file_path: "src/api/routes.ts",
        commit_sha: "a".repeat(40),
        start_line: 1,
        end_line: 20,
        extractor: "test",
        captured_at: "2026-02-14T00:00:00.000Z",
      }),
      JSON.stringify({
        evidence_id: "EVID_repo_a_002",
        repo_id: repoId,
        file_path: "openapi.yaml",
        commit_sha: "b".repeat(40),
        start_line: 1,
        end_line: 10,
        extractor: "test",
        captured_at: "2026-02-14T00:00:00.000Z",
      }),
      "",
    ].join("\n"),
    "utf8",
  );
  writeJson(join(indexRepoDir, "repo_index.json"), {
    version: 1,
    repo_id: repoId,
    scanned_at: "2026-02-14T00:00:00.000Z",
    api_surface: {
      openapi_files: ["openapi.yaml"],
      routes_controllers: ["src/api/routes.ts"],
      events_topics: [],
    },
    migrations_schema: [],
  });

  const result = await runSkillsDraft({ projectRoot: join(root, "ops"), scope: `repo:${repoId}` });
  assert.equal(result.ok, true);
  assert.equal(result.scope, `repo:${repoId}`);
  assert.ok(result.draft_id.startsWith("DRAFT-"));
  assert.equal(result.candidate_skill_id, "repo-a-api-contract");

  const draft = JSON.parse(readFileSync(result.path, "utf8"));
  assert.equal(draft.status, "pending");
  assert.deepEqual(draft.evidence_refs, ["EVID_repo_a_001", "EVID_repo_a_002"]);
});

