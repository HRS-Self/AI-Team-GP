import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runSkillsAuthor } from "../src/lane_a/skills/skill-author.js";
import { sha256Hex } from "../src/utils/fs-hash.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("skills-author publishes skill package, updates registry, and marks draft published", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-skills-author-"));
  const projectId = "proj-skills-author";
  const knowledge = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo: knowledge, activeTeams: [], sharedPacks: [] });

  const llmProfilesAbs = join(root, "ops", "config", "LLM_PROFILES.json");
  writeJson(llmProfilesAbs, {
    version: 1,
    profiles: {
      "skill.author": {
        provider: "openai",
        model: "gpt-5.2-mini",
        options: { reasoning: "standard" },
      },
    },
  });

  const repoId = "repo-a";
  const evidenceRepoDir = join(root, "knowledge", "evidence", "repos", repoId);
  mkdirSync(evidenceRepoDir, { recursive: true });
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
      "",
    ].join("\n"),
    "utf8",
  );

  const draftDirAbs = join(root, "ops", "ai", "lane_a", "skills", "drafts");
  mkdirSync(draftDirAbs, { recursive: true });
  const draftId = "DRAFT-20260214_120000000";
  const draftAbs = join(draftDirAbs, `${draftId}.json`);
  writeJson(draftAbs, {
    version: 1,
    draft_id: draftId,
    scope: "repo:repo-a",
    candidate_skill_id: "repo-a-api-contract",
    reason: "clustered from API evidence",
    evidence_refs: ["EVID_repo_a_001"],
    status: "pending",
  });

  const aiTeamRepoRoot = mkdtempSync(join(tmpdir(), "ai-team-global-skills-"));
  mkdirSync(join(aiTeamRepoRoot, "skills"), { recursive: true });
  writeJson(join(aiTeamRepoRoot, "skills", "SKILLS.json"), {
    version: 1,
    updated_at: "2026-02-14T00:00:00.000Z",
    skills: {},
  });

  const skillMd = [
    "# Overview",
    "API contract guardrails for repo-a.",
    "",
    "# When to use",
    "- Use during API endpoint changes.",
    "",
    "# When NOT to use",
    "- Do not use for UI-only changes.",
    "",
    "# Constraints",
    "- Preserve backward compatibility.",
    "",
    "# Known failure modes",
    "- Missing API regression tests.",
    "",
  ].join("\n");

  const result = await runSkillsAuthor({
    projectRoot: join(root, "ops"),
    draftId,
    aiTeamRepoRoot,
    invokeSkillAuthorImpl: async () => ({
      skill_md: skillMd,
      metadata: {
        version: 1,
        skill_id: "repo-a-api-contract",
        title: "Repo A API Contract",
        domain: "api-contracts",
        applies_to: ["repo:repo-a", "project:proj-skills-author"],
        created_at: "2026-02-14T00:00:00.000Z",
        updated_at: "2026-02-14T00:00:00.000Z",
        evidence_refs: ["EVID_repo_a_001"],
        source_scope: "repo:repo-a",
        dependencies: [],
        author: "skill.author",
      },
    }),
  });
  assert.equal(result.ok, true);
  assert.equal(result.skill_id, "repo-a-api-contract");

  const skillJson = JSON.parse(readFileSync(join(aiTeamRepoRoot, "skills", "repo-a-api-contract", "skill.json"), "utf8"));
  assert.equal(skillJson.hash, sha256Hex(skillMd));
  assert.equal(skillJson.author, "skill.author");

  const registry = JSON.parse(readFileSync(join(aiTeamRepoRoot, "skills", "SKILLS.json"), "utf8"));
  assert.equal(registry.skills["repo-a-api-contract"].path, "skills/repo-a-api-contract/skill.md");
  assert.equal(registry.skills["repo-a-api-contract"].status, "active");

  const updatedDraft = JSON.parse(readFileSync(draftAbs, "utf8"));
  assert.equal(updatedDraft.status, "published");
  assert.equal(updatedDraft.published_skill_id, "repo-a-api-contract");
});
