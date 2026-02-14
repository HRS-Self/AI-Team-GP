import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runSkillsRefresh } from "../src/lane_a/skills/skills-refresh.js";
import { sha256Hex } from "../src/utils/fs-hash.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function makeSkillMd(title) {
  return [
    "# Overview",
    `${title} overview.`,
    "",
    "# When to use",
    "- Use for matching repository changes.",
    "",
    "# When NOT to use",
    "- Do not use for unrelated updates.",
    "",
    "# Constraints",
    "- Keep compatibility behavior stable.",
    "",
    "# Known failure modes",
    "- Missing validation coverage.",
    "",
  ].join("\n");
}

function makeMetadata({ skillId, hash, evidenceRefs }) {
  return {
    version: 1,
    skill_id: skillId,
    title: `Title ${skillId}`,
    domain: "domain-core",
    applies_to: ["repo:repo-a"],
    created_at: "2026-02-14T00:00:00.000Z",
    updated_at: "2026-02-14T00:00:00.000Z",
    hash,
    evidence_refs: evidenceRefs,
    source_scope: "repo:repo-a",
    dependencies: [],
    author: "skill.author",
  };
}

test("skills-refresh validates skills and creates refresh drafts for stale evidence", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-skills-refresh-"));
  const projectId = "proj-skills-refresh";
  const knowledge = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: [], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, knowledgeRepo: knowledge, activeTeams: [], sharedPacks: [] });

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

  const aiTeamRepoRoot = mkdtempSync(join(tmpdir(), "ai-team-global-skills-refresh-"));
  const skillsRoot = join(aiTeamRepoRoot, "skills");
  mkdirSync(skillsRoot, { recursive: true });

  const validSkillId = "repo-a-valid-skill";
  const validSkillMd = makeSkillMd("Valid Skill");
  const validSkillDir = join(skillsRoot, validSkillId);
  mkdirSync(validSkillDir, { recursive: true });
  writeFileSync(join(validSkillDir, "skill.md"), validSkillMd + "\n", "utf8");
  writeJson(join(validSkillDir, "skill.json"), makeMetadata({ skillId: validSkillId, hash: sha256Hex(validSkillMd + "\n"), evidenceRefs: ["EVID_repo_a_001"] }));

  const staleSkillId = "repo-a-stale-skill";
  const staleSkillMd = makeSkillMd("Stale Skill");
  const staleSkillDir = join(skillsRoot, staleSkillId);
  mkdirSync(staleSkillDir, { recursive: true });
  writeFileSync(join(staleSkillDir, "skill.md"), staleSkillMd + "\n", "utf8");
  writeJson(join(staleSkillDir, "skill.json"), makeMetadata({ skillId: staleSkillId, hash: sha256Hex(staleSkillMd + "\n"), evidenceRefs: ["EVID_missing_999"] }));

  writeJson(join(skillsRoot, "SKILLS.json"), {
    version: 1,
    updated_at: "2026-02-14T00:00:00.000Z",
    skills: {
      [validSkillId]: {
        skill_id: validSkillId,
        title: "Valid Skill",
        description: "Valid",
        tags: ["domain-core"],
        path: `skills/${validSkillId}/skill.md`,
        status: "active",
      },
      [staleSkillId]: {
        skill_id: staleSkillId,
        title: "Stale Skill",
        description: "Stale",
        tags: ["domain-core"],
        path: `skills/${staleSkillId}/skill.md`,
        status: "active",
      },
    },
  });

  const result = await runSkillsRefresh({
    projectRoot: join(root, "ops"),
    aiTeamRepoRoot,
  });
  assert.equal(result.ok, true);
  assert.equal(result.checked, 2);
  assert.equal(result.valid, 1);
  assert.equal(result.stale, 1);
  assert.equal(result.refresh_drafts_created, 1);

  const draftsDir = join(root, "ops", "ai", "lane_a", "skills", "drafts");
  const drafts = readdirSync(draftsDir).filter((name) => name.startsWith("DRAFT_REFRESH_") && name.endsWith(".json"));
  assert.equal(drafts.length, 1);
  const refreshDraft = JSON.parse(readFileSync(join(draftsDir, drafts[0]), "utf8"));
  assert.equal(refreshDraft.candidate_skill_id, staleSkillId);
  assert.equal(refreshDraft.status, "pending");
});

