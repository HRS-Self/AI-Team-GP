import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { approveMergeApproval } from "../src/lane_b/gates/merge-approval.js";

function writeJson(absPath, obj) {
  mkdirSync(dirname(resolve(absPath)), { recursive: true });
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

async function setupMergeApprovalWork({
  wid,
  obligations,
  qaApproval = null,
  patchEdits = [{ path: "src/index.js" }],
}) {
  const root = mkdtempSync(join(tmpdir(), "ai-team-merge-qa-auto-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", projectMode: "greenfield", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });

  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRootAbs;

  const workDirAbs = join(paths.laneB.workAbs, wid);
  mkdirSync(join(workDirAbs, "CI"), { recursive: true });
  mkdirSync(join(workDirAbs, "QA"), { recursive: true });
  mkdirSync(join(workDirAbs, "patch-plans"), { recursive: true });

  const patchPlanRel = `ai/lane_b/work/${wid}/patch-plans/repo-a.json`;
  writeJson(join(workDirAbs, "META.json"), { version: 1, workId: wid });
  writeJson(join(workDirAbs, "PR.json"), {
    version: 1,
    workId: wid,
    owner: "o",
    repo: "r",
    pr_number: 14,
    url: "https://example.invalid/pr/14",
    base_branch: "main",
    head_branch: `ai/${wid}/repo-a/topic`,
  });
  writeJson(join(workDirAbs, "MERGE_APPROVAL.json"), {
    version: 1,
    workId: wid,
    status: "pending",
    mode: "manual",
    bundle_hash: "bundle_x",
    approved_at: null,
    approved_by: null,
    reason_codes: [],
    notes: null,
  });
  writeJson(join(workDirAbs, "CI", "CI_Status.json"), {
    version: 1,
    workId: wid,
    pr_number: 14,
    head_sha: "b".repeat(40),
    captured_at: "2026-02-11T00:00:00.000Z",
    overall: "success",
    checks: [],
    latest_feedback: null,
  });
  writeJson(join(workDirAbs, "patch-plans", "repo-a.json"), {
    version: 1,
    work_id: wid,
    repo_id: "repo-a",
    edits: patchEdits,
  });
  writeJson(join(workDirAbs, "BUNDLE.json"), {
    version: 1,
    work_id: wid,
    bundle_hash: "bundle_x",
    repos: [{ repo_id: "repo-a", patch_plan_json_path: patchPlanRel }],
  });
  writeJson(join(workDirAbs, "QA", "obligations.json"), {
    version: 1,
    workId: wid,
    created_at: "2026-02-11T00:00:00.000Z",
    changed_paths_by_repo: [{ repo_id: "repo-a", paths: ["src/index.js"] }],
    suggested_test_directives: [],
    api_surface_bindings_by_repo: [],
    ...obligations,
  });
  if (qaApproval) writeJson(join(workDirAbs, "QA_APPROVAL.json"), qaApproval);

  return {
    root,
    paths,
    restoreEnv: () => {
      if (typeof prevRoot === "string") process.env.AI_PROJECT_ROOT = prevRoot;
      else delete process.env.AI_PROJECT_ROOT;
    },
  };
}

function readLatestMergeEvent(paths) {
  const segFiles = readdirSync(paths.laneA.eventsSegmentsAbs)
    .filter((f) => f.endsWith(".jsonl"))
    .sort((a, b) => a.localeCompare(b));
  const events = [];
  for (const file of segFiles) {
    const text = readFileSync(join(paths.laneA.eventsSegmentsAbs, file), "utf8");
    const lines = String(text || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
    for (const line of lines) {
      const parsed = JSON.parse(line);
      if (parsed && parsed.type === "merge" && typeof parsed.id === "string") events.push(parsed);
    }
  }
  return events.sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)) || String(a.id).localeCompare(String(b.id))).at(-1);
}

test("merge approval high-risk dual-signoff blocks when QA signoff is missing", async () => {
  const ctx = await setupMergeApprovalWork({
    wid: "W-high-risk",
    obligations: {
      risk_level: "high",
      must_add_unit: false,
      must_add_integration: false,
      must_add_e2e: true,
    },
    patchEdits: [{ path: "e2e/auth.spec.ts" }],
  });
  try {
    const res = await approveMergeApproval({ workId: "W-high-risk", approvedBy: "owner" });
    assert.equal(res.ok, false);
    assert.ok(String(res.message || "").toLowerCase().includes("dual signoff"));
  } finally {
    ctx.restoreEnv();
  }
});

test("merge approval with explicit QA waiver creates INVARIANT_WAIVER decision and enriched merge event", async () => {
  const ctx = await setupMergeApprovalWork({
    wid: "W-waive",
    obligations: {
      risk_level: "low",
      must_add_unit: false,
      must_add_integration: false,
      must_add_e2e: true,
    },
    qaApproval: {
      version: 1,
      workId: "W-waive",
      status: "approved",
      by: "qa-lead",
      notes: "waive: e2e",
      updated_at: "2026-02-11T01:00:00.000Z",
    },
    patchEdits: [{ path: "src/service.js" }],
  });

  try {
    const res = await approveMergeApproval({ workId: "W-waive", approvedBy: "owner" });
    assert.equal(res.ok, true);

    const decisions = readdirSync(ctx.paths.knowledge.decisionsAbs).filter((n) => n.startsWith("DECISION-DEC_invariant_waiver_") && n.endsWith(".json"));
    assert.ok(decisions.length >= 1);
    const decision = JSON.parse(readFileSync(join(ctx.paths.knowledge.decisionsAbs, decisions.at(-1)), "utf8"));
    assert.equal(decision.type, "INVARIANT_WAIVER");
    assert.equal(decision.status, "open");

    const mergeEvent = readLatestMergeEvent(ctx.paths);
    assert.equal(mergeEvent.work_id, "W-waive");
    assert.equal(mergeEvent.merge_sha, "b".repeat(40));
    assert.equal(mergeEvent.risk_level, "low");
    assert.equal(mergeEvent.obligations.must_add_e2e, true);
    assert.equal(mergeEvent.qa_waiver.explicit, true);
    assert.ok(Array.isArray(mergeEvent.qa_waiver.waived_obligations));
    assert.ok(mergeEvent.qa_waiver.waived_obligations.includes("e2e"));
  } finally {
    ctx.restoreEnv();
  }
});
