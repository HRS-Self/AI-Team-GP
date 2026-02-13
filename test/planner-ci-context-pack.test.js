import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { buildPlannerCiContextSection } from "../src/lane_b/agents/agent-runner.js";

test("Planner context pack includes latest CI feedback when present", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-planner-ci-"));
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  try {
    const wid = "W-1";
    const workDirAbs = join(root, "ops", "ai", "lane_b", "work", wid);
    mkdirSync(join(workDirAbs, "CI"), { recursive: true });

    const base = "feedback_20260205_000003000";

    writeFileSync(
      join(workDirAbs, "PR.json"),
      JSON.stringify({ version: 1, workId: wid, owner: "o", repo: "r", pr_number: 1, url: "https://example/pr/1", head_branch: "h", base_branch: "b", created_at: "t" }, null, 2) + "\n",
      "utf8",
    );
    writeFileSync(
      join(workDirAbs, "CI", "CI_Status.json"),
      JSON.stringify(
        {
          version: 1,
          workId: wid,
          pr_number: 1,
          head_sha: "abc",
          captured_at: new Date().toISOString(),
          overall: "failed",
          checks: [{ name: "ci", status: "completed", conclusion: "failure", url: null }],
          latest_feedback: base,
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );
    writeFileSync(
      join(workDirAbs, "CI", `${base}.json`),
      JSON.stringify({ snapshot_id: "20260205_000003000", checks: [{ name: "ci", top_error_lines: ["boom"] }] }, null, 2) + "\n",
      "utf8",
    );
    writeFileSync(join(workDirAbs, "CI", `${base}.md`), "# CI feedback\n\nboom\n", "utf8");

    const section = await buildPlannerCiContextSection({ workDir: `ai/lane_b/work/${wid}` });
    assert.equal(typeof section, "string");
    assert.ok(section.includes("=== PR/CI CONTEXT (work-scoped) ==="));
    assert.ok(section.includes(`${base}.json`));
    assert.ok(section.includes("boom"));
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});
