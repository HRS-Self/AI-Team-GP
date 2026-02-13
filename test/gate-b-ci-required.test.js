import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { requestMergeApproval } from "../src/lane_b/gates/merge-approval.js";
import { readWorkStatusSnapshot, updateWorkStatus } from "../src/utils/status-writer.js";

test("Merge approval cannot be requested unless CI is green (no early merge approval)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-gateb-"));
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = join(root, "ops");

  try {
    const wid = "W-1";
    const workDirAbs = join(root, "ops", "ai", "lane_b", "work", wid);
    mkdirSync(join(workDirAbs, "CI"), { recursive: true });

    await updateWorkStatus({ workId: wid, stage: "CI_PENDING", blocked: false });
    writeFileSync(join(workDirAbs, "META.json"), JSON.stringify({ version: 1, work_id: wid }, null, 2) + "\n", "utf8");
    writeFileSync(join(workDirAbs, "BUNDLE.json"), JSON.stringify({ version: 1, work_id: wid, bundle_hash: "deadbeef" }, null, 2) + "\n", "utf8");
    writeFileSync(join(workDirAbs, "PR.json"), JSON.stringify({ version: 1, workId: wid, owner: "o", repo: "r", pr_number: 1, url: "u", head_branch: "h", base_branch: "b", created_at: "t" }, null, 2) + "\n", "utf8");
    writeFileSync(
      join(workDirAbs, "CI", "CI_Status.json"),
      JSON.stringify(
        {
          version: 1,
          workId: wid,
          pr_number: 1,
          head_sha: "abc",
          captured_at: new Date().toISOString(),
          overall: "success",
          checks: [{ name: "ci", status: "completed", conclusion: "success", url: null, required: null }],
          latest_feedback: null,
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );

    const res = await requestMergeApproval({ workId: wid, dryRun: true });
    assert.equal(res.ok, false);

    const status = await readWorkStatusSnapshot(wid);
    assert.equal(status.ok, true);
    assert.equal(status.snapshot.current_stage, "CI_PENDING");
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});
