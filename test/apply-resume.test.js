import test from "node:test";
import assert from "node:assert/strict";

import { classifyApplyResume } from "../src/lane_b/agents/apply-resume.js";

test("classifyApplyResume: skips when already succeeded and bundle hash matches", () => {
  const res = classifyApplyResume({
    statusEntry: { status: "succeeded", branch: "ai/W-20260205_182641123_ff4a48/dp-frontend-portal", commit: "abc", bundle_hash: "h1" },
    expectedBranch: "ai/W-20260205_182641123_ff4a48/dp-frontend-portal",
    currentBundleHash: "h1",
  });
  assert.equal(res.mode, "skip");
});

test("classifyApplyResume: does not skip when bundle hash mismatches", () => {
  const res = classifyApplyResume({
    statusEntry: { status: "succeeded", branch: "ai/W-20260205_182641123_ff4a48/dp-frontend-portal", commit: "abc", bundle_hash: "old" },
    expectedBranch: "ai/W-20260205_182641123_ff4a48/dp-frontend-portal",
    currentBundleHash: "new",
  });
  assert.equal(res.mode, "full");
  assert.equal(res.reason, "bundle_hash_mismatch");
});

test("classifyApplyResume: resumes PR step when PR label failed after commit", () => {
  const res = classifyApplyResume({
    statusEntry: { status: "failed_final", reason_code: "pr_label_failed", branch: "ai/W-20260205_182641123_ff4a48/dp-frontend-portal", commit: "abc", bundle_hash: "h1" },
    expectedBranch: "ai/W-20260205_182641123_ff4a48/dp-frontend-portal",
    currentBundleHash: "h1",
  });
  assert.equal(res.mode, "resume_pr");
});

test("classifyApplyResume: refuses resume on branch mismatch (no silent cross-branch)", () => {
  const res = classifyApplyResume({
    statusEntry: { status: "failed_final", reason_code: "pr_label_failed", branch: "ai/W-OLD/dp-frontend-portal", commit: "abc" },
    expectedBranch: "ai/W-NEW/dp-frontend-portal",
    currentBundleHash: "",
  });
  assert.equal(res.mode, "invalid");
  assert.equal(res.reason, "branch_mismatch");
});
