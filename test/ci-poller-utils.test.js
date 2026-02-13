import test from "node:test";
import assert from "node:assert/strict";

import { computeCiSnapshotHash } from "../src/lane_b/ci/ci-poller.js";
import { formatFsSafeUtcTimestamp } from "../src/utils/naming.js";

test("formatFsSafeUtcTimestamp: produces symbol-safe UTC timestamp", () => {
  const d = new Date("2026-02-04T21:33:12.123Z");
  const s = formatFsSafeUtcTimestamp(d);
  assert.equal(s, "20260204_213312123");
  assert.match(s, /^[0-9]{8}_[0-9]{9}$/);
});

test("computeCiSnapshotHash: stable across key ordering and ignores captured_at/latest_feedback", () => {
  const a = {
    version: 1,
    workId: "W-1",
    pr_number: 1,
    head_sha: "abc",
    captured_at: "2026-02-05T00:00:00.000Z",
    overall: "failed",
    checks: [{ name: "c", status: "completed", conclusion: "failure", url: "x" }],
    latest_feedback: "feedback_20260205_000000000",
  };
  const b = {
    latest_feedback: "feedback_20260205_000000000",
    checks: [{ conclusion: "failure", name: "c", status: "completed", url: "x" }],
    overall: "failed",
    captured_at: "2026-02-05T00:00:01.000Z",
    head_sha: "abc",
    pr_number: 1,
    workId: "W-1",
    version: 1,
  };
  assert.equal(computeCiSnapshotHash(a), computeCiSnapshotHash(b));
});
