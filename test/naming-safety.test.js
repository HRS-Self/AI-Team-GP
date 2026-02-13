import test from "node:test";
import assert from "node:assert/strict";

import { formatFsSafeUtcTimestamp, makeId, workBranchName } from "../src/utils/naming.js";

test("formatFsSafeUtcTimestamp: output is strictly [0-9_] and fixed width", () => {
  const d = new Date("2026-02-05T18:26:41.123Z");
  const s = formatFsSafeUtcTimestamp(d);
  assert.equal(s, "20260205_182641123");
  assert.match(s, /^[0-9]{8}_[0-9]{9}$/);
});

test("makeId: generated ids contain no forbidden symbols", () => {
  const ts = "20260205_182641123";
  const wid = makeId("W", { timestamp: ts, seed: "x" });
  const iid = makeId("I", { timestamp: ts, seed: "x" });
  const tid = makeId("T", { timestamp: ts, seed: "x" });
  for (const id of [wid, iid, tid]) {
    assert.ok(!/[ :.+]/.test(id), `id must not contain forbidden characters: ${id}`);
    assert.match(id, /^[WIT]-[0-9]{8}_[0-9]{9}_[a-f0-9]{6}$/);
  }
});

test("workBranchName: matches strict ai/<workId>/<repoId> policy", () => {
  const branch = workBranchName({ workId: "W-20260205_182641123_ff4a48", repoId: "dp-frontend-portal" });
  assert.equal(branch, "ai/W-20260205_182641123_ff4a48/dp-frontend-portal");
  assert.ok(!/[ :.+]/.test(branch));
});

