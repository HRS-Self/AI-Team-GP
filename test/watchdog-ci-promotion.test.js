import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runWatchdog } from "../src/lane_b/watchdog-runner.js";
import { readWorkStatusSnapshot, updateWorkStatus } from "../src/utils/status-writer.js";

function writeJson(pathAbs, obj) {
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function readJson(pathAbs) {
  return JSON.parse(readFileSync(pathAbs, "utf8"));
}

function writeCiStatus({ root, workId, overall }) {
  const ciDir = join(root, "ai", "lane_b", "work", workId, "CI");
  mkdirSync(ciDir, { recursive: true });
  writeJson(join(ciDir, "CI_Status.json"), {
    version: 1,
    workId,
    pr_number: 1,
    head_sha: "deadbeef",
    captured_at: new Date().toISOString(),
    overall,
    checks: [
      { name: "tests", status: "COMPLETED", conclusion: overall === "success" ? "SUCCESS" : "FAILURE", url: null, required: true },
    ],
    latest_feedback: null,
  });
}

function writeMeta({ root, workId, createdAt }) {
  const workDir = join(root, "ai", "lane_b", "work", workId);
  mkdirSync(workDir, { recursive: true });
  writeJson(join(workDir, "META.json"), {
    version: 1,
    work_id: workId,
    created_at: createdAt,
    priority: 50,
    depends_on: [],
    blocks: [],
    labels: [],
    repo_scopes: [],
    target_branch: null,
  });
}

test("watchdog-ci promotes APPLIED -> CI_GREEN when CI/status.json overall==success", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-watchdog-ci-green-applied-"));
  const opsRoot = join(root, "ops");
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRoot;

  try {
    const wid = "W-1";
    await updateWorkStatus({ workId: wid, stage: "APPLIED", blocked: false });
    let updateCalls = 0;
    const ciUpdater = async ({ workId }) => {
      updateCalls += 1;
      writeCiStatus({ root: opsRoot, workId, overall: "success" });
      return { ok: true };
    };

    const res = await runWatchdog({ orchestrator: {}, limit: 1, workId: null, watchdogCi: true, watchdogPrepr: false, maxMinutes: 1, ciUpdater });
    assert.equal(res.ok, true);
    assert.equal(updateCalls, 1);

    const status = await readWorkStatusSnapshot(wid);
    assert.equal(status.ok, true);
    assert.equal(status.snapshot.current_stage, "CI_GREEN");

    const statusJsonPath = join(opsRoot, "ai", "lane_b", "work", wid, "status.json");
    assert.equal(existsSync(statusJsonPath), true);
    const s = readJson(statusJsonPath);
    assert.equal(s.stage, "CI_GREEN");
    assert.ok(/^[0-9]{8}_[0-9]{9}$/.test(String(s.updated_at || "")));
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});

test("watchdog-ci promotes CI_PENDING -> CI_GREEN when CI/status.json overall==success", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-watchdog-ci-green-pending-"));
  const opsRoot = join(root, "ops");
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRoot;

  try {
    const wid = "W-1";
    await updateWorkStatus({ workId: wid, stage: "CI_PENDING", blocked: false });
    let updateCalls = 0;
    const ciUpdater = async ({ workId }) => {
      updateCalls += 1;
      writeCiStatus({ root: opsRoot, workId, overall: "success" });
      return { ok: true };
    };

    const res = await runWatchdog({ orchestrator: {}, limit: 1, watchdogCi: true, watchdogPrepr: false, maxMinutes: 1, ciUpdater });
    assert.equal(res.ok, true);
    assert.equal(updateCalls, 1);

    const status = await readWorkStatusSnapshot(wid);
    assert.equal(status.ok, true);
    assert.equal(status.snapshot.current_stage, "CI_GREEN");
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});

test("watchdog-ci does not promote when CI/status.json overall==failed", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-watchdog-ci-failed-"));
  const opsRoot = join(root, "ops");
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRoot;

  try {
    const wid = "W-1";
    await updateWorkStatus({ workId: wid, stage: "CI_PENDING", blocked: false });
    let updateCalls = 0;
    const ciUpdater = async ({ workId }) => {
      updateCalls += 1;
      writeCiStatus({ root: opsRoot, workId, overall: "failed" });
      return { ok: true };
    };

    const res = await runWatchdog({ orchestrator: {}, limit: 1, watchdogCi: true, watchdogPrepr: false, maxMinutes: 1, ciUpdater });
    assert.equal(res.ok, true);
    assert.equal(updateCalls, 1);

    const status = await readWorkStatusSnapshot(wid);
    assert.equal(status.ok, true);
    assert.equal(status.snapshot.current_stage, "CI_PENDING");
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});

test("watchdog-ci does not promote when already CI_GREEN", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-watchdog-ci-already-green-"));
  const opsRoot = join(root, "ops");
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRoot;

  try {
    const wid = "W-1";
    await updateWorkStatus({ workId: wid, stage: "CI_GREEN", blocked: false });
    let updateCalls = 0;
    const ciUpdater = async ({ workId }) => {
      updateCalls += 1;
      writeCiStatus({ root: opsRoot, workId, overall: "success" });
      return { ok: true };
    };

    const res = await runWatchdog({ orchestrator: {}, limit: 1, watchdogCi: true, watchdogPrepr: false, maxMinutes: 1, ciUpdater });
    assert.equal(res.ok, true);
    assert.equal(updateCalls, 0);

    const status = await readWorkStatusSnapshot(wid);
    assert.equal(status.ok, true);
    assert.equal(status.snapshot.current_stage, "CI_GREEN");
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});

test("watchdog-ci advances at most one stage per run (stops after first CI_GREEN promotion)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-watchdog-ci-one-transition-"));
  const opsRoot = join(root, "ops");
  const prevRoot = process.env.AI_PROJECT_ROOT;
  process.env.AI_PROJECT_ROOT = opsRoot;

  try {
    const wid1 = "W-1";
    const wid2 = "W-2";
    writeMeta({ root: opsRoot, workId: wid1, createdAt: "2020-01-01T00:00:00.000Z" });
    writeMeta({ root: opsRoot, workId: wid2, createdAt: "2020-01-02T00:00:00.000Z" });

    await updateWorkStatus({ workId: wid1, stage: "CI_PENDING", blocked: false });
    await updateWorkStatus({ workId: wid2, stage: "CI_PENDING", blocked: false });
    let updateCalls = 0;
    const ciUpdater = async ({ workId }) => {
      updateCalls += 1;
      writeCiStatus({ root: opsRoot, workId, overall: "success" });
      return { ok: true };
    };

    const res = await runWatchdog({ orchestrator: {}, limit: 2, watchdogCi: true, watchdogPrepr: false, maxMinutes: 1, ciUpdater });
    assert.equal(res.ok, true);
    assert.equal(updateCalls, 1);

    const s1 = await readWorkStatusSnapshot(wid1);
    const s2 = await readWorkStatusSnapshot(wid2);
    assert.equal(s1.ok, true);
    assert.equal(s2.ok, true);
    assert.equal(s1.snapshot.current_stage, "CI_GREEN");
    assert.equal(s2.snapshot.current_stage, "CI_PENDING");
  } finally {
    process.env.AI_PROJECT_ROOT = prevRoot;
  }
});
