import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { laneALockPath, laneALockStatusDir, loadProjectPaths } from "../src/paths/project-paths.js";
import { acquireOpsLock, releaseOpsLock } from "../src/utils/ops-lock.js";

function writeJson(pathAbs, obj) {
  mkdirSync(dirname(resolve(pathAbs)), { recursive: true });
  writeFileSync(pathAbs, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

async function setupProjectHome() {
  const root = mkdtempSync(join(tmpdir(), "ai-team-lane-a-lock-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({
    projectRoot: root,
    projectId: "t",
    activeTeams: ["Tooling"],
    sharedPacks: [],
  });
  writeProjectConfig({
    projectRoot: root,
    projectId: "t",
    knowledgeRepo,
    activeTeams: ["Tooling"],
    sharedPacks: [],
  });
  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  writeJson(join(paths.opsConfigAbs, "REPOS.json"), {
    version: 1,
    repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }],
  });
  return { root, opsRootAbs, paths };
}

function owner(projectRoot) {
  return {
    pid: process.pid,
    uid: process.getuid?.() ?? null,
    user: process.env.USER || null,
    host: "test-host",
    cwd: process.cwd(),
    command: "node --test",
    project_root: projectRoot,
    ai_project_root: projectRoot,
  };
}

test("acquires lock when none exists", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });

  const acquired = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: owner(opsRootAbs) });
  assert.equal(acquired.ok, true);
  assert.equal(acquired.acquired, true);
  assert.equal(acquired.lock.lock_name, "lane-a-orchestrate");
  assert.ok(typeof acquired.lock.owner_token === "string" && acquired.lock.owner_token.length >= 32);

  const released = await releaseOpsLock({ lockPath, owner: { owner_token: acquired.lock.owner_token } });
  assert.equal(released.ok, true);
  assert.equal(released.released, true);
});

test("second acquire returns lock_held", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });

  const first = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: owner(opsRootAbs) });
  assert.equal(first.ok, true);
  assert.equal(first.acquired, true);

  const second = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: { ...owner(opsRootAbs), pid: process.pid + 1 } });
  assert.equal(second.ok, true);
  assert.equal(second.acquired, false);
  assert.equal(second.reason, "lock_held");

  await releaseOpsLock({ lockPath, owner: { owner_token: first.lock.owner_token } });
});

test("stale lock is broken and reacquired", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });

  writeJson(lockPath, {
    version: 1,
    lock_name: "lane-a-orchestrate",
    created_at: "2020-01-01T00:00:00.000Z",
    expires_at: "2020-01-01T00:00:01.000Z",
    pid: 1234,
    uid: "1000",
    user: "old",
    host: "old-host",
    cwd: "/tmp",
    command: "node old.js",
    project_root: opsRootAbs,
    ai_project_root: opsRootAbs,
    owner_token: "a".repeat(32),
  });

  const acquired = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: owner(opsRootAbs) });
  assert.equal(acquired.ok, true);
  assert.equal(acquired.acquired, true);
  assert.equal(acquired.broke_stale, true);

  const staleArtifacts = readdirSync(dirname(lockPath)).filter((f) => f.startsWith("lane-a-orchestrate.lock.json.stale-"));
  assert.ok(staleArtifacts.length >= 1);

  await releaseOpsLock({ lockPath, owner: { owner_token: acquired.lock.owner_token } });
});

test("release fails if owner token mismatches", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });

  const acquired = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: owner(opsRootAbs) });
  assert.equal(acquired.ok, true);
  assert.equal(acquired.acquired, true);

  const badRelease = await releaseOpsLock({ lockPath, owner: { owner_token: "f".repeat(32) } });
  assert.equal(badRelease.ok, true);
  assert.equal(badRelease.released, false);
  assert.equal(badRelease.reason, "not_owner");

  const goodRelease = await releaseOpsLock({ lockPath, owner: { owner_token: acquired.lock.owner_token } });
  assert.equal(goodRelease.ok, true);
  assert.equal(goodRelease.released, true);
});

test("lock paths are ops-only under ai/lane_a/locks", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });
  const statusDir = await laneALockStatusDir({ projectRoot: opsRootAbs });

  assert.ok(lockPath.startsWith(join(opsRootAbs, "ai", "lane_a", "locks")));
  assert.ok(lockPath.endsWith("lane-a-orchestrate.lock.json"));
  assert.ok(statusDir.startsWith(join(opsRootAbs, "ai", "lane_a", "locks", "status")));
});

test("crash safety: expired lock can be reacquired", async () => {
  const { opsRootAbs } = await setupProjectHome();
  const lockPath = await laneALockPath({ projectRoot: opsRootAbs });

  const first = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: owner(opsRootAbs) });
  assert.equal(first.ok, true);
  assert.equal(first.acquired, true);

  writeJson(lockPath, {
    ...first.lock,
    created_at: "2020-01-01T00:00:00.000Z",
    expires_at: "2020-01-01T00:00:01.000Z",
  });

  const second = await acquireOpsLock({ lockPath, ttlMs: 60_000, owner: { ...owner(opsRootAbs), pid: process.pid + 7 } });
  assert.equal(second.ok, true);
  assert.equal(second.acquired, true);

  await releaseOpsLock({ lockPath, owner: { owner_token: second.lock.owner_token } });
});
