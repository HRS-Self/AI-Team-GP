import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { registerLaneAHealthRoutes } from "../src/web/lane-a-health.js";

function writeJson(absPath, obj) {
  mkdirSync(dirname(absPath), { recursive: true });
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function makeProjectFixture({ root, projectCode }) {
  const projectHomeAbs = join(root, projectCode);
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({
    projectRoot: projectHomeAbs,
    projectId: projectCode,
    activeTeams: ["Tooling"],
    sharedPacks: [],
  });
  writeProjectConfig({
    projectRoot: projectHomeAbs,
    projectId: projectCode,
    knowledgeRepo,
    activeTeams: ["Tooling"],
    sharedPacks: [],
  });

  const repoId = "repo-a";
  mkdirSync(join(knowledgeRepo.reposRootAbs, repoId), { recursive: true });
  writeFileSync(
    join(knowledgeRepo.opsRootAbs, "config", "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: repoId, path: repoId, team_id: "Tooling", status: "active" }] }, null, 2) + "\n",
    "utf8",
  );

  return {
    project_code: projectCode,
    root_dir: projectHomeAbs,
    ops_dir: knowledgeRepo.opsRootAbs,
    repos_dir: knowledgeRepo.reposRootAbs,
    knowledge_dir: knowledgeRepo.knowledgeRootAbs,
    repo_id: repoId,
  };
}

function toRegistryProject(fx) {
  const now = new Date().toISOString();
  return {
    project_code: fx.project_code,
    status: "active",
    root_dir: fx.root_dir,
    ops_dir: fx.ops_dir,
    repos_dir: fx.repos_dir,
    created_at: now,
    updated_at: now,
    ports: { webui_port: 8090, websvc_port: 8091 },
    pm2: {
      ecosystem_path: join(fx.ops_dir, "pm2", "ecosystem.config.cjs"),
      apps: [`${fx.project_code}-webui`, `${fx.project_code}-websvc`],
    },
    cron: { installed: false, entries: [] },
    knowledge: {
      type: "git",
      abs_path: fx.knowledge_dir,
      git_remote: "",
      default_branch: "main",
      active_branch: "main",
      last_commit_sha: null,
    },
    repos: [
      {
        repo_id: fx.repo_id,
        owner_repo: `${fx.project_code}/${fx.repo_id}`,
        abs_path: join(fx.repos_dir, fx.repo_id),
        default_branch: "main",
        active_branch: "main",
        last_seen_head_sha: null,
        active: true,
      },
    ],
  };
}

function writeRegistry({ engineRoot, projects }) {
  const dirAbs = join(engineRoot, "ai", "registry");
  mkdirSync(dirAbs, { recursive: true });
  const now = new Date().toISOString();
  writeFileSync(
    join(dirAbs, "REGISTRY.json"),
    JSON.stringify(
      {
        version: 2,
        host_id: "test-host",
        created_at: now,
        updated_at: now,
        ports: {
          webui_base: 8090,
          webui_next: 8090,
          websvc_base: 8091,
          websvc_next: 8091,
        },
        projects: projects.map((p) => toRegistryProject(p)),
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
}

async function startHealthApp(engineRoot) {
  const app = express();
  registerLaneAHealthRoutes(app, { engineRoot });
  const server = await new Promise((resolvePromise) => {
    const s = app.listen(0, "127.0.0.1", () => resolvePromise(s));
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : null;
  return { server, baseUrl: `http://127.0.0.1:${port}` };
}

test("lane-a health html renders even when artifacts are missing", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-web-lane-a-health-html-"));
  const project = makeProjectFixture({ root, projectCode: "alpha" });
  writeRegistry({ engineRoot: root, projects: [project] });

  const { server, baseUrl } = await startHealthApp(root);
  t.after(() => server.close());

  const res = await fetch(`${baseUrl}/lane-a/health?project=alpha`);
  const body = await res.text();
  assert.equal(res.status, 200);
  assert.ok(body.includes("Lane A Health"));
  assert.ok(body.includes("Project Summary"));
  assert.ok(body.includes("none"));
});

test("lane-a health json returns contract and uses registry project list", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-web-lane-a-health-json-"));
  const alpha = makeProjectFixture({ root, projectCode: "alpha" });
  const beta = makeProjectFixture({ root, projectCode: "beta" });
  writeRegistry({ engineRoot: root, projects: [beta, alpha] });

  const { server, baseUrl } = await startHealthApp(root);
  t.after(() => server.close());

  const res = await fetch(`${baseUrl}/lane-a/health?format=json`);
  const json = await res.json();
  assert.equal(res.status, 200);
  assert.equal(json.version, 1);
  assert.equal(typeof json.generated_at, "string");
  assert.equal(Array.isArray(json.projects), true);
  assert.equal(json.projects.length, 2);
  assert.deepEqual(
    json.projects.map((p) => p.project_code),
    ["alpha", "beta"],
  );
});

test("lane-a artifact endpoint allows allowed file and blocks traversal", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-web-lane-a-artifact-"));
  const project = makeProjectFixture({ root, projectCode: "alpha" });
  writeRegistry({ engineRoot: root, projects: [project] });

  const hintDir = join(project.ops_dir, "ai", "lane_a", "refresh_hints");
  mkdirSync(hintDir, { recursive: true });
  const hintFile = "RH-20260212_010101000__repo-repo-a.json";
  writeJson(join(hintDir, hintFile), { version: 1, scope: "repo:repo-a", reason: "stale:repo_stale", recommended_action: "knowledge-refresh" });

  const { server, baseUrl } = await startHealthApp(root);
  t.after(() => server.close());

  const okRes = await fetch(`${baseUrl}/lane-a/artifact?project=alpha&kind=refresh_hint&name=${encodeURIComponent(hintFile)}`);
  const okText = await okRes.text();
  assert.equal(okRes.status, 200);
  assert.ok(okRes.headers.get("content-type")?.includes("application/json"));
  assert.ok(okText.includes("recommended_action"));

  const badRes = await fetch(`${baseUrl}/lane-a/artifact?project=alpha&kind=refresh_hint&name=${encodeURIComponent("../config/PROJECT.json")}`);
  assert.ok(badRes.status >= 400);

  const outsideRes = await fetch(`${baseUrl}/lane-a/artifact?project=alpha&kind=committee&name=${encodeURIComponent("../../config/PROJECT.json")}`);
  assert.ok(outsideRes.status >= 400);
});
