import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { runQaPackUpdate } from "../src/lane_a/knowledge/qa-pack-update.js";

function sh(cmd, { cwd }) {
  const r = spawnSync("bash", ["-lc", cmd], { cwd, encoding: "utf8" });
  if (r.status !== 0) throw new Error(`Command failed (${r.status}): ${cmd}\n${String(r.stderr || r.stdout || "")}`);
  return String(r.stdout || "").trim();
}

function writeJson(absPath, obj) {
  writeFileSync(absPath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

test("--qa-pack-update merges strategist invariants into knowledge qa pack and commits (push skipped when no origin)", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-qa-pack-"));
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId: "t", activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId: "t", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = knowledgeRepo.opsRootAbs;
  const knowledgeRootAbs = knowledgeRepo.knowledgeRootAbs;

  // Ensure knowledge repo can commit in CI.
  sh('git config user.email "test@example.com"', { cwd: knowledgeRootAbs });
  sh('git config user.name "Test"', { cwd: knowledgeRootAbs });

  // Seed a QA strategist committee output (ops-only).
  const ts = "2026_02_12_000000";
  const outDir = join(opsRootAbs, "ai", "lane_a", "committee", ts);
  mkdirSync(outDir, { recursive: true });
  writeJson(join(outDir, "qa_strategist.system.json"), {
    version: 1,
    role: "qa_strategist",
    scope: "system",
    created_at: "2026-02-12T00:00:00.000Z",
    risk: { level: "normal", notes: "" },
    required_invariants: [
      { id: "INV_login_authz", text: "Login must enforce authz and reject invalid tokens.", severity: "high", evidence_refs: ["EVID_1"], evidence_missing: [] },
    ],
    test_obligations: {
      unit: { required: true, why: "lock core logic", suggested_test_directives: [], target_paths: [] },
      integration: { required: true, why: "public contract", suggested_test_directives: [], target_paths: [] },
      e2e: { required: false, why: "not required", suggested_test_directives: [], target_paths: [] },
    },
    facts: [],
    unknowns: [],
  });

  const res1 = await runQaPackUpdate({ projectRoot: opsRootAbs, scope: "system", dryRun: false });
  assert.equal(res1.ok, true);
  assert.equal(res1.committed, true);
  assert.equal(res1.pushed, false);
  assert.equal(res1.push_skipped, true);

  const invAbs = join(knowledgeRootAbs, "qa", "invariants.json");
  assert.ok(existsSync(invAbs));
  const inv = JSON.parse(readFileSync(invAbs, "utf8"));
  assert.equal(inv.version, 1);
  assert.ok(Array.isArray(inv.invariants));
  assert.ok(inv.invariants.some((i) => i.id === "INV_login_authz" && i.requires && i.requires.integration === true));

  const scenariosAbs = join(knowledgeRootAbs, "qa", "scenarios_e2e.md");
  assert.ok(existsSync(scenariosAbs));
  const scenarios = readFileSync(scenariosAbs, "utf8");
  assert.ok(scenarios.includes("<!-- invariant:INV_login_authz -->"));

  // Second run: idempotent (no new commit).
  const res2 = await runQaPackUpdate({ projectRoot: opsRootAbs, scope: "system", dryRun: false });
  assert.equal(res2.ok, true);
  assert.equal(res2.committed, false);
});

