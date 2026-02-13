import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { readdir } from "node:fs/promises";
import { resolve } from "node:path";

import { ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { resolveStatePath } from "../../project/state-paths.js";

function nowISO() {
  return new Date().toISOString();
}

export function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

export function validateScope(scope) {
  const raw = String(scope || "").trim();
  if (!raw) throw new Error("Missing --scope (expected: system or repo:<repo_id>).");
  if (raw === "system") return { scope: "system", kind: "system", repo_id: null };
  const m = raw.match(/^repo:([a-z0-9-_]+)$/);
  if (!m) throw new Error("Invalid --scope (expected: system or repo:<repo_id>, where repo_id matches ^[a-z0-9-_]+$).");
  return { scope: raw, kind: "repo", repo_id: m[1] };
}

export function knowledgeRootDir(rootAbs = null) {
  const root = String(rootAbs || "").trim();
  if (!root) throw new Error("Missing knowledge root (expected absolute K_ROOT).");
  return resolve(root);
}

export async function ensureKnowledgeStructure({ knowledgeRootAbs }) {
  const kr = knowledgeRootDir(knowledgeRootAbs);

  await ensureDir(kr);
  await ensureDir(`${kr}/qa`);
  await ensureDir(`${kr}/ssot/system`);
  await ensureDir(`${kr}/ssot/repos`);
  await ensureDir(`${kr}/evidence/index/repos`);
  await ensureDir(`${kr}/evidence/system`);
  await ensureDir(`${kr}/evidence/repos`);
  await ensureDir(`${kr}/views/teams`);
  await ensureDir(`${kr}/views/repos`);
  await ensureDir(`${kr}/docs`);
  await ensureDir(`${kr}/sessions`);
  await ensureDir(`${kr}/decisions`);
  await ensureDir(`${kr}/events`);

  // QA Pack (knowledge repo contract).
  const qaInvariantsAbs = `${kr}/qa/invariants.json`;
  const qaInvariants = await readTextIfExists(qaInvariantsAbs);
  if (!qaInvariants) {
    await writeText(
      qaInvariantsAbs,
      JSON.stringify(
        {
          version: 1,
          invariants: [],
        },
        null,
        2,
      ) + "\n",
    );
  }

  const qaScenariosAbs = `${kr}/qa/scenarios_e2e.md`;
  const qaScenarios = await readTextIfExists(qaScenariosAbs);
  if (!qaScenarios) {
    await writeText(
      qaScenariosAbs,
      [
        "# E2E Scenarios",
        "",
        "This file is part of the knowledge QA pack contract.",
        "Append-only by tooling; humans may enrich details under each scenario.",
        "",
      ].join("\n"),
    );
  }

  const qaMatrixAbs = `${kr}/qa/test_matrix.json`;
  const qaMatrix = await readTextIfExists(qaMatrixAbs);
  if (!qaMatrix) {
    await writeText(
      qaMatrixAbs,
      JSON.stringify(
        {
          version: 1,
          overall: { total_work_items: 0, must_add_unit: 0, must_add_integration: 0, must_add_e2e: 0 },
          by_repo_id: {},
          by_invariant_id: {},
          samples: { work_ids: [] },
        },
        null,
        2,
      ) + "\n",
    );
  }

  const qaRiskAbs = `${kr}/qa/risk_rules.json`;
  const qaRisk = await readTextIfExists(qaRiskAbs);
  if (!qaRisk) {
    await writeText(
      qaRiskAbs,
      JSON.stringify(
        {
          version: 1,
          rules: [],
        },
        null,
        2,
      ) + "\n",
    );
  }

  // Minimal system SSOT stubs (curated, git-worthy).
  const assumptionsPath = `${kr}/ssot/system/assumptions.json`;
  const assumptions = await readTextIfExists(assumptionsPath);
  if (!assumptions) {
    await writeText(
      assumptionsPath,
      JSON.stringify(
        {
          version: 1,
          scope: "system",
          updated_at: nowISO(),
          sources: [],
          invariants: [],
          boundaries: [],
          constraints: [],
          risks: [],
          open_questions: [],
          decisions_needed: [],
        },
        null,
        2,
      ) + "\n",
    );
  }

  const integrationPath = `${kr}/ssot/system/integration.json`;
  const integration = await readTextIfExists(integrationPath);
  if (!integration) {
    await writeText(
      integrationPath,
      JSON.stringify(
        {
          version: 1,
          scope: "system",
          updated_at: nowISO(),
          contracts: [],
          gaps: [],
          known_unknowns: [],
        },
        null,
        2,
      ) + "\n",
    );
  }

  return { ok: true, knowledge_root: kr };
}

export async function listSessionFiles({ knowledgeRootAbs }) {
  const kr = knowledgeRootDir(knowledgeRootAbs);
  const abs = resolveStatePath(`${kr}/sessions`);
  if (!existsSync(abs)) return [];
  const entries = await readdir(abs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.startsWith("SESSION-") && e.name.endsWith(".md"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));
}

export function loadPromptText(relPathFromRepoRoot) {
  const abs = resolve(relPathFromRepoRoot);
  if (!existsSync(abs)) throw new Error(`Missing prompt: ${relPathFromRepoRoot}`);
  return readFileSync(abs, "utf8");
}
