import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { loadProjectPaths } from "../../paths/project-paths.js";
import { validateDependencyGraphOverride } from "../../contracts/validators/index.js";
import { loadEffectiveDependencyGraph } from "./dependency-graph.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

async function readJsonAbs(absPath) {
  const raw = await readFile(resolve(absPath), "utf8");
  return JSON.parse(String(raw || ""));
}

export async function runKnowledgeDepsApprove({ projectRoot, by, notes = null, dryRun = false } = {}) {
  const paths = await loadProjectPaths({ projectRoot });
  const byName = normStr(by);
  if (!byName) return { ok: false, message: "Missing --by \"<name>\"." };

  const loaded = await loadEffectiveDependencyGraph({ paths });
  if (!loaded.ok) return { ok: false, message: `dependency_graph.json missing. Run --knowledge-index first.`, reason: loaded.reason, graph_path: loaded.graph_path };

  if (!existsSync(loaded.override_path)) {
    return { ok: false, message: `dependency_graph.override.json missing. Run --knowledge-index first to create it.`, override_path: loaded.override_path };
  }

  const override = await readJsonAbs(loaded.override_path);
  const next = {
    ...override,
    updated_at: new Date().toISOString(),
    status: "approved",
    approved_by: byName,
    approved_at: new Date().toISOString(),
    notes: notes != null ? String(notes) : override.notes ?? null,
  };
  validateDependencyGraphOverride(next);

  const ts = nowFsSafeUtcTimestamp();
  const decisionsDirAbs = join(paths.knowledge.ssotSystemAbs, "decisions");
  const decisionAbs = join(decisionsDirAbs, `dependency_approval_${ts}.json`);
  const audit = {
    version: 1,
    type: "dependency_approval",
    approved_by: byName,
    approved_at: next.approved_at,
    notes: next.notes,
    effective_graph_hash: loaded.effective_hash,
    graph_path: loaded.graph_path,
    override_path: loaded.override_path,
  };

  if (!dryRun) {
    await writeTextAtomic(loaded.override_path, JSON.stringify(next, null, 2) + "\n");
    await mkdir(decisionsDirAbs, { recursive: true });
    await writeTextAtomic(decisionAbs, JSON.stringify(audit, null, 2) + "\n");
  }

  return { ok: true, projectRoot: paths.opsRootAbs, override_path: loaded.override_path, decision_path: decisionAbs, effective_graph_hash: loaded.effective_hash, dry_run: !!dryRun };
}

