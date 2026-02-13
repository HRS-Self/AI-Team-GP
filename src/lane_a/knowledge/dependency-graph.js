import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { sha256Hex } from "../../utils/fs-hash.js";
import { jsonStableStringify } from "../../utils/json.js";
import { validateDependencyGraph, validateDependencyGraphOverride } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
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

async function readJsonOptional(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return { ok: true, exists: false, json: null };
  const raw = await readFile(abs, "utf8");
  return { ok: true, exists: true, json: JSON.parse(String(raw || "")) };
}

export function dependencyGraphPaths(paths) {
  return {
    graphAbs: join(paths.knowledge.ssotSystemAbs, "dependency_graph.json"),
    overrideAbs: join(paths.knowledge.ssotSystemAbs, "dependency_graph.override.json"),
    decisionsDirAbs: join(paths.knowledge.ssotSystemAbs, "decisions"),
  };
}

export function computeDependencyGraphHash(effectiveGraph) {
  const normalized = jsonStableStringify(effectiveGraph, 2);
  return sha256Hex(normalized);
}

export function applyDependencyOverrides({ baseGraph, override }) {
  const b = baseGraph;
  const o = override;
  const overrides = isPlainObject(o?.overrides) ? o.overrides : { remove_edges: [], add_edges: [], external_projects: [] };
  const remove = new Set(
    (Array.isArray(overrides.remove_edges) ? overrides.remove_edges : [])
      .map((e) => `${normStr(e?.from_repo_id)}::${normStr(e?.to_repo_id)}`)
      .filter((k) => k !== "::"),
  );
  const addEdges = Array.isArray(overrides.add_edges) ? overrides.add_edges : [];

  const edges = [];
  for (const e of Array.isArray(b?.edges) ? b.edges : []) {
    const k = `${normStr(e?.from_repo_id)}::${normStr(e?.to_repo_id)}`;
    if (remove.has(k)) continue;
    edges.push(e);
  }
  for (const e of addEdges) edges.push(e);

  // Prefer explicit external project list in override when present (non-empty), otherwise keep detected.
  const overrideExternal = Array.isArray(overrides.external_projects) ? overrides.external_projects : [];
  const external_projects = overrideExternal.length ? overrideExternal : Array.isArray(b?.external_projects) ? b.external_projects : [];

  const eff = {
    version: Number.isFinite(Number(b?.version)) ? Number(b.version) : 1,
    generated_at: normStr(b?.generated_at) || nowISO(),
    project: b?.project,
    nodes: Array.isArray(b?.nodes) ? b.nodes : [],
    edges: edges,
    external_projects,
  };

  // Deterministic sorting.
  eff.nodes = eff.nodes.slice().sort((a, b2) => normStr(a?.repo_id).localeCompare(normStr(b2?.repo_id)));
  eff.edges = eff.edges
    .slice()
    .sort((a, b2) => `${normStr(a?.from_repo_id)}::${normStr(a?.to_repo_id)}`.localeCompare(`${normStr(b2?.from_repo_id)}::${normStr(b2?.to_repo_id)}`));
  eff.external_projects = eff.external_projects.slice().sort((a, b2) => normStr(a?.project_code).localeCompare(normStr(b2?.project_code)));
  for (const ep of eff.external_projects) {
    if (Array.isArray(ep?.repos)) ep.repos = ep.repos.slice().sort((a, b2) => String(a).localeCompare(String(b2)));
  }

  validateDependencyGraph(eff);
  return eff;
}

export async function loadEffectiveDependencyGraph({ paths }) {
  const p = dependencyGraphPaths(paths);
  const baseRes = await readJsonOptional(p.graphAbs);
  if (!baseRes.exists) return { ok: false, missing: true, reason: "dependency_graph_missing", graph_path: p.graphAbs };
  validateDependencyGraph(baseRes.json);

  const overrideRes = await readJsonOptional(p.overrideAbs);
  const override = overrideRes.exists ? overrideRes.json : null;
  if (override) validateDependencyGraphOverride(override);

  const effective = override ? applyDependencyOverrides({ baseGraph: baseRes.json, override }) : baseRes.json;
  const hash = computeDependencyGraphHash(effective);
  const status = normStr(override?.status) || (effective.edges.length === 0 && effective.external_projects.length === 0 ? "approved" : "pending");
  return {
    ok: true,
    graph: baseRes.json,
    override: override || null,
    effective,
    effective_hash: hash,
    override_status: status,
    override_path: p.overrideAbs,
    graph_path: p.graphAbs,
  };
}

export function makeDefaultDependencyOverride({ approved, approvedBy = null, notes = null } = {}) {
  const ts = nowISO();
  const status = approved ? "approved" : "pending";
  return {
    version: 1,
    updated_at: ts,
    status,
    approved_by: approved ? approvedBy : null,
    approved_at: approved ? ts : null,
    notes: notes ?? null,
    overrides: {
      remove_edges: [],
      add_edges: [],
      external_projects: [],
    },
  };
}

export async function ensureDependencyOverrideExists({ paths, approvedWhenEmpty, dryRun }) {
  const p = dependencyGraphPaths(paths);
  if (existsSync(p.overrideAbs)) return { ok: true, created: false, path: p.overrideAbs };
  const ov = makeDefaultDependencyOverride({ approved: !!approvedWhenEmpty, approvedBy: null, notes: approvedWhenEmpty ? "auto-approved: no dependencies detected." : null });
  validateDependencyGraphOverride(ov);
  if (!dryRun) await writeTextAtomic(p.overrideAbs, JSON.stringify(ov, null, 2) + "\n");
  return { ok: true, created: true, path: p.overrideAbs, status: ov.status };
}

