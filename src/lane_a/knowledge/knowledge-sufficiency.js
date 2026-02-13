import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, rename, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { loadRepoRegistry } from "../../utils/repo-registry.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { evaluateScopeStaleness } from "../lane-a-staleness-policy.js";
import { validateDecisionPacket, validateSufficiency } from "../../contracts/validators/index.js";
import { readKnowledgeVersionOrDefault } from "./knowledge-version.js";
import { runKnowledgeStatus } from "./knowledge-status.js";
import { refreshPhasePrereqs } from "../phase-state.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

function scopeToLabel(scope) {
  const s = normStr(scope);
  if (s === "system") return "system";
  const m = /^repo:([A-Za-z0-9._-]+)$/.exec(s);
  if (!m) return "scope_invalid";
  return `repo_${m[1]}`;
}

function requireScope(scope) {
  const s = normStr(scope);
  if (!s) throw new Error("Missing --scope (system|repo:<id>).");
  if (s === "system") return "system";
  if (/^repo:[A-Za-z0-9._-]+$/.test(s)) return s;
  throw new Error("Invalid --scope (expected system|repo:<id>).");
}

function requireKnowledgeVersionString(v) {
  const s = normStr(v);
  if (!s) throw new Error("Missing --version vX.Y.Z");
  if (!/^v\d+(\.\d+)*$/.test(s)) throw new Error("Invalid --version (expected v<major>[.<minor>[.<patch>...]]).");
  return s;
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
  try {
    const t = String(readFileSync(abs, "utf8") || "");
    return { ok: true, exists: true, json: JSON.parse(t) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

export function sufficiencyPaths(paths) {
  const dirAbs = resolve(paths.laneA.sufficiencyAbs);
  const historyDirAbs = resolve(paths.laneA.sufficiencyHistoryAbs);
  const knowledgeDirAbs = resolve(paths.knowledge.decisionsAbs, "sufficiency");
  return {
    dirAbs,
    historyDirAbs,
    jsonAbs: join(dirAbs, "SUFFICIENCY.json"),
    knowledgeDirAbs,
    knowledgeLatestAbs: join(knowledgeDirAbs, "LATEST.json"),
  };
}

function defaultSufficiency({ scope, knowledgeVersion }) {
  const obj = {
    version: 1,
    scope,
    knowledge_version: knowledgeVersion,
    status: "insufficient",
    decided_by: null,
    decided_at: null,
    rationale_md_path: null,
    evidence_basis: [],
    blockers: [],
    stale_status: "fresh",
  };
  validateSufficiency(obj);
  return obj;
}

function staleStatusToken(staleInfo) {
  if (staleInfo && staleInfo.hard_stale === true) return "hard_stale";
  if (staleInfo && staleInfo.stale === true) return "soft_stale";
  return "fresh";
}

async function listOpenDecisionsForScope({ decisionsDirAbs, scope }) {
  const dirAbs = resolve(String(decisionsDirAbs || ""));
  if (!existsSync(dirAbs)) return [];
  const entries = await readdir(dirAbs, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && e.name.startsWith("DECISION-") && e.name.endsWith(".json"))
    .map((e) => join(dirAbs, e.name))
    .sort((a, b) => a.localeCompare(b));
  const out = [];
  for (const abs of files) {
    // eslint-disable-next-line no-await-in-loop
    const r = await readJsonOptional(abs);
    if (!r.ok || !r.exists || !isPlainObject(r.json)) continue;
    try {
      validateDecisionPacket(r.json);
    } catch {
      continue;
    }
    const pkt = r.json;
    if (normStr(pkt.status) !== "open") continue;
    if (normStr(pkt.scope) !== scope) continue;
    out.push({ decision_id: normStr(pkt.decision_id) || basename(abs), path: abs });
  }
  return out;
}

function deriveEvidenceBasis({ scope, paths }) {
  // Deterministic, bounded pointers for auditability.
  const basis = [];
  basis.push(`knowledge:VERSION.json`);
  if (scope === "system") {
    basis.push("knowledge:ssot/system/integration.json");
    basis.push("knowledge:ssot/system/gaps.json");
  } else {
    const repoId = scope.split(":")[1];
    basis.push(`knowledge:evidence/index/repos/${repoId}/repo_index.json`);
    basis.push(`knowledge:ssot/repos/${repoId}/scan.json`);
  }
  basis.push(`ops:${join(paths.laneA.rootAbs, "staleness.json")}`);
  return basis;
}

function computeBlockers({ scope, staleInfo, coverageComplete, openDecisions }) {
  const blockers = [];
  const reasons = Array.isArray(staleInfo?.reasons) ? staleInfo.reasons : [];
  if (staleInfo?.hard_stale === true) {
    blockers.push({
      id: "hard_stale",
      title: "Knowledge is hard-stale",
      details: `stale for scope ${scope}: ${reasons.join(", ") || "unknown"}`,
    });
  } else if (staleInfo?.stale === true) {
    blockers.push({
      id: "soft_stale",
      title: "Knowledge is stale",
      details: `stale for scope ${scope}: ${reasons.join(", ") || "unknown"}`,
    });
  }
  if (!coverageComplete) {
    blockers.push({
      id: "coverage_incomplete",
      title: "Scan coverage incomplete",
      details: `coverage is not complete for scope ${scope}`,
    });
  }
  if (openDecisions.length) {
    blockers.push({
      id: "open_decisions",
      title: "Open decision packets exist",
      details: `open decisions for scope ${scope}: ${openDecisions.map((d) => d.decision_id).slice(0, 25).join(", ")}`,
    });
  }
  return blockers;
}

async function coverageCompleteForScope({ statusJson, scope }) {
  if (!statusJson || typeof statusJson !== "object") return false;
  if (scope === "system") return statusJson.system?.scan_complete_all_repos === true;
  const repoId = scope.split(":")[1];
  const repos = Array.isArray(statusJson.repos) ? statusJson.repos : [];
  const r = repos.find((x) => normStr(x?.repo_id) === repoId) || null;
  return r?.scan?.complete === true;
}

async function readKnowledgeLatestIndex({ knowledgeLatestAbs }) {
  const r = await readJsonOptional(knowledgeLatestAbs);
  if (!r.ok) throw new Error(`Invalid sufficiency LATEST.json (${knowledgeLatestAbs}): ${r.message}`);
  if (!r.exists) return { ok: true, exists: false, latest: null };
  const j = r.json;
  if (!isPlainObject(j) || j.version !== 1 || !isPlainObject(j.latest_by_scope)) {
    throw new Error(`Invalid sufficiency LATEST.json shape: ${knowledgeLatestAbs}`);
  }
  return { ok: true, exists: true, latest: j };
}

function selectLatestEntry(latestIndex, scope) {
  const entry = latestIndex && isPlainObject(latestIndex.latest_by_scope) ? latestIndex.latest_by_scope[String(scope)] : null;
  if (!entry || !isPlainObject(entry)) return null;
  const rec = entry.record && typeof entry.record === "object" ? entry.record : null;
  if (!rec) return null;
  validateSufficiency(rec);
  return rec;
}

export async function readSufficiencyRecord({ projectRoot, scope, knowledgeVersion }) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const parsedScope = requireScope(scope);
  const kv = requireKnowledgeVersionString(knowledgeVersion);

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const sp = sufficiencyPaths(paths);
  const latestRes = await readKnowledgeLatestIndex({ knowledgeLatestAbs: sp.knowledgeLatestAbs });
  const latestRec = latestRes.exists ? selectLatestEntry(latestRes.latest, parsedScope) : null;
  const okMatch = latestRec && normStr(latestRec.knowledge_version) === kv;

  return {
    ok: true,
    exists: !!okMatch,
    sufficiency: okMatch ? latestRec : defaultSufficiency({ scope: parsedScope, knowledgeVersion: kv }),
    paths,
  };
}

async function writeKnowledgeLatest({ knowledgeLatestAbs, latestByScope, dryRun }) {
  const next = { version: 1, updated_at: nowISO(), latest_by_scope: latestByScope };
  if (!dryRun) {
    await writeTextAtomic(knowledgeLatestAbs, JSON.stringify(next, null, 2) + "\n");
  }
  return { ok: true, wrote: !dryRun, latest: next };
}

async function upsertKnowledgeLatest({ sp, record, recordFileName, dryRun }) {
  const res = await readKnowledgeLatestIndex({ knowledgeLatestAbs: sp.knowledgeLatestAbs });
  const prev = res.exists ? res.latest : { version: 1, updated_at: null, latest_by_scope: {} };
  const latest_by_scope = isPlainObject(prev.latest_by_scope) ? { ...prev.latest_by_scope } : {};
  latest_by_scope[String(record.scope)] = {
    scope: record.scope,
    knowledge_version: record.knowledge_version,
    status: record.status,
    decided_by: record.decided_by,
    decided_at: record.decided_at,
    record_json: recordFileName,
    record,
  };
  return await writeKnowledgeLatest({ knowledgeLatestAbs: sp.knowledgeLatestAbs, latestByScope: latest_by_scope, dryRun });
}

async function writeDecisionRecordPair({ sp, paths, record, scope, knowledgeVersion, notesText, dryRun }) {
  const ts = nowFsSafeUtcTimestamp();
  const scopeLabel = scopeToLabel(scope);
  const vLabel = String(knowledgeVersion);
  const baseName = `SUFF-${ts}__${scopeLabel}__${vLabel}`;
  const opsJsonAbs = join(sp.historyDirAbs, `${baseName}.json`);
  const opsMdAbs = join(sp.historyDirAbs, `${baseName}.md`);
  const knowledgeJsonAbs = join(sp.knowledgeDirAbs, `${baseName}.json`);

  if (!dryRun) {
    await mkdir(sp.historyDirAbs, { recursive: true });
    await mkdir(sp.dirAbs, { recursive: true });
    await mkdir(sp.knowledgeDirAbs, { recursive: true });
  }

  let rationale_md_path = null;
  if (notesText) {
    rationale_md_path = opsMdAbs;
    if (!dryRun) {
      await writeTextAtomic(
        opsMdAbs,
        [
          `# Sufficiency rationale`,
          ``,
          `scope: ${scope}`,
          `knowledge_version: ${knowledgeVersion}`,
          `written_at: ${nowISO()}`,
          ``,
          String(notesText).trim(),
          ``,
        ].join("\n") + "\n",
      );
    }
  }

  const rec = { ...record, rationale_md_path };
  validateSufficiency(rec);

  if (!dryRun) {
    await writeTextAtomic(opsJsonAbs, JSON.stringify(rec, null, 2) + "\n");
    await writeTextAtomic(sp.jsonAbs, JSON.stringify(rec, null, 2) + "\n");
    await writeTextAtomic(knowledgeJsonAbs, JSON.stringify(rec, null, 2) + "\n");
    await upsertKnowledgeLatest({ sp, record: rec, recordFileName: basename(knowledgeJsonAbs), dryRun: false });
  }

  return {
    ok: true,
    wrote: !dryRun,
    ops: { json: opsJsonAbs, md: notesText ? opsMdAbs : null },
    knowledge: { json: knowledgeJsonAbs, latest: sp.knowledgeLatestAbs },
    record: rec,
  };
}

export async function runKnowledgeSufficiencyStatus({ projectRoot, scope = "system", knowledgeVersion = null } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const parsedScope = requireScope(scope);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });

  const kv = knowledgeVersion ? requireKnowledgeVersionString(knowledgeVersion) : (await readKnowledgeVersionOrDefault({ projectRoot: paths.opsRootAbs })).version.current;
  const sp = sufficiencyPaths(paths);
  const recordRes = await readSufficiencyRecord({ projectRoot: paths.opsRootAbs, scope: parsedScope, knowledgeVersion: kv });

  const statusJson = await runKnowledgeStatus({ projectRoot: paths.opsRootAbs, dryRun: true });
  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  const registry = reposRes.ok ? reposRes.registry : { version: 1, repos: [], base_dir: paths.reposRootAbs };
  const staleInfo = await evaluateScopeStaleness({ paths, registry: { ...registry, base_dir: paths.reposRootAbs }, scope: parsedScope });
  const openDecisions = await listOpenDecisionsForScope({ decisionsDirAbs: paths.knowledge.decisionsAbs, scope: parsedScope });
  const coverageComplete = await coverageCompleteForScope({ statusJson, scope: parsedScope });

  const blockers = computeBlockers({ scope: parsedScope, staleInfo, coverageComplete, openDecisions });
  const nextAction =
    recordRes.sufficiency.status === "sufficient"
      ? { type: "delivery", reason: "sufficiency sufficient; delivery exporters allowed" }
      : recordRes.sufficiency.status === "proposed_sufficient"
        ? { type: "review", reason: "sufficiency proposed; approve or reject" }
        : { type: "committee_challenge", reason: "insufficient; run committee in challenge mode" };

  return {
    ok: true,
    projectRoot: paths.opsRootAbs,
    scope: parsedScope,
    knowledge_version: kv,
    sufficiency: {
      ...recordRes.sufficiency,
      blockers,
      stale_status: staleStatusToken(staleInfo),
    },
    next_action: nextAction,
    ops_paths: { current: sp.jsonAbs, history_dir: sp.historyDirAbs },
    knowledge_paths: { dir: sp.knowledgeDirAbs, latest: sp.knowledgeLatestAbs },
  };
}

export async function runKnowledgeSufficiencyPropose({ projectRoot, scope = "system", knowledgeVersion, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const parsedScope = requireScope(scope);
  const kv =
    knowledgeVersion == null
      ? (await readKnowledgeVersionOrDefault({ projectRoot: projectRootAbs })).version.current
      : requireKnowledgeVersionString(knowledgeVersion);

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });
  const sp = sufficiencyPaths(paths);

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) throw new Error(reposRes.message);
  const staleInfo = await evaluateScopeStaleness({ paths, registry: { ...reposRes.registry, base_dir: paths.reposRootAbs }, scope: parsedScope });

  const statusJson = await runKnowledgeStatus({ projectRoot: paths.opsRootAbs, dryRun: true });
  const coverageComplete = await coverageCompleteForScope({ statusJson, scope: parsedScope });
  const openDecisions = await listOpenDecisionsForScope({ decisionsDirAbs: paths.knowledge.decisionsAbs, scope: parsedScope });
  const blockers = computeBlockers({ scope: parsedScope, staleInfo, coverageComplete, openDecisions });

  const record = validateSufficiency({
    version: 1,
    scope: parsedScope,
    knowledge_version: kv,
    status: "proposed_sufficient",
    decided_by: null,
    decided_at: null,
    rationale_md_path: null,
    evidence_basis: deriveEvidenceBasis({ scope: parsedScope, paths }),
    blockers,
    stale_status: staleStatusToken(staleInfo),
  });

  const writeRes = await writeDecisionRecordPair({ sp, paths, record, scope: parsedScope, knowledgeVersion: kv, notesText: null, dryRun });
  const warnings = [];
  try {
    await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: !!dryRun });
  } catch (err) {
    warnings.push(err instanceof Error ? err.message : String(err));
  }
  return { ok: true, action: "propose", wrote: writeRes.wrote, record: writeRes.record, ops: writeRes.ops, knowledge: writeRes.knowledge, warnings: warnings.length ? warnings : null };
}

export async function runKnowledgeSufficiencyApprove({ projectRoot, scope = "system", knowledgeVersion, by, notes = null, notesFile = null, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const parsedScope = requireScope(scope);
  const kv = requireKnowledgeVersionString(knowledgeVersion);
  const name = normStr(by);
  if (!name) throw new Error("Missing --by \"<name>\".");

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });
  const sp = sufficiencyPaths(paths);

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) throw new Error(reposRes.message);
  const staleInfo = await evaluateScopeStaleness({ paths, registry: { ...reposRes.registry, base_dir: paths.reposRootAbs }, scope: parsedScope });

  const statusJson = await runKnowledgeStatus({ projectRoot: paths.opsRootAbs, dryRun: true });
  const coverageComplete = await coverageCompleteForScope({ statusJson, scope: parsedScope });
  const openDecisions = await listOpenDecisionsForScope({ decisionsDirAbs: paths.knowledge.decisionsAbs, scope: parsedScope });
  const blockers = computeBlockers({ scope: parsedScope, staleInfo, coverageComplete, openDecisions });

  if (staleStatusToken(staleInfo) === "hard_stale") throw new Error("Cannot approve sufficiency: scope is hard-stale.");
  if (!coverageComplete) throw new Error("Cannot approve sufficiency: scan coverage is incomplete for scope.");
  if (openDecisions.length) throw new Error("Cannot approve sufficiency: open decision packets exist for scope.");

  let notesText = normStr(notes) || null;
  if (!notesText && notesFile) {
    const abs = resolve(String(notesFile || ""));
    if (!existsSync(abs)) throw new Error(`--notes-file not found: ${abs}`);
    notesText = String(readFileSync(abs, "utf8") || "").trim() || null;
  }

  const record = validateSufficiency({
    version: 1,
    scope: parsedScope,
    knowledge_version: kv,
    status: "sufficient",
    decided_by: name,
    decided_at: nowISO(),
    rationale_md_path: null,
    evidence_basis: deriveEvidenceBasis({ scope: parsedScope, paths }),
    blockers: [],
    stale_status: staleStatusToken(staleInfo),
  });

  const writeRes = await writeDecisionRecordPair({ sp, paths, record, scope: parsedScope, knowledgeVersion: kv, notesText, dryRun });
  const warnings = [];
  try {
    await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: !!dryRun });
  } catch (err) {
    warnings.push(err instanceof Error ? err.message : String(err));
  }
  return { ok: true, action: "approve", wrote: writeRes.wrote, record: writeRes.record, ops: writeRes.ops, knowledge: writeRes.knowledge, warnings: warnings.length ? warnings : null };
}

export async function runKnowledgeSufficiencyReject({ projectRoot, scope = "system", knowledgeVersion, by, notes = null, notesFile = null, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const parsedScope = requireScope(scope);
  const kv = requireKnowledgeVersionString(knowledgeVersion);
  const name = normStr(by);
  if (!name) throw new Error("Missing --by \"<name>\".");

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });
  const sp = sufficiencyPaths(paths);

  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  const registry = reposRes.ok ? reposRes.registry : { version: 1, repos: [], base_dir: paths.reposRootAbs };
  const staleInfo = await evaluateScopeStaleness({ paths, registry: { ...registry, base_dir: paths.reposRootAbs }, scope: parsedScope });

  const statusJson = await runKnowledgeStatus({ projectRoot: paths.opsRootAbs, dryRun: true });
  const coverageComplete = await coverageCompleteForScope({ statusJson, scope: parsedScope });
  const openDecisions = await listOpenDecisionsForScope({ decisionsDirAbs: paths.knowledge.decisionsAbs, scope: parsedScope });
  const blockers = computeBlockers({ scope: parsedScope, staleInfo, coverageComplete, openDecisions });

  let notesText = normStr(notes) || null;
  if (!notesText && notesFile) {
    const abs = resolve(String(notesFile || ""));
    if (!existsSync(abs)) throw new Error(`--notes-file not found: ${abs}`);
    notesText = String(readFileSync(abs, "utf8") || "").trim() || null;
  }
  if (notesText) {
    blockers.push({ id: "rejected_by_human", title: "Rejected by human", details: notesText.split("\n")[0].slice(0, 200) });
  } else {
    blockers.push({ id: "rejected_by_human", title: "Rejected by human", details: `Rejected by ${name}` });
  }

  const record = validateSufficiency({
    version: 1,
    scope: parsedScope,
    knowledge_version: kv,
    status: "insufficient",
    decided_by: null,
    decided_at: null,
    rationale_md_path: null,
    evidence_basis: deriveEvidenceBasis({ scope: parsedScope, paths }),
    blockers,
    stale_status: staleStatusToken(staleInfo),
  });

  const writeRes = await writeDecisionRecordPair({ sp, paths, record, scope: parsedScope, knowledgeVersion: kv, notesText, dryRun });
  const warnings = [];
  try {
    await refreshPhasePrereqs({ projectRoot: paths.opsRootAbs, dryRun: !!dryRun });
  } catch (err) {
    warnings.push(err instanceof Error ? err.message : String(err));
  }
  return { ok: true, action: "reject", wrote: writeRes.wrote, record: writeRes.record, ops: writeRes.ops, knowledge: writeRes.knowledge, warnings: warnings.length ? warnings : null };
}

// Compatibility: "status/propose/confirm/revoke" legacy CLI commands.
export async function runKnowledgeSufficiencyConfirm({ projectRoot, by, dryRun = false } = {}) {
  const pr = requireAbsProjectRoot(projectRoot);
  const { version } = await readKnowledgeVersionOrDefault({ projectRoot: pr });
  return await runKnowledgeSufficiencyApprove({ projectRoot: pr, scope: "system", knowledgeVersion: version.current, by, dryRun });
}

export async function runKnowledgeSufficiencyRevoke({ projectRoot, reason, dryRun = false } = {}) {
  const pr = requireAbsProjectRoot(projectRoot);
  const { version } = await readKnowledgeVersionOrDefault({ projectRoot: pr });
  const r = normStr(reason) || "revoked";
  return await runKnowledgeSufficiencyReject({ projectRoot: pr, scope: "system", knowledgeVersion: version.current, by: "human", notes: r, dryRun });
}

export async function readSufficiencyOrDefault({ projectRoot } = {}) {
  const pr = requireAbsProjectRoot(projectRoot);
  const { version } = await readKnowledgeVersionOrDefault({ projectRoot: pr });
  return await readSufficiencyRecord({ projectRoot: pr, scope: "system", knowledgeVersion: version.current });
}

export async function requireConfirmedSufficiencyForDelivery({
  projectRoot,
  forceWithoutSufficiency = false,
  laneBLedgerAppend = null,
  scope = "system",
} = {}) {
  const pr = requireAbsProjectRoot(projectRoot);
  const parsedScope = requireScope(scope);
  const { version } = await readKnowledgeVersionOrDefault({ projectRoot: pr });
  const kv = version.current;

  const paths = await loadProjectPaths({ projectRoot: pr });
  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  const registry = reposRes.ok ? reposRes.registry : { version: 1, repos: [], base_dir: paths.reposRootAbs };
  const staleInfo = await evaluateScopeStaleness({ paths, registry: { ...registry, base_dir: paths.reposRootAbs }, scope: parsedScope });
  if (staleInfo.hard_stale === true) {
    return { ok: false, message: `Knowledge is hard-stale for scope ${parsedScope} (${(staleInfo.reasons || []).join(", ") || "stale"})` };
  }

  // Delivery rule:
  // - system scope: requires system sufficiency for current knowledge_version
  // - repo scope: may proceed if either repo scope OR system scope is sufficient (for current knowledge_version)
  const r = await readSufficiencyRecord({ projectRoot: pr, scope: parsedScope, knowledgeVersion: kv });
  const okSufficient = r.exists && r.sufficiency.status === "sufficient";
  if (okSufficient) return { ok: true, sufficiency: r.sufficiency, paths, via: parsedScope };
  let sys = null;
  if (parsedScope !== "system") {
    sys = await readSufficiencyRecord({ projectRoot: pr, scope: "system", knowledgeVersion: kv });
    const okSystem = sys.exists && sys.sufficiency.status === "sufficient";
    if (okSystem) return { ok: true, sufficiency: sys.sufficiency, paths, via: "system" };
  }

  if (!forceWithoutSufficiency) {
    return {
      ok: false,
      message: `Knowledge sufficiency not sufficient for ${parsedScope} @ ${kv}. Run --knowledge-sufficiency --propose and --approve before delivery.`,
    };
  }

  const user = normStr(process.env.SUDO_USER) || normStr(process.env.USER) || normStr(process.env.LOGNAME) || "unknown";
  const ev = { timestamp: nowISO(), type: "sufficiency_override", user, scope: parsedScope, knowledge_version: kv, status: r.sufficiency.status };
  if (typeof laneBLedgerAppend === "function") await laneBLedgerAppend(ev);
  return { ok: true, sufficiency: r.sufficiency, paths, override: ev };
}
