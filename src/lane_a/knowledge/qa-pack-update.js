import { existsSync } from "node:fs";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve, relative } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { ensureKnowledgeStructure, validateScope } from "./knowledge-utils.js";
import { getOriginUrl, probeGitWorkTree, runGit } from "./git-checks.js";
import { jsonStableStringify } from "../../utils/json.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function scopeKey(parsedScope) {
  if (!parsedScope || parsedScope.kind === "system") return "system";
  return `repo_${parsedScope.repo_id}`;
}

function stableUniq(arr) {
  const out = [];
  for (const x of Array.isArray(arr) ? arr : []) {
    const s = String(x || "").trim();
    if (!s) continue;
    if (!out.includes(s)) out.push(s);
  }
  return out;
}

function keywordsFromText(text) {
  const t = String(text || "").toLowerCase();
  const parts = t
    .replaceAll("'", " ")
    .replaceAll('"', " ")
    .split(/[^a-z0-9]+/g)
    .map((p) => p.trim())
    .filter(Boolean);
  const out = [];
  for (const p of parts) {
    if (p.length < 3) continue;
    if (out.includes(p)) continue;
    out.push(p);
  }
  return out;
}

async function readJsonIfExists(absPath) {
  try {
    const raw = await readFile(absPath, "utf8");
    if (!raw.trim()) return null;
    return JSON.parse(raw);
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
    throw err;
  }
}

async function writeJsonIfChanged(absPath, obj) {
  const next = jsonStableStringify(obj) + "\n";
  const prev = existsSync(absPath) ? await readFile(absPath, "utf8") : null;
  if (prev !== null && String(prev) === next) return { wrote: false, path: absPath };
  await mkdir(dirname(absPath), { recursive: true });
  await writeFile(absPath, next, "utf8");
  return { wrote: true, path: absPath };
}

async function readAllQaStrategistOutputs({ opsRootAbs, laneARootAbs, parsedScope }) {
  const dir = join(laneARootAbs, "committee");
  if (!existsSync(dir)) return [];
  let entries = [];
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch {
    entries = [];
  }

  const sk = scopeKey(parsedScope);
  const out = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const ts = e.name;
    const abs = join(dir, ts, `qa_strategist.${sk}.json`);
    if (!existsSync(abs)) continue;
    const j = await readJsonIfExists(abs);
    const rel = opsRootAbs ? relative(String(opsRootAbs), abs).split("\\").join("/") : null;
    if (j) out.push({ ts, abs, rel_from_ops: rel && !rel.startsWith("..") ? rel : null, json: j });
  }
  out.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
  return out;
}

function normalizeInvariant({ inv, fromOutput, source }) {
  const id = normStr(inv?.id);
  const text = normStr(inv?.text);
  const severityRaw = normStr(inv?.severity).toLowerCase();
  const severity = severityRaw === "high" || severityRaw === "medium" || severityRaw === "low" ? severityRaw : "medium";

  const obligations = isPlainObject(fromOutput?.test_obligations) ? fromOutput.test_obligations : null;
  const requires = {
    unit: !!obligations?.unit?.required,
    integration: !!obligations?.integration?.required,
    e2e: !!obligations?.e2e?.required,
  };

  const existingKeywords = Array.isArray(inv?.keywords) ? stableUniq(inv.keywords) : [];
  const keywords = existingKeywords.length ? existingKeywords : keywordsFromText(text);

  return {
    id,
    text,
    severity,
    keywords,
    requires,
    scopes: stableUniq([normStr(fromOutput?.scope)]),
    sources: source ? [source] : [],
  };
}

function mergeInvariants({ existingDoc, strategistOutputs }) {
  const cur = isPlainObject(existingDoc) ? existingDoc : { version: 1, invariants: [] };
  const curInv = Array.isArray(cur.invariants) ? cur.invariants : [];

  const byId = new Map();
  for (const i of curInv) {
    const id = normStr(i?.id);
    if (!id) continue;
    byId.set(id, i);
  }

  let added = 0;
  let updated = 0;
  const addedInvariantIds = [];

  for (const out of strategistOutputs) {
    const j = out.json;
    const invs = Array.isArray(j?.required_invariants) ? j.required_invariants : [];
    const source = {
      committee_output_path: out.rel_from_ops || null,
      committee_ts: out.ts,
      scope: normStr(j?.scope),
      created_at: normStr(j?.created_at),
    };

    for (const rawInv of invs) {
      const id = normStr(rawInv?.id);
      const text = normStr(rawInv?.text);
      if (!id || !text) continue;

      const nextNorm = normalizeInvariant({ inv: rawInv, fromOutput: j, source });
      const prev = byId.get(id);
      if (!prev) {
        byId.set(id, nextNorm);
        added += 1;
        addedInvariantIds.push(id);
        continue;
      }

      // Non-destructive merge: preserve existing keywords if present; merge scopes/sources.
      const merged = {
        ...prev,
        id,
        text: text || normStr(prev?.text),
        severity: nextNorm.severity || normStr(prev?.severity) || "medium",
        requires: isPlainObject(prev?.requires) ? { ...prev.requires, ...nextNorm.requires } : nextNorm.requires,
        keywords:
          Array.isArray(prev?.keywords) && prev.keywords.length
            ? stableUniq(prev.keywords)
            : Array.isArray(nextNorm.keywords)
              ? stableUniq(nextNorm.keywords)
              : [],
        scopes: stableUniq([...(Array.isArray(prev?.scopes) ? prev.scopes : []), ...(Array.isArray(nextNorm.scopes) ? nextNorm.scopes : [])]),
        sources: stableUniq([...(Array.isArray(prev?.sources) ? prev.sources.map((s) => jsonStableStringify(s)) : []), jsonStableStringify(source)]).map((s) => JSON.parse(s)),
      };

      const prevStable = jsonStableStringify(prev);
      const nextStable = jsonStableStringify(merged);
      if (prevStable !== nextStable) updated += 1;
      byId.set(id, merged);
    }
  }

  const invariants = Array.from(byId.values()).sort((a, b) => String(a.id).localeCompare(String(b.id)));
  const nextDoc = { version: 1, invariants };
  return { nextDoc, stats: { added, updated, added_invariant_ids: addedInvariantIds.sort() } };
}

function resolveGitBranchOrNull({ cwd }) {
  const branchRes = runGit({ cwd, args: ["rev-parse", "--abbrev-ref", "HEAD"], label: "git rev-parse --abbrev-ref HEAD" });
  const b = branchRes.ok ? branchRes.stdout.trim() : "";
  if (b && b !== "HEAD") return b;
  const symRes = runGit({ cwd, args: ["symbolic-ref", "--short", "HEAD"], label: "git symbolic-ref --short HEAD" });
  const sym = symRes.ok ? symRes.stdout.trim() : "";
  return sym || null;
}

async function ensureScenarioMarkers({ scenariosAbs, invariants }) {
  const prev = existsSync(scenariosAbs) ? await readFile(scenariosAbs, "utf8") : "";
  let text = String(prev || "");
  let appended = 0;

  for (const inv of Array.isArray(invariants) ? invariants : []) {
    const id = normStr(inv?.id);
    const invText = normStr(inv?.text);
    if (!id || !invText) continue;
    const marker = `<!-- invariant:${id} -->`;
    if (text.includes(marker)) continue;
    const block = [
      "",
      `## ${id}`,
      marker,
      "",
      `- Invariant: ${invText}`,
      `- Suggested E2E: (fill)`,
      "",
    ].join("\n");
    text += block;
    appended += 1;
  }

  if (text !== prev) await writeFile(scenariosAbs, text, "utf8");
  return { appended };
}

async function computeTestMatrixFromWorkHistory({ projectRootAbs }) {
  const workRootAbs = join(projectRootAbs, "ai", "lane_b", "work");
  if (!existsSync(workRootAbs)) {
    return {
      version: 1,
      overall: { total_work_items: 0, must_add_unit: 0, must_add_integration: 0, must_add_e2e: 0 },
      by_repo_id: {},
      by_invariant_id: {},
      samples: { work_ids: [] },
    };
  }

  let entries = [];
  try {
    entries = await readdir(workRootAbs, { withFileTypes: true });
  } catch {
    entries = [];
  }

  const workIds = entries.filter((e) => e.isDirectory()).map((e) => e.name).sort((a, b) => a.localeCompare(b));
  const overall = { total_work_items: 0, must_add_unit: 0, must_add_integration: 0, must_add_e2e: 0 };
  const byRepoId = {};
  const byInvariantId = {};

  for (const wid of workIds) {
    const abs = join(workRootAbs, wid, "QA", "obligations.json");
    const j = await readJsonIfExists(abs);
    if (!j) continue;
    overall.total_work_items += 1;
    if (j.must_add_unit) overall.must_add_unit += 1;
    if (j.must_add_integration) overall.must_add_integration += 1;
    if (j.must_add_e2e) overall.must_add_e2e += 1;

    const changed = Array.isArray(j.changed_paths_by_repo) ? j.changed_paths_by_repo : [];
    for (const r of changed) {
      const repoId = normStr(r?.repo_id);
      if (!repoId) continue;
      if (!byRepoId[repoId]) byRepoId[repoId] = { total: 0, must_add_unit: 0, must_add_integration: 0, must_add_e2e: 0 };
      byRepoId[repoId].total += 1;
      if (j.must_add_unit) byRepoId[repoId].must_add_unit += 1;
      if (j.must_add_integration) byRepoId[repoId].must_add_integration += 1;
      if (j.must_add_e2e) byRepoId[repoId].must_add_e2e += 1;
    }

    const matched = Array.isArray(j.invariants_matched_by_repo) ? j.invariants_matched_by_repo : [];
    for (const m of matched) {
      const ids = Array.isArray(m?.invariant_ids) ? m.invariant_ids : [];
      for (const idRaw of ids) {
        const id = normStr(idRaw);
        if (!id) continue;
        byInvariantId[id] = (byInvariantId[id] || 0) + 1;
      }
    }
  }

  const by_repo_id = {};
  for (const k of Object.keys(byRepoId).sort()) by_repo_id[k] = byRepoId[k];
  const by_invariant_id = {};
  for (const k of Object.keys(byInvariantId).sort()) by_invariant_id[k] = byInvariantId[k];

  return {
    version: 1,
    overall,
    by_repo_id,
    by_invariant_id,
    samples: { work_ids: workIds.slice(-50) },
  };
}

export async function runQaPackUpdate({ projectRoot, scope = "system", dryRun = false } = {}) {
  const projectRootAbs = resolve(String(projectRoot || ""));
  const parsedScope = validateScope(scope);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });

  await ensureKnowledgeStructure({ knowledgeRootAbs: paths.knowledge.rootAbs });

  const probe = probeGitWorkTree({ cwd: paths.knowledge.rootAbs });
  if (!probe.ok) return { ok: false, message: probe.message, knowledge_repo: paths.knowledge.rootAbs };

  const strategistOutputs = await readAllQaStrategistOutputs({ opsRootAbs: paths.opsRootAbs, laneARootAbs: paths.laneA.rootAbs, parsedScope });
  if (!strategistOutputs.length) {
    return { ok: false, message: `No QA strategist committee outputs found for scope ${parsedScope.scope}. Run --knowledge-committee --mode qa_strategist first.` };
  }

  const qaDirAbs = join(paths.knowledge.rootAbs, "qa");
  const invariantsAbs = join(qaDirAbs, "invariants.json");
  const scenariosAbs = join(qaDirAbs, "scenarios_e2e.md");
  const matrixAbs = join(qaDirAbs, "test_matrix.json");
  const riskAbs = join(qaDirAbs, "risk_rules.json");
  const qaPackRelPaths = ["qa/invariants.json", "qa/scenarios_e2e.md", "qa/test_matrix.json", "qa/risk_rules.json"];

  const prevInvDoc = await readJsonIfExists(invariantsAbs);
  const merged = mergeInvariants({ existingDoc: prevInvDoc, strategistOutputs });

  const writes = [];
  if (!dryRun) {
    writes.push(await writeJsonIfChanged(invariantsAbs, merged.nextDoc));
    const scenariosRes = await ensureScenarioMarkers({ scenariosAbs, invariants: merged.nextDoc.invariants });
    writes.push({ wrote: scenariosRes.appended > 0, path: scenariosAbs });
    const matrixDoc = await computeTestMatrixFromWorkHistory({ projectRootAbs: paths.opsRootAbs });
    writes.push(await writeJsonIfChanged(matrixAbs, matrixDoc));
    // risk_rules.json is created by ensureKnowledgeStructure; rewrite only if missing/invalid.
    const riskDoc = await readJsonIfExists(riskAbs);
    if (!riskDoc) {
      writes.push(await writeJsonIfChanged(riskAbs, { version: 1, rules: [] }));
    } else {
      writes.push({ wrote: false, path: riskAbs });
    }
  }

  // Always stage the QA pack files (even if only stubs were created by ensureKnowledgeStructure).
  const relTouched = stableUniq(
    [
      ...qaPackRelPaths,
      ...writes
        .filter((w) => w && w.wrote)
        .map((w) => relative(paths.knowledge.rootAbs, w.path))
        .filter((p) => p && !p.startsWith("..")),
    ].filter(Boolean),
  );

  if (dryRun) {
    return {
      ok: true,
      dry_run: true,
      scope: parsedScope.scope,
      knowledge_repo: paths.knowledge.rootAbs,
      touched: ["qa/invariants.json", "qa/scenarios_e2e.md", "qa/test_matrix.json", "qa/risk_rules.json"],
      stats: merged.stats,
      committed: false,
      pushed: false,
    };
  }

  // Git add/commit/push (commit is required if there are changes).
  if (relTouched.length) {
    const addRes = runGit({ cwd: paths.knowledge.rootAbs, args: ["add", ...relTouched], label: "git add qa pack" });
    if (!addRes.ok) return { ok: false, message: `Failed to git add QA pack.\n${String(addRes.stderr || addRes.stdout || addRes.error || "")}` };
  }

  const stagedRes = runGit({ cwd: paths.knowledge.rootAbs, args: ["diff", "--cached", "--name-only"], label: "git diff --cached --name-only" });
  const stagedAny = stagedRes.ok && stagedRes.stdout.trim().length > 0;
  if (!stagedAny) {
    return { ok: true, scope: parsedScope.scope, knowledge_repo: paths.knowledge.rootAbs, stats: merged.stats, committed: false, pushed: false };
  }

  const branch = resolveGitBranchOrNull({ cwd: paths.knowledge.rootAbs });

  const msg = `qa-pack-update(${parsedScope.scope}): invariants +${merged.stats.added} ~${merged.stats.updated}`;
  const commitRes = runGit({ cwd: paths.knowledge.rootAbs, args: ["commit", "-m", msg], label: "git commit -m <msg>" });
  if (!commitRes.ok && !String(commitRes.stderr || "").toLowerCase().includes("nothing to commit")) {
    return { ok: false, message: `Failed to git commit QA pack.\n${String(commitRes.stderr || commitRes.stdout || commitRes.error || "")}` };
  }

  const originRes = getOriginUrl({ cwd: paths.knowledge.rootAbs });
  if (!originRes.ok) {
    return {
      ok: true,
      scope: parsedScope.scope,
      knowledge_repo: paths.knowledge.rootAbs,
      branch,
      stats: merged.stats,
      committed: true,
      pushed: false,
      push_skipped: true,
      warnings: [originRes.warning],
    };
  }

  if (!branch || branch === "HEAD") return { ok: false, message: `Knowledge repo is in detached HEAD; cannot push. (${paths.knowledge.rootAbs})` };

  const pushRes = runGit({ cwd: paths.knowledge.rootAbs, args: ["push", "origin", branch], label: "git push origin <branch>" });
  if (!pushRes.ok) {
    const tail = String(pushRes.stderr || pushRes.stdout || pushRes.error || "").split("\n").slice(-12).join("\n");
    return { ok: false, message: `Knowledge repo push failed. Commit kept locally.\n${tail}`, knowledge_repo: paths.knowledge.rootAbs, branch };
  }

  return { ok: true, scope: parsedScope.scope, knowledge_repo: paths.knowledge.rootAbs, branch, stats: merged.stats, committed: true, pushed: true };
}
