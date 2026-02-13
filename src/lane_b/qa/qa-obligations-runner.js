import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve, posix as pathPosix } from "node:path";

import { loadProjectPaths } from "../../paths/project-paths.js";
import { validatePatchPlan } from "../../validators/patch-plan-validator.js";
import { validateRepoIndex } from "../../contracts/validators/index.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeRepoRelPath(p) {
  const s = String(p || "").trim().replaceAll("\\", "/");
  if (!s) return "";
  const norm = pathPosix.normalize(s);
  if (norm === "." || norm === "./") return ".";
  return norm.startsWith("./") ? norm.slice(2) : norm;
}

function looksLikeTestPath(p) {
  const s = normalizeRepoRelPath(p);
  if (!s || s === ".") return false;
  const lower = s.toLowerCase();
  if (lower.includes("__tests__/")) return true;
  if (lower.includes("/test/") || lower.startsWith("test/")) return true;
  if (lower.includes("/tests/") || lower.startsWith("tests/")) return true;
  if (lower.includes("/spec/") || lower.startsWith("spec/")) return true;
  if (/(^|\/).+\\.(test|spec)\\.(js|jsx|ts|tsx)$/.test(lower)) return true;
  if (/(^|\/).+_test\\.(go|py)$/.test(lower)) return true;
  if (lower.includes("/e2e/") || lower.startsWith("e2e/")) return true;
  if (lower.includes("/cypress/") || lower.includes("/playwright/")) return true;
  return false;
}

function looksLikeCodePath(p) {
  const s = normalizeRepoRelPath(p);
  if (!s || s === ".") return false;
  const lower = s.toLowerCase();
  if (looksLikeTestPath(lower)) return false;
  if (lower.endsWith(".md") || lower.endsWith(".txt")) return false;
  if (lower.endsWith(".png") || lower.endsWith(".jpg") || lower.endsWith(".jpeg") || lower.endsWith(".gif") || lower.endsWith(".svg")) return false;
  if (lower.endsWith(".json") || lower.endsWith(".yml") || lower.endsWith(".yaml")) return true; // configs are still "code-ish"
  if (lower.endsWith(".js") || lower.endsWith(".jsx") || lower.endsWith(".ts") || lower.endsWith(".tsx")) return true;
  if (lower.endsWith(".py") || lower.endsWith(".go") || lower.endsWith(".java") || lower.endsWith(".kt") || lower.endsWith(".cs")) return true;
  if (lower.endsWith(".rb") || lower.endsWith(".php") || lower.endsWith(".rs")) return true;
  return false;
}

function classifyRiskLevel(levelRaw) {
  const v = String(levelRaw || "").trim().toLowerCase();
  if (v === "high") return "high";
  if (v === "normal") return "normal";
  if (v === "low") return "low";
  return "unknown";
}

function maxRisk(a, b) {
  const order = { high: 3, normal: 2, low: 1, unknown: 0 };
  return (order[a] || 0) >= (order[b] || 0) ? a : b;
}

function normalizeKeyword(raw) {
  const s = String(raw || "").trim().toLowerCase();
  if (!s) return "";
  const norm = s.replaceAll("_", " ").replaceAll("-", " ").trim();
  return norm;
}

function severityToRiskLevel(sevRaw) {
  const s = String(sevRaw || "").trim().toLowerCase();
  if (s === "high") return "high";
  if (s === "medium" || s === "normal") return "normal";
  if (s === "low") return "low";
  return "unknown";
}

function invariantAppliesToRepo(inv, repoId) {
  const scopes = Array.isArray(inv?.scopes) ? inv.scopes.map((x) => String(x || "").trim()).filter(Boolean) : [];
  if (!scopes.length) return true; // backward compatible
  if (scopes.includes("system")) return true;
  return scopes.includes(`repo:${String(repoId || "").trim()}`);
}

function invariantMatchesChangedPaths(inv, changedPaths) {
  const kwsRaw = Array.isArray(inv?.keywords) ? inv.keywords : [];
  const kws = kwsRaw.map(normalizeKeyword).filter((k) => k && k.length >= 3);
  if (!kws.length) return { ok: false, matched_keywords: [] };
  const pathsLower = (Array.isArray(changedPaths) ? changedPaths : []).map((p) => String(p || "").toLowerCase());
  const matched = [];
  for (const kw of kws) {
    if (pathsLower.some((p) => p.includes(kw))) matched.push(kw);
  }
  return { ok: matched.length > 0, matched_keywords: matched.sort((a, b) => a.localeCompare(b)) };
}

async function readQaInvariantsFromKnowledge({ knowledgeRootAbs }) {
  const abs = join(String(knowledgeRootAbs || ""), "qa", "invariants.json");
  const raw = await readTextAbsIfExists(abs);
  if (!raw) return { ok: true, invariants: [], missing: true, path: abs };
  try {
    const j = JSON.parse(raw);
    const invs = Array.isArray(j?.invariants) ? j.invariants : [];
    return { ok: true, invariants: invs, missing: false, path: abs };
  } catch {
    return { ok: true, invariants: [], missing: false, invalid: true, path: abs };
  }
}

function computeApiSurfaceBindings({ repoIndex, changedPaths }) {
  const cp = new Set((Array.isArray(changedPaths) ? changedPaths : []).map(normalizeRepoRelPath).filter(Boolean));
  const api = repoIndex && isPlainObject(repoIndex.api_surface) ? repoIndex.api_surface : { openapi_files: [], routes_controllers: [], events_topics: [] };
  const openapi = Array.isArray(api.openapi_files) ? api.openapi_files : [];
  const routes = Array.isArray(api.routes_controllers) ? api.routes_controllers : [];
  const events = Array.isArray(api.events_topics) ? api.events_topics : [];
  return {
    known: {
      openapi_files: openapi.slice().sort(),
      routes_controllers: routes.slice().sort(),
      events_topics: events.slice().sort(),
    },
    touched: {
      openapi_files: openapi.filter((p) => cp.has(normalizeRepoRelPath(p))).sort(),
      routes_controllers: routes.filter((p) => cp.has(normalizeRepoRelPath(p))).sort(),
      events_topics: events.filter((p) => cp.has(normalizeRepoRelPath(p))).sort(),
    },
  };
}

function renderObligationsMd(doc) {
  const lines = [];
  lines.push(`# QA obligations`);
  lines.push("");
  lines.push(`Work: \`${doc.workId}\``);
  lines.push(`Created: \`${doc.created_at}\``);
  lines.push(`Risk: \`${doc.risk_level}\``);
  lines.push("");
  lines.push(`## Obligations`);
  lines.push("");
  lines.push(`- must_add_unit: \`${doc.must_add_unit ? "true" : "false"}\``);
  lines.push(`- must_add_integration: \`${doc.must_add_integration ? "true" : "false"}\``);
  lines.push(`- must_add_e2e: \`${doc.must_add_e2e ? "true" : "false"}\``);
  lines.push("");
  if (Array.isArray(doc.invariants_matched_by_repo) && doc.invariants_matched_by_repo.length) {
    lines.push(`## Matched invariants`);
    lines.push("");
    for (const r of doc.invariants_matched_by_repo) {
      lines.push(`- repo: \`${r.repo_id}\``);
      const ids = Array.isArray(r.invariant_ids) ? r.invariant_ids : [];
      if (!ids.length) {
        lines.push(`  - (none)`);
        continue;
      }
      lines.push(`  - ids: ${ids.map((x) => `\`${x}\``).join(", ")}`);
    }
    lines.push("");
  }
  lines.push(`## Changed paths`);
  lines.push("");
  for (const r of doc.changed_paths_by_repo || []) {
    lines.push(`- repo: \`${r.repo_id}\``);
    for (const p of r.paths || []) lines.push(`  - \`${p}\``);
  }
  lines.push("");
  if (Array.isArray(doc.suggested_test_directives) && doc.suggested_test_directives.length) {
    lines.push(`## Suggested test directives`);
    lines.push("");
    for (const d of doc.suggested_test_directives) lines.push(`- ${String(d).trim()}`);
    lines.push("");
  }
  lines.push(`## API surface bindings`);
  lines.push("");
  for (const b of doc.api_surface_bindings_by_repo || []) {
    lines.push(`- repo: \`${b.repo_id}\``);
    const touched = b.touched || {};
    const touchedAny =
      (Array.isArray(touched.openapi_files) && touched.openapi_files.length) ||
      (Array.isArray(touched.routes_controllers) && touched.routes_controllers.length) ||
      (Array.isArray(touched.events_topics) && touched.events_topics.length);
    if (!touchedAny) {
      lines.push(`  - touched: (none)`);
      continue;
    }
    for (const key of ["openapi_files", "routes_controllers", "events_topics"]) {
      const arr = Array.isArray(touched[key]) ? touched[key] : [];
      if (!arr.length) continue;
      lines.push(`  - touched.${key}: ${arr.map((p) => `\`${p}\``).join(", ")}`);
    }
  }
  lines.push("");
  return lines.join("\n");
}

async function readTextAbsIfExists(absPath) {
  try {
    return await readFile(absPath, "utf8");
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
    throw err;
  }
}

function resolveStateLikeAbs(projectRootAbs, maybeRelPath) {
  const p = String(maybeRelPath || "").trim();
  if (!p) return null;
  if (p.startsWith("/")) return p;
  return join(projectRootAbs, p);
}

export async function runQaObligations({ projectRoot, workId, dryRun = false } = {}) {
  const wid = normStr(workId);
  if (!wid) return { ok: false, message: "Missing workId." };
  const projectRootAbs = resolve(String(projectRoot || ""));
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });

  const workDirRel = `ai/lane_b/work/${wid}`;
  const workDirAbs = join(projectRootAbs, workDirRel);
  const bundleAbs = join(workDirAbs, "BUNDLE.json");
  const bundleText = await readTextAbsIfExists(bundleAbs);
  if (!bundleText) return { ok: false, message: `Missing ${workDirRel}/BUNDLE.json. Run propose with patch plans first.` };
  let bundle;
  try {
    bundle = JSON.parse(bundleText);
  } catch {
    return { ok: false, message: `Invalid JSON in ${workDirRel}/BUNDLE.json.` };
  }
  const repos = Array.isArray(bundle?.repos) ? bundle.repos : [];
  if (!repos.length) return { ok: false, message: "BUNDLE.json has no repos." };

  const changedByRepo = [];
  const bindingsByRepo = [];
  const matchedInvariantsByRepo = [];
  const suggested = [];
  let overallRisk = "unknown";

  let mustAddUnit = false;
  let mustAddIntegration = false;
  let mustAddE2e = false;

  const invLoaded = await readQaInvariantsFromKnowledge({ knowledgeRootAbs: paths.knowledge.rootAbs });
  const invariants = invLoaded.ok ? invLoaded.invariants : [];

  for (const r of repos.slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)))) {
    const repoId = normStr(r?.repo_id);
    const patchPlanPath = normStr(r?.patch_plan_json_path);
    if (!repoId || !patchPlanPath) continue;
    const planAbs = resolveStateLikeAbs(projectRootAbs, patchPlanPath);
    const planText = planAbs ? await readTextAbsIfExists(planAbs) : null;
    if (!planText) return { ok: false, message: `Missing patch plan JSON: ${patchPlanPath}` };
    let plan;
    try {
      plan = JSON.parse(planText);
    } catch {
      return { ok: false, message: `Invalid JSON in patch plan: ${patchPlanPath}` };
    }
    const v = validatePatchPlan(plan, { policy: null });
    if (!v.ok) return { ok: false, message: `Patch plan invalid for ${repoId}: ${v.errors.join(" | ")}` };

    overallRisk = maxRisk(overallRisk, classifyRiskLevel(plan?.risk?.level));

    const edits = Array.isArray(plan.edits) ? plan.edits : [];
    const changedPaths = Array.from(new Set(edits.map((e) => normalizeRepoRelPath(e?.path)).filter((p) => p && p !== "."))).sort((a, b) => a.localeCompare(b));
    changedByRepo.push({ repo_id: repoId, paths: changedPaths });

    const anyCode = changedPaths.some((p) => looksLikeCodePath(p));
    if (anyCode) mustAddUnit = true;

    const anyTestEdits = changedPaths.some((p) => looksLikeTestPath(p));
    if (anyTestEdits) {
      // presence of tests does not remove obligations; it affects apply gating only.
    }

    // Repo index bindings (best-effort; may not exist yet).
    let repoIndex = null;
    try {
      const idxAbs = join(paths.knowledge.evidenceIndexReposAbs, repoId, "repo_index.json");
      if (existsSync(idxAbs)) {
        repoIndex = JSON.parse(String(readFileSync(idxAbs, "utf8") || ""));
        validateRepoIndex(repoIndex);
      }
    } catch {
      repoIndex = null;
    }
    const bindings = repoIndex ? computeApiSurfaceBindings({ repoIndex, changedPaths }) : { known: { openapi_files: [], routes_controllers: [], events_topics: [] }, touched: { openapi_files: [], routes_controllers: [], events_topics: [] } };
    bindingsByRepo.push({ repo_id: repoId, ...bindings });

    const touchedApi =
      bindings.touched.openapi_files.length || bindings.touched.routes_controllers.length || bindings.touched.events_topics.length;
    if (touchedApi) mustAddIntegration = true;

    const anyMigration = changedPaths.some((p) => p.toLowerCase().includes("migration") || p.toLowerCase().includes("migrations/"));
    if (anyMigration) mustAddIntegration = true;

    const anyUi =
      changedPaths.some((p) => {
        const lower = p.toLowerCase();
        return lower.startsWith("ui/") || lower.includes("/ui/") || lower.startsWith("frontend/") || lower.includes("/frontend/") || lower.startsWith("apps/") || lower.includes("/pages/");
      });
    if (anyUi) mustAddE2e = true;

    // Invariants from knowledge QA pack: intersect keywords with changed paths.
    const matched = [];
    for (const inv of invariants) {
      const id = normStr(inv?.id);
      const text = normStr(inv?.text);
      if (!id || !text) continue;
      if (!invariantAppliesToRepo(inv, repoId)) continue;
      const match = invariantMatchesChangedPaths(inv, changedPaths);
      if (!match.ok) continue;
      matched.push({ id, severity: normStr(inv?.severity), requires: inv?.requires || null });

      overallRisk = maxRisk(overallRisk, severityToRiskLevel(inv?.severity));

      if (isPlainObject(inv?.requires)) {
        if (inv.requires.unit) mustAddUnit = true;
        if (inv.requires.integration) mustAddIntegration = true;
        if (inv.requires.e2e) mustAddE2e = true;
      }

      const directive = `Invariant ${id}: add/extend tests to enforce this invariant in ${repoId}.`;
      if (!suggested.includes(directive)) suggested.push(directive);
    }
    if (matched.length) {
      matchedInvariantsByRepo.push({
        repo_id: repoId,
        invariant_ids: matched.map((m) => m.id).sort((a, b) => a.localeCompare(b)),
      });
    }

    if (anyCode && !suggested.includes(`Add/extend unit tests for ${repoId} changed modules.`)) suggested.push(`Add/extend unit tests for ${repoId} changed modules.`);
    if (mustAddIntegration && !suggested.includes(`Add/extend integration tests for ${repoId} API/data contracts.`)) suggested.push(`Add/extend integration tests for ${repoId} API/data contracts.`);
    if (mustAddE2e && !suggested.includes(`Add/extend e2e tests for ${repoId} user flows.`)) suggested.push(`Add/extend e2e tests for ${repoId} user flows.`);
  }

  // Risk can elevate obligations.
  if (overallRisk === "high") {
    mustAddIntegration = true;
    if (!mustAddE2e) mustAddE2e = true;
    suggested.unshift("Risk is high: require integration + e2e coverage for the changed behavior.");
  }

  const out = {
    version: 1,
    workId: wid,
    created_at: nowISO(),
    risk_level: overallRisk,
    changed_paths_by_repo: changedByRepo,
    invariants_matched_by_repo: matchedInvariantsByRepo.sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id))),
    must_add_unit: mustAddUnit,
    must_add_integration: mustAddIntegration,
    must_add_e2e: mustAddE2e,
    suggested_test_directives: suggested,
    api_surface_bindings_by_repo: bindingsByRepo,
  };

  const qaDirRel = `${workDirRel}/QA`;
  const jsonPath = `${qaDirRel}/obligations.json`;
  const mdPath = `${qaDirRel}/obligations.md`;
  if (!dryRun) {
    await mkdir(join(projectRootAbs, qaDirRel), { recursive: true });
    await writeFile(join(projectRootAbs, jsonPath), JSON.stringify(out, null, 2) + "\n", "utf8");
    await writeFile(join(projectRootAbs, mdPath), renderObligationsMd(out) + "\n", "utf8");
  }

  return { ok: true, workId: wid, obligations_json: jsonPath, obligations_md: mdPath, obligations: out, dry_run: !!dryRun };
}
