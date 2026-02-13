import { existsSync, readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";

import { loadProjectPaths } from "../paths/project-paths.js";
import { loadRepoRegistry } from "../utils/repo-registry.js";
import { evaluateScopeStaleness } from "../lane_a/lane-a-staleness-policy.js";
import { validateKnowledgeVersion } from "../contracts/validators/index.js";
import { resolveStatePath } from "../project/state-paths.js";
import { appendFile } from "../utils/fs.js";
import { readSufficiencyRecord } from "../lane_a/knowledge/knowledge-sufficiency.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function parseVersionLike(v) {
  const s = normStr(v);
  if (!/^v\d+(\.\d+)*$/.test(s)) return null;
  return s;
}

function parseLaneAIntakeMetadata(text) {
  const lines = String(text || "")
    .split("\n")
    .slice(0, 200)
    .map((l) => l.trimEnd());
  const map = new Map();
  for (const l of lines) {
    const m = /^([A-Za-z_][A-Za-z0-9_-]*):\s*(.*?)\s*$/.exec(l.trim());
    if (!m) continue;
    const k = m[1].trim().toLowerCase();
    const v = m[2].trim();
    if (!k) continue;
    if (!map.has(k)) map.set(k, v);
  }
  const origin = normStr(map.get("origin"));
  const intake_approval_id = normStr(map.get("intake_approval_id")) || normStr(map.get("approval_id"));
  const knowledge_version = parseVersionLike(map.get("knowledge_version"));
  const scope = normStr(map.get("scope"));
  const sufficiency_override = normStr(map.get("sufficiency_override")).toLowerCase() === "true";
  return { origin: origin || null, intake_approval_id: intake_approval_id || null, knowledge_version: knowledge_version || null, scope: scope || null, sufficiency_override };
}

async function readJsonOptional(absPath) {
  const abs = resolve(String(absPath || ""));
  if (!existsSync(abs)) return { ok: true, exists: false, json: null };
  try {
    const t = await readFile(abs, "utf8");
    return { ok: true, exists: true, json: JSON.parse(String(t || "")) };
  } catch (err) {
    return { ok: false, exists: true, json: null, message: err instanceof Error ? err.message : String(err) };
  }
}

function loadKnowledgeVersionCurrentOrDefault(paths) {
  const abs = join(paths.laneA.rootAbs, "knowledge_version.json");
  if (!existsSync(abs)) return "v0";
  const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
  validateKnowledgeVersion(j);
  return normStr(j.current) || "v0";
}

async function readSufficientForDelivery({ projectRootAbs, scope, knowledgeVersion }) {
  // Rule: a repo-scoped intake may proceed if either:
  // - system scope is sufficient for the current knowledge_version, OR
  // - the specific repo scope is sufficient for the current knowledge_version.
  // System-scoped intake requires system sufficiency.
  const sys = await readSufficiencyRecord({ projectRoot: projectRootAbs, scope: "system", knowledgeVersion });
  if (sys.exists && normStr(sys.sufficiency.status) === "sufficient") return { ok: true, sufficient: true, status: "sufficient", via: "system" };
  if (scope === "system") return { ok: true, sufficient: false, status: normStr(sys.sufficiency.status) || "insufficient", via: "system" };
  const repo = await readSufficiencyRecord({ projectRoot: projectRootAbs, scope, knowledgeVersion });
  const okRepo = repo.exists && normStr(repo.sufficiency.status) === "sufficient";
  return { ok: true, sufficient: okRepo, status: normStr(repo.sufficiency.status) || "insufficient", via: okRepo ? "repo" : "repo" };
}

function requireValidScope(scope) {
  const s = normStr(scope);
  if (!s) return null;
  if (s === "system") return "system";
  if (/^repo:[A-Za-z0-9._-]+$/.test(s)) return s;
  return null;
}

export async function validateLaneAOriginIntake({ projectRoot, intakeText, metadata = null } = {}) {
  const projectRootAbs = typeof projectRoot === "string" && projectRoot.trim() ? resolve(projectRoot.trim()) : null;
  if (!projectRootAbs || !isAbsolute(projectRootAbs)) throw new Error("validateLaneAOriginIntake: projectRoot must be an absolute OPS_ROOT.");

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  const meta = metadata || parseLaneAIntakeMetadata(intakeText);
  if (normStr(meta.origin).toLowerCase() !== "lane_a") return { ok: true, lane_a: false, meta };

  const iaId = normStr(meta.intake_approval_id);
  const scope = requireValidScope(meta.scope);
  const kv = parseVersionLike(meta.knowledge_version);
  if (!iaId) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: "origin=lane_a requires intake_approval_id." };
  if (!scope) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: "origin=lane_a requires scope=system|repo:<id>." };
  if (!kv) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: "origin=lane_a requires knowledge_version=vX[.Y...]." };

  const iaAbs = join(paths.laneA.rootAbs, "intake_approvals", "processed", `${iaId}.json`);
  const iaRes = await readJsonOptional(iaAbs);
  if (!iaRes.ok) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: `Invalid IA file: ${iaRes.message}` };
  if (!iaRes.exists) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: `Missing intake approval: ${iaAbs}` };
  const ia = iaRes.json;
  const iaScope = normStr(ia?.scope);
  const iaKv = normStr(ia?.knowledge_version);
  if (normStr(ia?.id) !== iaId) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: `IA id mismatch (expected ${iaId}).` };
  if (iaScope !== scope) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: `IA scope mismatch (intake scope=${scope}, IA scope=${iaScope || "(missing)"}).` };
  if (iaKv !== kv) return { ok: false, lane_a: true, reason_code: "knowledge_version_mismatch", message: `IA knowledge_version mismatch (intake=${kv}, IA=${iaKv || "(missing)"}).` };

  // Version locking between lanes: intake version must match current Lane A version pointer.
  const currentKv = loadKnowledgeVersionCurrentOrDefault(paths);
  if (currentKv !== kv) {
    return { ok: false, lane_a: true, reason_code: "knowledge_version_mismatch", message: `knowledge_version mismatch (intake=${kv}, current=${currentKv}). Re-run update meeting and re-approve intake.` };
  }

  // Staleness enforcement (authoritative).
  const reposRes = await loadRepoRegistry({ projectRoot: paths.opsRootAbs });
  if (!reposRes.ok) return { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: reposRes.message };
  const st = await evaluateScopeStaleness({ paths, registry: reposRes.registry, scope });
  if (st.stale) return { ok: false, lane_a: true, reason_code: "knowledge_stale", message: `Knowledge is stale for scope ${scope} (${(st.reasons || []).join(", ")})`, reasons: st.reasons };

  // Sufficiency enforcement (allow override if IA explicitly marks it).
  const suff = await readSufficientForDelivery({ projectRootAbs: paths.opsRootAbs, scope, knowledgeVersion: kv });
  const overrideUsed = suff.sufficient !== true && (ia?.sufficiency_override === true || meta.sufficiency_override === true);
  const suffOk = suff.sufficient === true || overrideUsed;
  if (suffOk !== true) {
    return {
      ok: false,
      lane_a: true,
      reason_code: "lane_a_governance_violation",
      message: "Knowledge sufficiency not sufficient. Run --knowledge-sufficiency --propose and --approve before delivery (or explicitly override in IA).",
    };
  }

  return { ok: true, lane_a: true, meta: { ...meta, scope, knowledge_version: kv }, sufficiency_status: suff.status, sufficiency_override_used: overrideUsed };
}

export async function assertLaneAGovernanceForWorkId({ workId, phase, projectRoot = null } = {}) {
  const wid = normStr(workId);
  if (!wid) throw new Error("assertLaneAGovernanceForWorkId: workId is required.");
  const projectRootAbs = projectRoot ? resolve(String(projectRoot)) : null;

  const metaAbs = resolveStatePath(`ai/lane_b/work/${wid}/META.json`, { requiredRoot: true });
  let meta = null;
  if (existsSync(metaAbs)) {
    try {
      meta = JSON.parse(String(readFileSync(metaAbs, "utf8") || ""));
    } catch {
      meta = null;
    }
  }

  const intakeAbs = resolveStatePath(`ai/lane_b/work/${wid}/INTAKE.md`, { requiredRoot: true });
  const intakeText = existsSync(intakeAbs) ? String(readFileSync(intakeAbs, "utf8") || "") : "";
  const parsed = parseLaneAIntakeMetadata(intakeText);
  const origin = (normStr(meta?.origin) || normStr(parsed?.origin) || "").toLowerCase();
  if (origin !== "lane_a") return { ok: true, lane_a: false };
  // Prefer META fields if present.
  const merged = {
    ...parsed,
    origin: "lane_a",
    intake_approval_id: normStr(meta?.intake_approval_id) || parsed.intake_approval_id,
    knowledge_version: normStr(meta?.knowledge_version) || parsed.knowledge_version,
    scope: normStr(meta?.lane_a_scope) || parsed.scope,
    sufficiency_override: meta?.sufficiency_override === true || parsed.sufficiency_override === true,
  };

  const pr = projectRootAbs || (process.env.AI_PROJECT_ROOT ? resolve(String(process.env.AI_PROJECT_ROOT)) : null);
  const res = await validateLaneAOriginIntake({ projectRoot: pr, intakeText, metadata: merged });
  if (!res.ok) {
    return { ok: false, lane_a: true, reason_code: res.reason_code || "lane_a_governance_violation", message: `[${phase || "lane_b"}] ${res.message || "Lane A governance violation"}` };
  }
  if (res.sufficiency_override_used === true) {
    try {
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({ timestamp: new Date().toISOString(), type: "sufficiency_override_used", workId: wid, phase: phase || null, origin: "lane_a" }) + "\n",
      );
    } catch {
      // ignore
    }
  }
  return { ok: true, lane_a: true };
}
