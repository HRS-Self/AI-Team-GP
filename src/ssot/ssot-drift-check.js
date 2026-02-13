import { createHash } from "node:crypto";

import { readTextIfExists, writeText, ensureDir } from "../utils/fs.js";
import { nowTs } from "../utils/id.js";
import { resolveStatePath } from "../project/state-paths.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizePrefix(p) {
  const s = String(p || "").trim().replaceAll("\\", "/");
  if (!s) return null;
  return s.startsWith("./") ? s.slice(2) : s;
}

function pathMatchesPrefix(path, prefix) {
  const p = normalizePrefix(path);
  const pre = normalizePrefix(prefix);
  if (!p || !pre) return false;
  if (p === pre) return true;
  if (!pre.endsWith("/")) return p.startsWith(`${pre}/`);
  return p.startsWith(pre);
}

function collectImpactedPathsFromPatchPlans(plans) {
  const out = new Set();
  for (const plan of plans) {
    const edits = Array.isArray(plan?.edits) ? plan.edits : [];
    for (const e of edits) {
      const p = typeof e?.path === "string" ? e.path.trim() : "";
      if (p) out.add(p);
    }
    const scope = isPlainObject(plan?.scope) ? plan.scope : null;
    if (scope && Array.isArray(scope.allowed_paths)) {
      for (const p of scope.allowed_paths) {
        const s = typeof p === "string" ? p.trim() : "";
        if (s) out.add(s);
      }
    }
  }
  return Array.from(out).sort((a, b) => a.localeCompare(b));
}

function collectImpactedPathsFromProposals(proposals) {
  const out = new Set();
  for (const p of proposals) {
    const arr = Array.isArray(p?.likely_files_or_areas_impacted) ? p.likely_files_or_areas_impacted : [];
    for (const item of arr) {
      const s = String(item || "").trim();
      if (!s) continue;
      // Heuristic: capture file-ish tokens only (deterministic, conservative).
      for (const token of s.split(/\s+/g)) {
        const t = token.trim();
        if (!t) continue;
        if (t.includes("/") || t.includes(".") || t.includes("\\")) out.add(t.replaceAll("\\", "/"));
      }
    }
  }
  return Array.from(out).sort((a, b) => a.localeCompare(b));
}

function computeViolations({ constraints, repoIds, impactedPaths }) {
  const hard = [];
  const soft = [];

  const allowedRepoIds = Array.isArray(constraints?.allowed_repo_ids) ? constraints.allowed_repo_ids.map(String) : [];
  const forbiddenRepoIds = Array.isArray(constraints?.forbidden_repo_ids) ? constraints.forbidden_repo_ids.map(String) : [];
  const allowedPaths = Array.isArray(constraints?.allowed_paths) ? constraints.allowed_paths.map(String) : [];
  const forbiddenPaths = Array.isArray(constraints?.forbidden_paths) ? constraints.forbidden_paths.map(String) : [];

  for (const repoId of repoIds) {
    if (forbiddenRepoIds.includes(repoId)) {
      hard.push({
        rule_id: "ssot.constraints.forbidden_repo_ids",
        doc: "ssot/constraints",
        section: "forbidden_repo_ids",
        evidence: `repo_id '${repoId}' is forbidden by SSOT constraints.`,
      });
    }
    if (allowedRepoIds.length && !allowedRepoIds.includes(repoId)) {
      hard.push({
        rule_id: "ssot.constraints.allowed_repo_ids",
        doc: "ssot/constraints",
        section: "allowed_repo_ids",
        evidence: `repo_id '${repoId}' is not allowed by SSOT constraints.`,
      });
    }
  }

  for (const path of impactedPaths) {
    for (const pre of forbiddenPaths) {
      if (pathMatchesPrefix(path, pre)) {
        hard.push({
          rule_id: "ssot.constraints.forbidden_paths",
          doc: "ssot/constraints",
          section: "forbidden_paths",
          evidence: `path '${path}' matches forbidden prefix '${pre}'.`,
        });
        break;
      }
    }
    if (allowedPaths.length) {
      const ok = allowedPaths.some((pre) => pathMatchesPrefix(path, pre));
      if (!ok) {
        hard.push({
          rule_id: "ssot.constraints.allowed_paths",
          doc: "ssot/constraints",
          section: "allowed_paths",
          evidence: `path '${path}' is not within any allowed_paths prefix.`,
        });
      }
    }
  }

  return { hard, soft };
}

export async function runSsotDriftCheck({ workId }) {
  const workDir = `ai/lane_b/work/${workId}`;
  const ssotPath = `${workDir}/SSOT_BUNDLE.json`;
  const ssotText = await readTextIfExists(ssotPath);
  if (!ssotText) return { ok: false, message: `Missing ${ssotPath}. Run planner/reviewer/qa first to produce SSOT_BUNDLE.json.` };
  let ssot = null;
  try {
    ssot = JSON.parse(ssotText);
  } catch {
    return { ok: false, message: `Invalid JSON in ${ssotPath}.` };
  }

  const teamId = typeof ssot?.team_id === "string" ? ssot.team_id.trim() : "";
  const bundleSha = sha256Hex(ssotText);

  const routingPath = `${workDir}/ROUTING.json`;
  const routingText = await readTextIfExists(routingPath);
  let routing = null;
  try {
    routing = routingText ? JSON.parse(routingText) : null;
  } catch {
    routing = null;
  }
  const selectedRepos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.map((x) => String(x)).filter(Boolean) : [];

  // Prefer patch plans for impacted paths; fall back to proposals if patch plans missing.
  const plans = [];
  for (const repoId of selectedRepos) {
    const p = `${workDir}/patch-plans/${repoId}.json`;
    const txt = await readTextIfExists(p);
    if (!txt) continue;
    try {
      plans.push(JSON.parse(txt));
    } catch {
      return { ok: false, message: `Invalid JSON in patch plan: ${p}` };
    }
  }

  const proposals = [];
  if (!plans.length) {
    // Gather all proposal JSONs (if present).
    // Note: proposal JSON is authoritative; markdown is not required here.
    const proposalsDir = `${workDir}/proposals`;
    const { readdir } = await import("node:fs/promises");
    try {
      const entries = await readdir(resolveStatePath(proposalsDir), { withFileTypes: true });
      const files = entries
        .filter((e) => e.isFile() && e.name.endsWith(".json"))
        .map((e) => `${proposalsDir}/${e.name}`)
        .sort((a, b) => a.localeCompare(b));
      for (const f of files) {
        const txt = await readTextIfExists(f);
        if (!txt) continue;
        try {
          proposals.push(JSON.parse(txt));
        } catch {
          return { ok: false, message: `Invalid JSON in proposal: ${f}` };
        }
      }
    } catch {
      // ignore (no proposals dir)
    }
  }

  const impactedPaths = plans.length ? collectImpactedPathsFromPatchPlans(plans) : collectImpactedPathsFromProposals(proposals);
  const constraints = isPlainObject(ssot?.constraints) ? ssot.constraints : null;
  const { hard, soft } = computeViolations({ constraints, repoIds: selectedRepos, impactedPaths });

  const out = {
    workId,
    team_id: teamId || null,
    bundle_sha256: bundleSha,
    created_at: nowTs(),
    hard_violations: hard,
    soft_deviations: soft,
  };

  const outPath = `${workDir}/SSOT_DRIFT.json`;
  await ensureDir(workDir);
  await writeText(outPath, JSON.stringify(out, null, 2) + "\n");
  return { ok: true, workId, out_path: outPath, hard_violations: hard.length, soft_deviations: soft.length };
}
