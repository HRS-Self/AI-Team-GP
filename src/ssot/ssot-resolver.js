import { createHash } from "node:crypto";
import { join, resolve, relative, isAbsolute, sep } from "node:path";
import { existsSync } from "node:fs";
import { readFile, writeFile, mkdir } from "node:fs/promises";

import { validateSsotSnapshot } from "../validators/ssot-snapshot-validator.js";
import { validateSsotView } from "../validators/ssot-view-validator.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { probeGitWorkTree } from "../lane_a/knowledge/git-checks.js";

function nowISO() {
  return new Date().toISOString();
}

function sha256HexBytes(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function sha256HexText(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isSameOrSubpath(parentAbs, childAbs) {
  const rel = relative(parentAbs, childAbs);
  if (!rel) return true;
  if (rel.startsWith("..")) return false;
  return !rel.includes(`..${sep}`) && !isAbsolute(rel);
}

function assertSafeProjectRelativeOut({ projectRootAbs, outPath }) {
  const pr = resolve(projectRootAbs);
  const outAbs = isAbsolute(outPath) ? resolve(outPath) : resolve(pr, outPath);
  if (!isSameOrSubpath(pr, outAbs)) throw new Error(`--out must be inside PROJECT_ROOT.\n- PROJECT_ROOT: ${pr}\n- out: ${outAbs}`);
  return outAbs;
}

async function readJsonAbs(pathAbs, label) {
  const text = await readFile(pathAbs, "utf8");
  try {
    return { ok: true, text: String(text || ""), json: JSON.parse(String(text || "")) };
  } catch {
    return { ok: false, message: `Invalid JSON in ${label}: ${pathAbs}` };
  }
}

function viewFilenameForViewId(viewId) {
  if (viewId === "global") return "teams/global.json";
  if (viewId.startsWith("team:")) return `teams/team-${viewId.slice("team:".length)}.json`;
  if (viewId.startsWith("pack:")) return `teams/pack-${viewId.slice("pack:".length)}.json`;
  throw new Error(`Unsupported view_id: ${viewId}`);
}

export async function resolveSsotBundle({ projectRoot, view, outPath, dryRun = false }) {
  const projectRootAbs = resolve(String(projectRoot || ""));
  const outAbs = assertSafeProjectRelativeOut({ projectRootAbs, outPath });

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  const knowledgeRootAbs = paths.knowledge.rootAbs;
  const probe = probeGitWorkTree({ cwd: knowledgeRootAbs });
  if (!probe.ok) return { ok: false, message: `Knowledge repo git check failed:\n${probe.message}` };

  const requestedView = String(view || "").trim();
  if (!requestedView) return { ok: false, message: "Missing --view (expected 'team:<TeamID>')." };
  if (!requestedView.startsWith("team:")) return { ok: false, message: "Invalid --view. Expected 'team:<TeamID>'." };
  const teamId = requestedView.slice("team:".length).trim();
  if (!teamId) return { ok: false, message: "Invalid --view. TeamID missing (team:<TeamID>)." };

  const snapshotAbs = resolve(knowledgeRootAbs, "ssot", "system", "PROJECT_SNAPSHOT.json");
  if (!existsSync(snapshotAbs)) return { ok: false, message: `Missing SSOT snapshot: ${snapshotAbs}` };
  const snapshotRaw = await readJsonAbs(snapshotAbs, "SSOT snapshot");
  if (!snapshotRaw.ok) return snapshotRaw;
  const snapshotV = validateSsotSnapshot(snapshotRaw.json);
  if (!snapshotV.ok) return { ok: false, message: `Invalid SSOT snapshot (${snapshotAbs}): ${snapshotV.errors.join(" | ")}` };
  const snapshot = snapshotV.normalized;

  if (snapshot.project_code !== paths.cfg.project_code) {
    return { ok: false, message: `SSOT project_code mismatch. PROJECT.json=${paths.cfg.project_code} snapshot=${snapshot.project_code}` };
  }

  const viewsDirAbs = resolve(knowledgeRootAbs, "views");
  if (!existsSync(viewsDirAbs)) return { ok: false, message: `Missing SSOT views directory: ${viewsDirAbs}` };

  const globalPacks = Array.isArray(paths.cfg.ssot_bundle_policy?.global_packs) ? paths.cfg.ssot_bundle_policy.global_packs : [];
  const requiredViewIds = ["global", `team:${teamId}`, ...globalPacks.map((p) => `pack:${p}`)];

  const includedViews = [];
  const sectionIds = new Set();
  for (const viewId of requiredViewIds) {
    const fn = viewFilenameForViewId(viewId);
    const p = resolve(viewsDirAbs, fn);
    if (!existsSync(p)) return { ok: false, message: `Missing SSOT view file for ${viewId}: ${p}` };
    const vRaw = await readJsonAbs(p, `SSOT view ${viewId}`);
    if (!vRaw.ok) return vRaw;
    const vV = validateSsotView(vRaw.json);
    if (!vV.ok) return { ok: false, message: `Invalid SSOT view (${p}): ${vV.errors.join(" | ")}` };
    if (vV.normalized.view_id !== viewId) return { ok: false, message: `SSOT view_id mismatch in ${p} (expected ${viewId}).` };
    includedViews.push(viewId);
    for (const sid of vV.normalized.section_ids) sectionIds.add(sid);
  }

  const sectionIndex = new Map(snapshot.sections.map((s) => [s.id, s]));
  const missingIds = Array.from(sectionIds).filter((id) => !sectionIndex.has(id));
  if (missingIds.length) return { ok: false, message: `SSOT view references unknown section_ids: ${missingIds.join(", ")}` };

  const drift = [];
  const resolvedSections = [];
  for (const id of Array.from(sectionIds).sort((a, b) => a.localeCompare(b))) {
    const meta = sectionIndex.get(id);
    const relPath = String(meta.path || "").trim();
    if (!relPath) return { ok: false, message: `SSOT snapshot section '${id}' missing path.` };
    const abs = resolve(knowledgeRootAbs, relPath);
    if (!isSameOrSubpath(knowledgeRootAbs, abs)) return { ok: false, message: `SSOT section path escapes knowledge repo root: ${relPath}` };
    if (!existsSync(abs)) return { ok: false, message: `Missing SSOT section file for '${id}': ${abs}` };
    const buf = await readFile(abs);
    const sha = sha256HexBytes(buf);
    if (sha !== meta.sha256) drift.push({ id, expected: meta.sha256, actual: sha, path: relPath });

    const parsed = (() => {
      try {
        return JSON.parse(String(buf));
      } catch {
        return null;
      }
    })();
    if (!parsed) return { ok: false, message: `Invalid JSON in SSOT section '${id}': ${relPath}` };
    resolvedSections.push({ id, path: relPath, sha256: sha, json: parsed });
  }
  if (drift.length) {
    return {
      ok: false,
      message: `SSOT drift detected: ${drift.length} section(s) do not match PROJECT_SNAPSHOT.json sha256.`,
      drift,
      snapshot_path: snapshotAbs,
    };
  }

  const snapshotSha = sha256HexText(snapshotRaw.text);
  const bundleCore = {
    version: 1,
    project_code: snapshot.project_code,
    generated_at: nowISO(),
    requested_view: requestedView,
    included_views: includedViews.slice().sort((a, b) => a.localeCompare(b)),
    snapshot: { path: "ssot/system/PROJECT_SNAPSHOT.json", sha256: snapshotSha },
    sections: resolvedSections.slice().sort((a, b) => a.id.localeCompare(b.id)),
  };
  const bundleHash = sha256HexText(JSON.stringify(bundleCore, null, 2) + "\n");
  const bundle = { ...bundleCore, bundle_hash: bundleHash };

  if (dryRun) return { ok: true, dry_run: true, out: outAbs, bundle, bundle_hash: bundleHash };

  await mkdir(resolve(outAbs, ".."), { recursive: true });
  await writeFile(outAbs, JSON.stringify(bundle, null, 2) + "\n", "utf8");
  return { ok: true, dry_run: false, out: outAbs, out_rel: relative(projectRootAbs, outAbs).split(sep).join("/"), bundle_hash: bundleHash };
}
