import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";

import { jsonStableStringify, sortKeysDeep } from "../utils/json.js";
import { readTextIfExists, writeText } from "../utils/fs.js";
import { nowTs } from "../utils/id.js";
import { resolveStatePath } from "../project/state-paths.js";

function sha256HexText(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeStringArrayOrNull(v) {
  if (!Array.isArray(v)) return null;
  const out = [];
  for (const it of v) {
    const s = String(it || "").trim();
    if (!s) continue;
    if (!out.includes(s)) out.push(s);
  }
  return out;
}

function extractConstraintsFromSections(sections) {
  const allowed_paths = [];
  const forbidden_paths = [];
  const allowed_repo_ids = [];
  const forbidden_repo_ids = [];

  const merge = (dest, arr) => {
    const norm = normalizeStringArrayOrNull(arr);
    if (!norm) return;
    for (const s of norm) if (!dest.includes(s)) dest.push(s);
  };

  for (const s of Array.isArray(sections) ? sections : []) {
    const json = isPlainObject(s) ? s.json : null;
    if (!isPlainObject(json)) continue;

    const candidates = [];
    if (isPlainObject(json.constraints)) candidates.push(json.constraints);
    if (isPlainObject(json.delivery)) candidates.push(json.delivery);
    candidates.push(json);

    for (const c of candidates) {
      if (!isPlainObject(c)) continue;
      merge(allowed_paths, c.allowed_paths);
      merge(forbidden_paths, c.forbidden_paths);
      merge(allowed_repo_ids, c.allowed_repo_ids);
      merge(forbidden_repo_ids, c.forbidden_repo_ids);
    }
  }

  const out = {
    allowed_paths: allowed_paths.slice().sort((a, b) => a.localeCompare(b)),
    forbidden_paths: forbidden_paths.slice().sort((a, b) => a.localeCompare(b)),
    allowed_repo_ids: allowed_repo_ids.slice().sort((a, b) => a.localeCompare(b)),
    forbidden_repo_ids: forbidden_repo_ids.slice().sort((a, b) => a.localeCompare(b)),
  };

  const hasAny =
    out.allowed_paths.length || out.forbidden_paths.length || out.allowed_repo_ids.length || out.forbidden_repo_ids.length;
  return hasAny ? out : null;
}

function materializeWorkSsotBundleFromTeamBundle({ workId, teamId, teamBundleJson }) {
  const includedViews = Array.isArray(teamBundleJson?.included_views) ? teamBundleJson.included_views : [];
  const snapshot = isPlainObject(teamBundleJson?.snapshot) ? teamBundleJson.snapshot : null;
  const sections = Array.isArray(teamBundleJson?.sections) ? teamBundleJson.sections : [];

  const resolved_inputs = [];
  if (snapshot && typeof snapshot.path === "string" && typeof snapshot.sha256 === "string") {
    resolved_inputs.push({ ref_type: "ssot_snapshot", ref_id: snapshot.path, sha256: snapshot.sha256 });
  }
  for (const s of sections) {
    const id = typeof s?.id === "string" ? s.id.trim() : "";
    const path = typeof s?.path === "string" ? s.path.trim() : "";
    const sha = typeof s?.sha256 === "string" ? s.sha256.trim() : "";
    if (!id || !path || !sha) continue;
    resolved_inputs.push({ ref_type: "ssot_section", ref_id: path, sha256: sha, section_id: id });
  }

  const normalizedViews = includedViews.map((v) => String(v)).filter(Boolean).sort((a, b) => a.localeCompare(b));
  const normalizedInputs = resolved_inputs
    .slice()
    .map((x) => ({ ...x }))
    .sort((a, b) => `${a.ref_type}:${a.ref_id}`.localeCompare(`${b.ref_type}:${b.ref_id}`))
    .map((x) => x);

  // This work-level SSOT_BUNDLE.json is intentionally reference-only:
  // it MUST NOT duplicate full SSOT section contents (Lane A SSOT is the source of truth).
  const out = {
    version: 1,
    workId,
    team_id: teamId,
    created_at: nowTs(),
    ssot_snapshot_sha256: typeof snapshot?.sha256 === "string" ? snapshot.sha256 : null,
    views: normalizedViews,
    resolved_inputs: normalizedInputs.map((x) => {
      const { section_id, ...rest } = x;
      return rest;
    }),
    constraints: extractConstraintsFromSections(sections),
  };

  return out;
}

export async function ensureWorkSsotBundle({ workId, teamId, workDir, teamBundlePath, allowOverwriteTeamMismatch = false }) {
  const outPath = `${workDir}/SSOT_BUNDLE.json`;
  const outAbs = resolveStatePath(outPath);

  if (existsSync(outAbs)) {
    const existingText = await readFile(outAbs, "utf8");
    let parsed = null;
    try {
      parsed = JSON.parse(String(existingText || ""));
    } catch {
      return { ok: false, message: `Invalid JSON in existing SSOT_BUNDLE.json: ${outPath}` };
    }
    const existingTeam = typeof parsed?.team_id === "string" ? parsed.team_id.trim() : "";
    const existingWork = typeof parsed?.workId === "string" ? parsed.workId.trim() : "";
    if (existingWork && existingWork !== String(workId)) return { ok: false, message: `SSOT_BUNDLE.json workId mismatch: ${outPath}` };
    if (existingTeam && existingTeam !== String(teamId)) {
      if (!allowOverwriteTeamMismatch) {
        return {
          ok: false,
          message: `SSOT_BUNDLE.json team_id mismatch (expected ${teamId}, found ${existingTeam}). Refuse to overwrite ${outPath}.`,
        };
      }
    }
    if (!existingTeam || existingTeam === String(teamId)) return { ok: true, outPath };
  }

  const teamText = await readTextIfExists(teamBundlePath);
  if (!teamText) return { ok: false, message: `Missing SSOT team bundle: ${teamBundlePath}` };
  let teamJson = null;
  try {
    teamJson = JSON.parse(String(teamText || ""));
  } catch {
    return { ok: false, message: `Invalid SSOT team bundle JSON: ${teamBundlePath}` };
  }

  const bundle = materializeWorkSsotBundleFromTeamBundle({ workId, teamId, teamBundleJson: teamJson });
  await writeText(outPath, jsonStableStringify(bundle));
  return { ok: true, outPath };
}

export function renderSsotExcerptsForLlm({ teamBundleText, maxSectionChars = 1800, maxTotalChars = 18000 } = {}) {
  let teamJson = null;
  try {
    teamJson = JSON.parse(String(teamBundleText || ""));
  } catch {
    return "(unavailable: invalid SSOT team bundle JSON)";
  }

  const sections = Array.isArray(teamJson?.sections) ? teamJson.sections : [];

  const blocks = [];
  let used = 0;

  for (const s of sections.slice().sort((a, b) => String(a?.id || "").localeCompare(String(b?.id || "")))) {
    const id = typeof s?.id === "string" ? s.id.trim() : "";
    const sha = typeof s?.sha256 === "string" ? s.sha256.trim() : "";
    const path = typeof s?.path === "string" ? s.path.trim() : "";
    if (!id || !sha || !path) continue;

    const json = isPlainObject(s.json) ? s.json : null;
    const excerpt = json ? JSON.stringify(sortKeysDeep(json), null, 2) + "\n" : "(no JSON content)";
    const clipped = excerpt.length > maxSectionChars ? excerpt.slice(0, maxSectionChars) + "\n[...clipped...]\n" : excerpt;

    const header = `SSOT:${id}@${sha}\npath: ${path}\n`;
    const block = `${header}\n${clipped}`;
    if (used + block.length > maxTotalChars) break;
    used += block.length;
    blocks.push(block);
  }

  return blocks.length ? blocks.join("\n---\n") : "(no SSOT sections in bundle)";
}

export function sha256HexOfFileText(text) {
  return sha256HexText(text);
}
