import { existsSync, readFileSync } from "node:fs";
import { mkdir, rename, writeFile, readFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, ensureKnowledgeDirs, loadProjectPaths } from "../../paths/project-paths.js";
import { validateKnowledgeVersion } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
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

async function writeJsonAtomic(absPath, obj) {
  await writeTextAtomic(absPath, JSON.stringify(obj, null, 2) + "\n");
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

function isVersionLike(v) {
  return /^v\d+(\.\d+)*$/.test(String(v || "").trim());
}

function parseVersionSegments(v) {
  const s = String(v || "").trim();
  if (!isVersionLike(s)) throw new Error(`Invalid version '${s}'. Expected v<major>[.<minor>[.<patch>...]]`);
  return s
    .slice(1)
    .split(".")
    .map((x) => Number.parseInt(x, 10));
}

function formatVersion(segments) {
  return `v${segments.join(".")}`;
}

// Canonical scheme:
// - bump_patch: increment last segment; if only major exists (v1) => v1.0.1
// - bump_minor: increment minor; if only major exists (v1) => v1.1; drop deeper segments
// - bump_major: increment major; reset to v<major+1>
export function bumpVersion(current, kind) {
  const seg = parseVersionSegments(current);
  if (kind === "bump_major") return `v${seg[0] + 1}`;
  if (kind === "bump_minor") {
    if (seg.length === 1) return `v${seg[0]}.1`;
    const next = [seg[0], (seg[1] || 0) + 1];
    return formatVersion(next);
  }
  if (kind === "bump_patch") {
    if (seg.length === 1) return `v${seg[0]}.0.1`;
    const next = seg.slice();
    next[next.length - 1] = next[next.length - 1] + 1;
    return formatVersion(next);
  }
  if (kind === "no_bump") return current;
  throw new Error(`Unknown bump kind: ${kind}`);
}

export async function readKnowledgeVersionOrDefault({ projectRoot } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const abs = join(paths.laneA.rootAbs, "knowledge_version.json");
  if (!existsSync(abs)) {
    const obj = { version: 1, current: "v0", history: [] };
    validateKnowledgeVersion(obj);
    return { ok: true, exists: false, version: obj, path: abs, paths };
  }
  const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
  validateKnowledgeVersion(j);
  return { ok: true, exists: true, version: j, path: abs, paths };
}

function renderVersionMd({ version }) {
  const v = version;
  const lines = [];
  lines.push("KNOWLEDGE VERSION");
  lines.push("");
  lines.push(`current: ${v.current}`);
  lines.push("");
  lines.push("HISTORY");
  lines.push("");
  if (!v.history.length) lines.push("- (none)");
  for (const h of v.history) {
    const notes = h.notes ? ` notes=${h.notes}` : "";
    lines.push(`- ${h.at} ${h.v} reason=${h.reason} scope=${h.scope}${notes}`);
  }
  lines.push("");
  return lines.join("\n") + "\n";
}

export async function writeCompactKnowledgeVersionToRepo({ paths, version, dryRun }) {
  const knowledgeRootAbs = paths.knowledge.rootAbs;
  const jsonAbs = join(knowledgeRootAbs, "VERSION.json");
  const mdAbs = join(knowledgeRootAbs, "VERSION.md");
  const compact = { version: 1, current: version.current, history: version.history.slice(-50) };
  validateKnowledgeVersion(compact);
  if (!dryRun) {
    await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });
    await writeTextAtomic(jsonAbs, JSON.stringify(compact, null, 2) + "\n");
    await writeTextAtomic(mdAbs, renderVersionMd({ version: compact }));
  }
  return { ok: true, wrote: !dryRun, json: jsonAbs, md: mdAbs };
}

export async function bumpKnowledgeVersion({ projectRoot, kind, reason, scope, notes = "", dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const { version: prev, path: versionAbs, paths } = await readKnowledgeVersionOrDefault({ projectRoot: projectRootAbs });
  const k = normStr(kind);
  const r = normStr(reason) || "update_meeting";
  const s = normStr(scope) || "system";
  const at = nowISO();

  const nextCurrent = bumpVersion(prev.current, k);
  const hist = prev.history.slice();
  hist.push({ v: nextCurrent, at, reason: r, scope: s, ...(notes ? { notes: String(notes) } : {}) });

  const next = { version: 1, current: nextCurrent, history: hist };
  validateKnowledgeVersion(next);

  if (!dryRun) {
    await writeJsonAtomic(versionAbs, next);
    await writeCompactKnowledgeVersionToRepo({ paths, version: next, dryRun: false });
  }
  return { ok: true, previous: prev.current, current: next.current, wrote: !dryRun, ops_path: versionAbs };
}

export async function setKnowledgeVersionExplicit({ projectRoot, fromVersion = null, toVersion, scope = "system", reason, notes = "", dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const toV = normStr(toVersion);
  if (!isVersionLike(toV)) throw new Error(`Invalid toVersion '${toV}'. Expected v<major>[.<minor>[.<patch>...]]`);
  const r = normStr(reason) || "update_meeting";
  const s = normStr(scope) || "system";
  const at = nowISO();

  const { version: prev, path: versionAbs, paths } = await readKnowledgeVersionOrDefault({ projectRoot: projectRootAbs });
  const hist = prev.history.slice();
  const fromV = isVersionLike(fromVersion) ? normStr(fromVersion) : normStr(prev.current) || "v0";
  const noteText = notes ? String(notes) : "";
  const enrichedNotes = fromV && fromV !== toV ? `from=${fromV} ${noteText}`.trim() : noteText;
  hist.push({ v: toV, at, reason: r, scope: s, ...(enrichedNotes ? { notes: enrichedNotes } : {}) });

  const next = { version: 1, current: toV, history: hist };
  validateKnowledgeVersion(next);

  if (!dryRun) {
    await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
    await ensureKnowledgeDirs({ projectRoot: paths.opsRootAbs });
    await writeJsonAtomic(versionAbs, next);
    await writeCompactKnowledgeVersionToRepo({ paths, version: next, dryRun: false });
  }

  return { ok: true, previous: prev.current, current: next.current, wrote: !dryRun, ops_path: versionAbs };
}
