import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { readProjectConfig } from "../../project/project-config.js";
import { nowFsSafeUtcTimestamp } from "../../utils/naming.js";
import { validateScope } from "./knowledge-utils.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

export function stableKickoffQuestionId({ scope, phase, question }) {
  const base = [String(scope || ""), String(phase || ""), String(question || "")].join("\n");
  const h = createHash("sha256").update(base, "utf8").digest("hex").slice(0, 12);
  return `KQ_${h}`;
}

export function scopeTagForFilename(scope) {
  const s = normStr(scope);
  if (s === "system") return "system";
  const parsed = validateScope(s);
  if (parsed.kind === "repo") return `repo_${parsed.repo_id}`;
  return "system";
}

export async function readJsonAbs(pathAbs) {
  const abs = resolve(String(pathAbs || ""));
  const text = await readFile(abs, "utf8");
  return JSON.parse(String(text || ""));
}

let atomicCounter = 0;
export async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

export async function appendTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  let prev = "";
  try {
    prev = await readFile(abs, "utf8");
  } catch (err) {
    if (!(err && (err.code === "ENOENT" || err.code === "ENOTDIR"))) throw err;
    prev = "";
  }
  await writeTextAtomic(abs, String(prev || "") + String(text || ""));
}

export async function appendJsonlCapped(absPath, obj, { maxLines = 500 } = {}) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  const max = Number.isFinite(maxLines) ? Math.max(0, Math.floor(maxLines)) : 500;
  let lines = [];
  try {
    const prev = await readFile(abs, "utf8");
    lines = String(prev || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
  } catch (err) {
    if (!(err && (err.code === "ENOENT" || err.code === "ENOTDIR"))) throw err;
    lines = [];
  }
  const nextLine = JSON.stringify(obj);
  const keep = max > 0 ? Math.max(0, max - 1) : 0;
  const outLines = (keep ? lines.slice(-keep) : []).concat(nextLine);
  await writeTextAtomic(abs, outLines.join("\n") + "\n");
}

export function requireAbsoluteProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot (absolute path).");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path.");
  return resolve(raw);
}

export async function resolveKnowledgeProjectAbs({ projectRootAbs }) {
  const cfgRes = await readProjectConfig({ projectRoot: projectRootAbs });
  if (!cfgRes.ok) throw new Error(cfgRes.message);
  const cfg = cfgRes.config;
  const repoDir = normStr(cfg?.knowledge_repo_dir);
  if (!repoDir) throw new Error("PROJECT.json missing knowledge_repo_dir.");
  if (!isAbsolute(repoDir)) throw new Error("knowledge_repo_dir must be an absolute path.");
  const knowledgeProjectAbs = resolve(repoDir);
  return { knowledgeProjectAbs, cfg };
}

export function kickoffSessionStem({ scope }) {
  const ts = nowFsSafeUtcTimestamp();
  const tag = scopeTagForFilename(scope);
  return { ts, stem: `KICKOFF-${ts}__${tag}` };
}

function assertSummaryShape(summary, path) {
  if (!isPlainObject(summary)) throw new Error(`${path} must be an object.`);
  const allowed = new Set(["scope", "created_at", "latest_md", "latest_json", "sufficiency", "open_questions_count", "blocking_questions_count"]);
  for (const k of Object.keys(summary)) if (!allowed.has(k)) throw new Error(`${path} unknown field '${k}'.`);
  const scope = normStr(summary.scope);
  if (!(scope === "system" || scope.startsWith("repo:"))) throw new Error(`${path}.scope must be system or repo:<id>.`);
  const md = normStr(summary.latest_md);
  const js = normStr(summary.latest_json);
  if (!md || !js) throw new Error(`${path} must include latest_md and latest_json.`);
  const safeName = (s) => !(s.startsWith("/") || s.includes("..") || s.includes("\\") || s.includes("/"));
  if (!safeName(md) || !safeName(js)) throw new Error(`${path} latest_md/latest_json must be safe filenames (no path traversal).`);
  if (!isPlainObject(summary.sufficiency)) throw new Error(`${path}.sufficiency must be an object.`);
  const status = normStr(summary.sufficiency.status);
  if (!new Set(["insufficient", "partial", "sufficient"]).has(status)) throw new Error(`${path}.sufficiency.status invalid.`);
  return summary;
}

export function assertKickoffLatestShape(latest) {
  if (!isPlainObject(latest)) throw new Error("sessions/kickoff/LATEST.json must be a JSON object.");
  const allowed = new Set(["version", "updated_at", "latest_by_scope"]);
  for (const k of Object.keys(latest)) if (!allowed.has(k)) throw new Error(`sessions/kickoff/LATEST.json unknown field '${k}'.`);
  if (latest.version !== 2) throw new Error("sessions/kickoff/LATEST.json.version must be 2.");
  if (!normStr(latest.updated_at)) throw new Error("sessions/kickoff/LATEST.json.updated_at is required.");
  if (!isPlainObject(latest.latest_by_scope)) throw new Error("sessions/kickoff/LATEST.json.latest_by_scope must be an object.");
  for (const scope of Object.keys(latest.latest_by_scope).sort((a, b) => a.localeCompare(b))) {
    assertSummaryShape(latest.latest_by_scope[scope], `sessions/kickoff/LATEST.json.latest_by_scope.${scope}`);
    if (normStr(latest.latest_by_scope[scope].scope) !== scope) throw new Error(`sessions/kickoff/LATEST.json.latest_by_scope.${scope}.scope must match key.`);
  }
  return latest;
}
