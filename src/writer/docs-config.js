import { readFile } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

export async function readDocsConfigFile(projectRoot) {
  const abs = resolve(String(projectRoot || ""), "config", "DOCS.json");
  try {
    const text = await readFile(abs, "utf8");
    return { ok: true, path: abs, text: String(text || "") };
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return { ok: false, missing: true, path: abs, message: "Missing config/DOCS.json. Run --initial-project to create it." };
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, missing: false, path: abs, message: `Failed to read config/DOCS.json (${msg}).` };
  }
}

export function parseDocsConfig({ text }) {
  let parsed;
  try {
    parsed = JSON.parse(String(text || ""));
  } catch {
    return { ok: false, message: "Invalid config/DOCS.json: must be valid JSON." };
  }
  if (!isPlainObject(parsed) || parsed.version !== 1) return { ok: false, message: "Invalid config/DOCS.json: expected {version:1, ...}." };

  const project_key = normStr(parsed.project_key);
  const docs_repo_path = normStr(parsed.docs_repo_path);
  const knowledge_repo_path = normStr(parsed.knowledge_repo_path);
  const output_format = normStr(parsed.output_format) || "markdown";
  const parts_word_target = Number.isFinite(Number(parsed.parts_word_target)) ? Number(parsed.parts_word_target) : 1800;
  const max_docs_per_run = Number.isFinite(Number(parsed.max_docs_per_run)) ? Number(parsed.max_docs_per_run) : 3;

  const commitIn = isPlainObject(parsed.commit) ? parsed.commit : {};
  const commit = {
    enabled: commitIn.enabled !== false,
    branch: normStr(commitIn.branch) || "main",
    allow_dirty: commitIn.allow_dirty === true,
  };

  const errors = [];
  if (!project_key) errors.push("project_key is required.");
  if (!docs_repo_path) errors.push("docs_repo_path is required.");
  if (!knowledge_repo_path) errors.push("knowledge_repo_path is required.");
  if (docs_repo_path && !isAbsolute(docs_repo_path)) errors.push("docs_repo_path must be an absolute path.");
  if (knowledge_repo_path && !isAbsolute(knowledge_repo_path)) errors.push("knowledge_repo_path must be an absolute path.");
  if (!["markdown"].includes(output_format)) errors.push("output_format must be \"markdown\".");
  if (!Number.isFinite(parts_word_target) || parts_word_target <= 0) errors.push("parts_word_target must be a positive number.");
  if (!Number.isFinite(max_docs_per_run) || max_docs_per_run <= 0) errors.push("max_docs_per_run must be a positive number.");
  if (commit.enabled && !commit.branch) errors.push("commit.branch is required when commit.enabled is true.");

  if (errors.length) return { ok: false, message: `Invalid config/DOCS.json: ${errors.join(" ")}` };

  return {
    ok: true,
    normalized: {
      version: 1,
      project_key,
      docs_repo_path: resolve(docs_repo_path),
      knowledge_repo_path: resolve(knowledge_repo_path),
      output_format,
      parts_word_target,
      max_docs_per_run,
      commit,
    },
  };
}

