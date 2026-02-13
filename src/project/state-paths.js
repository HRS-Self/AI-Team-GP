import { isAbsolute, resolve, join } from "node:path";

import { resolveOpsRootAbs } from "../paths/project-paths.js";

export function getAIProjectRoot({ required = true } = {}) {
  try {
    return resolveOpsRootAbs({ projectRoot: null, required });
  } catch (err) {
    if (!required) return null;
    throw err;
  }
}

export function resolveProjectPath(path, { requiredRoot = true } = {}) {
  const p = String(path || "");
  if (!p) return p;
  if (isAbsolute(p)) return p;
  const root = getAIProjectRoot({ required: requiredRoot });
  if (!root) return p;
  return join(root, p);
}

export function resolveStatePath(path, { requiredRoot = false } = {}) {
  const p = String(path || "");
  if (!p) return p;
  if (isAbsolute(p)) return p;

  // Only remap known state paths to the project instance root.
  // Tooling repo files (e.g. README.md, src/...) must remain relative to CWD.
  const isState =
    p === "config" ||
    p.startsWith("config/") ||
    p === "ai" ||
    p.startsWith("ai/") ||
    p === "AI" ||
    p.startsWith("AI/"); // tolerate historical casing

  if (!isState) return p;

  return resolveProjectPath(p, { requiredRoot });
}
