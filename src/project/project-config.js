import { dirname, join, resolve } from "node:path";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";

import { validateProjectConfig } from "../validators/project-config-validator.js";

export function projectConfigPath(projectRoot) {
  return join(resolve(String(projectRoot || "")), "config", "PROJECT.json");
}

export async function readProjectConfig({ projectRoot }) {
  const path = projectConfigPath(projectRoot);
  if (!existsSync(path)) return { ok: false, exists: false, path, message: `Missing ${path}. This file is mandatory.` };
  try {
    const text = await readFile(path, "utf8");
    const parsed = JSON.parse(String(text || ""));
    const v = validateProjectConfig(parsed);
    if (!v.ok) return { ok: false, exists: true, path, message: `Invalid ${path}: ${v.errors.join(" | ")}` };
    return { ok: true, exists: true, path, config: v.normalized };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, exists: true, path, message: `Failed to read PROJECT.json (${msg}).` };
  }
}

export async function writeProjectConfig({ projectRoot, config, dryRun = false }) {
  const path = projectConfigPath(projectRoot);
  const v = validateProjectConfig(config);
  if (!v.ok) return { ok: false, message: `Refuse to write invalid ${path}: ${v.errors.join(" | ")}`, path };
  if (dryRun) return { ok: true, dry_run: true, path };
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(v.normalized, null, 2) + "\n", "utf8");
  return { ok: true, dry_run: false, path };
}
