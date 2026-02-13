import { mkdir, writeFile, appendFile as appendFileNative, readFile } from "node:fs/promises";
import { resolveStatePath } from "../project/state-paths.js";

export async function ensureDir(path) {
  await mkdir(resolveStatePath(path, { requiredRoot: true }), { recursive: true });
}

export async function writeText(path, text) {
  const p = resolveStatePath(path, { requiredRoot: true });
  await ensureDir(p.split("/").slice(0, -1).join("/") || ".");
  await writeFile(p, text, "utf8");
}

export async function appendFile(path, text) {
  const p = resolveStatePath(path, { requiredRoot: true });
  await ensureDir(p.split("/").slice(0, -1).join("/") || ".");
  await appendFileNative(p, text, "utf8");
}

export async function readTextIfExists(path) {
  const p = resolveStatePath(path, { requiredRoot: true });
  try {
    return await readFile(p, "utf8");
  } catch (err) {
    if (err && (err.code === "ENOENT" || err.code === "ENOTDIR")) return null;
    throw err;
  }
}
