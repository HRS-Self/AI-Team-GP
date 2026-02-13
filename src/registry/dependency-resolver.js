import { mkdir, rename, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { loadRegistry, resolveProjectPathsByCode } from "./project-registry.js";
import { nowFsSafeUtcTimestamp } from "../utils/naming.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
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

function requireAbsOpsRoot(opsRootAbs) {
  const raw = normStr(opsRootAbs);
  if (!raw) throw new Error("Missing opsRootAbs.");
  if (!isAbsolute(raw)) throw new Error("opsRootAbs must be absolute.");
  return resolve(raw);
}

export async function resolveProjectPaths({ toolRepoRoot = null, project_code, onMissing = null, opsRootAbs = null } = {}) {
  const code = normStr(project_code);
  if (!code) throw new Error("resolveProjectPaths: project_code is required.");
  const regRes = await loadRegistry({ toolRepoRoot, createIfMissing: true });
  const resolved = resolveProjectPathsByCode(regRes.registry, code);
  if (resolved) return { ok: true, project_code: code, paths: resolved };

  let decisionPath = null;
  if (onMissing === "decision_packet" && opsRootAbs) {
    const ops = requireAbsOpsRoot(opsRootAbs);
    const dir = join(ops, "ai", "lane_a", "decisions_needed");
    const ts = nowFsSafeUtcTimestamp();
    decisionPath = join(dir, `DEPENDENCY_MISSING-${ts}__${code}.json`);
    const pkt = {
      version: 1,
      type: "dependency_missing",
      project_code: code,
      message: `Dependency project not found or not active in AI-Team registry: ${code}`,
      created_at: new Date().toISOString(),
      hint: "Register the project via --initial-project or restore it in REGISTRY.json.",
    };
    await writeTextAtomic(decisionPath, JSON.stringify(pkt, null, 2) + "\n");
  }

  return { ok: false, project_code: code, missing: true, decision_path: decisionPath };
}

