import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, ensureLaneBDirs, loadProjectPaths } from "../paths/project-paths.js";
import { ensureDir, writeText } from "../utils/fs.js";
import { formatFsSafeUtcTimestamp, intakeId } from "../utils/id.js";
import { resolveStatePath } from "../project/state-paths.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

async function listIaFiles(iaDirAbs) {
  if (!existsSync(iaDirAbs)) return [];
  const entries = await readdir(iaDirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.startsWith("IA-") && e.name.endsWith(".json"))
    .map((e) => join(iaDirAbs, e.name))
    .sort((a, b) => a.localeCompare(b));
}

async function loadIa(absPath) {
  const j = JSON.parse(String(await readFile(absPath, "utf8") || ""));
  return j;
}

function renderIntakeFromChangeRequest({ ia, cr }) {
  const lines = [];
  lines.push("LANE A APPROVED INTAKE");
  lines.push("");
  lines.push("origin: lane_a");
  lines.push(`intake_approval_id: ${ia.id}`);
  lines.push(`knowledge_version: ${ia.knowledge_version}`);
  lines.push(`scope: ${ia.scope}`);
  if (ia && ia.sufficiency_override === true) lines.push("sufficiency_override: true");
  lines.push("");
  lines.push(`change_request_id: ${cr.id}`);
  lines.push(`type: ${cr.type}`);
  lines.push(`severity: ${cr.severity}`);
  lines.push(`title: ${cr.title}`);
  lines.push("");
  lines.push("BODY");
  lines.push("");
  lines.push(String(cr.body || "").trim());
  lines.push("");
  lines.push("INSTRUCTIONS");
  lines.push("");
  if (String(cr.scope || "").startsWith("repo:")) lines.push(`- Implement in repo \`${String(cr.scope).slice("repo:".length)}\` only.`);
  else lines.push("- Implement within system scope; triage must route to appropriate repos deterministically.");
  lines.push("- Do not expand scope beyond this intake without SSOT/decision update.");
  lines.push("");
  return lines.join("\n") + "\n";
}

async function readChangeRequestById({ changeRequestsAbs, id }) {
  const processedAbs = join(changeRequestsAbs, "processed");
  const jsonAbs = join(processedAbs, `${id}.json`);
  if (!existsSync(jsonAbs)) return null;
  const j = JSON.parse(String(readFileSync(jsonAbs, "utf8") || ""));
  return j;
}

async function writeInboxIntakeDeterministic({ intakeText, ia, itemId }) {
  const createdAtIso = typeof ia?.created_at === "string" && ia.created_at ? ia.created_at : new Date(0).toISOString();
  const ts = formatFsSafeUtcTimestamp(createdAtIso);
  const seed = `${ia.id}\n${itemId}\n${sha256Hex(intakeText)}`;
  const id = intakeId({ timestamp: ts, text: seed });
  const path = `ai/lane_b/inbox/${id}.md`;
  if (existsSync(resolveStatePath(path, { requiredRoot: true }))) return { ok: true, id, path, skipped: true };
  await ensureDir("ai/lane_b/inbox");
  await writeText(path, intakeText);
  return { ok: true, id, path, skipped: false };
}

export async function runLaneAToLaneB({ projectRoot, limit = null, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  await ensureLaneBDirs({ projectRoot: paths.opsRootAbs });

  const iaDirAbs = resolve(paths.laneA.rootAbs, "intake_approvals");
  const processedAbs = join(iaDirAbs, "processed");
  if (!dryRun) {
    await mkdir(iaDirAbs, { recursive: true });
    await mkdir(processedAbs, { recursive: true });
  }

  const files = await listIaFiles(iaDirAbs);
  const max = typeof limit === "number" && Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : files.length;
  const toProcess = files.slice(0, max);

  const created = [];
  const skipped = [];

  const changeRequestsAbs = resolve(paths.laneA.rootAbs, "change_requests");

  for (const iaAbs of toProcess) {
    // eslint-disable-next-line no-await-in-loop
    const ia = await loadIa(iaAbs);
    const items = Array.isArray(ia?.approved_items) ? ia.approved_items.map((x) => normStr(x)).filter(Boolean) : [];
    const approvals = [];
    for (const itemId of items) {
      if (!itemId.startsWith("CR-")) continue;
      // eslint-disable-next-line no-await-in-loop
      const cr = await readChangeRequestById({ changeRequestsAbs, id: itemId });
      if (!cr) continue;
      const intakeText = renderIntakeFromChangeRequest({ ia, cr });
      if (dryRun) {
        approvals.push({ item_id: itemId, ok: true, dry_run: true, inbox_path: null });
        continue;
      }
      // eslint-disable-next-line no-await-in-loop
      const res = await writeInboxIntakeDeterministic({ intakeText, ia, itemId });
      approvals.push({ item_id: itemId, ok: true, dry_run: false, inbox_path: res.path, skipped: res.skipped });
      if (res.skipped) skipped.push(res.path);
      else created.push(res.path);
    }

    if (!dryRun) {
      const target = join(processedAbs, basenameSafe(iaAbs));
      // eslint-disable-next-line no-await-in-loop
      await rename(iaAbs, target);
      // Write a small marker next to processed approvals for audit.
      const markerAbs = `${target}.bridge.json`;
      // eslint-disable-next-line no-await-in-loop
      await writeFileAtomic(markerAbs, JSON.stringify({ ok: true, processed_at: new Date().toISOString(), created: approvals }, null, 2) + "\n");
    }
  }

  return { ok: true, processed: toProcess.length, created_count: created.length, created, skipped_count: skipped.length, skipped, approvals_root: iaDirAbs };
}

function basenameSafe(absPath) {
  const base = resolve(String(absPath || "")).split("/").pop() || "IA.json";
  return base.replace(/[^A-Za-z0-9_.-]/g, "_");
}

let atomicCounter = 0;
async function writeFileAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}
