import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { ensureLaneADirs, loadProjectPaths } from "../../paths/project-paths.js";
import { validateChangeRequest } from "../../contracts/validators/index.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function nowISO() {
  return new Date().toISOString();
}

function fsSafeUtcTimestamp14() {
  const d = new Date();
  const y = String(d.getUTCFullYear()).padStart(4, "0");
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${y}${m}${day}_${hh}${mm}${ss}`;
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function parseScope(scope) {
  const s = normStr(scope);
  if (s === "system") return { scope: "system", scopeSlug: "system" };
  if (s.startsWith("repo:")) {
    const id = normStr(s.slice("repo:".length));
    if (!id) throw new Error("Invalid --scope. Expected repo:<id>.");
    const slug = `repo-${id.replace(/[^A-Za-z0-9_.-]/g, "_")}`;
    return { scope: `repo:${id}`, scopeSlug: slug };
  }
  throw new Error("Invalid --scope. Expected system or repo:<id>.");
}

function normalizeType(type) {
  const t = normStr(type).toLowerCase();
  if (t === "bug" || t === "feature" || t === "question") return t;
  throw new Error("Invalid --type. Expected bug|feature|question.");
}

function defaultSeverityForType(type) {
  if (type === "question") return "low";
  if (type === "bug") return "medium";
  return "medium";
}

function parseTitleAndBodyFromText(text) {
  const lines = String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n");
  const first = lines.find((l) => l.trim()) || "";
  const title = first.trim().slice(0, 160) || "Change request";
  const body = String(text || "").trim();
  return { title, body };
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

function summarizeCounts(items) {
  const byStatus = {};
  const byType = {};
  for (const it of items) {
    const s = normStr(it.status) || "unknown";
    const t = normStr(it.type) || "unknown";
    byStatus[s] = (byStatus[s] || 0) + 1;
    byType[t] = (byType[t] || 0) + 1;
  }
  return { byStatus, byType, total: items.length };
}

async function listChangeRequestJsonFiles(dirAbs) {
  if (!existsSync(dirAbs)) return [];
  const entries = await readdir(dirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile() && e.name.startsWith("CR-") && e.name.endsWith(".json"))
    .map((e) => join(dirAbs, e.name))
    .sort((a, b) => a.localeCompare(b));
}

export async function loadAllChangeRequests({ changeRequestsAbs }) {
  const processedAbs = join(changeRequestsAbs, "processed");
  const files = (await listChangeRequestJsonFiles(changeRequestsAbs)).concat(await listChangeRequestJsonFiles(processedAbs)).sort((a, b) => a.localeCompare(b));
  const items = [];
  for (const f of files) {
    // eslint-disable-next-line no-await-in-loop
    const j = JSON.parse(String(await readFile(f, "utf8") || ""));
    validateChangeRequest(j);
    items.push({ ...j, _path: f });
  }
  // Deterministic ordering: created_at then id.
  items.sort((a, b) => {
    const am = Date.parse(a.created_at);
    const bm = Date.parse(b.created_at);
    if (Number.isFinite(am) && Number.isFinite(bm) && am !== bm) return am - bm;
    return String(a.id).localeCompare(String(b.id));
  });
  return items;
}

async function writeStatusJson({ changeRequestsAbs, items }) {
  const statusAbs = join(changeRequestsAbs, "status.json");
  const open = items.filter((it) => it.status === "open");
  const inMeeting = items.filter((it) => it.status === "in_meeting");
  const processed = items.filter((it) => it.status === "processed");
  const rejected = items.filter((it) => it.status === "rejected");
  const out = {
    version: 1,
    captured_at: nowISO(),
    counts: summarizeCounts(items),
    open: { total: open.length, ids: open.map((x) => x.id).slice(0, 200) },
    in_meeting: { total: inMeeting.length, ids: inMeeting.map((x) => x.id).slice(0, 200) },
    processed: { total: processed.length },
    rejected: { total: rejected.length },
  };
  await writeJsonAtomic(statusAbs, out);
  return { ok: true, status_path: statusAbs, status: out };
}

function requireAbsProjectRoot(projectRoot) {
  const raw = normStr(projectRoot);
  if (!raw) throw new Error("Missing --projectRoot.");
  if (!isAbsolute(raw)) throw new Error("--projectRoot must be an absolute path (OPS_ROOT).");
  return resolve(raw);
}

export async function runKnowledgeChangeRequest({ projectRoot, type, scope, inputPath, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const t = normalizeType(type);
  const parsedScope = parseScope(scope);

  const inputAbs = resolve(String(inputPath || ""));
  if (!existsSync(inputAbs)) return { ok: false, message: `Missing --input file (${inputAbs}).` };

  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });

  const changeRequestsAbs = resolve(paths.laneA.rootAbs, "change_requests");
  const processedAbs = join(changeRequestsAbs, "processed");
  if (!dryRun) {
    await mkdir(changeRequestsAbs, { recursive: true });
    await mkdir(processedAbs, { recursive: true });
  }

  const raw = String(await readFile(inputAbs, "utf8") || "");
  const trimmed = raw.trim();
  if (!trimmed) return { ok: false, message: "Input is empty." };

  let jsonInput = null;
  try {
    jsonInput = JSON.parse(trimmed);
  } catch {
    jsonInput = null;
  }

  const created_at = nowISO();
  const ts = fsSafeUtcTimestamp14();
  const hash8 = sha256Hex(`${t}\n${parsedScope.scope}\n${trimmed}`).slice(0, 8);
  const id = `CR-${ts}__${t}__${parsedScope.scopeSlug}__${hash8}`;

  let title;
  let body;
  let severity;
  if (jsonInput && typeof jsonInput === "object" && !Array.isArray(jsonInput)) {
    // Accept JSON input only if it already matches contract fields (no inference).
    title = normStr(jsonInput.title);
    body = typeof jsonInput.body === "string" ? jsonInput.body : "";
    severity = normStr(jsonInput.severity) || defaultSeverityForType(t);
    if (!title || !body.trim()) return { ok: false, message: "JSON input must include non-empty title and body." };
  } else {
    const tb = parseTitleAndBodyFromText(trimmed);
    title = tb.title;
    body = tb.body;
    severity = defaultSeverityForType(t);
  }

  const cr = {
    version: 1,
    id,
    type: t,
    scope: parsedScope.scope,
    title,
    body,
    severity: ["low", "medium", "high"].includes(severity) ? severity : defaultSeverityForType(t),
    created_at,
    status: "open",
    linked_meeting_id: null,
  };
  validateChangeRequest(cr);

  const mdName = `${id}.md`;
  const jsonName = `${id}.json`;
  const mdAbs = join(changeRequestsAbs, mdName);
  const jsonAbs = join(changeRequestsAbs, jsonName);

  if (!dryRun) {
    const md = [`# ${cr.title}`, "", `type: ${cr.type}`, `scope: ${cr.scope}`, `severity: ${cr.severity}`, `created_at: ${cr.created_at}`, "", cr.body.trim(), ""].join("\n");
    await writeTextAtomic(mdAbs, md);
    await writeJsonAtomic(jsonAbs, cr);
    const items = await loadAllChangeRequests({ changeRequestsAbs });
    const statusRes = await writeStatusJson({ changeRequestsAbs, items });
    return { ok: true, id: cr.id, wrote: true, md: mdAbs, json: jsonAbs, status: statusRes.status };
  }

  return { ok: true, id: cr.id, wrote: false, md: mdAbs, json: jsonAbs };
}

export async function runKnowledgeChangeStatus({ projectRoot, json = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });

  const changeRequestsAbs = resolve(paths.laneA.rootAbs, "change_requests");
  const items = await loadAllChangeRequests({ changeRequestsAbs });
  const statusAbs = join(changeRequestsAbs, "status.json");
  const status = existsSync(statusAbs) ? JSON.parse(String(await readFile(statusAbs, "utf8") || "")) : null;
  const open = items.filter((it) => it.status === "open");
  const inMeeting = items.filter((it) => it.status === "in_meeting");
  return {
    ok: true,
    project_root: paths.opsRootAbs,
    change_requests_root: changeRequestsAbs,
    counts: summarizeCounts(items),
    open: open.map((x) => ({ id: x.id, type: x.type, scope: x.scope, title: x.title, severity: x.severity, created_at: x.created_at })),
    in_meeting: inMeeting.map((x) => ({ id: x.id, linked_meeting_id: x.linked_meeting_id, scope: x.scope, title: x.title, created_at: x.created_at })),
    status_path: statusAbs,
    status_json: json ? status : null,
  };
}

export async function bindOldestOpenChangeRequestsToMeeting({ projectRoot, meetingId, scope, maxBind = 25, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const parsedScope = parseScope(scope);

  const changeRequestsAbs = resolve(paths.laneA.rootAbs, "change_requests");
  const processedAbs = join(changeRequestsAbs, "processed");
  if (!dryRun) {
    await mkdir(changeRequestsAbs, { recursive: true });
    await mkdir(processedAbs, { recursive: true });
  }
  const items = await loadAllChangeRequests({ changeRequestsAbs });
  const candidates = items.filter((it) => it.status === "open" && it.scope === parsedScope.scope);
  const bind = candidates.slice(0, Math.max(0, Math.min(200, Math.floor(Number(maxBind) || 0) || 0)));

  const boundIds = [];
  for (const it of bind) {
    const next = { ...it, status: "in_meeting", linked_meeting_id: meetingId };
    validateChangeRequest(next);
    const jsonAbs = it._path;
    const mdAbs = jsonAbs.replace(/\.json$/, ".md");
    const jsonTargetAbs = join(processedAbs, `${it.id}.json`);
    const mdTargetAbs = join(processedAbs, `${it.id}.md`);
    if (!dryRun) {
      // Move raw files under processed/ once bound to a meeting (contract).
      if (existsSync(jsonAbs)) await rename(jsonAbs, jsonTargetAbs);
      if (existsSync(mdAbs)) await rename(mdAbs, mdTargetAbs);
      await writeJsonAtomic(jsonTargetAbs, next);
    }
    boundIds.push(it.id);
  }

  if (!dryRun) {
    const all = await loadAllChangeRequests({ changeRequestsAbs });
    await writeStatusJson({ changeRequestsAbs, items: all });
  }

  return { ok: true, bound: boundIds };
}

export async function markChangeRequestsProcessed({ projectRoot, ids, meetingId = null, dryRun = false } = {}) {
  const projectRootAbs = requireAbsProjectRoot(projectRoot);
  const paths = await loadProjectPaths({ projectRoot: projectRootAbs });
  await ensureLaneADirs({ projectRoot: paths.opsRootAbs });
  const changeRequestsAbs = resolve(paths.laneA.rootAbs, "change_requests");
  const processedAbs = join(changeRequestsAbs, "processed");

  const want = new Set((Array.isArray(ids) ? ids : []).map((x) => normStr(x)).filter(Boolean));
  if (!want.size) return { ok: true, updated: [] };

  const files = await listChangeRequestJsonFiles(processedAbs);
  const updated = [];
  for (const f of files) {
    // eslint-disable-next-line no-await-in-loop
    const j = JSON.parse(String(await readFile(f, "utf8") || ""));
    validateChangeRequest(j);
    if (!want.has(j.id)) continue;
    const next = { ...j, status: "processed", linked_meeting_id: meetingId || j.linked_meeting_id };
    validateChangeRequest(next);
    if (!dryRun) await writeJsonAtomic(f, next);
    updated.push(next.id);
  }
  if (!dryRun) {
    const all = await loadAllChangeRequests({ changeRequestsAbs });
    await writeStatusJson({ changeRequestsAbs, items: all });
  }
  return { ok: true, updated };
}
