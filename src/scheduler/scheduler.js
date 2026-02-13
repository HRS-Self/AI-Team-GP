import { readdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

import { readTextIfExists, writeText, appendFile } from "../utils/fs.js";
import { resolveStatePath } from "../project/state-paths.js";
import { readWorkStatusSnapshot, updateWorkStatus } from "../utils/status-writer.js";

function nowISO() {
  return new Date().toISOString();
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function stableSort(arr, keyFn) {
  return (arr || []).slice().sort((a, b) => String(keyFn(a)).localeCompare(String(keyFn(b))));
}

function parseWorkCreatedAt({ workId, routing, statusSnapshot }) {
  const ts = typeof routing?.timestamp === "string" ? routing.timestamp.trim() : "";
  if (ts) return ts;
  const fromStatus = typeof statusSnapshot?.last_updated === "string" ? statusSnapshot.last_updated.trim() : "";
  if (fromStatus) return fromStatus;
  // Best-effort: use the encoded timestamp portion from workId if it parses.
  const m = String(workId || "").match(/^W-([^/]+)-[0-9a-f]+$/i);
  if (m && m[1] && !Number.isNaN(Date.parse(m[1]))) return m[1];
  return nowISO();
}

function defaultMeta({ workId, created_at }) {
  return {
    version: 1,
    work_id: workId,
    created_at,
    priority: 50,
    depends_on: [],
    blocks: [],
    labels: [],
    repo_scopes: [],
    target_branch: null,
  };
}

async function readJsonIfExists(path) {
  const t = await readTextIfExists(path);
  if (!t) return { ok: false, missing: true, json: null };
  try {
    return { ok: true, missing: false, json: JSON.parse(t) };
  } catch {
    return { ok: false, missing: false, json: null };
  }
}

async function ensureMeta({ workId, routing, statusSnapshot }) {
  const path = `ai/lane_b/work/${workId}/META.json`;
  const existing = await readJsonIfExists(path);
  if (existing.ok && existing.json?.version === 1) {
    const meta = existing.json;
    let changed = false;

    // Add missing top-level keys (additive only).
    if (!("created_at" in meta) || !String(meta.created_at || "").trim()) {
      meta.created_at = parseWorkCreatedAt({ workId, routing, statusSnapshot });
      changed = true;
    }
    if (!("priority" in meta) || !Number.isFinite(Number(meta.priority))) {
      meta.priority = 50;
      changed = true;
    }
    if (!("depends_on" in meta) || !Array.isArray(meta.depends_on)) {
      meta.depends_on = [];
      changed = true;
    }
    if (!("blocks" in meta) || !Array.isArray(meta.blocks)) {
      meta.blocks = [];
      changed = true;
    }
    if (!("labels" in meta) || !Array.isArray(meta.labels)) {
      meta.labels = [];
      changed = true;
    }
    if (!("repo_scopes" in meta) || !Array.isArray(meta.repo_scopes)) {
      meta.repo_scopes = [];
      changed = true;
    }
    if (!("target_branch" in meta)) {
      meta.target_branch = null;
      changed = true;
    }

    // Backfill routing-derived values without overwriting user-defined values.
    const routingMode = typeof routing?.routing_mode === "string" ? routing.routing_mode : null;
    const selectedRepos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : [];
    if (routingMode === "repo_explicit" && !meta.repo_scopes.length && selectedRepos.length) {
      meta.repo_scopes = selectedRepos;
      changed = true;
    }
    const target = isPlainObject(routing?.target_branch) ? routing.target_branch : null;
    const targetName = typeof target?.name === "string" ? target.name.trim() : "";
    const targetSource = typeof target?.source === "string" ? target.source.trim() : "";
    if (!meta.target_branch && targetSource === "explicit" && targetName) {
      meta.target_branch = targetName;
      changed = true;
    }

    if (changed) await writeText(path, JSON.stringify(meta, null, 2) + "\n");
    return { ok: true, path, meta, created: false, updated: changed };
  }

  const created_at = parseWorkCreatedAt({ workId, routing, statusSnapshot });
  const meta = defaultMeta({ workId, created_at });
  await writeText(path, JSON.stringify(meta, null, 2) + "\n");
  return { ok: true, path, meta, created: true };
}

async function readRoutingIfExists(workId) {
  const p = `ai/lane_b/work/${workId}/ROUTING.json`;
  const t = await readTextIfExists(p);
  if (!t) return { ok: false, missing: true, routing: null };
  try {
    return { ok: true, missing: false, routing: JSON.parse(t) };
  } catch {
    return { ok: false, missing: false, routing: null };
  }
}

async function listWorkIds() {
  const dir = resolveStatePath("ai/lane_b/work");
  try {
    const entries = await readdir(dir, { withFileTypes: true });
    return entries
      .filter((e) => e.isDirectory() && e.name.startsWith("W-"))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

function touchedReposFromRouting(routing) {
  const repos = Array.isArray(routing?.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : [];
  return repos;
}

async function touchedReposFromBundle(workId) {
  const p = `ai/lane_b/work/${workId}/BUNDLE.json`;
  const t = await readTextIfExists(p);
  if (!t) return [];
  try {
    const b = JSON.parse(t);
    const repos = Array.isArray(b?.repos) ? b.repos : [];
    return repos.map((r) => String(r?.repo_id || "").trim()).filter(Boolean).sort((a, b2) => a.localeCompare(b2));
  } catch {
    return [];
  }
}

function touchedReposFromMeta(meta) {
  const repos = Array.isArray(meta?.repo_scopes) ? meta.repo_scopes.slice().filter(Boolean) : [];
  return repos;
}

async function touchedReposForWork({ workId, routing, meta }) {
  const fromRouting = touchedReposFromRouting(routing);
  if (fromRouting.length) return fromRouting;
  const fromMeta = touchedReposFromMeta(meta);
  if (fromMeta.length) return fromMeta;
  const fromBundle = await touchedReposFromBundle(workId);
  return fromBundle;
}

function isTerminalStage(stage) {
  const s = String(stage || "").trim();
  return s === "COMPLETED" || s === "FAILED" || s === "FAILED_FINAL" || s === "DONE" || s === "MERGED" || s === "CANCELLED";
}

function isWaitingForApproval(stage) {
  const s = String(stage || "").trim();
  return (
    s === "PLAN_APPROVAL_REQUESTED" ||
    s === "PLAN_APPROVAL_REQUIRED" ||
    s === "PLAN_APPROVED" ||
    s === "APPROVAL_REQUESTED" ||
    s === "APPROVAL_REQUIRED" ||
    s === "APPROVED" ||
    s === "REJECTED"
  );
}

function isBeyondWatchdog(stage) {
  const s = String(stage || "").trim();
  // Watchdog only advances work up to approval request; anything beyond should not be re-scheduled.
  return s === "APPLYING" || s === "APPLIED";
}

async function worktreesPresent(workId) {
  const dir = resolveStatePath(`ai/lane_b/work/${workId}/worktrees`);
  try {
    const entries = await readdir(dir, { withFileTypes: true });
    return entries.some((e) => e.name && e.name !== "." && e.name !== "..");
  } catch {
    return false;
  }
}

function schedulerConfigFromPolicies(policies) {
  const sched = isPlainObject(policies?.scheduler) ? policies.scheduler : {};
  const maxItems = Number.isFinite(Number(sched.max_items_per_run)) ? Number(sched.max_items_per_run) : 3;
  const maxPerRepo = Number.isFinite(Number(sched.max_active_per_repo)) ? Number(sched.max_active_per_repo) : 1;
  return {
    max_items_per_run: Math.max(1, Math.floor(maxItems)),
    max_active_per_repo: Math.max(1, Math.floor(maxPerRepo)),
  };
}

async function readPolicies() {
  const t = await readTextIfExists("config/POLICIES.json");
  if (!t) return { ok: false, policies: null };
  try {
    return { ok: true, policies: JSON.parse(t) };
  } catch {
    return { ok: false, policies: null };
  }
}

export async function computeSchedule({ limit = null, orderBy = "created_at", workIdAllowlist = null, dryRun = false } = {}) {
  const policiesRes = await readPolicies();
  const policies = policiesRes.ok ? policiesRes.policies : {};
  const cfg = schedulerConfigFromPolicies(policies);
  const maxItems = Number.isFinite(limit) && limit > 0 ? Math.min(cfg.max_items_per_run, Math.floor(limit)) : cfg.max_items_per_run;

  const allWorkIds = await listWorkIds();
  const allowSet = Array.isArray(workIdAllowlist) && workIdAllowlist.length ? new Set(workIdAllowlist.map((w) => String(w))) : null;
  const workIds = allowSet ? allWorkIds.filter((w) => allowSet.has(String(w))) : allWorkIds;
  const queueOrder = allowSet ? workIdAllowlist.map((w) => String(w)) : null;
  const queueIndex = queueOrder ? new Map(queueOrder.map((w, idx) => [w, idx])) : null;

  const statusById = new Map();
  const metaById = new Map();
  const routingById = new Map();
  const skipped = [];

  if (allowSet) {
    const missing = workIdAllowlist.map((w) => String(w)).filter((w) => !allWorkIds.includes(w));
    for (const missingId of missing) {
      skipped.push({ work_id: missingId, reason: "missing_work_dir", repos: [] });
    }
  }

  // First pass: load routing/status/meta and ensure meta exists.
  for (const workId of workIds) {
    let statusRes = await readWorkStatusSnapshot(workId);
    if (!statusRes.ok) {
      await updateWorkStatus({ workId, stage: "INTAKE_RECEIVED", blocked: false, note: "scheduler bootstrap: missing STATUS.md" });
      statusRes = await readWorkStatusSnapshot(workId);
    }
    const snap = statusRes.ok ? statusRes.snapshot : null;
    statusById.set(workId, snap);

    const routingRes = await readRoutingIfExists(workId);
    routingById.set(workId, routingRes.ok ? routingRes.routing : null);

    const ensured = await ensureMeta({ workId, routing: routingRes.ok ? routingRes.routing : null, statusSnapshot: snap });
    metaById.set(workId, ensured.meta);
  }

  // Dependencies check: mark blocked if depends_on not completed.
  for (const workId of workIds) {
    const meta = metaById.get(workId);
    const deps = Array.isArray(meta?.depends_on) ? meta.depends_on.slice().filter(Boolean) : [];
    if (!deps.length) continue;
    const unmet = deps.filter((d) => {
      const s = statusById.get(d);
      return !s || String(s.current_stage || "") !== "COMPLETED";
    });
    if (unmet.length) {
      await updateWorkStatus({
        workId,
        stage: "BLOCKED",
        blocked: true,
        blockingReason: `dependencies: ${unmet.join(", ")}`,
        artifacts: { meta_json: `ai/lane_b/work/${workId}/META.json` },
        note: "scheduler blocked due to unmet dependencies",
      });
      statusById.set(workId, (await readWorkStatusSnapshot(workId)).snapshot);
    }
  }

  // Active-per-repo map from "active" work items.
  const activeRepoCounts = new Map();
  for (const workId of workIds) {
    const snap = statusById.get(workId);
    const stage = String(snap?.current_stage || "");
    const active = stage === "APPLYING" || stage === "APPLIED" || (await worktreesPresent(workId));
    if (!active) continue;
    const routing = routingById.get(workId);
    const meta = metaById.get(workId);
    const repos = await touchedReposForWork({ workId, routing, meta });
    for (const r of repos) activeRepoCounts.set(r, (activeRepoCounts.get(r) || 0) + 1);
  }

  const candidates = [];

  for (const workId of workIds) {
    const snap = statusById.get(workId);
    const stage = String(snap?.current_stage || "").trim();
    const blocked = !!snap?.blocked;
    const meta = metaById.get(workId);
    const routing = routingById.get(workId);

    if (!snap) {
      skipped.push({ work_id: workId, reason: "missing_status", repos: [] });
      continue;
    }
    if (blocked) {
      skipped.push({ work_id: workId, reason: `blocked:${snap.blocking_reason || "unknown"}`, repos: [] });
      continue;
    }
    if (isTerminalStage(stage)) {
      skipped.push({ work_id: workId, reason: `terminal:${stage}`, repos: [] });
      continue;
    }
    if (isWaitingForApproval(stage)) {
      skipped.push({ work_id: workId, reason: `waiting:${stage}`, repos: [] });
      continue;
    }
    if (isBeyondWatchdog(stage)) {
      skipped.push({ work_id: workId, reason: `beyond_watchdog:${stage}`, repos: [] });
      continue;
    }

    const repos = await touchedReposForWork({ workId, routing, meta });
    const conflicts = repos.filter((r) => (activeRepoCounts.get(r) || 0) >= cfg.max_active_per_repo);
    if (conflicts.length) {
      skipped.push({ work_id: workId, reason: `repo_concurrency:${conflicts.join(",")}`, repos });
      continue;
    }

    const priority = Number.isFinite(Number(meta?.priority)) ? Number(meta.priority) : 50;
    const createdAt = typeof meta?.created_at === "string" ? meta.created_at : parseWorkCreatedAt({ workId, routing, statusSnapshot: snap });
    candidates.push({ work_id: workId, priority, created_at: createdAt, repos });
  }

  candidates.sort((a, b) => {
    if (orderBy === "queue" && queueIndex) {
      const ia = queueIndex.has(a.work_id) ? queueIndex.get(a.work_id) : Number.MAX_SAFE_INTEGER;
      const ib = queueIndex.has(b.work_id) ? queueIndex.get(b.work_id) : Number.MAX_SAFE_INTEGER;
      if (ia !== ib) return ia - ib;
    }
    if (orderBy === "created_at") {
      const ca = String(a.created_at || "");
      const cb = String(b.created_at || "");
      if (ca !== cb) return ca.localeCompare(cb);
      return String(a.work_id).localeCompare(String(b.work_id));
    }
    if (b.priority !== a.priority) return b.priority - a.priority;
    const ca = String(a.created_at || "");
    const cb = String(b.created_at || "");
    if (ca !== cb) return ca.localeCompare(cb);
    return String(a.work_id).localeCompare(String(b.work_id));
  });

  const selected = candidates.slice(0, maxItems).map((c) => ({
    work_id: c.work_id,
    reason: "eligible",
    score: { priority: c.priority, created_at: c.created_at },
    repos: c.repos,
  }));

  const schedule = {
    version: 1,
    generated_at: nowISO(),
    selected,
    skipped: stableSort(skipped, (x) => x.work_id),
  };

  if (!dryRun) {
    await writeText("ai/lane_b/schedule/SCHEDULE.json", JSON.stringify(schedule, null, 2) + "\n");
    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({
        timestamp: nowISO(),
        action: "schedule_computed",
        selected_count: selected.length,
        skipped_count: schedule.skipped.length,
        path: "ai/lane_b/schedule/SCHEDULE.json",
      }) + "\n",
    );
  }

  return { ok: true, schedule, path: "ai/lane_b/schedule/SCHEDULE.json" };
}
