import { readFileSync } from "node:fs";
import { join } from "node:path";

import { fail } from "./error.js";
import { assertIsoDateTimeZ, assertPlainObject } from "./primitives.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function assertNullableString(x, path) {
  if (x === null) return null;
  if (typeof x !== "string") fail(path, "must be string|null");
  return x;
}

function assertNullableIso(x, path) {
  if (x === null) return null;
  assertIsoDateTimeZ(x, path);
  return x;
}

function validatePhaseBlock(obj, path) {
  assertPlainObject(obj, path);
  const allowed = new Set(["status", "session_id", "started_at", "closed_at", "closed_by", "notes"]);
  for (const k of Object.keys(obj)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");
  const st = normStr(obj.status).toLowerCase();
  if (!(st === "not_started" || st === "in_progress" || st === "closed")) fail(`${path}.status`, "must be not_started|in_progress|closed");
  assertNullableString(obj.session_id, `${path}.session_id`);
  assertNullableIso(obj.started_at, `${path}.started_at`);
  assertNullableIso(obj.closed_at, `${path}.closed_at`);
  assertNullableString(obj.closed_by, `${path}.closed_by`);
  assertNullableString(obj.notes, `${path}.notes`);
  return obj;
}

function validatePrereqs(obj, path) {
  assertPlainObject(obj, path);
  const allowed = new Set(["scan_complete", "sufficiency", "human_confirmed_v1", "human_confirmed_at", "human_confirmed_by", "human_notes"]);
  for (const k of Object.keys(obj)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");
  if (typeof obj.scan_complete !== "boolean") fail(`${path}.scan_complete`, "must be boolean");
  const s = normStr(obj.sufficiency).toLowerCase();
  if (!(s === "unknown" || s === "insufficient" || s === "sufficient")) fail(`${path}.sufficiency`, "must be unknown|insufficient|sufficient");
  if (typeof obj.human_confirmed_v1 !== "boolean") fail(`${path}.human_confirmed_v1`, "must be boolean");
  assertNullableIso(obj.human_confirmed_at, `${path}.human_confirmed_at`);
  assertNullableString(obj.human_confirmed_by, `${path}.human_confirmed_by`);
  assertNullableString(obj.human_notes, `${path}.human_notes`);
  return obj;
}

export function validatePhaseState(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "projectRoot", "current_phase", "reverse", "forward", "prereqs"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");
  if (data.version !== 1) fail("$.version", "must be 1");
  if (!normStr(data.projectRoot)) fail("$.projectRoot", "is required");
  const cur = normStr(data.current_phase).toLowerCase();
  if (!(cur === "reverse" || cur === "forward")) fail("$.current_phase", "must be reverse|forward");
  validatePhaseBlock(data.reverse, "$.reverse");
  validatePhaseBlock(data.forward, "$.forward");
  validatePrereqs(data.prereqs, "$.prereqs");

  // Minimal invariants (do not infer/correct):
  // - closed phases must have closed_at + closed_by
  for (const key of ["reverse", "forward"]) {
    const blk = data[key];
    const st = normStr(blk.status).toLowerCase();
    if (st === "closed") {
      if (!normStr(blk.closed_by)) fail(`$.${key}.closed_by`, "required when status=closed");
      if (!normStr(blk.closed_at)) fail(`$.${key}.closed_at`, "required when status=closed");
    }
    if (st === "in_progress") {
      if (!normStr(blk.started_at)) fail(`$.${key}.started_at`, "required when status=in_progress");
    }
  }

  // If human_confirmed_v1, require by/at.
  if (data.prereqs.human_confirmed_v1 === true) {
    if (!normStr(data.prereqs.human_confirmed_by)) fail("$.prereqs.human_confirmed_by", "required when human_confirmed_v1=true");
    if (!normStr(data.prereqs.human_confirmed_at)) fail("$.prereqs.human_confirmed_at", "required when human_confirmed_v1=true");
  }

  return data;
}

export function readPhaseSchemaText() {
  return readFileSync(join(process.cwd(), "src", "contracts", "state", "phase.schema.json"), "utf8");
}

