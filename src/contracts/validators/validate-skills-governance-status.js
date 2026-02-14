import { fail } from "./error.js";
import { assertArray, assertBoolean, assertInt, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";

function assertStringArray(value, path) {
  assertArray(value, path);
  for (let i = 0; i < value.length; i += 1) assertNonUuidString(value[i], `${path}[${i}]`, { minLength: 1 });
}

export function validateSkillsGovernanceStatus(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set([
    "version",
    "projectRoot",
    "captured_at",
    "env",
    "skills",
    "drafts",
    "approvals",
    "candidates_created_this_run",
    "drafts_created_this_run",
    "notes",
  ]);
  for (const key of Object.keys(data)) if (!allowedTop.has(key)) fail(`$.${key}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertNonUuidString(data.projectRoot, "$.projectRoot", { minLength: 1 });
  assertIsoDateTimeZ(data.captured_at, "$.captured_at");

  assertPlainObject(data.env, "$.env");
  {
    const allowed = new Set(["enabled", "draft_daily_cap", "min_reuse_repos", "min_evidence_refs", "auto_author", "require_approval"]);
    for (const key of Object.keys(data.env)) if (!allowed.has(key)) fail(`$.env.${key}`, "unknown field");
    assertBoolean(data.env.enabled, "$.env.enabled");
    assertInt(data.env.draft_daily_cap, "$.env.draft_daily_cap", { min: 0 });
    assertInt(data.env.min_reuse_repos, "$.env.min_reuse_repos", { min: 1 });
    assertInt(data.env.min_evidence_refs, "$.env.min_evidence_refs", { min: 1 });
    assertBoolean(data.env.auto_author, "$.env.auto_author");
    assertBoolean(data.env.require_approval, "$.env.require_approval");
  }

  assertPlainObject(data.skills, "$.skills");
  {
    const allowed = new Set(["total", "known", "stale"]);
    for (const key of Object.keys(data.skills)) if (!allowed.has(key)) fail(`$.skills.${key}`, "unknown field");
    assertInt(data.skills.total, "$.skills.total", { min: 0 });
    assertStringArray(data.skills.known, "$.skills.known");
    assertStringArray(data.skills.stale, "$.skills.stale");
  }

  assertPlainObject(data.drafts, "$.drafts");
  {
    const allowed = new Set(["pending", "published", "refresh_pending"]);
    for (const key of Object.keys(data.drafts)) if (!allowed.has(key)) fail(`$.drafts.${key}`, "unknown field");
    assertStringArray(data.drafts.pending, "$.drafts.pending");
    assertStringArray(data.drafts.published, "$.drafts.published");
    assertStringArray(data.drafts.refresh_pending, "$.drafts.refresh_pending");
  }

  assertPlainObject(data.approvals, "$.approvals");
  {
    const allowed = new Set(["approved", "rejected"]);
    for (const key of Object.keys(data.approvals)) if (!allowed.has(key)) fail(`$.approvals.${key}`, "unknown field");
    assertStringArray(data.approvals.approved, "$.approvals.approved");
    assertStringArray(data.approvals.rejected, "$.approvals.rejected");
  }

  assertInt(data.candidates_created_this_run, "$.candidates_created_this_run", { min: 0 });
  assertInt(data.drafts_created_this_run, "$.drafts_created_this_run", { min: 0 });

  if (typeof data.notes !== "undefined" && typeof data.notes !== "string") fail("$.notes", "must be a string when provided");

  return data;
}

