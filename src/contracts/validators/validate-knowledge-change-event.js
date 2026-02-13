import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject, assertSha40 } from "./primitives.js";
import { fail } from "./error.js";

export function validateKnowledgeChangeEvent(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["version", "event_id", "type", "scope", "repo_id", "work_id", "pr_number", "commit", "artifacts", "summary", "timestamp"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  assertNonUuidString(data.event_id, "$.event_id", { minLength: 8 });
  const type = assertEnumString(data.type, "$.type", ["merge", "ci_fix", "schema_change", "api_change", "config_change"]);
  const scope = assertNonUuidString(data.scope, "$.scope", { minLength: 1 });
  const workId = assertNonUuidString(data.work_id, "$.work_id", { minLength: 1 });
  void workId;

  if (!(scope === "system" || scope.startsWith("repo:"))) fail("$.scope", "must be 'system' or 'repo:<repo_id>'");
  const repo_id = data.repo_id === null ? null : assertNonUuidString(data.repo_id, "$.repo_id", { minLength: 1 });
  if (scope === "system" && repo_id !== null) fail("$.repo_id", "must be null when scope is system");
  if (scope.startsWith("repo:")) {
    const expected = scope.slice("repo:".length);
    if (repo_id === null) fail("$.repo_id", "required when scope is repo:*");
    if (repo_id !== expected) fail("$.repo_id", "must match scope repo_id");
  }

  if (data.pr_number !== null) {
    if (typeof data.pr_number !== "number" || !Number.isFinite(data.pr_number) || data.pr_number <= 0) fail("$.pr_number", "must be a positive number or null");
  }

  assertSha40(data.commit, "$.commit");
  assertIsoDateTimeZ(data.timestamp, "$.timestamp");
  assertNonUuidString(data.summary, "$.summary", { minLength: 1 });

  assertPlainObject(data.artifacts, "$.artifacts");
  const aAllowed = new Set(["paths", "fingerprints"]);
  for (const k of Object.keys(data.artifacts)) if (!aAllowed.has(k)) fail(`$.artifacts.${k}`, "unknown field");
  assertArray(data.artifacts.paths, "$.artifacts.paths");
  for (let i = 0; i < data.artifacts.paths.length; i += 1) assertNonUuidString(data.artifacts.paths[i], `$.artifacts.paths[${i}]`, { minLength: 1 });
  assertArray(data.artifacts.fingerprints, "$.artifacts.fingerprints");
  for (let i = 0; i < data.artifacts.fingerprints.length; i += 1) assertNonUuidString(data.artifacts.fingerprints[i], `$.artifacts.fingerprints[${i}]`, { minLength: 1 });

  // Minimal type gating: merge/ci_fix should be repo-scoped.
  if ((type === "merge" || type === "ci_fix") && scope === "system") fail("$.scope", "must be repo-scoped for merge/ci_fix events");

  return data;
}

