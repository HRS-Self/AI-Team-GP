import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject, assertRelativeRepoPath, assertSha40 } from "./primitives.js";
import { fail } from "./error.js";

export function validateKnowledgeEvent(data) {
  assertPlainObject(data, "$");
  const type = assertNonUuidString(data.type, "$.type", { minLength: 1 });

  if (type === "decision_answered") {
    const allowedTop = new Set(["event_id", "type", "decision_id", "scope", "timestamp"]);
    for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");
    assertNonUuidString(data.event_id, "$.event_id", { minLength: 8 });
    assertNonUuidString(data.decision_id, "$.decision_id", { minLength: 8 });
    const scope = assertNonUuidString(data.scope, "$.scope", { minLength: 1 });
    if (!(scope === "system" || scope.startsWith("repo:"))) fail("$.scope", "must be 'system' or 'repo:<repo_id>'");
    assertIsoDateTimeZ(data.timestamp, "$.timestamp");
    return data;
  }

  const allowedTop = new Set(["event_id", "type", "repo_id", "merge_commit", "changed_files", "timestamp"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  assertNonUuidString(data.event_id, "$.event_id", { minLength: 8 });
  assertEnumString(data.type, "$.type", ["merge_applied", "api_changed", "behavior_changed"]);
  assertNonUuidString(data.repo_id, "$.repo_id", { minLength: 1 });
  assertSha40(data.merge_commit, "$.merge_commit");
  assertIsoDateTimeZ(data.timestamp, "$.timestamp");

  assertArray(data.changed_files, "$.changed_files");
  for (let i = 0; i < data.changed_files.length; i += 1) assertRelativeRepoPath(data.changed_files[i], `$.changed_files[${i}]`);
  return data;
}
