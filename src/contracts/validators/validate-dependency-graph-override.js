import { assertArray, assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject, assertRelativeRepoPath, assertInt } from "./primitives.js";
import { fail } from "./error.js";

function validateEvidenceArray(arr, path) {
  assertArray(arr, path);
  for (let i = 0; i < arr.length; i += 1) {
    const ev = arr[i];
    assertPlainObject(ev, `${path}[${i}]`);
    const allowed = new Set(["type", "path", "note"]);
    for (const k of Object.keys(ev)) if (!allowed.has(k)) fail(`${path}[${i}].${k}`, "unknown field");
    assertEnumString(ev.type, `${path}[${i}].type`, ["file"]);
    assertRelativeRepoPath(ev.path, `${path}[${i}].path`);
    assertNonUuidString(ev.note, `${path}[${i}].note`, { minLength: 1 });
  }
}

export function validateDependencyGraphOverride(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "updated_at", "status", "approved_by", "approved_at", "notes", "overrides"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");

  assertInt(data.version, "$.version", { min: 1 });
  assertIsoDateTimeZ(data.updated_at, "$.updated_at");
  assertEnumString(data.status, "$.status", ["pending", "approved"]);
  if (data.approved_by !== null && data.approved_by !== undefined) assertNonUuidString(data.approved_by, "$.approved_by", { minLength: 1 });
  if (data.approved_at !== null && data.approved_at !== undefined) assertIsoDateTimeZ(data.approved_at, "$.approved_at");
  if (data.notes !== null && data.notes !== undefined) assertNonUuidString(data.notes, "$.notes", { minLength: 0 });

  assertPlainObject(data.overrides, "$.overrides");
  {
    const o = data.overrides;
    const a = new Set(["remove_edges", "add_edges", "external_projects"]);
    for (const k of Object.keys(o)) if (!a.has(k)) fail(`$.overrides.${k}`, "unknown field");

    assertArray(o.remove_edges, "$.overrides.remove_edges");
    for (let i = 0; i < o.remove_edges.length; i += 1) {
      const e = o.remove_edges[i];
      assertPlainObject(e, `$.overrides.remove_edges[${i}]`);
      const allowedEdge = new Set(["from_repo_id", "to_repo_id"]);
      for (const k of Object.keys(e)) if (!allowedEdge.has(k)) fail(`$.overrides.remove_edges[${i}].${k}`, "unknown field");
      assertNonUuidString(e.from_repo_id, `$.overrides.remove_edges[${i}].from_repo_id`, { minLength: 1 });
      assertNonUuidString(e.to_repo_id, `$.overrides.remove_edges[${i}].to_repo_id`, { minLength: 1 });
    }

    assertArray(o.add_edges, "$.overrides.add_edges");
    for (let i = 0; i < o.add_edges.length; i += 1) {
      const e = o.add_edges[i];
      assertPlainObject(e, `$.overrides.add_edges[${i}]`);
      const allowedEdge = new Set(["from_repo_id", "to_repo_id", "reason", "evidence"]);
      for (const k of Object.keys(e)) if (!allowedEdge.has(k)) fail(`$.overrides.add_edges[${i}].${k}`, "unknown field");
      assertNonUuidString(e.from_repo_id, `$.overrides.add_edges[${i}].from_repo_id`, { minLength: 1 });
      assertNonUuidString(e.to_repo_id, `$.overrides.add_edges[${i}].to_repo_id`, { minLength: 1 });
      assertNonUuidString(e.reason, `$.overrides.add_edges[${i}].reason`, { minLength: 1 });
      validateEvidenceArray(e.evidence, `$.overrides.add_edges[${i}].evidence`);
    }

    assertArray(o.external_projects, "$.overrides.external_projects");
    for (let i = 0; i < o.external_projects.length; i += 1) {
      const ep = o.external_projects[i];
      assertPlainObject(ep, `$.overrides.external_projects[${i}]`);
      const allowedEp = new Set(["project_code", "knowledge_repo_dir", "repos", "reason"]);
      for (const k of Object.keys(ep)) if (!allowedEp.has(k)) fail(`$.overrides.external_projects[${i}].${k}`, "unknown field");
      assertNonUuidString(ep.project_code, `$.overrides.external_projects[${i}].project_code`, { minLength: 1 });
      assertNonUuidString(ep.knowledge_repo_dir, `$.overrides.external_projects[${i}].knowledge_repo_dir`, { minLength: 1 });
      assertArray(ep.repos, `$.overrides.external_projects[${i}].repos`);
      for (let j = 0; j < ep.repos.length; j += 1) assertNonUuidString(ep.repos[j], `$.overrides.external_projects[${i}].repos[${j}]`, { minLength: 1 });
      assertNonUuidString(ep.reason, `$.overrides.external_projects[${i}].reason`, { minLength: 1 });
    }
  }

  return data;
}

