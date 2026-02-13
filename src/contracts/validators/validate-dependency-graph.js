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

export function validateDependencyGraph(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "generated_at", "project", "nodes", "edges", "external_projects"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");

  assertInt(data.version, "$.version", { min: 1 });
  assertIsoDateTimeZ(data.generated_at, "$.generated_at");

  assertPlainObject(data.project, "$.project");
  {
    const p = data.project;
    const a = new Set(["code"]);
    for (const k of Object.keys(p)) if (!a.has(k)) fail(`$.project.${k}`, "unknown field");
    assertNonUuidString(p.code, "$.project.code", { minLength: 1 });
  }

  assertArray(data.nodes, "$.nodes");
  for (let i = 0; i < data.nodes.length; i += 1) {
    const n = data.nodes[i];
    assertPlainObject(n, `$.nodes[${i}]`);
    const a = new Set(["repo_id", "team_id", "type"]);
    for (const k of Object.keys(n)) if (!a.has(k)) fail(`$.nodes[${i}].${k}`, "unknown field");
    assertNonUuidString(n.repo_id, `$.nodes[${i}].repo_id`, { minLength: 1 });
    assertNonUuidString(n.team_id, `$.nodes[${i}].team_id`, { minLength: 1 });
    assertEnumString(n.type, `$.nodes[${i}].type`, ["repo"]);
  }

  assertArray(data.edges, "$.edges");
  for (let i = 0; i < data.edges.length; i += 1) {
    const e = data.edges[i];
    assertPlainObject(e, `$.edges[${i}]`);
    const a = new Set(["from_repo_id", "to_repo_id", "reason", "evidence"]);
    for (const k of Object.keys(e)) if (!a.has(k)) fail(`$.edges[${i}].${k}`, "unknown field");
    assertNonUuidString(e.from_repo_id, `$.edges[${i}].from_repo_id`, { minLength: 1 });
    assertNonUuidString(e.to_repo_id, `$.edges[${i}].to_repo_id`, { minLength: 1 });
    assertNonUuidString(e.reason, `$.edges[${i}].reason`, { minLength: 1 });
    validateEvidenceArray(e.evidence, `$.edges[${i}].evidence`);
  }

  assertArray(data.external_projects, "$.external_projects");
  for (let i = 0; i < data.external_projects.length; i += 1) {
    const ep = data.external_projects[i];
    assertPlainObject(ep, `$.external_projects[${i}]`);
    const a = new Set(["project_code", "knowledge_repo_dir", "repos", "reason"]);
    for (const k of Object.keys(ep)) if (!a.has(k)) fail(`$.external_projects[${i}].${k}`, "unknown field");
    assertNonUuidString(ep.project_code, `$.external_projects[${i}].project_code`, { minLength: 1 });
    assertNonUuidString(ep.knowledge_repo_dir, `$.external_projects[${i}].knowledge_repo_dir`, { minLength: 1 });
    assertArray(ep.repos, `$.external_projects[${i}].repos`);
    for (let j = 0; j < ep.repos.length; j += 1) assertNonUuidString(ep.repos[j], `$.external_projects[${i}].repos[${j}]`, { minLength: 1 });
    assertNonUuidString(ep.reason, `$.external_projects[${i}].reason`, { minLength: 1 });
  }

  return data;
}

