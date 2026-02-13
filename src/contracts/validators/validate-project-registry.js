import { assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function assertArray(x, path) {
  if (!Array.isArray(x)) fail(path, "must be an array");
  return x;
}

function assertInt(x, path, { min = null } = {}) {
  const n = typeof x === "number" ? x : Number.parseInt(String(x ?? ""), 10);
  if (!Number.isFinite(n)) fail(path, "must be an integer");
  if (!Number.isInteger(n)) fail(path, "must be an integer");
  if (min != null && n < min) fail(path, `must be >= ${min}`);
  return n;
}

function assertBool(x, path) {
  if (typeof x !== "boolean") fail(path, "must be boolean");
  return x;
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function validatePorts(obj, path) {
  assertPlainObject(obj, path);
  const allowed = new Set(["webui_base", "webui_next", "websvc_base", "websvc_next"]);
  for (const k of Object.keys(obj)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");
  assertInt(obj.webui_base, `${path}.webui_base`, { min: 1 });
  assertInt(obj.webui_next, `${path}.webui_next`, { min: 1 });
  assertInt(obj.websvc_base, `${path}.websvc_base`, { min: 1 });
  assertInt(obj.websvc_next, `${path}.websvc_next`, { min: 1 });
  return obj;
}

function validateProjectRepo(r, path) {
  assertPlainObject(r, path);
  const allowed = new Set(["repo_id", "owner_repo", "abs_path", "default_branch", "active_branch", "last_seen_head_sha", "active"]);
  for (const k of Object.keys(r)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");
  assertNonUuidString(r.repo_id, `${path}.repo_id`, { minLength: 1 });
  assertNonUuidString(r.owner_repo, `${path}.owner_repo`, { minLength: 1 });
  assertNonUuidString(r.abs_path, `${path}.abs_path`, { minLength: 1 });
  assertNonUuidString(r.default_branch, `${path}.default_branch`, { minLength: 1 });
  assertNonUuidString(r.active_branch, `${path}.active_branch`, { minLength: 1 });
  if (r.last_seen_head_sha !== null && r.last_seen_head_sha !== undefined) assertNonUuidString(r.last_seen_head_sha, `${path}.last_seen_head_sha`, { minLength: 7 });
  assertBool(r.active, `${path}.active`);
  return r;
}

function validateProject(p, path) {
  assertPlainObject(p, path);
  const allowed = new Set([
    "project_code",
    "status",
    "root_dir",
    "ops_dir",
    "repos_dir",
    "created_at",
    "updated_at",
    "ports",
    "pm2",
    "cron",
    "knowledge",
    "repos",
  ]);
  for (const k of Object.keys(p)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");

  assertNonUuidString(p.project_code, `${path}.project_code`, { minLength: 1 });
  const status = assertNonUuidString(p.status, `${path}.status`, { minLength: 1 }).toLowerCase();
  if (!(status === "active" || status === "removed")) fail(`${path}.status`, "must be active|removed");

  assertNonUuidString(p.root_dir, `${path}.root_dir`, { minLength: 1 });
  assertNonUuidString(p.ops_dir, `${path}.ops_dir`, { minLength: 1 });
  assertNonUuidString(p.repos_dir, `${path}.repos_dir`, { minLength: 1 });

  assertIsoDateTimeZ(p.created_at, `${path}.created_at`);
  assertIsoDateTimeZ(p.updated_at, `${path}.updated_at`);

  assertPlainObject(p.ports, `${path}.ports`);
  {
    const a = new Set(["webui_port", "websvc_port"]);
    for (const k of Object.keys(p.ports)) if (!a.has(k)) fail(`${path}.ports.${k}`, "unknown field");
    assertInt(p.ports.webui_port, `${path}.ports.webui_port`, { min: 1 });
    assertInt(p.ports.websvc_port, `${path}.ports.websvc_port`, { min: 1 });
  }

  assertPlainObject(p.pm2, `${path}.pm2`);
  {
    const a = new Set(["ecosystem_path", "apps"]);
    for (const k of Object.keys(p.pm2)) if (!a.has(k)) fail(`${path}.pm2.${k}`, "unknown field");
    assertNonUuidString(p.pm2.ecosystem_path, `${path}.pm2.ecosystem_path`, { minLength: 1 });
    assertArray(p.pm2.apps, `${path}.pm2.apps`);
    for (let i = 0; i < p.pm2.apps.length; i += 1) assertNonUuidString(p.pm2.apps[i], `${path}.pm2.apps[${i}]`, { minLength: 1 });
  }

  assertPlainObject(p.cron, `${path}.cron`);
  {
    const a = new Set(["installed", "entries"]);
    for (const k of Object.keys(p.cron)) if (!a.has(k)) fail(`${path}.cron.${k}`, "unknown field");
    assertBool(p.cron.installed, `${path}.cron.installed`);
    assertArray(p.cron.entries, `${path}.cron.entries`);
    for (let i = 0; i < p.cron.entries.length; i += 1) assertNonUuidString(p.cron.entries[i], `${path}.cron.entries[${i}]`, { minLength: 1 });
  }

  assertPlainObject(p.knowledge, `${path}.knowledge`);
  {
    const a = new Set(["type", "abs_path", "git_remote", "default_branch", "active_branch", "last_commit_sha"]);
    for (const k of Object.keys(p.knowledge)) if (!a.has(k)) fail(`${path}.knowledge.${k}`, "unknown field");
    const type = assertNonUuidString(p.knowledge.type, `${path}.knowledge.type`, { minLength: 1 }).toLowerCase();
    if (!(type === "git" || type === "readonly" || type === "external")) fail(`${path}.knowledge.type`, "must be git|readonly|external");
    assertNonUuidString(p.knowledge.abs_path, `${path}.knowledge.abs_path`, { minLength: 1 });
    assertNonUuidString(p.knowledge.git_remote, `${path}.knowledge.git_remote`, { minLength: 0 });
    assertNonUuidString(p.knowledge.default_branch, `${path}.knowledge.default_branch`, { minLength: 1 });
    assertNonUuidString(p.knowledge.active_branch, `${path}.knowledge.active_branch`, { minLength: 1 });
    if (p.knowledge.last_commit_sha !== null && p.knowledge.last_commit_sha !== undefined) assertNonUuidString(p.knowledge.last_commit_sha, `${path}.knowledge.last_commit_sha`, { minLength: 7 });
  }

  assertArray(p.repos, `${path}.repos`);
  for (let i = 0; i < p.repos.length; i += 1) validateProjectRepo(p.repos[i], `${path}.repos[${i}]`);
  return p;
}

export function validateProjectRegistry(data) {
  assertPlainObject(data, "$");
  const allowed = new Set(["version", "host_id", "created_at", "updated_at", "ports", "projects"]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");
  if (data.version !== 2) fail("$.version", "must be 2");
  assertNonUuidString(data.host_id, "$.host_id", { minLength: 1 });
  assertIsoDateTimeZ(data.created_at, "$.created_at");
  assertIsoDateTimeZ(data.updated_at, "$.updated_at");
  validatePorts(data.ports, "$.ports");
  assertArray(data.projects, "$.projects");
  for (let i = 0; i < data.projects.length; i += 1) validateProject(data.projects[i], `$.projects[${i}]`);
  return data;
}
