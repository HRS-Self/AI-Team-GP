import { assertArray, assertInt, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject, assertRelativeRepoPath, assertHex64, assertSha40 } from "./primitives.js";
import { fail } from "./error.js";

export function validateRepoIndex(data) {
  assertPlainObject(data, "$");

  const allowedTop = new Set([
    "version",
    "repo_id",
    "scanned_at",
    "head_sha",
    "languages",
    "entrypoints",
    "build_commands",
    "api_surface",
    "migrations_schema",
    "cross_repo_dependencies",
    "hotspots",
    "fingerprints",
    "dependencies",
  ]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  assertInt(data.version, "$.version", { min: 1 });
  assertNonUuidString(data.repo_id, "$.repo_id", { minLength: 1 });
  assertIsoDateTimeZ(data.scanned_at, "$.scanned_at");
  assertSha40(data.head_sha, "$.head_sha");

  assertArray(data.languages, "$.languages");
  for (let i = 0; i < data.languages.length; i += 1) assertNonUuidString(data.languages[i], `$.languages[${i}]`, { minLength: 1 });

  assertArray(data.entrypoints, "$.entrypoints");
  for (let i = 0; i < data.entrypoints.length; i += 1) assertRelativeRepoPath(data.entrypoints[i], `$.entrypoints[${i}]`);

  assertPlainObject(data.build_commands, "$.build_commands");
  {
    const bc = data.build_commands;
    const allowed = new Set(["package_manager", "install", "lint", "build", "test", "scripts", "evidence_files"]);
    for (const k of Object.keys(bc)) if (!allowed.has(k)) fail(`$.build_commands.${k}`, "unknown field");
    assertNonUuidString(bc.package_manager, "$.build_commands.package_manager", { minLength: 1 });
    assertArray(bc.install, "$.build_commands.install");
    for (let i = 0; i < bc.install.length; i += 1) assertNonUuidString(bc.install[i], `$.build_commands.install[${i}]`, { minLength: 1 });
    assertArray(bc.lint, "$.build_commands.lint");
    for (let i = 0; i < bc.lint.length; i += 1) assertNonUuidString(bc.lint[i], `$.build_commands.lint[${i}]`, { minLength: 1 });
    assertArray(bc.build, "$.build_commands.build");
    for (let i = 0; i < bc.build.length; i += 1) assertNonUuidString(bc.build[i], `$.build_commands.build[${i}]`, { minLength: 1 });
    assertArray(bc.test, "$.build_commands.test");
    for (let i = 0; i < bc.test.length; i += 1) assertNonUuidString(bc.test[i], `$.build_commands.test[${i}]`, { minLength: 1 });
    assertPlainObject(bc.scripts, "$.build_commands.scripts");
    for (const [k, v] of Object.entries(bc.scripts)) {
      assertNonUuidString(k, `$.build_commands.scripts[${JSON.stringify(k)}]`, { minLength: 1 });
      assertNonUuidString(v, `$.build_commands.scripts[${JSON.stringify(k)}]`, { minLength: 1 });
    }
    assertArray(bc.evidence_files, "$.build_commands.evidence_files");
    for (let i = 0; i < bc.evidence_files.length; i += 1) assertRelativeRepoPath(bc.evidence_files[i], `$.build_commands.evidence_files[${i}]`);
  }

  assertPlainObject(data.api_surface, "$.api_surface");
  {
    const a = data.api_surface;
    const allowed = new Set(["openapi_files", "routes_controllers", "events_topics"]);
    for (const k of Object.keys(a)) if (!allowed.has(k)) fail(`$.api_surface.${k}`, "unknown field");
    assertArray(a.openapi_files, "$.api_surface.openapi_files");
    for (let i = 0; i < a.openapi_files.length; i += 1) assertRelativeRepoPath(a.openapi_files[i], `$.api_surface.openapi_files[${i}]`);
    assertArray(a.routes_controllers, "$.api_surface.routes_controllers");
    for (let i = 0; i < a.routes_controllers.length; i += 1) assertRelativeRepoPath(a.routes_controllers[i], `$.api_surface.routes_controllers[${i}]`);
    assertArray(a.events_topics, "$.api_surface.events_topics");
    for (let i = 0; i < a.events_topics.length; i += 1) assertRelativeRepoPath(a.events_topics[i], `$.api_surface.events_topics[${i}]`);
  }

  assertArray(data.migrations_schema, "$.migrations_schema");
  for (let i = 0; i < data.migrations_schema.length; i += 1) assertRelativeRepoPath(data.migrations_schema[i], `$.migrations_schema[${i}]`);

  assertArray(data.cross_repo_dependencies, "$.cross_repo_dependencies");
  for (let i = 0; i < data.cross_repo_dependencies.length; i += 1) {
    const d = data.cross_repo_dependencies[i];
    assertPlainObject(d, `$.cross_repo_dependencies[${i}]`);
    const allowed = new Set(["type", "target", "evidence_refs"]);
    for (const k of Object.keys(d)) if (!allowed.has(k)) fail(`$.cross_repo_dependencies[${i}].${k}`, "unknown field");
    assertNonUuidString(d.type, `$.cross_repo_dependencies[${i}].type`, { minLength: 1 });
    if (!["npm", "maven", "gradle", "git", "http"].includes(String(d.type))) fail(`$.cross_repo_dependencies[${i}].type`, "invalid value");
    assertNonUuidString(d.target, `$.cross_repo_dependencies[${i}].target`, { minLength: 1 });
    assertArray(d.evidence_refs, `$.cross_repo_dependencies[${i}].evidence_refs`, { minItems: 1 });
    for (let j = 0; j < d.evidence_refs.length; j += 1) assertRelativeRepoPath(d.evidence_refs[j], `$.cross_repo_dependencies[${i}].evidence_refs[${j}]`);
  }

  assertArray(data.hotspots, "$.hotspots");
  for (let i = 0; i < data.hotspots.length; i += 1) {
    const h = data.hotspots[i];
    assertPlainObject(h, `$.hotspots[${i}]`);
    const allowed = new Set(["file_path", "reason"]);
    for (const k of Object.keys(h)) if (!allowed.has(k)) fail(`$.hotspots[${i}].${k}`, "unknown field");
    assertRelativeRepoPath(h.file_path, `$.hotspots[${i}].file_path`);
    assertNonUuidString(h.reason, `$.hotspots[${i}].reason`, { minLength: 1 });
  }

  assertPlainObject(data.fingerprints, "$.fingerprints");
  for (const [k, v] of Object.entries(data.fingerprints)) {
    assertPlainObject(v, `$.fingerprints[${JSON.stringify(k)}]`);
    const allowed = new Set(["sha256"]);
    for (const kk of Object.keys(v)) if (!allowed.has(kk)) fail(`$.fingerprints[${JSON.stringify(k)}].${kk}`, "unknown field");
    assertHex64(v.sha256, `$.fingerprints[${JSON.stringify(k)}].sha256`);
  }

  assertPlainObject(data.dependencies, "$.dependencies");
  {
    const d = data.dependencies;
    const allowed = new Set(["version", "detected_at", "mode", "depends_on"]);
    for (const k of Object.keys(d)) if (!allowed.has(k)) fail(`$.dependencies.${k}`, "unknown field");
    assertInt(d.version, "$.dependencies.version", { min: 1 });
    assertIsoDateTimeZ(d.detected_at, "$.dependencies.detected_at");
    const mode = assertNonUuidString(d.mode, "$.dependencies.mode", { minLength: 1 });
    if (mode !== "detected") fail("$.dependencies.mode", "must be detected");
    assertArray(d.depends_on, "$.dependencies.depends_on");
    for (let i = 0; i < d.depends_on.length; i += 1) {
      const it = d.depends_on[i];
      assertPlainObject(it, `$.dependencies.depends_on[${i}]`);
      const a = new Set([
        "kind",
        "repo_id",
        "owner_repo",
        "project_code",
        "abs_path",
        "active_branch",
        "knowledge_abs_path",
        "knowledge_git_remote",
        "knowledge_active_branch",
        "reason",
        "evidence",
      ]);
      for (const k of Object.keys(it)) if (!a.has(k)) fail(`$.dependencies.depends_on[${i}].${k}`, "unknown field");
      const kind = assertNonUuidString(it.kind, `$.dependencies.depends_on[${i}].kind`, { minLength: 1 });
      if (kind !== "project_repo") fail(`$.dependencies.depends_on[${i}].kind`, "must be project_repo");
      assertNonUuidString(it.repo_id, `$.dependencies.depends_on[${i}].repo_id`, { minLength: 1 });
      assertNonUuidString(it.owner_repo, `$.dependencies.depends_on[${i}].owner_repo`, { minLength: 1 });
      assertNonUuidString(it.project_code, `$.dependencies.depends_on[${i}].project_code`, { minLength: 1 });
      assertNonUuidString(it.abs_path, `$.dependencies.depends_on[${i}].abs_path`, { minLength: 1 });
      assertNonUuidString(it.active_branch, `$.dependencies.depends_on[${i}].active_branch`, { minLength: 1 });
      assertNonUuidString(it.knowledge_abs_path, `$.dependencies.depends_on[${i}].knowledge_abs_path`, { minLength: 1 });
      if (it.knowledge_git_remote !== null && it.knowledge_git_remote !== undefined) assertNonUuidString(it.knowledge_git_remote, `$.dependencies.depends_on[${i}].knowledge_git_remote`, { minLength: 0 });
      assertNonUuidString(it.knowledge_active_branch, `$.dependencies.depends_on[${i}].knowledge_active_branch`, { minLength: 1 });
      assertNonUuidString(it.reason, `$.dependencies.depends_on[${i}].reason`, { minLength: 1 });
      assertArray(it.evidence, `$.dependencies.depends_on[${i}].evidence`);
      for (let j = 0; j < it.evidence.length; j += 1) {
        const ev = it.evidence[j];
        assertPlainObject(ev, `$.dependencies.depends_on[${i}].evidence[${j}]`);
        const b = new Set(["type", "path", "note"]);
        for (const k of Object.keys(ev)) if (!b.has(k)) fail(`$.dependencies.depends_on[${i}].evidence[${j}].${k}`, "unknown field");
        const t = assertNonUuidString(ev.type, `$.dependencies.depends_on[${i}].evidence[${j}].type`, { minLength: 1 });
        if (t !== "file") fail(`$.dependencies.depends_on[${i}].evidence[${j}].type`, "must be file");
        assertRelativeRepoPath(ev.path, `$.dependencies.depends_on[${i}].evidence[${j}].path`);
        assertNonUuidString(ev.note, `$.dependencies.depends_on[${i}].evidence[${j}].note`, { minLength: 1 });
      }
    }
  }

  return data;
}
