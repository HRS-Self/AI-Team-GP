function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function nonEmptyString(x) {
  return typeof x === "string" && x.length > 0;
}

/**
 * Deterministically build `.github/ai-ci.json` content from a patch plan's `commands`.
 *
 * Hard rules (no inference / no defaults):
 * - commands must exist as an object on the patch plan input (raw JSON).
 * - At least one of install/lint/build/test must be a non-null string.
 * - Do NOT include null keys.
 * - Preserve command strings verbatim.
 * - Ignore cwd/package_manager (engine-only metadata).
 */
export function buildAiCiContractFromPatchPlan({ patchPlanJson }) {
  const plan = isPlainObject(patchPlanJson) ? patchPlanJson : null;
  if (!plan) return { ok: false, message: "Patch plan JSON must be an object." };

  if (!Object.prototype.hasOwnProperty.call(plan, "commands")) {
    return { ok: false, message: "Patch plan missing required top-level key: commands." };
  }
  const commands = isPlainObject(plan.commands) ? plan.commands : null;
  if (!commands) return { ok: false, message: "Patch plan commands must be an object." };

  const install = commands.install === null ? null : commands.install;
  const lint = commands.lint === null ? null : commands.lint;
  const build = commands.build === null ? null : commands.build;
  const test = commands.test === null ? null : commands.test;

  const out = { version: 1 };
  if (nonEmptyString(install)) out.install = install;
  if (nonEmptyString(lint)) out.lint = lint;
  if (nonEmptyString(build)) out.build = build;
  if (nonEmptyString(test)) out.test = test;

  const hasAny =
    Object.prototype.hasOwnProperty.call(out, "install") ||
    Object.prototype.hasOwnProperty.call(out, "lint") ||
    Object.prototype.hasOwnProperty.call(out, "build") ||
    Object.prototype.hasOwnProperty.call(out, "test");

  if (!hasAny) {
    return { ok: false, message: "Patch plan commands invalid for CI contract: all commands are null/empty (install/lint/build/test)." };
  }

  // Deterministic output ordering (version first, then install/lint/build/test).
  const ordered = { version: 1 };
  if (Object.prototype.hasOwnProperty.call(out, "install")) ordered.install = out.install;
  if (Object.prototype.hasOwnProperty.call(out, "lint")) ordered.lint = out.lint;
  if (Object.prototype.hasOwnProperty.call(out, "build")) ordered.build = out.build;
  if (Object.prototype.hasOwnProperty.call(out, "test")) ordered.test = out.test;

  const text = JSON.stringify(ordered, null, 2) + "\n";
  return { ok: true, contract: ordered, text };
}

