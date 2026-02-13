import { isAbsolute, posix as pathPosix } from "node:path";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function isStringArray(x) {
  return Array.isArray(x) && x.every((v) => typeof v === "string");
}

function validateGitRefName(nameRaw) {
  if (!isNonEmptyString(nameRaw)) throw new Error("name must be a non-empty string");
  const name = nameRaw.trim();
  // Keep rules conservative and readable. We don't need to accept every possible git ref:
  // we need to reject unsafe/ambiguous names (whitespace, traversal, weird punctuation).
  if (/\s/.test(name)) throw new Error("name must not contain whitespace");
  if (name.startsWith("-")) throw new Error("name must not start with '-'");
  if (name.startsWith("/") || name.endsWith("/")) throw new Error("name must not start or end with '/'");
  if (name.includes("..")) throw new Error("name must not contain '..'");
  if (name.includes("//")) throw new Error("name must not contain '//'");
  if (name.includes("@{")) throw new Error("name must not contain '@{'");
  if (name.includes("\\")) throw new Error("name must not contain '\\\\'");
  if (name.includes("\0")) throw new Error("name must not contain NUL byte");
  // Allow typical branch characters: letters, digits, dot, underscore, dash, slash.
  if (!/^[A-Za-z0-9._/-]+$/.test(name)) throw new Error("name contains invalid characters");
  return name;
}

function normalizeRepoRelPath(p) {
  if (!isNonEmptyString(p)) throw new Error("path must be a non-empty string");
  const s = p.trim();
  if (s.includes("\0")) throw new Error("path contains NUL byte");
  if (s.includes("\\")) throw new Error("path must use forward slashes");
  if (isAbsolute(s) || s.startsWith("/")) throw new Error("path must be repo-relative (not absolute)");
  const norm = pathPosix.normalize(s);
  if (norm === "." || norm === "./") return ".";
  if (norm.startsWith("../") || norm === "..") throw new Error("path traversal is not allowed");
  if (norm.includes("/../")) throw new Error("path traversal is not allowed");
  return norm;
}

function globToRegExp(glob) {
  // Minimal glob: * matches within segment; ** matches across segments.
  const g = normalizeRepoRelPath(glob);
  if (g === ".") return /^.*$/;
  const esc = (ch) => (/[\\^$+?.()|[\]{}]/.test(ch) ? `\\${ch}` : ch);
  let out = "^";
  for (let i = 0; i < g.length; i += 1) {
    const ch = g[i];
    if (ch === "*") {
      const next = g[i + 1];
      if (next === "*") {
        out += ".*";
        i += 1;
      } else {
        out += "[^/]*";
      }
      continue;
    }
    out += esc(ch);
  }
  out += "$";
  return new RegExp(out);
}

function compileMatchers(patterns) {
  const matchers = [];
  for (const raw of Array.isArray(patterns) ? patterns : []) {
    const p = String(raw || "").trim();
    if (!p) continue;
    const hasGlob = p.includes("*");
    if (hasGlob) {
      matchers.push({ type: "glob", raw: p, re: globToRegExp(p) });
    } else {
      const norm = normalizeRepoRelPath(p);
      matchers.push({ type: "prefix", raw: p, norm });
    }
  }
  return matchers;
}

function matchesAny(pathRel, matchers) {
  const p = normalizeRepoRelPath(pathRel);
  for (const m of matchers) {
    if (m.type === "glob") {
      if (m.re.test(p)) return true;
      continue;
    }
    if (m.norm === ".") return true;
    if (p === m.norm) return true;
    if (p.startsWith(`${m.norm}/`)) return true;
  }
  return false;
}

function get(obj, key) {
  return isPlainObject(obj) ? obj[key] : undefined;
}

function looksLikeAbsolutePathInCommand(raw) {
  const s = String(raw || "");
  if (!s.trim()) return false;
  // Avoid flagging URLs.
  const withoutUrls = s.replace(new RegExp("https?://\\\\S+", "g"), "URL");
  const tokens = withoutUrls.split(/\s+/).filter(Boolean);
  for (const t of tokens) {
    if (t.startsWith("/")) return true;
    if (/^[A-Za-z]:\\/.test(t)) return true;
    if (t.includes("=/")) return true;
  }
  return false;
}

export function validatePatchPlan(plan, { policy = null, expected_proposal_hash = null, expected_proposal_agent_id = null } = {}) {
  const errors = [];
  const add = (msg) => errors.push(msg);

  if (!isPlainObject(plan)) return { ok: false, errors: ["Patch plan must be a JSON object."], normalized: null };

  if (plan.version !== 1) add("version must be 1.");
  if (!isNonEmptyString(plan.work_id)) add("work_id must be a non-empty string.");
  if (!isNonEmptyString(plan.repo_id)) add("repo_id must be a non-empty string.");
  if (!isNonEmptyString(plan.repo_path)) add("repo_path must be a non-empty string.");
  if (isNonEmptyString(plan.repo_path)) {
    try {
      const norm = normalizeRepoRelPath(plan.repo_path);
      if (norm === ".") add("repo_path must not be '.' (expected repos[].path from config/REPOS.json).");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      add(`repo_path invalid: ${msg}`);
    }
  }
  if (typeof plan.is_hexa !== "boolean") add("is_hexa must be boolean.");
  if (!isNonEmptyString(plan.team_id)) add("team_id must be a non-empty string.");
  if (!isNonEmptyString(plan.kind)) add("kind must be a non-empty string.");
  if (!isNonEmptyString(plan.intent_summary)) add("intent_summary must be a non-empty string.");
  if (typeof plan.warnings !== "undefined" && !isStringArray(plan.warnings)) add("warnings must be string[] when present.");

  // target_branch
  const targetBranch = plan.target_branch;
  if (!isPlainObject(targetBranch)) add("target_branch must be an object.");
  if (isPlainObject(targetBranch)) {
    if (!isNonEmptyString(targetBranch.name)) add("target_branch.name must be a non-empty string.");
    if (targetBranch.source !== "routing") add("target_branch.source must be 'routing'.");
    if (targetBranch.confidence !== 1) add("target_branch.confidence must be 1.");
    if (isNonEmptyString(targetBranch.name)) {
      try {
        validateGitRefName(targetBranch.name);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        add(`target_branch.name invalid: ${msg}`);
      }
    }
  }

  // derived_from (proposal provenance)
  const derived = plan.derived_from;
  if (!isPlainObject(derived)) add("derived_from must be an object.");
  if (isPlainObject(derived)) {
    if (!isNonEmptyString(derived.proposal_id)) add("derived_from.proposal_id must be a non-empty string.");
    if (!isNonEmptyString(derived.proposal_hash)) add("derived_from.proposal_hash must be a non-empty string.");
    if (!isNonEmptyString(derived.proposal_agent_id)) add("derived_from.proposal_agent_id must be a non-empty string.");
    if (!isNonEmptyString(derived.timestamp)) add("derived_from.timestamp must be a non-empty string.");
  }
  if (isPlainObject(derived) && expected_proposal_hash && String(derived.proposal_hash || "").trim() !== String(expected_proposal_hash).trim()) {
    add("derived_from.proposal_hash does not match expected proposal hash.");
  }
  if (isPlainObject(derived) && expected_proposal_agent_id && String(derived.proposal_agent_id || "").trim() !== String(expected_proposal_agent_id).trim()) {
    add("derived_from.proposal_agent_id does not match expected proposal agent_id.");
  }

  // scope
  const scope = plan.scope;
  if (!isPlainObject(scope)) add("scope must be an object.");
  const allowedPaths = get(scope, "allowed_paths");
  const forbiddenPaths = get(scope, "forbidden_paths");
  const allowedOpsRaw = get(scope, "allowed_ops");
  if (!Array.isArray(allowedPaths) || !allowedPaths.every(isNonEmptyString)) add("scope.allowed_paths must be string[].");
  if (typeof forbiddenPaths !== "undefined" && (!Array.isArray(forbiddenPaths) || !forbiddenPaths.every(isNonEmptyString)))
    add("scope.forbidden_paths must be string[] if present.");
  const allowedOps = Array.isArray(allowedOpsRaw) && allowedOpsRaw.length ? allowedOpsRaw : ["edit", "add"];
  if (!Array.isArray(allowedOps) || !allowedOps.every((x) => x === "edit" || x === "add" || x === "delete")) add("scope.allowed_ops must be edit|add|delete[].");

  const allowedMatchers = compileMatchers(Array.isArray(allowedPaths) ? allowedPaths : []);
  const forbiddenMatchers = compileMatchers(Array.isArray(forbiddenPaths) ? forbiddenPaths : []);

  // edits
  const edits = plan.edits;
  if (!Array.isArray(edits)) add("edits must be an array.");
  if (Array.isArray(edits)) {
    edits.forEach((e, i) => {
      if (!isPlainObject(e)) {
        add(`edits[${i}] must be an object.`);
        return;
      }
      if (Object.prototype.hasOwnProperty.call(e, "diff")) add(`edits[${i}].diff is forbidden; use edits[${i}].patch.`);
      if (Object.prototype.hasOwnProperty.call(e, "instructions")) add(`edits[${i}].instructions is forbidden; use edits[${i}].patch.`);
      const path = e.path;
      const op = e.op;
      if (!isNonEmptyString(path)) add(`edits[${i}].path must be a non-empty string.`);
      if (op !== "edit" && op !== "add" && op !== "delete") add(`edits[${i}].op must be edit|add|delete.`);
      if (isNonEmptyString(op) && !allowedOps.includes(op)) add(`edits[${i}].op '${op}' is not allowed by scope.allowed_ops.`);
      if (!isNonEmptyString(e.rationale)) add(`edits[${i}].rationale must be a non-empty string.`);
      if (!isNonEmptyString(e.patch)) add(`edits[${i}].patch missing/empty.`);
      if (isNonEmptyString(path)) {
        try {
          const norm = normalizeRepoRelPath(path);
          if (!matchesAny(norm, allowedMatchers)) add(`edits[${i}].path '${norm}' is out of scope (not under scope.allowed_paths).`);
          if (matchesAny(norm, forbiddenMatchers)) add(`edits[${i}].path '${norm}' is forbidden by scope.forbidden_paths.`);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          add(`edits[${i}].path invalid: ${msg}`);
        }
      }
    });
  }

  // commands (must be explicit nulls)
  const commands = plan.commands;
  const requiredCommandKeys = ["cwd", "package_manager", "install", "lint", "test", "build"];
  if (!isPlainObject(commands)) add("commands must be an object.");
  for (const k of requiredCommandKeys) {
    if (!isPlainObject(commands) || !Object.prototype.hasOwnProperty.call(commands, k)) add(`commands.${k} must be present (use null when not applicable).`);
  }
  if (isPlainObject(commands)) {
    try {
      normalizeRepoRelPath(commands.cwd);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      add(`commands.cwd invalid: ${msg}`);
    }
    const pm = commands.package_manager;
    if (!(pm === null || pm === "npm" || pm === "yarn" || pm === "pnpm")) add("commands.package_manager must be npm|yarn|pnpm|null.");
    for (const k of ["install", "lint", "test", "build"]) {
      const v = commands[k];
      if (!(v === null || typeof v === "string")) add(`commands.${k} must be string|null.`);
      if (typeof v === "string") {
        if (looksLikeAbsolutePathInCommand(v)) add(`commands.${k} must not include absolute filesystem paths; use commands.cwd + a repo-relative command.`);
      }
    }
  }

  // risk
  const risk = plan.risk;
  if (!isPlainObject(risk)) add("risk must be an object.");
  if (isPlainObject(risk)) {
    if (!(risk.level === "low" || risk.level === "normal" || risk.level === "high")) add("risk.level must be low|normal|high.");
    if (typeof risk.notes !== "string") add("risk.notes must be a string.");
  }

  // constraints
  const constraints = plan.constraints;
  const requiredConstraintKeys = ["no_branch_create", "requires_training", "hexa_authoring_mode", "blockly_compat_required"];
  if (!isPlainObject(constraints)) add("constraints must be an object.");
  for (const k of requiredConstraintKeys) {
    if (!isPlainObject(constraints) || !Object.prototype.hasOwnProperty.call(constraints, k)) add(`constraints.${k} must be present (use null when not applicable).`);
  }
  if (isPlainObject(constraints)) {
    if (typeof constraints.no_branch_create !== "boolean") add("constraints.no_branch_create must be boolean.");
    if (typeof constraints.requires_training !== "boolean") add("constraints.requires_training must be boolean.");
    if (!(constraints.hexa_authoring_mode === null || constraints.hexa_authoring_mode === "recipe" || constraints.hexa_authoring_mode === "cooked_code"))
      add("constraints.hexa_authoring_mode must be recipe|cooked_code|null.");
    if (!(constraints.blockly_compat_required === null || typeof constraints.blockly_compat_required === "boolean"))
      add("constraints.blockly_compat_required must be boolean|null.");
  }

  // Hexa training constraints.
  if (plan.is_hexa === true && isPlainObject(constraints) && constraints.requires_training === true) {
    if (!(constraints.hexa_authoring_mode === "recipe" || constraints.hexa_authoring_mode === "cooked_code")) {
      add("Hexa requires_training=true: constraints.hexa_authoring_mode must be set.");
    }
    const policyRecipe = isPlainObject(policy) && isPlainObject(policy.hexa) ? policy.hexa.recipe_policy : null;
    const allowedModes = isPlainObject(policyRecipe) && Array.isArray(policyRecipe.authoring_modes_allowed) ? policyRecipe.authoring_modes_allowed : [];
    const blocklyAllowed = allowedModes.map((x) => String(x)).includes("blockly");
    const fallbackCooked = !!(isPlainObject(policyRecipe) && policyRecipe.fallback_to_cooked_code_when_blockly_incompatible);
    if (constraints.hexa_authoring_mode === "recipe" && blocklyAllowed && fallbackCooked) {
      if (constraints.blockly_compat_required !== true) {
        add("Hexa recipe mode requires blockly_compat_required=true (per hexa.recipe_policy).");
      }
    }
  }

  const normalized = {
    ...plan,
    derived_from: isPlainObject(derived) ? derived : null,
    target_branch: isPlainObject(targetBranch)
      ? { name: String(targetBranch.name || "").trim(), source: "routing", confidence: 1 }
      : null,
    scope: {
      allowed_paths: Array.isArray(allowedPaths) ? allowedPaths.slice() : [],
      forbidden_paths: Array.isArray(forbiddenPaths) ? forbiddenPaths.slice() : [],
      allowed_ops: allowedOps.slice(),
    },
  };

  return { ok: errors.length === 0, errors, normalized };
}
