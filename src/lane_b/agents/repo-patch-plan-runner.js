import { readFileSync, existsSync, rmSync, mkdirSync } from "node:fs";
import { resolve, relative, sep, posix as pathPosix, isAbsolute as isAbsFsPath } from "node:path";
import { createHash } from "node:crypto";
import { readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import { spawnSync } from "node:child_process";

import { ensureDir, readTextIfExists, writeText, appendFile } from "../../utils/fs.js";
import { appendStatusHistory } from "../../utils/status-json-history.js";
import { jsonStableStringify } from "../../utils/json.js";
import { nowTs } from "../../utils/id.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { loadPolicies } from "../../policy/resolve.js";
import { validatePatchPlan } from "../../validators/patch-plan-validator.js";
import { scanWithGitGrepInRoots, scanFallbackInRoots } from "../../utils/repo-scan.js";
import { resolveStatePath, getAIProjectRoot } from "../../project/state-paths.js";
import { agentsForTeam } from "./agent-registry.js";
import { resolveGitRefForBranch, gitShowFileAtRef, headLines } from "../../utils/git-files.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";
import { resolveSsotBundle } from "../../ssot/ssot-resolver.js";
import { buildWorkScopedPrCiContextPack } from "../ci/ci-context-pack.js";
import { assertLaneAGovernanceForWorkId } from "../lane-a-governance.js";
import { maybeAugmentLlmMessagesWithSkills } from "../../llm/prompt-augment.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function runGit({ cwd, args, input = null } = {}) {
  const res = spawnSync("git", Array.isArray(args) ? args : [], {
    cwd,
    encoding: "utf8",
    input: typeof input === "string" ? input : input === null ? undefined : String(input),
  });
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
  };
}

function hasOriginRemote(repoAbs) {
  const res = runGit({ cwd: repoAbs, args: ["remote", "get-url", "origin"] });
  return res.ok;
}

function gitFetchPruneIfPossible(repoAbs) {
  if (!hasOriginRemote(repoAbs)) return { ok: true, skipped: true };
  const res = runGit({ cwd: repoAbs, args: ["fetch", "--prune", "origin"] });
  return { ok: res.ok, skipped: false, ...res };
}

function removeGitWorktreeIfExists({ repoAbs, worktreeAbs }) {
  // Remove git's worktree registration (if any) and delete the directory.
  // Ignore errors: the directory may not be a registered worktree.
  runGit({ cwd: repoAbs, args: ["worktree", "remove", "--force", worktreeAbs] });
  if (existsSync(worktreeAbs)) rmSync(worktreeAbs, { recursive: true, force: true });
}

function addDetachedWorktreeAtRef({ repoAbs, worktreeAbs, gitRef }) {
  const wtAbs = String(worktreeAbs || "").trim();
  const ref = String(gitRef || "").trim();
  if (!wtAbs || !ref) return { ok: false, status: 1, stdout: "", stderr: "missing worktree/ref" };

  // If the worktree already exists (e.g., created by proposer for evidence), reuse it by resetting to the ref.
  const list = runGit({ cwd: repoAbs, args: ["worktree", "list", "--porcelain"] });
  const registered = list.ok && list.stdout.split("\n").some((l) => l.trim() === `worktree ${wtAbs}`);
  if (registered && existsSync(wtAbs)) {
    const resolved = runGit({ cwd: repoAbs, args: ["rev-parse", ref] });
    if (resolved.ok) {
      const sha = resolved.stdout.trim();
      const co = runGit({ cwd: wtAbs, args: ["checkout", "--detach", sha] });
      if (co.ok) {
        runGit({ cwd: wtAbs, args: ["reset", "--hard", sha] });
        runGit({ cwd: wtAbs, args: ["clean", "-fd"] });
        return { ok: true, status: 0, stdout: "reused", stderr: "" };
      }
    }
    // If reuse failed, fall through to recreate deterministically.
  }

  removeGitWorktreeIfExists({ repoAbs, worktreeAbs: wtAbs });
  rmSync(wtAbs, { recursive: true, force: true });
  // Ensure parent exists.
  const parent = resolve(wtAbs, "..");
  if (!existsSync(parent)) mkdirSync(parent, { recursive: true });
  return runGit({ cwd: repoAbs, args: ["worktree", "add", "--detach", wtAbs, ref] });
}

function applyIntentToText({ original, intent, op }) {
  const type = String(intent?.type || "").trim();
  const src = String(original ?? "");

  if (op === "delete") {
    // Delete handled by filesystem operation, not text transform.
    return { ok: true, text: src, changed: true };
  }

  if (type === "append_line") {
    const line = String(intent?.line ?? "");
    const base = src.length ? (src.endsWith("\n") ? src : `${src}\n`) : "";
    const next = `${base}${line}\n`;
    return { ok: true, text: next, changed: next !== src };
  }

  if (type === "insert_after_match") {
    const match = String(intent?.match ?? "");
    const line = String(intent?.line ?? "");
    const idx = src.indexOf(match);
    if (idx < 0) return { ok: false, error: `match not found: ${JSON.stringify(match)}` };
    // Insert after the matched line (line-based insertion).
    const lines = src.split("\n");
    const matchLineIndex = lines.findIndex((l) => l.includes(match));
    if (matchLineIndex < 0) return { ok: false, error: `match not found in any line: ${JSON.stringify(match)}` };
    lines.splice(matchLineIndex + 1, 0, line);
    const next = lines.join("\n");
    return { ok: true, text: next, changed: next !== src };
  }

  if (type === "replace_first") {
    const match = String(intent?.match ?? "");
    const replacement = String(intent?.replacement ?? "");
    const idx = src.indexOf(match);
    if (idx < 0) return { ok: false, error: `match not found: ${JSON.stringify(match)}` };
    const next = `${src.slice(0, idx)}${replacement}${src.slice(idx + match.length)}`;
    return { ok: true, text: next, changed: next !== src };
  }

  return { ok: false, error: `unsupported intent.type: ${JSON.stringify(type)}` };
}

async function deriveUnifiedPatchesFromIntents({ repoAbs, baseRef, workDir, repoId, targetBranchName, edits }) {
  const worktreesRoot = `${workDir}/worktrees/patch-plan/${repoId}`;
  const baseRel = `${worktreesRoot}/base`;
  const checkRel = `${worktreesRoot}/check`;
  const baseAbs = resolveStatePath(baseRel, { requiredRoot: true });
  const checkAbs = resolveStatePath(checkRel, { requiredRoot: true });

  await ensureDir(worktreesRoot);

  // Create detached worktrees at the exact base ref (branch HEAD).
  const baseAdd = addDetachedWorktreeAtRef({ repoAbs, worktreeAbs: baseAbs, gitRef: baseRef });
  if (!baseAdd.ok) {
    return {
      ok: false,
      reason: "worktree_add_failed",
      details: baseAdd.stderr.trim() || baseAdd.stdout.trim() || "git worktree add failed",
      baseRef,
      targetBranchName,
    };
  }
  const checkAdd = addDetachedWorktreeAtRef({ repoAbs, worktreeAbs: checkAbs, gitRef: baseRef });
  if (!checkAdd.ok) {
    removeGitWorktreeIfExists({ repoAbs, worktreeAbs: baseAbs });
    return {
      ok: false,
      reason: "worktree_add_failed",
      details: checkAdd.stderr.trim() || checkAdd.stdout.trim() || "git worktree add failed",
      baseRef,
      targetBranchName,
    };
  }

  try {
    const seenPaths = new Set();
    const computed = [];

    for (let i = 0; i < edits.length; i += 1) {
      const e = edits[i];
      if (!isPlainObject(e)) return { ok: false, reason: "invalid_edit", details: `edits[${i}] must be an object` };
      if (Object.prototype.hasOwnProperty.call(e, "diff")) return { ok: false, reason: "contract_violation", details: `edits[${i}].diff is forbidden` };
      if (Object.prototype.hasOwnProperty.call(e, "patch")) return { ok: false, reason: "contract_violation", details: `edits[${i}].patch must not be provided by LLM (computed by engine)` };

      const pathRaw = String(e.path || "").trim();
      const op = String(e.op || "").trim();
      const rationale = String(e.rationale || "").trim();
      const intent = e.intent;

      const pathNorm = normalizeRepoRelativePath(pathRaw, { allowDot: false });
      if (!pathNorm.ok) return { ok: false, reason: "invalid_path", details: `edits[${i}].path invalid: ${pathNorm.error}` };
      const pathRel = pathNorm.value;
      if (seenPaths.has(pathRel)) return { ok: false, reason: "duplicate_path", details: `Duplicate edits[].path not allowed: ${pathRel}` };
      seenPaths.add(pathRel);

      if (!(op === "edit" || op === "add" || op === "delete")) return { ok: false, reason: "invalid_op", details: `edits[${i}].op must be edit|add|delete` };
      if (!rationale) return { ok: false, reason: "invalid_rationale", details: `edits[${i}].rationale missing/empty` };
      if (!isPlainObject(intent)) return { ok: false, reason: "invalid_intent", details: `edits[${i}].intent must be an object` };

      const fileAbs = resolve(baseAbs, pathRel);
      const existed = existsSync(fileAbs);

      if (op === "add" && existed) return { ok: false, reason: "add_exists", details: `edits[${i}]: op=add but file already exists: ${pathRel}` };
      if (op === "edit" && !existed) return { ok: false, reason: "edit_missing", details: `edits[${i}]: op=edit but file does not exist: ${pathRel}` };
      if (op === "delete" && !existed) return { ok: false, reason: "delete_missing", details: `edits[${i}]: op=delete but file does not exist: ${pathRel}` };

      let originalText = "";
      if (existed && op !== "delete") {
        originalText = await readFile(fileAbs, "utf8");
      }

      if (op === "delete") {
        rmSync(fileAbs, { force: true });
      } else {
        const applied = applyIntentToText({ original: originalText, intent, op });
        if (!applied.ok) return { ok: false, reason: "intent_apply_failed", details: `edits[${i}]: ${applied.error}` };
        if (!applied.changed) return { ok: false, reason: "no_change", details: `edits[${i}]: intent produced no change for ${pathRel}` };
        await mkdir(resolve(fileAbs, ".."), { recursive: true });
        await writeFile(fileAbs, applied.text, "utf8");
      }

      const diffRes = runGit({ cwd: baseAbs, args: ["diff", "--", pathRel] });
      const patchText = diffRes.stdout;
      if (!patchText.trim()) return { ok: false, reason: "empty_diff", details: `edits[${i}]: no diff generated for ${pathRel}` };

      computed.push({ path: pathRel, op, rationale, patch: patchText });
    }

    // Validate: each derived patch applies to a fresh checkout of the same base ref.
    for (let i = 0; i < computed.length; i += 1) {
      const p = computed[i];
      const checkRes = runGit({ cwd: checkAbs, args: ["apply", "--recount", "--check", "-"], input: p.patch });
      if (!checkRes.ok) {
        return {
          ok: false,
          reason: "git_apply_check_failed",
          details: checkRes.stderr.trim() || checkRes.stdout.trim() || "git apply --check failed",
          path: p.path,
          patch_preview: String(p.patch || "")
            .split("\n")
            .slice(0, 50)
            .join("\n"),
          baseRef,
          targetBranchName,
        };
      }
    }

    return { ok: true, edits: computed, baseRef, targetBranchName };
  } finally {
    removeGitWorktreeIfExists({ repoAbs, worktreeAbs: baseAbs });
    removeGitWorktreeIfExists({ repoAbs, worktreeAbs: checkAbs });
    // Avoid leaving confusing empty directories (worktree roots are temporary by design).
    try {
      rmSync(resolve(baseAbs, ".."), { recursive: true, force: true });
    } catch {
      // ignore
    }
  }
}

function deriveSearchTerms({ intakeMd, tasksMd, proposalsMd }) {
  const allowShort = new Set(["rn", "ui", "api", "jwt", "oidc", "oauth"]);
  const stop = new Set([
    "the",
    "and",
    "or",
    "to",
    "for",
    "of",
    "in",
    "on",
    "a",
    "an",
    "is",
    "are",
    "be",
    "with",
    "by",
    "from",
    "this",
    "that",
    "it",
    "we",
    "you",
    "work",
    "item",
    "team",
    "scope",
    "tasks",
    "task",
    "notes",
    "none",
    "open",
    "questions",
    "question",
    "confirm",
    "dependencies",
    "acceptance",
    "criteria",
    "review",
    "draft",
    "identify",
    "scoped",
    "responsibility",
    "boundaries",
    "captured",
    "declared",
    "parallel",
    "run",
    "runs",
    "update",
    "change",
    "add",
    "create",
  ]);

  const cleaned = `${String(intakeMd || "")}\n${String(tasksMd || "")}\n${String(proposalsMd || "")}`
    .toLowerCase()
    .replace(/[`"'.,:;()[\]{}<>!?/\\|+=_*~-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const tokens = cleaned.split(" ").filter(Boolean);
  const counts = new Map();
  for (const t of tokens) {
    if (stop.has(t)) continue;
    if (t.length < 4 && !allowShort.has(t)) continue;
    counts.set(t, (counts.get(t) || 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([t]) => t)
    .slice(0, 12);
}

function defaultCommandsFromRegistry(repo) {
  const cmds = isPlainObject(repo?.commands) ? repo.commands : {};
  // Patch plan v1 contract: cwd must be repo-relative.
  const rawCwd = typeof cmds.cwd === "string" && cmds.cwd.trim() ? cmds.cwd.trim() : ".";
  const cwd = normalizeRepoRelativePath(rawCwd, { allowDot: true }).ok ? normalizeRepoRelativePath(rawCwd, { allowDot: true }).value : ".";
  const pm = typeof cmds.package_manager === "string" ? cmds.package_manager : null;
  return {
    cwd,
    package_manager: pm === "npm" || pm === "yarn" || pm === "pnpm" ? pm : null,
    install: typeof cmds.install === "string" ? normalizeCommandString(cmds.install) : null,
    lint: typeof cmds.lint === "string" ? normalizeCommandString(cmds.lint) : null,
    test: typeof cmds.test === "string" ? normalizeCommandString(cmds.test) : null,
    build: typeof cmds.build === "string" ? normalizeCommandString(cmds.build) : null,
  };
}

function defaultConstraints({ isHexa, policies }) {
  const hexa = isPlainObject(policies?.hexa) ? policies.hexa : {};
  const allowBranchCreate = hexa.allow_branch_create === true;
  const requiresTraining = hexa.requires_training !== false;
  return {
    // Branch creation restrictions are Hexa-only constraints.
    // Non-Hexa repos must not inherit Hexa policy defaults.
    no_branch_create: isHexa ? !allowBranchCreate : false,
    requires_training: isHexa ? requiresTraining : false,
    hexa_authoring_mode: isHexa && requiresTraining ? "recipe" : null,
    blockly_compat_required: isHexa && requiresTraining ? true : null,
  };
}

function normalizeRepoRelativePath(raw, { allowDot = true } = {}) {
  const s = typeof raw === "string" ? raw.trim() : "";
  if (!s) return { ok: true, value: "." };
  if (s.includes("\0")) return { ok: false, error: "path contains NUL byte" };
  if (s.includes("\\")) return { ok: false, error: "path must use forward slashes" };
  if (isAbsFsPath(s) || s.startsWith("/")) return { ok: false, error: "path must be repo-relative (not absolute)" };
  const norm = pathPosix.normalize(s);
  if (allowDot && (norm === "." || norm === "./")) return { ok: true, value: "." };
  if (norm === "." || norm === "./") return { ok: false, error: "path must not be '.' here" };
  if (norm === ".." || norm.startsWith("../") || norm.includes("/../")) return { ok: false, error: "path traversal is not allowed" };
  return { ok: true, value: norm };
}

function normalizeCommandString(raw) {
  const s0 = String(raw || "").trim();
  if (!s0) return null;
  // Normalize common "run command in absolute repo root" patterns into cwd-relative commands.
  // The apply runner executes these in `commands.cwd`, so prefix/cwd flags are unnecessary and often non-portable.
  let s1 = s0;
  // npm --prefix /abs/path <subcmd> ...
  s1 = s1.replace(/\bnpm\s+--prefix(?:=|\s+)\S+\s+(run\s+)?/i, (m) => (/\brun\s+$/i.test(m) ? "npm run " : "npm "));
  // yarn --cwd /abs/path <subcmd> ...
  s1 = s1.replace(/\byarn\s+--cwd(?:=|\s+)\S+\s+/i, "yarn ");
  // pnpm --dir /abs/path <subcmd> ...
  s1 = s1.replace(/\bpnpm\s+--dir(?:=|\s+)\S+\s+/i, "pnpm ");

  // Collapse repeated whitespace introduced by removals.
  s1 = s1.replace(/\s+/g, " ").trim();
  if (!s1) return null;
  if (hasAbsolutePathLikeToken(s1)) return null;
  return s1;
}

function hasAbsolutePathLikeToken(cmd) {
  const s = String(cmd || "");
  if (!s.trim()) return false;
  // Avoid flagging URLs.
  const withoutUrls = s.replace(new RegExp("https?://\\\\S+", "g"), "URL");
  const tokens = withoutUrls.split(/\s+/).filter(Boolean);
  for (const t of tokens) {
    if (t.startsWith("/")) return true;
    if (/^[A-Za-z]:\\/.test(t)) return true;
    // Common flag style: --prefix=/opt/... or path=/opt/...
    if (t.includes("=/")) return true;
  }
  return false;
}

function coercePatchPlan({ generated, meta, policies }) {
  const base = isPlainObject(generated) ? generated : {};
  const isHexa = !!meta.is_hexa;
  const warnings = [];

  const commands = isPlainObject(base.commands) ? base.commands : {};
  // Enforce contract-clean repo-relative cwd; refuse to accept absolute cwd from the LLM.
  const rawCwd = typeof commands.cwd === "string" && commands.cwd.trim() ? commands.cwd.trim() : meta.commands.cwd;
  const cwdNorm = normalizeRepoRelativePath(rawCwd, { allowDot: true });
  const cwd = cwdNorm.ok ? cwdNorm.value : meta.commands.cwd || ".";
  if (!cwdNorm.ok) warnings.push(`commands.cwd was invalid (${cwdNorm.error}); using '${cwd}'.`);

  const coerceCmd = (key) => {
    const v = commands[key];
    if (v === null) return { value: null, warned: false };
    if (typeof v === "string" && v.trim()) {
      const normalized = normalizeCommandString(v);
      if (normalized && normalized !== v.trim()) warnings.push(`commands.${key} normalized to remove non-portable cwd/prefix flags.`);
      if (normalized) return { value: normalized, warned: false };
      // Fall back to registry default if present; otherwise null.
      if (meta.commands[key]) return { value: meta.commands[key], warned: false };
      warnings.push(`commands.${key} contained an unsafe absolute path; set to null.`);
      return { value: null, warned: true };
    }
    // Prefer registry defaults for missing/invalid types.
    return { value: meta.commands[key] ?? null, warned: false };
  };

  const coercedCommands = {
    cwd,
    // Prefer repo registry defaults; do not allow the LLM to null out package_manager when the repo registry already identified it.
    package_manager:
      commands.package_manager === "npm" || commands.package_manager === "yarn" || commands.package_manager === "pnpm"
        ? commands.package_manager
        : meta.commands.package_manager,
    // Prefer repo registry commands; allow LLM to null out commands, but do not accept absolute-path command strings.
    install: coerceCmd("install").value,
    lint: coerceCmd("lint").value,
    test: coerceCmd("test").value,
    build: coerceCmd("build").value,
  };

  const scope = isPlainObject(base.scope) ? base.scope : {};
  const coercedScope = {
    allowed_paths: Array.isArray(scope.allowed_paths) ? scope.allowed_paths : ["."],
    forbidden_paths: Array.isArray(scope.forbidden_paths) ? scope.forbidden_paths : [],
    allowed_ops: Array.isArray(scope.allowed_ops) ? scope.allowed_ops : ["edit", "add"],
  };

  const risk = isPlainObject(base.risk) ? base.risk : {};
  const coercedRisk = {
    level: risk.level === "low" || risk.level === "normal" || risk.level === "high" ? risk.level : "normal",
    notes: typeof risk.notes === "string" ? risk.notes : "",
  };

  const constraints = isPlainObject(base.constraints) ? base.constraints : {};
  const defaults = defaultConstraints({ isHexa, policies });
  const coercedConstraints = {
    no_branch_create: typeof constraints.no_branch_create === "boolean" ? constraints.no_branch_create : defaults.no_branch_create,
    requires_training: typeof constraints.requires_training === "boolean" ? constraints.requires_training : defaults.requires_training,
    hexa_authoring_mode:
      constraints.hexa_authoring_mode === "recipe" || constraints.hexa_authoring_mode === "cooked_code" || constraints.hexa_authoring_mode === null
        ? constraints.hexa_authoring_mode
        : defaults.hexa_authoring_mode,
    blockly_compat_required: typeof constraints.blockly_compat_required === "boolean" || constraints.blockly_compat_required === null ? constraints.blockly_compat_required : defaults.blockly_compat_required,
  };

  return {
    version: 1,
    work_id: meta.work_id,
    repo_id: meta.repo_id,
    repo_path: meta.repo_path,
    target_branch: meta.target_branch,
    team_id: meta.team_id,
    kind: meta.kind,
    is_hexa: isHexa,
    ...(warnings.length ? { warnings } : {}),
    derived_from: meta.derived_from,
    intent_summary: typeof base.intent_summary === "string" && base.intent_summary.trim() ? base.intent_summary.trim() : meta.intent_summary,
    scope: coercedScope,
    edits: Array.isArray(base.edits) ? base.edits : [],
    commands: coercedCommands,
    risk: coercedRisk,
    constraints: coercedConstraints,
  };
}

function isSuccessProposalJson(obj, { workId, teamId }) {
  if (!isPlainObject(obj)) return { ok: false, message: "proposal must be an object" };
  if (obj.version !== 1) return { ok: false, message: "proposal.version must be 1" };
  if (String(obj.work_id || "") !== String(workId)) return { ok: false, message: "proposal.work_id mismatch" };
  if (String(obj.team_id || "") !== String(teamId)) return { ok: false, message: "proposal.team_id mismatch" };
  if (obj.status !== "SUCCESS") return { ok: false, message: "proposal.status must be SUCCESS" };
  if (typeof obj.agent_id !== "string" || !obj.agent_id.trim()) return { ok: false, message: "proposal.agent_id missing" };
  return { ok: true };
}

async function listProposalJsonFilesForTeam({ workId, teamId }) {
  const dir = `ai/lane_b/work/${workId}/proposals`;
  try {
    const entries = await readdir(resolveStatePath(dir), { withFileTypes: true });
    return entries
      .filter((e) => e.isFile() && e.name.endsWith(".json") && e.name.startsWith(`${teamId}__`))
      .map((e) => `${dir}/${e.name}`)
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

function renderPatchPlanMd(planJson) {
  const edits = Array.isArray(planJson?.edits) ? planJson.edits : [];
  const commands = planJson?.commands || {};
  const scope = planJson?.scope || {};
  const risk = planJson?.risk || {};
  const constraints = planJson?.constraints || {};
  const derived = planJson?.derived_from || {};

  return [
    "# Patch Plan (repo-scoped)",
    "",
    `- repo_id: ${planJson.repo_id}`,
    `- team_id: ${planJson.team_id}`,
    `- work_id: ${planJson.work_id}`,
    `- target_branch: ${planJson?.target_branch?.name ? String(planJson.target_branch.name) : "(missing)"}`,
    `- derived_from: ${derived && derived.proposal_id ? String(derived.proposal_id) : "(missing)"}`,
    "",
    "## Intent",
    "",
    String(planJson.intent_summary || "").trim() || "(missing)",
    "",
    "## Scope",
    "",
    `- allowed_paths: ${JSON.stringify(scope.allowed_paths || [])}`,
    `- forbidden_paths: ${JSON.stringify(scope.forbidden_paths || [])}`,
    `- allowed_ops: ${JSON.stringify(scope.allowed_ops || [])}`,
    "",
    "## Edits",
    "",
    ...(edits.length
      ? edits.map((e) => `- \`${String(e.path || "").trim()}\` (${String(e.op || "").trim()}): ${String(e.rationale || "").trim()}`)
      : ["- (none)"]),
    "",
    "## Commands",
    "",
    `- cwd: \`${String(commands.cwd || ".")}\``,
    `- package_manager: ${commands.package_manager === null ? "null" : `\`${String(commands.package_manager)}\``}`,
    `- install: ${commands.install === null ? "null" : `\`${String(commands.install)}\``}`,
    `- lint: ${commands.lint === null ? "null" : `\`${String(commands.lint)}\``}`,
    `- test: ${commands.test === null ? "null" : `\`${String(commands.test)}\``}`,
    `- build: ${commands.build === null ? "null" : `\`${String(commands.build)}\``}`,
    "",
    "## Risk",
    "",
    `- level: ${String(risk.level || "(missing)")}`,
    `- notes: ${String(risk.notes || "").trim() || "(none)"}`,
    "",
    "## Constraints",
    "",
    `- no_branch_create: ${String(constraints.no_branch_create)}`,
    `- requires_training: ${String(constraints.requires_training)}`,
    `- hexa_authoring_mode: ${constraints.hexa_authoring_mode === null ? "null" : `\`${String(constraints.hexa_authoring_mode)}\``}`,
    `- blockly_compat_required: ${constraints.blockly_compat_required === null ? "null" : String(constraints.blockly_compat_required)}`,
    "",
    "## JSON (authoritative)",
    "",
    "```json",
    jsonStableStringify(planJson, 2).trimEnd(),
    "```",
    "",
  ].join("\n");
}

async function generateRepoPatchPlanJson({ llm, systemPrompt, schemaText, userPrompt }) {
  const attempts = [];
  if (!llm) return { ok: false, attempts: [{ attempt: 0, raw: "", error: "LLM unavailable." }] };
  const baseMessages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userPrompt },
  ];
  const augmented = await maybeAugmentLlmMessagesWithSkills({
    baseMessages,
    projectRoot: process.env.AI_PROJECT_ROOT || null,
    input: {
      scope: "system",
      base_system: String(systemPrompt || ""),
      base_prompt: String(userPrompt || ""),
      context: { role: "lane_b.repo_patch_plan" },
      constraints: { output: "json_only" },
      knowledge_snippets: [],
    },
  });
  const invokeMessages = augmented.messages;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    let response;
    try {
      response = await llm.invoke(invokeMessages);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      attempts.push({ attempt, raw: "", error: `LLM invocation failed: ${msg}` });
      continue;
    }
    const raw = typeof response?.content === "string" ? response.content : String(response?.content ?? "");
    let parsed = null;
    let parseError = null;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      parseError = err instanceof Error ? err.message : String(err);
    }
    if (parsed) return { ok: true, json: parsed, raw };
    attempts.push({ attempt, raw, error: `JSON parse failed: ${parseError}. Schema:\n${schemaText.trim()}` });
  }
  return { ok: false, attempts };
}

export async function runRepoPatchPlans({ repoRoot, workId, repoIds = null, outputSuffix = "", branchNameOverride = null, extraContextPath = null } = {}) {
  let projectRoot;
  try {
    projectRoot = getAIProjectRoot({ required: true });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Cannot create repo patch plans: ${msg}` };
  }

  const workDir = `ai/lane_b/work/${workId}`;
  {
    const gov = await assertLaneAGovernanceForWorkId({ workId, phase: "repo_patch_plans" });
    if (!gov.ok) return gov;
  }
  const intakeMd = await readTextIfExists(`${workDir}/INTAKE.md`);
  const routingJson = await readTextIfExists(`${workDir}/ROUTING.json`);
  if (!intakeMd) return { ok: false, message: `Missing ${workDir}/INTAKE.md.` };
  if (!routingJson) return { ok: false, message: `Missing ${workDir}/ROUTING.json.` };
  const proposalFailed = await readTextIfExists(`${workDir}/PROPOSAL_FAILED.json`);
  if (proposalFailed) return { ok: false, message: `Cannot create repo patch plans: proposal phase FAILED (see ${workDir}/PROPOSAL_FAILED.json).` };

  const reposLoaded = await loadRepoRegistry();
  if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message };
  const registry = reposLoaded.registry;

  const policiesLoaded = await loadPolicies();
  if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message };
  const policies = policiesLoaded.policies;

  const selectedRepoIds = Array.isArray(repoIds) && repoIds.length ? repoIds.slice() : null;
  let routing;
  try {
    routing = JSON.parse(routingJson);
  } catch {
    return { ok: false, message: `Invalid JSON in ${workDir}/ROUTING.json.` };
  }
  if (routing?.needs_confirmation) {
    return { ok: false, message: "Cannot create repo patch plans: work item is BLOCKED; resolve routing decision first." };
  }
  const targetBranchName = typeof routing?.target_branch?.name === "string" && routing.target_branch.name.trim() ? routing.target_branch.name.trim() : null;
  const targetBranchValid = routing?.target_branch?.valid !== false;
  if (!targetBranchName || !targetBranchValid) {
    return { ok: false, message: "Cannot create repo patch plans: routing.target_branch is missing/invalid; rerun intake/routing and resolve decisions." };
  }

  const routingRepoIds = Array.isArray(routing?.selected_repos) ? routing.selected_repos.slice().filter(Boolean) : [];
  const reposToPlan = (selectedRepoIds || routingRepoIds).slice().sort((a, b) => String(a).localeCompare(String(b)));
  if (!reposToPlan.length) return { ok: false, message: "No repos selected in ROUTING.json (selected_repos empty)." };

  const byId = new Map((registry.repos || []).map((r) => [String(r.repo_id), r]));

  const systemPrompt = readFileSync(resolve(repoRoot, "src/llm/prompts/repo-patch-plan.system.txt"), "utf8");
  const schemaText = readFileSync(resolve(repoRoot, "src/schemas/patch-plan-intent.schema.json"), "utf8");

  const agentsText = await readTextIfExists("config/AGENTS.json");
  if (!agentsText) return { ok: false, message: "Missing config/AGENTS.json (required). Run: node src/cli.js --agents-generate" };
  try {
    const cfg = JSON.parse(agentsText);
    if (!cfg || cfg.version !== 3 || !Array.isArray(cfg.agents)) {
      return { ok: false, message: "Invalid config/AGENTS.json (expected {version:3, agents:[...]}). Run: node src/cli.js --agents-migrate" };
    }
    const hasLegacyModel = cfg.agents.some((a) => a && typeof a === "object" && Object.prototype.hasOwnProperty.call(a, "model"));
    if (hasLegacyModel) return { ok: false, message: "AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate" };
  } catch {
    return { ok: false, message: "Invalid config/AGENTS.json (must be valid JSON)." };
  }

  let createLlmClient = null;
  let llmUnavailableMsg = null;
  try {
    const mod = await import("../../llm/client.js");
    createLlmClient = mod.createLlmClient;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    createLlmClient = null;
    llmUnavailableMsg = `LLM client unavailable (${msg}).`;
  }

  const { loadLlmProfiles, resolveLlmProfileOrError } = await import("../../llm/llm-profiles.js");
  const profilesLoaded = await loadLlmProfiles();
  if (!profilesLoaded.ok) return { ok: false, message: profilesLoaded.message, ...(profilesLoaded.errors ? { errors: profilesLoaded.errors } : {}) };

  await ensureDir(`${workDir}/patch-plans`);
  await ensureDir(`${workDir}/ssot`);
  const suffixRaw = typeof outputSuffix === "string" ? outputSuffix.trim() : "";
  const suffix = suffixRaw && !suffixRaw.startsWith(".") ? `.${suffixRaw}` : suffixRaw;

  const extraContextText = extraContextPath ? await readTextIfExists(extraContextPath) : null;
  const prCiContextText = await buildWorkScopedPrCiContextPack({ workDir });

  const llmCache = new Map();
  function llmForPlannerAgent(agent) {
    if (!createLlmClient) return { llm: null, reason: llmUnavailableMsg || "LLM unavailable." };
    const profileKey = agent && typeof agent.llm_profile === "string" ? agent.llm_profile.trim() : "";
    const resolved = resolveLlmProfileOrError({ profiles: profilesLoaded.profiles, profileKey });
    if (!resolved.ok) return { llm: null, model: null, reason: resolved.message };
    const cacheKey = `profile:${resolved.profile_key}`;
    if (llmCache.has(cacheKey)) return llmCache.get(cacheKey);
    const client = createLlmClient({ ...resolved.profile });
    const v = client && client.ok ? { llm: client.llm, model: client.model, reason: null } : { llm: null, model: null, reason: client?.message || "LLM unavailable." };
    llmCache.set(cacheKey, v);
    return v;
  }

  const created = [];
  const invalid = [];
  for (const repoId of reposToPlan) {
    const repo = byId.get(String(repoId)) || null;
    if (!repo) return { ok: false, message: `Repo not found in registry: ${repoId}` };
    if (String(repo.status || "").trim().toLowerCase() !== "active") continue;

    const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repo.path });
    if (!repoAbs) return { ok: false, message: `Repo path missing for ${repoId}.` };

    const override = typeof branchNameOverride === "string" && branchNameOverride.trim() ? branchNameOverride.trim() : null;
    const planningBranchName = override || targetBranchName;

    const fetched = gitFetchPruneIfPossible(repoAbs);
    if (!fetched.ok) {
      return { ok: false, message: `Cannot create repo patch plans: git fetch failed for ${repoId}: ${(fetched.stderr || fetched.stdout || "").trim() || "fetch_failed"}` };
    }

    const branchRef = resolveGitRefForBranch(repoAbs, planningBranchName);
    if (!branchRef) {
      return { ok: false, message: `Cannot create repo patch plans: planning branch '${planningBranchName}' not found in repo ${repoId}.` };
    }

    const teamId = String(repo.team_id || "").trim();
    const kind = String(repo.Kind || repo.kind || "").trim();
    const isHexa = !!repo.IsHexa;
    if (!teamId) return { ok: false, message: `Repo ${repoId} missing team_id.` };
    if (!kind) return { ok: false, message: `Repo ${repoId} missing Kind.` };

    const planners = agentsForTeam(teamId, { role: "planner", implementation: "llm" });
    if (!planners.length) {
      return {
        ok: false,
        message: `Cannot create repo patch plans: no planner agent registered for team ${teamId}. Run: node src/cli.js --agents-generate.`,
      };
    }
    const planner = planners[0];
    const llmInfo = llmForPlannerAgent(planner);
    const llm = llmInfo.llm;

    const taskMd = await readTextIfExists(`${workDir}/tasks/${teamId}.md`);
    if (!taskMd) return { ok: false, message: `Missing ${workDir}/tasks/${teamId}.md.` };

    const ssotOut = `${workDir}/ssot/SSOT_BUNDLE.team-${teamId}.json`;
    const ssotRes = await resolveSsotBundle({ projectRoot, view: `team:${teamId}`, outPath: ssotOut, dryRun: false });
    if (!ssotRes.ok) return { ok: false, message: `Cannot create repo patch plans: SSOT resolution failed for team ${teamId}: ${ssotRes.message}` };
    const ssotText = await readTextIfExists(ssotOut);
    if (!ssotText) return { ok: false, message: `Cannot create repo patch plans: missing SSOT bundle after resolve: ${ssotOut}` };

    const proposalsDir = `${workDir}/proposals`;
    const proposalJsonPaths = await listProposalJsonFilesForTeam({ workId, teamId });
    if (!proposalJsonPaths.length) {
      return {
        ok: false,
        message: `Cannot create repo patch plans: missing SUCCESS proposal JSON for team ${teamId} (expected ${proposalsDir}/${teamId}__*.json).`,
      };
    }
    const proposalJsonPath = proposalJsonPaths[0];
    const proposalJsonText = await readTextIfExists(proposalJsonPath);
    if (!proposalJsonText) return { ok: false, message: `Cannot create repo patch plans: missing proposal JSON: ${proposalJsonPath}` };
    let proposalJson;
    try {
      proposalJson = JSON.parse(proposalJsonText);
    } catch {
      return { ok: false, message: `Cannot create repo patch plans: invalid proposal JSON: ${proposalJsonPath}` };
    }
    const proposalOk = isSuccessProposalJson(proposalJson, { workId, teamId });
    if (!proposalOk.ok) return { ok: false, message: `Cannot create repo patch plans: proposal invalid for team ${teamId}: ${proposalOk.message}` };
    const proposalHash = sha256Hex(proposalJsonText);

    // Read proposals by scanning directory listing (no glob dependency).
    let proposalFiles = [];
    try {
      proposalFiles = (await readdir(resolveStatePath(proposalsDir), { withFileTypes: true }))
        .filter((d) => d.isFile() && d.name.endsWith(".md") && d.name.startsWith(`${teamId}__`))
        .map((d) => d.name)
        .sort((a, b) => a.localeCompare(b));
    } catch {
      proposalFiles = [];
    }
    const proposalTexts = [];
    for (const name of proposalFiles.slice(0, 3)) {
      const t = await readTextIfExists(`${proposalsDir}/${name}`);
      if (t) proposalTexts.push({ path: `${proposalsDir}/${name}`, text: t });
    }

    const proposalsJoined = proposalTexts.map((p) => `--- ${p.path} ---\n${p.text.trim()}`).join("\n\n");
    const terms = deriveSearchTerms({ intakeMd, tasksMd: taskMd, proposalsMd: proposalsJoined });

    // Evidence must be anchored to the target branch ref; do not read the working tree checkout.
    const scan = scanWithGitGrepInRoots({ repoRoots: [{ repo_id: repoId, abs_path: repoAbs, exists: true, git_ref: branchRef }], terms });
    const fallback = scan.total_matches
      ? null
      : await scanFallbackInRoots({ repoRoots: [{ repo_id: repoId, abs_path: repoAbs, exists: true, git_ref: branchRef }], terms });
    const scanFinal = fallback && !scan.total_matches ? fallback : scan;

    // Branch-specific evidence for common files (read-only).
    const wantsReadme = /\breadme\b/i.test(intakeMd) || /\breadme\.md\b/i.test(intakeMd);
    let readmeAtBranch = null;
    if (wantsReadme) {
      const shown = gitShowFileAtRef(repoAbs, branchRef, "README.md");
      if (shown.ok && typeof shown.content === "string") readmeAtBranch = headLines(shown.content, 60);
    }

    const repoPathRaw = typeof repo.path === "string" && repo.path.trim() ? repo.path.trim() : "";
    let repoPathRel = repoPathRaw || repoId;
    // If registry path is absolute but under base_dir, convert to a base_dir-relative path for portability.
    if (repoPathRaw && isAbsFsPath(repoPathRaw)) {
      try {
        const baseAbs = resolve(String(registry?.base_dir || "").trim());
        repoPathRel = relative(baseAbs, repoAbs).split(sep).join("/");
      } catch {
        // fall back to the raw path (validator will reject if absolute)
        repoPathRel = repoPathRaw;
      }
    }
    const meta = {
      work_id: workId,
      repo_id: repoId,
      // Patch plan JSON must be portable: never embed absolute filesystem paths.
      // `repo_path` is the project registry path (repos[].path), relative to config/REPOS.json.base_dir.
      repo_path: repoPathRel,
      target_branch: { name: targetBranchName, source: "routing", confidence: 1 },
      team_id: teamId,
      kind,
      is_hexa: isHexa,
      commands: defaultCommandsFromRegistry(repo),
      intent_summary: `Patch plan for ${repoId} (repo-scoped).`,
      derived_from: {
        proposal_id: proposalJsonPath,
        proposal_hash: proposalHash,
        proposal_agent_id: String(proposalJson.agent_id || "").trim(),
        timestamp: nowTs(),
      },
    };

    const userPrompt = [
      `You must output a single JSON object that matches this schema exactly:\n${schemaText.trim()}`,
      "",
      "Hard constraints:",
      `- This intent is for work_id=${workId}, repo_id=${repoId}, repo_path=${repoPathRel}, team_id=${teamId}.`,
      `- Do NOT output identity fields like work_id/repo_id/repo_path/team_id/target_branch in JSON; the engine fills those.`,
      `- Planner agent: ${planner.agent_id}`,
      `- kind is ${kind}`,
      `- is_hexa is ${isHexa ? "true" : "false"}`,
      `- Target branch (from ROUTING.json): ${targetBranchName}`,
      `- Planning ref: ${branchRef}`,
      "- commands must include explicit keys with null values (never omit install/lint/test/build).",
      "- Every edits[] item must target exactly one repo-relative path and stay within scope.allowed_paths.",
      "- Do NOT output edits[].patch or unified diffs. Output edits[].intent only; the engine will compute the patch deterministically against the target branch.",
      "- Do NOT output edits[].diff or edits[].instructions (forbidden).",
      "- Each edits[] item must include an intent.type of append_line|insert_after_match|replace_first.",
      "- Do not emit multiple edits for the same file path (one edit per path).",
      "- IMPORTANT: Patch plan JSON must NOT include absolute filesystem paths anywhere. Use cwd-relative commands (e.g., `npm run lint`) and repo-relative paths only.",
      "",
      ...(prCiContextText ? [prCiContextText.trim(), ""] : []),
      ...(extraContextText
        ? [
            "Additional context (read-only):",
            "",
            `=== EXTRA_CONTEXT (${extraContextPath}) ===`,
            extraContextText.trim(),
            "",
          ]
        : []),
      "Inputs (read-only):",
      "",
      `=== SSOT_BUNDLE.team-${teamId}.json (authoritative; cite SSOT sections by id+sha256) ===`,
      ssotText.trim(),
      "",
      `=== INTAKE.md ===\n${intakeMd.trim()}`,
      "",
      `=== tasks/${teamId}.md ===\n${taskMd.trim()}`,
      "",
      `=== proposals for ${teamId} ===\n${proposalsJoined || "(none)"}`,
      "",
      "=== EVIDENCE (read-only repo scan) ===",
      `Repo root (absolute, evidence only): ${repoAbs}`,
      `Search terms: ${terms.join(", ") || "(none)"}`,
      `Matches (captured): ${scanFinal.total_matches}`,
      ...(scanFinal.hits || []).flatMap((h) => [`- ${h.path}`, ...(h.lines || []).slice(0, 6).map((l) => `  - ${l}`)]),
      ...(readmeAtBranch
        ? [
            "",
            "=== README.md at target branch (first lines) ===",
            `ref: ${branchRef}:README.md`,
            "```",
            readmeAtBranch,
            "```",
          ]
        : []),
      "",
      "Defaults you must preserve unless explicitly justified:",
      `- commands default: ${JSON.stringify(meta.commands)}`,
      `- constraints default: ${JSON.stringify(defaultConstraints({ isHexa, policies }))}`,
    ].join("\n");

    const generated = await generateRepoPatchPlanJson({ llm, systemPrompt, schemaText, userPrompt });

    if (!generated.ok) {
      await ensureDir(`${workDir}/failure-reports`);
      const reportPath = `${workDir}/failure-reports/patch-plan_${repoId}.md`;
      const lines = [];
      lines.push(`# Patch plan generation failed (LLM): ${repoId}`);
      lines.push("");
      lines.push(`workId: \`${workId}\``);
      lines.push(`team_id: \`${teamId}\``);
      lines.push(`agent_id: \`${planner.agent_id}\``);
      lines.push(`target_branch: \`${targetBranchName}\``);
      lines.push(`target_ref: \`${branchRef}\``);
      lines.push("");
      lines.push("LLM attempts:");
      lines.push("");
      for (const a of generated.attempts || []) {
        lines.push(`- attempt ${a.attempt}: ${String(a.error || "").trim() || "(no error message)"}`);
      }
      lines.push("");
      lines.push("Next action:");
      lines.push("");
      lines.push("- Fix LLM availability/output formatting and re-run `--propose --with-patch-plans`.");
      await writeText(reportPath, lines.join("\n"));

      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({
          timestamp: nowTs(),
          action: "patch_plan_invalid",
          workId,
          repo_id: repoId,
          team_id: teamId,
          agent_id: planner.agent_id,
          error_count: (generated.attempts || []).length || 1,
          first_error: (generated.attempts || [])[0]?.error || "LLM generation failed",
        }) + "\n",
      );

      invalid.push({
        repo_id: repoId,
        team_id: teamId,
        agent_id: planner.agent_id,
        errors: (generated.attempts || []).map((a) => String(a.error || "").trim()).filter(Boolean),
        preview: { failure_report_path: reportPath },
      });
      continue;
    }

    const plan = coercePatchPlan({
      generated: generated.ok ? generated.json : null,
      meta,
      policies,
    });

    // Derive unified diffs deterministically from the target branch content (no LLM hunks).
    const derived = await deriveUnifiedPatchesFromIntents({
      repoAbs,
      baseRef: branchRef,
      workDir,
      repoId,
      targetBranchName,
      edits: Array.isArray(plan.edits) ? plan.edits : [],
    });

    if (!derived.ok) {
      await ensureDir(`${workDir}/failure-reports`);
      const reportPath = `${workDir}/failure-reports/patch-plan_${repoId}.md`;
      const lines = [];
      lines.push(`# Patch plan generation failed (patch derivation): ${repoId}`);
      lines.push("");
      lines.push(`workId: \`${workId}\``);
      lines.push(`team_id: \`${teamId}\``);
      lines.push(`agent_id: \`${planner.agent_id}\``);
      lines.push(`target_branch: \`${targetBranchName}\``);
      lines.push(`target_ref: \`${branchRef}\``);
      lines.push("");
      lines.push(`Reason: \`${derived.reason || "unknown"}\``);
      lines.push("");
      lines.push("Details:");
      lines.push("");
      lines.push("```");
      lines.push(String(derived.details || "").trim() || "(none)");
      lines.push("```");
      lines.push("");
      if (derived.patch_preview) {
        lines.push("Patch (first 50 lines):");
        lines.push("");
        lines.push("```diff");
        lines.push(String(derived.patch_preview).trimEnd());
        lines.push("```");
        lines.push("");
      }
      lines.push("Next action:");
      lines.push("");
      lines.push("- Revise the patch plan intent (match strings, paths, ops) so the engine can derive a patch against the target branch.");
      await writeText(reportPath, lines.join("\n"));

      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({
          timestamp: nowTs(),
          action: "patch_plan_invalid",
          workId,
          repo_id: repoId,
          team_id: teamId,
          agent_id: planner.agent_id,
          error_count: 1,
          first_error: `${derived.reason || "patch_derivation_failed"}: ${String(derived.details || "").slice(0, 200)}`,
        }) + "\n",
      );

      invalid.push({
        repo_id: repoId,
        team_id: teamId,
        agent_id: planner.agent_id,
        errors: [`${derived.reason || "patch_derivation_failed"}: ${String(derived.details || "").trim()}`],
        preview: { failure_report_path: reportPath },
      });
      continue;
    }

    const planWithPatches = { ...plan, edits: derived.edits };
    const validated = validatePatchPlan(planWithPatches, { policy: policies });
    const jsonPath = `${workDir}/patch-plans/${repoId}${suffix}.json`;
    const mdPath = `${workDir}/patch-plans/${repoId}${suffix}.md`;

    if (!validated.ok) {
      // Fatal: do not write invalid patch plans; record for failure report.
      await ensureDir(`${workDir}/failure-reports`);
      const reportPath = `${workDir}/failure-reports/patch-plan_${repoId}.md`;
      const lines = [];
      lines.push(`# Patch plan validation failed: ${repoId}`);
      lines.push("");
      lines.push(`workId: \`${workId}\``);
      lines.push(`team_id: \`${teamId}\``);
      lines.push(`agent_id: \`${planner.agent_id}\``);
      lines.push(`target_branch: \`${targetBranchName}\``);
      lines.push("");
      lines.push("Validation errors:");
      lines.push("");
      for (const e of validated.errors) lines.push(`- ${e}`);
      lines.push("");
      lines.push("Next action:");
      lines.push("");
      lines.push("- Fix the generator output and re-run `--propose --with-patch-plans`.");
      await writeText(reportPath, lines.join("\n"));

      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({
          timestamp: nowTs(),
          action: "patch_plan_invalid",
          workId,
          repo_id: repoId,
          team_id: teamId,
          agent_id: planner.agent_id,
          error_count: validated.errors.length,
          first_error: validated.errors[0] || null,
        }) + "\n",
      );
      invalid.push({
        repo_id: repoId,
        team_id: teamId,
        agent_id: planner.agent_id,
        errors: validated.errors,
        // Include a small JSON preview for debugging (do not embed absolute paths in the patch plan JSON itself).
        preview: {
          failure_report_path: reportPath,
          repo_path: plan.repo_path,
          warnings: Array.isArray(plan.warnings) ? plan.warnings : [],
          commands: plan.commands,
          scope: plan.scope,
          edits_paths: (planWithPatches.edits || []).map((e) => String(e?.path || "").trim()).filter(Boolean),
        },
      });
      continue;
    }

    const jsonText = jsonStableStringify(validated.normalized);
    await writeText(jsonPath, jsonText);
    await writeText(mdPath, renderPatchPlanMd(validated.normalized));

    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({
        timestamp: nowTs(),
        action: "patch_plan_created",
        workId,
        repo_id: repoId,
        team_id: teamId,
        agent_id: planner.agent_id,
        output_json: jsonPath,
        output_md: mdPath,
        hash: sha256Hex(jsonText),
      }) + "\n",
    );
    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({ timestamp: nowTs(), action: "patch_plan_validated", workId, repo_id: repoId, output_json: jsonPath, ok: true }) + "\n",
    );

    created.push({ repo_id: repoId, ok: true, patch_plan_json: jsonPath, patch_plan_md: mdPath, hash: sha256Hex(jsonText) });
  }

  if (invalid.length) {
    await ensureDir(`${workDir}/failure-reports`);
    const reportPath = `${workDir}/failure-reports/patch-plan-validation.md`;
    const lines = [];
    lines.push(`# Patch plan validation failed: ${workId}`);
    lines.push("");
    lines.push(`Timestamp: ${nowTs()}`);
    lines.push("");
    lines.push("Patch plan JSONs must be contract-clean. Fix the generator outputs and re-run `--propose --with-patch-plans`.");
    lines.push("");
    for (const entry of invalid.slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)))) {
      lines.push(`## ${entry.repo_id}`);
      lines.push("");
      lines.push(`- team_id: \`${entry.team_id}\``);
      lines.push(`- agent_id: \`${entry.agent_id}\``);
      lines.push("");
      lines.push("Validation errors:");
      lines.push("");
      for (const e of entry.errors) lines.push(`- ${e}`);
      lines.push("");
      if (entry.preview) {
        lines.push("Offending values (preview):");
        lines.push("");
        lines.push("```json");
        lines.push(JSON.stringify(entry.preview, null, 2));
        lines.push("```");
        lines.push("");
      }
      lines.push("How to fix:");
      lines.push("");
      lines.push("- Ensure `commands.cwd` is repo-relative (usually `.`). Never use `/opt/...`.");
      lines.push("- Do not embed absolute filesystem paths in `commands.*`; rely on `commands.cwd` + relative commands (e.g. `npm run lint`).");
      lines.push("- Ensure all `edits[].path` are repo-relative and under `scope.allowed_paths`.");
      lines.push("");
    }
    await writeText(reportPath, lines.join("\n"));

    const statusPath = `${workDir}/status.json`;
    const statusHistoryPath = `${workDir}/status-history.json`;
    await appendStatusHistory({ statusPath, historyPath: statusHistoryPath });
    await writeText(
      statusPath,
      JSON.stringify(
        {
          workId,
          status: "failed",
          failure_stage: "PATCH_PLAN",
          blocked: true,
          blocking_reason: "PATCH_PLAN_INVALID",
          repos: Object.fromEntries(invalid.map((x) => [x.repo_id, { status: "failed", reason: "patch_plan_invalid" }])),
        },
        null,
        2,
      ) + "\n",
    );

    await updateWorkStatus({
      workId,
      stage: "FAILED",
      blocked: true,
      blockingReason: "PATCH_PLAN_INVALID",
      artifacts: {
        patch_plans_dir: `ai/lane_b/work/${workId}/patch-plans/`,
        patch_plan_validation_report: reportPath,
        work_status_json: `${workDir}/status.json`,
      },
      note: `patch_plan_invalid repos=${invalid.map((x) => x.repo_id).join(",")}`,
    });
    await writeGlobalStatusFromPortfolio();

    return { ok: false, workId, message: `Patch plan validation failed for ${invalid.length} repo(s).`, created, invalid, report: reportPath };
  }

  return { ok: true, workId, created };
}
