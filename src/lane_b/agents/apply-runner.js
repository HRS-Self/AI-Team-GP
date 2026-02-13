import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve, isAbsolute, relative, sep, posix as pathPosix, dirname } from "node:path";

import { loadPolicies } from "../../policy/resolve.js";
import { appendFile, ensureDir, readTextIfExists, writeText } from "../../utils/fs.js";
import { jsonStableStringify } from "../../utils/json.js";
import { appendStatusHistory } from "../../utils/status-json-history.js";
import { classifyGitApplyCheck } from "../../utils/git-apply-check.js";
import { loadRepoRegistry, resolveRepoAbsPath } from "../../utils/repo-registry.js";
import { isLegacyUnsafeName, workBranchName } from "../../utils/naming.js";
import { validatePatchPlan } from "../../validators/patch-plan-validator.js";
import { resolveStatePath } from "../../project/state-paths.js";
import { updateWorkStatus, writeGlobalStatusFromPortfolio } from "../../utils/status-writer.js";
import { assertGhReady, ghJson, addPrLabel, prNumberFromUrl } from "../../github/gh.js";
import { parseGitHubOwnerRepo } from "../../integrations/github-actions.js";
import { classifyApplyResume } from "./apply-resume.js";
import { buildAiCiContractFromPatchPlan } from "../ci/ai-ci-contract.js";
import { auditQaObligationsAgainstEditPaths } from "../qa/qa-obligations-audit.js";

function nowISO() {
  return new Date().toISOString();
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeRepoRelPathForCompare(p) {
  const s = String(p || "").trim();
  if (!s) return "";
  const replaced = s.replaceAll("\\", "/");
  const norm = pathPosix.normalize(replaced);
  if (norm === "." || norm === "./") return ".";
  return norm.startsWith("./") ? norm.slice(2) : norm;
}

function registryRepoPathForPlanCompare({ baseDir, repoPath, repoAbs }) {
  const raw = String(repoPath || "").trim();
  if (!raw) return "";
  if (isAbsolute(raw)) {
    try {
      const baseAbs = resolve(String(baseDir || "").trim());
      const rel = relative(baseAbs, repoAbs).split(sep).join("/");
      return normalizeRepoRelPathForCompare(rel);
    } catch {
      return normalizeRepoRelPathForCompare(raw);
    }
  }
  return normalizeRepoRelPathForCompare(raw);
}

function sanitizeBranchName(name) {
  const raw = String(name || "").trim();
  const replaced = raw.replaceAll(":", "-").replaceAll(" ", "-");
  const cleaned = replaced
    .replace(/[^0-9A-Za-z._/-]+/g, "-")
    .replace(/\/+/g, "/")
    .replace(/-+/g, "-")
    .replace(/^[-/]+|[-/]+$/g, "");
  return cleaned || "work";
}

async function writeAiCiContractToWorktree({ worktreeAbs, text }) {
  const targetAbs = resolve(worktreeAbs, ".github", "ai-ci.json");
  await mkdir(dirname(targetAbs), { recursive: true });
  await writeFile(targetAbs, String(text || ""), "utf8");
  return targetAbs;
}

function run(cmd, { cwd, timeoutMs = null } = {}) {
  const res = spawnSync(cmd, {
    cwd,
    shell: true,
    encoding: "utf8",
    timeout: Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0 ? Number(timeoutMs) : undefined,
  });
  const errMsg = res.error instanceof Error ? res.error.message : res.error ? String(res.error) : null;
  return {
    ok: res.status === 0,
    status: typeof res.status === "number" ? res.status : null,
    stdout: String(res.stdout || ""),
    stderr: String(res.stderr || ""),
    error: errMsg,
    timed_out: !!(res.error && String(res.error.code || "").toUpperCase() === "ETIMEDOUT"),
  };
}

function isCommandUnavailableResult(res) {
  const status = typeof res?.status === "number" ? res.status : null;
  const stderr = String(res?.stderr || "");
  if (status === 127) return true;
  if (status === 126 && stderr.toLowerCase().includes("permission denied")) return false;
  if (/command not found/i.test(stderr)) return true;
  if (/not recognized as an internal or external command/i.test(stderr)) return true;
  return false;
}

function resolveBaseRefForTargetBranch(repoAbs, branchName) {
  const b = String(branchName || "").trim();
  if (!b) return null;
  const originRef = `refs/remotes/origin/${b}`;
  if (run(`git show-ref --verify --quiet "${originRef}" && echo yes || echo no`, { cwd: repoAbs }).stdout.trim() === "yes") return `origin/${b}`;
  const localRef = `refs/heads/${b}`;
  if (run(`git show-ref --verify --quiet "${localRef}" && echo yes || echo no`, { cwd: repoAbs }).stdout.trim() === "yes") return b;
  return null;
}

function inferFailureCategory({ cmd }) {
  const c = String(cmd || "").toLowerCase();
  if (c.includes("permission denied") || c.includes("eacces")) return "permissions";
  if (c.includes("lint")) return "lint";
  if (c.includes("typecheck") || c.includes("tsc")) return "typecheck";
  if (c.includes(" test") || c.includes("npm test") || c.includes("yarn test")) return "test";
  if (c.includes("build")) return "build";
  if (c.includes("install") || c.includes("npm ci") || c.includes("pnpm install") || c.includes("yarn install")) return "missing dependency";
  if (c.includes("enoent") || c.includes("no such file") || c.includes("package.json")) return "env/config";
  return "other";
}

function normalizeLocalCheckMode(value, fallback) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "required" || v === "optional" || v === "skip") return v;
  return String(fallback || "skip").trim().toLowerCase() || "skip";
}

function appendLedger(obj) {
  return appendFile("ai/lane_b/ledger.jsonl", JSON.stringify(obj) + "\n");
}

function safeJsonParse(text) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    return { ok: false, message: err instanceof Error ? err.message : String(err) };
  }
}

async function writePrArtifact({ workId, pr }) {
  const path = `ai/lane_b/work/${workId}/PR.json`;
  const existingText = await readTextIfExists(path);
  const existing = existingText ? safeJsonParse(existingText) : { ok: false, json: null };
  const createdAt = existing.ok && typeof existing.json?.created_at === "string" ? existing.json.created_at : nowISO();

  const next = {
    version: 1,
    workId: String(workId),
    owner: typeof pr?.owner === "string" ? pr.owner : null,
    repo: typeof pr?.repo === "string" ? pr.repo : null,
    pr_number: typeof pr?.pr_number === "number" ? pr.pr_number : Number.parseInt(String(pr?.pr_number || "").trim(), 10),
    url: typeof pr?.url === "string" ? pr.url : (typeof pr?.pr_url === "string" ? pr.pr_url : null),
    head_branch: typeof pr?.head_branch === "string" ? pr.head_branch : null,
    base_branch: typeof pr?.base_branch === "string" ? pr.base_branch : null,
    created_at: createdAt,
    last_seen_at: nowISO(),
  };

  await writeText(path, JSON.stringify(next, null, 2) + "\n");
  return path;
}

function getRepoFullNameFromOrigin(repoAbs) {
  const remote = run("git remote get-url origin", { cwd: repoAbs });
  if (!remote.ok) return null;
  const parsed = parseGitHubOwnerRepo(String(remote.stdout || "").trim());
  if (!parsed) return null;
  return `${parsed.owner}/${parsed.repo}`;
}

async function appendDecisionNeeded({ workId, repoId, reason, nextAction }) {
  const path = "ai/lane_b/DECISIONS_NEEDED.md";
  const marker = `ApplyDecision:${workId}:${repoId}:${reason}`;
  const existing = (await readTextIfExists(path)) || "";
  if (existing.includes(marker)) return;

  const block = [
    "",
    `## Apply decision required (${workId})`,
    "",
    `- repo_id: \`${repoId}\``,
    `- reason: \`${reason}\``,
    "",
    "Next action:",
    "",
    `- ${String(nextAction || "(none)").trim() || "(none)"}`,
    "",
    `<!-- ${marker} -->`,
    "",
  ].join("\n");

  await writeText(path, existing.trimEnd() + "\n" + block);
}

function extractTouchedPathsFromUnifiedDiff(patchText) {
  const paths = new Set();
  for (const line of String(patchText || "").split("\n")) {
    const m = line.match(/^diff --git a\/(.+?) b\/(.+?)\s*$/);
    if (m) {
      const a = m[1];
      const b = m[2];
      if (a && a !== "/dev/null") paths.add(a);
      if (b && b !== "/dev/null") paths.add(b);
      continue;
    }
    const m2 = line.match(/^\+\+\+ b\/(.+?)\s*$/);
    if (m2 && m2[1] && m2[1] !== "/dev/null") paths.add(m2[1]);
  }
  return Array.from(paths).sort((a, b) => a.localeCompare(b));
}

async function enforceQaObligationsBeforeApply({ workId, workDir, bundle }) {
  const obligationsPath = `${workDir}/QA/obligations.json`;
  const obligationsText = await readTextIfExists(obligationsPath);
  if (!obligationsText) {
    return {
      ok: false,
      reason: "qa_obligations_missing",
      message: `QA obligations are required before apply. Missing ${obligationsPath}. Run: node src/cli.js --qa-obligations --workId ${workId}`,
    };
  }
  const parsed = safeJsonParse(obligationsText);
  if (!parsed.ok) {
    return { ok: false, reason: "qa_obligations_invalid", message: `Invalid JSON in ${obligationsPath} (${parsed.message}).` };
  }
  const obligations = parsed.json;

  const qaApprovalPath = `${workDir}/QA_APPROVAL.json`;
  const approvalText = await readTextIfExists(qaApprovalPath);
  let approval = { status: "pending", by: null, notes: null };
  if (approvalText) {
    const ap = safeJsonParse(approvalText);
    if (!ap.ok) return { ok: false, reason: "qa_approval_invalid", message: `Invalid JSON in ${qaApprovalPath} (${ap.message}).` };
    approval = ap.json || approval;
  }
  const approvalStatus = String(approval?.status || "pending").trim().toLowerCase() || "pending";

  const editPaths = [];
  const repos = Array.isArray(bundle?.repos) ? bundle.repos : [];
  for (const r of repos) {
    const planPath = String(r?.patch_plan_json_path || "").trim();
    if (!planPath) continue;
    // eslint-disable-next-line no-await-in-loop
    const planText = await readTextIfExists(planPath);
    if (!planText) continue;
    try {
      const plan = JSON.parse(planText);
      const edits = Array.isArray(plan?.edits) ? plan.edits : [];
      for (const e of edits) editPaths.push(e?.path);
    } catch {
      // ignore; patch plan validity is enforced elsewhere
    }
  }

  const audit = auditQaObligationsAgainstEditPaths({ obligations, editPaths, qaApprovalStatus: approvalStatus, qaApprovalNotes: approval?.notes });
  if (!audit.ok) {
    if (audit.missing && audit.missing.includes("qa_rejected")) {
      return { ok: false, reason: "qa_rejected", message: `Apply blocked: QA status is rejected (${qaApprovalPath}).` };
    }
    const missing = Array.isArray(audit.missing) ? audit.missing : ["unit", "integration", "e2e"];
    const waiverHint = `If you are explicitly waiving an obligation, run: node src/cli.js --qa-approve --workId ${workId} --by "<name>" --notes "waive: ${missing.join(",")}"`;
    return { ok: false, reason: "qa_obligations_unmet", message: `Apply blocked: QA obligations require ${missing.join(", ")} test edits, but no corresponding test changes were found in patch plans. Add tests to patch plans or explicitly waive. ${waiverHint}` };
  }

  return { ok: true, obligations_path: obligationsPath, ...audit };
}

function firstLines(text, count = 20) {
  return String(text || "")
    .split("\n")
    .slice(0, Math.max(1, Number.isFinite(count) ? count : 20));
}

function parseFlagsText(flagsText) {
  const acks = new Set();
  for (const line of String(flagsText || "").split("\n")) {
    const m = line.match(/^\s*HexaTrainingAck:\s*(.+?)\s*$/);
    if (m && m[1]) acks.add(String(m[1]).trim());
  }
  return { hexaTrainingAckRepos: acks };
}

function readJsonSafe(text, label) {
  try {
    return { ok: true, json: JSON.parse(String(text || "")) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { ok: false, message: `Invalid JSON in ${label} (${msg}).` };
  }
}

function readBundleJsonSafe(text, workId) {
  const parsed = readJsonSafe(text, "BUNDLE.json");
  if (!parsed.ok) return parsed;
  const obj = parsed.json;
  if (obj?.version !== 1) return { ok: false, message: "BUNDLE.json version must be 1." };
  if (obj?.work_id !== workId) return { ok: false, message: "BUNDLE.json work_id mismatch." };
  if (!Array.isArray(obj?.repos) || !obj.repos.length) return { ok: false, message: "BUNDLE.json repos[] missing/empty." };
  if (typeof obj?.bundle_hash !== "string" || !obj.bundle_hash.trim()) return { ok: false, message: "BUNDLE.json bundle_hash missing." };
  return { ok: true, bundle: obj };
}

function patchPlanPinsOrNull(bundle) {
  const inputs = bundle && typeof bundle === "object" ? bundle.inputs : null;
  const plans = inputs && Array.isArray(inputs.patch_plan_jsons) ? inputs.patch_plan_jsons : null;
  if (!plans) return null;

  const pins = [];
  for (const it of plans) {
    const path = typeof it?.path === "string" ? it.path.trim() : "";
    const sha256 = typeof it?.sha256 === "string" ? it.sha256.trim() : "";
    if (!path || !sha256) return null;
    pins.push({ path, sha256 });
  }
  return pins;
}

function ssotBundlePinsOrNull(bundle) {
  const inputs = bundle && typeof bundle === "object" ? bundle.inputs : null;
  const ssot = inputs && Array.isArray(inputs.ssot_bundle_jsons) ? inputs.ssot_bundle_jsons : null;
  if (!ssot) return null;
  const pins = [];
  for (const it of ssot) {
    const path = typeof it?.path === "string" ? it.path.trim() : "";
    const sha256 = typeof it?.sha256 === "string" ? it.sha256.trim() : "";
    if (!path || !sha256) return null;
    pins.push({ path, sha256 });
  }
  return pins;
}

async function validatePinnedPatchPlans(bundle) {
  const pins = patchPlanPinsOrNull(bundle);
  if (!pins) return { ok: false, errors: ["BUNDLE.json is missing inputs.patch_plan_jsons pins (required for apply)."], mode: "no_pins" };

  const errors = [];
  const pinned = new Map(pins.map((p) => [p.path, p.sha256]));
  const pinnedPaths = new Set(pins.map((p) => p.path));

  for (const r of Array.isArray(bundle?.repos) ? bundle.repos : []) {
    const plan = typeof r?.patch_plan_json_path === "string" ? r.patch_plan_json_path.trim() : "";
    if (plan && !pinnedPaths.has(plan)) errors.push(`Bundle patch_plan_json_path not pinned in inputs (${plan}).`);
  }

  for (const [path, expected] of pinned.entries()) {
    const text = await readTextIfExists(path);
    if (!text) {
      errors.push(`Missing pinned patch plan JSON: ${path}`);
      continue;
    }
    const actual = sha256Hex(text);
    if (actual !== expected) errors.push(`Pinned patch plan sha mismatch for ${path} (expected ${expected}, computed ${actual}).`);
  }

  return { ok: errors.length === 0, errors, mode: "pinned" };
}

async function validatePinnedSsotBundles(bundle) {
  const pins = ssotBundlePinsOrNull(bundle);
  if (!pins) return { ok: false, errors: ["BUNDLE.json is missing inputs.ssot_bundle_jsons pins (required)."], mode: "no_pins" };

  const errors = [];
  const pinned = new Map(pins.map((p) => [p.path, p.sha256]));
  const pinnedPaths = new Set(pins.map((p) => p.path));

  for (const r of Array.isArray(bundle?.repos) ? bundle.repos : []) {
    const ssotPath = typeof r?.ssot_bundle_json_path === "string" ? r.ssot_bundle_json_path.trim() : "";
    if (ssotPath && !pinnedPaths.has(ssotPath)) errors.push(`Bundle ssot_bundle_json_path not pinned in inputs (${ssotPath}).`);
  }

  for (const [path, expected] of pinned.entries()) {
    const text = await readTextIfExists(path);
    if (!text) {
      errors.push(`Missing pinned SSOT bundle JSON: ${path}`);
      continue;
    }
    const actual = sha256Hex(text);
    if (actual !== expected) errors.push(`Pinned SSOT bundle sha mismatch for ${path} (expected ${expected}, computed ${actual}).`);
  }

  return { ok: errors.length === 0, errors, mode: "pinned" };
}

async function writeFailureReport({ workId, repoId, branch, category, failing, preflight, nextAction, shouldRevisePlan }) {
  const dir = `ai/lane_b/work/${workId}/failure-reports`;
  await ensureDir(dir);
  const path = `${dir}/${repoId}.md`;
  const lines = [];
  lines.push(`# Failure report: ${repoId}`);
  lines.push("");
  lines.push(`Work item: ${workId}`);
  lines.push(`Repo: ${repoId}`);
  lines.push(`Branch: ${branch}`);
  lines.push("");
  lines.push("## Failure category");
  lines.push("");
  lines.push(`- ${category}`);
  lines.push("");
  lines.push("## Failing commands");
  lines.push("");
  for (const f of failing) lines.push(`- \`${f.cmd}\` (exit ${String(f.exit_code)})`);
  lines.push("");
  lines.push("## Top error lines");
  lines.push("");
  for (const f of failing) {
    lines.push(`### ${f.cmd}`);
    lines.push("");
    lines.push("```");
    for (const l of (f.top_error_lines || []).slice(0, 12)) lines.push(l);
    lines.push("```");
    lines.push("");
  }
  lines.push("## Preflight");
  lines.push("");
  lines.push(`- repo_root: \`${preflight.repo_root}\``);
  lines.push(`- cwd: \`${preflight.cwd}\``);
  if (preflight.ls) {
    lines.push("");
    lines.push("```");
    lines.push(String(preflight.ls).trimEnd());
    lines.push("```");
  }
  lines.push("");
  lines.push("## Proposed minimal next action");
  lines.push("");
  lines.push(String(nextAction || "(none)").trim() || "(none)");
  lines.push("");
  lines.push("## Should the patch plan be revised?");
  lines.push("");
  lines.push(`- ${shouldRevisePlan ? "yes" : "no"}`);
  lines.push("");
  await writeText(path, lines.join("\n"));
  return path;
}

async function writeApplyPreconditionFailure({ workId, title, requiredMessage, details }) {
  const dir = `ai/lane_b/work/${workId}/failure-reports`;
  await ensureDir(dir);
  const path = `${dir}/apply-preconditions.md`;
  const lines = [];
  lines.push(`# Apply precondition failed`);
  lines.push("");
  lines.push(`Work item: ${workId}`);
  lines.push("");
  lines.push(`## ${String(title || "Precondition").trim() || "Precondition"}`);
  lines.push("");
  lines.push(String(requiredMessage || "").trim() || "(missing message)");
  lines.push("");
  if (details) {
    lines.push("## Details");
    lines.push("");
    lines.push("```json");
    lines.push(JSON.stringify(details, null, 2));
    lines.push("```");
    lines.push("");
  }
  await writeText(path, lines.join("\n"));
  return path;
}

export async function runApplyPatchPlans({ repoRoot, workId, onlyRepoId = null, mode = "prepr" } = {}) {
  const workDir = `ai/lane_b/work/${workId}`;
  const stages = [];
  const applyMode = String(mode || "prepr").trim();
  const isCiFix = applyMode === "ci_fix";
  const noteStage = (stage, ok, note = null) => {
    stages.push({ stage, ok: !!ok, ...(note ? { note: String(note) } : {}) });
  };
  const blockAndReturn = async (message) => {
    const msg = String(message || "Apply blocked.").trim() || "Apply blocked.";
    try {
      if (!isCiFix) {
        await updateWorkStatus({
          workId,
          stage: "BLOCKED",
          blocked: true,
          blockingReason: msg,
          artifacts: { decisions_md: "ai/lane_b/DECISIONS_NEEDED.md" },
          note: "apply preflight refused",
        });
        await writeGlobalStatusFromPortfolio();
      }
    } catch {
      // Best-effort only (do not mask original failure).
    }
    return { ok: false, message: msg, stages };
  };

  const metaText = await readTextIfExists(`${workDir}/META.json`);
  if (!metaText) {
    noteStage("preflight.work_exists", false);
    return { ok: false, message: `Work item not found: missing ${workDir}/META.json.`, stages };
  }
  noteStage("preflight.work_exists", true);

  // GitHub CLI is mandatory for AI-only CI lane (PR creation + labeling).
  try {
    assertGhReady();
    noteStage("preflight.gh_ready", true);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const details = err && typeof err === "object" && "details" in err ? err.details : null;
    const reportPath = await writeApplyPreconditionFailure({
      workId,
      title: "GitHub CLI required",
      requiredMessage: msg,
      details,
    });
    await appendLedger({ timestamp: nowISO(), action: "apply_precondition_failed", workId, reason: "gh_required", report: reportPath });
    noteStage("preflight.gh_ready", false, msg);
    return await blockAndReturn(msg);
  }

  const proposalFailed = await readTextIfExists(`${workDir}/PROPOSAL_FAILED.json`);
  if (proposalFailed) {
    noteStage("preflight.proposal_ok", false, "proposal_failed");
    return await blockAndReturn(`Cannot apply: proposal phase FAILED (see ${workDir}/PROPOSAL_FAILED.json).`);
  }
  noteStage("preflight.proposal_ok", true);
  const statusMdText = await readTextIfExists(`${workDir}/STATUS.md`);
  if (statusMdText && statusMdText.includes('"blocking_reason": "PATCH_PLAN_INVALID"')) {
    noteStage("preflight.patch_plan_valid", false, "PATCH_PLAN_INVALID");
    return await blockAndReturn(`Cannot apply: patch plan validation FAILED (see ${workDir}/failure-reports/patch-plan-validation.md).`);
  }
  noteStage("preflight.patch_plan_valid", true);

  let ciFixPr = null;
  let gateABundleHash = null;
  if (isCiFix) {
    const prText = await readTextIfExists(`${workDir}/PR.json`);
    if (!prText) return await blockAndReturn(`Cannot proceed: missing ${workDir}/PR.json (required for ci_fix apply).`);
    const prParsed = readJsonSafe(prText, `${workDir}/PR.json`);
    if (!prParsed.ok) return await blockAndReturn(prParsed.message);
    ciFixPr = prParsed.json;
    const head = typeof ciFixPr?.head_branch === "string" ? ciFixPr.head_branch.trim() : "";
    if (!head) return await blockAndReturn(`Cannot proceed: PR.json missing head_branch (${workDir}/PR.json).`);
  } else {
    // Apply approval (PR creation permission) is mandatory for pre-PR apply/PR.
    let applyApprovalText = await readTextIfExists(`${workDir}/APPLY_APPROVAL.json`);
    let approvalLabel = "APPLY_APPROVAL.json";
    if (!applyApprovalText) {
      // Back-compat: accept legacy Gate A artifact.
      applyApprovalText = await readTextIfExists(`${workDir}/GATE_A.json`);
      approvalLabel = "GATE_A.json";
    }
    if (!applyApprovalText) {
      noteStage("apply_approval.exists", false);
      return await blockAndReturn(`Cannot proceed: missing ${workDir}/APPLY_APPROVAL.json. Run: node src/cli.js --apply-approval --workId ${workId}`);
    }
    noteStage("apply_approval.exists", true);
    const applyApprovalParsed = readJsonSafe(applyApprovalText, `${workDir}/${approvalLabel}`);
    if (!applyApprovalParsed.ok) return await blockAndReturn(applyApprovalParsed.message);
    const applyApproval = applyApprovalParsed.json;
    if (String(applyApproval?.status || "") !== "approved") {
      noteStage("apply_approval.approved", false, String(applyApproval?.status || "missing"));
      return await blockAndReturn(`Cannot proceed: apply-approval status != approved (${String(applyApproval?.status || "missing")}).`);
    }
    noteStage("apply_approval.approved", true);
    const bh = typeof applyApproval?.bundle_hash === "string" ? applyApproval.bundle_hash.trim() : "";
    if (!bh) return await blockAndReturn(`Cannot proceed: ${approvalLabel} missing bundle_hash.`);
    gateABundleHash = bh;
  }

  const bundleText = await readTextIfExists(`${workDir}/BUNDLE.json`);
  if (!bundleText) {
    noteStage("bundle.exists", false);
    return await blockAndReturn(`Missing ${workDir}/BUNDLE.json.`);
  }
  noteStage("bundle.exists", true);
  const bundleParsed = readBundleJsonSafe(bundleText, workId);
  if (!bundleParsed.ok) return await blockAndReturn(bundleParsed.message);
  const bundle = bundleParsed.bundle;

  if (!isCiFix) {
    if (String(bundle.bundle_hash || "") !== gateABundleHash) {
      noteStage("bundle.hash_match", false);
      return await blockAndReturn(`apply-approval bundle_hash does not match current bundle; refusing to apply (approval=${gateABundleHash}, bundle=${bundle.bundle_hash}).`);
    }
    noteStage("bundle.hash_match", true);
  }
  // Apply is deterministic and executes strictly from patch plan JSON. It does not read proposal prose.
  const pins = await validatePinnedPatchPlans(bundle);
  if (!pins.ok) return await blockAndReturn(`Bundle patch plan pins invalid: ${pins.errors.join(" | ")}`);
  const ssotPins = await validatePinnedSsotBundles(bundle);
  if (!ssotPins.ok) return await blockAndReturn(`Bundle SSOT pins invalid: ${ssotPins.errors.join(" | ")}`);

  if (!isCiFix) {
    const qaGate = await enforceQaObligationsBeforeApply({ workId, workDir, bundle });
    if (!qaGate.ok) {
      noteStage("qa.obligations_met", false, qaGate.reason || "qa_blocked");
      return await blockAndReturn(qaGate.message);
    }
    noteStage("qa.obligations_met", true);
  }

  const routingText = await readTextIfExists(`${workDir}/ROUTING.json`);
  if (!routingText) return await blockAndReturn(`Missing ${workDir}/ROUTING.json.`);
  let routing;
  try {
    routing = JSON.parse(routingText);
  } catch {
    return await blockAndReturn(`Invalid JSON in ${workDir}/ROUTING.json.`);
  }
  const targetBranchName = typeof routing?.target_branch?.name === "string" && routing.target_branch.name.trim() ? routing.target_branch.name.trim() : null;
  const targetBranchValid = routing?.target_branch?.valid !== false;
  if (!targetBranchName || !targetBranchValid) {
    return await blockAndReturn(`Cannot apply: routing.target_branch is missing/invalid for ${workId}.`);
  }

  const reposLoaded = await loadRepoRegistry();
  if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message, stages };
  const registry = reposLoaded.registry;
  noteStage("registry.loaded", true);

  const policiesLoaded = await loadPolicies();
  if (!policiesLoaded.ok) return { ok: false, message: policiesLoaded.message, stages };
  const policies = policiesLoaded.policies;
  noteStage("policies.loaded", true);

  const flagsText = (await readTextIfExists(`${workDir}/FLAGS.md`)) || "";
  const flags = parseFlagsText(flagsText);

  await ensureDir(`${workDir}/execution-plans`);
  await ensureDir(`${workDir}/apply-logs`);
  await ensureDir(`${workDir}/diffs`);
  await ensureDir(`${workDir}/worktrees`);

  if (!isCiFix) {
    await updateWorkStatus({
      workId,
      stage: "APPLYING",
      blocked: false,
      artifacts: {
        apply_logs_dir: `${workDir}/apply-logs/`,
        execution_plans_dir: `${workDir}/execution-plans/`,
        diffs_dir: `${workDir}/diffs/`,
        apply_status_json: `${workDir}/status.json`,
      },
      note: `apply_started target_branch=${targetBranchName}`,
    });
    await writeGlobalStatusFromPortfolio();
  }

  const statusPath = `${workDir}/status.json`;
  const statusHistoryPath = `${workDir}/status-history.json`;
  const statusText = (await readTextIfExists(statusPath)) || null;
  let statusJson = null;
  try {
    statusJson = statusText ? JSON.parse(statusText) : { workId, repos: {} };
  } catch {
    // Preserve whatever was there (even invalid JSON) before replacing.
    await appendStatusHistory({ statusPath, historyPath: statusHistoryPath });
    statusJson = { workId, repos: {} };
  }

  const workIdBranch = sanitizeBranchName(workId);
  const results = [];
  const persistStatus = async () => {
    const nextText = jsonStableStringify(statusJson, 2);
    const currentText = (await readTextIfExists(statusPath)) || null;
    if (currentText && currentText.trim() === nextText.trim()) return;
    if (currentText) await appendStatusHistory({ statusPath, historyPath: statusHistoryPath });
    await writeText(statusPath, nextText);
  };
  // Ensure status.json exists early for troubleshooting/debugging.
  await persistStatus();
  const recordFailure = async ({ repoId, branch, commit = null, status = "failed_final", reason_code, details = null, failure_report_path = null }) => {
    statusJson.repos[repoId] = {
      status,
      branch,
      commit,
      ...(bundle && typeof bundle.bundle_hash === "string" && bundle.bundle_hash.trim() ? { bundle_hash: bundle.bundle_hash.trim() } : {}),
      reason_code: reason_code || "apply_failed",
      ...(details ? { details } : {}),
      ...(failure_report_path ? { failure_report_path } : {}),
    };
    await persistStatus();
    await appendLedger({ timestamp: nowISO(), action: "apply_failed_final", workId, repo_id: repoId, branch, reason: reason_code || "apply_failed", ...(failure_report_path ? { failure_report_path } : {}) });
  };

  for (const bundleRepo of bundle.repos.slice().sort((a, b) => String(a.repo_id).localeCompare(String(b.repo_id)))) {
    const repoId = String(bundleRepo.repo_id || "").trim();
    if (onlyRepoId && repoId !== String(onlyRepoId).trim()) continue;
    noteStage(`repo.${repoId}.start`, true);
    const patchPlanJsonPath = String(bundleRepo.patch_plan_json_path || "").trim();
    let branchRequested;
    let branch;
    try {
      if (isLegacyUnsafeName(workId) || !/^W-[A-Za-z0-9_-]+$/.test(workId)) {
        // Legacy work ids may contain forbidden characters; warn but keep the system runnable without auto-renaming old work folders.
        const legacyCore = sanitizeBranchName(workId);
        branch = `ai/${legacyCore}/${repoId}`;
        branchRequested = branch;
        await appendLedger({ timestamp: nowISO(), action: "naming_legacy_workid_detected", workId, sanitized: legacyCore, repo_id: repoId, branch });
      } else {
        branch = workBranchName({ workId, repoId });
        branchRequested = branch;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      results.push({ repo_id: repoId, ok: false, message: "invalid_branch_name", error: msg });
      await recordFailure({ repoId, branch: "(invalid)", reason_code: "invalid_branch_name", details: msg });
      continue;
    }

    const repo = (registry.repos || []).find((r) => String(r?.repo_id || "").trim() === repoId) || null;
    if (!repo || String(repo.status || "").trim().toLowerCase() !== "active") {
      results.push({ repo_id: repoId, ok: false, message: "invalid_repo" });
      await recordFailure({ repoId, branch, reason_code: "invalid_repo" });
      continue;
    }
    const repoAbs = resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repo.path });
    if (!repoAbs || !existsSync(repoAbs)) {
      results.push({ repo_id: repoId, ok: false, message: `Repo path missing: ${repoAbs || "(null)"}` });
      await recordFailure({ repoId, branch, reason_code: "env/config", details: `Repo path missing: ${repoAbs || "(null)"}` });
      continue;
    }

    const baseRef = resolveBaseRefForTargetBranch(repoAbs, targetBranchName);
    if (!baseRef) {
      results.push({ repo_id: repoId, ok: false, message: "target_branch_missing", target_branch: targetBranchName });
      await recordFailure({ repoId, branch, reason_code: "env/config", details: `Target branch not found in repo: ${targetBranchName}` });
      continue;
    }

    // Idempotency / resume: use status.json as the deterministic checkpoint source.
    const prior = statusJson && isPlainObject(statusJson?.repos?.[repoId]) ? statusJson.repos[repoId] : null;
    const resume = classifyApplyResume({ statusEntry: prior, expectedBranch: branch, currentBundleHash: bundle.bundle_hash });
    if (resume.mode === "invalid") {
      const details = resume.details ? JSON.stringify(resume.details) : "";
      results.push({ repo_id: repoId, ok: false, message: "apply_state_invalid", details });
      await recordFailure({ repoId, branch, reason_code: "apply_state_invalid", details: `Cannot resume: ${resume.reason}${details ? ` ${details}` : ""}` });
      continue;
    }
    if (resume.mode === "skip") {
      noteStage(`repo.${repoId}.skip`, true, resume.reason);
      results.push({ repo_id: repoId, ok: true, skipped: true, reason: resume.reason, branch: prior?.branch || branch, commit: prior?.commit || null });
      continue;
    }

    const branchRef = `refs/heads/${branch}`;
    const branchExists = run(`git show-ref --verify --quiet "${branchRef}" && echo yes || echo no`, { cwd: repoAbs }).stdout.trim() === "yes";

    const planText = await readTextIfExists(patchPlanJsonPath);
    if (!planText) {
      results.push({ repo_id: repoId, ok: false, message: `Missing patch plan JSON: ${patchPlanJsonPath}` });
      await recordFailure({ repoId, branch, reason_code: "invalid_patch_plan", details: `Missing patch plan JSON: ${patchPlanJsonPath}` });
      continue;
    }
    const planParsed = readJsonSafe(planText, patchPlanJsonPath);
    if (!planParsed.ok) {
      results.push({ repo_id: repoId, ok: false, message: planParsed.message });
      await recordFailure({ repoId, branch, reason_code: "invalid_patch_plan", details: planParsed.message });
      continue;
    }

    // Patch Plan v1 contract: edits[].patch is the only payload. edits[].diff is forbidden.
    // (No backward compatibility: instruction-mode is not supported and must not appear.)
    const contractViolations = [];
    const rawEdits = Array.isArray(planParsed.json?.edits) ? planParsed.json.edits : [];
    rawEdits.forEach((e, i) => {
      if (!isPlainObject(e)) return;
      if (Object.prototype.hasOwnProperty.call(e, "diff")) contractViolations.push(`Patch plan contract violation: edits[${i}].diff is forbidden; use edits[${i}].patch.`);
      if (Object.prototype.hasOwnProperty.call(e, "instructions"))
        contractViolations.push(`Patch plan contract violation: edits[${i}].instructions is forbidden; use edits[${i}].patch.`);
      if (typeof e.patch !== "string" || !e.patch.trim()) contractViolations.push(`Patch plan missing edits[${i}].patch.`);
    });
    if (contractViolations.length) {
      results.push({ repo_id: repoId, ok: false, message: "invalid_patch_plan", errors: contractViolations });
      await recordFailure({ repoId, branch, reason_code: "invalid_patch_plan", details: contractViolations });
      continue;
    }

    const expectedProposalHash = typeof bundleRepo.proposal_sha256 === "string" && bundleRepo.proposal_sha256.trim() ? bundleRepo.proposal_sha256.trim() : null;
    const validated = validatePatchPlan(planParsed.json, { policy: policies, expected_proposal_hash: expectedProposalHash });
    if (!validated.ok) {
      results.push({ repo_id: repoId, ok: false, message: "invalid_patch_plan", errors: validated.errors });
      await recordFailure({ repoId, branch, reason_code: "invalid_patch_plan", details: validated.errors });
      continue;
    }
    const plan = validated.normalized;

    // AI-only CI contract is derived strictly from patch plan commands (no inference / no defaults).
    const ciContract = buildAiCiContractFromPatchPlan({ patchPlanJson: planParsed.json });
    if (!ciContract.ok) {
      const reportPath = await writeFailureReport({
        workId,
        repoId,
        branch,
        category: "ci_contract",
        failing: [{ cmd: "derive .github/ai-ci.json from patch plan commands", exit_code: 1, top_error_lines: [ciContract.message] }],
        preflight: { repo_root: repoAbs, cwd: repoAbs, ls: run("ls -la", { cwd: repoAbs }).stdout },
        nextAction: "Fix patch plan commands (must include at least one of install/lint/build/test; must include commands object), then regenerate patch plan and rerun apply.",
        shouldRevisePlan: true,
      });
      await recordFailure({ repoId, branch, reason_code: "ci_contract_invalid", details: ciContract.message, failure_report_path: reportPath });
      results.push({ repo_id: repoId, ok: false, message: "ci_contract_invalid", failure_report_path: reportPath });
      continue;
    }

    // Registry pinning (portable: repo_path must match repos[].path, not an absolute filesystem path)
    const expectedRepoPath = registryRepoPathForPlanCompare({ baseDir: registry.base_dir, repoPath: repo.path, repoAbs });
    const planRepoPath = normalizeRepoRelPathForCompare(plan.repo_path);
    if (planRepoPath !== expectedRepoPath || String(plan.team_id || "") !== String(repo.team_id || "") || String(plan.kind || "") !== String(repo.Kind || repo.kind || "") || !!plan.is_hexa !== !!repo.IsHexa) {
      results.push({ repo_id: repoId, ok: false, message: "invalid_patch_plan", reason: "metadata_mismatch" });
      await recordFailure({ repoId, branch, reason_code: "invalid_patch_plan", details: "metadata_mismatch" });
      continue;
    }

    const aiCiContractText = ciContract.text;

    // Hexa refusal gates
    if (plan.is_hexa && plan.constraints?.no_branch_create && !branchExists) {
      results.push({ repo_id: repoId, ok: false, message: "policy_refusal", reason: "no_branch_create" });
      await appendDecisionNeeded({
        workId,
        repoId,
        reason: "no_branch_create",
        nextAction: `Create the branch \`${branch}\` ahead of time (without the apply runner), or revise project policy to allow branch creation for Hexa repos, then rerun.`,
      });
      await recordFailure({ repoId, branch, status: "failed_final", reason_code: "policy_refusal", details: "no_branch_create" });
      continue;
    }
    if (plan.is_hexa && plan.constraints?.requires_training) {
      if (!flags.hexaTrainingAckRepos.has(repoId)) {
        results.push({ repo_id: repoId, ok: false, message: "missing_training_ack" });
        await appendDecisionNeeded({
          workId,
          repoId,
          reason: "missing_training_ack",
          nextAction: `Add a line \`HexaTrainingAck: ${repoId}\` to \`ai/lane_b/work/${workId}/FLAGS.md\`, then rerun apply.`,
        });
        await recordFailure({ repoId, branch, status: "failed_final", reason_code: "missing_training_ack" });
        continue;
      }
    }

    let resumePrOnly = resume.mode === "resume_pr";
    let commit = prior?.commit || null;
    let didNewCommit = false;

    await appendLedger({ timestamp: nowISO(), action: "apply_started", workId, repo_id: repoId, branch, branch_requested: branchRequested });

    // Prepare worktree (branch per repo).
    const wtList = run("git worktree list --porcelain", { cwd: repoAbs }).stdout;
    let existingWorktreeAbs = null;
    {
      let current = null;
      for (const line of String(wtList || "").split("\n")) {
        if (line.startsWith("worktree ")) current = { path: line.slice("worktree ".length).trim(), branch: null };
        else if (current && line.startsWith("branch ")) current.branch = line.slice("branch ".length).trim();
        else if (current && !line.trim()) {
          if (current.branch === branchRef && current.path) existingWorktreeAbs = current.path;
          current = null;
        }
      }
      if (current && current.branch === branchRef && current.path) existingWorktreeAbs = current.path;
    }

    let worktreeAbs = existingWorktreeAbs;
    let worktreeDir = null;
    if (!worktreeAbs) {
      worktreeDir = `${workDir}/worktrees/${repoId}`;
      let attempt = 0;
      while (existsSync(resolveStatePath(worktreeDir, { requiredRoot: true })) && attempt < 50) {
        attempt += 1;
        worktreeDir = `${workDir}/worktrees/${repoId}-${attempt}`;
      }
      await ensureDir(worktreeDir);

      worktreeAbs = resolveStatePath(worktreeDir, { requiredRoot: true });
      const wtCmd = branchExists ? `git worktree add "${worktreeAbs}" "${branch}"` : `git worktree add -b "${branch}" "${worktreeAbs}" "${baseRef}"`;
      const wtRes = run(wtCmd, { cwd: repoAbs });
      if (!wtRes.ok) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "env/config",
          failing: [{ cmd: wtCmd, exit_code: wtRes.status, top_error_lines: String(wtRes.stderr || wtRes.stdout).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: repoAbs, ls: run("ls -la", { cwd: repoAbs }).stdout },
          nextAction: "Fix git/worktree environment (permissions, existing branch conflicts), then rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, reason_code: "worktree_create_failed", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "worktree_create_failed", failure_report_path: reportPath });
        continue;
      }
    }
    if (!worktreeDir) worktreeDir = worktreeAbs;

    const applyLogPath = `${workDir}/apply-logs/${repoId}.md`;
    const executionPlanPath = `${workDir}/execution-plans/${repoId}.json`;

    if (!resumePrOnly) {
      // Apply unified diffs
      const editsWithPatch = (plan.edits || []).filter((e) => isPlainObject(e) && typeof e.patch === "string" && e.patch.trim());
      if (!editsWithPatch.length) {
        results.push({ repo_id: repoId, ok: false, message: "invalid_patch_plan", reason: "no_patches" });
        await recordFailure({ repoId, branch, reason_code: "invalid_patch_plan", details: "no_patches" });
        continue;
      }

      const expectedPaths = new Set((plan.edits || []).map((e) => String(e?.path || "").trim()).filter(Boolean));
      const combinedPatch = editsWithPatch.map((e) => String(e.patch || "").trimEnd() + "\n").join("\n");
      const touched = extractTouchedPathsFromUnifiedDiff(combinedPatch);
      const outOfScopeTouched = touched.filter((p) => !expectedPaths.has(p));
      if (outOfScopeTouched.length) {
        results.push({ repo_id: repoId, ok: false, message: "out_of_scope", paths: outOfScopeTouched });
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "scope_check (patch touches paths not listed in edits[])", exit_code: null, top_error_lines: outOfScopeTouched.slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Revise patch plan edits[] to list every touched path (or fix the patch to match edits[]); rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "out_of_scope", details: outOfScopeTouched, failure_report_path: reportPath });
        continue;
      }

      // Robust diff generation:
      // 1) Apply patch plan edits using `git apply --recount` (tolerates incorrect hunk headers).
      // 2) Generate auditing diff via `git diff` (valid hunks) and apply *that* patch deterministically.
      const rawPatchPath = `${workDir}/diffs/${repoId}.raw.patch`;
      await writeText(rawPatchPath, combinedPatch);
      const rawPatchAbs = resolveStatePath(rawPatchPath, { requiredRoot: true });

      const rawCheck = classifyGitApplyCheck({ cwd: worktreeAbs, patchFileAbs: rawPatchAbs, recount: true });
      if (!rawCheck.ok) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "patch_invalid",
          failing: [{ cmd: "classify git apply --check", exit_code: 1, top_error_lines: [rawCheck.message] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix engine execution environment and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, reason_code: "patch_invalid", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "patch_invalid", failure_report_path: reportPath });
        continue;
      }
      if (rawCheck.status === "already_applied") {
        resumePrOnly = true;
        noteStage(`repo.${repoId}.patch_already_applied`, true, "raw_patch");
      }
      if (rawCheck.status === "fails") {
        const patchHead = firstLines(await readTextIfExists(rawPatchPath), 20);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "patch_invalid",
          failing: [
            {
              cmd: `git apply --recount --check ${rawPatchPath}`,
              exit_code: rawCheck.forward?.status ?? null,
              top_error_lines: [...String(rawCheck.forward?.stderr || rawCheck.forward?.stdout || "").split("\n").slice(0, 12), "--- PATCH (first 20 lines) ---", ...patchHead],
            },
          ],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Revise the patch plan edits[].patch so it applies cleanly to the target branch; rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "patch_invalid", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "patch_invalid", failure_report_path: reportPath });
        continue;
      }

      if (resumePrOnly) {
        // Patch is already present on this branch; skip re-applying and proceed to resume PR steps.
        const head = run("git rev-parse HEAD", { cwd: worktreeAbs }).stdout.trim() || null;
        if (head) commit = head;
      } else {
        const rawApplyRes = run(`git apply --recount "${rawPatchAbs}"`, { cwd: worktreeAbs });
        if (!rawApplyRes.ok) {
          const patchHead = firstLines(await readTextIfExists(rawPatchPath), 20);
          const reportPath = await writeFailureReport({
            workId,
            repoId,
            branch,
            category: "patch_invalid",
            failing: [
              {
                cmd: `git apply --recount ${rawPatchPath}`,
                exit_code: rawApplyRes.status,
                top_error_lines: [...String(rawApplyRes.stderr || rawApplyRes.stdout).split("\n").slice(0, 12), "--- PATCH (first 20 lines) ---", ...patchHead],
              },
            ],
            preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
            nextAction: "Revise the patch plan edits[].patch so it applies cleanly; rerun apply.",
            shouldRevisePlan: true,
          });
          await recordFailure({ repoId, branch, reason_code: "patch_invalid", failure_report_path: reportPath });
          results.push({ repo_id: repoId, ok: false, message: "patch_invalid", failure_report_path: reportPath });
          continue;
        }
      }

      if (!resumePrOnly) {
        const patchPlanPatchPath = `${workDir}/diffs/${repoId}.plan.patch`;
      const diffCmd = expectedPaths.size ? `git diff --no-ext-diff -- ${Array.from(expectedPaths).map((p) => `"${p.replaceAll('"', '\\"')}"`).join(" ")}` : "git diff --no-ext-diff";
      const generatedPatch = run(diffCmd, { cwd: worktreeAbs }).stdout;
      await writeText(patchPlanPatchPath, generatedPatch);

      await appendLedger({
        timestamp: nowISO(),
        action: "diff_generated",
        workId,
        repo_id: repoId,
        branch,
        patch_path: patchPlanPatchPath,
        sha256: sha256Hex(generatedPatch),
      });

      // Revert edits (clean state) and validate/apply the generated patch (valid hunks, deterministic).
      run("git reset --hard", { cwd: worktreeAbs });
      run("git clean -fd", { cwd: worktreeAbs });

      const patchFileAbs = resolveStatePath(patchPlanPatchPath, { requiredRoot: true });
      const genCheckRes = run(`git apply --check "${patchFileAbs}"`, { cwd: worktreeAbs });
      if (!genCheckRes.ok) {
        const patchHead = firstLines(await readTextIfExists(patchPlanPatchPath), 20);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "patch_invalid",
          failing: [
            {
              cmd: `git apply --check ${patchPlanPatchPath}`,
              exit_code: genCheckRes.status,
              top_error_lines: [...String(genCheckRes.stderr || genCheckRes.stdout).split("\n").slice(0, 12), "--- PATCH (first 20 lines) ---", ...patchHead],
            },
          ],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Investigate generated patch (diff artifact) and revise patch plan edits; rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "patch_invalid", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "patch_invalid", failure_report_path: reportPath });
        continue;
      }

      const genApplyRes = run(`git apply --index "${patchFileAbs}"`, { cwd: worktreeAbs });
      if (!genApplyRes.ok) {
        const patchHead = firstLines(await readTextIfExists(patchPlanPatchPath), 20);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "patch_invalid",
          failing: [
            {
              cmd: `git apply --index ${patchPlanPatchPath}`,
              exit_code: genApplyRes.status,
              top_error_lines: [...String(genApplyRes.stderr || genApplyRes.stdout).split("\n").slice(0, 12), "--- PATCH (first 20 lines) ---", ...patchHead],
            },
          ],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Investigate generated patch (diff artifact) and revise patch plan edits; rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "patch_invalid", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "patch_invalid", failure_report_path: reportPath });
        continue;
      }

      const changed = run("git diff --name-only --cached", { cwd: worktreeAbs }).stdout.split("\n").map((l) => l.trim()).filter(Boolean);
      const extra = changed.filter((p) => !expectedPaths.has(p));
      if (extra.length) {
        results.push({ repo_id: repoId, ok: false, message: "out_of_scope", paths: extra });
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "scope_check (staged changes include extra paths)", exit_code: null, top_error_lines: extra.slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Revise patch plan edits[] to list every touched path (or fix the patch to match edits[]); rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "out_of_scope", details: extra, failure_report_path: reportPath });
        continue;
      }
      if (!changed.length) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "git diff --name-only --cached", exit_code: null, top_error_lines: ["No staged changes from patch plan."] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Verify the patch plan actually changes files, then rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "no_changes", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "no_changes", failure_report_path: reportPath });
        continue;
      }

      // Generate repo-local CI contract derived strictly from patch plan commands.
      try {
        await writeAiCiContractToWorktree({ worktreeAbs, text: aiCiContractText });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "ci_contract",
          failing: [{ cmd: "write .github/ai-ci.json", exit_code: 1, top_error_lines: String(msg).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix filesystem permissions and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, reason_code: "ci_contract_write_failed", details: msg, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_contract_write_failed", failure_report_path: reportPath });
        continue;
      }
      const addCiRes = run('git add ".github/ai-ci.json"', { cwd: worktreeAbs });
      if (!addCiRes.ok) {
        const msg = String(addCiRes.stderr || addCiRes.stdout || addCiRes.error || "").trim() || "git add failed";
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "ci_contract",
          failing: [{ cmd: "git add .github/ai-ci.json", exit_code: addCiRes.status, top_error_lines: String(msg).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix git working tree state and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, reason_code: "ci_contract_stage_failed", details: msg, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_contract_stage_failed", failure_report_path: reportPath });
        continue;
      }

      // Phase 6 invariant: do NOT run any local lint/test/build here.
      // CI (GitHub checks) is the only source of truth; use `--watchdog` (CI enabled) after apply/push.
      const executed = [];
      const skipped = [
        { check: "lint", reason: "ci_only_truth" },
        { check: "test", reason: "ci_only_truth" },
        { check: "build", reason: "ci_only_truth" },
      ];
      const failing = [];
      const cmdCwd = resolve(worktreeAbs, String(plan.commands.cwd || "."));
      await appendLedger({ timestamp: nowISO(), action: "command_plan_created", workId, repo_id: repoId, branch, commands: plan.commands, local_checks: "disabled" });

      const diffText = run("git diff --cached", { cwd: worktreeAbs }).stdout;
      await writeText(`${workDir}/diffs/${repoId}.patch`, diffText);

      const executionPlan = { repo_id: repoId, repo_path: repoAbs, commands: plan.commands, local_checks: { executed, skipped, failing }, finished_at: nowISO() };
      await writeText(executionPlanPath, JSON.stringify(executionPlan, null, 2) + "\n");

      const logLines = [];
      logLines.push(`# Apply log: ${repoId}`);
      logLines.push("");
      logLines.push(`Work item: ${workId}`);
      logLines.push(`Branch requested: ${branchRequested}`);
      logLines.push(`Branch created: ${branch}`);
      logLines.push(`Worktree: ${worktreeDir}`);
      logLines.push(`Patch plan: ${patchPlanJsonPath}`);
      logLines.push("");
      logLines.push("## Local checks");
      logLines.push("");
      logLines.push("- skipped: local lint/test/build are disabled by policy (CI-only truth).");
      logLines.push("");
      await writeText(applyLogPath, logLines.join("\n"));

      if (!isCiFix) {
        await updateWorkStatus({ workId, stage: "APPLYING", repos: { [repoId]: { applied: true } } });
        await writeGlobalStatusFromPortfolio();
      }

      // Commit staged changes (required).
      const hasChanges = run("git status --porcelain", { cwd: worktreeAbs }).stdout.trim().length > 0;
      if (!hasChanges) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "git status --porcelain", exit_code: null, top_error_lines: ["No changes to commit after applying patch plan."] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Verify the patch plan actually changes files, then rerun apply.",
          shouldRevisePlan: true,
        });
        await recordFailure({ repoId, branch, reason_code: "no_changes", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "no_changes", failure_report_path: reportPath });
        continue;
      }

      const commitMsg = `${workId}: apply patch plan (${repoId})`;
      const commitRes = run(`git commit -m "${commitMsg.replaceAll('"', '\\"')}"`, { cwd: worktreeAbs });
      if (!commitRes.ok) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: `git commit -m "${commitMsg}"`, exit_code: commitRes.status, top_error_lines: String(commitRes.stderr || commitRes.stdout).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix git commit failure (user.name/email, hooks), then rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, reason_code: "commit_failed", details: commitRes, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "commit_failed", failure_report_path: reportPath });
        continue;
      }

      commit = run("git rev-parse HEAD", { cwd: worktreeAbs }).stdout.trim() || null;
      if (!commit) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "git rev-parse HEAD", exit_code: null, top_error_lines: ["Failed to read commit SHA after commit."] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Rerun apply; if persistent, inspect the git worktree state.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, reason_code: "commit_failed", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "commit_failed", failure_report_path: reportPath });
        continue;
      }

      // Push branch to origin (required for PR/CI).
      if (plan.constraints?.no_branch_create === true) {
        const remoteRef = run(`git ls-remote --heads origin "${branch}"`, { cwd: repoAbs });
        const exists = remoteRef.ok && String(remoteRef.stdout || "").trim().length > 0;
        if (!exists) {
          await appendDecisionNeeded({
            workId,
            repoId,
            reason: "push_refused_no_branch_create",
            nextAction: `Create the remote branch \`${branch}\` ahead of time (or adjust policy), then rerun apply.`,
          });
          await recordFailure({ repoId, branch, commit, reason_code: "policy_refusal", details: "push_refused_no_branch_create" });
          results.push({ repo_id: repoId, ok: false, message: "policy_refusal", reason: "push_refused_no_branch_create" });
          continue;
        }
      }

      const pushRes = run(`git push -u origin "${branch}"`, { cwd: worktreeAbs });
      if (!pushRes.ok) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "env/config",
          failing: [{ cmd: `git push -u origin ${branch}`, exit_code: pushRes.status, top_error_lines: String(pushRes.stderr || pushRes.stdout).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix git remote credentials/permissions and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "push_failed", details: pushRes, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "push_failed", failure_report_path: reportPath });
        continue;
      }
      }
    }

    if (resumePrOnly) {
      // Resume mode: do NOT re-apply the patch plan (it may already be committed/pushed).
      // Only ensure the CI contract exists on the branch and retry PR/label steps.
      const head = run("git rev-parse HEAD", { cwd: worktreeAbs }).stdout.trim() || null;
      if (head) commit = head;

      try {
        await writeAiCiContractToWorktree({ worktreeAbs, text: aiCiContractText });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "ci_contract",
          failing: [{ cmd: "write .github/ai-ci.json", exit_code: 1, top_error_lines: String(msg).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix filesystem permissions and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "ci_contract_write_failed", details: msg, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_contract_write_failed", failure_report_path: reportPath });
        continue;
      }
      const addCiRes = run('git add ".github/ai-ci.json"', { cwd: worktreeAbs });
      if (!addCiRes.ok) {
        const msg = String(addCiRes.stderr || addCiRes.stdout || addCiRes.error || "").trim() || "git add failed";
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "ci_contract",
          failing: [{ cmd: "git add .github/ai-ci.json", exit_code: addCiRes.status, top_error_lines: String(msg).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix git working tree state and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "ci_contract_stage_failed", details: msg, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_contract_stage_failed", failure_report_path: reportPath });
        continue;
      }
      const hasChanges = run("git status --porcelain", { cwd: worktreeAbs }).stdout.trim().length > 0;
      if (hasChanges) {
        const commitMsg = `${workId}: add/update ai-ci contract (${repoId})`;
        const commitRes = run(`git commit -m "${commitMsg.replaceAll('"', '\\"')}"`, { cwd: worktreeAbs });
        if (!commitRes.ok) {
          const reportPath = await writeFailureReport({
            workId,
            repoId,
            branch,
            category: "other",
            failing: [{ cmd: `git commit -m "${commitMsg}"`, exit_code: commitRes.status, top_error_lines: String(commitRes.stderr || commitRes.stdout).split("\n").slice(0, 12) }],
            preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
            nextAction: "Fix git commit failure (user.name/email, hooks), then rerun apply.",
            shouldRevisePlan: false,
          });
          await recordFailure({ repoId, branch, commit, reason_code: "commit_failed", details: commitRes, failure_report_path: reportPath });
          results.push({ repo_id: repoId, ok: false, message: "commit_failed", failure_report_path: reportPath });
          continue;
        }
        commit = run("git rev-parse HEAD", { cwd: worktreeAbs }).stdout.trim() || commit;
        didNewCommit = true;
      }
      if (didNewCommit) {
        const pushRes = run(`git push -u origin "${branch}"`, { cwd: worktreeAbs });
        if (!pushRes.ok) {
          const reportPath = await writeFailureReport({
            workId,
            repoId,
            branch,
            category: "env/config",
            failing: [{ cmd: `git push -u origin ${branch}`, exit_code: pushRes.status, top_error_lines: String(pushRes.stderr || pushRes.stdout).split("\n").slice(0, 12) }],
            preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
            nextAction: "Fix git remote credentials/permissions and rerun apply.",
            shouldRevisePlan: false,
          });
          await recordFailure({ repoId, branch, commit, reason_code: "push_failed", details: pushRes, failure_report_path: reportPath });
          results.push({ repo_id: repoId, ok: false, message: "push_failed", failure_report_path: reportPath });
          continue;
        }
      }
    }

    // PR handling (AI-only CI lane trigger).
    let prUrl = null;
    let prNumber = null;
    let repoFullName = getRepoFullNameFromOrigin(repoAbs);
    if (!repoFullName) {
      try {
        const rv = ghJson(["repo", "view", "--json", "nameWithOwner"], { cwd: worktreeAbs, label: "gh repo view --json nameWithOwner" });
        repoFullName = typeof rv?.nameWithOwner === "string" ? rv.nameWithOwner.trim() : null;
      } catch {
        repoFullName = null;
      }
    }
    if (!repoFullName) {
      const reportPath = await writeFailureReport({
        workId,
        repoId,
        branch,
        category: "env/config",
        failing: [{ cmd: "resolve repo owner/name (origin or gh repo view)", exit_code: 1, top_error_lines: ["Cannot determine repo owner/name (required for PR)."] }],
        preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
        nextAction: "Ensure origin points to github.com and gh is authenticated, then rerun apply.",
        shouldRevisePlan: false,
      });
      await recordFailure({ repoId, branch, commit, reason_code: "pr_create_failed", failure_report_path: reportPath });
      results.push({ repo_id: repoId, ok: false, message: "pr_create_failed", failure_report_path: reportPath });
      continue;
    }

    if (isCiFix) {
      const expectedHead = typeof ciFixPr?.head_branch === "string" ? ciFixPr.head_branch.trim() : "";
      const expectedOwner = typeof ciFixPr?.owner === "string" ? ciFixPr.owner.trim() : "";
      const expectedRepo = typeof ciFixPr?.repo === "string" ? ciFixPr.repo.trim() : "";
      const expectedFull = expectedOwner && expectedRepo ? `${expectedOwner}/${expectedRepo}` : "";
      if (expectedHead && branch !== expectedHead) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "ci_fix preflight", exit_code: 1, top_error_lines: [`Expected head_branch=${expectedHead}`, `Got branch=${branch}`] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Ensure PR.json.head_branch matches the work branch and rerun.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "ci_fix_branch_mismatch", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_fix_branch_mismatch", failure_report_path: reportPath });
        continue;
      }
      if (expectedFull && repoFullName !== expectedFull) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "ci_fix preflight", exit_code: 1, top_error_lines: [`Expected repo=${expectedFull}`, `Got repo=${repoFullName}`] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Ensure PR.json owner/repo matches the repo being fixed and rerun.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "ci_fix_repo_mismatch", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_fix_repo_mismatch", failure_report_path: reportPath });
        continue;
      }

      const expectedPrNumber = typeof ciFixPr?.pr_number === "number" ? ciFixPr.pr_number : Number.parseInt(String(ciFixPr?.pr_number || "").trim(), 10);
      if (!Number.isFinite(expectedPrNumber) || expectedPrNumber <= 0) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "ci_fix preflight", exit_code: 1, top_error_lines: ["PR.json missing/invalid pr_number."] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Fix PR.json and rerun.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "ci_fix_pr_missing", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_fix_pr_missing", failure_report_path: reportPath });
        continue;
      }

      try {
        const view = ghJson(["pr", "view", String(expectedPrNumber), "--repo", repoFullName, "--json", "number,url,state,headRefName"], {
          cwd: worktreeAbs,
          label: "gh pr view --json number,url,state,headRefName",
        });
        const state = typeof view?.state === "string" ? view.state.trim().toLowerCase() : "";
        const headRefName = typeof view?.headRefName === "string" ? view.headRefName.trim() : "";
        if (state && state !== "open") throw new Error(`PR state is not open: ${state}`);
        if (expectedHead && headRefName && headRefName !== expectedHead) throw new Error(`PR head branch mismatch: expected ${expectedHead}, got ${headRefName}`);
        prUrl = typeof view?.url === "string" ? view.url : (typeof ciFixPr?.url === "string" ? ciFixPr.url : null);
        prNumber = typeof view?.number === "number" ? view.number : expectedPrNumber;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: `gh pr view ${expectedPrNumber} --repo ${repoFullName}`, exit_code: 1, top_error_lines: String(msg).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Ensure the original PR is still open and accessible (ci-fix must not create a new PR).",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "ci_fix_pr_view_failed", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "ci_fix_pr_view_failed", failure_report_path: reportPath });
        continue;
      }
    } else {
      // Reuse existing PR for the branch if present; otherwise create a new PR.
      try {
        const listed = ghJson(["pr", "list", "--repo", repoFullName, "--state", "open", "--head", branch, "--json", "number,url", "--limit", "1"], { cwd: worktreeAbs, label: "gh pr list" });
        const first = Array.isArray(listed) && listed.length ? listed[0] : null;
        if (first && typeof first.url === "string") prUrl = first.url;
        if (first && typeof first.number === "number") prNumber = first.number;
      } catch {
        // ignore; create below
      }

      if (!prUrl) {
        const title = `${workId}: ${repoId}`;
        const body = [`Work: ${workId}`, `Repo: ${repoId}`, `Bundle: ${bundle.bundle_hash}`, "", "Generated by AI-Team engine (AI-only CI lane)."].join("\n");
        const prCreateRes = run(
          `gh pr create --repo "${repoFullName}" --base "${targetBranchName}" --head "${branch}" --title "${title.replaceAll('"', '\\"')}" --body "${body.replaceAll('"', '\\"')}"`,
          { cwd: worktreeAbs },
        );
        if (!prCreateRes.ok) {
          const msg = String(prCreateRes.stderr || prCreateRes.stdout || prCreateRes.error || "").trim() || "gh pr create failed";
          const reportPath = await writeFailureReport({
            workId,
            repoId,
            branch,
            category: "env/config",
            failing: [
              {
                cmd: `gh pr create --repo ${repoFullName} --base ${targetBranchName} --head ${branch}`,
                exit_code: prCreateRes.status,
                top_error_lines: String(msg).split("\n").slice(0, 12),
              },
            ],
            preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
            nextAction: "Fix gh authentication/permissions and rerun apply.",
            shouldRevisePlan: false,
          });
          await recordFailure({ repoId, branch, commit, reason_code: "pr_create_failed", details: prCreateRes, failure_report_path: reportPath });
          results.push({ repo_id: repoId, ok: false, message: "pr_create_failed", failure_report_path: reportPath });
          continue;
        }
      }

      if (!prUrl) {
        try {
          const view = ghJson(["pr", "view", "--repo", repoFullName, branch, "--json", "number,url"], { cwd: worktreeAbs, label: "gh pr view --json number,url" });
          prUrl = typeof view?.url === "string" ? view.url : null;
          prNumber = typeof view?.number === "number" ? view.number : prNumber;
        } catch {
          prUrl = null;
        }
      }
      if (!prUrl) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "gh pr view (resolve PR url)", exit_code: 1, top_error_lines: ["Failed to resolve PR after create/list."] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Verify PR exists and gh can access it, then rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "pr_create_failed", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "pr_create_failed", failure_report_path: reportPath });
        continue;
      }
      if (!prNumber) prNumber = prNumberFromUrl(prUrl);
      if (!prNumber) {
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "other",
          failing: [{ cmd: "parse PR number", exit_code: 1, top_error_lines: [`PR URL: ${prUrl}`, "Could not parse PR number."] }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Ensure PR URL is standard (/pull/<n>) and rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "pr_create_failed", failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "pr_create_failed", failure_report_path: reportPath });
        continue;
      }

      try {
        addPrLabel({ repo: repoFullName, prNumber, labels: ["ai-team"] });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const reportPath = await writeFailureReport({
          workId,
          repoId,
          branch,
          category: "permissions",
          failing: [{ cmd: `gh pr edit ${prNumber} --repo ${repoFullName} --add-label ai-team`, exit_code: 1, top_error_lines: String(msg).split("\n").slice(0, 12) }],
          preflight: { repo_root: repoAbs, cwd: worktreeAbs, ls: run("ls -la", { cwd: worktreeAbs }).stdout },
          nextAction: "Ensure the `ai-team` label exists and you have permission to apply labels; then rerun apply.",
          shouldRevisePlan: false,
        });
        await recordFailure({ repoId, branch, commit, reason_code: "pr_label_failed", details: msg, failure_report_path: reportPath });
        results.push({ repo_id: repoId, ok: false, message: "pr_label_failed", failure_report_path: reportPath });
        continue;
      }

      const [owner, repoName] = String(repoFullName).split("/", 2);
      const prJsonPath = await writePrArtifact({
        workId,
        pr: {
          owner,
          repo: repoName,
          pr_number: prNumber,
          url: prUrl,
          base_branch: targetBranchName,
          head_branch: branch,
        },
      });
      await appendLedger({ timestamp: nowISO(), action: "pr_created", workId, repo_id: repoId, repo_full_name: repoFullName, pr_number: prNumber, pr_url: prUrl, base: targetBranchName, head: branch, labels: ["ai-team"], pr_json: prJsonPath });
      await updateWorkStatus({ workId, stage: "APPLIED", repos: { [repoId]: { pr_created: true, pr_url: prUrl } }, note: `repo=${repoId}` });
      await updateWorkStatus({ workId, stage: "CI_PENDING", repos: { [repoId]: { ci_status: "pending" } }, note: `repo=${repoId}` });
      await writeGlobalStatusFromPortfolio();
    }

    statusJson.repos[repoId] = {
      status: "succeeded",
      branch,
      commit,
      pushed: true,
      bundle_hash: bundle.bundle_hash,
      log_path: applyLogPath,
      diff_path: `${workDir}/diffs/${repoId}.patch`,
      execution_plan_path: executionPlanPath,
      pr_json: `ai/lane_b/work/${workId}/PR.json`,
    };
    await persistStatus();

    await appendLedger({ timestamp: nowISO(), action: "apply_succeeded", workId, repo_id: repoId, branch, commit });
    results.push({ repo_id: repoId, ok: true, branch, commit, pr_url: prUrl, pr_number: prNumber, apply_log: applyLogPath, execution_plan: executionPlanPath });
    noteStage(`repo.${repoId}.done`, true);
  }

  const failedRepos = Object.entries(statusJson.repos || {})
    .filter(([, v]) => String(v?.status || "") === "failed_final")
    .map(([k, v]) => ({ repo_id: k, reason_code: String(v?.reason_code || "") }));
  const blockReasons = new Set(["policy_refusal", "missing_training_ack", "target_branch_missing"]);
  const shouldBlock = failedRepos.some((r) => blockReasons.has(r.reason_code));

  if (!isCiFix) {
    if (failedRepos.length) {
      await updateWorkStatus({
        workId,
        stage: shouldBlock ? "BLOCKED" : "FAILED",
        blocked: shouldBlock,
        blockingReason: shouldBlock ? `Apply blocked: ${failedRepos.map((r) => `${r.repo_id}:${r.reason_code || "failed"}`).join(", ")}` : null,
        artifacts: { apply_status_json: `${workDir}/status.json` },
        note: `failed_repos=${failedRepos.length}`,
      });
    } else {
      await updateWorkStatus({
        workId,
        stage: "CI_PENDING",
        blocked: false,
        artifacts: { apply_status_json: `${workDir}/status.json`, pr_json: `${workDir}/PR.json` },
        note: "apply complete; awaiting CI",
      });
    }
    await writeGlobalStatusFromPortfolio();
  }

  return { ok: failedRepos.length === 0, workId, results, stages };
}
