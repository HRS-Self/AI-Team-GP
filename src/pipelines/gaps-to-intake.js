import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { nowTs } from "../utils/id.js";
import { appendFile as appendStateFile, ensureDir, readTextIfExists, writeText } from "../utils/fs.js";
import { readProjectConfig } from "../project/project-config.js";
import { ensureLaneBDirs, loadProjectPaths } from "../paths/project-paths.js";
import { validateGaps } from "../validators/gaps-validator.js";
import { validateKnowledgeGapsFile } from "../validators/knowledge-gap-validator.js";
import { requireConfirmedSufficiencyForDelivery } from "../lane_a/knowledge/knowledge-sufficiency.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function stableIdSuffixHex12({ type, id, projectCode }) {
  return sha256Hex([String(type || ""), String(projectCode || ""), String(id || "")].join("\n")).slice(0, 12);
}

function yamlList(lines, key, arr) {
  const a = Array.isArray(arr) ? arr : [];
  lines.push(`${key}:`);
  if (!a.length) {
    lines.push("  -");
    return;
  }
  for (const v of a) lines.push(`  - ${String(v)}`);
}

function yamlScalar(v) {
  const s = String(v ?? "");
  if (!s.length) return "\"\"";
  if (/^[a-zA-Z0-9_./:-]+$/.test(s) && !s.includes("\n")) return s;
  return JSON.stringify(s);
}

function renderGapIntakeMd({ gap, projectCode }) {
  const fm = [];
  fm.push("---");
  fm.push("source_type: gap");
  fm.push(`gap_id: ${gap.gap_id}`);
  fm.push(`project_code: ${projectCode}`);
  fm.push(`impact: ${gap.impact}`);
  fm.push(`risk_level: ${gap.risk_level}`);
  yamlList(fm, "target_teams", gap.target_teams);
  if (gap.target_repos && gap.target_repos.length) yamlList(fm, "target_repos", gap.target_repos);
  else fm.push("target_repos: []");
  yamlList(fm, "ssot_refs", gap.ssot_refs);
  fm.push("---");
  fm.push("");

  const body = [];
  body.push(`# Gap: ${gap.gap_id}`);
  body.push("");
  body.push(`Title: ${gap.title}`);
  body.push("");
  body.push("## Summary");
  body.push("");
  body.push(gap.summary);
  body.push("");
  body.push("## Observed evidence");
  body.push("");
  for (const e of gap.observed_evidence) body.push(`- ${e}`);
  body.push("");
  body.push("## Recommended action");
  body.push("");
  body.push(gap.recommended_action);
  body.push("");
  body.push("## Acceptance Criteria");
  body.push("");
  for (const ac of gap.acceptance_criteria) body.push(`- ${ac}`);
  body.push("");
  body.push("## Dependencies");
  body.push("");
  body.push(`- must_run_after: ${JSON.stringify(gap.dependencies.must_run_after)}`);
  body.push(`- can_run_in_parallel_with: ${JSON.stringify(gap.dependencies.can_run_in_parallel_with)}`);
  body.push("");
  body.push("## Instruction");
  body.push("");
  if (gap.target_repos && gap.target_repos.length === 1) {
    body.push(
      `Address this gap for repo \`${gap.target_repos[0]}\` within the SSOT constraints referenced above. Do not expand scope beyond this repo without an explicit SSOT change request.`,
    );
  } else {
    body.push(
      `Address this gap for team(s) ${gap.target_teams.map((t) => `\`${t}\``).join(", ")} within the SSOT constraints referenced above. Do not expand scope beyond SSOT.`,
    );
  }
  body.push("");

  return fm.concat(body).join("\n") + "\n";
}

function renderKnowledgeGapIntakeMd({ gap, projectCode }) {
  const fm = [];
  fm.push("---");
  fm.push("source_type: gap");
  fm.push("source: lane_a_gap");
  fm.push(`gap_id: ${yamlScalar(gap.gap_id)}`);
  fm.push(`project_code: ${yamlScalar(projectCode)}`);
  fm.push(`scope: ${yamlScalar(gap.scope)}`);
  fm.push(`category: ${yamlScalar(gap.category)}`);
  fm.push(`severity: ${yamlScalar(gap.severity)}`);
  fm.push(`risk: ${yamlScalar(gap.risk)}`);
  fm.push(`repo_id: ${yamlScalar(gap.suggested_intake.repo_id)}`);
  fm.push("labels:");
  for (const l of Array.isArray(gap.suggested_intake.labels) ? gap.suggested_intake.labels : []) fm.push(`  - ${yamlScalar(l)}`);
  fm.push("---");
  fm.push("");

  const body = [];
  body.push(`# Gap: ${gap.gap_id}`);
  body.push("");
  body.push(`Title: ${gap.suggested_intake.title}`);
  body.push("");
  body.push("## Summary");
  body.push("");
  body.push(gap.summary);
  body.push("");
  body.push("## Expected");
  body.push("");
  body.push(gap.expected);
  body.push("");
  body.push("## Observed");
  body.push("");
  body.push(gap.observed);
  body.push("");
  body.push("## Evidence");
  body.push("");
  const evidence = Array.isArray(gap.evidence) ? gap.evidence : [];
  if (!evidence.length) body.push("- (none)");
  for (const e of evidence) {
    if (!e || typeof e !== "object") continue;
    if (e.type === "file") body.push(`- file: ${e.path}${e.hint ? ` (${e.hint})` : ""}`);
    if (e.type === "grep") body.push(`- grep: ${e.pattern}${Number.isFinite(Number(e.hits)) ? ` (hits=${Number(e.hits)})` : ""}`);
    if (e.type === "endpoint") body.push(`- endpoint: ${e.method} ${e.path}`);
  }
  body.push("");
  body.push("## Suggested intake body");
  body.push("");
  body.push(gap.suggested_intake.body);
  body.push("");

  return fm.concat(body).join("\n") + "\n";
}

async function loadTeamsAndReposSets() {
  const teamsText = await readTextIfExists("config/TEAMS.json");
  const reposText = await readTextIfExists("config/REPOS.json");
  if (!teamsText) return { ok: false, message: "Missing config/TEAMS.json (required for gaps validation)." };
  if (!reposText) return { ok: false, message: "Missing config/REPOS.json (required for gaps validation)." };
  let teamsJson;
  let reposJson;
  try {
    teamsJson = JSON.parse(teamsText);
  } catch {
    return { ok: false, message: "Invalid JSON in config/TEAMS.json." };
  }
  try {
    reposJson = JSON.parse(reposText);
  } catch {
    return { ok: false, message: "Invalid JSON in config/REPOS.json." };
  }
  const teams = Array.isArray(teamsJson?.teams) ? teamsJson.teams : [];
  const repos = Array.isArray(reposJson?.repos) ? reposJson.repos : [];
  return {
    ok: true,
    teamsById: new Set(teams.map((t) => String(t?.team_id || "").trim()).filter(Boolean)),
    reposById: new Set(repos.map((r) => String(r?.repo_id || "").trim()).filter(Boolean)),
  };
}

async function readProcessedSet(absPath) {
  if (!existsSync(absPath)) return new Set();
  const text = await readFile(absPath, "utf8");
  const out = new Set();
  for (const line of String(text || "").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const obj = JSON.parse(trimmed);
      const key = typeof obj?.key === "string" ? obj.key.trim() : "";
      if (key) out.add(key);
    } catch {
      // ignore
    }
  }
  return out;
}

async function writeFailureArtifact({ laneBLogsAbs, workType, timestamp, message, errors = [] }) {
  await ensureDir("ai/lane_b/logs/failures");
  const mdRel = `ai/lane_b/logs/failures/${workType}-${timestamp}.md`;
  const jsonRel = `ai/lane_b/logs/failures/${workType}-${timestamp}.error.json`;
  const lines = [];
  lines.push(`# ${workType} FAILED`);
  lines.push("");
  lines.push(`Timestamp: ${timestamp}`);
  lines.push("");
  lines.push("## Error");
  lines.push("");
  lines.push(String(message || "").trim() || "(unknown error)");
  if (errors.length) {
    lines.push("");
    lines.push("## Validation errors");
    lines.push("");
    for (const e of errors) lines.push(`- ${String(e)}`);
  }
  lines.push("");
  await writeText(mdRel, lines.join("\n"));
  await writeText(
    jsonRel,
    JSON.stringify({ ok: false, work_type: workType, timestamp, message: String(message || "").trim() || "(unknown error)", errors }, null, 2) + "\n",
  );
  return { md: join(laneBLogsAbs, "failures", `${workType}-${timestamp}.md`), json: join(laneBLogsAbs, "failures", `${workType}-${timestamp}.error.json`) };
}

function dedupeKey({ gap_id, scope, repo_id }) {
  return [String(gap_id || ""), String(scope || ""), String(repo_id || "")].join("|");
}

export async function runGapsToIntake({ impact = null, risk = null, limit = null, forceWithoutSufficiency = false, dryRun = false } = {}) {
  const timestamp = nowTs();

  const cfgRes = await readProjectConfig({ projectRoot: process.env.AI_PROJECT_ROOT });
  if (!cfgRes.ok) {
    const laneBLogsAbs = resolve(String(process.env.AI_PROJECT_ROOT || ""), "ai", "lane_b", "logs");
    const failure = await writeFailureArtifact({ laneBLogsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: cfgRes.message });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [cfgRes.message], failure: failure.md, failure_json: failure.json };
  }
  const cfg = cfgRes.config;
  const projectCode = cfg.project_code;

  const paths = await loadProjectPaths({ projectRoot: process.env.AI_PROJECT_ROOT || null });
  await ensureLaneBDirs({ projectRoot: paths.opsRootAbs });

  const sets = await loadTeamsAndReposSets();
  if (!sets.ok) {
    const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: sets.message });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [sets.message], failure: failure.md, failure_json: failure.json };
  }

  const candidates = [];

  const ssotGapsAbs = join(paths.knowledge.ssotSystemAbs, "GAPS.json");
  if (existsSync(ssotGapsAbs)) {
    let gapsJson;
    try {
      gapsJson = JSON.parse(await readFile(ssotGapsAbs, "utf8"));
    } catch {
      const msg = `Invalid JSON in ${ssotGapsAbs}`;
      const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: msg });
      return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [msg], failure: failure.md, failure_json: failure.json };
    }

    const v = validateGaps(gapsJson, { teamsById: sets.teamsById, reposById: sets.reposById, expectedProjectCode: projectCode });
    if (!v.ok) {
      const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: `GAPS.json failed validation (${ssotGapsAbs}).`, errors: v.errors });
      return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: v.errors, failure: failure.md, failure_json: failure.json };
    }
    for (const g of v.normalized.items) candidates.push({ kind: "ssot_gaps", gap: g });
  }

  const systemGapsAbs = join(paths.knowledge.ssotSystemAbs, "gaps.json");
  if (existsSync(systemGapsAbs)) {
    let j;
    try {
      j = JSON.parse(await readFile(systemGapsAbs, "utf8"));
    } catch {
      const msg = `Invalid JSON in ${systemGapsAbs}`;
      const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: msg });
      return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [msg], failure: failure.md, failure_json: failure.json };
    }
    const v = validateKnowledgeGapsFile(j);
    if (!v.ok) {
      const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: `System gaps failed validation (${systemGapsAbs}).`, errors: v.errors });
      return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: v.errors, failure: failure.md, failure_json: failure.json };
    }
    for (const g of v.normalized.gaps) candidates.push({ kind: "knowledge_gaps", gap: g });
  }

  if (!candidates.length) {
    const msg = `No gaps found.\nLooked for:\n- ${ssotGapsAbs}\n- ${systemGapsAbs}`;
    const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: msg });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [msg], failure: failure.md, failure_json: failure.json };
  }

  const filtered = candidates
    .filter((it) => {
      if (!impact) return true;
      if (it.kind === "ssot_gaps") return String(it.gap.impact) === String(impact);
      return String(it.gap.severity) === String(impact);
    })
    .filter((it) => {
      if (!risk) return true;
      if (it.kind === "ssot_gaps") return String(it.gap.risk_level) === String(risk);
      return String(it.gap.risk) === String(risk);
    })
    .slice()
    .sort((a, b) => String(a.gap.gap_id).localeCompare(String(b.gap.gap_id)));

  const limited = typeof limit === "number" && Number.isFinite(limit) ? filtered.slice(0, Math.max(0, limit)) : filtered;

  // Delivery gating: enforce versioned knowledge sufficiency for each delivery scope.
  // Rule:
  // - system scope requires system sufficiency
  // - repo scope requires repo sufficiency OR system sufficiency
  {
    const requiredScopes = new Set();
    for (const it of limited) {
      const gap = it.gap;
      if (it.kind === "knowledge_gaps") {
        const repoId = String(gap?.suggested_intake?.repo_id || "").trim();
        if (repoId) requiredScopes.add(`repo:${repoId}`);
        else requiredScopes.add("system");
        continue;
      }
      const tr = Array.isArray(gap?.target_repos) ? gap.target_repos.map((x) => String(x || "").trim()).filter(Boolean) : [];
      if (tr.length === 1) requiredScopes.add(`repo:${tr[0]}`);
      else requiredScopes.add("system");
    }
    if (!requiredScopes.size) requiredScopes.add("system");

    const scopes = Array.from(requiredScopes).sort((a, b) => a.localeCompare(b));
    for (const s of scopes) {
      // eslint-disable-next-line no-await-in-loop
      const gate = await requireConfirmedSufficiencyForDelivery({
        projectRoot: paths.opsRootAbs,
        scope: s,
        forceWithoutSufficiency,
        laneBLedgerAppend: async (ev) => {
          await appendStateFile("ai/lane_b/ledger.jsonl", JSON.stringify(ev) + "\n");
        },
      });
      if (!gate.ok) {
        const failure = await writeFailureArtifact({ laneBLogsAbs: paths.laneB.logsAbs, workType: "GAPS_TO_INTAKE", timestamp, message: gate.message });
        return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [gate.message], failure: failure.md, failure_json: failure.json };
      }
    }
  }

  const processedAbs = join(paths.laneB.cacheAbs, "processed_gaps.jsonl");
  const processed = await readProcessedSet(processedAbs);

  await ensureDir("ai/lane_b/inbox");
  await ensureDir("ai/lane_b/cache");

  const promoted = [];
  const skipped = [];

  for (const it of limited) {
    const gap = it.gap;
    const scope = it.kind === "knowledge_gaps" ? String(gap.scope || "").trim() : "system";
    const repoId = it.kind === "knowledge_gaps" ? String(gap?.suggested_intake?.repo_id || "").trim() : Array.isArray(gap.target_repos) && gap.target_repos.length === 1 ? String(gap.target_repos[0] || "").trim() : "";
    const key = dedupeKey({ gap_id: gap.gap_id, scope, repo_id: repoId });

    if (processed.has(key)) {
      skipped.push({ id: gap.gap_id, reason: "already_promoted" });
      continue;
    }

    if (it.kind === "knowledge_gaps") {
      if (!repoId || !sets.reposById.has(repoId)) {
        skipped.push({ id: gap.gap_id, reason: `unknown_repo_id:${repoId || "(missing)"}` });
        continue;
      }
    }

    const intakeBody = it.kind === "ssot_gaps" ? renderGapIntakeMd({ gap, projectCode }) : renderKnowledgeGapIntakeMd({ gap, projectCode });
    const hash12 = stableIdSuffixHex12({ type: "gap", id: gap.gap_id, projectCode });
    const intakeId = `I-${timestamp}-${hash12}`;
    const intakeRelPath = `ai/lane_b/inbox/${intakeId}.md`;

    if (dryRun) {
      promoted.push({ id: gap.gap_id, intake_file: `${intakeId}.md`, intake_path: intakeRelPath, dry_run: true });
      continue;
    }

    await writeText(intakeRelPath, intakeBody);
    const contentSha = sha256Hex(intakeBody);

    await appendStateFile(
      "ai/lane_b/cache/processed_gaps.jsonl",
      JSON.stringify({ timestamp, type: "gap", key, gap_id: gap.gap_id, scope, repo_id: repoId || null, intake_id: intakeId, intake_path: intakeRelPath, sha256: contentSha }) + "\n",
    );
    processed.add(key);

    await appendStateFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp, action: "gap_promoted", gap_id: gap.gap_id, intake_id: intakeId, scope, repo_id: repoId || null }) + "\n");

    promoted.push({ id: gap.gap_id, intake_file: `${intakeId}.md`, intake_path: intakeRelPath });
  }

  if (!dryRun && promoted.length) {
    let snapshotSha = null;
    const snapshotAbs = join(paths.knowledge.ssotSystemAbs, "PROJECT_SNAPSHOT.json");
    if (existsSync(snapshotAbs)) {
      try {
        snapshotSha = sha256Hex(await readFile(snapshotAbs, "utf8"));
      } catch {
        snapshotSha = null;
      }
    }
    const createdIntakes = promoted
      .map((p) => String(p?.intake_path || "").split("/").pop() || "")
      .map((fn) => fn.replace(/\\.md$/i, ""))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    await appendStateFile(
      "ai/lane_b/cache/knowledge_exports.jsonl",
      JSON.stringify({ type: "gap_export", created_at: timestamp, knowledge_snapshot_sha256: snapshotSha, created_intakes: createdIntakes }) + "\n",
    );
  }

  return {
    ok: true,
    promoted_count: promoted.length,
    skipped_count: skipped.length,
    promoted,
    skipped,
    errors: [],
    dry_run: dryRun,
  };
}
