import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { nowTs } from "../utils/id.js";
import { writeText, readTextIfExists, ensureDir, appendFile as appendStateFile } from "../utils/fs.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { validateBacklogSeeds } from "../validators/backlog-seeds-validator.js";
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

function renderSeedIntakeMd({ seed, projectCode }) {
  const fm = [];
  fm.push("---");
  fm.push("source_type: backlog_seed");
  fm.push(`seed_id: ${seed.seed_id}`);
  fm.push(`project_code: ${projectCode}`);
  fm.push(`phase: ${seed.phase}`);
  fm.push(`priority: ${seed.priority}`);
  yamlList(fm, "target_teams", seed.target_teams);
  if (seed.target_repos && seed.target_repos.length) yamlList(fm, "target_repos", seed.target_repos);
  else fm.push("target_repos: []");
  yamlList(fm, "ssot_refs", seed.ssot_refs);
  fm.push("---");
  fm.push("");

  const body = [];
  body.push(`# Backlog Seed: ${seed.seed_id}`);
  body.push("");
  body.push(`Title: ${seed.title}`);
  body.push("");
  body.push("## Summary");
  body.push("");
  body.push(seed.summary);
  body.push("");
  body.push("## Rationale");
  body.push("");
  body.push(seed.rationale);
  body.push("");
  body.push("## Acceptance Criteria");
  body.push("");
  for (const ac of seed.acceptance_criteria) body.push(`- ${ac}`);
  body.push("");
  body.push("## Dependencies");
  body.push("");
  body.push(`- must_run_after: ${JSON.stringify(seed.dependencies.must_run_after)}`);
  body.push(`- can_run_in_parallel_with: ${JSON.stringify(seed.dependencies.can_run_in_parallel_with)}`);
  body.push("");
  body.push("## Instruction");
  body.push("");

  if (seed.target_repos && seed.target_repos.length === 1) {
    body.push(
      `Implement this seed for repo \`${seed.target_repos[0]}\` within the SSOT constraints referenced above. Do not expand scope beyond this repo without an explicit SSOT change request.`,
    );
  } else {
    body.push(
      `Implement this seed for team(s) ${seed.target_teams.map((t) => `\`${t}\``).join(", ")} within the SSOT constraints referenced above. Do not expand scope beyond SSOT.`,
    );
  }
  body.push("");

  return fm.concat(body).join("\n") + "\n";
}

async function loadTeamsAndReposSets() {
  const teamsText = await readTextIfExists("config/TEAMS.json");
  const reposText = await readTextIfExists("config/REPOS.json");
  if (!teamsText) return { ok: false, message: "Missing config/TEAMS.json (required for seeds validation)." };
  if (!reposText) return { ok: false, message: "Missing config/REPOS.json (required for seeds validation)." };
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

async function readPromotionsSet({ promotionsPathAbs }) {
  if (!existsSync(promotionsPathAbs)) return new Set();
  const text = await readFile(promotionsPathAbs, "utf8");
  const out = new Set();
  for (const line of String(text || "").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const obj = JSON.parse(trimmed);
      const type = String(obj?.type || "").trim();
      const id = String(obj?.id || "").trim();
      if (type && id) out.add(`${type}:${id}`);
    } catch {
      // ignore malformed lines (ledger is append-only; operators can fix separately)
    }
  }
  return out;
}

async function writeFailureArtifact({ workType, timestamp, message, errors = [] }) {
  await ensureDir("ai/failures");
  const path = `ai/failures/${workType}-${timestamp}.md`;
  const jsonPath = `ai/failures/${workType}-${timestamp}.error.json`;
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
  await writeText(path, lines.join("\n"));
  await writeText(
    jsonPath,
    JSON.stringify(
      {
        ok: false,
        work_type: workType,
        timestamp,
        message: String(message || "").trim() || "(unknown error)",
        errors: Array.isArray(errors) ? errors.map((e) => String(e)) : [],
      },
      null,
      2,
    ) + "\n",
  );
  return { md: path, json: jsonPath };
}

export async function runSeedsToIntake({ phase = null, limit = null, forceWithoutSufficiency = false, dryRun = false } = {}) {
  const timestamp = nowTs();

  let paths;
  try {
    paths = await loadProjectPaths();
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const failure = await writeFailureArtifact({ workType: "SEEDS_TO_INTAKE", timestamp, message: msg });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [msg], failure: failure.md, failure_json: failure.json };
  }

  const projectCode = paths.cfg.project_code;

  const sets = await loadTeamsAndReposSets();
  if (!sets.ok) {
    const failure = await writeFailureArtifact({ workType: "SEEDS_TO_INTAKE", timestamp, message: sets.message });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [sets.message], failure: failure.md, failure_json: failure.json };
  }

  const knowledgeRootAbs = resolve(paths.knowledge.rootAbs);
  const seedsPathAbs = join(knowledgeRootAbs, "ssot", "system", "BACKLOG_SEEDS.json");
  if (!existsSync(seedsPathAbs)) {
    const msg = `Missing backlog seeds file: ${seedsPathAbs}`;
    const failure = await writeFailureArtifact({ workType: "SEEDS_TO_INTAKE", timestamp, message: msg });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [msg], failure: failure.md, failure_json: failure.json };
  }

  let seedsJson;
  try {
    seedsJson = JSON.parse(await readFile(seedsPathAbs, "utf8"));
  } catch {
    const msg = `Invalid JSON in ${seedsPathAbs}`;
    const failure = await writeFailureArtifact({ workType: "SEEDS_TO_INTAKE", timestamp, message: msg });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [msg], failure: failure.md, failure_json: failure.json };
  }

  const v = validateBacklogSeeds(seedsJson, {
    teamsById: sets.teamsById,
    reposById: sets.reposById,
    expectedProjectCode: projectCode,
  });
  if (!v.ok) {
    const failure = await writeFailureArtifact({ workType: "SEEDS_TO_INTAKE", timestamp, message: `BACKLOG_SEEDS.json failed validation (${seedsPathAbs}).`, errors: v.errors });
    return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: v.errors, failure: failure.md, failure_json: failure.json };
  }

  const filtered = v.normalized.items
    .filter((it) => (phase ? Number(it.phase) === Number(phase) : true))
    .slice();

  const limited = typeof limit === "number" && Number.isFinite(limit) ? filtered.slice(0, Math.max(0, limit)) : filtered;

  // Delivery gating: enforce versioned knowledge sufficiency for each delivery scope.
  // Rule:
  // - system scope requires system sufficiency
  // - repo scope requires repo sufficiency OR system sufficiency
  {
    const requiredScopes = new Set();
    for (const seed of limited) {
      const tr = Array.isArray(seed?.target_repos) ? seed.target_repos.map((x) => String(x || "").trim()).filter(Boolean) : [];
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
        const failure = await writeFailureArtifact({ workType: "SEEDS_TO_INTAKE", timestamp, message: gate.message });
        return { ok: false, promoted_count: 0, skipped_count: 0, promoted: [], skipped: [], errors: [gate.message], failure: failure.md, failure_json: failure.json };
      }
    }
  }

  const promotionsPathAbs = resolve(paths.laneB.cacheAbs, "promotions.jsonl");
  const already = await readPromotionsSet({ promotionsPathAbs });

  await ensureDir("ai/lane_b/inbox");
  await ensureDir("ai/lane_b/cache");

  const promoted = [];
  const skipped = [];

  for (const seed of limited) {
    const key = `seed:${seed.seed_id}`;
    if (already.has(key)) {
      skipped.push({ id: seed.seed_id, reason: "already_promoted" });
      continue;
    }

    const intakeBody = renderSeedIntakeMd({ seed, projectCode });
    const hash12 = stableIdSuffixHex12({ type: "seed", id: seed.seed_id, projectCode });
    const intakeId = `I-${timestamp}-${hash12}`;
    const intakeRelPath = `ai/lane_b/inbox/${intakeId}.md`;

    if (dryRun) {
      promoted.push({ id: seed.seed_id, intake_file: `${intakeId}.md`, intake_path: intakeRelPath, dry_run: true });
      continue;
    }

    await writeText(intakeRelPath, intakeBody);
    const contentSha = sha256Hex(intakeBody);

    await appendStateFile(
      promotionsPathAbs,
      JSON.stringify({ timestamp, type: "seed", id: seed.seed_id, intake_id: intakeId, intake_path: intakeRelPath, sha256: contentSha, dry_run: false }) + "\n",
    );

    already.add(key);
    promoted.push({ id: seed.seed_id, intake_file: `${intakeId}.md`, intake_path: intakeRelPath });
  }

  // Export ledger (project-scoped, append-only): links knowledge exports to created intake(s).
  if (!dryRun && promoted.length) {
    const snapshotAbs = join(knowledgeRootAbs, "ssot", "system", "PROJECT_SNAPSHOT.json");
    let snapshotSha = null;
    if (existsSync(snapshotAbs)) {
      try {
        snapshotSha = sha256Hex(await readFile(snapshotAbs, "utf8"));
      } catch {
        snapshotSha = null;
      }
    }
    const intakeIds = promoted
      .map((p) => String(p?.intake_path || "").split("/").pop() || "")
      .map((fn) => fn.replace(/\.md$/i, ""))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    await ensureDir("ai/lane_b/cache");
    await appendStateFile(
      "ai/lane_b/cache/knowledge_exports.jsonl",
      JSON.stringify({
        type: "seed_export",
        created_at: timestamp,
        knowledge_snapshot_sha256: snapshotSha,
        session_id: null,
        created_intakes: intakeIds,
      }) + "\n",
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
