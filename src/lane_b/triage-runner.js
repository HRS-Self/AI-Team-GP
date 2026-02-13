import { readdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import { isAbsolute, resolve } from "node:path";

import { appendFile, ensureDir, readTextIfExists, writeText } from "../utils/fs.js";
import { resolveStatePath } from "../project/state-paths.js";
import { writeGlobalStatusFromPortfolio } from "../utils/status-writer.js";
import { loadRepoRegistry, findExplicitRepoReferences } from "../utils/repo-registry.js";
import { validateTriagedBatch, validateTriagedRepoItem } from "../validators/triaged-repo-item-validator.js";
import { formatFsSafeUtcTimestamp } from "../utils/naming.js";
import { loadProjectPaths } from "../paths/project-paths.js";
import { validateLaneAOriginIntake } from "./lane-a-governance.js";
import { evaluateScopeStaleness } from "../lane_a/lane-a-staleness-policy.js";
import { readKnowledgeVersionOrDefault } from "../lane_a/knowledge/knowledge-version.js";
import { readSufficiencyRecord } from "../lane_a/knowledge/knowledge-sufficiency.js";
import { runKnowledgeChangeRequest } from "../lane_a/knowledge/change-requests.js";

function nowISO() {
  return new Date().toISOString();
}

function safeTimestampForFilename(iso) {
  const d = iso ? new Date(String(iso)) : new Date();
  return formatFsSafeUtcTimestamp(d);
}

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function normalizeIntent(text) {
  return String(text || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function oneLineSummary(text) {
  const s = String(text || "").trim().replace(/\s+/g, " ");
  return s.length <= 160 ? s : s.slice(0, 160).trimEnd() + "…";
}

function extractExplicitBranchName(intakeText) {
  const text = String(intakeText || "");
  const patterns = [
    /\btarget\s+branch\s*:\s*([A-Za-z0-9._/\-]+)\b/i,
    /\b(?:in|on|against|checkout)\s+(?:the\s+)?branch\s+([A-Za-z0-9._/\-]+)\b/i,
    /\b(?:in|on|against)\s+([A-Za-z0-9._/\-]+)\s+branch\b/i,
  ];
  for (const re of patterns) {
    const m = text.match(re);
    if (m && m[1]) return { name: String(m[1]).trim(), matched_token: m[0] };
  }
  if (/\bdevelop\s+branch\b/i.test(text)) return { name: "develop", matched_token: "develop branch" };
  if (/\bmain\s+branch\b/i.test(text)) return { name: "main", matched_token: "main branch" };
  if (/\bmaster\s+branch\b/i.test(text)) return { name: "master", matched_token: "master branch" };
  return null;
}

function intakeAllowsArchivedRepos(intakeText) {
  const t = String(intakeText || "").toLowerCase();
  return t.includes("include archived") || t.includes("include inactive") || t.includes("include archived repos") || t.includes("include inactive repos");
}

function includesAllActiveReposPhrase(intakeText) {
  const t = String(intakeText || "").toLowerCase();
  return (
    t.includes("every active repo") ||
    t.includes("all active repos") ||
    t.includes("every active repository") ||
    t.includes("all active repositories") ||
    t.includes("every repo") ||
    t.includes("all repos")
  );
}

function keywordScore(intakeText, repo) {
  const lower = String(intakeText || "").toLowerCase();
  let score = 0;
  for (const raw of Array.isArray(repo?.keywords) ? repo.keywords : []) {
    const kw = String(raw || "").trim().toLowerCase();
    if (!kw) continue;
    if (lower.includes(kw)) score += 1;
  }
  return score;
}

function parseHeaderMap(text) {
  const lines = String(text || "")
    .split("\n")
    .slice(0, 200)
    .map((l) => l.trimEnd());
  const map = new Map();
  for (const l of lines) {
    const m = /^([A-Za-z_][A-Za-z0-9_-]*):\s*(.*?)\s*$/.exec(l.trim());
    if (!m) continue;
    const k = m[1].trim().toLowerCase();
    const v = m[2].trim();
    if (!k) continue;
    if (!map.has(k)) map.set(k, v);
  }
  return map;
}

function normalizeScopeOrNull(scopeRaw) {
  const s = String(scopeRaw || "").trim();
  if (!s) return null;
  if (s === "system") return "system";
  if (/^repo:[A-Za-z0-9._-]+$/.test(s)) return s;
  return null;
}

async function appendTriageLog(obj) {
  await ensureDir("ai/lane_b/logs");
  await appendFile("ai/lane_b/logs/triage.log", JSON.stringify({ timestamp: nowISO(), ...obj }) + "\n");
}

async function writeTriageFailure({ runTsSafe, rawIntakeId, errorMessage }) {
  await ensureDir("ai/lane_b/triage");
  const path = `ai/lane_b/triage/TRIAGE_FAILED-${runTsSafe}.md`;
  const existing = (await readTextIfExists(path)) || "";
  const block = [
    "",
    `## Failed: ${rawIntakeId}`,
    "",
    "### Error",
    "",
    "```",
    String(errorMessage || "").trimEnd(),
    "```",
    "",
  ].join("\n");
  await writeText(path, existing.trimEnd() + "\n" + block);
  return path;
}

function renderRunReport({ runTsIso, processed, createdItems, failures }) {
  const lines = [];
  lines.push(`# TRIAGE REPORT`);
  lines.push("");
  lines.push(`Timestamp: ${runTsIso}`);
  lines.push("");
  lines.push(`- processed_intakes: ${processed}`);
  lines.push(`- created_triage_items: ${createdItems.length}`);
  lines.push(`- failures: ${failures.length}`);
  lines.push("");

  if (failures.length) {
    lines.push("## Failures");
    lines.push("");
    for (const f of failures) lines.push(`- ${f.raw_intake_id}: ${f.error_message}`);
    lines.push("");
  }

  lines.push("## Created triage items");
  lines.push("");
  if (!createdItems.length) {
    lines.push("- (none)");
    lines.push("");
    return lines.join("\n");
  }

  for (const item of createdItems) {
    lines.push(`### ${item.triaged_id}`);
    lines.push("");
    lines.push(`- raw_intake_id: \`${item.raw_intake_id}\``);
    lines.push(`- repo_id: \`${item.repo_id}\``);
    lines.push(`- team_id: \`${item.team_id}\``);
    lines.push(`- target_branch: \`${item.target_branch}\``);
    lines.push("");
    lines.push(`Summary: ${item.summary}`);
    lines.push("");
  }

  return lines.join("\n");
}

export async function runTriage({ repoRoot = process.cwd(), limit = 10, dryRun = false } = {}) {
  const runTsIso = nowISO();
  const runTsSafe = safeTimestampForFilename(runTsIso);
  const maxItems = Number.isFinite(Number(limit)) ? Math.max(1, Math.floor(Number(limit))) : 10;

  const paths = await loadProjectPaths({ projectRoot: null });

  await ensureDir("ai/lane_b/inbox");
  await ensureDir("ai/lane_b/inbox/.processed");
  await ensureDir("ai/lane_b/inbox/triaged");
  await ensureDir("ai/lane_b/inbox/triaged/archive");
  await ensureDir("ai/lane_b/triage");
  if (dryRun) await ensureDir("ai/lane_b/triage/dry-run");

  const reposLoaded = await loadRepoRegistry();
  if (!reposLoaded.ok) return { ok: false, message: reposLoaded.message };
  const registry = reposLoaded.registry;
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  const activeRepos = repos.filter((r) => String(r?.status || "").trim().toLowerCase() === "active");
  const repoById = new Map(activeRepos.map((r) => [String(r.repo_id), r]));

  const inboxEntries = await readdir(resolveStatePath("ai/lane_b/inbox"), { withFileTypes: true });
  const intakeFiles = inboxEntries
    .filter((e) => e.isFile() && e.name.startsWith("I-") && e.name.endsWith(".md"))
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b));

  const unprocessed = [];
  for (const f of intakeFiles) {
    const rawId = f.replace(/\.md$/i, "");
    if (await readTextIfExists(`ai/lane_b/inbox/.processed/${rawId}.json`)) continue;
    unprocessed.push(f);
  }

  const toProcess = unprocessed.slice(0, maxItems);
  if (!toProcess.length) {
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: runTsIso, action: "triage_completed", processed_count: 0, created_count: 0 }) + "\n");
    await appendTriageLog({ action: "triage_completed", processed_count: 0, created_count: 0 });
    return { ok: true, processed_count: 0, created_count: 0, report: null };
  }

  const createdItems = [];
  const failures = [];

  for (const sourceFile of toProcess) {
    const rawId = sourceFile.replace(/\.md$/i, "");
    const intakeMd = await readTextIfExists(`ai/lane_b/inbox/${sourceFile}`);
    if (!intakeMd) {
      const failure_artifact = await writeTriageFailure({ runTsSafe, rawIntakeId: rawId, errorMessage: `Missing ai/lane_b/inbox/${sourceFile}` });
      failures.push({ raw_intake_id: rawId, error_message: "missing_intake_md", failure_artifact });
      await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "triage_failed", raw_intake_id: rawId, error: "missing_intake_md", failure_artifact }) + "\n");
      continue;
    }

    // Lane A governance enforcement (only when the intake claims Lane A origin).
    let laneAGov = null;
    try {
      laneAGov = await validateLaneAOriginIntake({ projectRoot: paths.opsRootAbs, intakeText: intakeMd });
    } catch (err) {
      laneAGov = { ok: false, lane_a: true, reason_code: "lane_a_governance_violation", message: err instanceof Error ? err.message : String(err) };
    }
    if (laneAGov && laneAGov.lane_a === true && laneAGov.ok === false) {
      const failure_artifact = await writeTriageFailure({
        runTsSafe,
        rawIntakeId: rawId,
        errorMessage: `lane_a_governance_violation: ${laneAGov.message || "blocked"}`,
      });
      failures.push({ raw_intake_id: rawId, error_message: "lane_a_governance_violation", failure_artifact });
      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "triage_failed", raw_intake_id: rawId, error: "lane_a_governance_violation", reason_code: laneAGov.reason_code || null, failure_artifact }) + "\n",
      );
      await appendTriageLog({ action: "triage_failed", raw_intake_id: rawId, error: "lane_a_governance_violation" });
      continue;
    }

    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "triage_started", raw_intake_id: rawId, limit: maxItems, dry_run: dryRun }) + "\n");
    await appendTriageLog({ action: "triage_started", raw_intake_id: rawId, dry_run: dryRun });

    const intakeLine = intakeMd
      .split("\n")
      .map((l) => l.trim())
      .find((l) => l.toLowerCase().startsWith("intake:"));
    const intakeText = intakeLine ? intakeLine.slice("intake:".length).trim() : intakeMd;
    const hdr = parseHeaderMap(intakeMd);
    const origin = String(hdr.get("origin") || "").trim().toLowerCase() || null;
    const scopeHint = normalizeScopeOrNull(hdr.get("scope"));

    const explicitBranch = extractExplicitBranchName(intakeText);
    const allowArchived = intakeAllowsArchivedRepos(intakeText);

    let repoIds = [];
    if (includesAllActiveReposPhrase(intakeText)) {
      repoIds = activeRepos.map((r) => String(r.repo_id)).sort((a, b) => a.localeCompare(b));
    } else {
      const explicitRefs = findExplicitRepoReferences({ intakeText, registry });
      const filtered = explicitRefs.filter((m) => {
        const st = String(m.status || "").toLowerCase();
        return st === "active" || allowArchived;
      });
      if (filtered.length) {
        repoIds = filtered.map((m) => String(m.repo_id)).sort((a, b) => a.localeCompare(b));
      } else {
        const scored = activeRepos
          .map((r) => ({ repo_id: String(r.repo_id), score: keywordScore(intakeText, r) }))
          .sort((a, b) => b.score - a.score || a.repo_id.localeCompare(b.repo_id));
        const bestScore = scored.length ? scored[0].score : 0;
        if (bestScore > 0) repoIds = scored.filter((x) => x.score === bestScore).map((x) => x.repo_id).sort((a, b) => a.localeCompare(b));
      }
    }

    if (!repoIds.length) {
      const failure_artifact = await writeTriageFailure({
        runTsSafe,
        rawIntakeId: rawId,
        errorMessage: "Could not resolve any repo_id deterministically. Add an explicit repo_id/name/path or say 'every active repo'.",
      });
      failures.push({ raw_intake_id: rawId, error_message: "no_repos_resolved", failure_artifact });
      await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "triage_failed", raw_intake_id: rawId, error: "no_repos_resolved", failure_artifact }) + "\n");
      await appendTriageLog({ action: "triage_failed", raw_intake_id: rawId, error: "no_repos_resolved" });
      continue;
    }

    // Lane A-origin: enforce scope narrowing (repo:<id> must triage to exactly that repo).
    if (laneAGov && laneAGov.lane_a === true) {
      const sc = typeof laneAGov.meta?.scope === "string" ? laneAGov.meta.scope.trim() : "";
      const m = /^repo:([A-Za-z0-9._-]+)$/.exec(sc);
      if (m) {
        const scopedRepo = m[1];
        if (repoById.has(scopedRepo)) repoIds = [scopedRepo];
      }
    }

    // Bug reports: if knowledge is insufficient or hard-stale for scope, route to Lane A instead of creating Lane B triage/work.
    if (origin === "bug_report") {
      const effectiveScope = scopeHint || (repoIds.length === 1 ? `repo:${repoIds[0]}` : "system");
      const kvRes = await readKnowledgeVersionOrDefault({ projectRoot: paths.opsRootAbs });
      const currentKv = String(kvRes.version?.current || "v0").trim() || "v0";

      const st = await evaluateScopeStaleness({ paths, registry, scope: effectiveScope });
      const suff = await readSufficiencyRecord({ projectRoot: paths.opsRootAbs, scope: effectiveScope, knowledgeVersion: currentKv });
      const sufficient = suff.exists && String(suff.sufficiency?.status || "").trim() === "sufficient";
      const blocked = st.hard_stale === true || sufficient !== true;

      if (blocked) {
        let routed = null;
        if (!dryRun) {
          const intakeAbs = resolveStatePath(`ai/lane_b/inbox/${sourceFile}`, { requiredRoot: true });
          routed = await runKnowledgeChangeRequest({ projectRoot: paths.opsRootAbs, type: "bug", scope: effectiveScope, inputPath: intakeAbs, dryRun: false });
          const markerPath = `ai/lane_b/inbox/.processed/${rawId}.json`;
          await writeText(
            markerPath,
            JSON.stringify(
              {
                version: 1,
                raw_intake_id: rawId,
                processed_at: runTsIso,
                action: "routed_to_lane_a",
                origin: "bug_report",
                scope: effectiveScope,
                knowledge_version: currentKv,
                staleness: { stale: !!st.stale, hard_stale: !!st.hard_stale, reasons: Array.isArray(st.reasons) ? st.reasons : [] },
                sufficiency_status: suff.exists ? String(suff.sufficiency?.status || "").trim() : "insufficient",
                change_request_id: routed && routed.ok ? routed.id : null,
              },
              null,
              2,
            ) + "\n",
          );
        }

        await appendFile(
          "ai/lane_b/ledger.jsonl",
          JSON.stringify({
            timestamp: nowISO(),
            action: "triage_routed_bug_report_to_lane_a",
            raw_intake_id: rawId,
            scope: effectiveScope,
            knowledge_version: currentKv,
            hard_stale: !!st.hard_stale,
            sufficient: sufficient === true,
            change_request_id: routed && routed.ok ? routed.id : null,
          }) + "\n",
        );
        await appendTriageLog({ action: "triage_routed_bug_report_to_lane_a", raw_intake_id: rawId, scope: effectiveScope });
        continue;
      }
    }

    const triagedIds = [];
    const repoIdsOut = [];
    const normalized = normalizeIntent(intakeText);
    const batchId = `BATCH-${rawId}`;

    for (const repoId of repoIds) {
      const repo = repoById.get(String(repoId)) || null;
      if (!repo) continue;
      const teamId = String(repo.team_id || "").trim();
      if (!teamId) continue;

      const branch = explicitBranch?.name || String(repo.active_branch || "").trim() || "develop";
      const dedupeKey = sha256Hex(`${repoId}\n${normalized}`).slice(0, 16);
      const triagedHash = sha256Hex(`${rawId}\n${repoId}\n${dedupeKey}`).slice(0, 12);
      const triagedId = `T-${runTsSafe}-${triagedHash}`;

      const base = {
        version: 1,
        triaged_id: triagedId,
        raw_intake_id: rawId,
        created_at: runTsIso,
        repo_id: repoId,
        team_id: teamId,
        target_branch: branch,
        summary: `Repo-scoped: ${repoId} — ${oneLineSummary(intakeText)}`,
        instructions: `Work on repo_id=${repoId} (team_id=${teamId}) targeting branch '${branch}'. Raw intake: ${rawId}.`,
        dedupe_key: dedupeKey,
        ...(laneAGov && laneAGov.lane_a === true
          ? {
              origin: "lane_a",
              intake_approval_id: laneAGov.meta?.intake_approval_id || null,
              knowledge_version: laneAGov.meta?.knowledge_version || null,
              lane_a_scope: laneAGov.meta?.scope || null,
              sufficiency_override: laneAGov.meta?.sufficiency_override === true,
            }
          : {}),
      };

      const validated = validateTriagedRepoItem(base, { triagedId, rawIntakeId: rawId, createdAt: runTsIso });
      if (!validated.ok) {
        const failure_artifact = await writeTriageFailure({ runTsSafe, rawIntakeId: rawId, errorMessage: `triaged item invalid for ${repoId}: ${validated.errors.join(" | ")}` });
        failures.push({ raw_intake_id: rawId, error_message: "triaged_item_invalid", failure_artifact });
        await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "triage_failed", raw_intake_id: rawId, error: "triaged_item_invalid", failure_artifact }) + "\n");
        triagedIds.length = 0;
        break;
      }

      const outPath = dryRun ? `ai/lane_b/triage/dry-run/${triagedId}.json` : `ai/lane_b/inbox/triaged/${triagedId}.json`;
      await writeText(outPath, JSON.stringify(validated.normalized, null, 2) + "\n");

      createdItems.push({ ...validated.normalized, path: outPath });
      triagedIds.push(triagedId);
      repoIdsOut.push(repoId);

      await appendFile(
        "ai/lane_b/ledger.jsonl",
        JSON.stringify({ timestamp: nowISO(), action: "triage_created", raw_intake_id: rawId, triaged_id: triagedId, repo_id: repoId, team_id: teamId, batch_id: batchId, path: outPath }) + "\n",
      );
      await appendTriageLog({ action: "triage_created", raw_intake_id: rawId, triaged_id: triagedId, repo_id: repoId, team_id: teamId, batch_id: batchId });
    }

    if (!triagedIds.length) continue;

    const batchPath = dryRun ? `ai/lane_b/triage/dry-run/${batchId}.json` : `ai/lane_b/inbox/triaged/BATCH-${rawId}.json`;
    const batchRaw = {
      version: 1,
      batch_id: batchId,
      raw_intake_id: rawId,
      created_at: runTsIso,
      triaged_ids: triagedIds.slice(),
      repo_ids: repoIdsOut.slice(),
      status: "triaged",
    };
    const batchValidated = validateTriagedBatch(batchRaw, { batchId, rawIntakeId: rawId, createdAt: runTsIso });
    if (!batchValidated.ok) {
      const failure_artifact = await writeTriageFailure({ runTsSafe, rawIntakeId: rawId, errorMessage: `batch invalid: ${batchValidated.errors.join(" | ")}` });
      failures.push({ raw_intake_id: rawId, error_message: "batch_invalid", failure_artifact });
      await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "triage_failed", raw_intake_id: rawId, error: "batch_invalid", failure_artifact }) + "\n");
      continue;
    }
    await writeText(batchPath, JSON.stringify(batchValidated.normalized, null, 2) + "\n");
    await appendFile(
      "ai/lane_b/ledger.jsonl",
      JSON.stringify({ timestamp: nowISO(), action: "batch_created", raw_intake_id: rawId, batch_id: batchId, triaged_ids: triagedIds.slice(), path: batchPath }) + "\n",
    );
    await appendTriageLog({ action: "batch_created", raw_intake_id: rawId, batch_id: batchId, triaged_ids: triagedIds.slice() });

    if (!dryRun) {
      const markerPath = `ai/lane_b/inbox/.processed/${rawId}.json`;
      await writeText(
        markerPath,
        JSON.stringify({ version: 1, raw_intake_id: rawId, processed_at: runTsIso, batch_id: batchId, triaged_ids: triagedIds.slice() }, null, 2) + "\n",
      );
    }
  }

  const reportContent = renderRunReport({ runTsIso, processed: toProcess.length, createdItems, failures });
  const reportPath = `ai/lane_b/triage/TRIAGE_REPORT-${runTsSafe}.md`;
  await writeText(reportPath, reportContent.trimEnd() + "\n");

  await appendFile(
    "ai/lane_b/ledger.jsonl",
    JSON.stringify({ timestamp: nowISO(), action: "triage_completed", processed_count: toProcess.length, created_count: createdItems.length, report: reportPath }) + "\n",
  );
  await appendTriageLog({ action: "triage_completed", processed_count: toProcess.length, created_count: createdItems.length, report: reportPath });

  try {
    await writeGlobalStatusFromPortfolio();
  } catch {
    // ignore
  }

  return {
    ok: failures.length === 0,
    processed_count: toProcess.length,
    created_count: createdItems.length,
    report: reportPath,
    created: createdItems.map((x) => ({ triaged_id: x.triaged_id, path: x.path, raw_intake_id: x.raw_intake_id, repo_id: x.repo_id })),
    failures,
  };
}

// Cron/PM2 helper: allow callers to provide an explicit project root instead of relying on the parent process environment.
export async function triageRunner(projectRoot, opts = {}) {
  const next = typeof projectRoot === "string" && projectRoot.trim() ? resolve(projectRoot.trim()) : null;
  if (next && !isAbsolute(next)) throw new Error(`triageRunner(projectRoot): projectRoot must be absolute (got: ${projectRoot}).`);

  const prev = process.env.AI_PROJECT_ROOT;
  if (next) process.env.AI_PROJECT_ROOT = next;
  try {
    return await runTriage(opts);
  } finally {
    if (typeof prev === "string") process.env.AI_PROJECT_ROOT = prev;
    else delete process.env.AI_PROJECT_ROOT;
  }
}
