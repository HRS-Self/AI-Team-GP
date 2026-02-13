import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readdir, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { nowFsSafeUtcTimestamp } from "../utils/naming.js";
import { sha256Hex } from "../utils/fs-hash.js";
import { validateDecisionPacket, validateKnowledgeScan, validateRepoIndex } from "../contracts/validators/index.js";
import { resolveRepoAbsPath } from "../utils/repo-registry.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function msOrNull(iso) {
  const s = normStr(iso);
  if (!s) return null;
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? ms : null;
}

function msFromFsSafeUtcTimestamp(ts) {
  // format: YYYYMMDD_HHMMSSmmm (8 + "_" + 9)
  const s = normStr(ts);
  const m = /^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(\d{3})$/.exec(s);
  if (!m) return null;
  const year = Number(m[1]);
  const month0 = Number(m[2]) - 1;
  const day = Number(m[3]);
  const hour = Number(m[4]);
  const min = Number(m[5]);
  const sec = Number(m[6]);
  const ms = Number(m[7]);
  const out = Date.UTC(year, month0, day, hour, min, sec, ms);
  return Number.isFinite(out) ? out : null;
}

function clampInt(n, { min, max }) {
  const x = Number.parseInt(String(n ?? ""), 10);
  if (!Number.isFinite(x)) return min;
  return Math.max(min, Math.min(max, x));
}

function git(repoAbs, args, { timeoutMs = 30_000 } = {}) {
  const res = spawnSync("git", ["-C", repoAbs, ...args], { encoding: "utf8", timeout: timeoutMs });
  return { ok: res.status === 0, status: res.status, stdout: String(res.stdout || ""), stderr: String(res.stderr || "") };
}

function segmentFilesSorted(segmentsDirAbs) {
  if (!existsSync(segmentsDirAbs)) return [];
  return readdir(segmentsDirAbs, { withFileTypes: true }).then((entries) =>
    entries
      // Support both legacy and current segment naming:
      // - legacy: events-YYYYMMDD-HH.jsonl
      // - current: YYYYMMDD-HHMMSS.jsonl
      .filter((e) => e.isFile() && (/^events-\d{8}-\d{2}\.jsonl$/.test(e.name) || /^\d{8}-\d{6}\.jsonl$/.test(e.name)))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b)),
  );
}

async function findLatestMergeEventTimestamp({ segmentsDirAbs, repoId, maxFiles = 48 }) {
  const files = await segmentFilesSorted(segmentsDirAbs);
  const tail = files.slice(Math.max(0, files.length - maxFiles));
  for (let i = tail.length - 1; i >= 0; i -= 1) {
    const abs = join(segmentsDirAbs, tail[i]);
    // eslint-disable-next-line no-await-in-loop
    const text = readFileSync(abs, "utf8");
    const lines = String(text || "")
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
    for (let j = lines.length - 1; j >= 0; j -= 1) {
      try {
        const obj = JSON.parse(lines[j]);
        if (normStr(obj.type) !== "merge") continue;
        if (normStr(obj.repo_id) !== repoId) continue;
        const ts = normStr(obj.timestamp);
        if (ts) return ts;
      } catch {
        // ignore invalid lines
      }
    }
  }
  return null;
}

function readRepoIndexScannedAtIso({ knowledgeRootAbs, repoId }) {
  const abs = join(knowledgeRootAbs, "evidence", "index", "repos", repoId, "repo_index.json");
  if (!existsSync(abs)) return null;
  try {
    const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    const iso = normStr(j?.scanned_at);
    return iso || null;
  } catch {
    return null;
  }
}

function readRepoIndexHeadSha({ knowledgeRootAbs, repoId }) {
  const abs = join(knowledgeRootAbs, "evidence", "index", "repos", repoId, "repo_index.json");
  if (!existsSync(abs)) return null;
  try {
    const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    const sha = normStr(j?.head_sha);
    return sha || null;
  } catch {
    return null;
  }
}

function readRepoScanScannedAt({ knowledgeRootAbs, repoId }) {
  const abs = join(knowledgeRootAbs, "ssot", "repos", repoId, "scan.json");
  if (!existsSync(abs)) return null;
  try {
    const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
    validateKnowledgeScan(j);
    const ts = normStr(j?.scanned_at);
    return ts || null;
  } catch {
    return null;
  }
}

function coverageCompleteForRepo({ knowledgeRootAbs, repoId }) {
  const idxAbs = join(knowledgeRootAbs, "evidence", "index", "repos", repoId, "repo_index.json");
  const scanAbs = join(knowledgeRootAbs, "ssot", "repos", repoId, "scan.json");
  if (!existsSync(idxAbs) || !existsSync(scanAbs)) return false;
  try {
    const idx = JSON.parse(String(readFileSync(idxAbs, "utf8") || ""));
    validateRepoIndex(idx);
    const scan = JSON.parse(String(readFileSync(scanAbs, "utf8") || ""));
    validateKnowledgeScan(scan);
    return true;
  } catch {
    return false;
  }
}

function computeRepoHeadSha(repoAbs) {
  const res = git(repoAbs, ["rev-parse", "HEAD"]);
  if (!res.ok) return { ok: false, sha: null, error: normStr(res.stderr) || normStr(res.stdout) || "git rev-parse HEAD failed" };
  const sha = normStr(res.stdout).split("\n")[0];
  return { ok: true, sha: sha || null, error: null };
}

function listActiveRepoIds(registry) {
  const repos = Array.isArray(registry?.repos) ? registry.repos : [];
  return repos
    .filter((r) => String(r?.status || "").trim().toLowerCase() === "active")
    .map((r) => normStr(r?.repo_id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

export async function evaluateRepoStaleness({
  paths,
  registry,
  repoId,
  nowMs = null,
  thresholdMinutes = null,
} = {}) {
  const repo_id = normStr(repoId);
  if (!paths || !paths.knowledge || !paths.laneA) throw new Error("evaluateRepoStaleness: paths is required.");
  if (!registry || !isPlainObject(registry)) throw new Error("evaluateRepoStaleness: registry is required.");
  if (!repo_id) throw new Error("evaluateRepoStaleness: repoId is required.");

  const repos = Array.isArray(registry.repos) ? registry.repos : [];
  const repoCfg = repos.find((r) => normStr(r?.repo_id) === repo_id) || null;
  const repoAbs = repoCfg ? resolveRepoAbsPath({ baseDir: registry.base_dir, repoPath: repoCfg.path }) : null;

  const repoHead = repoAbs && existsSync(repoAbs) ? computeRepoHeadSha(repoAbs) : { ok: false, sha: null, error: "repo path missing" };
  const last_scanned_head_sha = readRepoIndexHeadSha({ knowledgeRootAbs: paths.knowledge.rootAbs, repoId: repo_id });

  const last_scan_time = readRepoScanScannedAt({ knowledgeRootAbs: paths.knowledge.rootAbs, repoId: repo_id }) || readRepoIndexScannedAtIso({ knowledgeRootAbs: paths.knowledge.rootAbs, repoId: repo_id });
  const last_scan_ms = msFromFsSafeUtcTimestamp(last_scan_time) ?? msOrNull(last_scan_time);

  const last_merge_event_time = await findLatestMergeEventTimestamp({ segmentsDirAbs: paths.laneA.eventsSegmentsAbs, repoId: repo_id });
  const last_merge_ms = msOrNull(last_merge_event_time);

  const reasons = [];
  let stale = false;

  const coverage_complete = coverageCompleteForRepo({ knowledgeRootAbs: paths.knowledge.rootAbs, repoId: repo_id });
  if (!coverage_complete) {
    stale = true;
    reasons.push("coverage_incomplete");
  }

  const headKnown = repoHead.ok && !!repoHead.sha;
  // Staleness condition: repo HEAD != last scanned head sha.
  // If repo HEAD is unavailable (e.g., repo clone missing), do not assert staleness purely from the head check.
  if (headKnown && last_scanned_head_sha && repoHead.sha !== last_scanned_head_sha) {
    stale = true;
    reasons.push("head_sha_mismatch");
  }

  const mergeAfterScan = Number.isFinite(last_merge_ms) && Number.isFinite(last_scan_ms) && last_merge_ms > last_scan_ms;
  if (mergeAfterScan) {
    stale = true;
    reasons.push("merge_event_after_scan");
  }

  // Preserve legacy fields used by existing call sites; staleness is authoritative and binary now.
  const thresholdMin = thresholdMinutes == null ? clampInt(process.env.LANE_A_STALE_THRESHOLD_MINUTES ?? 30, { min: 1, max: 24 * 60 }) : clampInt(thresholdMinutes, { min: 1, max: 24 * 60 });
  const thresholdMs = thresholdMin * 60_000;
  const now = Number.isFinite(Number(nowMs)) ? Number(nowMs) : Date.now();
  const ageExceeded = Number.isFinite(last_scan_ms) ? now - last_scan_ms > thresholdMs : true;
  // "Hard stale" blocks high-confidence outputs:
  // - if merge events are newer than scan, OR
  // - if scan age exceeds threshold.
  const hard_stale = stale && (mergeAfterScan || ageExceeded);

  const stale_reason = stale ? reasons.slice().sort((a, b) => a.localeCompare(b))[0] || "stale" : null;

  return {
    ok: true,
    repo_id,
    repo_abs: repoAbs,
    repo_head_sha: repoHead.sha,
    last_scanned_head_sha,
    last_scan_time,
    last_merge_event_time,
    threshold_minutes: thresholdMin,
    stale,
    hard_stale,
    stale_reason,
    stale_reasons: reasons.slice().sort((a, b) => a.localeCompare(b)),
    coverage_complete,
  };
}

export function computeSystemStaleness({ repoStaleness, knowledgeRootAbs }) {
  const infos = Array.isArray(repoStaleness) ? repoStaleness : [];
  const anyRepoStale = infos.some((s) => s && s.ok === true && s.stale === true);

  const reasons = [];
  let stale = false;
  if (anyRepoStale) {
    stale = true;
    reasons.push("repo_stale");
  }
  // NOTE: integration gaps belong to sufficiency/committee gates, not freshness/staleness.
  return { stale, reasons: reasons.sort((a, b) => a.localeCompare(b)), has_medium_or_higher_integration_gaps: false };
}

export async function evaluateScopeStaleness({ paths, registry, scope }) {
  const raw = normStr(scope);
  if (!raw) throw new Error("evaluateScopeStaleness: scope is required.");
  if (raw === "system") {
    const repoIds = listActiveRepoIds(registry);
    const infos = [];
    for (const repoId of repoIds) {
      // eslint-disable-next-line no-await-in-loop
      const s = await evaluateRepoStaleness({ paths, registry, repoId });
      infos.push(s);
    }
    const sys = computeSystemStaleness({ repoStaleness: infos, knowledgeRootAbs: paths.knowledge.rootAbs });
    const staleRepoIds = infos.filter((s) => s && s.stale === true).map((s) => s.repo_id).filter(Boolean).sort((a, b) => a.localeCompare(b));
    const hardStaleRepoIds = infos.filter((s) => s && s.hard_stale === true).map((s) => s.repo_id).filter(Boolean).sort((a, b) => a.localeCompare(b));
    const reasons = []
      .concat(sys.reasons)
      .concat(staleRepoIds.length ? ["repo_stale"] : [])
      .filter(Boolean);
    return {
      ok: true,
      scope: "system",
      stale: sys.stale || staleRepoIds.length > 0,
      hard_stale: sys.stale || hardStaleRepoIds.length > 0,
      reasons: Array.from(new Set(reasons)).sort((a, b) => a.localeCompare(b)),
      stale_repos: staleRepoIds,
      hard_stale_repos: hardStaleRepoIds,
    };
  }
  const m = /^repo:([A-Za-z0-9._-]+)$/.exec(raw);
  if (!m) throw new Error("evaluateScopeStaleness: invalid scope (expected system|repo:<id>).");
  const repoId = m[1];
  const s = await evaluateRepoStaleness({ paths, registry, repoId });
  return {
    ok: true,
    scope: `repo:${repoId}`,
    stale: s.stale === true,
    hard_stale: s.hard_stale === true,
    reasons: Array.isArray(s.stale_reasons) ? s.stale_reasons : [],
    stale_repos: s.stale ? [repoId] : [],
    hard_stale_repos: s.hard_stale ? [repoId] : [],
  };
}

let atomicCounter = 0;
async function writeTextAtomic(absPath, text) {
  const abs = resolve(String(absPath || ""));
  await mkdir(dirname(abs), { recursive: true });
  atomicCounter += 1;
  const tmp = `${abs}.tmp.${process.pid}.${atomicCounter.toString(16)}`;
  await writeFile(tmp, String(text || ""), "utf8");
  await rename(tmp, abs);
}

function buildRefreshRequiredDecision({ repoId, blockingState, createdAtIso, staleInfo, producer }) {
  const scope = repoId ? `repo:${repoId}` : "system";
  const decision_id = `DEC_refresh_required_${sha256Hex(`${scope}\n${blockingState}\nrefresh_required`).slice(0, 16)}`;
  const question_id = `Q_refresh_required_${sha256Hex(`${scope}\n${blockingState}\nrefresh_required_question`).slice(0, 16)}`;

  const known = [];
  if (staleInfo?.repo_head_sha) known.push(`repo_head_sha:${String(staleInfo.repo_head_sha)}`);
  if (staleInfo?.last_scanned_head_sha) known.push(`last_scanned_head_sha:${String(staleInfo.last_scanned_head_sha)}`);
  if (staleInfo?.last_scan_time) known.push(`last_scan_time:${String(staleInfo.last_scan_time)}`);
  if (staleInfo?.last_merge_event_time) known.push(`last_merge_event_time:${String(staleInfo.last_merge_event_time)}`);

  const pkt = {
    version: 1,
    decision_id,
    scope,
    trigger: producer === "writer" ? "state_machine" : "repo_committee",
    blocking_state: String(blockingState || "UNKNOWN"),
    context: {
      summary: `Refresh required before proceeding (${scope}).`,
      why_automation_failed: `STALE_BLOCKED: ${normStr(staleInfo?.stale_reason) || "stale"}`,
      what_is_known: known.length ? known : ["stale detected"],
    },
    questions: [
      {
        id: question_id,
        question: "Is the knowledge state refreshed (index/scan/refresh-from-events and/or gap resolution) and staleness cleared so Lane A may proceed?",
        expected_answer_type: "text",
        constraints: "Answer should include what actions were taken (commands or decisions) and confirmation that staleness is cleared.",
        blocks: [String(blockingState || "UNKNOWN")],
      },
    ],
    assumptions_if_unanswered: "Automation will not proceed while the repo is stale.",
    created_at: createdAtIso,
    status: "open",
  };
  validateDecisionPacket(pkt);
  return pkt;
}

export async function writeRefreshRequiredDecisionPacketIfNeeded({
  paths,
  repoId,
  blockingState,
  staleInfo,
  producer,
  dryRun = false,
} = {}) {
  const repo_id = repoId ? normStr(repoId) : null;
  const created_at = new Date().toISOString();
  const ts = nowFsSafeUtcTimestamp();

  const decisionsDirAbs = paths?.knowledge?.decisionsAbs || join(paths?.knowledge?.rootAbs || "", "decisions");
  const dirAbs = resolve(String(decisionsDirAbs || ""));

  if (!dirAbs) throw new Error("writeRefreshRequiredDecisionPacketIfNeeded: decisions dir missing.");

  // Idempotence: if an open refresh-required decision already exists for this scope, do not create another.
  if (existsSync(dirAbs)) {
    const entries = await readdir(dirAbs, { withFileTypes: true });
    const prefix = repo_id ? `DECISION-refresh-required-${repo_id}-` : "DECISION-refresh-required-system-";
    const existing = entries
      .filter((e) => e.isFile() && e.name.startsWith(prefix) && e.name.endsWith(".json"))
      .map((e) => join(dirAbs, e.name))
      .sort((a, b) => a.localeCompare(b));
    for (const abs of existing) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const j = JSON.parse(String(readFileSync(abs, "utf8") || ""));
        if (normStr(j?.status) === "open") return { ok: true, wrote: false, json_abs: abs, md_abs: abs.replace(/\\.json$/, ".md"), decision_id: normStr(j?.decision_id) || null };
      } catch {
        // ignore malformed old files
      }
    }
  }

  const pkt = buildRefreshRequiredDecision({ repoId: repo_id, blockingState, createdAtIso: created_at, staleInfo, producer });

  const stem = repo_id ? `DECISION-refresh-required-${repo_id}-${ts}` : `DECISION-refresh-required-system-${ts}`;
  const jsonAbs = join(dirAbs, `${stem}.json`);
  const mdAbs = join(dirAbs, `${stem}.md`);

  const md = [
    `# Decision: refresh required (${repo_id ? `repo:${repo_id}` : "system"})`,
    "",
    `CreatedAt: ${created_at}`,
    "",
    "## Why this is being asked",
    "",
    "Lane A detected staleness relative to repo HEAD and/or merge events. Automation is blocked until a refresh is performed.",
    "",
    "## What will unblock",
    "",
    "- Run the appropriate refresh (index/scan and/or refresh-from-events).",
    "- Re-run the blocked command (committee or writer).",
    "",
  ].join("\n");

  if (!dryRun) {
    await mkdir(dirAbs, { recursive: true });
    await writeTextAtomic(jsonAbs, JSON.stringify(pkt, null, 2) + "\n");
    await writeTextAtomic(mdAbs, md + "\n");
  }

  return { ok: true, wrote: true, json_abs: jsonAbs, md_abs: mdAbs, decision_id: pkt.decision_id };
}
