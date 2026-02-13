import { existsSync } from "node:fs";
import { readFile, readdir, realpath, stat } from "node:fs/promises";
import { extname, isAbsolute, join, relative, resolve, sep } from "node:path";

import { getProject as getRegistryProject, listProjects, loadRegistry } from "../registry/project-registry.js";
import { loadProjectPaths } from "../paths/project-paths.js";

function nowISO() {
  return new Date().toISOString();
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function toIsoFromMs(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return null;
  try {
    return new Date(ms).toISOString();
  } catch {
    return null;
  }
}

function toRelPosix(baseAbs, absPath) {
  return relative(resolve(baseAbs), resolve(absPath)).split(sep).join("/");
}

function pathInside(baseAbs, candidateAbs) {
  const rel = relative(resolve(baseAbs), resolve(candidateAbs));
  if (!rel) return true;
  if (rel.startsWith("..")) return false;
  return !rel.includes(`..${sep}`);
}

function htmlEsc(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizeScope(scopeRaw) {
  const s = normStr(scopeRaw).toLowerCase();
  if (!s || s === "all") return "system";
  if (s === "system") return "system";
  if (s.startsWith("repo:")) {
    const repoId = normStr(s.slice("repo:".length));
    return repoId ? `repo:${repoId}` : "system";
  }
  return "system";
}

function normalizeFlags(statusLike) {
  const src = isPlainObject(statusLike) ? statusLike : {};
  const st = isPlainObject(src.staleness) ? src.staleness : {};
  const hard_stale = src.hard_stale === true || st.hard_stale === true;
  const stale = src.stale === true || st.stale === true || hard_stale;
  const degraded = src.degraded === true || normStr(src.degraded_reason) === "soft_stale" || (stale && !hard_stale);
  const reasonsSrc = []
    .concat(Array.isArray(src.reasons) ? src.reasons : [])
    .concat(Array.isArray(src.stale_reasons) ? src.stale_reasons : [])
    .concat(Array.isArray(st.reasons) ? st.reasons : [])
    .concat(Array.isArray(st.stale_reasons) ? st.stale_reasons : []);
  const reasons = Array.from(new Set(reasonsSrc.map((x) => normStr(x)).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  return { stale, hard_stale, degraded, reasons };
}

function emptyScope(scope) {
  return {
    scope,
    hard_stale: false,
    stale: false,
    degraded: false,
    reasons: [],
    artifacts: {
      latest_refresh_hint: null,
      latest_decision_packet: null,
      latest_update_meeting: null,
      latest_review_meeting: null,
      latest_committee_status: null,
      latest_writer_status: null,
    },
  };
}

function ensureScope(map, scope) {
  if (!map.has(scope)) map.set(scope, emptyScope(scope));
  return map.get(scope);
}

function mergeScopeFlags(scopeObj, flags) {
  scopeObj.hard_stale = scopeObj.hard_stale || flags.hard_stale === true;
  scopeObj.stale = scopeObj.stale || flags.stale === true;
  scopeObj.degraded = scopeObj.degraded || flags.degraded === true;
  const merged = new Set([...(Array.isArray(scopeObj.reasons) ? scopeObj.reasons : []), ...(Array.isArray(flags.reasons) ? flags.reasons : [])]);
  scopeObj.reasons = Array.from(merged).sort((a, b) => a.localeCompare(b));
}

function setLatestArtifact(scopeObj, key, candidate) {
  if (!candidate) return;
  const curr = scopeObj.artifacts[key];
  const currMs = Date.parse(curr?.captured_at || "");
  const nextMs = Date.parse(candidate?.captured_at || "");
  if (!curr || !Number.isFinite(currMs) || (Number.isFinite(nextMs) && nextMs > currMs)) {
    scopeObj.artifacts[key] = candidate;
  }
}

async function listFiles(dirAbs, { filePattern = null } = {}) {
  if (!dirAbs || !existsSync(dirAbs)) return [];
  const entries = await readdir(dirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isFile())
    .map((e) => e.name)
    .filter((name) => (filePattern ? filePattern.test(name) : true))
    .sort((a, b) => a.localeCompare(b));
}

async function listDirs(dirAbs, { dirPattern = null } = {}) {
  if (!dirAbs || !existsSync(dirAbs)) return [];
  const entries = await readdir(dirAbs, { withFileTypes: true });
  return entries
    .filter((e) => e.isDirectory())
    .map((e) => e.name)
    .filter((name) => (dirPattern ? dirPattern.test(name) : true))
    .sort((a, b) => a.localeCompare(b));
}

async function statMtimeIso(absPath) {
  try {
    const st = await stat(absPath);
    return toIsoFromMs(Number(st.mtimeMs));
  } catch {
    return null;
  }
}

async function readJsonOptional(absPath) {
  if (!absPath || !existsSync(absPath)) return null;
  try {
    const raw = await readFile(absPath, "utf8");
    const parsed = JSON.parse(String(raw || ""));
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function collectWriterStatusFiles(writerRootAbs, maxDepth = 5, baseRel = "") {
  const dirAbs = baseRel ? join(writerRootAbs, baseRel) : writerRootAbs;
  if (!existsSync(dirAbs)) return [];
  if (maxDepth < 0) return [];
  const entries = await readdir(dirAbs, { withFileTypes: true });
  const out = [];
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    const rel = baseRel ? `${baseRel}/${entry.name}` : entry.name;
    const abs = join(writerRootAbs, rel);
    if (entry.isFile() && entry.name === "WRITER_STATUS.json") out.push({ abs, rel });
    if (entry.isDirectory()) {
      // eslint-disable-next-line no-await-in-loop
      const child = await collectWriterStatusFiles(writerRootAbs, maxDepth - 1, rel);
      out.push(...child);
    }
  }
  return out;
}

function meetingKindFromDir(dirName) {
  if (/^UM-\d{8}_\d{6}__/.test(dirName)) return "update_meeting";
  if (/^M-\d{8}_\d{6}__/.test(dirName)) return "review_meeting";
  return null;
}

function artifactUrl({ projectCode, kind, name }) {
  return `/lane-a/artifact?project=${encodeURIComponent(projectCode)}&kind=${encodeURIComponent(kind)}&name=${encodeURIComponent(name)}`;
}

function pickLatestArtifact(project, key) {
  const scopes = Array.isArray(project?.scopes) ? project.scopes : [];
  const withArtifacts = scopes.map((s) => s?.artifacts?.[key]).filter(Boolean);
  if (!withArtifacts.length) return null;
  return withArtifacts
    .slice()
    .sort((a, b) => (Date.parse(String(b?.captured_at || "")) || 0) - (Date.parse(String(a?.captured_at || "")) || 0))[0];
}

async function loadRepoIds({ project, paths }) {
  const set = new Set();
  const fromRegistry = Array.isArray(project?.repos) ? project.repos : [];
  for (const r of fromRegistry) {
    const repoId = normStr(r?.repo_id);
    if (repoId) set.add(repoId);
  }

  const reposCfgAbs = join(paths.opsConfigAbs, "REPOS.json");
  const reposCfg = await readJsonOptional(reposCfgAbs);
  const repos = Array.isArray(reposCfg?.repos) ? reposCfg.repos : [];
  for (const r of repos) {
    const repoId = normStr(r?.repo_id);
    if (repoId) set.add(repoId);
  }

  return Array.from(set).sort((a, b) => a.localeCompare(b));
}

function latestTimestampFromScope(scope) {
  const arts = isPlainObject(scope?.artifacts) ? scope.artifacts : {};
  const times = Object.values(arts)
    .map((a) => (a ? Date.parse(String(a.captured_at || "")) : NaN))
    .filter((ms) => Number.isFinite(ms));
  if (!times.length) return null;
  return new Date(Math.max(...times)).toISOString();
}

async function collectProjectHealth({ project, artifactPathBuilder }) {
  const projectCode = normStr(project?.project_code);
  const rootDir = normStr(project?.root_dir);
  const opsRoot = normStr(project?.ops_dir) || (rootDir ? resolve(rootDir, "ops") : "");
  const scopeMap = new Map();
  ensureScope(scopeMap, "system");

  if (!projectCode || !opsRoot || !isAbsolute(opsRoot)) {
    return {
      project_code: projectCode || "(unknown)",
      summary: {
        hard_stale: false,
        stale: false,
        degraded: false,
        last_seen_at: null,
        open_update_meeting_count: 0,
        open_review_meeting_count: 0,
        open_decision_packets_count: 0,
      },
      scopes: Array.from(scopeMap.values()),
    };
  }

  let paths = null;
  try {
    paths = await loadProjectPaths({ projectRoot: opsRoot });
  } catch {
    return {
      project_code: projectCode,
      summary: {
        hard_stale: false,
        stale: false,
        degraded: false,
        last_seen_at: null,
        open_update_meeting_count: 0,
        open_review_meeting_count: 0,
        open_decision_packets_count: 0,
      },
      scopes: Array.from(scopeMap.values()),
    };
  }

  const repoIds = await loadRepoIds({ project, paths });
  for (const repoId of repoIds) ensureScope(scopeMap, `repo:${repoId}`);

  const summaryCounters = {
    open_update_meeting_count: 0,
    open_review_meeting_count: 0,
    open_decision_packets_count: 0,
  };

  const systemCommitteeAbs = join(paths.knowledge.ssotSystemAbs, "committee", "integration", "integration_status.json");
  const systemCommitteeJson = await readJsonOptional(systemCommitteeAbs);
  if (systemCommitteeJson) {
    const scope = ensureScope(scopeMap, "system");
    mergeScopeFlags(scope, normalizeFlags(systemCommitteeJson));
    const captured_at = (await statMtimeIso(systemCommitteeAbs)) || null;
    setLatestArtifact(scope, "latest_committee_status", {
      name: "system/committee/integration/integration_status.json",
      url: artifactPathBuilder({ projectCode, kind: "committee", name: "system/committee/integration/integration_status.json" }),
      captured_at,
    });
  }

  for (const repoId of repoIds) {
    const committeeAbs = join(paths.knowledge.ssotReposAbs, repoId, "committee", "committee_status.json");
    const committeeJson = await readJsonOptional(committeeAbs);
    if (!committeeJson) continue;
    const scope = ensureScope(scopeMap, `repo:${repoId}`);
    mergeScopeFlags(scope, normalizeFlags(committeeJson));
    const relName = `repos/${repoId}/committee/committee_status.json`;
    const captured_at = (await statMtimeIso(committeeAbs)) || null;
    setLatestArtifact(scope, "latest_committee_status", {
      name: relName,
      url: artifactPathBuilder({ projectCode, kind: "committee", name: relName }),
      captured_at,
    });
  }

  const refreshHints = await listFiles(paths.laneA.refreshHintsAbs, { filePattern: /^RH-.*\.json$/ });
  for (const fileName of refreshHints) {
    const abs = join(paths.laneA.refreshHintsAbs, fileName);
    // eslint-disable-next-line no-await-in-loop
    const json = await readJsonOptional(abs);
    const scopeStr = normalizeScope(json?.scope);
    const captured_at = (await statMtimeIso(abs)) || null;
    const scope = ensureScope(scopeMap, scopeStr);
    scope.stale = true;
    setLatestArtifact(scope, "latest_refresh_hint", {
      name: fileName,
      url: artifactPathBuilder({ projectCode, kind: "refresh_hint", name: fileName }),
      captured_at,
    });
    const reason = normStr(json?.reason);
    if (reason) mergeScopeFlags(scope, { stale: true, hard_stale: false, degraded: false, reasons: [reason] });
  }

  const decisionPackets = await listFiles(paths.laneA.decisionPacketsAbs, { filePattern: /^DP-.*\.md$/ });
  summaryCounters.open_decision_packets_count = decisionPackets.length;
  for (const fileName of decisionPackets) {
    const abs = join(paths.laneA.decisionPacketsAbs, fileName);
    // eslint-disable-next-line no-await-in-loop
    const text = existsSync(abs) ? await readFile(abs, "utf8") : "";
    const repoId = normStr((/^repo_id:\s*(.+)$/im.exec(String(text || "")) || [])[1]);
    const scopeStr = repoId ? `repo:${repoId}` : "system";
    const scope = ensureScope(scopeMap, scopeStr);
    scope.stale = true;
    scope.degraded = true;
    const captured_at = (await statMtimeIso(abs)) || null;
    setLatestArtifact(scope, "latest_decision_packet", {
      name: fileName,
      url: artifactPathBuilder({ projectCode, kind: "decision_packet", name: fileName }),
      captured_at,
    });
  }

  const meetingDirs = await listDirs(paths.laneA.meetingsAbs);
  for (const dirName of meetingDirs) {
    const kind = meetingKindFromDir(dirName);
    if (!kind) continue;
    const meetingJsonAbs = join(paths.laneA.meetingsAbs, dirName, "MEETING.json");
    // eslint-disable-next-line no-await-in-loop
    const meeting = await readJsonOptional(meetingJsonAbs);
    if (!meeting) continue;
    const status = normStr(meeting.status).toLowerCase();
    if (kind === "update_meeting" && status !== "closed") summaryCounters.open_update_meeting_count += 1;
    if (kind === "review_meeting" && status !== "closed") summaryCounters.open_review_meeting_count += 1;
    const scopeStr = normalizeScope(meeting.scope);
    const captured_at = normStr(meeting.updated_at) || normStr(meeting.created_at) || (await statMtimeIso(meetingJsonAbs));
    const scope = ensureScope(scopeMap, scopeStr);
    setLatestArtifact(scope, kind === "update_meeting" ? "latest_update_meeting" : "latest_review_meeting", {
      name: `${dirName}/MEETING.json`,
      url: artifactPathBuilder({ projectCode, kind, name: `${dirName}/MEETING.json` }),
      captured_at: captured_at || null,
    });
  }

  const writerRootAbs = join(paths.laneA.logsAbs, "writer_artifacts");
  const writerStatuses = await collectWriterStatusFiles(writerRootAbs, 6);
  for (const item of writerStatuses) {
    // eslint-disable-next-line no-await-in-loop
    const json = await readJsonOptional(item.abs);
    if (!json) continue;
    const scopeStr = normalizeScope(json.scope);
    const scope = ensureScope(scopeMap, scopeStr);
    mergeScopeFlags(scope, normalizeFlags(json));
    const mdAbs = join(writerRootAbs, item.rel.replace(/WRITER_STATUS\.json$/, "STATUS.md"));
    const chosenAbs = existsSync(mdAbs) ? mdAbs : item.abs;
    const chosenName = existsSync(mdAbs) ? item.rel.replace(/WRITER_STATUS\.json$/, "STATUS.md") : item.rel;
    const captured_at = (await statMtimeIso(chosenAbs)) || null;
    setLatestArtifact(scope, "latest_writer_status", {
      name: chosenName,
      url: artifactPathBuilder({ projectCode, kind: "writer", name: chosenName }),
      captured_at,
    });
  }

  const scopes = Array.from(scopeMap.values()).sort((a, b) => {
    if (a.scope === "system") return -1;
    if (b.scope === "system") return 1;
    return a.scope.localeCompare(b.scope);
  });

  const allTimestamps = scopes
    .map((s) => latestTimestampFromScope(s))
    .filter(Boolean)
    .map((iso) => Date.parse(String(iso || "")))
    .filter((ms) => Number.isFinite(ms));
  const last_seen_at = allTimestamps.length ? new Date(Math.max(...allTimestamps)).toISOString() : null;

  return {
    project_code: projectCode,
    summary: {
      hard_stale: scopes.some((s) => s.hard_stale === true),
      stale: scopes.some((s) => s.stale === true),
      degraded: scopes.some((s) => s.degraded === true),
      last_seen_at,
      open_update_meeting_count: summaryCounters.open_update_meeting_count,
      open_review_meeting_count: summaryCounters.open_review_meeting_count,
      open_decision_packets_count: summaryCounters.open_decision_packets_count,
    },
    scopes,
  };
}

function badge(flag, trueLabel) {
  if (flag) return `<span class="badge badge-on">${htmlEsc(trueLabel)}</span>`;
  return `<span class="badge badge-off">no</span>`;
}

function renderArtifactLink(artifact) {
  if (!artifact || !artifact.url) return "none";
  const ts = normStr(artifact.captured_at) || "unknown";
  return `<a href="${htmlEsc(artifact.url)}">${htmlEsc(artifact.name)}</a><div class="ts">${htmlEsc(ts)}</div>`;
}

function renderSummaryLinks(project) {
  const latest = {
    refresh: pickLatestArtifact(project, "latest_refresh_hint"),
    decision: pickLatestArtifact(project, "latest_decision_packet"),
    update: pickLatestArtifact(project, "latest_update_meeting"),
    review: pickLatestArtifact(project, "latest_review_meeting"),
    committee: pickLatestArtifact(project, "latest_committee_status"),
    writer: pickLatestArtifact(project, "latest_writer_status"),
  };
  return [
    `<div>${latest.refresh ? `<a href="${htmlEsc(latest.refresh.url)}">latest refresh hint</a>` : "latest refresh hint: none"}</div>`,
    `<div>${latest.decision ? `<a href="${htmlEsc(latest.decision.url)}">latest decision packet</a>` : "latest decision packet: none"}</div>`,
    `<div>${latest.update ? `<a href="${htmlEsc(latest.update.url)}">latest update meeting</a>` : "latest update meeting: none"}</div>`,
    `<div>${latest.review ? `<a href="${htmlEsc(latest.review.url)}">latest review meeting</a>` : "latest review meeting: none"}</div>`,
    `<div>${latest.committee ? `<a href="${htmlEsc(latest.committee.url)}">latest committee report</a>` : "latest committee report: none"}</div>`,
    `<div>${latest.writer ? `<a href="${htmlEsc(latest.writer.url)}">latest writer report</a>` : "latest writer report: none"}</div>`,
  ].join("");
}

export function renderLaneAHealthHtml({ payload, selectedProject = "" } = {}) {
  const projects = Array.isArray(payload?.projects) ? payload.projects : [];
  const projectCodes = projects.map((p) => normStr(p.project_code)).filter(Boolean).sort((a, b) => a.localeCompare(b));

  const lines = [];
  lines.push("<!doctype html>");
  lines.push("<html><head><meta charset=\"utf-8\"/>");
  lines.push("<title>Lane A Health</title>");
  lines.push("<style>");
  lines.push("body{font-family:Arial,sans-serif;margin:16px;color:#111} table{border-collapse:collapse;width:100%;margin:12px 0} th,td{border:1px solid #ddd;padding:8px;vertical-align:top} th{background:#f6f7f9;text-align:left} .badge{padding:2px 8px;border-radius:10px;font-size:12px;font-weight:700} .badge-on{background:#fde68a;color:#92400e} .badge-off{background:#e5e7eb;color:#374151} .section{margin-top:20px} .ts{font-size:11px;color:#555} .filters{margin:8px 0 14px 0} .nav{display:flex;gap:12px;align-items:center;margin:10px 0 14px 0} .nav a{color:#1d4ed8;text-decoration:none} .nav a:hover{text-decoration:underline} .nav a.active{color:#111;background:#dbeafe;border:1px solid #93c5fd;border-radius:8px;padding:6px 10px;text-decoration:none}");
  lines.push("</style>");
  lines.push("</head><body>");
  lines.push("<h1>Lane A Health</h1>");
  lines.push("<div class=\"nav\">");
  lines.push("<strong>AI-Team Web</strong>");
  lines.push("<a href=\"/intake\">Intake</a>");
  lines.push("<a href=\"/interview\">Interview</a>");
  lines.push("<a href=\"/projects\">Projects</a>");
  lines.push("<a href=\"/lane-a/health\" class=\"active\">Lane A Status</a>");
  lines.push("</div>");
  lines.push(`<div>Generated: ${htmlEsc(normStr(payload?.generated_at) || "unknown")}</div>`);

  if (projectCodes.length > 1) {
    lines.push("<div class=\"filters\"><form method=\"GET\" action=\"/lane-a/health\">");
    lines.push("<label for=\"project\">Project:</label> ");
    lines.push("<select id=\"project\" name=\"project\">");
    lines.push(`<option value=""${selectedProject ? "" : " selected"}>All</option>`);
    for (const code of projectCodes) {
      const selected = code === selectedProject ? " selected" : "";
      lines.push(`<option value="${htmlEsc(code)}"${selected}>${htmlEsc(code)}</option>`);
    }
    lines.push("</select> <input type=\"hidden\" name=\"format\" value=\"html\"/> <button type=\"submit\">Apply</button>");
    lines.push("</form></div>");
  }

  lines.push("<div class=\"section\"><h2>Project Summary</h2>");
  lines.push("<table><thead><tr>");
  lines.push("<th>project_code</th><th>last_seen_at</th><th>hard_stale</th><th>stale</th><th>degraded</th><th>open_update_meeting?</th><th>open_decision_packets?</th><th>links</th>");
  lines.push("</tr></thead><tbody>");
  if (!projects.length) {
    lines.push("<tr><td colspan=\"8\">none</td></tr>");
  } else {
    for (const p of projects) {
      const s = p.summary || {};
      lines.push("<tr>");
      lines.push(`<td>${htmlEsc(p.project_code)}</td>`);
      lines.push(`<td>${htmlEsc(normStr(s.last_seen_at) || "none")}</td>`);
      lines.push(`<td>${badge(s.hard_stale === true, "hard")}</td>`);
      lines.push(`<td>${badge(s.stale === true, "stale")}</td>`);
      lines.push(`<td>${badge(s.degraded === true, "degraded")}</td>`);
      lines.push(`<td>${s.open_update_meeting_count > 0 ? "yes" : "no"} (${Number(s.open_update_meeting_count || 0)})</td>`);
      lines.push(`<td>${Number(s.open_decision_packets_count || 0)}</td>`);
      lines.push(`<td>${renderSummaryLinks(p)}</td>`);
      lines.push("</tr>");
    }
  }
  lines.push("</tbody></table></div>");

  lines.push("<div class=\"section\"><h2>Details Per Scope</h2>");
  for (const p of projects) {
    lines.push(`<h3>${htmlEsc(p.project_code)}</h3>`);
    lines.push("<table><thead><tr>");
    lines.push("<th>scope</th><th>hard_stale</th><th>stale</th><th>degraded</th><th>reasons</th><th>latest_refresh_hint</th><th>latest_decision_packet</th><th>latest_update_meeting</th><th>latest_review_meeting</th><th>latest_committee_status</th><th>latest_writer_status</th>");
    lines.push("</tr></thead><tbody>");
    const scopes = Array.isArray(p.scopes) ? p.scopes : [];
    if (!scopes.length) {
      lines.push("<tr><td colspan=\"11\">none</td></tr>");
    } else {
      for (const scope of scopes) {
        lines.push("<tr>");
        lines.push(`<td>${htmlEsc(scope.scope)}</td>`);
        lines.push(`<td>${badge(scope.hard_stale === true, "hard")}</td>`);
        lines.push(`<td>${badge(scope.stale === true, "stale")}</td>`);
        lines.push(`<td>${badge(scope.degraded === true, "degraded")}</td>`);
        lines.push(`<td>${htmlEsc((Array.isArray(scope.reasons) ? scope.reasons : []).join(", ") || "none")}</td>`);
        lines.push(`<td>${renderArtifactLink(scope.artifacts?.latest_refresh_hint)}</td>`);
        lines.push(`<td>${renderArtifactLink(scope.artifacts?.latest_decision_packet)}</td>`);
        lines.push(`<td>${renderArtifactLink(scope.artifacts?.latest_update_meeting)}</td>`);
        lines.push(`<td>${renderArtifactLink(scope.artifacts?.latest_review_meeting)}</td>`);
        lines.push(`<td>${renderArtifactLink(scope.artifacts?.latest_committee_status)}</td>`);
        lines.push(`<td>${renderArtifactLink(scope.artifacts?.latest_writer_status)}</td>`);
        lines.push("</tr>");
      }
    }
    lines.push("</tbody></table>");
  }
  lines.push("</div>");

  lines.push("</body></html>");
  return lines.join("");
}

export async function buildLaneAHealthPayload({
  engineRoot,
  projectCode = "",
} = {}) {
  const root = resolve(String(engineRoot || process.cwd()));
  const regRes = await loadRegistry({ toolRepoRoot: root, createIfMissing: true });
  const projectFilter = normStr(projectCode);

  const sourceProjects = projectFilter
    ? (() => {
        const p = getRegistryProject(regRes.registry, projectFilter);
        return p && normStr(p.status).toLowerCase() === "active" ? [p] : [];
      })()
    : listProjects(regRes.registry)
        .filter((p) => normStr(p.status).toLowerCase() === "active")
        .map((p) => getRegistryProject(regRes.registry, p.project_code))
        .filter(Boolean);

  const projects = [];
  for (const p of sourceProjects.sort((a, b) => normStr(a.project_code).localeCompare(normStr(b.project_code)))) {
    // eslint-disable-next-line no-await-in-loop
    const health = await collectProjectHealth({
      project: p,
      artifactPathBuilder: ({ projectCode: code, kind, name }) => artifactUrl({ projectCode: code, kind, name }),
    });
    projects.push(health);
  }

  return {
    version: 1,
    generated_at: nowISO(),
    projects,
  };
}

function allowedBaseForKind(paths, kindRaw) {
  const kind = normStr(kindRaw).toLowerCase();
  if (kind === "refresh_hint") return paths.laneA.refreshHintsAbs;
  if (kind === "decision_packet") return paths.laneA.decisionPacketsAbs;
  if (kind === "update_meeting") return paths.laneA.meetingsAbs;
  if (kind === "review_meeting") return paths.laneA.meetingsAbs;
  if (kind === "committee") return paths.knowledge.ssotAbs;
  if (kind === "writer") return join(paths.laneA.logsAbs, "writer_artifacts");
  return null;
}

function isSafeRelName(name) {
  const n = normStr(name);
  if (!n) return false;
  if (n.includes("\0")) return false;
  if (n.startsWith("/") || n.startsWith("\\")) return false;
  const parts = n.split(/[\\/]+/g).filter(Boolean);
  if (!parts.length) return false;
  return !parts.some((p) => p === "." || p === "..");
}

export async function resolveLaneAArtifact({
  engineRoot,
  projectCode,
  kind,
  name,
} = {}) {
  const root = resolve(String(engineRoot || process.cwd()));
  const code = normStr(projectCode);
  if (!code) return { ok: false, status: 400, message: "Missing project." };
  if (!isSafeRelName(name)) return { ok: false, status: 400, message: "Invalid artifact name." };

  const regRes = await loadRegistry({ toolRepoRoot: root, createIfMissing: true });
  const project = getRegistryProject(regRes.registry, code);
  if (!project || normStr(project.status).toLowerCase() !== "active") return { ok: false, status: 404, message: "Project not found." };

  const opsRoot = normStr(project.ops_dir) || (normStr(project.root_dir) ? resolve(project.root_dir, "ops") : "");
  if (!opsRoot || !isAbsolute(opsRoot)) return { ok: false, status: 404, message: "Project paths unavailable." };

  let paths = null;
  try {
    paths = await loadProjectPaths({ projectRoot: opsRoot });
  } catch {
    return { ok: false, status: 404, message: "Project paths unavailable." };
  }

  const baseAbs = allowedBaseForKind(paths, kind);
  if (!baseAbs) return { ok: false, status: 400, message: "Invalid artifact kind." };
  if (!existsSync(baseAbs)) return { ok: false, status: 404, message: "Artifact not found." };

  const requestedAbs = resolve(baseAbs, String(name));
  if (!pathInside(baseAbs, requestedAbs)) return { ok: false, status: 403, message: "Forbidden." };

  try {
    const [baseReal, fileReal] = await Promise.all([realpath(baseAbs), realpath(requestedAbs)]);
    if (!pathInside(baseReal, fileReal)) return { ok: false, status: 403, message: "Forbidden." };
    const st = await stat(fileReal);
    if (!st.isFile()) return { ok: false, status: 404, message: "Artifact not found." };
    const content = await readFile(fileReal, "utf8");
    const ext = extname(fileReal).toLowerCase();
    const contentType = ext === ".json" ? "application/json; charset=utf-8" : "text/plain; charset=utf-8";
    return {
      ok: true,
      status: 200,
      contentType,
      body: content,
      name: toRelPosix(baseReal, fileReal),
    };
  } catch {
    return { ok: false, status: 404, message: "Artifact not found." };
  }
}

export function registerLaneAHealthRoutes(app, { engineRoot, authMiddleware = null } = {}) {
  const middleware = typeof authMiddleware === "function" ? authMiddleware : (_req, _res, next) => next();

  app.get("/lane-a/health", middleware, async (req, res) => {
    const projectCode = typeof req.query?.project === "string" ? req.query.project : "";
    const format = normStr(req.query?.format).toLowerCase() || "html";
    try {
      const payload = await buildLaneAHealthPayload({ engineRoot, projectCode });
      if (format === "json") return res.status(200).json(payload);
      const html = renderLaneAHealthHtml({ payload, selectedProject: normStr(projectCode) });
      return res.status(200).type("html").send(html);
    } catch (err) {
      return res.status(500).json({ ok: false, message: err instanceof Error ? err.message : "lane-a-health failed" });
    }
  });

  app.get("/lane-a/artifact", middleware, async (req, res) => {
    const projectCode = typeof req.query?.project === "string" ? req.query.project : "";
    const kind = typeof req.query?.kind === "string" ? req.query.kind : "";
    const name = typeof req.query?.name === "string" ? req.query.name : "";
    try {
      const r = await resolveLaneAArtifact({ engineRoot, projectCode, kind, name });
      if (!r.ok) return res.status(r.status || 400).json({ ok: false, message: r.message || "Artifact access failed." });
      return res.status(200).type(r.contentType || "text/plain; charset=utf-8").send(r.body || "");
    } catch {
      return res.status(500).json({ ok: false, message: "Artifact access failed." });
    }
  });
}
