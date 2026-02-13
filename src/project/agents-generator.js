import { jsonStableStringify } from "../utils/json.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function toInt(raw, fallback) {
  const s = typeof raw === "string" ? raw.trim() : String(raw ?? "").trim();
  if (!s) return fallback;
  const n = Number.parseInt(s, 10);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function pad2(n) {
  const x = String(n);
  return x.length >= 2 ? x : `0${x}`;
}

export function generateAgentsConfig({
  teamsConfig,
  plannersPerTeam = 1,
  createApplierPerTeam = true,
  createWriterAgent = true,
  createQaStrategistAgent = true,
} = {}) {
  const teams = Array.isArray(teamsConfig?.teams) ? teamsConfig.teams : [];
  const teamIds = teams
    .map((t) => String(t?.team_id || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  const plannerCount = Math.max(1, toInt(plannersPerTeam, 1));

  const agents = [];

  for (const teamId of teamIds) {
    for (let i = 1; i <= plannerCount; i += 1) {
      agents.push({
        agent_id: `${teamId}__planner__${pad2(i)}`,
        team_id: teamId,
        role: "planner",
        implementation: "llm",
        llm_profile: "planner.code_generation",
        capacity: 1,
        enabled: true,
      });
    }

    if (createApplierPerTeam) {
      agents.push({
        agent_id: `${teamId}__applier__01`,
        team_id: teamId,
        role: "applier",
        implementation: "code",
        capacity: 1,
        enabled: true,
      });
    }
  }

  // Project-scoped Technical Writer agent (LLM) used for Phase 8 docs generation.
  // Deterministic team_id assignment: prefer Tooling if present, otherwise first team.
  if (createWriterAgent) {
    const writerTeamId = teamIds.includes("Tooling") ? "Tooling" : teamIds[0] || null;
    if (writerTeamId) {
      agents.push({
        agent_id: "TechnicalWriter__writer__01",
        team_id: writerTeamId,
        role: "writer",
        implementation: "llm",
        llm_profile: "tech.writer",
        capacity: 1,
        enabled: true,
      });
    }
  }

  // Project-scoped QA Inspector agent (LLM) used for Lane B QA planning artifacts.
  // Deterministic: only generate if the project declares a QA team.
  if (createQaStrategistAgent && teamIds.includes("QA")) {
    agents.push({
      agent_id: "QA__inspector__01",
      team_id: "QA",
      role: "qa_inspector",
      implementation: "llm",
      llm_profile: "qa.inspector",
      capacity: 1,
      enabled: true,
    });
  }

  agents.sort((a, b) => String(a.agent_id).localeCompare(String(b.agent_id)));

  return {
    version: 3,
    agents,
  };
}

export function validateAgentsConfigCoversTeams({ teamsConfig, agentsConfig }) {
  const errors = [];

  const teams = Array.isArray(teamsConfig?.teams) ? teamsConfig.teams : [];
  const teamIds = teams
    .map((t) => String(t?.team_id || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));

  if (!isPlainObject(agentsConfig) || agentsConfig.version !== 3 || !Array.isArray(agentsConfig.agents)) {
    return { ok: false, errors: ["Invalid config/AGENTS.json: expected {version:3, agents:[...]} (run: node src/cli.js --agents-migrate)."] };
  }

  const enabled = agentsConfig.agents.filter((a) => a && a.enabled === true);
  const byTeam = new Map();

  for (const a of enabled) {
    const teamId = String(a.team_id || "").trim();
    if (!teamId) continue;

    const roleRaw = String(a.role || "").trim();
    const implRaw = String(a.implementation || "").trim();

    if (Object.prototype.hasOwnProperty.call(a, "model")) errors.push(`AGENTS.json contains legacy key 'model' for agent_id ${String(a.agent_id || "")}. Run: node src/cli.js --agents-migrate`);
    if (String(implRaw || "").trim() === "llm") {
      const prof = typeof a.llm_profile === "string" ? a.llm_profile.trim() : "";
      if (!prof) errors.push(`Agent ${String(a.agent_id || "")} is missing llm_profile (implementation=llm).`);
    }

    if (!byTeam.has(teamId)) byTeam.set(teamId, new Set());
    if (roleRaw && implRaw) byTeam.get(teamId).add(`${roleRaw}:${implRaw}`);
  }

  for (const teamId of teamIds) {
    const roles = byTeam.get(teamId) || new Set();
    if (!roles.has("planner:llm")) errors.push(`AGENTS.json missing enabled planner (implementation=llm) for team_id ${teamId}.`);
  }

  return { ok: errors.length === 0, errors };
}

export function agentsConfigToText(config) {
  return jsonStableStringify(config, 2);
}
