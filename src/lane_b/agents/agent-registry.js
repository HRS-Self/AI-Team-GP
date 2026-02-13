import { readFileSync } from "node:fs";

import { resolveStatePath } from "../../project/state-paths.js";
import { AGENT_REGISTRY_PATH } from "../../utils/repo-registry.js";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normalizeAgentsConfig(parsed) {
  if (!isPlainObject(parsed) || !Array.isArray(parsed.agents)) return { ok: false, message: "Invalid config/AGENTS.json: expected an object with agents[] array." };

  const version = parsed.version;
  const out = [];

  if (version === 3) {
    for (const a of parsed.agents) {
      if (!isPlainObject(a)) continue;
      const agent_id = String(a.agent_id || "").trim();
      const team_id = String(a.team_id || "").trim();
      const role = String(a.role || "").trim();
      const implementation = String(a.implementation || "").trim();
      const enabled = a.enabled === true;
      const capacity = Number.isFinite(Number(a.capacity)) ? Number(a.capacity) : 1;
      const llm_profile = typeof a.llm_profile === "string" && a.llm_profile.trim() ? a.llm_profile.trim() : null;

      if (!agent_id || !team_id) continue;
      // Lane B QA role is qa_inspector. Accept legacy qa_strategist as an alias.
      const normalizedRole = role === "qa_strategist" ? "qa_inspector" : role;
      if (!["planner", "applier", "reviewer", "writer", "interviewer", "pr_description", "qa_inspector"].includes(normalizedRole)) continue;
      if (!["llm", "code"].includes(implementation)) continue;
      if (Object.prototype.hasOwnProperty.call(a, "model")) {
        return { ok: false, message: "AGENTS.json contains legacy key 'model'. Run: node src/cli.js --agents-migrate" };
      }
      out.push({ agent_id, team_id, role: normalizedRole, implementation, enabled, capacity: Math.max(1, capacity), llm_profile });
    }
    return { ok: true, version: 3, agents: out };
  }

  return { ok: false, message: "Unsupported config/AGENTS.json version (expected 3). Run: node src/cli.js --agents-migrate" };
}

function loadAgentsJsonNormalized() {
  try {
    const text = readFileSync(resolveStatePath(AGENT_REGISTRY_PATH, { requiredRoot: true }), "utf8");
    const parsed = JSON.parse(text);
    const norm = normalizeAgentsConfig(parsed);
    if (!norm.ok) return null;
    return norm;
  } catch {
    return null;
  }
}

export function agentsForTeam(teamId, { role = null, implementation = null } = {}) {
  const cfg = loadAgentsJsonNormalized();
  if (!cfg) return [];

  const team = String(teamId || "").trim();
  const requiredRole = role ? String(role).trim() : null;
  const requiredImpl = implementation ? String(implementation).trim() : null;

  const enabled = cfg.agents
    .filter((a) => a.enabled === true && a.team_id === team)
    .filter((a) => (requiredRole ? a.role === requiredRole : true))
    .filter((a) => (requiredImpl ? a.implementation === requiredImpl : true))
    .slice()
    .sort((a, b) => a.agent_id.localeCompare(b.agent_id));

  return enabled;
}
