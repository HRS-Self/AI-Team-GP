const AGENTS = {
  backend: [
    {
      slot: "backend-1",
      specialties: ["service", "api", "nest", "idp"],
      maxBoundaries: ["identity", "service"],
    },
    {
      slot: "backend-2",
      specialties: ["service", "api"],
      maxBoundaries: ["service"],
    },
  ],
  portal: [
    {
      slot: "portal-1",
      specialties: ["nextjs", "frontend", "ui"],
      maxBoundaries: ["frontend"],
    },
  ],
  mobile: [
    {
      slot: "mobile-1",
      specialties: ["react-native", "android"],
      maxBoundaries: ["mobile"],
    },
  ],
  devops: [
    {
      slot: "devops-1",
      specialties: ["github-actions", "ci", "cd", "infra"],
      maxBoundaries: ["infrastructure"],
    },
  ],
  qa: [
    {
      slot: "qa-1",
      specialties: ["tests", "e2e", "integration"],
      maxBoundaries: ["quality"],
    },
  ],
};

function scoreAgent(agent, { domain, securityBoundary }) {
  let score = 0;
  const d = domain.toLowerCase();
  if (agent.specialties.some((s) => d.includes(s))) score += 2;
  if (agent.maxBoundaries.includes(securityBoundary)) score += 1;
  return score;
}

export function pickAgentSlot({ team, domain, scope }) {
  const list = AGENTS[team] || [];
  if (!list.length) return null;

  const securityBoundary = scope?.securityBoundary || "unknown";
  const ranked = [...list]
    .map((agent) => ({ agent, score: scoreAgent(agent, { domain, securityBoundary }) }))
    .sort((a, b) => b.score - a.score);

  return ranked[0]?.agent?.slot || list[0].slot;
}

