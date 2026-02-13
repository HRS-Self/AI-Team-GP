import { TEAMS } from "../teams/teams.js";
import { routeDomainToTeam } from "../teams/routing-rules.js";
import { pickAgentSlot } from "../teams/agents.js";

function scopeFromDomain(domain) {
  if (domain === "backend") return { boundedContext: "service:unknown", securityBoundary: "service" };
  if (domain === "portal") return { boundedContext: "portal", securityBoundary: "frontend" };
  if (domain.startsWith("mobile:")) return { boundedContext: domain, securityBoundary: "mobile" };
  if (domain === "idp") return { boundedContext: "idp", securityBoundary: "identity" };
  if (domain === "devops") return { boundedContext: "cicd", securityBoundary: "infrastructure" };
  if (domain === "qa") return { boundedContext: "quality", securityBoundary: "quality" };
  return { boundedContext: domain, securityBoundary: "unknown" };
}

export function buildAssignments(plan) {
  if (plan?.routing?.requiresConfirmation) {
    const domain = "backend";
    const teamKey = routeDomainToTeam(domain);
    const team = TEAMS[teamKey];
    const scope = plan?.routing?.explicitScope?.boundedContext && plan?.routing?.explicitScope?.securityBoundary
      ? {
          boundedContext: plan.routing.explicitScope.boundedContext,
          securityBoundary: plan.routing.explicitScope.securityBoundary,
        }
      : scopeFromDomain(domain);
    const agentSlot = pickAgentSlot({ team: teamKey, domain, scope });

    return {
      assignments: [
        {
          assignmentId: "1",
          team: teamKey,
          teamName: team.name,
          agentSlot,
          title: "Triage: confirm routing + required tags",
          domain,
          kind: "analysis",
          scope,
          constraints: {
            avoidCrossBoundaryChanges: true,
            requireCI: true,
          },
          outputs: {
            prRequired: false,
            docsRequired: true,
          },
          confirmationNeeded: {
            routing: {
              detectedDomains: plan.routing.detectedDomains,
              confidence: plan.routing.confidence,
              threshold: plan.routing.threshold,
              highRiskAreas: plan.routing.highRiskAreas,
            },
            requiredTags: plan.routing.highRiskAreas?.length ? ["BoundedContext", "SecurityBoundary"] : [],
          },
        },
      ],
    };
  }

  const assignments = plan.items.map((item, idx) => {
    const teamKey = routeDomainToTeam(item.domain);
    const team = TEAMS[teamKey];
    const scope = scopeFromDomain(item.domain);
    const agentSlot = pickAgentSlot({ team: teamKey, domain: item.domain, scope });

    return {
      assignmentId: `${idx + 1}`,
      team: teamKey,
      teamName: team.name,
      agentSlot,
      title: item.title,
      domain: item.domain,
      kind: item.kind,
      scope,
      constraints: {
        avoidCrossBoundaryChanges: true,
        requireCI: true,
      },
      outputs: {
        prRequired: item.kind !== "analysis",
        docsRequired: item.kind === "analysis",
      },
    };
  });

  return { assignments };
}
