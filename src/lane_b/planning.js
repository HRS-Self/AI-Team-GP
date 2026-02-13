import { normalizeWhitespace } from "../utils/text.js";

const ROUTING_CONFIDENCE_THRESHOLD = 0.6;

const DOMAIN_RULES = {
  idp: [/(idp|identity|oauth|oidc|sso|auth)\b/i],
  backend: [/\b(nest|backend|api|service|endpoint|controller)\b/i],
  portal: [/\b(portal|next\.js|nextjs|frontend|ui|dashboard)\b/i],
  "mobile:rn": [/\b(react native|rn)\b/i],
  "mobile:android": [/\b(android|kotlin|gradle)\b/i],
  devops: [/\b(ci|github actions|pipeline|deploy|helm|k8s|terraform)\b/i],
  qa: [/\b(test|qa|e2e|integration|regression)\b/i],
};

function parseIntakeDirectives(rawText) {
  const lines = rawText.split("\n");
  const tags = {};
  const resolvedDecisions = [];
  const allowedKeys = new Set(["boundedcontext", "securityboundary", "domains", "domain", "resolveddecision", "resolvedecision", "resolvedecisions"]);

  for (const line of lines) {
    const m = line.match(/^\s*([A-Za-z][A-Za-z0-9_ -]{0,40}):\s*(.+?)\s*$/);
    if (!m) continue;
    const key = m[1].toLowerCase().replace(/\s+/g, "");
    const value = m[2];
    if (!allowedKeys.has(key)) continue;

    if (key === "resolveddecision" || key === "resolvedecision" || key === "resolvedecisions") {
      const [matchText, resolutionText] = value.split("=>").map((s) => s.trim());
      resolvedDecisions.push({ matchText, resolutionText: resolutionText || null });
      continue;
    }

    tags[key] = value;
  }

  return { tags, resolvedDecisions };
}

function scoreDomains(text) {
  const scores = {};
  for (const [domain, rules] of Object.entries(DOMAIN_RULES)) {
    scores[domain] = rules.reduce((sum, re) => sum + (re.test(text) ? 1 : 0), 0);
  }
  return scores;
}

function topDomains(scores) {
  const entries = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  const [topDomain, topScore] = entries[0] || [null, 0];
  const [, secondScore] = entries[1] || [null, 0];
  return { entries, topDomain, topScore, secondScore };
}

function confidenceFromScores({ topScore, secondScore }) {
  if (!topScore) return 0;
  const raw = (topScore - secondScore) / topScore;
  return Math.max(0, Math.min(1, raw));
}

function detectHighRiskAreas(text) {
  const t = text.toLowerCase();
  const areas = [];

  if (/\b(idp|oauth|oidc|sso|auth|jwt|token)\b/.test(t) || /\b(encrypt|encryption|crypto|key|kms|hsm)\b/.test(t)) {
    areas.push("idp_auth_crypto");
  }
  if (/\b(compiler|packager|packaging|bytecode|transpil|bundle)\b/.test(t)) {
    areas.push("compiler_packager");
  }
  if (/\b(cross[- ]service|api contract|contract|breaking change|versioning|schema|openapi|proto)\b/.test(t)) {
    areas.push("cross_service_contract");
  }

  return areas;
}

function parseExplicitDomains(tags) {
  const raw = tags.domains || tags.domain;
  if (!raw) return null;
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function explicitScopeFromTags(tags) {
  const boundedContext = tags.boundedcontext || null;
  const securityBoundary = tags.securityboundary || null;
  return { boundedContext, securityBoundary };
}

export function planFromIntake(intake) {
  const rawText = intake.text;
  const text = normalizeWhitespace(rawText);

  const { tags, resolvedDecisions } = parseIntakeDirectives(rawText);
  const explicitDomains = parseExplicitDomains(tags);
  const explicitScope = explicitScopeFromTags(tags);

  const highRiskAreas = detectHighRiskAreas(text);
  const requiresExplicitTags = highRiskAreas.length > 0;
  const missingExplicitTags = requiresExplicitTags && (!explicitScope.boundedContext || !explicitScope.securityBoundary);

  const scores = scoreDomains(text);
  const ranked = topDomains(scores);
  const detectedDomains = ranked.entries.filter(([, s]) => s > 0).map(([d]) => d);
  const confidence = explicitDomains ? 1 : confidenceFromScores(ranked);

  const domains = explicitDomains && explicitDomains.length ? explicitDomains : detectedDomains;
  const primaryDomain = domains[0] || ranked.topDomain || "backend";

  const requiresConfirmation =
    missingExplicitTags || (!explicitDomains && confidence < ROUTING_CONFIDENCE_THRESHOLD && detectedDomains.length > 1);

  const decisionsNeeded = [];
  const risks = [];

  if (missingExplicitTags) {
    decisionsNeeded.push("**Routing tags**: Provide `BoundedContext:` and `SecurityBoundary:` for this high-risk intake.");
  }
  if (!explicitDomains && confidence < ROUTING_CONFIDENCE_THRESHOLD && detectedDomains.length > 1) {
    decisionsNeeded.push(`**Routing confirmation**: Confirm primary domain/team (detected: ${detectedDomains.join(", ")}).`);
  }
  if (highRiskAreas.includes("cross_service_contract")) {
    decisionsNeeded.push("**API contract risk**: Confirm whether any cross-service/API contract changes are involved and how they will be versioned.");
    risks.push({
      category: "Technical",
      text: "Cross-service/API contract changes can cause breaking downstream impact; require compatibility/versioning plan and staged rollout.",
    });
  }
  if (highRiskAreas.includes("compiler_packager")) {
    decisionsNeeded.push("**Compiler/packager**: Confirm the owning bounded context and required reviewers for proprietary build/packager changes.");
    risks.push({
      category: "Technical",
      text: "Proprietary compiler/packager changes can invalidate build outputs; require explicit ownership review and reproducible verification via CI.",
    });
  }
  if (highRiskAreas.includes("idp_auth_crypto")) {
    risks.push({
      category: "Technical",
      text: "Auth/crypto/IDP changes are high-risk; require architecture/security review and careful rollout/rollback planning.",
    });
  }

  const items = requiresConfirmation
    ? [
        {
          title: "Triage: confirm routing + required tags",
          domain: "backend",
          kind: "analysis",
          blocked: false,
        },
      ]
    : [
        {
          title: "Clarify scope and acceptance criteria",
          domain: primaryDomain,
          kind: "analysis",
        },
        {
          title: "Implement scoped code change",
          domain: primaryDomain,
          kind: "implementation",
        },
        {
          title: "Add/update tests for change",
          domain: domains.includes("qa") ? "qa" : primaryDomain,
          kind: "testing",
        },
        {
          title: "CI gate + report outcome",
          domain: "devops",
          kind: "gate",
        },
      ];

  const markdown = [
    "# PLAN",
    "",
    "## Intake",
    "",
    `- Work item: ${intake.id}`,
    "",
    "## Routing",
    "",
    `- Detected domains: ${detectedDomains.length ? detectedDomains.join(", ") : "none (defaulted)"}`,
    `- Confidence: ${confidence.toFixed(2)} (threshold ${ROUTING_CONFIDENCE_THRESHOLD.toFixed(2)})`,
    `- High-risk areas: ${highRiskAreas.length ? highRiskAreas.join(", ") : "none"}`,
    `- Requires confirmation: ${requiresConfirmation ? "yes" : "no"}`,
    "",
    "## Plan items",
    "",
    ...items.map((it, idx) => `${idx + 1}. [${it.domain}] (${it.kind}) ${it.title}`),
    "",
    "## Notes",
    "",
    "- All assignments must declare a bounded context / security boundary before making changes.",
    ...(requiresExplicitTags ? ["- High-risk policy: require explicit tags for bounded context and security boundary (no guessing)."] : []),
    ...(decisionsNeeded.length ? ["", "## Decisions needed", "", ...decisionsNeeded.map((d) => `- ${d}`)] : []),
    "",
  ].join("\n");

  return {
    items,
    markdown,
    routing: {
      detectedDomains,
      explicitDomains,
      confidence,
      threshold: ROUTING_CONFIDENCE_THRESHOLD,
      requiresConfirmation,
      highRiskAreas,
      explicitScope,
      tags,
    },
    risks,
    decisionsNeeded,
    resolvedDecisions,
  };
}
