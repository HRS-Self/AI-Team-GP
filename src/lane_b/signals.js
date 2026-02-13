function touchesSensitiveBoundary({ assignments }) {
  return assignments.some((a) => ["identity", "service"].includes(a.scope.securityBoundary));
}

function needsTechWriting({ intake, plan }) {
  const t = intake.text.toLowerCase();
  if (/(docs|documentation|readme|runbook|guide)/.test(t)) return true;
  return plan.items.some((p) => p.kind === "analysis");
}

export function evaluateGovernanceSignals({ intake, plan, assignments }) {
  const signals = [];

  if (touchesSensitiveBoundary(assignments)) {
    signals.push({
      type: "architect_review_requested",
      reason: "Work touches identity/service boundary; require architecture/security review for scope and boundaries.",
    });
  }

  if (plan?.routing?.highRiskAreas?.length) {
    signals.push({
      type: "architect_review_requested",
      reason: `High-risk area detected (${plan.routing.highRiskAreas.join(", ")}); require architecture/security review before execution.`,
    });
  }

  if (needsTechWriting({ intake, plan })) {
    signals.push({
      type: "tech_writer_requested",
      reason: "Work includes analysis/clarification or explicitly mentions documentation.",
    });
  }

  return { signals };
}
