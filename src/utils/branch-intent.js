function normalizeBranchName(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  if (lower === "main" || lower === "master" || lower === "develop" || lower === "dev") return lower === "dev" ? "develop" : lower;
  return s;
}

function extractExplicitTargetBranchFromIntake(intakeText) {
  const t = String(intakeText || "");

  const shorthand = t.match(/\b(main|master|develop)\s+branch\b/i);
  if (shorthand) {
    const raw = shorthand[0];
    const name = normalizeBranchName(shorthand[1]);
    return name ? { name, source: "explicit", matched_token: raw, confidence: 1.0 } : null;
  }

  const shorthand2 = t.match(/\b(?:in|on|against)\s+(?:the\s+)?(main|master|develop|dev)\b(?!\s*branch)\b/i);
  if (shorthand2) {
    const raw = shorthand2[0];
    const name = normalizeBranchName(shorthand2[1]);
    return name ? { name, source: "explicit", matched_token: raw, confidence: 1.0 } : null;
  }

  const patterns = [
    /\btarget\s+branch\s*[:=]\s*([A-Za-z0-9._/-]+)\b/i,
    /\b(?:in|on|against)\s+(?:the\s+)?([A-Za-z0-9._/-]+)\s+branch\b/i,
    /\b(?:in|on|against)\s+(?:the\s+)?branch\s+([A-Za-z0-9._/-]+)\b/i,
    /\bcheckout\s+([A-Za-z0-9._/-]+)\b/i,
  ];

  for (const re of patterns) {
    const m = t.match(re);
    if (!m) continue;
    const raw = m[0];
    const name = normalizeBranchName(m[1]);
    if (!name) continue;
    return { name, source: "explicit", matched_token: raw, confidence: 1.0 };
  }

  return null;
}

export { normalizeBranchName, extractExplicitTargetBranchFromIntake };
