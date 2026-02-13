import { readTextIfExists } from "../../utils/fs.js";

function firstNLines(text, n) {
  return String(text || "")
    .split("\n")
    .slice(0, Math.max(0, Number(n) || 0))
    .join("\n");
}

async function readLatestCiFeedback({ workDir }) {
  const statusText = await readTextIfExists(`${workDir}/CI/CI_Status.json`);
  if (!statusText) return { ok: true, exists: false, status: null, feedback_json: null, feedback_md: null };
  let status = null;
  try {
    status = JSON.parse(statusText);
  } catch {
    return { ok: false, message: `Invalid JSON in ${workDir}/CI/CI_Status.json.` };
  }
  const base = typeof status?.latest_feedback === "string" && status.latest_feedback.trim() ? status.latest_feedback.trim() : null;
  const feedbackJsonText = base ? await readTextIfExists(`${workDir}/CI/${base}.json`) : null;
  const feedbackMdText = base ? await readTextIfExists(`${workDir}/CI/${base}.md`) : null;
  return { ok: true, exists: true, status, feedback_base: base, feedback_json: feedbackJsonText, feedback_md: feedbackMdText };
}

export async function buildWorkScopedPrCiContextPack({ workDir }) {
  const prText = await readTextIfExists(`${workDir}/PR.json`);
  if (!prText) return null;

  const ci = await readLatestCiFeedback({ workDir });
  if (!ci.ok) return `=== PR/CI CONTEXT (work-scoped) ===\n${ci.message || "CI context read failed."}\n`;

  const blocks = [];
  blocks.push("=== PR/CI CONTEXT (work-scoped) ===");
  blocks.push(`PR.json: ${workDir}/PR.json`);
  blocks.push("```json");
  blocks.push(firstNLines(prText, 200));
  blocks.push("```");

  if (ci.exists) {
    blocks.push("");
    blocks.push(`CI/CI_Status.json: ${workDir}/CI/CI_Status.json`);
    blocks.push("```json");
    blocks.push(firstNLines(JSON.stringify(ci.status, null, 2), 260));
    blocks.push("```");
    if (ci.feedback_base && ci.feedback_json) {
      blocks.push("");
      blocks.push(`CI feedback (json): ${workDir}/CI/${ci.feedback_base}.json`);
      blocks.push("```json");
      blocks.push(firstNLines(ci.feedback_json, 220));
      blocks.push("```");
    }
    if (ci.feedback_base && ci.feedback_md) {
      blocks.push("");
      blocks.push(`CI feedback (md): ${workDir}/CI/${ci.feedback_base}.md`);
      blocks.push("```");
      blocks.push(firstNLines(ci.feedback_md, 180));
      blocks.push("```");
    }
  } else {
    blocks.push("");
    blocks.push("(No CI/CI_Status.json present yet.)");
  }

  blocks.push("");
  return blocks.join("\n");
}
