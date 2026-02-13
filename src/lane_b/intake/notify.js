import { createHash } from "node:crypto";
import { readTextIfExists, writeText } from "../../utils/fs.js";
import { nowTs } from "../../utils/id.js";

function sha256Hex(text) {
  return createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

function parseDecisionBlocks(decisionsMd) {
  const text = String(decisionsMd || "");
  if (text.includes("No pending decisions.")) return [];

  const lines = text.split("\n");
  const blocks = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line.startsWith("Work item: ")) continue;

    const workId = line.slice("Work item: ".length).trim();
    if (!workId) continue;

    let intake = null;
    let question = null;
    const options = { A: null, B: null };

    for (let j = i + 1; j < lines.length; j += 1) {
      const l = lines[j].trim();
      if (l.startsWith("Work item: ")) break;
      if (!l) continue;

      const intakeMatch = l.match(/^Intake:\s*(.+?)\s*$/);
      if (intakeMatch) {
        intake = intakeMatch[1].trim();
        continue;
      }

      const opt = l.match(/^\s*-\s*([AB])\s*:\s*(.+?)\s*$/);
      if (opt) {
        options[opt[1]] = opt[2];
        continue;
      }

      if (!question && !l.startsWith("- ")) {
        question = l;
      }
    }

    if (workId && question && options.A && options.B) {
      blocks.push({ workId, intake, question, options });
    }
  }

  return blocks;
}

function parseEscalatedWorkIds(portfolioMd) {
  const lines = String(portfolioMd || "").split("\n");
  const start = lines.findIndex((l) => l.trim() === "## ESCALATED");
  if (start === -1) return [];

  const out = [];
  for (let i = start + 1; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (line.startsWith("## ")) break;
    const m = line.match(/^\-\s+(W-[^\s]+)\s+\|/);
    if (m) out.push(m[1]);
  }
  return out;
}

function createStubNotifier() {
  return {
    provider: "stub",
    async send({ to, message }) {
      // eslint-disable-next-line no-console
      console.log(`[notify:stub] to=${to} message=${message}`);
      return { ok: true };
    },
  };
}

async function sendPlivo({ to, message }) {
  const authId = process.env.PLIVO_AUTH_ID;
  const authToken = process.env.PLIVO_AUTH_TOKEN;
  const from = process.env.PLIVO_FROM;

  if (!authId || !authToken || !from) {
    return { ok: false, error: "Missing PLIVO_AUTH_ID/PLIVO_AUTH_TOKEN/PLIVO_FROM." };
  }

  const url = `https://api.plivo.com/v1/Account/${encodeURIComponent(authId)}/Message/`;
  const basic = Buffer.from(`${authId}:${authToken}`).toString("base64");

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ src: from, dst: to, text: message }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    return { ok: false, error: `Plivo send failed: ${res.status} ${body}` };
  }

  return { ok: true };
}

async function sendTwilio({ to, message }) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  const from = process.env.TWILIO_FROM;

  if (!accountSid || !authToken || !from) {
    return { ok: false, error: "Missing TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN/TWILIO_FROM." };
  }

  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(accountSid)}/Messages.json`;
  const basic = Buffer.from(`${accountSid}:${authToken}`).toString("base64");

  const form = new URLSearchParams();
  form.set("From", from);
  form.set("To", to);
  form.set("Body", message);

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: form.toString(),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    return { ok: false, error: `Twilio send failed: ${res.status} ${body}` };
  }

  return { ok: true };
}

export function createNotifier() {
  const provider = String(process.env.NOTIFY_PROVIDER || "stub").trim().toLowerCase();
  if (provider === "plivo") {
    return {
      provider: "plivo",
      async send({ to, message }) {
        return await sendPlivo({ to, message });
      },
    };
  }
  if (provider === "twilio") {
    return {
      provider: "twilio",
      async send({ to, message }) {
        return await sendTwilio({ to, message });
      },
    };
  }
  return createStubNotifier();
}

async function readState() {
  const text = await readTextIfExists("ai/.intake_state.json");
  if (!text) {
    return {
      decisions_hash: null,
      pending_decision_workIds: [],
      escalated_workIds: [],
      updated_at: null,
    };
  }
  try {
    const obj = JSON.parse(text);
    return {
      decisions_hash: obj?.decisions_hash || null,
      pending_decision_workIds: Array.isArray(obj?.pending_decision_workIds) ? obj.pending_decision_workIds : [],
      escalated_workIds: Array.isArray(obj?.escalated_workIds) ? obj.escalated_workIds : [],
      updated_at: obj?.updated_at || null,
    };
  } catch {
    return {
      decisions_hash: null,
      pending_decision_workIds: [],
      escalated_workIds: [],
      updated_at: null,
    };
  }
}

async function writeState(state) {
  await writeText(
    "ai/.intake_state.json",
    JSON.stringify(
      {
        ...state,
        updated_at: nowTs(),
      },
      null,
      2,
    ) + "\n",
  );
}

function formatDecisionMessage(block) {
  const intake = block.intake ? ` — ${block.intake}` : "";
  return `AI-Team: Decision needed for ${block.workId}${intake}. Options: A) ${block.options.A} B) ${block.options.B}`;
}

export async function notifyIfNeeded() {
  const to = String(process.env.NOTIFY_TO || "").trim();

  const notifier = createNotifier();
  const prev = await readState();

  const decisionsMd = (await readTextIfExists("ai/lane_b/DECISIONS_NEEDED.md")) || "";
  const portfolioMd = (await readTextIfExists("ai/lane_b/PORTFOLIO.md")) || "";

  const decisionsHash = sha256Hex(decisionsMd);
  const currentDecisions = parseDecisionBlocks(decisionsMd);
  const currentDecisionIds = currentDecisions.map((d) => d.workId);

  const currentEscalated = parseEscalatedWorkIds(portfolioMd);

  const newDecisionIds = currentDecisionIds.filter((id) => !prev.pending_decision_workIds.includes(id));
  const newEscalations = currentEscalated.filter((id) => !prev.escalated_workIds.includes(id));

  if (!to) {
    await writeState({
      decisions_hash: decisionsHash,
      pending_decision_workIds: currentDecisionIds,
      escalated_workIds: currentEscalated,
      updated_at: prev.updated_at,
    });
    return {
      ok: true,
      provider: notifier.provider,
      sent: 0,
      decisions_changed: prev.decisions_hash !== decisionsHash,
      new_decisions: [],
      new_escalations: [],
      notes: ["NOTIFY_TO not set; skipping notifications."],
    };
  }

  let sent = 0;
  const errors = [];

  for (const id of newDecisionIds) {
    const block = currentDecisions.find((d) => d.workId === id);
    if (!block) continue;
    try {
      const res = await notifier.send({ to, message: formatDecisionMessage(block) });
      if (res.ok) sent += 1;
      else errors.push(res.error || `Failed to send decision notification for ${id}.`);
    } catch (err) {
      errors.push(`Failed to send decision notification for ${id}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  for (const id of newEscalations) {
    try {
      const res = await notifier.send({ to, message: `AI-Team: Escalation pending for ${id}.` });
      if (res.ok) sent += 1;
      else errors.push(res.error || `Failed to send escalation notification for ${id}.`);
    } catch (err) {
      errors.push(`Failed to send escalation notification for ${id}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  await writeState({
    decisions_hash: decisionsHash,
    pending_decision_workIds: currentDecisionIds,
    escalated_workIds: currentEscalated,
    updated_at: prev.updated_at,
  });

  return {
    ok: errors.length === 0,
    provider: notifier.provider,
    sent,
    decisions_changed: prev.decisions_hash !== decisionsHash,
    new_decisions: newDecisionIds,
    new_escalations: newEscalations,
    errors,
  };
}
