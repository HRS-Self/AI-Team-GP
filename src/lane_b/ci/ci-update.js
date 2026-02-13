import { appendFile } from "../../utils/fs.js";
import { pollCiForWork } from "./ci-poller.js";

function nowISO() {
  return new Date().toISOString();
}

export async function runCiUpdate({ workId }) {
  const wid = String(workId || "").trim();
  if (!wid) return { ok: false, message: "Missing --workId <id>." };

  try {
    const res = await pollCiForWork({ workId: wid });
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "ci_polled", workId: wid, overall: res.ok ? res.overall : null }) + "\n");
    return res;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    await appendFile("ai/lane_b/ledger.jsonl", JSON.stringify({ timestamp: nowISO(), action: "ci_poll_failed", workId: wid, error: msg }) + "\n");
    return { ok: false, message: msg };
  }
}
