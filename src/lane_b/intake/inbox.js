import { ensureDir, writeText } from "../../utils/fs.js";
import { intakeId, nowTs } from "../../utils/id.js";

export async function enqueueInboxNote({ text, source }) {
  const timestamp = nowTs();
  const id = intakeId({ timestamp, text: String(text || "") });

  await ensureDir("ai/lane_b/inbox");
  const path = `ai/lane_b/inbox/${id}.md`;

  const body = [
    `Intake: ${String(text || "").trim()}`,
    `Source: ${String(source || "web").trim()}`,
    `CreatedAt: ${timestamp}`,
    "",
  ].join("\n");

  await writeText(path, body);
  return { ok: true, intake_file: path, createdAt: timestamp };
}
