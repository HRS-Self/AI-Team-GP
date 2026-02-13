import { readTextIfExists, writeText } from "./fs.js";

export async function appendStatusHistory({ statusPath, historyPath }) {
  const currentText = await readTextIfExists(statusPath);
  if (!currentText) return;
  let current;
  try {
    current = JSON.parse(currentText);
  } catch (err) {
    current = { parse_error: String(err?.message || err), raw: currentText };
  }
  let history = [];
  const historyText = await readTextIfExists(historyPath);
  if (historyText) {
    try {
      const parsed = JSON.parse(historyText);
      history = Array.isArray(parsed) ? parsed : [];
    } catch {
      history = [];
    }
  }
  history.push(current);
  await writeText(historyPath, JSON.stringify(history, null, 2) + "\n");
}
