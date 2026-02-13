import { createHash } from "node:crypto";

export function sha256Hex(input) {
  const h = createHash("sha256");
  if (Buffer.isBuffer(input)) h.update(input);
  else h.update(String(input ?? ""), "utf8");
  return h.digest("hex");
}

