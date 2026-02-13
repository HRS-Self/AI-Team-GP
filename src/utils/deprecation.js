const warned = new Set();

export function warnDeprecatedOnce(key, message) {
  const k = String(key || "").trim();
  if (!k) return;
  if (warned.has(k)) return;
  warned.add(k);
  process.stderr.write(`DEPRECATED: ${String(message || "").trim()}\n`);
}

