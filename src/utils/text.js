export function normalizeWhitespace(text) {
  return text.replace(/\r\n/g, "\n").replace(/[ \t]+/g, " ").trim();
}

