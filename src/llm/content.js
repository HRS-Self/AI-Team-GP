function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function safeStringify(x, maxBytes = 256 * 1024) {
  try {
    const text = JSON.stringify(x, null, 2);
    if (typeof text !== "string") return null;
    const limit = Math.max(1024, Number(maxBytes) || 0);
    return text.length > limit ? text.slice(0, limit) : text;
  } catch {
    return null;
  }
}

/**
 * LangChain message content can be:
 * - string
 * - array of blocks (e.g. [{type:"text", text:"..."}])
 * - object
 *
 * This normalizes it into text for downstream JSON parsing, while optionally
 * returning a debug JSON string for non-string content.
 */
export function normalizeLlmContentToText(content, { debugMaxBytes = 256 * 1024 } = {}) {
  if (typeof content === "string") return { text: content, debug_json: null };

  if (Array.isArray(content)) {
    const texts = [];
    for (const part of content) {
      if (typeof part === "string") {
        if (part) texts.push(part);
        continue;
      }
      if (isPlainObject(part)) {
        if (typeof part.text === "string" && part.text) texts.push(part.text);
        else if (typeof part.content === "string" && part.content) texts.push(part.content);
      }
    }
    const debug = safeStringify(content, debugMaxBytes);
    return { text: texts.join(""), debug_json: debug };
  }

  if (isPlainObject(content)) {
    const text = typeof content.text === "string" ? content.text : typeof content.content === "string" ? content.content : "";
    const debug = safeStringify(content, debugMaxBytes);
    return { text: text || "", debug_json: debug };
  }

  return { text: String(content ?? ""), debug_json: null };
}

