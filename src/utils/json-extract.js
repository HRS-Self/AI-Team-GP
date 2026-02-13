function normText(x) {
  return typeof x === "string" ? x : String(x ?? "");
}

function tryParseJson(text) {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (err) {
    return { ok: false, error: err };
  }
}

function extractFromCodeFence(text) {
  const t = normText(text).trim();
  if (!t) return null;
  const m = t.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  if (!m) return null;
  const inner = String(m[1] || "").trim();
  return inner || null;
}

function findFirstParsableJsonObject(text) {
  const t = normText(text);
  for (let start = t.indexOf("{"); start !== -1; start = t.indexOf("{", start + 1)) {
    let depth = 0;
    let inString = false;
    let escaped = false;
    for (let i = start; i < t.length; i += 1) {
      const ch = t[i];
      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }
        if (ch === "\\") {
          escaped = true;
          continue;
        }
        if (ch === "\"") {
          inString = false;
        }
        continue;
      }

      if (ch === "\"") {
        inString = true;
        continue;
      }

      if (ch === "{") {
        depth += 1;
        continue;
      }
      if (ch === "}") {
        depth -= 1;
        if (depth === 0) {
          const candidate = t.slice(start, i + 1).trim();
          const parsed = tryParseJson(candidate);
          if (parsed.ok) return { ok: true, text: candidate, value: parsed.value };
          break;
        }
      }
    }
  }
  return { ok: false, text: null, value: null };
}

export function parseJsonObjectFromText(rawText) {
  const direct = tryParseJson(normText(rawText).trim());
  if (direct.ok && direct.value && typeof direct.value === "object" && !Array.isArray(direct.value)) {
    return { ok: true, value: direct.value, extracted: false, mode: "direct" };
  }

  const fenced = extractFromCodeFence(rawText);
  if (fenced) {
    const parsed = tryParseJson(fenced);
    if (parsed.ok && parsed.value && typeof parsed.value === "object" && !Array.isArray(parsed.value)) {
      return { ok: true, value: parsed.value, extracted: true, mode: "code_fence" };
    }
  }

  const found = findFirstParsableJsonObject(rawText);
  if (found.ok) {
    return { ok: true, value: found.value, extracted: true, mode: "substring" };
  }

  return { ok: false, value: null, extracted: false, mode: "none" };
}

