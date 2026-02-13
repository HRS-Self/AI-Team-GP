import test from "node:test";
import assert from "node:assert/strict";

import { normalizeLlmContentToText } from "../src/llm/content.js";

test("normalizeLlmContentToText returns string content unchanged", async () => {
  const res = normalizeLlmContentToText("{\"ok\":true}");
  assert.deepEqual(res, { text: "{\"ok\":true}", debug_json: null });
});

test("normalizeLlmContentToText extracts text from block array", async () => {
  const content = [{ type: "text", text: "{\"a\":1}" }];
  const res = normalizeLlmContentToText(content);
  assert.equal(res.text, "{\"a\":1}");
  assert.ok(typeof res.debug_json === "string" && res.debug_json.includes("\"text\""));
});

