import test from "node:test";
import assert from "node:assert/strict";

import { parseJsonObjectFromText } from "../src/utils/json-extract.js";

test("parseJsonObjectFromText parses fenced JSON", async () => {
  const raw = "```json\n{\"a\":1,\"b\":{\"c\":2}}\n```";
  const res = parseJsonObjectFromText(raw);
  assert.equal(res.ok, true);
  assert.deepEqual(res.value, { a: 1, b: { c: 2 } });
  assert.equal(res.extracted, true);
});

test("parseJsonObjectFromText extracts first JSON object substring", async () => {
  const raw = "Here you go:\n\n{ \"x\": 1 }\nThanks!";
  const res = parseJsonObjectFromText(raw);
  assert.equal(res.ok, true);
  assert.deepEqual(res.value, { x: 1 });
  assert.equal(res.extracted, true);
});

