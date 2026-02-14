import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

test("skill-author does not import or reference createDeepAgent/createAgent", () => {
  const fileAbs = join(process.cwd(), "src", "lane_a", "skills", "skill-author.js");
  const text = readFileSync(fileAbs, "utf8");
  assert.equal(/\bcreateDeepAgent\b/.test(text), false);
  assert.equal(/\bcreateAgent\b/.test(text), false);
  assert.equal(/from\s+["']langchain["']/.test(text), false);
});

