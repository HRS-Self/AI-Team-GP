import test from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

function walkFiles(dirAbs, out = []) {
  const entries = readdirSync(dirAbs, { withFileTypes: true });
  for (const entry of entries) {
    const abs = join(dirAbs, entry.name);
    if (entry.isDirectory()) {
      walkFiles(abs, out);
      continue;
    }
    if (entry.isFile() && (abs.endsWith(".js") || abs.endsWith(".ts"))) out.push(abs);
  }
  return out;
}

test("createDeepAgent usage is limited to prompt engineer module", () => {
  const srcAbs = join(process.cwd(), "src");
  assert.equal(statSync(srcAbs).isDirectory(), true);

  const hits = [];
  const files = walkFiles(srcAbs);
  for (const fileAbs of files) {
    const text = readFileSync(fileAbs, "utf8");
    if (/\bcreateDeepAgent\b/.test(text)) {
      hits.push(relative(process.cwd(), fileAbs).replaceAll("\\", "/"));
    }
  }

  assert.deepEqual(hits.sort(), ["src/prompt_engineer/prompt-engineer.js"]);
});
