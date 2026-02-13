import test from "node:test";
import assert from "node:assert/strict";

import { assertGhInstalled } from "../src/github/gh.js";

test("PR/CI features hard-fail when gh is missing (no REST fallback)", () => {
  const prevPath = process.env.PATH;
  try {
    process.env.PATH = "/__gh_missing__";
    assert.throws(
      () => assertGhInstalled(),
      (err) => err instanceof Error && err.message.includes("Missing required dependency: gh"),
    );
  } finally {
    process.env.PATH = prevPath;
  }
});

