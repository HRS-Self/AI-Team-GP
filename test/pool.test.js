import test from "node:test";
import assert from "node:assert/strict";

import { runWorkerPool } from "../src/utils/pool.js";

test("runWorkerPool respects concurrency cap", async () => {
  const items = Array.from({ length: 9 }).map((_, i) => i);
  const concurrency = 3;
  let active = 0;
  let maxActive = 0;

  const results = await runWorkerPool({
    items,
    concurrency,
    worker: async (x) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((r) => setTimeout(r, 20));
      active -= 1;
      return x * 2;
    },
  });

  assert.equal(maxActive <= concurrency, true);
  assert.deepEqual(results, items.map((x) => x * 2));
});

