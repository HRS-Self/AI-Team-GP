export async function runWorkerPool({ items, concurrency, worker }) {
  const arr = Array.isArray(items) ? items.slice() : [];
  const limit = Number.isFinite(Number(concurrency)) ? Math.floor(Number(concurrency)) : 1;
  const c = Math.max(1, Math.min(32, limit));
  if (typeof worker !== "function") throw new Error("worker must be a function");

  const results = new Array(arr.length);
  let next = 0;

  const runOne = async () => {
    while (true) {
      const idx = next;
      next += 1;
      if (idx >= arr.length) return;
      results[idx] = await worker(arr[idx], idx);
    }
  };

  const runners = [];
  for (let i = 0; i < Math.min(c, arr.length); i += 1) runners.push(runOne());
  await Promise.all(runners);
  return results;
}

