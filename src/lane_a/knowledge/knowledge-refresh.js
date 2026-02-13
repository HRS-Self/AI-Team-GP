import { runKnowledgeScan } from "./knowledge-scan.js";
import { runKnowledgeSynthesize } from "./knowledge-synthesize.js";

export async function runKnowledgeRefresh({ projectRoot = null, concurrency = 4, dryRun = false } = {}) {
  const scan = await runKnowledgeScan({ projectRoot, repoId: null, limit: null, concurrency, dryRun });
  if (!scan.ok) return { ok: false, phase: "scan", blocked: false, blocker: null, ...scan };

  const synth = await runKnowledgeSynthesize({ projectRoot, dryRun });
  if (!synth.ok) return { ok: false, phase: "synthesize", scan, ...synth };
  return { ok: true, scan, synth, dry_run: dryRun };
}
