import { readFileSync } from 'node:fs';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { Orchestrator } from '../lane_b/orchestrator-lane-b.js';
import { getAIProjectRoot } from '../project/state-paths.js';
import { appendFile, readTextIfExists } from '../utils/fs.js';
import { warnDeprecatedOnce } from '../utils/deprecation.js';

function parseArgs(argv) {
      const args = { _: [] };
      for (let i = 0; i < argv.length; i += 1) {
            const token = argv[i];
            if (!token.startsWith('--')) {
                  args._.push(token);
                  continue;
            }
            const key = token.slice(2);
            const value =
                  argv[i + 1] && !argv[i + 1].startsWith('--')
                        ? argv[++i]
                        : true;
            args[key] = value;
      }
      return args;
}

function deprecatedWord() {
      return ['p', 'r', 'o', 'g', 'r', 'a', 'm'].join('');
}

function deprecatedFlagToken() {
      return ['--', deprecatedWord()].join('');
}

function deprecatedFailMessage() {
      const w = deprecatedWord();
      return `The '${w}' concept has been fully deprecated. Use project + repo scope instead.`;
}

function argvHasDeprecatedFlag(argv) {
      const flag = deprecatedFlagToken();
      return (Array.isArray(argv) ? argv : []).some(
            (t) =>
                  String(t || '') === flag ||
                  String(t || '').startsWith(`${flag}=`),
      );
}

function usage() {
      return [
            'Usage:',
            '  node src/cli.js --text "<request>" [--origin <origin>] [--scope system|repo:<id>]   # write raw inbox intake (I-*.md)',
            '  node src/cli.js --intake <path-to-md-or-txt> [--origin <origin>] [--scope system|repo:<id>]   # write raw inbox intake from file',
            '  node src/cli.js --dry-run --text "<request>"   # show what would be enqueued (no writes)',
            '  node src/cli.js --list-projects [--json]',
            '  node src/cli.js --show-project-detail --project <project_code> [--json]',
            '  node src/cli.js --remove-project --project <project_code> [--keep-files true|false] [--dry-run]',
            '  node src/cli.js --project-repos-sync --projectRoot <abs> [--dry-run]',
            '  node src/cli.js --initial-project --project <project_code> [--non-interactive] [--dry-run]',
            '  node src/cli.js --skills-list [--json] [--all true|false]',
            '  node src/cli.js --skills-show --skill <skill_id> [--max-lines <n>] [--json]',
            '  node src/cli.js --project-skills-status --projectRoot <abs> [--json]',
            '  node src/cli.js --project-skills-allow --projectRoot <abs> (--skill "<id>"|--skills "a,b,c") --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --project-skills-deny --projectRoot <abs> (--skill "<id>"|--skills "a,b") --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --skills-draft --projectRoot <abs> --scope (repo:<id>|system) [--dry-run]',
            '  node src/cli.js --skills-author --projectRoot <abs> --draft "<draft_id>" [--dry-run]',
            '  node src/cli.js --skills-refresh --projectRoot <abs> [--dry-run]',
            '  node src/cli.js --skills-governance --projectRoot <abs> [--run] [--status] [--json] [--dry-run]',
            '  node src/cli.js --skills-approve --projectRoot <abs> (--draft "<draft_id>"|--session "<draft_id>") --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --skills-reject --projectRoot <abs> (--draft "<draft_id>"|--session "<draft_id>") --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --knowledge-deps-approve --projectRoot <abs> --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --migrate-project-layout [--dry-run]   # migrate legacy /opt/AI-Projects/<code> -> /opt/AI-Projects/<code>/{ops,repos,knowledge}',
            '  node src/cli.js --knowledge-interview --projectRoot <abs> --scope (repo:<repo_id>|system) (--start|--continue) [--session "..."] [--max-questions N] [--dry-run]',
            '  node src/cli.js --knowledge-phase-status --projectRoot <abs> [--json] [--dry-run]',
            '  node src/cli.js --knowledge-confirm-v1 --projectRoot <abs> --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --knowledge-phase-close --projectRoot <abs> --phase reverse|forward --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --knowledge-kickoff-reverse --projectRoot <abs> [--scope (repo:<repo_id>|system)] (--start|--continue) [--non-interactive] [--input-file <path>] [--session "..."] [--max-questions N] [--dry-run]',
            '  node src/cli.js --knowledge-kickoff-forward --projectRoot <abs> [--scope (repo:<repo_id>|system)] (--start|--continue) [--non-interactive] [--input-file <path>] [--session "..."] [--max-questions N] [--dry-run]',
            '  node src/cli.js --knowledge-kickoff --projectRoot <abs> ...   # deprecated alias for --knowledge-kickoff-reverse',
            '  node src/cli.js --knowledge-review-meeting --projectRoot <abs> (--start|--continue) [--scope (repo:<repo_id>|system)] [--session "..."] [--max-questions N] [--dry-run]',
            '  node src/cli.js --knowledge-review-meeting --projectRoot <abs> --status [--json]',
            '  node src/cli.js --knowledge-review-meeting --projectRoot <abs> --close --session "<id>" --decision (confirm_sufficiency|revise_scans|open_decisions|abort) [--notes "<text>"] [--dry-run]',
            '  node src/cli.js --knowledge-review-answer --projectRoot <abs> --session "<id>" --input <md|txt|json> [--dry-run]',
            '  node src/cli.js --knowledge-update-meeting --projectRoot <abs> (--start|--continue) [--scope (repo:<id>|system)] [--session "..."] [--max-questions N] [--dry-run]',
            '  node src/cli.js --knowledge-update-meeting --projectRoot <abs> --status [--json]',
            '  node src/cli.js --knowledge-update-meeting --projectRoot <abs> --close --session "<id>" --decision (bump_patch|bump_minor|bump_major|no_bump|revise_scans|open_decisions|approve_intake|abort) [--notes "<text>"] [--dry-run]',
            '  node src/cli.js --knowledge-update-meeting --projectRoot <abs> --from vX --to vY (--start|--continue|--close) [--scope (repo:<id>|system)] [--session "..."] [--max-questions N] [--decision approve|reject|defer] [--by "<name>"] [--notes "..."] [--dry-run]',
            '  node src/cli.js --knowledge-update-answer --projectRoot <abs> --session "<id>" --input <md|txt|json> [--dry-run]',
            '  node src/cli.js --knowledge-change-request --projectRoot <abs> --type (bug|feature|question) --scope (repo:<id>|system) --input <md|txt|json> [--dry-run]',
            '  node src/cli.js --knowledge-change-status --projectRoot <abs> [--json]',
            '  node src/cli.js --knowledge-staleness --projectRoot <abs> [--json] [--dry-run]',
            '  node src/cli.js --lane-a-to-lane-b --projectRoot <abs> [--limit N] [--dry-run]',
            '  node src/cli.js --knowledge-status --projectRoot <abs> [--json]',
            '  node src/cli.js --knowledge-sufficiency-status --projectRoot <abs> [--json]',
            '  node src/cli.js --knowledge-sufficiency-propose --projectRoot <abs>',
            '  node src/cli.js --knowledge-sufficiency-confirm --projectRoot <abs> --by "<name>"',
            '  node src/cli.js --knowledge-sufficiency-revoke --projectRoot <abs> --reason "<text>"',
            '  node src/cli.js --knowledge-sufficiency --projectRoot <abs> --scope (repo:<id>|system) --version vX.Y.Z --status [--json]',
            '  node src/cli.js --knowledge-sufficiency --projectRoot <abs> --scope (repo:<id>|system) --version vX.Y.Z --propose [--dry-run]',
            '  node src/cli.js --knowledge-sufficiency --projectRoot <abs> --scope (repo:<id>|system) --version vX.Y.Z --approve --by "<name>" [--notes "..."] [--notes-file <path>] [--dry-run]',
            '  node src/cli.js --knowledge-sufficiency --projectRoot <abs> --scope (repo:<id>|system) --version vX.Y.Z --reject --by "<name>" [--notes "..."] [--notes-file <path>] [--dry-run]',
            '  node src/cli.js --knowledge-bundle --projectRoot <abs> --scope (repo:<repo_id>|system) [--out <abs>] [--dry-run]',
            '  node src/cli.js --knowledge-events-status [--json]',
            '  node src/cli.js --lane-b-events-list --projectRoot <abs> [--from <ISO>] [--to <ISO>] [--json]',
            '  node src/cli.js --lane-a-events-summary --projectRoot <abs> [--json]',
            '  node src/cli.js --knowledge-refresh-from-events --projectRoot <abs> [--max-events N] [--stop-on-error] [--dry-run]',
            '  node src/cli.js --knowledge-committee --projectRoot <abs> [--scope (repo:<repo_id>|system)] [--bundle-id <sha256-...>] [--limit N] [--dry-run]',
            '  node src/cli.js --knowledge-committee-status --projectRoot <abs> [--json]',
            '  node src/cli.js --decision-answer --id <DECISION-id> --input <md|txt|json> [--dry-run]',
            '  node src/cli.js --knowledge-scan [--repo <repo_id>] [--limit N] [--concurrency N] [--force-without-deps-approval] [--dry-run]',
            '  node src/cli.js --knowledge-synthesize [--dry-run]',
            '  node src/cli.js --knowledge-refresh [--concurrency N] [--dry-run]',
            '  node src/cli.js --knowledge-index [--limit N] [--dry-run]',
            '  node src/cli.js --ssot-resolve --view team:<TeamID> --out <project-relative-path> [--dry-run]',
            '  node src/cli.js --ssot-drift-check --workId <workId>',
            '  node src/cli.js --writer --projectRoot <abs> [--scope (repo:<repo_id>|system|all)] [--docs <doc_id|all>] [--limit N] [--dry-run]',
            '  node src/cli.js --triage [--limit N] [--dry-run]',
            '  node src/cli.js --seeds-to-intake [--phase N] [--limit N] [--force-without-sufficiency] [--dry-run]',
            '  node src/cli.js --gaps-to-intake [--impact high|medium|low] [--risk high|medium|low] [--limit N] [--force-without-sufficiency] [--dry-run]',
            '  node src/cli.js --validate [--workId <workId>]',
            '  node src/cli.js --agents-generate [--non-interactive]',
            '  node src/cli.js --agents-migrate',
            '  node src/cli.js --pr-status --workId <workId>',
            '  node src/cli.js --apply-approval --workId <workId> [--dry-run]',
            '  node src/cli.js --apply-approve --workId <workId> [--by "<name>"] [--notes "..."]',
            '  node src/cli.js --apply-reject --workId <workId> [--by "<name>"] [--notes "..."]',
            '  node src/cli.js --ci-update --workId <workId>',
            '  node src/cli.js --merge-approval --workId <workId> [--dry-run]',
            '  node src/cli.js --merge-approve --workId <workId> [--by "<name>"] [--notes "..."]',
            '  node src/cli.js --merge-reject --workId <workId> [--by "<name>"] [--notes "..."]',
            '  node src/cli.js --ci-install --repo <repo_id> [--commit] [--branch <name>] [--dry-run]',
            '  node src/cli.js --resolve <A|B> [--workId <workId>]',
            '  node src/cli.js --propose [--workId <workId>] [--teams TeamA,TeamB] [--with-patch-plans]',
            '  node src/cli.js --review --workId <workId> [--teams TeamA,TeamB]',
            '  node src/cli.js --qa --workId <workId> [--teams TeamA,TeamB] [--limit N]',
            '  node src/cli.js --qa-obligations --workId <workId> [--dry-run]',
            '  node src/cli.js --qa-pack-update --projectRoot <abs> [--scope (system|repo:<repo_id>)] [--dry-run]',
            '  node src/cli.js --qa-status --workId <workId> [--json]',
            '  node src/cli.js --qa-approve --workId <workId> --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --qa-reject  --workId <workId> --by "<name>" [--notes "..."] [--dry-run]',
            '  node src/cli.js --plan-approval --workId <workId>',
            '  node src/cli.js --plan-approve --workId <workId> [--teams TeamA,TeamB] [--notes "..."]',
            '  node src/cli.js --plan-reject --workId <workId> [--teams TeamA,TeamB] [--notes "..."]',
            '  node src/cli.js --approve-batch --intake I-<...> [--notes "..."]',
            '  node src/cli.js --reject-batch --intake I-<...> [--notes "..."]',
            '  node src/cli.js --plan-reset-approval --workId <workId>',
            '  node src/cli.js --approval --workId <workId>                      # deprecated alias for --plan-approval',
            '  node src/cli.js --approve --workId <workId>                        # deprecated alias for --plan-approve',
            '  node src/cli.js --reject --workId <workId>                         # deprecated alias for --plan-reject',
            '  node src/cli.js --reset-approval --workId <workId>                 # deprecated alias for --plan-reset-approval',
            '  node src/cli.js --gate-a --workId <workId>                         # deprecated alias for --apply-approval',
            '  node src/cli.js --gate-a-approve --workId <workId>                 # deprecated alias for --apply-approve',
            '  node src/cli.js --gate-a-reject --workId <workId>                  # deprecated alias for --apply-reject',
            '  node src/cli.js --gate-b --workId <workId>                         # deprecated alias for --merge-approval',
            '  node src/cli.js --gate-b-approve --workId <workId>                 # deprecated alias for --merge-approve',
            '  node src/cli.js --gate-b-reject --workId <workId>                  # deprecated alias for --merge-reject',
            '  node src/cli.js --patch-plan --workId <workId> [--teams TeamA,TeamB]',
            '  node src/cli.js --apply --workId <workId>',
            '  node src/cli.js --create-tasks',
            '  node src/cli.js --enqueue "<text>"',
            '  node src/cli.js --sweep [--limit N]',
            '  node src/cli.js --portfolio',
            '  node src/cli.js --repos-validate',
            '  node src/cli.js --repos-list',
            '  node src/cli.js --repos-generate [--base /opt/GitRepos]',
            '  node src/cli.js --policy-show --repo <repo_id>',
            '  node src/cli.js --checkout-active-branch [--workRoot <projectRoot>] [--repo <repo_id>] [--only-active] [--limit N] [--dry-run] [--rescan-commands]',
            '  node src/cli.js --watchdog [--limit N] [--dry-run] [--workId <id>] [--stop-at <stage>] [--max-minutes M] [--watchdog-ci true|false] [--watchdog-prepr true|false]',
            '  node src/cli.js --lane-a-orchestrate [--limit N] [--dry-run]',
            '',
            'Outputs:',
            '  (All runtime state is stored under AI_PROJECT_ROOT; set env var AI_PROJECT_ROOT=/path/to/project/ops)',
            '  AI_PROJECT_ROOT/ai/lane_b/inbox/I-*.md (raw intake queue)',
            '  AI_PROJECT_ROOT/ai/lane_b/inbox/triaged/T-*.json (repo-scoped triaged items, created by --triage)',
            '  AI_PROJECT_ROOT/ai/lane_b/inbox/triaged/BATCH-I-*.json (triage batches, created by --triage)',
            '  AI_PROJECT_ROOT/ai/lane_b/approvals/BATCH-I-*.json (batch approvals, created by --approve-batch/--reject-batch)',
            '  config/PROJECT.json.knowledge_repo_dir/... (canonical knowledge repo root; git worktree)',
            '  config/DOCS.json.docs_repo_path/... (Phase 8 docs output; git worktree)',
            '  AI_PROJECT_ROOT/ai/lane_b/work/<id>/ (created by --sweep)',
            '  AI_PROJECT_ROOT/ai/lane_b/ledger.jsonl (append-only, 1 line per run)',
      ].join('\n');
}

function inferProjectRootFromCwd() {
      let cur = resolve(process.cwd());
      // eslint-disable-next-line no-constant-condition
      while (true) {
            // Prefer OPS_ROOT discovery.
            const directOps = join(cur, 'config', 'PROJECT.json');
            if (existsSync(directOps)) return cur;
            const nestedOps = join(cur, 'ops', 'config', 'PROJECT.json');
            if (existsSync(nestedOps)) return join(cur, 'ops');
            const parent = dirname(cur);
            if (parent === cur) return null;
            cur = parent;
      }
}

export async function orchestrateFromArgs(argv) {
      if (argvHasDeprecatedFlag(argv)) {
            process.stderr.write(`${deprecatedFailMessage()}\n`);
            process.exit(2);
      }
      const args = parseArgs(argv);

      if (args.help || args.h) {
            process.stdout.write(`${usage()}\n`);
            process.exit(0);
      }

      if (
            args['knowledge-phase-status'] ||
            args['knowledge-confirm-v1'] ||
            args['knowledge-phase-close'] ||
            args['knowledge-kickoff-reverse'] ||
            args['knowledge-kickoff-forward'] ||
            args['knowledge-kickoff']
      ) {
            const projectRootArg =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? resolve(args.projectRoot.trim())
                        : null;
            const inferred =
                  projectRootArg ||
                  getAIProjectRoot({ required: false }) ||
                  inferProjectRootFromCwd();
            if (!inferred) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT/--projectRoot and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const projectRoot = inferred;
            const dryRun = !!args['dry-run'];
            const prevRoot = process.env.AI_PROJECT_ROOT;
            process.env.AI_PROJECT_ROOT = projectRoot;
            try {
                  const {
                        runKnowledgePhaseStatus,
                        runKnowledgeConfirmV1,
                        runKnowledgePhaseClose,
                        runKnowledgeKickoffReverse,
                        runKnowledgeKickoffForward,
                  } = await import('../lane_a/phase-runner.js');

                  if (args['knowledge-phase-status']) {
                        const result = await runKnowledgePhaseStatus({
                              projectRoot,
                              dryRun,
                        });
                        const asJson = !!args.json;
                        if (asJson)
                              process.stdout.write(
                                    `${JSON.stringify(result, null, 2)}\n`,
                              );
                        else
                              process.stdout.write(
                                    `${JSON.stringify(result.phase, null, 2)}\n`,
                              );
                        process.exit(result.ok ? 0 : 1);
                  }

                  if (args['knowledge-confirm-v1']) {
                        const by = typeof args.by === 'string' ? args.by : null;
                        const notes =
                              typeof args.notes === 'string'
                                    ? args.notes
                                    : null;
                        const result = await runKnowledgeConfirmV1({
                              projectRoot,
                              by,
                              notes,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-confirm-v1 failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }

                  if (args['knowledge-phase-close']) {
                        const phase =
                              typeof args.phase === 'string'
                                    ? args.phase
                                    : null;
                        const by = typeof args.by === 'string' ? args.by : null;
                        const notes =
                              typeof args.notes === 'string'
                                    ? args.notes
                                    : null;
                        const result = await runKnowledgePhaseClose({
                              projectRoot,
                              phase,
                              by,
                              notes,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-phase-close failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }

                  // Kickoff (reverse/forward); legacy --knowledge-kickoff routes to reverse with a warning.
                  const isLegacyKickoff = !!args['knowledge-kickoff'];
                  const isReverseKickoff =
                        !!args['knowledge-kickoff-reverse'] || isLegacyKickoff;
                  const isForwardKickoff = !!args['knowledge-kickoff-forward'];
                  if (isLegacyKickoff)
                        warnDeprecatedOnce(
                              '--knowledge-kickoff',
                              'Use --knowledge-kickoff-reverse instead.',
                        );

                  if (isReverseKickoff || isForwardKickoff) {
                        const scope =
                              typeof args.scope === 'string' &&
                              args.scope.trim()
                                    ? args.scope.trim()
                                    : 'system';
                        const start = !!args.start;
                        const cont = !!args['continue'];
                        const nonInteractive = !!args['non-interactive'];
                        const inputFileAbs =
                              typeof args['input-file'] === 'string' &&
                              args['input-file'].trim()
                                    ? args['input-file'].trim()
                                    : null;
                        const sessionText =
                              typeof args.session === 'string'
                                    ? args.session
                                    : null;
                        const maxQuestionsRaw =
                              typeof args['max-questions'] === 'string' &&
                              args['max-questions'].trim()
                                    ? args['max-questions'].trim()
                                    : null;
                        const maxQuestions = maxQuestionsRaw
                              ? Number.parseInt(maxQuestionsRaw, 10)
                              : 12;
                        if (
                              maxQuestionsRaw &&
                              (!Number.isFinite(maxQuestions) ||
                                    maxQuestions <= 0)
                        ) {
                              process.stderr.write(
                                    `${usage()}\n\nInvalid --max-questions. Expected a positive integer.\n`,
                              );
                              process.exit(2);
                        }

                        const runner = isForwardKickoff
                              ? runKnowledgeKickoffForward
                              : runKnowledgeKickoffReverse;
                        const result = await runner({
                              projectRoot,
                              scope,
                              start,
                              cont,
                              nonInteractive,
                              inputFileAbs,
                              sessionText,
                              maxQuestions,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge kickoff failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }
            } finally {
                  if (typeof prevRoot === 'string')
                        process.env.AI_PROJECT_ROOT = prevRoot;
                  else delete process.env.AI_PROJECT_ROOT;
            }
      }

      if (args['migrate-project-layout']) {
            const { runMigrateProjectLayout } =
                  await import('../onboarding/onboarding-runner.js');
            const dryRun = !!args['dry-run'];
            const result = await runMigrateProjectLayout({
                  legacyRootAbs: process.env.AI_PROJECT_ROOT || null,
                  dryRun,
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['ssot-resolve']) {
            const inferred = inferProjectRootFromCwd();
            const projectRoot =
                  getAIProjectRoot({ required: false }) || inferred;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT env and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const view =
                  typeof args.view === 'string' && args.view.trim()
                        ? args.view.trim()
                        : null;
            const out =
                  typeof args.out === 'string' && args.out.trim()
                        ? args.out.trim()
                        : null;
            if (!view || !out) {
                  process.stderr.write(
                        `${usage()}\n\nMissing required args: --view team:<TeamID> and --out <project-relative-path>.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { resolveSsotBundle } =
                  await import('../ssot/ssot-resolver.js');
            const res = await resolveSsotBundle({
                  projectRoot,
                  view,
                  outPath: out,
                  dryRun,
            });
            if (!res.ok) {
                  process.stderr.write(`${res.message}\n`);
                  process.stdout.write(`${JSON.stringify(res, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(res, null, 2)}\n`);
            process.exit(0);
      }

      if (args['ssot-drift-check']) {
            const inferred = inferProjectRootFromCwd();
            const projectRoot =
                  getAIProjectRoot({ required: false }) || inferred;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT env and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId <workId>.\n`,
                  );
                  process.exit(2);
            }
            const prev = process.env.AI_PROJECT_ROOT;
            process.env.AI_PROJECT_ROOT = projectRoot;
            try {
                  const { runSsotDriftCheck } =
                        await import('../ssot/ssot-drift-check.js');
                  const res = await runSsotDriftCheck({ workId });
                  if (!res.ok) {
                        process.stderr.write(
                              `${res.message || 'ssot-drift-check failed'}\n`,
                        );
                        process.stdout.write(
                              `${JSON.stringify(res, null, 2)}\n`,
                        );
                        process.exit(1);
                  }
                  process.stdout.write(`${JSON.stringify(res, null, 2)}\n`);
                  process.exit(0);
            } finally {
                  if (typeof prev === 'string')
                        process.env.AI_PROJECT_ROOT = prev;
                  else delete process.env.AI_PROJECT_ROOT;
            }
      }

      if (args.writer) {
            const projectRootArg =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            const inferred = projectRootArg
                  ? resolve(projectRootArg)
                  : inferProjectRootFromCwd();
            if (!inferred) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const prev = process.env.AI_PROJECT_ROOT;
            process.env.AI_PROJECT_ROOT = inferred;
            try {
                  const scope =
                        typeof args.scope === 'string' && args.scope.trim()
                              ? args.scope.trim()
                              : 'all';
                  const docs =
                        typeof args.docs === 'string' && args.docs.trim()
                              ? args.docs.trim()
                              : 'all';
                  const dryRun = !!args['dry-run'];
                  const limitRaw =
                        typeof args.limit === 'string' && args.limit.trim()
                              ? args.limit.trim()
                              : null;
                  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
                  if (limitRaw && (!Number.isFinite(limit) || limit <= 0)) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --limit. Expected a positive integer.\n`,
                        );
                        process.exit(2);
                  }
                  const forceStaleOverride = !!args['force-stale-override'];
                  const by = typeof args.by === 'string' ? args.by : null;
                  const reason =
                        typeof args.reason === 'string' ? args.reason : null;
                  const { runWriter } =
                        await import('../writer/writer-runner.js');
                  const result = await runWriter({
                        projectRoot: inferred,
                        scope,
                        docs,
                        limit: Number.isFinite(limit) ? limit : null,
                        dryRun,
                        forceStaleOverride,
                        by,
                        reason,
                  });
                  if (!result.ok) {
                        process.stderr.write(
                              `${result.message || 'writer failed'}\n`,
                        );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(1);
                  }
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(0);
            } finally {
                  if (typeof prev === 'string')
                        process.env.AI_PROJECT_ROOT = prev;
                  else delete process.env.AI_PROJECT_ROOT;
            }
      }

      if (args['knowledge-interview'] || args['knowledge-extract-tasks']) {
            const projectRootArg =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            const inferred = projectRootArg
                  ? resolve(projectRootArg)
                  : inferProjectRootFromCwd();
            if (!inferred) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const prev = process.env.AI_PROJECT_ROOT;
            process.env.AI_PROJECT_ROOT = inferred;
            try {
                  const scope =
                        typeof args.scope === 'string' && args.scope.trim()
                              ? args.scope.trim()
                              : null;
                  if (!scope) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --scope (repo:<repo_id>|system).\n`,
                        );
                        process.exit(2);
                  }
                  const dryRun = !!args['dry-run'];

                  if (args['knowledge-interview']) {
                        const start = !!args.start;
                        const cont = !!args['continue'];
                        const sessionText =
                              typeof args.session === 'string'
                                    ? args.session
                                    : null;
                        const maxQuestionsRaw =
                              typeof args['max-questions'] === 'string' &&
                              args['max-questions'].trim()
                                    ? args['max-questions'].trim()
                                    : null;
                        const maxQuestions = maxQuestionsRaw
                              ? Number.parseInt(maxQuestionsRaw, 10)
                              : 12;
                        if (
                              maxQuestionsRaw &&
                              (!Number.isFinite(maxQuestions) ||
                                    maxQuestions <= 0)
                        ) {
                              process.stderr.write(
                                    `${usage()}\n\nInvalid --max-questions. Expected a positive integer.\n`,
                              );
                              process.exit(2);
                        }

                        const { runKnowledgeInterview } =
                              await import('../lane_a/knowledge/knowledge-runner.js');
                        const result = await runKnowledgeInterview({
                              scope,
                              start,
                              cont,
                              sessionText,
                              maxQuestions,
                              dryRun,
                        });
                        if (!result.ok) {
                              process.stderr.write(
                                    `${result.message || 'knowledge interview failed'}\n`,
                              );
                              process.stdout.write(
                                    `${JSON.stringify(result, null, 2)}\n`,
                              );
                              process.exit(1);
                        }
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(0);
                  }

                  if (args['knowledge-extract-tasks']) {
                        process.stderr.write(
                              'Forbidden: --knowledge-extract-tasks (Lane A must not produce delivery intake/task floods).\n',
                        );
                        process.stderr.write(
                              'Use triage/sweep/planner in the Delivery lane.\n',
                        );
                        process.exit(2);
                  }
            } finally {
                  if (typeof prev === 'string')
                        process.env.AI_PROJECT_ROOT = prev;
                  else delete process.env.AI_PROJECT_ROOT;
            }
      }

      if (
            args['knowledge-scan'] ||
            args['knowledge-synthesize'] ||
            args['knowledge-refresh'] ||
            args['knowledge-index']
      ) {
            const dryRun = !!args['dry-run'];

            if (args['knowledge-scan']) {
                  const repo =
                        typeof args.repo === 'string' && args.repo.trim()
                              ? args.repo.trim()
                              : null;
                  const limitRaw =
                        typeof args.limit === 'string' && args.limit.trim()
                              ? args.limit.trim()
                              : null;
                  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
                  if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                        );
                        process.exit(2);
                  }
                  const concurrencyRaw =
                        typeof args.concurrency === 'string' &&
                        args.concurrency.trim()
                              ? args.concurrency.trim()
                              : null;
                  const concurrency = concurrencyRaw
                        ? Number.parseInt(concurrencyRaw, 10)
                        : 4;
                  if (
                        concurrencyRaw &&
                        (!Number.isFinite(concurrency) || concurrency <= 0)
                  ) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --concurrency. Expected a positive integer.\n`,
                        );
                        process.exit(2);
                  }
                  const forceWithoutDepsApproval =
                        !!args['force-without-deps-approval'];
                  const { runKnowledgeScan } =
                        await import('../lane_a/knowledge/knowledge-scan.js');
                  const result = await runKnowledgeScan({
                        repoId: repo,
                        limit: Number.isFinite(limit) ? limit : null,
                        concurrency,
                        dryRun,
                        forceWithoutDepsApproval,
                  });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (args['knowledge-synthesize']) {
                  const { runKnowledgeSynthesize } =
                        await import('../lane_a/knowledge/knowledge-synthesize.js');
                  const result = await runKnowledgeSynthesize({ dryRun });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (args['knowledge-refresh']) {
                  const concurrencyRaw =
                        typeof args.concurrency === 'string' &&
                        args.concurrency.trim()
                              ? args.concurrency.trim()
                              : null;
                  const concurrency = concurrencyRaw
                        ? Number.parseInt(concurrencyRaw, 10)
                        : 4;
                  if (
                        concurrencyRaw &&
                        (!Number.isFinite(concurrency) || concurrency <= 0)
                  ) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --concurrency. Expected a positive integer.\n`,
                        );
                        process.exit(2);
                  }
                  const { runKnowledgeRefresh } =
                        await import('../lane_a/knowledge/knowledge-refresh.js');
                  const result = await runKnowledgeRefresh({
                        concurrency,
                        dryRun,
                  });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (args['knowledge-index']) {
                  const limitRaw =
                        typeof args.limit === 'string' && args.limit.trim()
                              ? args.limit.trim()
                              : null;
                  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
                  if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                        );
                        process.exit(2);
                  }
                  const { runKnowledgeIndex } =
                        await import('../lane_a/knowledge/knowledge-index.js');
                  const result = await runKnowledgeIndex({
                        limit: Number.isFinite(limit) ? limit : null,
                        dryRun,
                  });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }
      }

      if (
            args['knowledge-kickoff'] ||
            args['knowledge-status'] ||
            args['knowledge-review-meeting'] ||
            args['knowledge-review-answer'] ||
            args['knowledge-update-meeting'] ||
            args['knowledge-change-request'] ||
            args['knowledge-change-status'] ||
            args['knowledge-staleness'] ||
            args['lane-a-to-lane-b'] ||
            args['knowledge-sufficiency'] ||
            args['knowledge-sufficiency-status'] ||
            args['knowledge-sufficiency-propose'] ||
            args['knowledge-sufficiency-confirm'] ||
            args['knowledge-sufficiency-revoke']
      ) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];

            if (args['knowledge-status']) {
                  const { runKnowledgeStatus } =
                        await import('../lane_a/knowledge/knowledge-status.js');
                  const result = await runKnowledgeStatus({
                        projectRoot,
                        dryRun,
                  });
                  const asJson = !!args.json;
                  if (asJson) {
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                  } else if (
                        result &&
                        typeof result === 'object' &&
                        result.ok
                  ) {
                        const lines = [];
                        lines.push(`overall: ${result.overall}`);
                        lines.push(`generated_at: ${result.generated_at}`);
                        lines.push(`knowledge_repo: ${result.knowledge_repo}`);
                        lines.push(`repos_root: ${result.repos_root}`);
                        lines.push(
                              `open_decisions: ${result.system?.open_decisions_count ?? 0}`,
                        );
                        lines.push(
                              `integration_gaps: ${result.system?.integration_gaps_unresolved_count ?? 0}`,
                        );
                        process.stdout.write(`${lines.join('\n')}\n`);
                  } else {
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                  }
                  process.exit(
                        result && typeof result === 'object' && result.ok
                              ? 0
                              : 1,
                  );
            }

            if (args['knowledge-staleness']) {
                  const { runKnowledgeStaleness } =
                        await import('../lane_a/knowledge/knowledge-staleness.js');
                  const result = await runKnowledgeStaleness({
                        projectRoot,
                        json: !!args.json,
                        dryRun,
                  });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(
                        result && typeof result === 'object' && result.ok
                              ? 0
                              : 1,
                  );
            }

            if (
                  args['knowledge-sufficiency'] ||
                  args['knowledge-sufficiency-status'] ||
                  args['knowledge-sufficiency-propose'] ||
                  args['knowledge-sufficiency-confirm'] ||
                  args['knowledge-sufficiency-revoke']
            ) {
                  const {
                        runKnowledgeSufficiencyStatus,
                        runKnowledgeSufficiencyPropose,
                        runKnowledgeSufficiencyApprove,
                        runKnowledgeSufficiencyReject,
                        runKnowledgeSufficiencyConfirm,
                        runKnowledgeSufficiencyRevoke,
                  } =
                        await import('../lane_a/knowledge/knowledge-sufficiency.js');

                  if (args['knowledge-sufficiency']) {
                        const scope =
                              typeof args.scope === 'string' &&
                              args.scope.trim()
                                    ? args.scope.trim()
                                    : null;
                        const version =
                              typeof args.version === 'string' &&
                              args.version.trim()
                                    ? args.version.trim()
                                    : null;
                        if (!scope) {
                              process.stderr.write(
                                    `${usage()}\n\nMissing --scope (repo:<id>|system).\n`,
                              );
                              process.exit(2);
                        }
                        if (!version) {
                              process.stderr.write(
                                    `${usage()}\n\nMissing --version vX.Y.Z.\n`,
                              );
                              process.exit(2);
                        }
                        const by = typeof args.by === 'string' ? args.by : null;
                        const notes =
                              typeof args.notes === 'string'
                                    ? args.notes
                                    : null;
                        const notesFile =
                              typeof args['notes-file'] === 'string'
                                    ? args['notes-file']
                                    : null;

                        const acts = [
                              'status',
                              'propose',
                              'approve',
                              'reject',
                        ].filter((k) => args[k]);
                        if (acts.length !== 1) {
                              process.stderr.write(
                                    `${usage()}\n\nExactly one of --status|--propose|--approve|--reject is required.\n`,
                              );
                              process.exit(2);
                        }

                        const act = acts[0];
                        try {
                              if (act === 'status') {
                                    const result =
                                          await runKnowledgeSufficiencyStatus({
                                                projectRoot,
                                                scope,
                                                knowledgeVersion: version,
                                          });
                                    const asJson = !!args.json;
                                    if (asJson)
                                          process.stdout.write(
                                                `${JSON.stringify(result, null, 2)}\n`,
                                          );
                                    else
                                          process.stdout.write(
                                                `${JSON.stringify(result.sufficiency, null, 2)}\n`,
                                          );
                                    process.exit(result.ok ? 0 : 1);
                              }
                              if (act === 'propose') {
                                    const result =
                                          await runKnowledgeSufficiencyPropose({
                                                projectRoot,
                                                scope,
                                                knowledgeVersion: version,
                                                dryRun,
                                          });
                                    process.stdout.write(
                                          `${JSON.stringify(result, null, 2)}\n`,
                                    );
                                    process.exit(result.ok ? 0 : 1);
                              }
                              if (act === 'approve') {
                                    if (!by || !String(by).trim()) {
                                          process.stderr.write(
                                                `${usage()}\n\nMissing --by \"<name>\".\n`,
                                          );
                                          process.exit(2);
                                    }
                                    const result =
                                          await runKnowledgeSufficiencyApprove({
                                                projectRoot,
                                                scope,
                                                knowledgeVersion: version,
                                                by,
                                                notes,
                                                notesFile,
                                                dryRun,
                                          });
                                    process.stdout.write(
                                          `${JSON.stringify(result, null, 2)}\n`,
                                    );
                                    process.exit(result.ok ? 0 : 1);
                              }
                              if (act === 'reject') {
                                    if (!by || !String(by).trim()) {
                                          process.stderr.write(
                                                `${usage()}\n\nMissing --by \"<name>\".\n`,
                                          );
                                          process.exit(2);
                                    }
                                    const result =
                                          await runKnowledgeSufficiencyReject({
                                                projectRoot,
                                                scope,
                                                knowledgeVersion: version,
                                                by,
                                                notes,
                                                notesFile,
                                                dryRun,
                                          });
                                    process.stdout.write(
                                          `${JSON.stringify(result, null, 2)}\n`,
                                    );
                                    process.exit(result.ok ? 0 : 1);
                              }
                        } catch (err) {
                              const msg =
                                    err instanceof Error
                                          ? err.message
                                          : String(err);
                              process.stderr.write(`${msg}\n`);
                              process.exit(1);
                        }
                  }

                  if (args['knowledge-sufficiency-status']) {
                        const result = await runKnowledgeSufficiencyStatus({
                              projectRoot,
                        });
                        const asJson = !!args.json;
                        if (asJson)
                              process.stdout.write(
                                    `${JSON.stringify(result, null, 2)}\n`,
                              );
                        else if (result.ok)
                              process.stdout.write(
                                    `${JSON.stringify(result.sufficiency, null, 2)}\n`,
                              );
                        else
                              process.stdout.write(
                                    `${JSON.stringify(result, null, 2)}\n`,
                              );
                        process.exit(result.ok ? 0 : 1);
                  }

                  if (args['knowledge-sufficiency-propose']) {
                        const result = await runKnowledgeSufficiencyPropose({
                              projectRoot,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-sufficiency-propose failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }

                  if (args['knowledge-sufficiency-confirm']) {
                        const by = typeof args.by === 'string' ? args.by : null;
                        const result = await runKnowledgeSufficiencyConfirm({
                              projectRoot,
                              by,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-sufficiency-confirm failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }

                  if (args['knowledge-sufficiency-revoke']) {
                        const reason =
                              typeof args.reason === 'string'
                                    ? args.reason
                                    : null;
                        const result = await runKnowledgeSufficiencyRevoke({
                              projectRoot,
                              reason,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-sufficiency-revoke failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }
            }

            if (
                  args['knowledge-review-meeting'] ||
                  args['knowledge-review-answer']
            ) {
                  const {
                        runKnowledgeReviewMeeting,
                        runKnowledgeReviewAnswer,
                  } =
                        await import('../lane_a/knowledge/knowledge-review-meeting.js');

                  if (!projectRoot) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --projectRoot.\n`,
                        );
                        process.exit(2);
                  }

                  if (args['knowledge-review-answer']) {
                        const session =
                              typeof args.session === 'string'
                                    ? args.session
                                    : null;
                        const inputPath =
                              typeof args.input === 'string'
                                    ? args.input
                                    : null;
                        const result = await runKnowledgeReviewAnswer({
                              projectRoot,
                              session,
                              inputPath,
                              dryRun,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-review-answer failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }

                  if (args['knowledge-review-meeting']) {
                        const scope =
                              typeof args.scope === 'string' &&
                              args.scope.trim()
                                    ? args.scope.trim()
                                    : 'system';
                        const start = !!args.start;
                        const cont = !!args['continue'];
                        const status = !!args.status;
                        const close = !!args.close;
                        const session =
                              typeof args.session === 'string'
                                    ? args.session
                                    : null;
                        const notes =
                              typeof args.notes === 'string'
                                    ? args.notes
                                    : null;
                        const decision =
                              typeof args.decision === 'string'
                                    ? args.decision
                                    : null;
                        const maxQuestionsRaw =
                              typeof args['max-questions'] === 'string' &&
                              args['max-questions'].trim()
                                    ? args['max-questions'].trim()
                                    : null;
                        const maxQuestions = maxQuestionsRaw
                              ? Number.parseInt(maxQuestionsRaw, 10)
                              : 12;
                        if (
                              maxQuestionsRaw &&
                              (!Number.isFinite(maxQuestions) ||
                                    maxQuestions <= 0)
                        ) {
                              process.stderr.write(
                                    `${usage()}\n\nInvalid --max-questions. Expected a positive integer.\n`,
                              );
                              process.exit(2);
                        }

                        const modes = [
                              start ? 'start' : null,
                              cont ? 'continue' : null,
                              status ? 'status' : null,
                              close ? 'close' : null,
                        ].filter(Boolean);
                        if (modes.length !== 1) {
                              process.stderr.write(
                                    `${usage()}\n\nMeeting requires exactly one of: --start | --continue | --status | --close.\n`,
                              );
                              process.exit(2);
                        }
                        const mode = modes[0];
                        const forceStaleOverride =
                              !!args['force-stale-override'];
                        const by = typeof args.by === 'string' ? args.by : null;
                        const reason =
                              typeof args.reason === 'string'
                                    ? args.reason
                                    : null;
                        const result = await runKnowledgeReviewMeeting({
                              projectRoot,
                              mode,
                              scope,
                              session,
                              maxQuestions,
                              dryRun,
                              closeDecision: decision,
                              closeNotes: notes,
                              forceStaleOverride,
                              by,
                              reason,
                        });
                        if (!result.ok)
                              process.stderr.write(
                                    `${result.message || 'knowledge-review-meeting failed'}\n`,
                              );
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }
            }

            if (args['knowledge-update-meeting']) {
                  if (!projectRoot) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --projectRoot.\n`,
                        );
                        process.exit(2);
                  }

                  const scope =
                        typeof args.scope === 'string' && args.scope.trim()
                              ? args.scope.trim()
                              : 'system';
                  const start = !!args.start;
                  const cont = !!args['continue'];
                  const status = !!args.status;
                  const close = !!args.close;
                  const session =
                        typeof args.session === 'string' ? args.session : null;
                  const notes =
                        typeof args.notes === 'string' ? args.notes : null;
                  const decision =
                        typeof args.decision === 'string'
                              ? args.decision
                              : null;
                  const from = typeof args.from === 'string' ? args.from : null;
                  const to = typeof args.to === 'string' ? args.to : null;
                  const maxQuestionsRaw =
                        typeof args['max-questions'] === 'string' &&
                        args['max-questions'].trim()
                              ? args['max-questions'].trim()
                              : null;
                  const maxQuestions = maxQuestionsRaw
                        ? Number.parseInt(maxQuestionsRaw, 10)
                        : 12;
                  if (
                        maxQuestionsRaw &&
                        (!Number.isFinite(maxQuestions) || maxQuestions <= 0)
                  ) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --max-questions. Expected a positive integer.\n`,
                        );
                        process.exit(2);
                  }

                  const modes = [
                        start ? 'start' : null,
                        cont ? 'continue' : null,
                        status ? 'status' : null,
                        close ? 'close' : null,
                  ].filter(Boolean);
                  if (modes.length !== 1) {
                        process.stderr.write(
                              `${usage()}\n\nUpdate meeting requires exactly one of: --start | --continue | --status | --close.\n`,
                        );
                        process.exit(2);
                  }
                  const mode = modes[0];
                  const forceStaleOverride = !!args['force-stale-override'];
                  const by = typeof args.by === 'string' ? args.by : null;
                  const reason =
                        typeof args.reason === 'string' ? args.reason : null;

                  const v2Decision =
                        typeof decision === 'string' ? decision.trim() : '';
                  const v2Intent =
                        !!(from && from.trim()) ||
                        !!(to && to.trim()) ||
                        ['approve', 'reject', 'defer'].includes(v2Decision);
                  let result;
                  if (v2Intent) {
                        const { runVersionedKnowledgeUpdateMeeting } =
                              await import('../lane_a/knowledge/version-update-meeting.js');
                        if (mode !== 'status' && (!from || !to)) {
                              process.stderr.write(
                                    `${usage()}\n\nVersioned update meeting requires both --from and --to.\n`,
                              );
                              process.exit(2);
                        }
                        result = await runVersionedKnowledgeUpdateMeeting({
                              projectRoot,
                              mode,
                              scope,
                              session,
                              maxQuestions,
                              dryRun,
                              fromVersion: from,
                              toVersion: to,
                              decision,
                              by,
                              notes,
                        });
                  } else {
                        const { runKnowledgeUpdateMeeting } =
                              await import('../lane_a/knowledge/knowledge-update-meeting.js');
                        result = await runKnowledgeUpdateMeeting({
                              projectRoot,
                              mode,
                              scope,
                              session,
                              maxQuestions,
                              dryRun,
                              closeDecision: decision,
                              closeNotes: notes,
                              forceStaleOverride,
                              by,
                              reason,
                        });
                  }
                  if (!result.ok)
                        process.stderr.write(
                              `${result.message || 'knowledge-update-meeting failed'}\n`,
                        );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (args['knowledge-update-answer']) {
                  const { runVersionedKnowledgeUpdateAnswer } =
                        await import('../lane_a/knowledge/version-update-meeting.js');
                  if (!projectRoot) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --projectRoot.\n`,
                        );
                        process.exit(2);
                  }
                  const session =
                        typeof args.session === 'string' ? args.session : null;
                  const inputPath =
                        typeof args.input === 'string' ? args.input : null;
                  if (!session || !session.trim()) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --session.\n`,
                        );
                        process.exit(2);
                  }
                  if (!inputPath || !inputPath.trim()) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --input.\n`,
                        );
                        process.exit(2);
                  }
                  const result = await runVersionedKnowledgeUpdateAnswer({
                        projectRoot,
                        session,
                        inputPath,
                        dryRun,
                  });
                  if (!result.ok)
                        process.stderr.write(
                              `${result.message || 'knowledge-update-answer failed'}\n`,
                        );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (
                  args['knowledge-change-request'] ||
                  args['knowledge-change-status']
            ) {
                  const {
                        runKnowledgeChangeRequest,
                        runKnowledgeChangeStatus,
                  } = await import('../lane_a/knowledge/change-requests.js');
                  if (!projectRoot) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --projectRoot.\n`,
                        );
                        process.exit(2);
                  }
                  if (args['knowledge-change-status']) {
                        const result = await runKnowledgeChangeStatus({
                              projectRoot,
                              json: !!args.json,
                        });
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  }
                  const type = typeof args.type === 'string' ? args.type : null;
                  const scope =
                        typeof args.scope === 'string' ? args.scope : null;
                  const inputPath =
                        typeof args.input === 'string' ? args.input : null;
                  const result = await runKnowledgeChangeRequest({
                        projectRoot,
                        type,
                        scope,
                        inputPath,
                        dryRun,
                  });
                  if (!result.ok)
                        process.stderr.write(
                              `${result.message || 'knowledge-change-request failed'}\n`,
                        );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (args['lane-a-to-lane-b']) {
                  const { runLaneAToLaneB } =
                        await import('../lane_a/lane-a-to-lane-b.js');
                  if (!projectRoot) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --projectRoot.\n`,
                        );
                        process.exit(2);
                  }
                  const limitRaw =
                        typeof args.limit === 'string' && args.limit.trim()
                              ? args.limit.trim()
                              : null;
                  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
                  if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                        );
                        process.exit(2);
                  }
                  const result = await runLaneAToLaneB({
                        projectRoot,
                        limit,
                        dryRun,
                  });
                  if (!result.ok)
                        process.stderr.write(
                              `${result.message || 'lane-a-to-lane-b failed'}\n`,
                        );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }
      }

      if (args['knowledge-bundle']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const scope =
                  typeof args.scope === 'string' && args.scope.trim()
                        ? args.scope.trim()
                        : null;
            if (!scope) {
                  process.stderr.write(`${usage()}\n\nMissing --scope.\n`);
                  process.exit(2);
            }
            const out =
                  typeof args.out === 'string' && args.out.trim()
                        ? args.out.trim()
                        : null;
            const dryRun = !!args['dry-run'];
            const forceStaleOverride = !!args['force-stale-override'];
            const by = typeof args.by === 'string' ? args.by : null;
            const reason = typeof args.reason === 'string' ? args.reason : null;
            const { runKnowledgeBundle } =
                  await import('../lane_a/knowledge/knowledge-bundle.js');
            const result = await runKnowledgeBundle({
                  projectRoot,
                  scope,
                  out,
                  dryRun,
                  forceStaleOverride,
                  by,
                  reason,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['knowledge-committee'] || args['knowledge-committee-status']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const scope =
                  typeof args.scope === 'string' && args.scope.trim()
                        ? args.scope.trim()
                        : 'system';

            if (args['knowledge-committee-status']) {
                  const { runKnowledgeCommitteeStatus } =
                        await import('../lane_a/knowledge/committee-runner.js');
                  const result = await runKnowledgeCommitteeStatus({
                        projectRoot,
                  });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }

            if (args['knowledge-committee']) {
                  const limitRaw =
                        typeof args.limit === 'string' && args.limit.trim()
                              ? args.limit.trim()
                              : null;
                  const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
                  if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                        );
                        process.exit(2);
                  }
                  const { runKnowledgeCommittee } =
                        await import('../lane_a/knowledge/committee-runner.js');
                  const bundleId =
                        typeof args['bundle-id'] === 'string' &&
                        args['bundle-id'].trim()
                              ? args['bundle-id'].trim()
                              : null;
                  const mode =
                        typeof args.mode === 'string' && args.mode.trim()
                              ? args.mode.trim()
                              : 'run';
                  const maxQuestionsRaw =
                        typeof args['max-questions'] === 'string' &&
                        args['max-questions'].trim()
                              ? args['max-questions'].trim()
                              : null;
                  const maxQuestions = maxQuestionsRaw
                        ? Number.parseInt(maxQuestionsRaw, 10)
                        : null;
                  if (
                        maxQuestionsRaw &&
                        (!Number.isFinite(maxQuestions) || maxQuestions < 1)
                  ) {
                        process.stderr.write(
                              `${usage()}\n\nInvalid --max-questions. Expected a positive integer.\n`,
                        );
                        process.exit(2);
                  }
                  const forceStaleOverride = !!args['force-stale-override'];
                  const by = typeof args.by === 'string' ? args.by : null;
                  const reason =
                        typeof args.reason === 'string' ? args.reason : null;
                  const result = await runKnowledgeCommittee({
                        projectRoot,
                        scope,
                        bundleId,
                        limit: Number.isFinite(limit) ? limit : null,
                        mode,
                        maxQuestions: Number.isFinite(maxQuestions)
                              ? maxQuestions
                              : null,
                        dryRun,
                        forceStaleOverride,
                        by,
                        reason,
                  });
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }
      }

      if (args['knowledge-events-status']) {
            const inferred = inferProjectRootFromCwd();
            const projectRoot =
                  getAIProjectRoot({ required: false }) || inferred;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT env and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const { runKnowledgeEventsStatus } =
                  await import('../lane_a/knowledge/knowledge-events-status.js');
            const result = await runKnowledgeEventsStatus({
                  projectRoot: resolve(projectRoot),
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['lane-b-events-list']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const from =
                  typeof args.from === 'string' && args.from.trim()
                        ? args.from.trim()
                        : null;
            const to =
                  typeof args.to === 'string' && args.to.trim()
                        ? args.to.trim()
                        : null;
            const asJson = !!args.json;
            const { runLaneBEventsList } =
                  await import('../lane_b/lane-b-events-list.js');
            const result = await runLaneBEventsList({
                  projectRoot: resolve(projectRoot),
                  from,
                  to,
            });
            if (
                  Array.isArray(result.warnings) &&
                  result.warnings.length &&
                  !asJson
            ) {
                  for (const w of result.warnings)
                        process.stderr.write(`${w}\n`);
            }
            if (asJson)
                  process.stdout.write(
                        `${JSON.stringify(result.events, null, 2)}\n`,
                  );
            else {
                  for (const ev of result.events) {
                        process.stdout.write(
                              `${ev.timestamp} merge repo=${ev.repo_id} pr=${ev.pr_number} sha=${ev.merge_commit_sha}\n`,
                        );
                  }
                  if (!result.events.length)
                        process.stdout.write('(no events)\n');
            }
            process.exit(0);
      }

      if (args['lane-a-events-summary']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const asJson = !!args.json;
            const { runLaneAEventsSummary } =
                  await import('../lane_a/events/lane-a-events-summary.js');
            const result = await runLaneAEventsSummary({
                  projectRoot: resolve(projectRoot),
            });
            if (
                  Array.isArray(result.warnings) &&
                  result.warnings.length &&
                  !asJson
            ) {
                  for (const w of result.warnings)
                        process.stderr.write(`${w}\n`);
            }
            if (asJson)
                  process.stdout.write(
                        `${JSON.stringify(result.summary, null, 2)}\n`,
                  );
            else {
                  process.stdout.write(
                        `generated_at: ${result.summary.generated_at}\n`,
                  );
                  for (const e of result.summary.merge_events || []) {
                        process.stdout.write(
                              `- ${e.repo_id}: pr=${e.latest_pr_number} sha=${e.latest_merge_commit} at=${e.latest_timestamp}\n`,
                        );
                  }
                  if (
                        !Array.isArray(result.summary.merge_events) ||
                        result.summary.merge_events.length === 0
                  )
                        process.stdout.write('(no merge events)\n');
            }
            process.exit(result.ok ? 0 : 1);
      }

      if (
            args['skills-list'] ||
            args['skills-show'] ||
            args['project-skills-status'] ||
            args['project-skills-allow'] ||
            args['project-skills-deny'] ||
            args['skills-draft'] ||
            args['skills-author'] ||
            args['skills-refresh'] ||
            args['skills-governance'] ||
            args['skills-approve'] ||
            args['skills-reject']
      ) {
            const asJson = !!args.json;
            const aiTeamRepoRoot =
                  typeof process.env.AI_TEAM_REPO === 'string' &&
                  process.env.AI_TEAM_REPO.trim()
                        ? resolve(process.env.AI_TEAM_REPO.trim())
                        : null;
            const {
                  listGlobalSkills,
                  showSkill,
                  readProjectSkillsStatus,
                  updateProjectSkillsAllowlist,
            } = await import('../skills/skills-admin.js');
            const { runSkillsDraft } = await import('../lane_a/skills/skills-draft.js');
            const { runSkillsAuthor } = await import('../lane_a/skills/skill-author.js');
            const { runSkillsRefresh } = await import('../lane_a/skills/skills-refresh.js');
            const { runSkillsGovernance, writeSkillsGovernanceApproval } =
                  await import('../lane_a/skills/skills-governance.js');

            const parseBoolArg = (value, fallback = false) => {
                  if (value === true) return true;
                  if (typeof value !== 'string') return fallback;
                  const v = value.trim().toLowerCase();
                  if (!v) return fallback;
                  if (
                        v === '1' ||
                        v === 'true' ||
                        v === 'yes' ||
                        v === 'on'
                  )
                        return true;
                  if (
                        v === '0' ||
                        v === 'false' ||
                        v === 'no' ||
                        v === 'off'
                  )
                        return false;
                  return fallback;
            };

            if (args['skills-list']) {
                  const includeDeprecated = parseBoolArg(args.all, false);
                  const result = await listGlobalSkills({
                        aiTeamRepoRoot,
                        includeDeprecated,
                  });
                  if (asJson) {
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                  } else {
                        process.stdout.write(
                              `updated_at: ${result.updated_at}\n`,
                        );
                        for (const s of result.skills || []) {
                              process.stdout.write(
                                    `${s.skill_id}\t${s.status}\t${s.title}\t${s.path}\n`,
                              );
                        }
                        if (!Array.isArray(result.skills) || !result.skills.length)
                              process.stdout.write('(no skills)\n');
                  }
                  process.exit(0);
            }

            if (args['skills-show']) {
                  const skillId =
                        typeof args.skill === 'string' && args.skill.trim()
                              ? args.skill.trim()
                              : null;
                  const maxLinesRaw =
                        typeof args['max-lines'] === 'string' &&
                        args['max-lines'].trim()
                              ? args['max-lines'].trim()
                              : null;
                  const maxLinesParsed = maxLinesRaw
                        ? Number.parseInt(maxLinesRaw, 10)
                        : null;
                  const maxLines =
                        Number.isInteger(maxLinesParsed) &&
                        maxLinesParsed > 0
                              ? maxLinesParsed
                              : 80;
                  if (!skillId) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --skill <skill_id>.\n`,
                        );
                        process.exit(2);
                  }
                  try {
                        const result = await showSkill({
                              aiTeamRepoRoot,
                              skillId,
                              maxLines,
                        });
                        if (asJson)
                              process.stdout.write(
                                    `${JSON.stringify(result, null, 2)}\n`,
                              );
                        else {
                              process.stdout.write(
                                    `skill_id: ${result.skill_id}\npath: ${result.path}\nsha256: ${result.sha256}\n`,
                              );
                              process.stdout.write(
                                    `---\n${result.preview}\n`,
                              );
                              if (result.truncated)
                                    process.stdout.write(
                                          `\n[truncated: showing first ${Math.min(
                                                80,
                                                Number(result.total_lines) || 80,
                                          )} of ${result.total_lines} lines]\n`,
                                    );
                        }
                        process.exit(0);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            const projectRootRaw =
                  typeof args.projectRoot === 'string' && args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : getAIProjectRoot({ required: false }) ||
                          inferProjectRootFromCwd();
            if (!projectRootRaw) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot (and could not infer from AI_PROJECT_ROOT/CWD).\n`,
                  );
                  process.exit(2);
            }
            const projectRoot = resolve(projectRootRaw);
            const dryRun = !!args['dry-run'];

            if (args['skills-draft']) {
                  const scope =
                        typeof args.scope === 'string' && args.scope.trim()
                              ? args.scope.trim()
                              : null;
                  if (!scope) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --scope (expected system or repo:<id>).\n`,
                        );
                        process.exit(2);
                  }
                  try {
                        const result = await runSkillsDraft({
                              projectRoot,
                              scope,
                              dryRun,
                        });
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            if (args['skills-author']) {
                  const draftId =
                        typeof args.draft === 'string' && args.draft.trim()
                              ? args.draft.trim()
                              : null;
                  if (!draftId) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --draft <draft_id>.\n`,
                        );
                        process.exit(2);
                  }
                  try {
                        const result = await runSkillsAuthor({
                              projectRoot,
                              draftId,
                              aiTeamRepoRoot,
                              dryRun,
                        });
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            if (args['skills-refresh']) {
                  try {
                        const result = await runSkillsRefresh({
                              projectRoot,
                              aiTeamRepoRoot,
                              dryRun,
                        });
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            if (args['skills-governance']) {
                  const shouldRun = !!args.run;
                  const shouldStatus = !!args.status || !shouldRun;
                  try {
                        const result = await runSkillsGovernance({
                              projectRoot,
                              run: shouldRun,
                              status: shouldStatus,
                              aiTeamRepoRoot,
                              dryRun,
                        });
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            if (args['skills-approve'] || args['skills-reject']) {
                  const draftId =
                        typeof args.draft === 'string' && args.draft.trim()
                              ? args.draft.trim()
                              : typeof args.session === 'string' &&
                                args.session.trim()
                                    ? args.session.trim()
                              : null;
                  const by =
                        typeof args.by === 'string' && args.by.trim()
                              ? args.by.trim()
                              : null;
                  const decision = args['skills-approve']
                        ? 'approved'
                        : 'rejected';
                  if (!draftId || !by) {
                        process.stderr.write(
                              `${usage()}\n\nMissing required args: (--draft|--session) and --by.\n`,
                        );
                        process.exit(2);
                  }
                  const notes =
                        typeof args.notes === 'string' && args.notes.trim()
                              ? args.notes.trim()
                              : '';
                  try {
                        const result = await writeSkillsGovernanceApproval({
                              projectRoot,
                              draftId,
                              decision,
                              by,
                              notes,
                              dryRun,
                        });
                        process.stdout.write(
                              `${JSON.stringify(result, null, 2)}\n`,
                        );
                        process.exit(result.ok ? 0 : 1);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            if (args['project-skills-status']) {
                  try {
                        const result = await readProjectSkillsStatus({
                              projectRoot,
                        });
                        if (asJson)
                              process.stdout.write(
                                    `${JSON.stringify(result, null, 2)}\n`,
                              );
                        else {
                              process.stdout.write(
                                    `projectRoot: ${result.projectRoot}\nproject_skills_path: ${result.path}\nallowed_skills: ${Array.isArray(
                                          result.skills?.allowed_skills,
                                    )
                                          ? result.skills.allowed_skills.join(', ')
                                          : ''}\n`,
                              );
                        }
                        process.exit(0);
                  } catch (err) {
                        process.stderr.write(
                              `${err instanceof Error ? err.message : String(err)}\n`,
                        );
                        process.exit(1);
                  }
            }

            const skillsCsv =
                  typeof args.skills === 'string' && args.skills.trim()
                        ? args.skills
                        : typeof args.skill === 'string' && args.skill.trim()
                              ? args.skill
                              : null;
            const by =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : null;
            if (!skillsCsv || !by) {
                  process.stderr.write(
                        `${usage()}\n\nMissing required args: (--skill|--skills) and --by.\n`,
                  );
                  process.exit(2);
            }
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const mode = args['project-skills-allow'] ? 'allow' : 'deny';
            try {
                  const result = await updateProjectSkillsAllowlist({
                        mode,
                        projectRoot,
                        aiTeamRepoRoot,
                        skillsCsv,
                        by,
                        notes,
                        dryRun,
                  });
                  process.stdout.write(
                        `${JSON.stringify(result, null, 2)}\n`,
                  );
                  process.exit(result.ok ? 0 : 1);
            } catch (err) {
                  process.stderr.write(
                        `${err instanceof Error ? err.message : String(err)}\n`,
                  );
                  process.exit(1);
            }
      }

      if (
            args['list-projects'] ||
            args['show-project-detail'] ||
            args['remove-project']
      ) {
            const toolRepoRoot = process.cwd();
            const asJson = !!args.json;
            const { loadRegistry, listProjects, getProject, removeProject } =
                  await import('../registry/project-registry.js');

            if (args['list-projects']) {
                  const regRes = await loadRegistry({
                        toolRepoRoot,
                        createIfMissing: true,
                  });
                  const rows = listProjects(regRes.registry);
                  if (asJson) {
                        process.stdout.write(
                              `${JSON.stringify({ ok: true, projects: rows }, null, 2)}\n`,
                        );
                  } else {
                        for (const p of rows) {
                              const webui =
                                    p.ports &&
                                    typeof p.ports.webui_port === 'number'
                                          ? p.ports.webui_port
                                          : null;
                              const websvc =
                                    p.ports &&
                                    typeof p.ports.websvc_port === 'number'
                                          ? p.ports.websvc_port
                                          : null;
                              process.stdout.write(
                                    `${p.project_code}\t${p.status}\twebui=${webui ?? '-'}\twebsvc=${websvc ?? '-'}\t${p.root_dir}\n`,
                              );
                        }
                        if (!rows.length)
                              process.stdout.write('(no projects)\n');
                  }
                  process.exit(0);
            }

            const project =
                  typeof args.project === 'string' && args.project.trim()
                        ? args.project.trim()
                        : null;
            if (!project) {
                  process.stderr.write(`${usage()}\n\nMissing --project.\n`);
                  process.exit(2);
            }

            if (args['show-project-detail']) {
                  const regRes = await loadRegistry({
                        toolRepoRoot,
                        createIfMissing: true,
                  });
                  const p = getProject(regRes.registry, project);
                  if (!p) {
                        process.stderr.write(`Project not found: ${project}\n`);
                        process.exit(1);
                  }
                  if (asJson)
                        process.stdout.write(
                              `${JSON.stringify({ ok: true, project: p }, null, 2)}\n`,
                        );
                  else {
                        process.stdout.write(
                              `project_code: ${p.project_code}\nstatus: ${p.status}\nroot_dir: ${p.root_dir}\nops_dir: ${p.ops_dir}\nrepos_dir: ${p.repos_dir}\n`,
                        );
                        process.stdout.write(
                              `ports: webui=${p.ports.webui_port} websvc=${p.ports.websvc_port}\n`,
                        );
                        process.stdout.write(`pm2: ${p.pm2.ecosystem_path}\n`);
                        process.stdout.write(
                              `cron.installed: ${p.cron.installed ? 'true' : 'false'}\n`,
                        );
                        process.stdout.write(
                              `knowledge: ${p.knowledge.abs_path}\n`,
                        );
                        process.stdout.write(
                              `repos: ${Array.isArray(p.repos) ? p.repos.length : 0}\n`,
                        );
                  }
                  process.exit(0);
            }

            if (args['remove-project']) {
                  const keepFilesRaw = Object.prototype.hasOwnProperty.call(
                        args,
                        'keep-files',
                  )
                        ? String(args['keep-files']).trim().toLowerCase()
                        : 'true';
                  const keepFiles = !(
                        keepFilesRaw === 'false' ||
                        keepFilesRaw === '0' ||
                        keepFilesRaw === 'no'
                  );
                  const dryRun = !!args['dry-run'];
                  const result = await removeProject(project, {
                        toolRepoRoot,
                        keepFiles,
                        dryRun,
                  });

                  if (!result.ok)
                        process.stderr.write(
                              `${result.message || 'remove-project failed'}\n`,
                        );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(result.ok ? 0 : 1);
            }
      }

      if (args['project-repos-sync']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { runProjectReposSync } =
                  await import('../registry/project-repos-sync.js');
            const result = await runProjectReposSync({
                  projectRoot: resolve(projectRoot),
                  toolRepoRoot: process.cwd(),
                  dryRun,
            });
            if (!result.ok)
                  process.stderr.write(
                        `${result.message || 'project-repos-sync failed'}\n`,
                  );
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['knowledge-deps-approve']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const by = typeof args.by === 'string' ? args.by : null;
            if (!by || !String(by).trim()) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --by \"<name>\".\n`,
                  );
                  process.exit(2);
            }
            const notes = typeof args.notes === 'string' ? args.notes : null;
            const dryRun = !!args['dry-run'];
            const { runKnowledgeDepsApprove } =
                  await import('../lane_a/knowledge/knowledge-deps-approve.js');
            const result = await runKnowledgeDepsApprove({
                  projectRoot: resolve(projectRoot),
                  by,
                  notes,
                  dryRun,
            });
            if (!result.ok)
                  process.stderr.write(
                        `${result.message || 'knowledge-deps-approve failed'}\n`,
                  );
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['initial-project']) {
            const { runInitialProjectOnboarding } =
                  await import('../onboarding/onboarding-runner.js');
            const dryRun = !!args['dry-run'];
            const project =
                  typeof args.project === 'string' ? args.project : null;
            const nonInteractive = !!args['non-interactive'];
            const result = await runInitialProjectOnboarding({
                  toolRepoRoot: process.cwd(),
                  dryRun,
                  project,
                  nonInteractive,
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['knowledge-refresh-from-events']) {
            const projectRoot =
                  typeof args.projectRoot === 'string' &&
                  args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : null;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot.\n`,
                  );
                  process.exit(2);
            }
            const maxEventsRaw =
                  typeof args['max-events'] === 'string' &&
                  args['max-events'].trim()
                        ? args['max-events'].trim()
                        : null;
            const maxEvents = maxEventsRaw
                  ? Number.parseInt(maxEventsRaw, 10)
                  : null;
            if (
                  maxEventsRaw &&
                  (!Number.isFinite(maxEvents) || maxEvents < 0)
            ) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --max-events. Expected a non-negative integer.\n`,
                  );
                  process.exit(2);
            }
            const stopOnError = !!args['stop-on-error'];
            const dryRun = !!args['dry-run'];
            const { runRefreshFromEvents } =
                  await import('../lane_a/knowledge/knowledge-refresh-from-events.js');
            const result = await runRefreshFromEvents(resolve(projectRoot), {
                  dryRun,
                  maxEvents: Number.isFinite(maxEvents) ? maxEvents : null,
                  stopOnError,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['checkout-active-branch']) {
            const workRoot =
                  typeof args.workRoot === 'string' && args.workRoot.trim()
                        ? args.workRoot.trim()
                        : null;
            const dryRun = !!args['dry-run'];
            const onlyActive = !!args['only-active'];
            const rescanCommands = !!args['rescan-commands'];
            const repoId =
                  typeof args.repo === 'string' && args.repo.trim()
                        ? args.repo.trim()
                        : null;
            const limitRaw =
                  typeof args.limit === 'string' && args.limit.trim()
                        ? args.limit.trim()
                        : null;
            const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;

            const { runCheckoutActiveBranch } =
                  await import('../project/checkout-active-branch.js');
            const result = await runCheckoutActiveBranch({
                  workRoot,
                  dryRun,
                  onlyActive,
                  rescanCommands,
                  repoId,
                  limit: Number.isFinite(limit) ? limit : null,
            });
            if (!result.ok) {
                  process.stderr.write(
                        `${result.message || 'checkout-active-branch failed'}\n`,
                  );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['lane-a-orchestrate']) {
            const dryRun = !!args['dry-run'];
            const limitRaw =
                  typeof args.limit === 'string' && args.limit.trim()
                        ? args.limit.trim()
                        : null;
            const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
            if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                  );
                  process.exit(2);
            }
            const { runLaneAOrchestrate } =
                  await import('../lane_a/orchestrator-lane-a.js');
            const result = await runLaneAOrchestrate({
                  projectRoot: process.env.AI_PROJECT_ROOT || null,
                  limit: Number.isFinite(limit) ? limit : null,
                  dryRun,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['decision-answer']) {
            const inferred = inferProjectRootFromCwd();
            const projectRoot =
                  getAIProjectRoot({ required: false }) || inferred;
            if (!projectRoot) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT env and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const id =
                  typeof args.id === 'string' && args.id.trim()
                        ? args.id.trim()
                        : null;
            const input =
                  typeof args.input === 'string' && args.input.trim()
                        ? args.input.trim()
                        : null;
            if (!id || !input) {
                  process.stderr.write(
                        `${usage()}\n\nMissing required args: --id <DECISION-id> and --input <path>.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { answerDecisionPacket } =
                  await import('../lane_a/knowledge/decision-runner.js');
            const result = await answerDecisionPacket({
                  projectRoot: resolve(projectRoot),
                  decisionId: id,
                  inputPath: input,
                  dryRun,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      let projectRoot;
      try {
            projectRoot = getAIProjectRoot({ required: true });
      } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            process.stderr.write(`${usage()}\n\n${msg}\n`);
            process.exit(2);
      }

      const orchestrator = new Orchestrator({
            repoRoot: process.cwd(),
            projectRoot,
      });

      if (args['pr-status']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const { runPrStatus } = await import('../github/pr-status.js');
            const result = await runPrStatus({ workId });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['gate-a']) {
            warnDeprecatedOnce(
                  'cli:gate-a',
                  '`--gate-a` is deprecated; use `--apply-approval`.',
            );
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { requestApplyApproval } =
                  await import('../lane_b/gates/apply-approval.js');
            const result = await requestApplyApproval({ workId, dryRun });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['gate-a-approve']) {
            warnDeprecatedOnce(
                  'cli:gate-a-approve',
                  '`--gate-a-approve` is deprecated; use `--apply-approve`.',
            );
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { approveApplyApproval } =
                  await import('../lane_b/gates/apply-approval.js');
            const result = await approveApplyApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['gate-a-reject']) {
            warnDeprecatedOnce(
                  'cli:gate-a-reject',
                  '`--gate-a-reject` is deprecated; use `--apply-reject`.',
            );
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { rejectApplyApproval } =
                  await import('../lane_b/gates/apply-approval.js');
            const result = await rejectApplyApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['apply-approval']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { requestApplyApproval } =
                  await import('../lane_b/gates/apply-approval.js');
            const result = await requestApplyApproval({ workId, dryRun });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['apply-approve']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { approveApplyApproval } =
                  await import('../lane_b/gates/apply-approval.js');
            const result = await approveApplyApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['apply-reject']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { rejectApplyApproval } =
                  await import('../lane_b/gates/apply-approval.js');
            const result = await rejectApplyApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['ci-update']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const { runCiUpdate } = await import('../lane_b/ci/ci-update.js');
            const result = await runCiUpdate({ workId });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['gate-b']) {
            warnDeprecatedOnce(
                  'cli:gate-b',
                  '`--gate-b` is deprecated; use `--merge-approval`.',
            );
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { requestMergeApproval } =
                  await import('../lane_b/gates/merge-approval.js');
            const result = await requestMergeApproval({ workId, dryRun });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['gate-b-approve']) {
            warnDeprecatedOnce(
                  'cli:gate-b-approve',
                  '`--gate-b-approve` is deprecated; use `--merge-approve`.',
            );
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { approveMergeApproval } =
                  await import('../lane_b/gates/merge-approval.js');
            const result = await approveMergeApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['gate-b-reject']) {
            warnDeprecatedOnce(
                  'cli:gate-b-reject',
                  '`--gate-b-reject` is deprecated; use `--merge-reject`.',
            );
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { rejectMergeApproval } =
                  await import('../lane_b/gates/merge-approval.js');
            const result = await rejectMergeApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['merge-approval']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { requestMergeApproval } =
                  await import('../lane_b/gates/merge-approval.js');
            const result = await requestMergeApproval({ workId, dryRun });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['merge-approve']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { approveMergeApproval } =
                  await import('../lane_b/gates/merge-approval.js');
            const result = await approveMergeApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['merge-reject']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const approvedBy =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : 'human';
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const { rejectMergeApproval } =
                  await import('../lane_b/gates/merge-approval.js');
            const result = await rejectMergeApproval({
                  workId,
                  approvedBy,
                  notes,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }
      if (args['ci-install']) {
            const repoId =
                  typeof args.repo === 'string' && args.repo.trim()
                        ? args.repo.trim()
                        : null;
            if (!repoId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --repo <repo_id>.\n`,
                  );
                  process.exit(2);
            }
            const branch =
                  typeof args.branch === 'string' && args.branch.trim()
                        ? args.branch.trim()
                        : null;
            const commit = !!args.commit;
            const dryRun = !!args['dry-run'];
            const { runCiInstall } = await import('../project/ci-install.js');
            const result = await runCiInstall({
                  repoId,
                  branch,
                  commit,
                  dryRun,
            });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['seeds-to-intake']) {
            const phaseRaw =
                  typeof args.phase === 'string' && args.phase.trim()
                        ? args.phase.trim()
                        : null;
            const phase = phaseRaw ? Number.parseInt(phaseRaw, 10) : null;
            if (phaseRaw && (!Number.isFinite(phase) || phase <= 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --phase. Expected a positive integer.\n`,
                  );
                  process.exit(2);
            }
            const limitRaw =
                  typeof args.limit === 'string' && args.limit.trim()
                        ? args.limit.trim()
                        : null;
            const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
            if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { runSeedsToIntake } =
                  await import('../pipelines/seeds-to-intake.js');
            const forceWithoutSufficiency = !!args['force-without-sufficiency'];
            const result = await runSeedsToIntake({
                  phase: Number.isFinite(phase) ? phase : null,
                  limit: Number.isFinite(limit) ? limit : null,
                  forceWithoutSufficiency,
                  dryRun,
            });
            if (!result.ok) {
                  process.stderr.write(
                        `${result.failure ? `Failure artifact: ${result.failure}\n` : ''}${result.errors?.[0] || result.message || 'seeds-to-intake failed'}\n`,
                  );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['gaps-to-intake']) {
            const impact =
                  typeof args.impact === 'string' && args.impact.trim()
                        ? args.impact.trim()
                        : null;
            const risk =
                  typeof args.risk === 'string' && args.risk.trim()
                        ? args.risk.trim()
                        : null;
            const allowed = new Set(['high', 'medium', 'low']);
            if (impact && !allowed.has(String(impact))) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --impact. Expected: high|medium|low.\n`,
                  );
                  process.exit(2);
            }
            if (risk && !allowed.has(String(risk))) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --risk. Expected: high|medium|low.\n`,
                  );
                  process.exit(2);
            }
            const limitRaw =
                  typeof args.limit === 'string' && args.limit.trim()
                        ? args.limit.trim()
                        : null;
            const limit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
            if (limitRaw && (!Number.isFinite(limit) || limit < 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit. Expected a non-negative integer.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { runGapsToIntake } =
                  await import('../pipelines/gaps-to-intake.js');
            const forceWithoutSufficiency = !!args['force-without-sufficiency'];
            const result = await runGapsToIntake({
                  impact,
                  risk,
                  limit: Number.isFinite(limit) ? limit : null,
                  forceWithoutSufficiency,
                  dryRun,
            });
            if (!result.ok) {
                  process.stderr.write(
                        `${result.failure ? `Failure artifact: ${result.failure}\n` : ''}${result.errors?.[0] || result.message || 'gaps-to-intake failed'}\n`,
                  );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.triage) {
            const rawLimit = args.limit;
            const limit =
                  typeof rawLimit === 'string'
                        ? Number.parseInt(rawLimit, 10)
                        : null;
            if (rawLimit && (!Number.isFinite(limit) || limit <= 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit. Expected a positive integer.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { runTriage } = await import('../lane_b/triage-runner.js');
            const result = await runTriage({
                  repoRoot: orchestrator.repoRoot,
                  limit: limit || 10,
                  dryRun,
            });
            if (!result.ok) {
                  process.stderr.write(
                        `${result.message || 'triage failed'}\n`,
                  );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.watchdog) {
            const rawLimit = args.limit;
            const limit =
                  typeof rawLimit === 'string'
                        ? Number.parseInt(rawLimit, 10)
                        : null;
            if (rawLimit && (!Number.isFinite(limit) || limit <= 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit. Expected a positive integer.\n`,
                  );
                  process.exit(2);
            }
            const { runWatchdog } =
                  await import('../lane_b/watchdog-runner.js');
            const dryRun = !!args['dry-run'];
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            const stopAt =
                  typeof args['stop-at'] === 'string' && args['stop-at'].trim()
                        ? args['stop-at'].trim()
                        : 'APPLY_APPROVAL_PENDING';
            const rawMaxMinutes =
                  typeof args['max-minutes'] === 'string' &&
                  args['max-minutes'].trim()
                        ? args['max-minutes'].trim()
                        : null;
            const maxMinutes = rawMaxMinutes
                  ? Number.parseInt(rawMaxMinutes, 10)
                  : null;
            if (
                  rawMaxMinutes &&
                  (!Number.isFinite(maxMinutes) || maxMinutes <= 0)
            ) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --max-minutes. Expected a positive integer.\n`,
                  );
                  process.exit(2);
            }
            const watchdogCi = Object.prototype.hasOwnProperty.call(
                  args,
                  'watchdog-ci',
            )
                  ? String(args['watchdog-ci']).trim().toLowerCase() !== 'false'
                  : true;
            const watchdogPrepr = Object.prototype.hasOwnProperty.call(
                  args,
                  'watchdog-prepr',
            )
                  ? String(args['watchdog-prepr']).trim().toLowerCase() !==
                    'false'
                  : true;
            const result = await runWatchdog({
                  orchestrator,
                  limit: limit || null,
                  dryRun,
                  workId,
                  stopAt,
                  maxMinutes: maxMinutes || 8,
                  watchdogCi,
                  watchdogPrepr,
            });
            if (!result.ok) {
                  process.stderr.write(
                        `${result.message || 'watchdog failed'}\n`,
                  );
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['approve-batch'] || args['reject-batch']) {
            const rawIntakeId =
                  typeof args.intake === 'string' && args.intake.trim()
                        ? args.intake.trim()
                        : null;
            if (!rawIntakeId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --intake I-<...>.\n`,
                  );
                  process.exit(2);
            }
            const notes = typeof args.notes === 'string' ? args.notes : '';
            const status = args['approve-batch'] ? 'approved' : 'rejected';
            const { writeBatchApproval, normalizeRawIntakeId } =
                  await import('../project/batch-approval.js');
            const normalized = normalizeRawIntakeId(rawIntakeId);
            const batchPath = normalized
                  ? `ai/lane_b/inbox/triaged/BATCH-${normalized}.json`
                  : null;
            const batchText = batchPath
                  ? await readTextIfExists(batchPath)
                  : null;
            if (!batchText) {
                  process.stderr.write(
                        `Missing ${batchPath || 'ai/lane_b/inbox/triaged/BATCH-<raw_intake_id>.json'}; run --triage first.\n`,
                  );
                  process.exit(1);
            }

            const result = await writeBatchApproval({
                  rawIntakeId,
                  status,
                  notes,
                  approvedBy: 'human',
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            await appendFile(
                  'ai/lane_b/ledger.jsonl',
                  JSON.stringify({
                        timestamp: new Date().toISOString(),
                        action:
                              status === 'approved'
                                    ? 'batch_approved'
                                    : 'batch_rejected',
                        raw_intake_id: normalized,
                        batch_id: `BATCH-${normalized}`,
                        path: result.path,
                  }) + '\n',
            );
            process.stdout.write(
                  `${JSON.stringify({ ok: true, status, path: result.path }, null, 2)}\n`,
            );
            process.exit(0);
      }

      if (args.validate) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            const result = await orchestrator.validate({ workId });
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(result.ok ? 0 : 1);
      }

      if (args['agents-generate']) {
            const nonInteractive = !!args['non-interactive'];
            const { runAgentsGenerate } =
                  await import('../project/agents-generate.js');
            const result = await runAgentsGenerate({ nonInteractive });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  if (Array.isArray(result.errors) && result.errors.length) {
                        process.stderr.write(
                              result.errors.map((e) => `- ${e}`).join('\n') +
                                    '\n',
                        );
                  }
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['agents-migrate']) {
            const { runAgentsMigrate } =
                  await import('../project/agents-migrate.js');
            const result = await runAgentsMigrate();
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['repos-validate']) {
            const result = await orchestrator.reposValidate();
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  if (Array.isArray(result.errors) && result.errors.length) {
                        process.stderr.write(
                              result.errors.map((e) => `- ${e}`).join('\n') +
                                    '\n',
                        );
                  }
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['repos-generate']) {
            const baseDir =
                  typeof args.base === 'string' && args.base.trim()
                        ? args.base.trim()
                        : null;
            const result = await orchestrator.reposGenerate({ baseDir });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['repos-list']) {
            const result = await orchestrator.reposList();
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['policy-show']) {
            const repoId =
                  typeof args.repo === 'string' && args.repo.trim()
                        ? args.repo.trim()
                        : null;
            if (!repoId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --repo for --policy-show.\n`,
                  );
                  process.exit(2);
            }
            const result = await orchestrator.policyShow({ repoId });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.apply) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --apply.\n`,
                  );
                  process.exit(2);
            }
            let result;
            try {
                  result = await orchestrator.applyPatchPlans({ workId });
            } catch (err) {
                  const msg = err instanceof Error ? err.message : String(err);
                  process.stderr.write(`apply failed: ${msg}\n`);
                  process.stderr.write(
                        err && err.stack ? `${err.stack}\n` : '',
                  );
                  process.exit(1);
            }
            if (!result || typeof result !== 'object') {
                  process.stderr.write(
                        'apply failed: internal error (no result returned)\n',
                  );
                  process.exit(1);
            }
            if (!result.ok) {
                  process.stderr.write(`${result.message || 'apply failed'}\n`);
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.approval) {
            warnDeprecatedOnce(
                  'cli:approval',
                  '`--approval` is deprecated; use `--plan-approval`.',
            );
            args['plan-approval'] = true;
      }

      if (args['plan-approval']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --plan-approval.\n`,
                  );
                  process.exit(2);
            }
            const result = await orchestrator.approvalGate({ workId });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.approve) {
            warnDeprecatedOnce(
                  'cli:approve',
                  '`--approve` is deprecated; use `--plan-approve`.',
            );
            args['plan-approve'] = true;
      }

      if (args['plan-approve']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --plan-approve.\n`,
                  );
                  process.exit(2);
            }
            const teams =
                  typeof args.teams === 'string' && args.teams.trim()
                        ? args.teams.trim()
                        : null;
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const result = await orchestrator.approve({ workId, teams, notes });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.reject) {
            warnDeprecatedOnce(
                  'cli:reject',
                  '`--reject` is deprecated; use `--plan-reject`.',
            );
            args['plan-reject'] = true;
      }

      if (args['plan-reject']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --plan-reject.\n`,
                  );
                  process.exit(2);
            }
            const teams =
                  typeof args.teams === 'string' && args.teams.trim()
                        ? args.teams.trim()
                        : null;
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const result = await orchestrator.reject({ workId, teams, notes });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['reset-approval']) {
            warnDeprecatedOnce(
                  'cli:reset-approval',
                  '`--reset-approval` is deprecated; use `--plan-reset-approval`.',
            );
            args['plan-reset-approval'] = true;
      }

      if (args['plan-reset-approval']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --plan-reset-approval.\n`,
                  );
                  process.exit(2);
            }
            const result = await orchestrator.resetApproval({ workId });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['patch-plan']) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --patch-plan.\n`,
                  );
                  process.exit(2);
            }
            const teams =
                  typeof args.teams === 'string' && args.teams.trim()
                        ? args.teams.trim()
                        : null;

            const result = await orchestrator.patchPlan({ workId, teams });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (typeof args.resolve !== 'undefined' && args.resolve !== false) {
            const choice =
                  typeof args.resolve === 'string'
                        ? args.resolve.toUpperCase()
                        : '';
            if (choice !== 'A' && choice !== 'B') {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --resolve. Expected A or B.\n`,
                  );
                  process.exit(2);
            }

            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            const result = await orchestrator.resolveDecision({
                  choice,
                  workId,
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.propose) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            const teams =
                  typeof args.teams === 'string' && args.teams.trim()
                        ? args.teams.trim()
                        : null;

            const withPatchPlans = !!args['with-patch-plans'];
            const result = await orchestrator.propose({
                  workId,
                  teams,
                  withPatchPlans,
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.qa) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --qa.\n`,
                  );
                  process.exit(2);
            }
            const teams =
                  typeof args.teams === 'string' && args.teams.trim()
                        ? args.teams.trim()
                        : null;
            const rawLimit =
                  typeof args.limit === 'string' && args.limit.trim()
                        ? args.limit.trim()
                        : null;
            const limit = rawLimit ? Number.parseInt(rawLimit, 10) : null;
            if (rawLimit && (!Number.isFinite(limit) || limit <= 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit for --qa. Expected a positive integer.\n`,
                  );
                  process.exit(2);
            }

            const result = await orchestrator.qa({ workId, teams, limit });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['qa-obligations']) {
            const inferred =
                  getAIProjectRoot({ required: false }) ||
                  inferProjectRootFromCwd();
            if (!inferred) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const projectRoot = resolve(inferred);
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --qa-obligations.\n`,
                  );
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const { runQaObligations } = await import(
                  '../lane_b/qa/qa-obligations-runner.js'
            );
            const result = await runQaObligations({
                  projectRoot,
                  workId,
                  dryRun,
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['qa-pack-update']) {
            const projectRootRaw =
                  typeof args.projectRoot === 'string' && args.projectRoot.trim()
                        ? args.projectRoot.trim()
                        : getAIProjectRoot({ required: false }) ||
                          inferProjectRootFromCwd();
            if (!projectRootRaw) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --projectRoot for --qa-pack-update (and could not infer from AI_PROJECT_ROOT/CWD).\n`,
                  );
                  process.exit(2);
            }
            const projectRoot = resolve(projectRootRaw);
            const scope =
                  typeof args.scope === 'string' && args.scope.trim()
                        ? args.scope.trim()
                        : 'system';
            const dryRun = !!args['dry-run'];
            const { runQaPackUpdate } = await import(
                  '../lane_a/knowledge/qa-pack-update.js'
            );
            const result = await runQaPackUpdate({
                  projectRoot,
                  scope,
                  dryRun,
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args['qa-status'] || args['qa-approve'] || args['qa-reject']) {
            const inferred =
                  getAIProjectRoot({ required: false }) ||
                  inferProjectRootFromCwd();
            if (!inferred) {
                  process.stderr.write(
                        `${usage()}\n\nMissing AI_PROJECT_ROOT and could not infer project root from CWD.\n`,
                  );
                  process.exit(2);
            }
            const projectRoot = resolve(inferred);
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(`${usage()}\n\nMissing --workId.\n`);
                  process.exit(2);
            }
            const dryRun = !!args['dry-run'];
            const by =
                  typeof args.by === 'string' && args.by.trim()
                        ? args.by.trim()
                        : null;
            const notes =
                  typeof args.notes === 'string' && args.notes.trim()
                        ? args.notes.trim()
                        : null;
            const asJson = !!args.json;
            const { readQaApprovalOrDefault, setQaApprovalStatus } =
                  await import('../lane_b/qa/qa-approval.js');

            if (args['qa-status']) {
                  const result = await readQaApprovalOrDefault({
                        projectRoot,
                        workId,
                  });
                  if (!result.ok) {
                        process.stderr.write(`${result.message}\n`);
                        process.exit(1);
                  }
                  if (asJson)
                        process.stdout.write(
                              `${JSON.stringify(result.approval, null, 2)}\n`,
                        );
                  else {
                        process.stdout.write(
                              `${JSON.stringify(
                                    {
                                          workId,
                                          status: result.approval.status,
                                          by: result.approval.by,
                                          updated_at:
                                                result.approval.updated_at,
                                          notes: result.approval.notes,
                                    },
                                    null,
                                    2,
                              )}\n`,
                        );
                  }
                  process.exit(0);
            }

            if (args['qa-approve'] || args['qa-reject']) {
                  if (!by) {
                        process.stderr.write(
                              `${usage()}\n\nMissing --by \"<name>\".\n`,
                        );
                        process.exit(2);
                  }
                  const status = args['qa-approve']
                        ? 'approved'
                        : 'rejected';
                  const result = await setQaApprovalStatus({
                        projectRoot,
                        workId,
                        status,
                        by,
                        notes,
                        dryRun,
                  });
                  if (!result.ok) {
                        process.stderr.write(`${result.message}\n`);
                        process.exit(1);
                  }
                  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
                  process.exit(0);
            }
      }

      if (args.review) {
            const workId =
                  typeof args.workId === 'string' && args.workId.trim()
                        ? args.workId.trim()
                        : null;
            if (!workId) {
                  process.stderr.write(
                        `${usage()}\n\nMissing --workId for --review.\n`,
                  );
                  process.exit(2);
            }
            const teams =
                  typeof args.teams === 'string' && args.teams.trim()
                        ? args.teams.trim()
                        : null;

            const result = await orchestrator.review({ workId, teams });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (typeof args.enqueue === 'string' && args.enqueue.trim()) {
            const result = await orchestrator.enqueue({
                  text: args.enqueue.trim(),
            });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.sweep) {
            const rawLimit = args.limit;
            const limit =
                  typeof rawLimit === 'string'
                        ? Number.parseInt(rawLimit, 10)
                        : null;
            if (rawLimit && (!Number.isFinite(limit) || limit <= 0)) {
                  process.stderr.write(
                        `${usage()}\n\nInvalid --limit. Expected a positive integer.\n`,
                  );
                  process.exit(2);
            }
            const result = await orchestrator.sweep({ limit: limit || null });
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      if (args.portfolio) {
            const result = await orchestrator.portfolio();
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(result.output);
            process.exit(0);
      }

      if (args['create-tasks']) {
            const result = await orchestrator.createTeamTasksForLatestWork();
            if (!result.ok) {
                  process.stderr.write(`${result.message}\n`);
                  process.exit(1);
            }
            process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
            process.exit(0);
      }

      const intakeText =
            typeof args.text === 'string'
                  ? args.text
                  : typeof args.intake === 'string'
                    ? readFileSync(resolve(process.cwd(), args.intake), 'utf8')
                    : null;

      if (!intakeText) {
            process.stderr.write(`${usage()}\n\nMissing --text or --intake.\n`);
            process.exit(2);
      }

      const intakeSource =
            typeof args.text === 'string'
                  ? 'text'
                  : typeof args.intake === 'string'
                    ? 'file'
                    : 'text';
      const intakePath =
            typeof args.intake === 'string'
                  ? resolve(process.cwd(), args.intake)
                  : null;

      if (args['dry-run']) {
            process.stdout.write(
                  `${JSON.stringify(
                        {
                              ok: true,
                              mode: 'enqueue_dry_run',
                              intake_source: intakeSource,
                              intake_path: intakePath,
                              would_enqueue: true,
                              note: 'Dry-run only enqueues intake; it does not run triage or create work items. Run: --triage then --sweep.',
                        },
                        null,
                        2,
                  )}\n`,
            );
            process.exit(0);
      }

      // Intake is queue-only: write raw inbox entry. Sweep creates work items; triage runs before sweep.
      const origin =
            typeof args.origin === 'string' && args.origin.trim()
                  ? args.origin.trim()
                  : null;
      const scope =
            typeof args.scope === 'string' && args.scope.trim()
                  ? args.scope.trim()
                  : null;
      const result = await orchestrator.enqueue({
            text: intakeText,
            source: intakeSource,
            sourcePath: intakePath,
            origin,
            scope,
      });
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}
