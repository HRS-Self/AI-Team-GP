# AI-Team

Multi-agent SDLC automation with strict IO isolation and a hard project layout contract.

## Hard Contract V1 (layout)

Project home: `/opt/AI-Projects/<ProjectCodeName>/`

- **OPS_ROOT** (runtime, mutable): `/opt/AI-Projects/<code>/ops`
     - `ops/config/` (operational config)
     - `ops/ai/lane_a/` (Lane A runtime-only; raw scans/evidence/events segments)
     - `ops/ai/lane_b/` (Lane B runtime-only; inbox/work/approvals/etc)
- **REPOS_ROOT** (code clones): `/opt/AI-Projects/<code>/repos`
- **K_ROOT** (knowledge git repo): `/opt/AI-Projects/<code>/knowledge`
     - `knowledge/ssot/` (canonical machine-readable SSOT)
     - `knowledge/evidence/` (curated evidence + index)
     - `knowledge/sessions/`, `knowledge/decisions/`, `knowledge/views/`, `knowledge/docs/`
     - `knowledge/events/summary.json` (compact summary only; segments stay in ops)

Environment:

- `AI_PROJECT_ROOT` must be **OPS_ROOT** (must end with `/ops`).

## Onboarding / Migration

- Create a new project layout: `node src/cli.js --initial-project --project <project_code> --non-interactive`
- Dry run: `node src/cli.js --initial-project --dry-run`

### Onboarding Contract (Guardrail)

`--initial-project` is a contract, not “just scaffolding”.

It generates **routing-grade metadata** used by Lane B routing and orchestration. Do **not** remove, rename, or silently disable this generation logic (or the generated fields) without **explicit confirmation from the project owner**.

Key generated outputs under `AI_PROJECT_ROOT/config/`:

- `TEAMS.json` (teams + `scope_hints[]` + `risk_level` used by routing)
- `AGENTS.json` (agent pool per team; required for execution after routing)
- `REPOS.json` (repo registry including inferred `active_branch`, `keywords[]`, and `commands` used for routing + deterministic execution)

Legacy migration (from `/opt/AI-Projects/<code>`):

- Set `AI_PROJECT_ROOT=/opt/AI-Projects/<code>` and run: `node src/cli.js --migrate-project-layout`

## Project Registry (global, host-local)

The global registry lives in the AI-Team repo at:

- `ai/registry/REGISTRY.json` (source of truth)

Commands:

- `node src/cli.js --list-projects [--json]`
- `node src/cli.js --show-project-detail --project <project_code> [--json]`
- `node src/cli.js --remove-project --project <project_code> [--keep-files true|false] [--dry-run]`
- `node src/cli.js --project-repos-sync --projectRoot <abs> [--dry-run]`

## Config (OPS_ROOT)

Required config files under `AI_PROJECT_ROOT/config/`:

- `PROJECT.json` (must include `repos_root_abs`, `ops_root_abs`, `knowledge_repo_dir`)
- `REPOS.json`, `TEAMS.json`, `AGENTS.json`, `LLM_PROFILES.json`, `POLICIES.json`, `DOCS.json`

## Lane A (Knowledge)

Lane A runtime-only outputs:

- `AI_PROJECT_ROOT/ai/lane_a/scans_raw/`
- `AI_PROJECT_ROOT/ai/lane_a/evidence_raw/`
- `AI_PROJECT_ROOT/ai/lane_a/events/segments/*.jsonl` (+ checkpoints)

Lane A curated outputs (git-worthy) are written only to K_ROOT:

- `ssot/system/*`, `ssot/repos/<repo_id>/*`
- `evidence/index/**` (repo index + fingerprints)
- `events/summary.json` (derived from ops segments)

Committee contract note:

- `committee_status.json.blocking_issues[].evidence_missing` is always present and must be `string[]` of actionable descriptions (not IDs), e.g. `need evidence: file: src/auth/** ...`.

### Cross-project dependencies (dependency graph → approve → scan)

Lane A reuses knowledge from other projects via an explicit, human-approved dependency graph stored in the knowledge repo:

- `K_ROOT/ssot/system/dependency_graph.json` (detected, deterministic)
- `K_ROOT/ssot/system/dependency_graph.override.json` (human overlay; must be `status=approved` before scan)

Workflow:

1. `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-index`
2. Review/edit `K_ROOT/ssot/system/dependency_graph.override.json`
3. Approve: `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-deps-approve --projectRoot /opt/AI-Projects/<code>/ops --by "Your Name"`
4. Scan: `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-scan`

Notes:

- `--knowledge-scan` refuses to run while dependencies are pending approval (writes ops blocker: `OPS_ROOT/ai/lane_a/blockers/DEPS_NOT_APPROVED.json`).
- You can bypass for debugging with `--force-without-deps-approval`, but it will still fail if required external knowledge artifacts are missing.
- If an external dependency is missing required artifacts, scan fails with `external_dependency_bundle_missing`; run Lane A in the external project first (`--knowledge-index` + `--knowledge-scan`).

## Lane B (Delivery pipeline)

All Lane B state is under:

- `AI_PROJECT_ROOT/ai/lane_b/inbox/`
- `AI_PROJECT_ROOT/ai/lane_b/work/<workId>/`
- `AI_PROJECT_ROOT/ai/lane_b/approvals/`, `AI_PROJECT_ROOT/ai/lane_b/schedule/`, `AI_PROJECT_ROOT/ai/lane_b/cache/`

## QA enforcement (pre-apply + merge audit)

QA is a **contract of obligations** that must be defined **before apply**. Merge-time QA is an **audit that obligations were met**, not a second “invent tests now” phase.

Lane A (committee role: `qa-strategist`, CLI mode `qa_strategist`):

- Prompt: `src/llm/prompts/committee/qa-strategist.system.txt`
- Run: `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-committee --projectRoot /opt/AI-Projects/<code>/ops --scope (system|repo:<repo_id>) --mode qa_strategist`
- Outputs:
     - `AI_PROJECT_ROOT/ai/lane_a/committee/<TS>/qa_strategist.<scope>.json`
     - `AI_PROJECT_ROOT/ai/lane_a/committee/<TS>/qa_strategist.<scope>.md`
     - where `<scope>` is `system` or `repo_<repo_id>`

Lane B (obligations + approval):

- QA plan inspection (Lane B LLM role: `qa-inspector`, agent role `qa_inspector`):
     - Prompt: `src/llm/prompts/qa_inspector.system.txt`
- Generate obligations (required before apply): `node src/cli.js --qa-obligations --workId <workId>`
     - Writes: `AI_PROJECT_ROOT/ai/lane_b/work/<workId>/QA/obligations.json` and `.md`
- QA approval state (optional unless waiving obligations):
     - File: `AI_PROJECT_ROOT/ai/lane_b/work/<workId>/QA_APPROVAL.json` (missing = `pending`)
     - CLI:
          - `node src/cli.js --qa-status --workId <workId> [--json]`
          - `node src/cli.js --qa-approve --workId <workId> --by "Your Name" [--notes "..."]`
          - `node src/cli.js --qa-reject  --workId <workId> --by "Your Name" [--notes "..."]`

Enforcement rules:

- **Apply** is blocked if `obligations.must_add_unit|integration|e2e == true` and patch plans contain **no corresponding test edits**.
- To proceed you must either:
     - add tests into patch plans (recommended), or
     - explicitly waive the obligation in QA approval notes, e.g. `--notes "waive: unit"` / `--notes "waive: integration"` / `--notes "waive: e2e"` / `--notes "waive: all"`.
- **Merge approval** does not invent tests; it only audits that obligations were satisfied (or explicitly waived) and still requires **CI_GREEN**.

Post-merge QA automation:

- On merge approval approval, Lane B emits an enriched merge event record (ops events segments) that includes:
     - `workId`, `pr`, `merge_sha`, `changed_paths`, `obligations`, `risk_level` (plus legacy merge fields for compatibility).
- Lane A orchestrator consumes QA merge events:
     - If `obligations.must_add_e2e == true` and merged files contain no E2E test edits, it auto-enqueues a Lane B follow-up intake: `Add E2E tests for <scope>`.
     - Follow-up intake includes linkage headers back to the original `workId` and merge event id.
- If a QA obligation is explicitly waived (for example via `QA_APPROVAL.notes` containing `waive: ...`), merge approval auto-creates a decision packet in `K_ROOT/decisions/` with:
     - `type: INVARIANT_WAIVER`
     - status `open` and answer flow via `node src/cli.js --decision-answer --id <DECISION-id> --input <file>`.
- Optional dual-signoff policy is enforced for high-risk QA (`risk_level=high`):
     - merge approval requires both owner approval and QA approval (`QA_APPROVAL.status=approved`);
     - persisted in `AI_PROJECT_ROOT/ai/lane_b/work/<workId>/MERGE_APPROVAL.json` via `dual_signoff_required`, `owner_signoff`, `qa_signoff`.

## WebUI command registry (single source of truth)

WebUI command exposure is fully data-driven from:

- `src/web/commandRegistry.js`

Every command entry includes:

```json
{
  "cmd": "--knowledge-scan",
  "lane": "lane_a",
  "exposeInWebUI": true,
  "label": "Scan Knowledge",
  "description": "Runs repository knowledge scan.",
  "group": "lane_a_pipeline",
  "order": 30,
  "params": [
    { "name": "repo", "type": "string", "required": false },
    { "name": "limit", "type": "int", "required": false },
    { "name": "concurrency", "type": "int", "required": false }
  ]
}
```

Web API:

- `GET /api/command-registry?webOnly=1`
- `POST /api/run-command` with `{ "cmd": "--...", "args": { ... } }`

Rules:

- `/api/run-command` only runs commands where `exposeInWebUI=true`.
- `project_admin` lane commands are blocked in `/api/run-command`.
- UI pages must not hardcode CLI flags; they resolve actions from the registry.

### Commands currently exposed in WebUI

| lane | group | order | cmd | label |
|---|---:|---:|---|---|
| lane_a | lane_a_interview | 10 | `--knowledge-interview` | Knowledge Interview |
| lane_a | lane_a_meetings | 10 | `--knowledge-change-status` | Change Requests |
| lane_a | lane_a_meetings | 20 | `--knowledge-change-request` | Submit Change Request |
| lane_a | lane_a_meetings | 30 | `--knowledge-update-meeting` | Update Meeting |
| lane_a | lane_a_meetings | 40 | `--knowledge-review-answer` | Review Meeting Answer |
| lane_a | lane_a_phases | 10 | `--knowledge-phase-status` | Phase Status |
| lane_a | lane_a_phases | 20 | `--knowledge-confirm-v1` | Confirm v1 |
| lane_a | lane_a_phases | 30 | `--knowledge-phase-close` | Close Phase |
| lane_a | lane_a_phases | 40 | `--knowledge-kickoff-reverse` | Kickoff Reverse |
| lane_a | lane_a_phases | 50 | `--knowledge-kickoff-forward` | Kickoff Forward |
| lane_a | lane_a_pipeline | 10 | `--knowledge-status` | Knowledge Status |
| lane_a | lane_a_pipeline | 20 | `--knowledge-refresh` | Refresh Knowledge |
| lane_a | lane_a_pipeline | 30 | `--knowledge-scan` | Scan Knowledge |
| lane_a | lane_a_pipeline | 40 | `--knowledge-synthesize` | Synthesize Knowledge |
| lane_b | lane_b_intake | 10 | `--text` | Submit Intake |
| lane_b | lane_b_intake | 20 | `--triage` | Triage Intake |
| lane_b | lane_b_intake | 30 | `--sweep` | Sweep |
| lane_b | lane_b_intake | 40 | `--watchdog` | Watchdog |
| lane_b | lane_b_intake | 50 | `--portfolio` | Portfolio |
| bridge | bridge | 10 | `--lane-a-to-lane-b` | Lane A → Lane B |

## Common commands

Validate project instance:

- `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --validate`

Normalize clones to active branches:

- `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --checkout-active-branch --workRoot /opt/AI-Projects/<code>/ops --only-active`

Knowledge interview:

- `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-interview --projectRoot /opt/AI-Projects/<code>/ops --scope system --start`

Lane A orchestrator:

- `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --lane-a-orchestrate --limit 1`

Knowledge sufficiency (versioned, delivery gate):

- Status (defaults to current `ops/ai/lane_a/knowledge_version.json`): `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-sufficiency --projectRoot /opt/AI-Projects/<code>/ops --scope system --status`
- Propose draft for a version: `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-sufficiency --projectRoot /opt/AI-Projects/<code>/ops --scope system --version v1.0.0 --propose`
- Approve (human): `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-sufficiency --projectRoot /opt/AI-Projects/<code>/ops --scope system --version v1.0.0 --approve --by "Your Name"`
- Records:
     - Ops latest pointer: `OPS_ROOT/ai/lane_a/sufficiency/SUFFICIENCY.json`
     - Ops history: `OPS_ROOT/ai/lane_a/sufficiency/history/`
     - Git decision store: `K_ROOT/decisions/sufficiency/` (+ `LATEST.json`)

Committee (challenge mode, one question per run; no LLM calls):

- `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --knowledge-committee --projectRoot /opt/AI-Projects/<code>/ops --scope system --mode challenge --max-questions 1`

Seeds/Gaps → Intake (deterministic, idempotent):

- Seeds input: `K_ROOT/ssot/system/BACKLOG_SEEDS.json`
- Gaps input: `K_ROOT/ssot/system/GAPS.json`
- Run:
     - `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --seeds-to-intake --phase 1 --limit 10`
     - `AI_PROJECT_ROOT=/opt/AI-Projects/<code>/ops node src/cli.js --gaps-to-intake --impact high --limit 10`

Verification helpers:

- `bash scripts/verify-gap2-hardening.sh`
- `bash scripts/verify-seeds-gaps-to-intake.sh`
