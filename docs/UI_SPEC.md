# WebUI Layout Spec (V1)

## Pages
- `/lane-a`
- `/lane-b`
- `/bridge`

Each page is mobile-first and uses the command registry (`/api/command-registry?webOnly=1`) as the source for command buttons.

## Shared Layout
- Top: **Status** section
- Middle: workflow tabs + command buttons
- Bottom: logs/artifacts links
- Fixed bottom navigation: `Lane A | Lane B | Bridge`
- Advanced/destructive commands are collapsed under **Advanced Commands**.

## Lane A
- Tabs: `Status | Interview | Committee | Meetings | Approvals`
- Status shows Lane A health flags, reasons, phase timeline (`Reverse → Sufficiency → Forward`), and latest artifact links.
- Commands are filtered from lane `lane_a`, then ordered by `group` + `order`.

## Lane B
- Tabs: `Status | Intake | Triage | Approvals | Work Items`
- Status shows portfolio output (work items + approval/CI/QA context from CLI output).
- Commands are filtered from lane `lane_b`, then ordered by `group` + `order`.

## Bridge
- Tabs: `Status | Events | SSOT`
- Commands include transfer/event/drift workflows from lane `bridge`.
- Commands are filtered from lane `bridge`, then ordered by `group` + `order`.

## Command Execution
- All buttons execute via `POST /api/run-command`.
- Request body:
  - `{ "cmd": "--flag", "args": { ... } }`
- Commands with required params open a form before execution.
- Commands marked `confirm: true` require user confirmation before run.

## Artifact Safety
- Artifact links use safe HTTP endpoints (for example `/lane-a/artifact`) and never expose raw absolute filesystem paths in the UI.
