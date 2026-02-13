# API: `GET /api/status-overview`

Returns a unified, resilient status snapshot for Lane A + Lane B for the selected/current project.

## Query Params
- `project` (optional): project code from registry.

## Response Schema
```json
{
  "version": 1,
  "generated_at": "<ISO>",
  "laneA": {
    "health": {
      "hard_stale": false,
      "stale": false,
      "degraded": false,
      "last_scan": "<ISO|null>",
      "last_merge_event": "<ISO|null>"
    },
    "phases": {
      "reverse": { "status": "ok|pending|blocked", "message": "<string>" },
      "sufficiency": { "status": "ok|pending|blocked", "message": "<string>" },
      "forward": { "status": "ok|pending|blocked", "message": "<string>" }
    },
    "repos": [
      {
        "repo_id": "<id>",
        "coverage": "<percent>",
        "stale": false,
        "hard_stale": false,
        "degraded": false,
        "committee_status": {},
        "latest_artifacts": {
          "refresh_hint": { "name": "<string>", "url": "<api artifact link>" },
          "decision_packet": { "name": "<string>", "url": "<api artifact link>" },
          "update_meeting": { "name": "<string>", "url": "<api artifact link>" },
          "review_meeting": { "name": "<string>", "url": "<api artifact link>" },
          "committee_report": { "name": "<string>", "url": "<api artifact link>" },
          "writer_report": { "name": "<string>", "url": "<api artifact link>" }
        }
      }
    ]
  },
  "laneB": {
    "inbox_count": 0,
    "triage_count": 0,
    "active_work": [],
    "watchdog_status": {
      "last_action": "<string|null>",
      "last_event_at": "<ISO|null>",
      "last_started_at": "<ISO|null>",
      "last_finished_at": "<ISO|null>",
      "last_failed_at": "<ISO|null>",
      "last_work_id": "<string|null>"
    }
  }
}
```

## Notes
- Output is resilient: missing files/directories produce defaults (no 500 for absent artifacts).
- Artifact references are safe API URLs only (no absolute filesystem paths).
- Used by `/lane-a`, `/lane-b`, and `/bridge` status panels.
