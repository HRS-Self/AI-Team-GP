const ALL_COMMANDS = Object.freeze([
  "--agents-generate",
  "--agents-migrate",
  "--apply",
  "--apply-approval",
  "--apply-approve",
  "--apply-reject",
  "--approval",
  "--approve",
  "--approve-batch",
  "--checkout-active-branch",
  "--ci-install",
  "--ci-update",
  "--create-tasks",
  "--decision-answer",
  "--enqueue",
  "--gaps-to-intake",
  "--gate-a",
  "--gate-a-approve",
  "--gate-a-reject",
  "--gate-b",
  "--gate-b-approve",
  "--gate-b-reject",
  "--initial-project",
  "--intake",
  "--knowledge-bundle",
  "--knowledge-change-request",
  "--knowledge-change-status",
  "--knowledge-committee",
  "--knowledge-committee-status",
  "--knowledge-confirm-v1",
  "--knowledge-deps-approve",
  "--knowledge-events-status",
  "--knowledge-extract-tasks",
  "--knowledge-index",
  "--knowledge-interview",
  "--knowledge-kickoff",
  "--knowledge-kickoff-forward",
  "--knowledge-kickoff-reverse",
  "--knowledge-phase-close",
  "--knowledge-phase-status",
  "--knowledge-refresh",
  "--knowledge-refresh-from-events",
  "--knowledge-review-answer",
  "--knowledge-review-meeting",
  "--knowledge-scan",
  "--knowledge-staleness",
  "--knowledge-status",
  "--knowledge-sufficiency",
  "--knowledge-sufficiency-confirm",
  "--knowledge-sufficiency-propose",
  "--knowledge-sufficiency-revoke",
  "--knowledge-sufficiency-status",
  "--knowledge-synthesize",
  "--knowledge-update-answer",
  "--knowledge-update-meeting",
  "--lane-a-events-summary",
  "--lane-a-orchestrate",
  "--lane-a-to-lane-b",
  "--lane-b-events-list",
  "--list-projects",
  "--merge-approval",
  "--merge-approve",
  "--merge-reject",
  "--migrate-project-layout",
  "--patch-plan",
  "--plan-approval",
  "--plan-approve",
  "--plan-reject",
  "--plan-reset-approval",
  "--policy-show",
  "--portfolio",
  "--pr-status",
  "--project-repos-sync",
  "--propose",
  "--qa",
  "--qa-approve",
  "--qa-obligations",
  "--qa-pack-update",
  "--qa-reject",
  "--qa-status",
  "--reject",
  "--reject-batch",
  "--remove-project",
  "--repos-generate",
  "--repos-list",
  "--repos-validate",
  "--reset-approval",
  "--resolve",
  "--review",
  "--seeds-to-intake",
  "--show-project-detail",
  "--ssot-drift-check",
  "--ssot-resolve",
  "--sweep",
  "--text",
  "--triage",
  "--validate",
  "--watchdog",
  "--writer",
]);

const BRIDGE_COMMANDS = new Set([
  "--gaps-to-intake",
  "--knowledge-events-status",
  "--lane-a-to-lane-b",
  "--lane-b-events-list",
  "--seeds-to-intake",
  "--ssot-drift-check",
]);

const LANE_B_COMMANDS = new Set([
  "--apply",
  "--apply-approval",
  "--apply-approve",
  "--apply-reject",
  "--approval",
  "--approve",
  "--approve-batch",
  "--ci-update",
  "--create-tasks",
  "--enqueue",
  "--gate-a",
  "--gate-a-approve",
  "--gate-a-reject",
  "--gate-b",
  "--gate-b-approve",
  "--gate-b-reject",
  "--intake",
  "--merge-approval",
  "--merge-approve",
  "--merge-reject",
  "--patch-plan",
  "--plan-approval",
  "--plan-approve",
  "--plan-reject",
  "--plan-reset-approval",
  "--portfolio",
  "--pr-status",
  "--propose",
  "--qa",
  "--qa-approve",
  "--qa-obligations",
  "--qa-reject",
  "--qa-status",
  "--reject",
  "--reject-batch",
  "--reset-approval",
  "--resolve",
  "--review",
  "--sweep",
  "--text",
  "--triage",
  "--validate",
  "--watchdog",
]);

const LANE_A_COMMANDS = new Set([
  "--decision-answer",
  "--knowledge-bundle",
  "--knowledge-change-request",
  "--knowledge-change-status",
  "--knowledge-committee",
  "--knowledge-committee-status",
  "--knowledge-confirm-v1",
  "--knowledge-deps-approve",
  "--knowledge-events-status",
  "--knowledge-extract-tasks",
  "--knowledge-index",
  "--knowledge-interview",
  "--knowledge-kickoff",
  "--knowledge-kickoff-forward",
  "--knowledge-kickoff-reverse",
  "--knowledge-phase-close",
  "--knowledge-phase-status",
  "--knowledge-refresh",
  "--knowledge-refresh-from-events",
  "--knowledge-review-answer",
  "--knowledge-review-meeting",
  "--knowledge-scan",
  "--knowledge-staleness",
  "--knowledge-status",
  "--knowledge-sufficiency",
  "--knowledge-sufficiency-confirm",
  "--knowledge-sufficiency-propose",
  "--knowledge-sufficiency-revoke",
  "--knowledge-sufficiency-status",
  "--knowledge-synthesize",
  "--knowledge-update-answer",
  "--knowledge-update-meeting",
  "--lane-a-events-summary",
  "--lane-a-orchestrate",
  "--qa-pack-update",
  "--ssot-resolve",
  "--writer",
]);

const UI_OVERRIDES = {
  "--text": {
    exposeInWebUI: true,
    label: "Submit Intake",
    description: "Create a raw intake item.",
    group: "Lane B Intake",
    tab: "Intake",
    order: 10,
    webAction: "intake_submit",
    params: [
      { name: "text", type: "string", required: true },
      { name: "origin", type: "string", required: false },
      { name: "scope", type: "string", required: false },
    ],
  },
  "--triage": {
    exposeInWebUI: true,
    label: "Triage",
    description: "Triages inbox items into repo-scoped tasks.",
    group: "Lane B Workflow",
    tab: "Triage",
    order: 20,
    webAction: "triage",
    params: [{ name: "limit", type: "int", required: false }],
  },
  "--sweep": {
    exposeInWebUI: true,
    label: "Sweep",
    description: "Creates work items from triaged intake.",
    group: "Lane B Workflow",
    tab: "Triage",
    order: 10,
    webAction: "sweep",
    params: [{ name: "limit", type: "int", required: false }],
  },
  "--propose": {
    exposeInWebUI: true,
    label: "Propose",
    description: "Generates proposal for a work item.",
    group: "Lane B Workflow",
    tab: "Triage",
    order: 30,
    params: [
      { name: "workId", type: "string", required: false },
      { name: "teams", type: "string", required: false },
      { name: "with-patch-plans", type: "bool", required: false },
    ],
    confirm: true,
  },
  "--plan-approval": {
    exposeInWebUI: true,
    label: "Plan Approval",
    description: "Reads plan approval state.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 40,
    params: [{ name: "workId", type: "string", required: true }],
  },
  "--plan-approve": {
    exposeInWebUI: true,
    label: "Plan Approve",
    description: "Approves plan for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 50,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "teams", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--plan-reject": {
    exposeInWebUI: true,
    label: "Plan Reject",
    description: "Rejects plan for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 55,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "teams", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--qa-obligations": {
    exposeInWebUI: true,
    label: "QA Obligations",
    description: "Generates QA obligations before apply.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 60,
    params: [{ name: "workId", type: "string", required: true }],
  },
  "--qa-status": {
    exposeInWebUI: true,
    label: "QA Status",
    description: "Reads QA approval state for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 65,
    params: [{ name: "workId", type: "string", required: true }],
  },
  "--qa-approve": {
    exposeInWebUI: true,
    label: "QA Approve",
    description: "Approves QA state for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 66,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "by", type: "string", required: true },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--qa-reject": {
    exposeInWebUI: true,
    label: "QA Reject",
    description: "Rejects QA state for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 67,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "by", type: "string", required: true },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--apply-approval": {
    exposeInWebUI: true,
    label: "Apply Approval",
    description: "Reads apply approval state.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 70,
    params: [{ name: "workId", type: "string", required: true }],
  },
  "--apply-approve": {
    exposeInWebUI: true,
    label: "Apply Approve",
    description: "Approves apply stage.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 80,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "by", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--apply-reject": {
    exposeInWebUI: true,
    label: "Apply Reject",
    description: "Rejects apply stage.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 85,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "by", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--apply": {
    exposeInWebUI: true,
    label: "Apply",
    description: "Applies approved patch plans.",
    group: "Lane B Workflow",
    tab: "Work Items",
    order: 90,
    params: [{ name: "workId", type: "string", required: true }],
    confirm: true,
  },
  "--watchdog": {
    exposeInWebUI: true,
    label: "Watchdog",
    description: "Advances Lane B stages deterministically.",
    group: "Lane B Workflow",
    tab: "Work Items",
    order: 100,
    webAction: "watchdog",
    params: [
      { name: "limit", type: "int", required: false },
      { name: "workId", type: "string", required: false },
      { name: "stop-at", type: "string", required: false },
      { name: "max-minutes", type: "int", required: false },
      { name: "watchdog-ci", type: "bool", required: false },
      { name: "watchdog-prepr", type: "bool", required: false },
    ],
    confirm: true,
  },
  "--merge-approval": {
    exposeInWebUI: true,
    label: "Merge Approval",
    description: "Reads merge approval state.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 110,
    params: [{ name: "workId", type: "string", required: true }],
  },
  "--merge-approve": {
    exposeInWebUI: true,
    label: "Merge Approve",
    description: "Approves merge for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 120,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "by", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--merge-reject": {
    exposeInWebUI: true,
    label: "Merge Reject",
    description: "Rejects merge for a work item.",
    group: "Lane B Workflow",
    tab: "Approvals",
    order: 125,
    params: [
      { name: "workId", type: "string", required: true },
      { name: "by", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--portfolio": {
    exposeInWebUI: true,
    label: "Portfolio",
    description: "Shows current Lane B work portfolio.",
    group: "Lane B Status",
    tab: "Status",
    order: 10,
    webAction: "portfolio",
    params: [],
  },
  "--knowledge-interview": {
    exposeInWebUI: true,
    label: "Knowledge Interview",
    description: "Runs knowledge interview start/continue.",
    group: "Lane A → Interview",
    tab: "Interview",
    order: 10,
    webAction: "knowledge_interview",
    params: [
      { name: "scope", type: "string", required: true },
      { name: "start", type: "bool", required: false },
      { name: "continue", type: "bool", required: false },
      { name: "session", type: "string", required: false },
      { name: "max-questions", type: "int", required: false },
    ],
  },
  "--knowledge-status": {
    exposeInWebUI: true,
    label: "Knowledge Status",
    description: "Reads Lane A knowledge status.",
    group: "Lane A → Status",
    tab: "Status",
    order: 10,
    webAction: "knowledge_status",
    params: [{ name: "json", type: "bool", required: false }],
    injectProjectRoot: true,
  },
  "--knowledge-phase-status": {
    exposeInWebUI: true,
    label: "Phase Status",
    description: "Reads Lane A reverse/forward phase status.",
    group: "Lane A → Status",
    tab: "Status",
    order: 15,
    webAction: "knowledge_phase_status",
    params: [{ name: "json", type: "bool", required: false }],
  },
  "--knowledge-index": {
    exposeInWebUI: true,
    label: "Knowledge Index",
    description: "Indexes repositories for deterministic scans.",
    group: "Lane A → Status",
    tab: "Status",
    order: 20,
    params: [{ name: "limit", type: "int", required: false }],
    confirm: true,
  },
  "--knowledge-scan": {
    exposeInWebUI: true,
    label: "Knowledge Scan",
    description: "Runs repository knowledge scan.",
    group: "Lane A → Status",
    tab: "Status",
    order: 30,
    webAction: "knowledge_scan",
    params: [
      { name: "repo", type: "string", required: false },
      { name: "limit", type: "int", required: false },
      { name: "concurrency", type: "int", required: false },
    ],
    confirm: true,
  },
  "--knowledge-kickoff-reverse": {
    exposeInWebUI: true,
    label: "Kickoff Reverse",
    description: "Runs reverse kickoff in non-interactive mode.",
    group: "Lane A → Status",
    tab: "Status",
    order: 40,
    webAction: "knowledge_kickoff_reverse",
    params: [
      { name: "scope", type: "string", required: false },
      { name: "start", type: "bool", required: false },
      { name: "continue", type: "bool", required: false },
      { name: "non-interactive", type: "bool", required: false },
      { name: "input-file", type: "string", required: true },
      { name: "session", type: "string", required: false },
      { name: "max-questions", type: "int", required: false },
    ],
    defaultArgs: { start: true, "non-interactive": true },
  },
  "--knowledge-committee": {
    exposeInWebUI: true,
    label: "Knowledge Committee",
    description: "Runs committee review for a scope.",
    group: "Lane A → Status",
    tab: "Status",
    order: 50,
    webAction: "knowledge_committee",
    params: [
      { name: "scope", type: "string", required: false },
      { name: "limit", type: "int", required: false },
      { name: "mode", type: "string", required: false },
      { name: "max-questions", type: "int", required: false },
    ],
    injectProjectRoot: true,
    confirm: true,
  },
  "--knowledge-sufficiency": {
    exposeInWebUI: true,
    label: "Knowledge Sufficiency",
    description: "Checks/proposes/approves sufficiency for a scope+version.",
    group: "Lane A → Status",
    tab: "Status",
    order: 60,
    webAction: "knowledge_sufficiency",
    params: [
      { name: "scope", type: "string", required: false },
      { name: "version", type: "string", required: false },
      { name: "status", type: "bool", required: false },
      { name: "propose", type: "bool", required: false },
      { name: "approve", type: "bool", required: false },
      { name: "reject", type: "bool", required: false },
      { name: "by", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    defaultArgs: { scope: "system", status: true },
    injectProjectRoot: true,
  },
  "--knowledge-confirm-v1": {
    exposeInWebUI: true,
    label: "Confirm v1",
    description: "Human confirms v1 understanding.",
    group: "Lane A → Status",
    tab: "Status",
    order: 70,
    webAction: "knowledge_confirm_v1",
    params: [
      { name: "by", type: "string", required: true },
      { name: "notes", type: "string", required: false },
    ],
  },
  "--knowledge-kickoff-forward": {
    exposeInWebUI: true,
    label: "Kickoff Forward",
    description: "Runs forward kickoff in non-interactive mode.",
    group: "Lane A → Status",
    tab: "Status",
    order: 80,
    webAction: "knowledge_kickoff_forward",
    params: [
      { name: "scope", type: "string", required: false },
      { name: "start", type: "bool", required: false },
      { name: "continue", type: "bool", required: false },
      { name: "non-interactive", type: "bool", required: false },
      { name: "input-file", type: "string", required: true },
      { name: "session", type: "string", required: false },
      { name: "max-questions", type: "int", required: false },
    ],
    defaultArgs: { start: true, "non-interactive": true },
  },
  "--knowledge-committee-status": {
    exposeInWebUI: true,
    label: "Committee Status",
    description: "Reads committee status summary.",
    group: "Lane A → Committee",
    tab: "Committee",
    order: 10,
    params: [],
    injectProjectRoot: true,
  },
  "--knowledge-change-status": {
    exposeInWebUI: true,
    label: "Change Requests",
    description: "Reads knowledge change request queue/status.",
    group: "Lane A → Meetings",
    tab: "Meetings",
    order: 10,
    webAction: "knowledge_change_status",
    params: [{ name: "json", type: "bool", required: false }],
    injectProjectRoot: true,
  },
  "--knowledge-change-request": {
    exposeInWebUI: true,
    label: "Submit Change Request",
    description: "Creates a change request from web input.",
    group: "Lane A → Meetings",
    tab: "Meetings",
    order: 20,
    webAction: "knowledge_change_request",
    params: [
      { name: "type", type: "string", required: true },
      { name: "scope", type: "string", required: true },
      { name: "input", type: "string", required: true },
    ],
    injectProjectRoot: true,
  },
  "--knowledge-update-meeting": {
    exposeInWebUI: true,
    label: "Update Meeting",
    description: "Runs update meeting start/continue/close.",
    group: "Lane A → Meetings",
    tab: "Meetings",
    order: 30,
    webAction: "knowledge_update_meeting",
    params: [
      { name: "scope", type: "string", required: false },
      { name: "start", type: "bool", required: false },
      { name: "continue", type: "bool", required: false },
      { name: "close", type: "bool", required: false },
      { name: "session", type: "string", required: false },
      { name: "decision", type: "string", required: false },
      { name: "notes", type: "string", required: false },
    ],
    injectProjectRoot: true,
  },
  "--knowledge-review-meeting": {
    exposeInWebUI: true,
    label: "Review Meeting",
    description: "Runs review meeting start/continue/close.",
    group: "Lane A → Meetings",
    tab: "Meetings",
    order: 40,
    params: [
      { name: "scope", type: "string", required: false },
      { name: "start", type: "bool", required: false },
      { name: "continue", type: "bool", required: false },
      { name: "status", type: "bool", required: false },
      { name: "close", type: "bool", required: false },
      { name: "session", type: "string", required: false },
      { name: "decision", type: "string", required: false },
      { name: "notes", type: "string", required: false },
      { name: "max-questions", type: "int", required: false },
    ],
    injectProjectRoot: true,
  },
  "--knowledge-review-answer": {
    exposeInWebUI: true,
    label: "Review Meeting Answer",
    description: "Sends answer content into review meeting.",
    group: "Lane A → Meetings",
    tab: "Meetings",
    order: 50,
    webAction: "knowledge_review_answer",
    params: [
      { name: "session", type: "string", required: true },
      { name: "input", type: "string", required: true },
    ],
    injectProjectRoot: true,
  },
  "--knowledge-update-answer": {
    exposeInWebUI: true,
    label: "Update Meeting Answer",
    description: "Submits answer to update meeting.",
    group: "Lane A → Meetings",
    tab: "Meetings",
    order: 60,
    params: [
      { name: "session", type: "string", required: true },
      { name: "input", type: "string", required: true },
    ],
    injectProjectRoot: true,
  },
  "--knowledge-sufficiency-status": {
    exposeInWebUI: true,
    label: "Sufficiency Status",
    description: "Reads sufficiency status pointer.",
    group: "Lane A → Approvals",
    tab: "Approvals",
    order: 10,
    params: [{ name: "json", type: "bool", required: false }],
    injectProjectRoot: true,
  },
  "--knowledge-sufficiency-propose": {
    exposeInWebUI: true,
    label: "Sufficiency Propose",
    description: "Proposes sufficiency draft.",
    group: "Lane A → Approvals",
    tab: "Approvals",
    order: 20,
    params: [],
    injectProjectRoot: true,
  },
  "--knowledge-sufficiency-confirm": {
    exposeInWebUI: true,
    label: "Sufficiency Confirm",
    description: "Confirms sufficiency with a human identity.",
    group: "Lane A → Approvals",
    tab: "Approvals",
    order: 30,
    params: [{ name: "by", type: "string", required: true }],
    injectProjectRoot: true,
    confirm: true,
  },
  "--knowledge-sufficiency-revoke": {
    exposeInWebUI: true,
    label: "Sufficiency Revoke",
    description: "Revokes sufficiency state.",
    group: "Lane A → Approvals",
    tab: "Approvals",
    order: 40,
    params: [{ name: "reason", type: "string", required: true }],
    injectProjectRoot: true,
    confirm: true,
  },
  "--decision-answer": {
    exposeInWebUI: true,
    label: "Decision Answer",
    description: "Answers an open decision packet.",
    group: "Lane A → Approvals",
    tab: "Approvals",
    order: 50,
    params: [
      { name: "id", type: "string", required: true },
      { name: "input", type: "string", required: true },
    ],
  },
  "--knowledge-phase-close": {
    exposeInWebUI: true,
    label: "Close Phase",
    description: "Closes a knowledge phase.",
    group: "Lane A → Approvals",
    tab: "Approvals",
    order: 60,
    webAction: "knowledge_phase_close",
    params: [
      { name: "phase", type: "string", required: true },
      { name: "by", type: "string", required: true },
      { name: "notes", type: "string", required: false },
    ],
    confirm: true,
  },
  "--lane-a-to-lane-b": {
    exposeInWebUI: true,
    label: "Lane A → Lane B",
    description: "Moves approved Lane A outputs into Lane B intake.",
    group: "Bridge Workflow",
    tab: "Status",
    order: 10,
    webAction: "lane_a_to_lane_b",
    params: [{ name: "limit", type: "int", required: false }],
    injectProjectRoot: true,
    confirm: true,
  },
  "--gaps-to-intake": {
    exposeInWebUI: true,
    label: "Gaps → Intake",
    description: "Creates Lane B intake from knowledge gaps.",
    group: "Bridge Workflow",
    tab: "Status",
    order: 20,
    params: [
      { name: "impact", type: "string", required: false },
      { name: "risk", type: "string", required: false },
      { name: "limit", type: "int", required: false },
      { name: "force-without-sufficiency", type: "bool", required: false },
    ],
    confirm: true,
  },
  "--lane-b-events-list": {
    exposeInWebUI: true,
    label: "Lane B Events",
    description: "Lists recent Lane B merge events.",
    group: "Bridge Workflow",
    tab: "Events",
    order: 30,
    params: [
      { name: "from", type: "string", required: false },
      { name: "to", type: "string", required: false },
      { name: "json", type: "bool", required: false },
    ],
    injectProjectRoot: true,
  },
  "--knowledge-events-status": {
    exposeInWebUI: true,
    label: "Knowledge Events Status",
    description: "Shows knowledge event stream status.",
    group: "Bridge Workflow",
    tab: "Events",
    order: 40,
    params: [{ name: "json", type: "bool", required: false }],
  },
  "--ssot-drift-check": {
    exposeInWebUI: true,
    label: "SSOT Drift Check",
    description: "Checks SSOT drift for a work item.",
    group: "Bridge Workflow",
    tab: "SSOT",
    order: 50,
    params: [{ name: "workId", type: "string", required: true }],
    confirm: true,
  },
};

const LANE_PRIORITY = Object.freeze({
  lane_a: 1,
  lane_b: 2,
  bridge: 3,
  project_admin: 4,
});

function assertValidType(type) {
  return type === "string" || type === "int" || type === "bool";
}

function laneFromCommand(cmd) {
  if (BRIDGE_COMMANDS.has(cmd)) return "bridge";
  if (LANE_A_COMMANDS.has(cmd)) return "lane_a";
  if (LANE_B_COMMANDS.has(cmd)) return "lane_b";
  return "project_admin";
}

function defaultLabel(cmd) {
  return String(cmd || "")
    .replace(/^--/, "")
    .split("-")
    .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : part))
    .join(" ");
}

function defaultGroup(lane) {
  if (lane === "lane_a") return "lane_a_misc";
  if (lane === "lane_b") return "lane_b_misc";
  if (lane === "bridge") return "bridge";
  return "project_admin";
}

function defaultDescription(cmd, lane) {
  return `Runs ${cmd} (${lane}).`;
}

function normalizeParams(params) {
  const src = Array.isArray(params) ? params : [];
  return src.map((param) => {
    const p = param && typeof param === "object" ? param : {};
    const name = typeof p.name === "string" ? p.name.trim() : "";
    const type = typeof p.type === "string" ? p.type.trim() : "";
    if (!name || !assertValidType(type)) throw new Error(`Invalid command registry param: ${JSON.stringify(param)}`);
    return { name, type, required: p.required === true };
  });
}

function normalizeDefaultArgs(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  return Object.freeze({ ...raw });
}

function buildRegistry() {
  const laneOrder = new Map();
  const entries = [];
  for (const cmd of ALL_COMMANDS) {
    const lane = laneFromCommand(cmd);
    const ordinal = (laneOrder.get(lane) || 0) + 1;
    laneOrder.set(lane, ordinal);
    const override = UI_OVERRIDES[cmd] || {};
    entries.push({
      cmd,
      lane,
      exposeInWebUI: override.exposeInWebUI === true,
      label: override.label || defaultLabel(cmd),
      description: override.description || defaultDescription(cmd, lane),
      group: override.group || defaultGroup(lane),
      order: Number.isFinite(Number(override.order)) ? Number(override.order) : 1000 + ordinal,
      params: normalizeParams(override.params || []),
      webAction: typeof override.webAction === "string" ? override.webAction : null,
      tab: typeof override.tab === "string" && override.tab.trim() ? override.tab.trim() : null,
      confirm: override.confirm === true,
      injectProjectRoot: override.injectProjectRoot === true,
      defaultArgs: normalizeDefaultArgs(override.defaultArgs),
    });
  }

  entries.sort((a, b) => {
    const laneCmp = (LANE_PRIORITY[a.lane] || 99) - (LANE_PRIORITY[b.lane] || 99);
    if (laneCmp !== 0) return laneCmp;
    const orderCmp = Number(a.order || 0) - Number(b.order || 0);
    if (orderCmp !== 0) return orderCmp;
    return a.cmd.localeCompare(b.cmd);
  });

  return Object.freeze(
    Object.fromEntries(
      entries.map((entry) => [
        entry.cmd,
        Object.freeze({
          cmd: entry.cmd,
          lane: entry.lane,
          exposeInWebUI: entry.exposeInWebUI,
          label: entry.label,
          description: entry.description,
          group: entry.group,
          order: entry.order,
          params: Object.freeze(entry.params.map((p) => Object.freeze({ ...p }))),
          webAction: entry.webAction,
          tab: entry.tab,
          confirm: entry.confirm,
          injectProjectRoot: entry.injectProjectRoot,
          defaultArgs: entry.defaultArgs,
        }),
      ]),
    ),
  );
}

export const commandRegistry = buildRegistry();

export function listCommandRegistry({ webOnly = false } = {}) {
  const rows = Object.values(commandRegistry);
  return webOnly ? rows.filter((row) => row.exposeInWebUI === true) : rows;
}

export function getCommandSpec(cmd) {
  const key = typeof cmd === "string" ? cmd.trim() : "";
  return key && Object.prototype.hasOwnProperty.call(commandRegistry, key) ? commandRegistry[key] : null;
}

function normalizeInt(raw) {
  if (typeof raw === "number" && Number.isInteger(raw)) return raw;
  if (typeof raw === "string" && raw.trim()) {
    if (!/^-?\d+$/.test(raw.trim())) return null;
    const parsed = Number.parseInt(raw.trim(), 10);
    return Number.isInteger(parsed) ? parsed : null;
  }
  return null;
}

function normalizeBool(raw) {
  if (typeof raw === "boolean") return raw;
  if (typeof raw === "string") {
    const s = raw.trim().toLowerCase();
    if (s === "true" || s === "1" || s === "yes") return true;
    if (s === "false" || s === "0" || s === "no") return false;
  }
  return null;
}

export function validateCommandArgs(spec, argsRaw) {
  if (!spec || typeof spec !== "object") return { ok: false, message: "Missing command spec." };
  const args = argsRaw && typeof argsRaw === "object" && !Array.isArray(argsRaw) ? argsRaw : {};
  const allowed = new Set((Array.isArray(spec.params) ? spec.params : []).map((p) => p.name));
  for (const key of Object.keys(args)) {
    if (!allowed.has(key)) return { ok: false, message: `Unexpected arg '${key}' for ${spec.cmd}.` };
  }

  const normalized = {};
  for (const param of Array.isArray(spec.params) ? spec.params : []) {
    const raw = Object.prototype.hasOwnProperty.call(args, param.name) ? args[param.name] : undefined;
    const missing = raw === undefined || raw === null || (typeof raw === "string" && raw.trim() === "");
    if (missing) {
      if (param.required) return { ok: false, message: `Missing required arg '${param.name}' for ${spec.cmd}.` };
      continue;
    }

    if (param.type === "string") {
      if (typeof raw !== "string") return { ok: false, message: `Arg '${param.name}' must be a string.` };
      normalized[param.name] = raw;
      continue;
    }

    if (param.type === "int") {
      const parsed = normalizeInt(raw);
      if (!Number.isInteger(parsed)) return { ok: false, message: `Arg '${param.name}' must be an integer.` };
      normalized[param.name] = parsed;
      continue;
    }

    if (param.type === "bool") {
      const parsed = normalizeBool(raw);
      if (typeof parsed !== "boolean") return { ok: false, message: `Arg '${param.name}' must be boolean.` };
      normalized[param.name] = parsed;
    }
  }

  return { ok: true, normalized };
}
