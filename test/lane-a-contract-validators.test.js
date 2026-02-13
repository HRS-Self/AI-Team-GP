import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { ContractValidationError, validateDecisionPacket, validateEvidenceRef, validateKnowledgeScan, validateRepoIndex } from "../src/contracts/validators/index.js";
import { validateCommitteeStatus } from "../src/contracts/validators/index.js";
import { validateCommitteeOutput } from "../src/contracts/validators/index.js";
import { validateQaCommitteeOutput } from "../src/contracts/validators/index.js";
import { initKnowledgeRepoWithMinimalSsot, writeProjectConfig } from "../src/test-helpers/ssot-fixture.js";
import { loadProjectPaths } from "../src/paths/project-paths.js";
import { runKnowledgeSynthesize } from "../src/lane_a/knowledge/knowledge-synthesize.js";

test("contract validators throw on first error with path", () => {
  assert.throws(
    () => validateEvidenceRef({ evidence_id: "550e8400-e29b-41d4-a716-446655440000" }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.evidence_id"),
  );
});

test("knowledge_scan_output validator rejects missing evidence_ids", () => {
  assert.throws(
    () =>
      validateKnowledgeScan({
        repo_id: "repo-a",
        scanned_at: "20260208_000000000",
        scan_version: 1,
        external_knowledge: [],
        facts: [{ fact_id: "F1", claim: "x", evidence_ids: [] }],
        unknowns: [],
        contradictions: [],
        coverage: { files_seen: 0, files_indexed: 0 },
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.facts[0].evidence_ids"),
  );
});

test("decision_packet validator rejects non-deterministic UUID ids", () => {
  assert.throws(
    () =>
      validateDecisionPacket({
        version: 1,
        decision_id: "550e8400-e29b-41d4-a716-446655440000",
        scope: "system",
        trigger: "state_machine",
        blocking_state: "DECISION_NEEDED",
        context: { summary: "s", why_automation_failed: "w", what_is_known: ["EVID_abc12345"] },
        questions: [
          {
            id: "Q_deadbeef00",
            question: "Which option should be chosen?",
            expected_answer_type: "choice",
            constraints: "Choose one: a|b",
            blocks: ["READY_FOR_WRITER"],
          },
        ],
        assumptions_if_unanswered: "block",
        created_at: "2026-02-08T00:00:00.000Z",
        status: "open",
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.decision_id"),
  );
});

test("decision_packet validator accepts optional INVARIANT_WAIVER type", () => {
  const ok = validateDecisionPacket({
    version: 1,
    type: "INVARIANT_WAIVER",
    decision_id: "DEC_invariant_waiver_a1b2c3d4",
    scope: "repo:repo-a",
    trigger: "state_machine",
    blocking_state: "MERGE_APPROVAL_APPROVED",
    context: { summary: "s", why_automation_failed: "w", what_is_known: ["work_id:W-1"] },
    questions: [
      {
        id: "Q_invariant_waiver_a1b2c3d4",
        question: "Should this waiver be accepted?",
        expected_answer_type: "choice",
        constraints: "Choose one: confirm|reject",
        blocks: ["MERGE_APPROVAL_APPROVED"],
      },
    ],
    assumptions_if_unanswered: "track waiver as unresolved policy debt",
    created_at: "2026-02-12T00:00:00.000Z",
    status: "open",
  });
  assert.equal(ok.type, "INVARIANT_WAIVER");
});

test("decision_packet validator rejects unknown type token", () => {
  assert.throws(
    () =>
      validateDecisionPacket({
        version: 1,
        type: "NOT_ALLOWED",
        decision_id: "DEC_invariant_waiver_a1b2c3d4",
        scope: "repo:repo-a",
        trigger: "state_machine",
        blocking_state: "MERGE_APPROVAL_APPROVED",
        context: { summary: "s", why_automation_failed: "w", what_is_known: ["work_id:W-1"] },
        questions: [
          {
            id: "Q_invariant_waiver_a1b2c3d4",
            question: "Should this waiver be accepted?",
            expected_answer_type: "choice",
            constraints: "Choose one: confirm|reject",
            blocks: ["MERGE_APPROVAL_APPROVED"],
          },
        ],
        assumptions_if_unanswered: "track waiver as unresolved policy debt",
        created_at: "2026-02-12T00:00:00.000Z",
        status: "open",
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.type"),
  );
});

test("repo_index validator accepts minimal valid object", () => {
  const ok = validateRepoIndex({
    version: 1,
    repo_id: "repo-a",
    scanned_at: "2026-02-08T00:00:00.000Z",
    head_sha: "a".repeat(40),
    languages: [],
    entrypoints: ["README.md"],
    build_commands: { package_manager: "npm", install: [], lint: [], build: [], test: [], scripts: {}, evidence_files: [] },
    hotspots: [],
    api_surface: { openapi_files: [], routes_controllers: [], events_topics: [] },
    migrations_schema: [],
    cross_repo_dependencies: [],
    fingerprints: { "README.md": { sha256: "a".repeat(64) } },
    dependencies: { version: 1, detected_at: "2026-02-08T00:00:00.000Z", mode: "detected", depends_on: [] },
  });
  assert.equal(ok.repo_id, "repo-a");
});

test("committee_status validator rejects evidence_missing that is ID-ish (SSOT:...)", () => {
  assert.throws(
    () =>
      validateCommitteeStatus({
        version: 1,
        repo_id: "repo-a",
        evidence_valid: false,
        blocking_issues: [
          { id: "ISSUE_aaaaaaaa", description: "x", severity: "high", evidence_missing: ["SSOT:system/vision.json"] },
        ],
        confidence: "low",
        next_action: "decision_needed",
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.blocking_issues[0].evidence_missing[0]"),
  );
});

test("committee_status validator accepts descriptive evidence_missing strings", () => {
  const ok = validateCommitteeStatus({
    version: 1,
    repo_id: "repo-a",
    evidence_valid: false,
    blocking_issues: [
      { id: "ISSUE_aaaaaaaa", description: "x", severity: "high", evidence_missing: ["need scan coverage of /src/foo/** (module not indexed)"] },
    ],
    confidence: "low",
    next_action: "rescan_needed",
  });
  assert.equal(ok.repo_id, "repo-a");
});

test("committee_status validator rejects missing evidence_missing field", () => {
  assert.throws(
    () =>
      validateCommitteeStatus({
        version: 1,
        repo_id: "repo-a",
        evidence_valid: false,
        blocking_issues: [{ id: "ISSUE_aaaaaaaa", description: "x", severity: "high" }],
        confidence: "low",
        next_action: "decision_needed",
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.blocking_issues[0].evidence_missing"),
  );
});

test("committee_output validator rejects fact without evidence_refs", () => {
  assert.throws(
    () =>
      validateCommitteeOutput({
        scope: "repo:repo-a",
        facts: [{ text: "x", evidence_refs: [] }],
        assumptions: [],
        unknowns: [],
        integration_edges: [],
        risks: [],
        verdict: "evidence_valid",
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.facts[0].evidence_refs"),
  );
});

test("committee_output validator rejects ID-ish evidence_missing strings", () => {
  assert.throws(
    () =>
      validateCommitteeOutput({
        scope: "repo:repo-a",
        facts: [{ text: "x", evidence_refs: ["EVID_aaaaaaaaaaaa"] }],
        assumptions: [{ text: "y", evidence_missing: ["SSOT:system/vision.json"] }],
        unknowns: [],
        integration_edges: [],
        risks: [],
        verdict: "evidence_invalid",
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.assumptions[0].evidence_missing[0]"),
  );
});

test("committee_output validator accepts descriptive evidence_missing strings", () => {
  const ok = validateCommitteeOutput({
    scope: "repo:repo-a",
    facts: [{ text: "x", evidence_refs: ["EVID_aaaaaaaaaaaa"] }],
    assumptions: [{ text: "y", evidence_missing: ["need evidence for endpoint: GET /v1/health (route not found)"] }],
    unknowns: [{ text: "z", evidence_missing: ["need evidence for file: src/auth/** (auth middleware behavior)"] }],
    integration_edges: [
      {
        from: "repo:repo-a",
        to: "repo:repo-b",
        type: "http",
        contract: "GET /v1/health",
        confidence: 0.5,
        evidence_refs: ["EVID_aaaaaaaaaaaa"],
        evidence_missing: [],
      },
    ],
    risks: ["risk"],
    verdict: "evidence_invalid",
  });
  assert.equal(ok.scope, "repo:repo-a");
});

test("qa_committee_output validator accepts minimal valid object", () => {
  const ok = validateQaCommitteeOutput({
    version: 1,
    role: "qa_strategist",
    scope: "repo:repo-a",
    created_at: "2026-02-08T00:00:00.000Z",
    risk: { level: "normal", notes: "" },
    required_invariants: [
      {
        id: "INV_1",
        text: "Responses remain backward compatible.",
        severity: "high",
        evidence_refs: ["EVID_aaaaaaaaaaaa"],
        evidence_missing: [],
      },
    ],
    test_obligations: {
      unit: { required: true, why: "x", suggested_test_directives: ["do x"], target_paths: ["src/"] },
      integration: { required: false, why: "", suggested_test_directives: [], target_paths: [] },
      e2e: { required: false, why: "", suggested_test_directives: [], target_paths: [] },
    },
    facts: [{ text: "Repo index exists.", evidence_refs: ["EVID_aaaaaaaaaaaa"] }],
    unknowns: [{ text: "Exact contracts not evidenced.", evidence_missing: ["need evidence for endpoint: GET /health (contract)"] }],
  });
  assert.equal(ok.role, "qa_strategist");
});

test("qa_committee_output validator rejects invariant with no evidence_refs and no evidence_missing", () => {
  assert.throws(
    () =>
      validateQaCommitteeOutput({
        version: 1,
        role: "qa_strategist",
        scope: "system",
        created_at: "2026-02-08T00:00:00.000Z",
        risk: { level: "unknown", notes: "" },
        required_invariants: [{ id: "INV_1", text: "x", severity: "high", evidence_refs: [], evidence_missing: [] }],
        test_obligations: {
          unit: { required: true, why: "x", suggested_test_directives: ["do x"], target_paths: ["src/"] },
          integration: { required: false, why: "", suggested_test_directives: [], target_paths: [] },
          e2e: { required: false, why: "", suggested_test_directives: [], target_paths: [] },
        },
        facts: [{ text: "f", evidence_refs: ["EVID_aaaaaaaaaaaa"] }],
        unknowns: [{ text: "u", evidence_missing: ["need evidence for file: src/x (x)"] }],
      }),
    (err) => err instanceof ContractValidationError && String(err.message).includes("$.required_invariants[0]"),
  );
});

test("knowledge synthesis is blocked if repo knowledge contract fails validation", async () => {
  const root = mkdtempSync(join(tmpdir(), "ai-team-contract-synth-block-"));

  const projectId = "proj-contract-block";
  const knowledgeRepo = initKnowledgeRepoWithMinimalSsot({ projectRoot: root, projectId, activeTeams: ["Tooling"], sharedPacks: [] });
  writeProjectConfig({ projectRoot: root, projectId, projectMode: "brownfield_takeover", knowledgeRepo, activeTeams: ["Tooling"], sharedPacks: [] });

  const opsRootAbs = join(root, "ops");
  const paths = await loadProjectPaths({ projectRoot: opsRootAbs });
  writeFileSync(
    join(paths.opsConfigAbs, "REPOS.json"),
    JSON.stringify({ version: 1, repos: [{ repo_id: "repo-a", path: "repo-a", status: "active", team_id: "Tooling" }] }, null, 2) + "\n",
    "utf8",
  );

  // Invalid repo scan: missing evidence_ids for fact (contract enforcement must stop synthesis).
  const repoDir = join(paths.knowledge.ssotReposAbs, "repo-a");
  mkdirSync(repoDir, { recursive: true });
  writeFileSync(
    join(repoDir, "scan.json"),
    JSON.stringify(
      {
        repo_id: "repo-a",
        scanned_at: "20260208_000000000",
        scan_version: 1,
        facts: [{ fact_id: "F1", claim: "x", evidence_ids: [] }],
        unknowns: [],
        contradictions: [],
        coverage: { files_seen: 0, files_indexed: 0 },
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );

  const res = await runKnowledgeSynthesize({ projectRoot: opsRootAbs, dryRun: true });
  assert.equal(res.ok, false);
  assert.equal(String(res.message || ""), "Repo scan contract failed validation.");
});
