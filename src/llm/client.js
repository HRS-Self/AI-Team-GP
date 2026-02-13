import { ChatOpenAI } from "@langchain/openai";

function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function normLower(x) {
  return normStr(x).toLowerCase();
}

function normNumberOrNull(x) {
  if (x === null || x === undefined || x === "") return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function looksLikeReasoningModelName(modelName) {
  const m = normStr(modelName);
  if (!m) return false;
  if (/^o\\d/.test(m)) return true;
  if (m.startsWith("gpt-5") && !m.startsWith("gpt-5-chat")) return true;
  return false;
}

function normalizeReasoning({ reasoning, reasoning_effort, reasoningEffort, options }) {
  const opts = isPlainObject(options) ? options : null;

  const reasoningObj =
    isPlainObject(reasoning)
      ? reasoning
      : isPlainObject(opts?.reasoning)
        ? opts.reasoning
        : null;

  const effortFromReasoningObj = normLower(reasoningObj?.effort);
  const effortRaw = normLower(
    effortFromReasoningObj ||
      reasoning_effort ||
      reasoningEffort ||
      opts?.reasoning_effort ||
      opts?.reasoningEffort ||
      (typeof opts?.reasoning === "string" ? opts.reasoning : ""),
  );

  // OpenAI supports: none|minimal|low|medium|high|xhigh.
  // We also accept "standard" as an explicit "not a reasoning model" synonym.
  if (effortRaw === "standard") return null;
  const effort = effortRaw && ["none", "minimal", "low", "medium", "high", "xhigh"].includes(effortRaw) ? effortRaw : null;
  if (!effort) return null;

  // Only include the params we support/need today.
  return { effort };
}

export function createLlmClient({
  provider,
  model,
  timeoutMs = null,
  temperature = null,
  maxRetries = null,
  verbosity = null,
  reasoning = null,
  reasoning_effort = null,
  reasoningEffort = null,
  useResponsesApi = null,
  options = null,
} = {}) {
  const p = String(provider || "").trim() || null;
  const m = String(model || "").trim() || null;
  if (!p) return { ok: false, message: "Missing provider selection for LLM client. Configure config/LLM_PROFILES.json and set agent.llm_profile." };
  if (!m) return { ok: false, message: "Missing model selection for LLM client. Configure config/LLM_PROFILES.json and set agent.llm_profile." };

  // Deterministic test stub: avoid network and force a timeout-like error.
  // Used only when explicitly enabled via env var.
  if (process.env.AI_TEAM_LLM_STUB === "timeout") {
    return {
      ok: true,
      model: m,
      llm: {
        invoke: async () => {
          throw new Error("Request timed out.");
        },
      },
    };
  }

  // Deterministic test stub: triage succeeds.
  if (process.env.AI_TEAM_LLM_STUB === "triage_ok") {
    return {
      ok: true,
      model: m,
      llm: {
        invoke: async () => {
          const triage = {
            version: 1,
            source_intake_id: "PLACEHOLDER",
            triage_id: "PLACEHOLDER",
            createdAt: new Date().toISOString(),
            tasks: [
              {
                task_id: "PLACEHOLDER",
                title: "Update README",
                description: "Update README.md as requested in the intake.",
                suggested_repo_ids: [],
                suggested_team_ids: [],
                target_branch: null,
                acceptance_criteria: ["README.md updated with the requested change."],
                risk_level: "low",
                dependencies: { depends_on_workIds: [], depends_on_task_ids: [] },
              },
            ],
            dedupe: { possible_duplicates: [] },
            confidence_overall: 0.8,
            questions_for_human: [],
          };
          return { content: JSON.stringify(triage) };
        },
      },
    };
  }

  // Deterministic test stub: knowledge scavenger succeeds with minimal valid JSON.
  if (process.env.AI_TEAM_LLM_STUB === "knowledge_scavenger_ok") {
    return {
      ok: true,
      model: m,
      llm: {
        invoke: async (messages) => {
          const user = Array.isArray(messages) ? messages.find((mm) => mm && mm.role === "user") : null;
          const userText = typeof user?.content === "string" ? user.content : String(user?.content ?? "");
          let payload = null;
          try {
            payload = JSON.parse(userText);
          } catch {
            payload = null;
          }
          const repoId = payload && typeof payload.repo_id === "string" && payload.repo_id.trim() ? payload.repo_id.trim() : "unknown-repo";
          const out = {
            version: 1,
            repo_id: repoId,
            scope: `repo:${repoId}`,
            captured_at: new Date().toISOString(),
            source_commit: typeof payload?.source_commit === "string" ? payload.source_commit : "unknown",
            extractor_version: "1",
            implemented_capabilities: ["stub_scavenger_ok"],
            exposed_interfaces: [],
            expected_interfaces: [],
            dependencies: [],
            config: [],
            known_constraints: [],
            known_unknowns: [],
            gaps: [],
          };
          return { content: JSON.stringify(out) };
        },
      },
    };
  }

  // Deterministic test stub: committee runner (repo committees + integration chair).
  // Modes:
  // - committee_all_pass: roles emit evidence_valid outputs with 1 fact backed by allowed evidence.
  // - committee_repo_fail: repo_skeptic emits evidence_invalid (missing evidence).
  // - committee_architect_no_evidence: repo_architect emits an invalid fact (empty evidence_refs).
  // - committee_architect_unknown_evidence: repo_architect references an evidence_ref not in allowed_evidence_ids.
  if (String(process.env.AI_TEAM_LLM_STUB || "").startsWith("committee_")) {
    const modeRaw = String(process.env.AI_TEAM_LLM_STUB || "");
    const [mode, modeArg] = modeRaw.split(":", 2);
    return {
      ok: true,
      model: m,
      llm: {
        invoke: async (messages) => {
          const sys = Array.isArray(messages) ? messages.find((mm) => mm && mm.role === "system") : null;
          const user = Array.isArray(messages) ? messages.find((mm) => mm && mm.role === "user") : null;
          const sysText = typeof sys?.content === "string" ? sys.content : String(sys?.content ?? "");
          const userText = typeof user?.content === "string" ? user.content : String(user?.content ?? "");
          let payload = null;
          try {
            payload = JSON.parse(userText);
          } catch {
            payload = null;
          }
          const repoId = payload && typeof payload.repo_id === "string" ? payload.repo_id.trim() : "";
          const allowed = Array.isArray(payload?.allowed_evidence_ids) ? payload.allowed_evidence_ids.map((x) => String(x || "").trim()).filter(Boolean) : [];
          const e0 = allowed.length ? allowed[0] : "EVID_aaaaaaaaaaaa";

          if (sysText.includes("qa_strategist")) {
            const scope = payload && typeof payload.scope === "string" && payload.scope.trim() ? payload.scope.trim() : (repoId ? `repo:${repoId}` : "system");
            return {
              content: JSON.stringify({
                version: 1,
                role: "qa_strategist",
                scope,
                created_at: new Date().toISOString(),
                risk: { level: "normal", notes: "Stub risk notes." },
                required_invariants: [
                  { id: "INV_stub_001", text: "API responses must remain backward compatible for existing clients.", severity: "high", evidence_refs: [e0], evidence_missing: [] },
                ],
                test_obligations: {
                  unit: { required: true, why: "Core logic is changing; unit tests must lock invariants.", suggested_test_directives: ["Add/extend unit tests for changed modules."], target_paths: ["test/", "src/"] },
                  integration: { required: true, why: "Public interfaces can regress; integration tests must cover API surface.", suggested_test_directives: ["Add/extend API contract tests."], target_paths: ["openapi.yaml", "src/"] },
                  e2e: { required: false, why: "No end-to-end flows identified in provided artifacts.", suggested_test_directives: [], target_paths: [] },
                },
                facts: [{ text: "Repository evidence index exists and was provided to the strategist.", evidence_refs: [e0] }],
                unknowns: [{ text: "Exact runtime environment and downstream client expectations are not fully evidenced.", evidence_missing: ["need evidence for endpoint: (list critical client-facing endpoints and SLAs)"] }],
              }),
            };
          }

          if (sysText.includes("repo_architect")) {
            if (mode === "committee_architect_no_evidence") {
              return {
                content: JSON.stringify({
                  scope: `repo:${repoId || "repo-a"}`,
                  facts: [{ text: "Stub fact with missing evidence", evidence_refs: [] }],
                  assumptions: [],
                  unknowns: [],
                  integration_edges: [],
                  risks: [],
                  verdict: "evidence_valid",
                }),
              };
            }
            if (mode === "committee_architect_unknown_evidence") {
              return {
                content: JSON.stringify({
                  scope: `repo:${repoId || "repo-a"}`,
                  facts: [{ text: "Stub fact with unknown evidence", evidence_refs: ["EVID_not_allowed"] }],
                  assumptions: [],
                  unknowns: [],
                  integration_edges: [],
                  risks: [],
                  verdict: "evidence_valid",
                }),
              };
            }
            return {
              content: JSON.stringify({
                scope: `repo:${repoId || "repo-a"}`,
                facts: [{ text: "Stub architect fact", evidence_refs: [e0] }],
                assumptions: [],
                unknowns: [],
                integration_edges: [],
                risks: [],
                verdict: "evidence_valid",
              }),
            };
          }

          if (sysText.includes("repo_skeptic")) {
            if (mode === "committee_repo_fail" || mode === "committee_repo_fail_repo") {
              if (modeArg && repoId && modeArg !== repoId) {
                return {
                  content: JSON.stringify({
                    scope: `repo:${repoId}`,
                    facts: [],
                    assumptions: [],
                    unknowns: [],
                    integration_edges: [],
                    risks: [],
                    verdict: "evidence_valid",
                  }),
                };
              }
              return {
                content: JSON.stringify({
                  scope: `repo:${repoId || "repo-b"}`,
                  facts: [],
                  assumptions: [],
                  unknowns: [{ text: "Cannot validate architect fact; missing proof.", evidence_missing: ["need evidence for file: (show route/controller implementing claimed behavior)"] }],
                  integration_edges: [],
                  risks: ["Unproven behavior may break integration."],
                  verdict: "evidence_invalid",
                }),
              };
            }
            return {
              content: JSON.stringify({
                scope: `repo:${repoId || "repo-a"}`,
                facts: [{ text: "Skeptic confirms at least one architect fact is evidence-backed.", evidence_refs: [e0] }],
                assumptions: [],
                unknowns: [],
                integration_edges: [],
                risks: [],
                verdict: "evidence_valid",
              }),
            };
          }

          if (sysText.includes("integration_chair")) {
            return {
              content: JSON.stringify({
                scope: "system",
                facts: allowed.length ? [{ text: "System has evidence-backed repo committee outputs.", evidence_refs: [e0] }] : [],
                assumptions: [],
                unknowns: [],
                integration_edges: [],
                risks: [],
                verdict: "evidence_valid",
              }),
            };
          }

          return { content: JSON.stringify({}) };
        },
      },
    };
  }

  // Deterministic test stub: proposal succeeds, patch plan fails schema (absolute cwd).
  // Deterministic test stub: proposal succeeds, patch plan fails schema (absolute cwd).
  if (process.env.AI_TEAM_LLM_STUB === "ok_proposal_invalid_patchplan_cwd") {
    return {
      ok: true,
      model: m,
      llm: {
        invoke: async (messages) => {
          const sys = Array.isArray(messages) ? messages.find((m) => m && m.role === "system") : null;
          const sysText = typeof sys?.content === "string" ? sys.content : String(sys?.content ?? "");
          const sysLower = sysText.toLowerCase();

          if (sysText.includes("team planner agent")) {
            const proposal = {
              ssot_references: [{ doc: "ssot/sections/vision.json", section: "vision", rule_id: "SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }],
              summary: "Update README with the requested change. (SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)",
              understanding_of_requirements: ["Modify the requested file(s) within scope. (SSOT:scope@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)"],
              proposed_approach: ["Edit the file(s) directly and keep the change minimal."],
              likely_files_or_areas_impacted: ["README.md"],
              risks_and_mitigations: ["Low risk; documentation-only change."],
              questions_or_clarifications_needed: [],
              suggested_validation: ["git diff -- README.md"],
            };
            return { content: JSON.stringify(proposal) };
          }

          if (sysText.includes("repo-scoped patch planning agent")) {
            const plan = {
              intent_summary: "Update README.md by adding a line.",
              scope: { allowed_paths: ["README.md"], forbidden_paths: [], allowed_ops: ["edit"] },
              edits: [
                {
                  path: "README.md",
                  op: "edit",
                  rationale: "Insert a line as requested. (SSOT:scope@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)",
                  intent: { type: "append_line", line: "added" },
                },
              ],
              commands: { cwd: "/tmp", package_manager: null, install: null, lint: null, test: null, build: null },
              risk: { level: "normal", notes: "" },
              constraints: { no_branch_create: true, requires_training: false, hexa_authoring_mode: null, blockly_compat_required: null },
            };
            return { content: JSON.stringify(plan) };
          }

          if (
            sysLower.includes("qa-inspector") ||
            sysLower.includes("qa inspector") ||
            sysLower.includes("qa_inspector") ||
            sysLower.includes("qa-strategist") ||
            sysLower.includes("qa strategist") ||
            sysLower.includes("qa_strategist")
          ) {
            const qa = {
              version: 1,
              work_id: "PLACEHOLDER",
              repo_id: "PLACEHOLDER",
              team_id: "PLACEHOLDER",
              target_branch: "develop",
              created_at: new Date().toISOString(),
              ssot_references: [{ doc: "ssot/sections/vision.json", section: "vision", rule_id: "SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }],
              ssot: { bundle_path: "ai/lane_b/work/PLACEHOLDER/ssot/SSOT_BUNDLE.team-PLACEHOLDER.json", bundle_hash: "PLACEHOLDER", snapshot_sha256: "PLACEHOLDER" },
              derived_from: {
                proposal_path: "ai/lane_b/work/PLACEHOLDER/proposals/PLACEHOLDER.json",
                proposal_sha256: "PLACEHOLDER",
                patch_plan_path: "ai/lane_b/work/PLACEHOLDER/patch-plans/PLACEHOLDER.json",
                patch_plan_sha256: "PLACEHOLDER",
                timestamp: new Date().toISOString(),
              },
              notes: "",
              tests: [
                {
                  test_id: "QA-001",
                  title: "README change is present",
                  type: "manual",
                  priority: "P3",
                  acceptance_criteria: ["README.md contains the requested update."],
                  steps: ["Open README.md", "Verify the inserted line exists."],
                  expected: ["The line exists exactly once."],
                  files_or_areas: ["README.md"],
                  ssot_refs: ["SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                },
              ],
              gaps: [],
            };
            return { content: JSON.stringify(qa) };
          }

          return { content: "{}" };
        },
      },
    };
  }

  // Deterministic test stub: proposal succeeds, patch plan fails schema (invalid edit path).
  if (process.env.AI_TEAM_LLM_STUB === "ok_proposal_invalid_patchplan_path") {
    return {
      ok: true,
      model: m,
      llm: {
        invoke: async (messages) => {
          const sys = Array.isArray(messages) ? messages.find((m) => m && m.role === "system") : null;
          const sysText = typeof sys?.content === "string" ? sys.content : String(sys?.content ?? "");
          const sysLower = sysText.toLowerCase();

          if (sysText.includes("team planner agent")) {
            const proposal = {
              ssot_references: [{ doc: "ssot/sections/vision.json", section: "vision", rule_id: "SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }],
              summary: "Update README with the requested change. (SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)",
              understanding_of_requirements: ["Modify the requested file(s) within scope. (SSOT:scope@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)"],
              proposed_approach: ["Edit the file(s) directly and keep the change minimal."],
              likely_files_or_areas_impacted: ["README.md"],
              risks_and_mitigations: ["Low risk; documentation-only change."],
              questions_or_clarifications_needed: [],
              suggested_validation: ["git diff -- README.md"],
            };
            return { content: JSON.stringify(proposal) };
          }

          if (sysText.includes("repo-scoped patch planning agent")) {
            const plan = {
              intent_summary: "Update README.md by adding a line.",
              scope: { allowed_paths: ["README.md"], forbidden_paths: [], allowed_ops: ["edit"] },
              edits: [
                {
                  path: "../README.md",
                  op: "edit",
                  rationale: "Invalid path traversal (test). (SSOT:scope@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)",
                  intent: { type: "append_line", line: "added" },
                },
              ],
              commands: { cwd: ".", package_manager: null, install: null, lint: null, test: null, build: null },
              risk: { level: "normal", notes: "" },
              constraints: { no_branch_create: true, requires_training: false, hexa_authoring_mode: null, blockly_compat_required: null },
            };
            return { content: JSON.stringify(plan) };
          }

          if (
            sysLower.includes("qa-inspector") ||
            sysLower.includes("qa inspector") ||
            sysLower.includes("qa_inspector") ||
            sysLower.includes("qa-strategist") ||
            sysLower.includes("qa strategist") ||
            sysLower.includes("qa_strategist")
          ) {
            const qa = {
              version: 1,
              work_id: "PLACEHOLDER",
              repo_id: "PLACEHOLDER",
              team_id: "PLACEHOLDER",
              target_branch: "develop",
              created_at: new Date().toISOString(),
              ssot_references: [{ doc: "ssot/sections/vision.json", section: "vision", rule_id: "SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }],
              ssot: { bundle_path: "ai/lane_b/work/PLACEHOLDER/ssot/SSOT_BUNDLE.team-PLACEHOLDER.json", bundle_hash: "PLACEHOLDER", snapshot_sha256: "PLACEHOLDER" },
              derived_from: {
                proposal_path: "ai/lane_b/work/PLACEHOLDER/proposals/PLACEHOLDER.json",
                proposal_sha256: "PLACEHOLDER",
                patch_plan_path: "ai/lane_b/work/PLACEHOLDER/patch-plans/PLACEHOLDER.json",
                patch_plan_sha256: "PLACEHOLDER",
                timestamp: new Date().toISOString(),
              },
              notes: "",
              tests: [
                {
                  test_id: "QA-001",
                  title: "README change is present",
                  type: "manual",
                  priority: "P3",
                  acceptance_criteria: ["README.md contains the requested update."],
                  steps: ["Open README.md", "Verify the inserted line exists."],
                  expected: ["The line exists exactly once."],
                  files_or_areas: ["README.md"],
                  ssot_refs: ["SSOT:vision@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                },
              ],
              gaps: [],
            };
            return { content: JSON.stringify(qa) };
          }

          return { content: "{}" };
        },
      },
    };
  }

  if (p !== "openai") return { ok: false, message: `Unsupported LLM provider '${p}'.` };

  const apiKey = process.env.OPENAI_API_KEY;
  const timeout = Number.isFinite(Number(timeoutMs)) && Number(timeoutMs) > 0 ? Number(timeoutMs) : 20_000;

  if (!apiKey || !String(apiKey).trim()) {
    return { ok: false, message: "Missing OPENAI_API_KEY. Set it in your environment or .env." };
  }

  const opts = isPlainObject(options) ? options : null;
  const reasoningNorm = normalizeReasoning({ reasoning, reasoning_effort, reasoningEffort, options: opts });

  // gpt-5* reasoning effort != none does not support temperature; omit it entirely in that case.
  // Default: temperature=0 for determinism when allowed.
  const tempInput = temperature ?? opts?.temperature ?? null;
  const tempRaw = normNumberOrNull(tempInput);
  const shouldOmitTemperature = reasoningNorm && reasoningNorm.effort && reasoningNorm.effort !== "none";
  const temp = shouldOmitTemperature ? undefined : tempRaw ?? 0;

  const retriesInput = maxRetries ?? opts?.maxRetries ?? opts?.max_retries ?? null;
  const retries = normNumberOrNull(retriesInput);

  const verbosityNorm = (() => {
    const v = normLower(verbosity ?? opts?.verbosity);
    return v && ["low", "medium", "high"].includes(v) ? v : null;
  })();
  const useResponsesApiInput = useResponsesApi ?? opts?.useResponsesApi ?? opts?.use_responses_api ?? null;
  const useResponsesApiNorm = typeof useResponsesApiInput === "boolean" ? useResponsesApiInput : null;

  // Per OpenAI guidance: reasoning models perform best with Responses API.
  const modelLooksReasoning = looksLikeReasoningModelName(m);
  const effectiveUseResponsesApi = useResponsesApiNorm !== null ? useResponsesApiNorm : reasoningNorm || modelLooksReasoning ? true : null;

  const llmFields = {
    apiKey,
    model: m,
    maxRetries: retries === null ? 0 : Math.max(0, Math.floor(retries)),
    timeout: Math.floor(timeout),
    ...(verbosityNorm ? { verbosity: verbosityNorm } : {}),
    ...(reasoningNorm ? { reasoning: reasoningNorm } : {}),
    ...(effectiveUseResponsesApi !== null ? { useResponsesApi: effectiveUseResponsesApi } : {}),
  };
  // Many reasoning models reject temperature unless reasoning effort is explicitly "none".
  // Default: omit temperature for reasoning models unless the profile explicitly sets effort:"none".
  const allowTemperature = !modelLooksReasoning || reasoningNorm?.effort === "none";
  if (allowTemperature && temp !== undefined) llmFields.temperature = temp;

  const llm = new ChatOpenAI(llmFields);

  return { ok: true, llm, model: m, provider: "openai" };
}
