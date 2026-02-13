import { assertEnumString, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject } from "./primitives.js";
import { fail } from "./error.js";

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function assertScope(s, path) {
  const v = assertNonUuidString(s, path, { minLength: 1 });
  if (v !== "system" && !/^repo:[A-Za-z0-9._-]+$/.test(v)) fail(path, "must be 'system' or 'repo:<repo_id>'");
  return v;
}

function assertVersionLike(v, path) {
  const s = assertNonUuidString(v, path, { minLength: 2 });
  if (!/^v\d+(\.\d+)*$/.test(s)) fail(path, "must match v<major>[.<minor>[.<patch>...]]");
  return s;
}

function assertAsked(arr, path) {
  if (!Array.isArray(arr)) fail(path, "must be an array");
  if (arr.length > 500) fail(path, "must have at most 500 items");
  const out = [];
  for (let i = 0; i < arr.length; i += 1) {
    const it = arr[i];
    assertPlainObject(it, `${path}[${i}]`);
    const allowed = new Set(["qid", "question", "asked_at"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`${path}[${i}].${k}`, "unknown field");
    const qid = assertNonUuidString(it.qid, `${path}[${i}].qid`, { minLength: 2 });
    const question = assertNonUuidString(it.question, `${path}[${i}].question`, { minLength: 3 });
    const asked_at = assertIsoDateTimeZ(it.asked_at, `${path}[${i}].asked_at`);
    out.push({ qid: normStr(qid), question: normStr(question), asked_at });
  }
  return out;
}

function assertAnswers(arr, path) {
  if (!Array.isArray(arr)) fail(path, "must be an array");
  if (arr.length > 500) fail(path, "must have at most 500 items");
  const out = [];
  for (let i = 0; i < arr.length; i += 1) {
    const it = arr[i];
    assertPlainObject(it, `${path}[${i}]`);
    const allowed = new Set(["qid", "answer_ref", "answered_at"]);
    for (const k of Object.keys(it)) if (!allowed.has(k)) fail(`${path}[${i}].${k}`, "unknown field");
    const qid = assertNonUuidString(it.qid, `${path}[${i}].qid`, { minLength: 2 });
    const answer_ref = assertNonUuidString(it.answer_ref, `${path}[${i}].answer_ref`, { minLength: 1 });
    const answered_at = assertIsoDateTimeZ(it.answered_at, `${path}[${i}].answered_at`);
    out.push({ qid: normStr(qid), answer_ref: normStr(answer_ref), answered_at });
  }
  return out;
}

function assertStringArray(arr, path, { maxItems = 50 } = {}) {
  if (!Array.isArray(arr)) fail(path, "must be an array");
  if (arr.length > maxItems) fail(path, `must have at most ${maxItems} items`);
  const out = [];
  for (let i = 0; i < arr.length; i += 1) {
    const s = assertNonUuidString(arr[i], `${path}[${i}]`, { minLength: 1 });
    out.push(normStr(s));
  }
  return out;
}

function assertNextQuestion(v, path) {
  if (v === null || typeof v === "undefined") return null;
  assertPlainObject(v, path);
  const allowed = new Set(["qid", "question"]);
  for (const k of Object.keys(v)) if (!allowed.has(k)) fail(`${path}.${k}`, "unknown field");
  const qid = assertNonUuidString(v.qid, `${path}.qid`, { minLength: 2 });
  const question = assertNonUuidString(v.question, `${path}.question`, { minLength: 3 });
  return { qid: normStr(qid), question: normStr(question) };
}

export function validateUpdateMeeting(data) {
  assertPlainObject(data, "$");
  const allowed = new Set([
    "version",
    "meeting_id",
    "scope",
    "from_version",
    "to_version",
    "status",
    "next_question",
    "asked",
    "answers",
    "decision",
    "notes",
    "resulting_actions",
    "created_at",
    "updated_at",
    "closed_at",
    "closed_by",
  ]);
  for (const k of Object.keys(data)) if (!allowed.has(k)) fail(`$.${k}`, "unknown field");

  if (data.version !== 1) fail("$.version", "must be 1");
  const meeting_id = assertNonUuidString(data.meeting_id, "$.meeting_id", { minLength: 3 });
  const scope = assertScope(data.scope, "$.scope");
  const from_version = assertVersionLike(data.from_version, "$.from_version");
  const to_version = assertVersionLike(data.to_version, "$.to_version");
  const status = assertEnumString(data.status, "$.status", ["open", "closed"]);
  const asked = assertAsked(data.asked, "$.asked");
  const answers = assertAnswers(data.answers, "$.answers");
  const next_question = assertNextQuestion(data.next_question, "$.next_question");
  const decision = data.decision === null || typeof data.decision === "undefined" ? null : assertEnumString(data.decision, "$.decision", ["approve", "reject", "defer"]);
  const notes = data.notes === null || typeof data.notes === "undefined" ? null : assertNonUuidString(data.notes, "$.notes", { minLength: 1 });
  const resulting_actions = assertStringArray(data.resulting_actions, "$.resulting_actions", { maxItems: 50 });

  const created_at = data.created_at === null || typeof data.created_at === "undefined" ? null : assertIsoDateTimeZ(data.created_at, "$.created_at");
  const updated_at = data.updated_at === null || typeof data.updated_at === "undefined" ? null : assertIsoDateTimeZ(data.updated_at, "$.updated_at");
  const closed_at = data.closed_at === null || typeof data.closed_at === "undefined" ? null : assertIsoDateTimeZ(data.closed_at, "$.closed_at");
  const closed_by = data.closed_by === null || typeof data.closed_by === "undefined" ? null : assertNonUuidString(data.closed_by, "$.closed_by", { minLength: 1 });

  if (status === "closed") {
    if (decision === null) fail("$.decision", "required when status is closed");
    if (closed_at === null) fail("$.closed_at", "required when status is closed");
    if (closed_by === null) fail("$.closed_by", "required when status is closed");
  } else {
    if (decision !== null) fail("$.decision", "must be null unless status is closed");
    if (closed_at !== null) fail("$.closed_at", "must be null unless status is closed");
    if (closed_by !== null) fail("$.closed_by", "must be null unless status is closed");
  }

  const unanswered = asked.filter((q) => !answers.some((a) => a.qid === q.qid));
  if (unanswered.length > 1) fail("$", "must not have more than one unanswered asked question");
  if (unanswered.length === 1 && (!next_question || next_question.qid !== unanswered[0].qid)) {
    fail("$.next_question", "must reference the single unanswered asked question when present");
  }
  if (unanswered.length === 0 && next_question !== null) fail("$.next_question", "must be null when no question is awaiting an answer");

  return {
    ...data,
    meeting_id: normStr(meeting_id),
    scope,
    from_version,
    to_version,
    status,
    asked,
    answers,
    next_question,
    decision,
    notes: notes === null ? null : normStr(notes),
    resulting_actions,
    created_at,
    updated_at,
    closed_at,
    closed_by: closed_by === null ? null : normStr(closed_by),
  };
}

