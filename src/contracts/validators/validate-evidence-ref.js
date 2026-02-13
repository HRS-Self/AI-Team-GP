import { assertInt, assertIsoDateTimeZ, assertNonUuidString, assertPlainObject, assertRelativeRepoPath, assertSha40 } from "./primitives.js";
import { fail } from "./error.js";

export function validateEvidenceRef(data) {
  assertPlainObject(data, "$");
  const allowedTop = new Set(["evidence_id", "repo_id", "file_path", "commit_sha", "start_line", "end_line", "symbol", "extractor", "captured_at"]);
  for (const k of Object.keys(data)) if (!allowedTop.has(k)) fail(`$.${k}`, "unknown field");

  assertNonUuidString(data.evidence_id, "$.evidence_id", { minLength: 8 });
  assertNonUuidString(data.repo_id, "$.repo_id", { minLength: 1 });
  assertRelativeRepoPath(data.file_path, "$.file_path");
  assertSha40(data.commit_sha, "$.commit_sha");
  assertNonUuidString(data.extractor, "$.extractor", { minLength: 1 });
  assertIsoDateTimeZ(data.captured_at, "$.captured_at");

  const hasLines = data.start_line != null || data.end_line != null;
  const hasSymbol = data.symbol != null;
  if (hasLines && hasSymbol) fail("$", "must provide either start_line/end_line OR symbol (not both)");
  if (hasLines) {
    if (data.start_line == null || data.end_line == null) fail("$", "must provide both start_line and end_line");
    assertInt(data.start_line, "$.start_line", { min: 1 });
    assertInt(data.end_line, "$.end_line", { min: 1 });
    if (data.end_line < data.start_line) fail("$", "end_line must be >= start_line");
  } else if (hasSymbol) {
    assertNonUuidString(data.symbol, "$.symbol", { minLength: 1 });
  }

  return data;
}

