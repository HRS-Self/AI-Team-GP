import test from "node:test";
import assert from "node:assert/strict";

import { parseDocsConfig } from "../src/writer/docs-config.js";

test("parseDocsConfig: accepts valid DOCS.json v1", () => {
  const v = parseDocsConfig({
    text: JSON.stringify(
      {
        version: 1,
        project_key: "tms",
        docs_repo_path: "/opt/GitRepos/Projects/tms/docs",
        knowledge_repo_path: "/opt/GitRepos/Projects/tms/knowledge",
        output_format: "markdown",
        parts_word_target: 1800,
        max_docs_per_run: 3,
        commit: { enabled: true, branch: "main" },
      },
      null,
      2,
    ),
  });
  assert.equal(v.ok, true);
  assert.equal(v.normalized.project_key, "tms");
  assert.equal(v.normalized.commit.enabled, true);
  assert.equal(v.normalized.commit.branch, "main");
});

test("parseDocsConfig: rejects non-absolute repo paths", () => {
  const v = parseDocsConfig({
    text: JSON.stringify(
      {
        version: 1,
        project_key: "tms",
        docs_repo_path: "docs",
        knowledge_repo_path: "knowledge",
        output_format: "markdown",
        parts_word_target: 1800,
        max_docs_per_run: 3,
        commit: { enabled: true, branch: "main" },
      },
      null,
      2,
    ),
  });
  assert.equal(v.ok, false);
  assert.match(v.message, /docs_repo_path must be an absolute path/i);
});

