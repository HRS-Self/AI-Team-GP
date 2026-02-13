import test from "node:test";
import assert from "node:assert/strict";

import { findExplicitRepoReferences } from "../src/utils/repo-registry.js";

test("findExplicitRepoReferences: resolves explicit repo by name/path", () => {
  const registry = {
    version: 1,
    base_dir: "/opt/GitRepos",
    repos: [
      { repo_id: "dp-portal", name: "DP_Frontend-Portal", path: "DP_Frontend-Portal", status: "active", team_id: "FrontendDP" },
      { repo_id: "tms-core-hexabackend-api", name: "TMS_Core_HexaBackend-API", path: "TMS_Core_HexaBackend-API", status: "active", team_id: "BackendTMSCore" },
    ],
  };

  const intake = "Update the README of DP_Frontend-Portal repos in develop branch.";
  const matches = findExplicitRepoReferences({ intakeText: intake, registry });
  assert.equal(matches.length, 1);
  assert.equal(matches[0].repo_id, "dp-portal");
  assert.equal(matches[0].team_id, "FrontendDP");
  assert.equal(matches[0].confidence, 1.0);
});

test("findExplicitRepoReferences: resolves normalized (case/space/underscore) repo reference", () => {
  const registry = {
    version: 1,
    base_dir: "/opt/GitRepos",
    repos: [{ repo_id: "dp-portal", name: "DP_Frontend-Portal", path: "DP_Frontend-Portal", status: "active", team_id: "FrontendDP" }],
  };

  const intake = "please update dp frontend portal on develop";
  const matches = findExplicitRepoReferences({ intakeText: intake, registry });
  assert.equal(matches.length, 1);
  assert.equal(matches[0].repo_id, "dp-portal");
  assert.match(matches[0].match_type, /normalized/);
});

test("findExplicitRepoReferences: returns multiple matches when intake mentions multiple repos", () => {
  const registry = {
    version: 1,
    base_dir: "/opt/GitRepos",
    repos: [
      { repo_id: "repo-a", name: "Repo-A", path: "Repo-A", status: "active", team_id: "A" },
      { repo_id: "repo-b", name: "Repo-B", path: "Repo-B", status: "active", team_id: "B" },
    ],
  };

  const intake = "Touch Repo-A and Repo-B";
  const matches = findExplicitRepoReferences({ intakeText: intake, registry });
  assert.equal(matches.length, 2);
  assert.deepEqual(
    matches.map((m) => m.repo_id),
    ["repo-a", "repo-b"],
  );
});

