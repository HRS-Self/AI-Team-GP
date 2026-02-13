import test from "node:test";
import assert from "node:assert/strict";

import { parseGitHubOwnerRepo } from "../src/integrations/github-actions.js";

test("parseGitHubOwnerRepo parses common origin URL forms", () => {
  assert.deepEqual(parseGitHubOwnerRepo("https://github.com/Conitdev/DP_Frontend-Portal.git"), { owner: "Conitdev", repo: "DP_Frontend-Portal" });
  assert.deepEqual(parseGitHubOwnerRepo("git@github.com:Conitdev/DP_Frontend-Portal.git"), { owner: "Conitdev", repo: "DP_Frontend-Portal" });
  assert.deepEqual(parseGitHubOwnerRepo("ssh://git@github.com/Conitdev/DP_Frontend-Portal.git"), { owner: "Conitdev", repo: "DP_Frontend-Portal" });
  assert.equal(parseGitHubOwnerRepo("file:///tmp/repo"), null);
});

