import test from "node:test";
import assert from "node:assert/strict";

import { createLlmClient } from "../src/llm/client.js";

test("createLlmClient passes reasoning effort from profile (reasoning.effort)", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  try {
    process.env.OPENAI_API_KEY = "test-key";
    delete process.env.AI_TEAM_LLM_STUB;

    const res = createLlmClient({ provider: "openai", model: "gpt-4.1-mini", reasoning: { effort: "high" } });
    assert.equal(res.ok, true);
    assert.equal(res.llm?.reasoning?.effort, "high");
  } finally {
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
    if (prevStub === undefined) delete process.env.AI_TEAM_LLM_STUB;
    else process.env.AI_TEAM_LLM_STUB = prevStub;
  }
});

test("createLlmClient accepts reasoning_effort shorthand", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  try {
    process.env.OPENAI_API_KEY = "test-key";
    delete process.env.AI_TEAM_LLM_STUB;

    const res = createLlmClient({ provider: "openai", model: "gpt-4.1-mini", reasoning_effort: "medium" });
    assert.equal(res.ok, true);
    assert.equal(res.llm?.reasoning?.effort, "medium");
  } finally {
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
    if (prevStub === undefined) delete process.env.AI_TEAM_LLM_STUB;
    else process.env.AI_TEAM_LLM_STUB = prevStub;
  }
});

test("createLlmClient supports profile.options.reasoning string and auto-enables Responses API", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  try {
    process.env.OPENAI_API_KEY = "test-key";
    delete process.env.AI_TEAM_LLM_STUB;

    const res = createLlmClient({ provider: "openai", model: "gpt-5.2-codex", options: { reasoning: "high" } });
    assert.equal(res.ok, true);
    assert.equal(res.llm?.reasoning?.effort, "high");
    assert.equal(res.llm?.useResponsesApi, true);
    assert.equal(res.llm?.temperature, undefined);
  } finally {
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
    if (prevStub === undefined) delete process.env.AI_TEAM_LLM_STUB;
    else process.env.AI_TEAM_LLM_STUB = prevStub;
  }
});

test("createLlmClient treats options.reasoning='standard' as no reasoning config", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  try {
    process.env.OPENAI_API_KEY = "test-key";
    delete process.env.AI_TEAM_LLM_STUB;

    const res = createLlmClient({ provider: "openai", model: "gpt-4.1-mini", options: { reasoning: "standard" } });
    assert.equal(res.ok, true);
    assert.equal(res.llm?.reasoning, undefined);
    assert.equal(res.llm?.useResponsesApi, false);
  } finally {
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
    if (prevStub === undefined) delete process.env.AI_TEAM_LLM_STUB;
    else process.env.AI_TEAM_LLM_STUB = prevStub;
  }
});

test("createLlmClient defaults to Responses API + omits temperature for gpt-5* models", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  const prevStub = process.env.AI_TEAM_LLM_STUB;
  try {
    process.env.OPENAI_API_KEY = "test-key";
    delete process.env.AI_TEAM_LLM_STUB;

    const res = createLlmClient({ provider: "openai", model: "gpt-5.2-codex" });
    assert.equal(res.ok, true);
    assert.equal(res.llm?.useResponsesApi, true);
    assert.equal(res.llm?.temperature, undefined);
  } finally {
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
    if (prevStub === undefined) delete process.env.AI_TEAM_LLM_STUB;
    else process.env.AI_TEAM_LLM_STUB = prevStub;
  }
});
