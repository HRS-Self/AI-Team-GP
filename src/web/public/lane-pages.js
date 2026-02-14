(function lanePagesBootstrap() {
  function $(id) {
    return document.getElementById(id);
  }

  function escapeHtml(raw) {
    return String(raw || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function renderJson(target, value) {
    if (!target) return;
    target.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  }

  function badge(label, ok) {
    const cls = ok ? "badge ok" : "badge bad";
    return `<span class="${cls}">${escapeHtml(label)}: ${ok ? "yes" : "no"}</span>`;
  }

  function sortByGroupOrder(a, b) {
    const g = String(a?.group || "").localeCompare(String(b?.group || ""));
    if (g !== 0) return g;
    const o = Number(a?.order || 0) - Number(b?.order || 0);
    if (o !== 0) return o;
    return String(a?.cmd || "").localeCompare(String(b?.cmd || ""));
  }

  function inferTab(spec, lane) {
    const explicit = typeof spec?.tab === "string" ? spec.tab.trim() : "";
    if (explicit) return explicit;
    const group = String(spec?.group || "");
    if (lane === "lane_a" && group.includes("→")) return group.split("→").pop().trim();
    if (lane === "lane_b") {
      if (group.includes("Intake")) return "Intake";
      if (group.includes("Status")) return "Status";
      return "Triage";
    }
    if (lane === "bridge") {
      if (String(spec?.cmd || "").includes("events")) return "Events";
      if (String(spec?.cmd || "").includes("ssot")) return "SSOT";
      return "Status";
    }
    return "Status";
  }

  function normalizeParamValue(param, raw, hasDefault) {
    if (param.type === "bool") {
      if (typeof raw === "boolean") return raw;
      return hasDefault ? Boolean(raw) : undefined;
    }
    if (param.type === "int") {
      const s = typeof raw === "string" ? raw.trim() : String(raw ?? "").trim();
      if (!s) return undefined;
      const n = Number.parseInt(s, 10);
      return Number.isInteger(n) ? n : undefined;
    }
    if (typeof raw !== "string") return undefined;
    return raw.trim() ? raw : undefined;
  }

  function makeParamControl(param, defaults) {
    const wrap = document.createElement("label");
    wrap.className = "param-row";
    const title = document.createElement("span");
    title.className = "param-name";
    title.textContent = `${param.name}${param.required ? " *" : ""}`;
    wrap.appendChild(title);

    const defaultValue = defaults && Object.prototype.hasOwnProperty.call(defaults, param.name) ? defaults[param.name] : undefined;
    const fieldId = `param-${param.name}`;

    if (param.type === "bool") {
      const input = document.createElement("input");
      input.type = "checkbox";
      input.id = fieldId;
      input.dataset.paramName = param.name;
      input.dataset.paramType = param.type;
      input.dataset.required = param.required ? "1" : "0";
      if (defaultValue === true) input.checked = true;
      wrap.appendChild(input);
      return wrap;
    }

    if (param.type === "int") {
      const input = document.createElement("input");
      input.type = "number";
      input.id = fieldId;
      input.placeholder = param.name;
      input.dataset.paramName = param.name;
      input.dataset.paramType = param.type;
      input.dataset.required = param.required ? "1" : "0";
      if (defaultValue !== undefined && defaultValue !== null) input.value = String(defaultValue);
      wrap.appendChild(input);
      return wrap;
    }

    const longText = /(notes|input|text|session|scope)/i.test(param.name);
    const input = longText ? document.createElement("textarea") : document.createElement("input");
    if (input.tagName === "INPUT") input.type = "text";
    input.id = fieldId;
    input.placeholder = param.name;
    input.dataset.paramName = param.name;
    input.dataset.paramType = param.type;
    input.dataset.required = param.required ? "1" : "0";
    if (defaultValue !== undefined && defaultValue !== null) input.value = String(defaultValue);
    wrap.appendChild(input);
    return wrap;
  }

  async function fetchJson(path, options = {}) {
    const res = await fetch(path, { credentials: "include", ...options });
    const json = await res.json().catch(() => ({}));
    return { res, json };
  }

  async function runCommand(cmd, args) {
    const { res, json } = await fetchJson("/api/run-command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cmd, args }),
    });
    return { ok: res.ok, status: res.status, json };
  }

  function artifactLink(title, item) {
    if (!item || !item.url) return `<span class="muted">${escapeHtml(title)}: none</span>`;
    return `<a target="_blank" rel="noopener noreferrer" href="${escapeHtml(item.url)}">${escapeHtml(title)} (${escapeHtml(item.name || "open")})</a>`;
  }

  function statusBadge(status) {
    const token = String(status || "").trim().toLowerCase();
    if (token === "ok") return `<span class="badge ok">ok</span>`;
    if (token === "blocked") return `<span class="badge bad">blocked</span>`;
    return `<span class="badge">pending</span>`;
  }

  function laneARecommendedCommand(overview) {
    const laneA = overview?.laneA || {};
    const health = laneA.health || {};
    const phases = laneA.phases || {};
    if (health.hard_stale === true || health.stale === true) return "--knowledge-refresh-from-events";
    if (String(phases?.reverse?.status || "") !== "ok") return "--knowledge-kickoff-reverse";
    if (String(phases?.sufficiency?.status || "") !== "ok") return "--knowledge-sufficiency --status";
    if (String(phases?.forward?.status || "") !== "ok") return "--knowledge-kickoff-forward";
    return "--knowledge-status --json";
  }

  function laneBRecommendedCommand(overview) {
    const laneB = overview?.laneB || {};
    if (Number(laneB.inbox_count || 0) > 0) return "--triage";
    if (Number(laneB.triage_count || 0) > 0) return "--sweep";
    if (Array.isArray(laneB.active_work) && laneB.active_work.length > 0) return "--watchdog";
    return "--portfolio";
  }

  function toCommandResultEnvelope(response) {
    const payload = response && response.json && typeof response.json === "object" ? response.json : {};
    const result = payload && payload.result && typeof payload.result === "object" ? payload.result : payload;
    return { payload, result };
  }

  function safeStringify(value) {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  function formatBytes(bytes) {
    const n = Number(bytes);
    if (!Number.isFinite(n) || n < 0) return "n/a";
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  }

  function skillCapBadge(bytes) {
    const n = Number(bytes);
    if (!Number.isFinite(n) || n <= 0) return "";
    if (n >= 80 * 1024) return '<span class="badge bad">over-cap</span>';
    if (n >= 72 * 1024) return '<span class="badge">near-cap</span>';
    return "";
  }

  function collectLatestArtifacts(overview) {
    const repos = Array.isArray(overview?.laneA?.repos) ? overview.laneA.repos.slice().sort((a, b) => String(a?.repo_id || "").localeCompare(String(b?.repo_id || ""))) : [];
    const out = {
      refresh_hint: null,
      decision_packet: null,
      update_meeting: null,
      review_meeting: null,
      committee_report: null,
      writer_report: null,
    };
    for (const repo of repos) {
      const art = repo && repo.latest_artifacts ? repo.latest_artifacts : {};
      for (const key of Object.keys(out)) {
        if (!out[key] && art && art[key] && art[key].url) out[key] = art[key];
      }
    }
    return out;
  }

  function renderLaneAStatus(target, overview) {
    const laneA = overview?.laneA || {};
    const health = laneA.health || {};
    const phases = laneA.phases || {};
    const repos = Array.isArray(laneA.repos) ? laneA.repos : [];
    const recommended = laneARecommendedCommand(overview);

    const rows = [];
    rows.push(`<div class="badge-row">${badge("hard_stale", health.hard_stale === true)} ${badge("stale", health.stale === true)} ${badge("degraded", health.degraded === true)}</div>`);
    rows.push(`<div class="muted">Last scan: ${escapeHtml(health.last_scan || "unknown")} • Last merge event: ${escapeHtml(health.last_merge_event || "unknown")}</div>`);
    rows.push(`<div class="timeline">Reverse <span>→</span> Sufficiency <span>→</span> Forward</div>`);
    rows.push(
      `<div class="phase-grid">
        <div>Reverse: ${statusBadge(phases?.reverse?.status)} <span class="muted">${escapeHtml(phases?.reverse?.message || "")}</span></div>
        <div>Sufficiency: ${statusBadge(phases?.sufficiency?.status)} <span class="muted">${escapeHtml(phases?.sufficiency?.message || "")}</span></div>
        <div>Forward: ${statusBadge(phases?.forward?.status)} <span class="muted">${escapeHtml(phases?.forward?.message || "")}</span></div>
      </div>`,
    );
    rows.push(`<div><strong>Next recommended command:</strong> <code>${escapeHtml(recommended)}</code></div>`);
    rows.push("<div class=\"muted\" style=\"margin-top:8px;\">Repositories</div>");
    if (!repos.length) {
      rows.push("<div class=\"muted\">No repositories found.</div>");
    } else {
      const repoRows = repos
        .slice()
        .sort((a, b) => String(a?.repo_id || "").localeCompare(String(b?.repo_id || "")))
        .map(
          (repo) =>
            `<tr>
              <td>${escapeHtml(repo.repo_id || "")}</td>
              <td>${escapeHtml(repo.coverage || "0%")}</td>
              <td>${badge("stale", repo.stale === true)}</td>
              <td>${badge("hard", repo.hard_stale === true)}</td>
              <td>${badge("degraded", repo.degraded === true)}</td>
            </tr>`,
        );
      rows.push(
        `<table class="status-table">
          <thead><tr><th>repo</th><th>coverage</th><th>stale</th><th>hard_stale</th><th>degraded</th></tr></thead>
          <tbody>${repoRows.join("")}</tbody>
        </table>`,
      );
    }
    target.innerHTML = rows.join("");
  }

  function renderLaneBStatus(target, overview) {
    const laneB = overview?.laneB || {};
    const active = Array.isArray(laneB.active_work) ? laneB.active_work : [];
    const watchdog = laneB.watchdog_status || {};
    const recommended = laneBRecommendedCommand(overview);
    const lines = [];
    lines.push(`<div class="badge-row">${badge("inbox>0", Number(laneB.inbox_count || 0) > 0)} ${badge("triage>0", Number(laneB.triage_count || 0) > 0)} ${badge("active_work>0", active.length > 0)}</div>`);
    lines.push(`<div>Inbox: <strong>${Number(laneB.inbox_count || 0)}</strong> • Triaged: <strong>${Number(laneB.triage_count || 0)}</strong> • Active Work: <strong>${active.length}</strong></div>`);
    lines.push(`<div><strong>Next recommended command:</strong> <code>${escapeHtml(recommended)}</code></div>`);
    lines.push(`<div class="muted">Watchdog: ${escapeHtml(watchdog.last_action || "none")} @ ${escapeHtml(watchdog.last_event_at || "unknown")}</div>`);
    lines.push(
      `<pre class="mini-json">${escapeHtml(
        JSON.stringify(
          {
            active_work: active.slice(0, 20),
            watchdog_status: watchdog,
          },
          null,
          2,
        ),
      )}</pre>`,
    );
    target.innerHTML = lines.join("");
  }

  function renderBridgeStatus(target, overview) {
    const lines = [];
    lines.push("<div class=\"muted\">Unified status overview (Lane A + Lane B):</div>");
    lines.push(`<pre class="mini-json">${escapeHtml(JSON.stringify(overview || {}, null, 2))}</pre>`);
    target.innerHTML = lines.join("");
  }

  function setLoading(target, msg) {
    if (!target) return;
    target.innerHTML = `<div class="muted">${escapeHtml(msg || "Loading…")}</div>`;
  }

  function showError(target, msg) {
    if (!target) return;
    target.innerHTML = `<div class="err">${escapeHtml(msg || "Request failed.")}</div>`;
  }

  async function loadStatus({ lane, projectCode, target }) {
    if (!target) return null;
    setLoading(target, "Loading status…");
    const query = projectCode && projectCode !== "(unknown)" ? `?project=${encodeURIComponent(projectCode)}` : "";
    const { res, json } = await fetchJson(`/api/status-overview${query}`);
    if (!res.ok || !json || json.version !== 1) {
      showError(target, json?.message || "Unable to load status overview.");
      return null;
    }

    if (lane === "lane_a") renderLaneAStatus(target, json);
    else if (lane === "lane_b") renderLaneBStatus(target, json);
    else renderBridgeStatus(target, json);
    return json;
  }

  function renderArtifactLinks(target, lane, overview) {
    if (!target) return;
    const links = [];
    if (lane === "lane_a" && overview) {
      const latest = collectLatestArtifacts(overview);
      links.push(artifactLink("Lane A Health", { url: "/lane-a/health", name: "dashboard" }));
      links.push(artifactLink("Latest refresh hint", latest.refresh_hint));
      links.push(artifactLink("Latest decision packet", latest.decision_packet));
      links.push(artifactLink("Latest update meeting", latest.update_meeting));
      links.push(artifactLink("Latest review meeting", latest.review_meeting));
      links.push(artifactLink("Latest committee report", latest.committee_report));
      links.push(artifactLink("Latest writer report", latest.writer_report));
    } else if (lane === "lane_b") {
      links.push(artifactLink("Lane A Health", { url: "/lane-a/health", name: "dashboard" }));
      links.push(artifactLink("Lane B Ledger", { url: "/api/ledger?lines=120", name: "ledger tail" }));
    } else {
      links.push(artifactLink("Lane A Health", { url: "/lane-a/health", name: "dashboard" }));
      links.push(artifactLink("Lane B Events (run command)", { url: "/bridge", name: "events tab" }));
      links.push(artifactLink("Lane B Ledger", { url: "/api/ledger?lines=120", name: "ledger tail" }));
    }
    target.innerHTML = `<div class="artifact-grid">${links.map((x) => `<div>${x}</div>`).join("")}</div>`;
  }

  function collectArgsFromModal({ modal, spec }) {
    const args = {};
    const defaults = spec && spec.defaultArgs && typeof spec.defaultArgs === "object" ? spec.defaultArgs : {};
    const controls = modal.querySelectorAll("[data-param-name]");
    for (const control of controls) {
      const name = control.dataset.paramName;
      const type = control.dataset.paramType;
      const required = control.dataset.required === "1";
      const hasDefault = Object.prototype.hasOwnProperty.call(defaults, name);
      const raw = type === "bool" ? control.checked : control.value;
      const normalized = normalizeParamValue({ type }, raw, hasDefault);
      if (normalized === undefined) {
        if (hasDefault) {
          args[name] = defaults[name];
          continue;
        }
        if (required) throw new Error(`Missing required param: ${name}`);
        continue;
      }
      args[name] = normalized;
    }
    return args;
  }

  function openParamModal(spec) {
    return new Promise((resolve) => {
      const modal = $("commandModal");
      const title = $("commandModalTitle");
      const body = $("commandModalBody");
      const cancel = $("commandModalCancel");
      const run = $("commandModalRun");

      if (!modal || !title || !body || !cancel || !run) {
        resolve(null);
        return;
      }

      title.textContent = `${spec.label} (${spec.cmd})`;
      body.innerHTML = "";

      const params = Array.isArray(spec.params) ? spec.params : [];
      const defaults = spec.defaultArgs && typeof spec.defaultArgs === "object" ? spec.defaultArgs : {};
      for (const param of params) {
        body.appendChild(makeParamControl(param, defaults));
      }

      modal.classList.remove("hidden");

      const close = (value) => {
        modal.classList.add("hidden");
        cancel.removeEventListener("click", onCancel);
        run.removeEventListener("click", onRun);
        resolve(value);
      };

      const onCancel = () => close(null);
      const onRun = () => {
        try {
          const args = collectArgsFromModal({ modal, spec });
          close(args);
        } catch (err) {
          alert(err instanceof Error ? err.message : String(err));
        }
      };

      cancel.addEventListener("click", onCancel);
      run.addEventListener("click", onRun);
    });
  }

  function splitPrimaryAdvanced(commands) {
    const primary = [];
    const advanced = [];
    for (const cmd of commands) {
      if (cmd.confirm === true) advanced.push(cmd);
      else primary.push(cmd);
    }
    return { primary, advanced };
  }

  async function initialize() {
    const cfg = window.LANE_PAGE_CONFIG || {};
    const lane = String(cfg.lane || "").trim();
    const tabs = Array.isArray(cfg.tabs) ? cfg.tabs : ["Status"];
    if (!lane) return;

    const statusEl = $("statusContent");
    const workflowEl = $("workflowContent");
    const tabsEl = $("laneTabs");
    const artifactsEl = $("artifactLinks");
    const outputEl = $("commandOutput");
    const projectEl = $("projectCode");

    $("logoutBtn")?.addEventListener("click", async () => {
      await fetch("/api/logout", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: "{}",
      }).catch(() => {});
      window.location.href = "/login";
    });

    const projectRes = await fetchJson("/api/project");
    const projectCode = projectRes.json?.project_key || "(unknown)";
    if (projectEl) projectEl.textContent = `Project: ${projectCode}`;

    const commands = (await window.AI_TEAM_UI.fetchCommandRegistry())
      .filter((spec) => spec && spec.exposeInWebUI === true && spec.lane === lane)
      .map((spec) => ({ ...spec, tab: inferTab(spec, lane) }))
      .sort(sortByGroupOrder);

    let laneAHealthProject = null;
    laneAHealthProject = await loadStatus({ lane, projectCode, target: statusEl });
    renderArtifactLinks(artifactsEl, lane, laneAHealthProject);

    const tabFromQuery = new URLSearchParams(window.location.search).get("tab");
    let activeTab = tabFromQuery && tabs.includes(tabFromQuery) ? tabFromQuery : tabs[0];
    const commandByName = new Map(commands.map((spec) => [spec.cmd, spec]));
    const skillsUiState = {
      showRawStatus: false,
      allowBy: "",
      allowNotes: "",
      governanceBy: "",
      governanceNotes: "",
      governanceSession: "",
      draftScope: "system",
      selectedSkillId: "",
      selectedSkillExpanded: false,
      allowDenyResult: null,
      governanceResult: null,
    };
    const skillDetailsCache = new Map();

    async function refreshLaneStatus() {
      laneAHealthProject = await loadStatus({ lane, projectCode, target: statusEl });
      renderArtifactLinks(artifactsEl, lane, laneAHealthProject);
    }

    async function runLaneCommand(cmd, args = {}, { silent = false } = {}) {
      const response = await runCommand(cmd, args);
      const { payload, result } = toCommandResultEnvelope(response);
      if (!silent) renderJson(outputEl, payload || {});
      return { response, payload, result };
    }

    async function loadSkillDetails(skillId, { expanded = false } = {}) {
      const id = String(skillId || "").trim();
      if (!id) return null;
      const cache = skillDetailsCache.get(id);
      if (cache && (!expanded || cache.expanded === true)) return cache;
      if (!commandByName.has("--skills-show")) return null;
      const args = { skill: id, json: true, "max-lines": expanded ? 5000 : 120 };
      const { result } = await runLaneCommand("--skills-show", args, { silent: true });
      if (!result || result.ok === false) return null;
      const next = {
        ...result,
        expanded: expanded || !!cache?.expanded,
      };
      skillDetailsCache.set(id, next);
      return next;
    }

    function readSkillDetails(skillId) {
      return skillDetailsCache.get(String(skillId || "").trim()) || null;
    }

    function parseSkillsStatus(result) {
      if (!result || typeof result !== "object") return { allowed_skills: [], pinned: {} };
      const skills = result.skills && typeof result.skills === "object" ? result.skills : {};
      return {
        allowed_skills: Array.isArray(skills.allowed_skills) ? skills.allowed_skills.map((id) => String(id).trim()).filter(Boolean).sort((a, b) => a.localeCompare(b)) : [],
        pinned: skills.pinned && typeof skills.pinned === "object" ? skills.pinned : {},
        project_code: typeof skills.project_code === "string" ? skills.project_code : "",
        updated_at: typeof skills.updated_at === "string" ? skills.updated_at : "",
      };
    }

    async function loadSkillsTabData({ runGovernance = false } = {}) {
      const jobs = [];
      if (commandByName.has("--project-skills-status")) jobs.push(runLaneCommand("--project-skills-status", { json: true }, { silent: true }).then((res) => ["projectStatus", res]));
      if (commandByName.has("--skills-list")) jobs.push(runLaneCommand("--skills-list", { json: true }, { silent: true }).then((res) => ["registry", res]));
      if (commandByName.has("--skills-governance")) {
        const govArgs = runGovernance ? { run: true, status: true, json: true } : { status: true, json: true };
        jobs.push(runLaneCommand("--skills-governance", govArgs, { silent: true }).then((res) => ["governance", res]));
      }

      const loaded = Object.fromEntries(await Promise.all(jobs));
      const projectStatusResult = loaded.projectStatus?.result || null;
      const registryResult = loaded.registry?.result || null;
      const governanceResult = loaded.governance?.result || null;

      const projectSkills = parseSkillsStatus(projectStatusResult);
      const allowedSet = new Set(projectSkills.allowed_skills);
      const registryRows = Array.isArray(registryResult?.skills) ? registryResult.skills.slice().sort((a, b) => String(a?.skill_id || "").localeCompare(String(b?.skill_id || ""))) : [];
      const candidateRows = registryRows.filter((row) => !allowedSet.has(String(row?.skill_id || "").trim()));
      const allowedRows = projectSkills.allowed_skills.map((skillId) => {
        const detail = readSkillDetails(skillId);
        const pinnedEntry = projectSkills.pinned && typeof projectSkills.pinned === "object" ? projectSkills.pinned[skillId] : null;
        return { skill_id: skillId, detail, pinned: pinnedEntry && typeof pinnedEntry === "object" ? pinnedEntry : null };
      });

      for (const row of allowedRows) {
        if (!row.detail && commandByName.has("--skills-show")) {
          // eslint-disable-next-line no-await-in-loop
          row.detail = await loadSkillDetails(row.skill_id, { expanded: false });
        }
      }

      return {
        loaded,
        projectSkills,
        registryRows,
        candidateRows,
        allowedRows,
        governanceEnvelope: loaded.governance || null,
        governanceStatus: governanceResult?.status && typeof governanceResult.status === "object" ? governanceResult.status : null,
      };
    }

    function renderSkillPreviewPanel() {
      const selected = String(skillsUiState.selectedSkillId || "").trim();
      if (!selected) return '<div class="muted">Select a skill from the registry list.</div>';
      const detail = readSkillDetails(selected);
      if (!detail) return `<div class="muted">No details loaded for ${escapeHtml(selected)}.</div>`;
      const cap = skillCapBadge(detail.bytes);
      const expandBtn = detail.truncated && !detail.expanded ? `<button data-action="skill-expand" data-skill-id="${escapeHtml(selected)}">Expand</button>` : "";
      return `
        <div class="skills-meta">
          <span><strong>${escapeHtml(selected)}</strong></span>
          <span>sha256: <code>${escapeHtml(detail.sha256 || "n/a")}</code></span>
          <span>size: ${escapeHtml(formatBytes(detail.bytes))}</span>
          ${cap}
          <span>updated_at: ${escapeHtml(detail.updated_at || "n/a")}</span>
          ${expandBtn}
        </div>
        <pre class="mini-json skills-preview">${escapeHtml(detail.preview || "")}</pre>
      `;
    }

    function renderSkillsTabCards(data) {
      const rawStatus = {
        project_skills_status: data.loaded.projectStatus?.result || null,
        skills_registry: data.loaded.registry?.result || null,
      };
      const allowedRowsHtml = data.allowedRows.length
        ? data.allowedRows
            .map((row) => {
              const hash = row.detail?.sha256 || row.pinned?.content_sha256 || "n/a";
              const bytes = row.detail?.bytes;
              const size = formatBytes(bytes);
              const updatedAt = row.detail?.updated_at || "n/a";
              const cap = skillCapBadge(bytes);
              return `<tr>
                <td>${escapeHtml(row.skill_id)}</td>
                <td><code>${escapeHtml(hash)}</code></td>
                <td>${escapeHtml(size)}</td>
                <td>${escapeHtml(updatedAt)}</td>
                <td>${cap}</td>
              </tr>`;
            })
            .join("")
        : '<tr><td colspan="5" class="muted">none</td></tr>';

      const candidateRowsHtml = data.candidateRows.length
        ? data.candidateRows
            .map((row) => `<tr><td>${escapeHtml(row.skill_id || "")}</td><td>${escapeHtml(row.title || "")}</td><td>${escapeHtml(row.description || "")}</td></tr>`)
            .join("")
        : '<tr><td colspan="3" class="muted">none</td></tr>';

      const registryRowsHtml = data.registryRows.length
        ? data.registryRows
            .map((row) => {
              const skillId = String(row?.skill_id || "").trim();
              const detail = readSkillDetails(skillId);
              const size = detail ? formatBytes(detail.bytes) : "n/a";
              const cap = detail ? skillCapBadge(detail.bytes) : "";
              return `<tr>
                <td>${escapeHtml(skillId)}</td>
                <td>${escapeHtml(row?.title || "")}</td>
                <td>${escapeHtml(Array.isArray(row?.tags) ? row.tags.join(", ") : "")}</td>
                <td>${escapeHtml(String(row?.status || ""))}</td>
                <td><code>${escapeHtml(detail?.sha256 || "")}</code></td>
                <td>${escapeHtml(size)} ${cap}</td>
                <td><button data-action="skill-show" data-skill-id="${escapeHtml(skillId)}">Show</button></td>
              </tr>`;
            })
            .join("")
        : '<tr><td colspan="7" class="muted">none</td></tr>';

      const allowCandidatesHtml = data.candidateRows.length
        ? data.candidateRows
            .map(
              (row) => `<tr>
                <td>${escapeHtml(row.skill_id || "")}</td>
                <td>${escapeHtml(row.title || "")}</td>
                <td><button class="primary" data-action="skill-allow" data-skill-id="${escapeHtml(row.skill_id || "")}">Allow</button></td>
              </tr>`,
            )
            .join("")
        : '<tr><td colspan="3" class="muted">none</td></tr>';

      const denyRowsHtml = data.allowedRows.length
        ? data.allowedRows
            .map(
              (row) => `<tr>
                <td>${escapeHtml(row.skill_id || "")}</td>
                <td><button class="danger" data-action="skill-deny" data-skill-id="${escapeHtml(row.skill_id || "")}">Deny</button></td>
              </tr>`,
            )
            .join("")
        : '<tr><td colspan="2" class="muted">none</td></tr>';

      const gov = data.governanceStatus || {};
      const env = gov.env || {};
      const draftStats = gov.drafts || {};
      const approvals = gov.approvals || {};
      const skillsStats = gov.skills || {};

      const governanceSummary = data.governanceEnvelope?.result
        ? `<div class="muted">wrote: ${escapeHtml(safeStringify(data.governanceEnvelope.result.wrote || {}))}</div>`
        : '<div class="muted">Governance status unavailable.</div>';

      return `
        <div class="skills-stack">
          <section class="workflow-card skills-card">
            <div class="skills-card-head">
              <h3>A) Skills Status</h3>
              <div class="btns">
                <button data-action="skills-reload-page">Refresh Page</button>
                <button data-action="skills-pretty"${skillsUiState.showRawStatus ? "" : " disabled"}>Pretty</button>
                <button data-action="skills-raw"${skillsUiState.showRawStatus ? " disabled" : ""}>Raw JSON</button>
              </div>
            </div>
            ${
              skillsUiState.showRawStatus
                ? `<pre class="mini-json">${escapeHtml(safeStringify(rawStatus))}</pre>`
                : `
                <div class="hint">project_code: ${escapeHtml(data.projectSkills.project_code || "unknown")} • updated_at: ${escapeHtml(data.projectSkills.updated_at || "unknown")}</div>
                <h4>Allowed Skills</h4>
                <table class="status-table"><thead><tr><th>skill_id</th><th>hash</th><th>size</th><th>last_updated</th><th>warn</th></tr></thead><tbody>${allowedRowsHtml}</tbody></table>
                <h4>Candidate Skills (Not Allowed)</h4>
                <table class="status-table"><thead><tr><th>skill_id</th><th>title</th><th>description</th></tr></thead><tbody>${candidateRowsHtml}</tbody></table>
              `
            }
          </section>
          <section class="workflow-card skills-card">
            <h3>B) Browse Registry</h3>
            <table class="status-table"><thead><tr><th>skill_id</th><th>title</th><th>tags</th><th>status</th><th>hash</th><th>size</th><th>action</th></tr></thead><tbody>${registryRowsHtml}</tbody></table>
            <div id="skillsShowOutput">${renderSkillPreviewPanel()}</div>
          </section>
          <section class="workflow-card skills-card">
            <h3>C) Allowlist Management</h3>
            <div class="skills-form-grid">
              <label>By <input type="text" id="skillsAllowBy" value="${escapeHtml(skillsUiState.allowBy)}" placeholder="operator name" /></label>
              <label>Notes <input type="text" id="skillsAllowNotes" value="${escapeHtml(skillsUiState.allowNotes)}" placeholder="optional notes" /></label>
            </div>
            <h4>Allow Candidates</h4>
            <table class="status-table"><thead><tr><th>skill_id</th><th>title</th><th>action</th></tr></thead><tbody>${allowCandidatesHtml}</tbody></table>
            <h4>Deny Allowed Skills</h4>
            <table class="status-table"><thead><tr><th>skill_id</th><th>action</th></tr></thead><tbody>${denyRowsHtml}</tbody></table>
            <pre class="mini-json">${escapeHtml(safeStringify(skillsUiState.allowDenyResult || {}))}</pre>
          </section>
          <section class="workflow-card skills-card">
            <h3>D) Governance</h3>
            <div class="btns">
              ${
                commandByName.has("--skills-governance")
                  ? '<button class="primary" data-action="skills-governance-run">Run Governance</button>'
                  : '<span class="muted">--skills-governance unavailable</span>'
              }
              ${
                commandByName.has("--skills-refresh")
                  ? '<button data-action="skills-refresh-run">Skills Refresh</button>'
                  : ""
              }
            </div>
            <div class="skills-form-grid">
              <label>Draft Scope <input type="text" id="skillsDraftScope" value="${escapeHtml(skillsUiState.draftScope)}" placeholder="system or repo:&lt;id&gt;" /></label>
              ${
                commandByName.has("--skills-draft")
                  ? '<label>&nbsp;<button data-action="skills-draft-run">Create Draft</button></label>'
                  : '<label class="muted">--skills-draft unavailable</label>'
              }
            </div>
            <div class="hint">latest capture: ${escapeHtml(gov.captured_at || "unknown")} • candidates this run: ${escapeHtml(String(gov.candidates_created_this_run ?? "0"))} • drafts this run: ${escapeHtml(
              String(gov.drafts_created_this_run ?? "0"),
            )}</div>
            <div class="hint">policy: enabled=${escapeHtml(String(env.enabled ?? false))}, cap=${escapeHtml(String(env.draft_daily_cap ?? "0"))}, min_reuse=${escapeHtml(
              String(env.min_reuse_repos ?? "0"),
            )}, min_evidence=${escapeHtml(String(env.min_evidence_refs ?? "0"))}, auto_author=${escapeHtml(String(env.auto_author ?? false))}, require_approval=${escapeHtml(
              String(env.require_approval ?? true),
            )}</div>
            <div class="hint">skills: total=${escapeHtml(String(skillsStats.total ?? 0))}, stale=${escapeHtml(Array.isArray(skillsStats.stale) ? skillsStats.stale.join(", ") || "(none)" : "(none)")}</div>
            <div class="hint">drafts pending=${escapeHtml(Array.isArray(draftStats.pending) ? draftStats.pending.join(", ") || "(none)" : "(none)")}, refresh_pending=${escapeHtml(
              Array.isArray(draftStats.refresh_pending) ? draftStats.refresh_pending.join(", ") || "(none)" : "(none)",
            )}</div>
            <div class="hint">approvals approved=${escapeHtml(Array.isArray(approvals.approved) ? approvals.approved.join(", ") || "(none)" : "(none)")}, rejected=${escapeHtml(
              Array.isArray(approvals.rejected) ? approvals.rejected.join(", ") || "(none)" : "(none)",
            )}</div>
            ${governanceSummary}
            <div class="skills-form-grid">
              <label>Session / Draft ID <input type="text" id="skillsGovSession" value="${escapeHtml(skillsUiState.governanceSession)}" placeholder="DRAFT-..." /></label>
              <label>By <input type="text" id="skillsGovBy" value="${escapeHtml(skillsUiState.governanceBy)}" placeholder="reviewer name" /></label>
              <label>Notes <input type="text" id="skillsGovNotes" value="${escapeHtml(skillsUiState.governanceNotes)}" placeholder="optional notes" /></label>
            </div>
            <div class="btns">
              ${
                commandByName.has("--skills-approve")
                  ? '<button class="primary" data-action="skills-approve">Approve</button>'
                  : ""
              }
              ${
                commandByName.has("--skills-reject")
                  ? '<button class="danger" data-action="skills-reject">Reject</button>'
                  : ""
              }
            </div>
            <pre class="mini-json">${escapeHtml(safeStringify(skillsUiState.governanceResult || {}))}</pre>
          </section>
        </div>
      `;
    }

    async function renderLaneASkillsTab({ runGovernance = false } = {}) {
      if (!workflowEl) return;
      workflowEl.innerHTML = '<div class="muted">Loading skills data…</div>';

      const data = await loadSkillsTabData({ runGovernance });
      workflowEl.innerHTML = renderSkillsTabCards(data);

      const refreshPage = workflowEl.querySelector('[data-action="skills-reload-page"]');
      refreshPage?.addEventListener("click", () => window.location.reload());

      workflowEl.querySelector('[data-action="skills-pretty"]')?.addEventListener("click", async () => {
        skillsUiState.showRawStatus = false;
        await renderLaneASkillsTab();
      });
      workflowEl.querySelector('[data-action="skills-raw"]')?.addEventListener("click", async () => {
        skillsUiState.showRawStatus = true;
        await renderLaneASkillsTab();
      });

      workflowEl.querySelectorAll('[data-action="skill-show"]').forEach((btn) => {
        btn.addEventListener("click", async () => {
          const skillId = btn.getAttribute("data-skill-id") || "";
          skillsUiState.selectedSkillId = skillId;
          skillsUiState.selectedSkillExpanded = false;
          await loadSkillDetails(skillId, { expanded: false });
          await renderLaneASkillsTab();
        });
      });

      workflowEl.querySelector('[data-action="skill-expand"]')?.addEventListener("click", async (ev) => {
        const skillId = ev.currentTarget?.getAttribute("data-skill-id") || skillsUiState.selectedSkillId;
        if (!skillId) return;
        skillsUiState.selectedSkillId = skillId;
        skillsUiState.selectedSkillExpanded = true;
        await loadSkillDetails(skillId, { expanded: true });
        await renderLaneASkillsTab();
      });

      const allowByInput = workflowEl.querySelector("#skillsAllowBy");
      const allowNotesInput = workflowEl.querySelector("#skillsAllowNotes");
      allowByInput?.addEventListener("input", () => {
        skillsUiState.allowBy = allowByInput.value;
      });
      allowNotesInput?.addEventListener("input", () => {
        skillsUiState.allowNotes = allowNotesInput.value;
      });

      const runAllowDeny = async (mode, skillId) => {
        const by = String(allowByInput?.value || skillsUiState.allowBy || "").trim();
        if (!by) {
          alert("Field 'By' is required for allow/deny.");
          return;
        }
        skillsUiState.allowBy = by;
        skillsUiState.allowNotes = String(allowNotesInput?.value || skillsUiState.allowNotes || "");
        const cmd = mode === "allow" ? "--project-skills-allow" : "--project-skills-deny";
        const args = { skill: skillId, by };
        if (skillsUiState.allowNotes.trim()) args.notes = skillsUiState.allowNotes.trim();
        const result = await runLaneCommand(cmd, args);
        skillsUiState.allowDenyResult = result.payload || {};
        renderJson(outputEl, skillsUiState.allowDenyResult);
        await refreshLaneStatus();
        await renderLaneASkillsTab();
      };

      workflowEl.querySelectorAll('[data-action="skill-allow"]').forEach((btn) => {
        btn.addEventListener("click", async () => {
          const skillId = btn.getAttribute("data-skill-id") || "";
          if (!skillId) return;
          const accepted = window.confirm(`Allow skill ${skillId}?`);
          if (!accepted) return;
          await runAllowDeny("allow", skillId);
        });
      });

      workflowEl.querySelectorAll('[data-action="skill-deny"]').forEach((btn) => {
        btn.addEventListener("click", async () => {
          const skillId = btn.getAttribute("data-skill-id") || "";
          if (!skillId) return;
          const accepted = window.confirm(`Deny skill ${skillId}?`);
          if (!accepted) return;
          await runAllowDeny("deny", skillId);
        });
      });

      const draftScopeInput = workflowEl.querySelector("#skillsDraftScope");
      draftScopeInput?.addEventListener("input", () => {
        skillsUiState.draftScope = draftScopeInput.value;
      });

      workflowEl.querySelector('[data-action="skills-governance-run"]')?.addEventListener("click", async () => {
        const accepted = window.confirm("Run skills governance now?");
        if (!accepted) return;
        const res = await runLaneCommand("--skills-governance", { run: true, status: true, json: true });
        skillsUiState.governanceResult = res.payload || {};
        renderJson(outputEl, skillsUiState.governanceResult);
        await refreshLaneStatus();
        await renderLaneASkillsTab();
      });

      workflowEl.querySelector('[data-action="skills-refresh-run"]')?.addEventListener("click", async () => {
        const accepted = window.confirm("Run skills refresh validation?");
        if (!accepted) return;
        const res = await runLaneCommand("--skills-refresh", {});
        skillsUiState.governanceResult = res.payload || {};
        renderJson(outputEl, skillsUiState.governanceResult);
        await refreshLaneStatus();
        await renderLaneASkillsTab();
      });

      workflowEl.querySelector('[data-action="skills-draft-run"]')?.addEventListener("click", async () => {
        const scope = String(draftScopeInput?.value || skillsUiState.draftScope || "").trim() || "system";
        skillsUiState.draftScope = scope;
        const accepted = window.confirm(`Create skills draft for scope ${scope}?`);
        if (!accepted) return;
        const res = await runLaneCommand("--skills-draft", { scope });
        skillsUiState.governanceResult = res.payload || {};
        renderJson(outputEl, skillsUiState.governanceResult);
        await refreshLaneStatus();
        await renderLaneASkillsTab();
      });

      const govSessionInput = workflowEl.querySelector("#skillsGovSession");
      const govByInput = workflowEl.querySelector("#skillsGovBy");
      const govNotesInput = workflowEl.querySelector("#skillsGovNotes");
      govSessionInput?.addEventListener("input", () => {
        skillsUiState.governanceSession = govSessionInput.value;
      });
      govByInput?.addEventListener("input", () => {
        skillsUiState.governanceBy = govByInput.value;
      });
      govNotesInput?.addEventListener("input", () => {
        skillsUiState.governanceNotes = govNotesInput.value;
      });

      const runGovernanceDecision = async (decisionCmd) => {
        const session = String(govSessionInput?.value || skillsUiState.governanceSession || "").trim();
        const by = String(govByInput?.value || skillsUiState.governanceBy || "").trim();
        const notes = String(govNotesInput?.value || skillsUiState.governanceNotes || "").trim();
        if (!session || !by) {
          alert("Session/Draft ID and By are required.");
          return;
        }
        skillsUiState.governanceSession = session;
        skillsUiState.governanceBy = by;
        skillsUiState.governanceNotes = notes;
        const args = { session, by };
        if (notes) args.notes = notes;
        const res = await runLaneCommand(decisionCmd, args);
        skillsUiState.governanceResult = res.payload || {};
        renderJson(outputEl, skillsUiState.governanceResult);
        await refreshLaneStatus();
        await renderLaneASkillsTab();
      };

      workflowEl.querySelector('[data-action="skills-approve"]')?.addEventListener("click", async () => {
        const accepted = window.confirm("Approve this governance draft?");
        if (!accepted) return;
        await runGovernanceDecision("--skills-approve");
      });

      workflowEl.querySelector('[data-action="skills-reject"]')?.addEventListener("click", async () => {
        const accepted = window.confirm("Reject this governance draft?");
        if (!accepted) return;
        await runGovernanceDecision("--skills-reject");
      });
    }

    function renderTabs() {
      if (!tabsEl) return;
      tabsEl.innerHTML = "";
      for (const tab of tabs) {
        const button = document.createElement("button");
        button.className = `tab-btn${tab === activeTab ? " active-tab" : ""}`;
        button.textContent = tab;
        button.addEventListener("click", () => {
          activeTab = tab;
          renderTabs();
          renderWorkflow().catch((err) => {
            renderJson(outputEl, { ok: false, message: err instanceof Error ? err.message : String(err) });
          });
        });
        tabsEl.appendChild(button);
      }
    }

    function renderButtons(list, mode, mountEl) {
      if (!mountEl) return;
      const section = document.createElement("div");
      section.className = "workflow-grid";
      for (const spec of list) {
        const card = document.createElement("div");
        card.className = "workflow-card";
        const btn = document.createElement("button");
        btn.className = "primary workflow-btn";
        btn.textContent = spec.label;
        btn.addEventListener("click", async () => {
          if (spec.confirm === true) {
            const accepted = window.confirm(`Run ${spec.cmd}? This command may be long-running or destructive.`);
            if (!accepted) return;
          }
          let args = {};
          if (Array.isArray(spec.params) && spec.params.length) {
            const input = await openParamModal(spec);
            if (input === null) return;
            args = input;
          }
          renderJson(outputEl, `Running ${spec.cmd}…`);
          const response = await runCommand(spec.cmd, args);
          renderJson(outputEl, response.json || {});
          await refreshLaneStatus();
        });

        const desc = document.createElement("div");
        desc.className = "hint";
        desc.textContent = `${spec.cmd} • ${spec.description}`;
        card.appendChild(btn);
        card.appendChild(desc);
        if (mode === "advanced") card.classList.add("advanced");
        section.appendChild(card);
      }
      mountEl.appendChild(section);
    }

    async function renderWorkflow() {
      if (!workflowEl) return;
      workflowEl.innerHTML = "";
      if (lane === "lane_a" && activeTab === "Skills") {
        await renderLaneASkillsTab();
        return;
      }
      const scoped = commands.filter((cmd) => cmd.tab === activeTab).sort(sortByGroupOrder);
      const { primary, advanced } = splitPrimaryAdvanced(scoped);
      if (!primary.length && !advanced.length) {
        workflowEl.innerHTML = "<div class=\"muted\">No commands mapped to this tab.</div>";
        return;
      }

      if (primary.length) renderButtons(primary, "primary", workflowEl);
      if (advanced.length) {
        const details = document.createElement("details");
        details.className = "advanced-details";
        const summary = document.createElement("summary");
        summary.textContent = "Advanced Commands";
        details.appendChild(summary);
        workflowEl.appendChild(details);
        const holder = document.createElement("div");
        details.appendChild(holder);
        renderButtons(advanced, "advanced", holder);
      }
    }

    renderTabs();
    await renderWorkflow();
  }

  initialize().catch((err) => {
    renderJson(document.getElementById("commandOutput"), { ok: false, message: err instanceof Error ? err.message : String(err) });
  });
})();
