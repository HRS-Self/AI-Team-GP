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

    let activeTab = tabs[0];

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
          renderWorkflow();
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
          laneAHealthProject = await loadStatus({ lane, projectCode, target: statusEl });
          renderArtifactLinks(artifactsEl, lane, laneAHealthProject);
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

    function renderWorkflow() {
      if (!workflowEl) return;
      workflowEl.innerHTML = "";
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
    renderWorkflow();
  }

  initialize().catch((err) => {
    renderJson(document.getElementById("commandOutput"), { ok: false, message: err instanceof Error ? err.message : String(err) });
  });
})();
