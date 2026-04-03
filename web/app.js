(function () {
  const cfg = window.DASHBOARD_CONFIG || {};
  const endpoint = cfg.API_ENDPOINT || "./api/dashboard_data.php";
  const rangeButtons = document.getElementById("rangeButtons");
  const statusEl = document.getElementById("statusMessage");
  const previewEl = document.getElementById("metadataPreview");

  let activeDays = Number(cfg.DEFAULT_DAYS || 30);

  function fmtInt(value) {
    return new Intl.NumberFormat("en-US").format(Number(value || 0));
  }

  function setStatus(message, isError) {
    statusEl.textContent = message;
    statusEl.style.color = isError ? "#b42318" : "#51606d";
  }

  async function fetchDashboardData(days) {
    const response = await fetch(endpoint + "?days=" + encodeURIComponent(days), {
      method: "GET",
      cache: "no-store",
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(body.error || "Request failed (" + response.status + ")");
    }
    const payload = await response.json();
    if (!payload.ok) {
      throw new Error(payload.error || "API returned an error");
    }
    return payload;
  }

  function renderCounts(payload) {
    const metadata = payload.metadata || {};
    const sessions = payload.session_rows || [];
    const projects = payload.project_rows || [];
    const models = payload.model_rows || [];
    const sources = payload.source_rows || [];
    const toolCalls = payload.tool_call_rows || [];

    document.getElementById("statSessions").textContent = fmtInt(
      metadata.total_sessions || sessions.length
    );
    document.getElementById("statTokens").textContent = fmtInt(metadata.total_tokens || 0);
    document.getElementById("statToolCalls").textContent = fmtInt(
      metadata.total_tool_calls || toolCalls.length
    );
    document.getElementById("statActiveDays").textContent = fmtInt(
      metadata.total_active_days || 0
    );

    document.getElementById("rowCounts").innerHTML = [
      `metadata: ${metadata.metadata_key ? "present" : "missing"}`,
      `source_rows: ${sources.length}`,
      `project_rows: ${projects.length}`,
      `model_rows: ${models.length}`,
      `session_rows: ${sessions.length}`,
      `tool_call_rows: ${toolCalls.length}`,
    ]
      .map((item) => "<li>" + item + "</li>")
      .join("");

    previewEl.textContent = JSON.stringify(metadata, null, 2);
  }

  async function load() {
    setStatus("Loading data for the selected range...", false);
    const payload = await fetchDashboardData(activeDays);
    renderCounts(payload);
    setStatus(
      "Showing " +
        activeDays +
        " day window (" +
        (payload.range_start || "--") +
        " to " +
        (payload.range_end || "--") +
        ").",
      false
    );
  }

  rangeButtons.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-days]");
    if (!button) return;
    activeDays = Number(button.dataset.days || 30);
    Array.from(rangeButtons.querySelectorAll("button")).forEach((node) => {
      node.classList.toggle("active", node === button);
    });
    load().catch((error) => setStatus(error.message || "Unknown error", true));
  });

  load().catch((error) => setStatus(error.message || "Unknown error", true));
})();

