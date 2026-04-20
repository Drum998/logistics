const DASHBOARD_CACHE_KEY = "logistics_ram_dashboard_v1";

function saveDashboardCache() {
  try {
    const dateEl = document.getElementById("shiftDate");
    const date = dateEl && dateEl.value;
    if (!date || !window.__lastResultsByVrn || typeof window.__lastResultsByVrn !== "object") return;
    const payload = {
      date,
      results: window.__lastResultsByVrn,
      errors: window.__lastErrorsByVrn || {},
    };
    sessionStorage.setItem(DASHBOARD_CACHE_KEY, JSON.stringify(payload));
  } catch (_e) {
    // QuotaExceededError or private browsing; ignore
  }
}

function restoreDashboardCache() {
  try {
    const raw = sessionStorage.getItem(DASHBOARD_CACHE_KEY);
    if (!raw) return false;
    const payload = JSON.parse(raw);
    if (!payload || typeof payload.date !== "string" || !payload.results || typeof payload.results !== "object") {
      return false;
    }
    const dateEl = document.getElementById("shiftDate");
    if (dateEl) dateEl.value = payload.date;
    window.__lastResultsByVrn = payload.results;
    window.__lastErrorsByVrn = { ...(payload.errors || {}) };
    if (Object.keys(window.__lastErrorsByVrn).length) {
      setError(formatErrorBanner(window.__lastErrorsByVrn));
    } else {
      setError("");
    }
    renderSummary(window.__lastResultsByVrn, window.__lastErrorsByVrn);
    const statusEl = document.getElementById("status");
    if (statusEl && statusEl.textContent) {
      statusEl.textContent = `${statusEl.textContent} Restored from this browser session (no API call). Run all vans to refresh.`;
    }
    return true;
  } catch (_e) {
    return false;
  }
}

function fmtSeconds(secs) {
  if (secs == null || Number.isNaN(secs)) return "-";
  const s = Math.max(0, Math.floor(secs));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  if (h > 0) return `${h}h ${m}m ${r}s`;
  if (m > 0) return `${m}m ${r}s`;
  return `${r}s`;
}

function secsToMinutes(secs) {
  if (secs == null || Number.isNaN(secs)) return 0;
  return (secs || 0) / 60.0;
}

function fmtKm(km) {
  if (km == null || Number.isNaN(km)) return "-";
  return km.toFixed(1);
}

function severityBandForMinutes(mins, lowMax, medMax) {
  if (mins <= lowMax) return { band: "low", label: "Low" };
  if (mins <= medMax) return { band: "med", label: "Med" };
  return { band: "high", label: "High" };
}

function setActiveTab(tabKey) {
  for (const btn of document.querySelectorAll(".tabBtn")) {
    btn.classList.toggle("active", btn.dataset.tab === tabKey);
  }
  for (const panel of document.querySelectorAll(".tabPanel")) {
    panel.classList.toggle("active", panel.id === `panel-${tabKey}`);
  }
}

async function loadVans() {
  const res = await fetch("/api/vans");
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Failed to parse /api/vans response as JSON. First 200 chars: ${text.slice(0, 200)}`);
  }
  const count = (data.vans || []).length;
  setStatus(`${count} vans configured. Ready.`);
}

function setError(msg) {
  const el = document.getElementById("error");
  if (!msg) {
    el.style.display = "none";
    el.textContent = "";
    return;
  }
  el.style.display = "block";
  el.textContent = msg;
}

function setStatus(msg) {
  document.getElementById("status").textContent = msg || "";
}

function setNarrative(msg) {
  const el = document.getElementById("narrative");
  if (el) el.textContent = msg || "";
}

function formatErrorBanner(errs) {
  if (!errs || !Object.keys(errs).length) return "";
  const parts = Object.entries(errs).map(([k, v]) => `${k}: ${v}`);
  return `Some vans failed: ${parts.join(" | ")}`;
}

function updateRetryUi() {
  const row = document.getElementById("retryRow");
  const hint = document.getElementById("retryHint");
  const errs = window.__lastErrorsByVrn || {};
  const keys = Object.keys(errs);
  if (!row) return;
  if (!keys.length) {
    row.style.display = "none";
    if (hint) hint.textContent = "";
    return;
  }
  row.style.display = "flex";
  if (hint) hint.textContent = `(${keys.length} van(s) to retry)`;
}

function getSelectedStatusFilter() {
  const active = document.querySelector("#statusChips .chip.active");
  return active ? active.dataset.status : "ALL";
}

function getNumericValue(id) {
  const el = document.getElementById(id);
  if (!el) return 0;
  const n = Number(el.value);
  return Number.isFinite(n) ? n : 0;
}

function applyVanFilters(vanRows) {
  const status = getSelectedStatusFilter();
  const minOverspeedMin = getNumericValue("minOverspeedMin");
  const minIdlingMin = getNumericValue("minIdlingMin");
  const minJourneys = getNumericValue("minJourneys");

  return (vanRows || []).filter((r) => {
    if (status !== "ALL" && r.status !== status) return false;
    if (r.status !== "OK") return true;
    if (secsToMinutes(r.overspeedSeconds) < minOverspeedMin) return false;
    if (secsToMinutes(r.idlingSeconds) < minIdlingMin) return false;
    if ((r.journeyCount || 0) < minJourneys) return false;
    return true;
  });
}

function renderSummary(resultsByVrn, errorsByVrn) {
  let totalOverspeedSeconds = 0;
  let totalOverspeedSegments = 0;
  let totalIdlingSeconds = 0;
  let totalJourneys = 0;
  let totalJourneySeconds = 0;
  let totalJourneyKm = 0;

  const journeys = [];
  let okCount = 0;
  let errorCount = 0;
  const vanRows = [];
  const seenVrn = new Set();

  for (const [vrn, r] of Object.entries(resultsByVrn || {})) {
    if (!r || r.status === "NO_DATA") {
      continue;
    }
    okCount += 1;
    seenVrn.add(vrn);
    totalOverspeedSeconds += r.overspeed?.totalSeconds || 0;
    totalOverspeedSegments += r.overspeed?.segmentCount || 0;
    totalIdlingSeconds += r.idling?.totalSeconds || 0;
    totalJourneys += r.journeys?.count || 0;
    totalJourneySeconds += r.journeys?.totalSeconds || 0;
    totalJourneyKm += r.journeys?.totalKm || 0;
    for (const j of r.journeys?.items || []) journeys.push({ vrn, ...j });
    vanRows.push({
      vrn,
      status: "OK",
      overspeedSeconds: r.overspeed?.totalSeconds || 0,
      idlingSeconds: r.idling?.totalSeconds || 0,
      journeyCount: r.journeys?.count || 0,
    });
  }

  for (const [vrn, _errMsg] of Object.entries(errorsByVrn || {})) {
    if (seenVrn.has(vrn)) continue;
    errorCount += 1;
    seenVrn.add(vrn);
    vanRows.push({ vrn, status: "ERROR" });
  }

  document.getElementById("cards").style.display = "grid";
  document.getElementById("journeyTable").style.display = journeys.length ? "table" : "none";

  document.getElementById("overspeedTime").textContent = fmtSeconds(totalOverspeedSeconds);
  document.getElementById("overspeedSegments").textContent = `${totalOverspeedSegments} segments`;
  document.getElementById("idlingTime").textContent = fmtSeconds(totalIdlingSeconds);
  document.getElementById("journeyCount").textContent = `${totalJourneys}`;
  document.getElementById("journeyTotals").textContent = `${fmtSeconds(totalJourneySeconds)} total, ${fmtKm(totalJourneyKm)} km`;

  journeys.sort((a, b) => (b.durationSeconds || 0) - (a.durationSeconds || 0));
  const tbody = document.getElementById("journeyTbody");
  tbody.innerHTML = "";
  for (const j of journeys.slice(0, 50)) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${j.vrn}</td>
      <td>${j.startUtc || "-"}</td>
      <td>${j.endUtc || "-"}</td>
      <td>${fmtSeconds(j.durationSeconds)}</td>
      <td>${fmtKm(j.distanceKm)}</td>
    `;
    tbody.appendChild(tr);
  }

  setStatus(`${okCount} vans with activity${errorCount ? ` | ${errorCount} fetch errors` : ""}`);
  setNarrative(
    `${okCount} vans had activity in this shift${errorCount ? `; ${errorCount} vans failed to load (see Vans tab or banner)` : ""}. ` +
      `Totals below are for active vans only. ` +
      `Overspeed ${fmtSeconds(totalOverspeedSeconds)} (${totalOverspeedSegments} segments); ` +
      `idling ${fmtSeconds(totalIdlingSeconds)}; journeys ${totalJourneys} (${fmtSeconds(totalJourneySeconds)}, ${fmtKm(totalJourneyKm)} km).`
  );

  const shiftDate = document.getElementById("shiftDate").value;
  const tbody2 = document.getElementById("vanTbody");
  if (tbody2) {
    const filtered = applyVanFilters(vanRows);
    filtered.sort((a, b) => a.vrn.localeCompare(b.vrn));
    tbody2.innerHTML = "";
    for (const r of filtered) {
      const tr = document.createElement("tr");
      const link =
        r.status === "OK" && shiftDate
          ? `/van/${encodeURIComponent(r.vrn)}?date=${encodeURIComponent(shiftDate)}`
          : r.status === "OK"
            ? `/van/${encodeURIComponent(r.vrn)}`
            : "";
      const vrnCell =
        r.status === "OK" && link ? `<a href="${link}">${r.vrn}</a>` : r.vrn;
      const overMin = secsToMinutes(r.overspeedSeconds || 0);
      const idleMin = secsToMinutes(r.idlingSeconds || 0);
      const overSev = r.status === "OK" ? severityBandForMinutes(overMin, 5, 20) : null;
      const idleSev = r.status === "OK" ? severityBandForMinutes(idleMin, 15, 45) : null;
      tr.innerHTML = `
        <td>${vrnCell}</td>
        <td>${r.status}</td>
        <td>${r.status === "OK" ? fmtSeconds(r.overspeedSeconds) : "-"}</td>
        <td>${r.status === "OK" ? `<span class="sev ${overSev.band}">${overSev.label}</span>` : "-"}</td>
        <td>${r.status === "OK" ? fmtSeconds(r.idlingSeconds) : "-"}</td>
        <td>${r.status === "OK" ? `<span class="sev ${idleSev.band}">${idleSev.label}</span>` : "-"}</td>
        <td>${r.status === "OK" ? r.journeyCount : "-"}</td>
      `;
      tbody2.appendChild(tr);
    }
    document.getElementById("vanTable").style.display = filtered.length ? "table" : "none";
  }
  updateRetryUi();
}

async function run() {
  setError("");
  const shiftDate = document.getElementById("shiftDate").value;
  if (!shiftDate) return setError("Please select a shift date.");

  setStatus("Fetching RAM history and computing metrics...");
  setNarrative("Computing daily summary...");
  document.getElementById("cards").style.display = "none";
  document.getElementById("journeyTable").style.display = "none";
  document.getElementById("vanTable").style.display = "none";

  const res = await fetch("/api/metrics", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ date: shiftDate }),
  });
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    setStatus("");
    return setError(`Server returned non-JSON (HTTP ${res.status}). First 200 chars: ${text.slice(0, 200)}`);
  }

  if (!res.ok) {
    setStatus("");
    return setError(data.error || `Request failed (${res.status})`);
  }

  const errsRaw = data.errors || {};
  window.__lastResultsByVrn = data.results || {};
  window.__lastErrorsByVrn = { ...errsRaw };
  if (Object.keys(window.__lastErrorsByVrn).length) {
    setError(formatErrorBanner(window.__lastErrorsByVrn));
  } else {
    setError("");
  }
  renderSummary(window.__lastResultsByVrn, window.__lastErrorsByVrn);
  saveDashboardCache();
}

async function retryFailedVans() {
  const shiftDate = document.getElementById("shiftDate").value;
  if (!shiftDate) return setError("Please select a shift date.");
  const toRetry = Object.keys(window.__lastErrorsByVrn || {});
  if (!toRetry.length) return;

  setError("");
  setStatus(`Retrying ${toRetry.length} van(s)...`);
  document.getElementById("retryFailedBtn").disabled = true;

  let res;
  let data;
  try {
    res = await fetch("/api/metrics/retry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date: shiftDate, vrns: toRetry }),
    });
    const text = await res.text();
    try {
      data = JSON.parse(text);
    } catch (e) {
      setStatus("");
      return setError(`Server returned non-JSON (HTTP ${res.status}). First 200 chars: ${text.slice(0, 200)}`);
    }
  } finally {
    document.getElementById("retryFailedBtn").disabled = false;
  }

  if (!res.ok) {
    setStatus("");
    return setError(data.error || `Request failed (${res.status})`);
  }

  const mergedResults = { ...(window.__lastResultsByVrn || {}) };
  const mergedErrors = { ...(window.__lastErrorsByVrn || {}) };

  for (const [vrn, metrics] of Object.entries(data.results || {})) {
    mergedResults[vrn] = metrics;
    delete mergedErrors[vrn];
  }
  for (const [vrn, msg] of Object.entries(data.errors || {})) {
    mergedErrors[vrn] = msg;
  }

  window.__lastResultsByVrn = mergedResults;
  window.__lastErrorsByVrn = mergedErrors;

  if (Object.keys(window.__lastErrorsByVrn).length) {
    setError(formatErrorBanner(window.__lastErrorsByVrn));
  } else {
    setError("");
  }
  renderSummary(window.__lastResultsByVrn, window.__lastErrorsByVrn);
  saveDashboardCache();
}

document.getElementById("runBtn").addEventListener("click", () => run().catch((e) => setError(String(e))));
document.getElementById("retryFailedBtn").addEventListener("click", () => retryFailedVans().catch((e) => setError(String(e))));
loadVans()
  .then(() => {
    restoreDashboardCache();
  })
  .catch((e) => setError(String(e)));

// Tabs
for (const btn of document.querySelectorAll(".tabBtn")) {
  btn.addEventListener("click", () => setActiveTab(btn.dataset.tab));
}

// Filters
function rerender() {
  // Re-run last render if we have it cached.
  if (window.__lastResultsByVrn) renderSummary(window.__lastResultsByVrn, window.__lastErrorsByVrn || {});
}
document.getElementById("statusChips").addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (!chip) return;
  for (const c of document.querySelectorAll("#statusChips .chip")) c.classList.remove("active");
  chip.classList.add("active");
  rerender();
});
for (const id of ["minOverspeedMin", "minIdlingMin", "minJourneys"]) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("input", () => rerender());
}

