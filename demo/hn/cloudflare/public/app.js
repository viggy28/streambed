const COLORS = ["#3366cc", "#ff5a1f", "#24856c", "#9c5dcc", "#d59b12"];

const presets = [
  {
    id: "database-mentions",
    icon: "↗",
    kicker: "TECHNOLOGY TRENDS",
    title: "PostgreSQL vs MySQL",
    short: "Database mentions",
    description: "Monthly HN story-title mentions during the last two years.",
    chart: { type: "line", category: 0, series: [1, 2] },
    sql: `SELECT
  strftime(date_trunc('month', created_at), '%Y-%m') AS month,
  count(*) FILTER (WHERE mentions_postgresql) AS postgresql,
  count(*) FILTER (WHERE mentions_mysql) AS mysql
FROM story_analytics
WHERE created_at >= date_trunc('month', current_date) - INTERVAL '24 months'
  AND created_at < date_trunc('month', current_date)
  AND (mentions_postgresql OR mentions_mysql)
GROUP BY 1
ORDER BY 1`,
  },
  {
    id: "story-volume",
    icon: "▥",
    kicker: "PUBLISHING ACTIVITY",
    title: "Story volume and score",
    short: "Monthly story volume",
    description: "How many tracked stories appeared each month, with their average score.",
    chart: { type: "line", category: 0, series: [1, 2] },
    sql: `SELECT
  strftime(date_trunc('month', created_at), '%Y-%m') AS month,
  count(*) AS stories,
  round(avg(score), 1) AS avg_score
FROM story_analytics
WHERE created_at >= current_date - INTERVAL '2 years'
GROUP BY 1
ORDER BY 1`,
  },
  {
    id: "most-discussed",
    icon: "◉",
    kicker: "CONVERSATION",
    title: "Most discussed stories",
    short: "Most discussed",
    description: "Stories that generated the largest conversations in the selected history.",
    chart: { type: "bar", category: 0, series: [1] },
    sql: `SELECT
  title,
  comment_count AS comments,
  score,
  strftime(created_at, '%Y-%m-%d') AS published
FROM stories
WHERE created_at >= current_date - INTERVAL '2 years'
  AND title IS NOT NULL
ORDER BY comment_count DESC NULLS LAST
LIMIT 15`,
  },
  {
    id: "trending-terms",
    icon: "≈",
    kicker: "TOPIC VELOCITY",
    title: "Terms over time",
    short: "Trending terms",
    description: "Compare monthly title mentions for several recurring HN topics.",
    chart: { type: "line", category: 0, series: [1, 2, 3, 4] },
    sql: `SELECT
  strftime(date_trunc('month', created_at), '%Y-%m') AS month,
  count(*) FILTER (WHERE mentions_ai) AS ai,
  count(*) FILTER (WHERE mentions_rust) AS rust,
  count(*) FILTER (WHERE mentions_python) AS python,
  count(*) FILTER (WHERE mentions_postgresql) AS postgresql
FROM story_analytics
WHERE created_at >= current_date - INTERVAL '2 years'
  AND (mentions_ai OR mentions_rust OR mentions_python OR mentions_postgresql)
GROUP BY 1
ORDER BY 1`,
  },
  {
    id: "front-page",
    icon: "#",
    kicker: "LIVE STATE",
    title: "Current front page",
    short: "Current front page",
    description: "The latest top 30 materialized from mutable Postgres rows.",
    chart: { type: "bar", category: 1, series: [2] },
    sql: `SELECT rank, title, score, comment_count
FROM front_page
ORDER BY rank
LIMIT 30`,
  },
  {
    id: "time-travel",
    icon: "◷",
    kicker: "SNAPSHOT TIME TRAVEL",
    title: "Earlier front page",
    short: "Rewind the front page",
    description: "Query the physical front-page table at a retained Iceberg snapshot.",
    chart: { type: "bar", category: 1, series: [2] },
    needsSnapshot: true,
    sql: "",
  },
];

const state = {
  preset: presets[0],
  result: null,
  view: "chart",
  snapshots: [],
  running: false,
};

const elements = {
  presetList: document.querySelector("#preset-list"),
  kicker: document.querySelector("#query-kicker"),
  title: document.querySelector("#query-title"),
  description: document.querySelector("#query-description"),
  editor: document.querySelector("#sql-editor"),
  run: document.querySelector("#run-query"),
  copy: document.querySelector("#copy-sql"),
  status: document.querySelector("#query-status"),
  statusMessage: document.querySelector("#status-message"),
  executionTime: document.querySelector("#execution-time"),
  rowCount: document.querySelector("#row-count"),
  chartContainer: document.querySelector("#chart-container"),
  tableContainer: document.querySelector("#table-container"),
  chartView: document.querySelector("#chart-view"),
  tableView: document.querySelector("#table-view"),
  snapshotControl: document.querySelector("#snapshot-control"),
  snapshotSelect: document.querySelector("#snapshot-select"),
};

function makeElement(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function renderPresets() {
  elements.presetList.replaceChildren();
  presets.forEach((preset, index) => {
    const button = makeElement("button", "preset-button");
    button.type = "button";
    button.dataset.preset = preset.id;
    button.setAttribute("aria-pressed", String(preset.id === state.preset.id));
    if (preset.id === state.preset.id) button.classList.add("active");
    button.append(makeElement("span", "preset-icon", preset.icon));
    const copy = makeElement("span");
    copy.append(makeElement("strong", "", preset.short));
    copy.append(makeElement("small", "", index === 0 ? "Featured analysis" : preset.kicker.toLowerCase()));
    button.append(copy);
    button.addEventListener("click", () => selectPreset(preset, true));
    elements.presetList.append(button);
  });
}

function selectPreset(preset, shouldRun = false) {
  state.preset = preset;
  document.querySelectorAll(".preset-button").forEach((button) => {
    const selected = button.dataset.preset === preset.id;
    button.classList.toggle("active", selected);
    button.setAttribute("aria-pressed", String(selected));
  });
  elements.kicker.textContent = preset.kicker;
  elements.title.textContent = preset.title;
  elements.description.textContent = preset.description;
  elements.snapshotControl.hidden = !preset.needsSnapshot;
  elements.editor.value = preset.needsSnapshot ? historicalSQL(elements.snapshotSelect.value) : preset.sql;
  if (preset.needsSnapshot && state.snapshots.length === 0) {
    setStatus("error", "No retained snapshots are available yet.");
    return;
  }
  if (shouldRun) runQuery();
}

function historicalSQL(timestamp) {
  if (!timestamp) return "-- Choose a retained snapshot above";
  const safeTimestamp = timestamp.replace(/[^0-9T:.+\-Z]/g, "");
  return `SELECT rank, title, score, comment_count
FROM front_page AS f
AT (TIMESTAMP => TIMESTAMPTZ '${safeTimestamp}')
ORDER BY rank
LIMIT 30`;
}

function setStatus(kind, message) {
  elements.status.className = `query-status ${kind}`;
  elements.statusMessage.textContent = message;
}

async function runQuery() {
  const sql = elements.editor.value.trim();
  if (!sql || sql.startsWith("--")) {
    setStatus("error", "Choose a snapshot or enter a SELECT query first.");
    return;
  }
  if (state.running) return;
  state.running = true;
  elements.run.disabled = true;
  const started = performance.now();
  setStatus("running", "Starting query…");
  const wakeMessage = window.setTimeout(() => {
    if (state.running) setStatus("running", "Waking the scale-to-zero query engine…");
  }, 1600);

  try {
    const response = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
      signal: AbortSignal.timeout(60_000),
    });
    const payload = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
    if (!response.ok) throw new Error(payload.error || `Query failed with HTTP ${response.status}`);
    state.result = payload;
    const elapsed = performance.now() - started;
    elements.executionTime.textContent = elapsed < 1000 ? `${Math.round(elapsed)} ms` : `${(elapsed / 1000).toFixed(1)} s`;
    elements.rowCount.textContent = `${payload.row_count.toLocaleString()} ${payload.row_count === 1 ? "row" : "rows"}`;
    renderResult();
    setStatus("success", payload.row_count === 0 ? "Query completed with no matching rows." : "Query completed.");
  } catch (error) {
    const message = error.name === "TimeoutError" ? "The request timed out while waking or running the query." : error.message;
    setStatus("error", message);
  } finally {
    window.clearTimeout(wakeMessage);
    state.running = false;
    elements.run.disabled = false;
  }
}

function renderResult() {
  renderTable();
  renderChart();
  setView(state.view);
}

function renderTable() {
  elements.tableContainer.replaceChildren();
  if (!state.result) return;
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const headerRow = document.createElement("tr");
  state.result.columns.forEach((column) => headerRow.append(makeElement("th", "", column.name)));
  head.append(headerRow);
  const body = document.createElement("tbody");
  state.result.rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((value) => tr.append(makeElement("td", "", formatValue(value))));
    body.append(tr);
  });
  table.append(head, body);
  elements.tableContainer.append(table);
}

function renderChart() {
  elements.chartContainer.replaceChildren();
  if (!state.result || state.result.rows.length === 0) {
    elements.chartContainer.append(makeElement("div", "empty-result", "No rows to visualize. Try another date range or query."));
    return;
  }
  const config = state.preset.chart;
  const seriesIndexes = config.series.filter((index) => state.result.columns[index]);
  const numericSeries = seriesIndexes.filter((index) => state.result.rows.some((row) => typeof row[index] === "number"));
  if (numericSeries.length === 0) {
    elements.chartContainer.append(makeElement("div", "empty-result", "This result is best viewed as a table."));
    return;
  }
  if (config.type === "bar") {
    renderBarChart(config.category, numericSeries[0]);
  } else {
    renderLineChart(config.category, numericSeries);
  }
}

function chartFrame() {
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 900 320");
  svg.setAttribute("class", "chart-svg");
  svg.setAttribute("role", "img");
  return { ns, svg, left: 62, top: 18, width: 812, height: 245 };
}

function renderLineChart(categoryIndex, seriesIndexes) {
  const frame = chartFrame();
  const { ns, svg, left, top, width, height } = frame;
  const rows = state.result.rows;
  const values = rows.flatMap((row) => seriesIndexes.map((index) => Number(row[index]) || 0));
  const max = Math.max(1, ...values);
  drawGrid(frame, max);
  seriesIndexes.forEach((seriesIndex, seriesPosition) => {
    const color = COLORS[seriesPosition % COLORS.length];
    const points = rows.map((row, index) => {
      const x = rows.length === 1 ? left + width / 2 : left + (index / (rows.length - 1)) * width;
      const y = top + height - ((Number(row[seriesIndex]) || 0) / max) * height;
      return { x, y, value: row[seriesIndex], label: row[categoryIndex] };
    });
    const path = document.createElementNS(ns, "path");
    path.setAttribute("class", "chart-line");
    path.setAttribute("stroke", color);
    path.setAttribute("d", points.map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" "));
    svg.append(path);
    points.forEach((point) => {
      const circle = document.createElementNS(ns, "circle");
      circle.setAttribute("class", "chart-point");
      circle.setAttribute("cx", point.x);
      circle.setAttribute("cy", point.y);
      circle.setAttribute("r", rows.length > 18 ? "3" : "4");
      circle.setAttribute("fill", color);
      const title = document.createElementNS(ns, "title");
      title.textContent = `${point.label}: ${formatValue(point.value)}`;
      circle.append(title);
      svg.append(circle);
    });
  });
  drawXLabels(frame, rows.map((row) => row[categoryIndex]));
  elements.chartContainer.append(svg, renderLegend(seriesIndexes));
}

function renderBarChart(categoryIndex, seriesIndex) {
  const frame = chartFrame();
  const { ns, svg, left, top, width, height } = frame;
  const rows = state.result.rows.slice(0, 20);
  const values = rows.map((row) => Number(row[seriesIndex]) || 0);
  const max = Math.max(1, ...values);
  drawGrid(frame, max);
  const step = width / Math.max(rows.length, 1);
  const barWidth = Math.max(5, Math.min(34, step * .68));
  rows.forEach((row, index) => {
    const value = values[index];
    const barHeight = (value / max) * height;
    const rect = document.createElementNS(ns, "rect");
    rect.setAttribute("class", "chart-bar");
    rect.setAttribute("x", left + index * step + (step - barWidth) / 2);
    rect.setAttribute("y", top + height - barHeight);
    rect.setAttribute("width", barWidth);
    rect.setAttribute("height", Math.max(barHeight, 1));
    rect.setAttribute("rx", "2");
    rect.setAttribute("fill", COLORS[index % 2]);
    const title = document.createElementNS(ns, "title");
    title.textContent = `${formatValue(row[categoryIndex])}: ${formatValue(value)}`;
    rect.append(title);
    svg.append(rect);
  });
  drawXLabels(frame, rows.map((row) => row[categoryIndex]));
  elements.chartContainer.append(svg, renderLegend([seriesIndex]));
}

function drawGrid(frame, max) {
  const { ns, svg, left, top, width, height } = frame;
  for (let index = 0; index <= 4; index++) {
    const y = top + (index / 4) * height;
    const line = document.createElementNS(ns, "line");
    line.setAttribute("class", "chart-grid");
    line.setAttribute("x1", left);
    line.setAttribute("x2", left + width);
    line.setAttribute("y1", y);
    line.setAttribute("y2", y);
    svg.append(line);
    const label = document.createElementNS(ns, "text");
    label.setAttribute("class", "chart-axis-text");
    label.setAttribute("x", left - 10);
    label.setAttribute("y", y + 4);
    label.setAttribute("text-anchor", "end");
    label.textContent = compactNumber(max * (1 - index / 4));
    svg.append(label);
  }
}

function drawXLabels(frame, labels) {
  const { ns, svg, left, top, width, height } = frame;
  const every = Math.max(1, Math.ceil(labels.length / 8));
  labels.forEach((value, index) => {
    if (index % every !== 0 && index !== labels.length - 1) return;
    const x = labels.length === 1 ? left + width / 2 : left + (index / (labels.length - 1)) * width;
    const text = document.createElementNS(ns, "text");
    text.setAttribute("class", "chart-axis-text");
    text.setAttribute("x", x);
    text.setAttribute("y", top + height + 25);
    text.setAttribute("text-anchor", "middle");
    const rendered = formatValue(value);
    text.textContent = rendered.length > 18 ? `${rendered.slice(0, 16)}…` : rendered;
    svg.append(text);
  });
}

function renderLegend(seriesIndexes) {
  const legend = makeElement("div", "chart-legend");
  seriesIndexes.forEach((seriesIndex, position) => {
    const item = makeElement("span");
    const swatch = makeElement("i");
    swatch.style.background = COLORS[position % COLORS.length];
    item.append(swatch, document.createTextNode(state.result.columns[seriesIndex].name.replaceAll("_", " ")));
    legend.append(item);
  });
  return legend;
}

function setView(view) {
  state.view = view;
  const chart = view === "chart";
  elements.chartContainer.hidden = !chart;
  elements.tableContainer.hidden = chart;
  elements.chartView.classList.toggle("active", chart);
  elements.chartView.setAttribute("aria-pressed", String(chart));
  elements.tableView.classList.toggle("active", !chart);
  elements.tableView.setAttribute("aria-pressed", String(!chart));
}

function formatValue(value) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return Number.isInteger(value) ? value.toLocaleString() : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(value);
}

function compactNumber(value) {
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

async function loadMetadata() {
  try {
    const response = await fetch("/metadata", { signal: AbortSignal.timeout(60_000) });
    if (!response.ok) throw new Error(`metadata returned HTTP ${response.status}`);
    const metadata = await response.json();
    const coverage = metadata.coverage || {};
    document.querySelector("#story-count").textContent = Number(coverage.story_count || 0).toLocaleString();
    document.querySelector("#coverage-range").textContent = coverage.coverage_start && coverage.coverage_end
      ? `${formatDate(coverage.coverage_start)} — ${formatDate(coverage.coverage_end)}`
      : "Waiting for history";
    document.querySelector("#last-ingested").textContent = coverage.last_ingested_at ? relativeTime(coverage.last_ingested_at) : "—";
    state.snapshots = metadata.snapshots || [];
    document.querySelector("#snapshot-count").textContent = metadata.snapshots_truncated ? `${state.snapshots.length}+` : state.snapshots.length.toLocaleString();
    renderSnapshotOptions();
  } catch (error) {
    document.querySelector("#coverage-range").textContent = "Temporarily unavailable";
    console.warn("Could not load demo metadata", error);
  }
}

function renderSnapshotOptions() {
  elements.snapshotSelect.replaceChildren();
  state.snapshots.forEach((snapshot) => {
    const option = document.createElement("option");
    option.value = snapshot.timestamp;
    option.textContent = new Date(snapshot.timestamp).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" }) + " UTC";
    elements.snapshotSelect.append(option);
  });
  if (state.preset.needsSnapshot) elements.editor.value = historicalSQL(elements.snapshotSelect.value);
}

function formatDate(value) {
  return new Date(value).toLocaleDateString(undefined, { month: "short", year: "numeric", timeZone: "UTC" });
}

function relativeTime(value) {
  const seconds = Math.round((new Date(value).getTime() - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  const units = [["day", 86400], ["hour", 3600], ["minute", 60]];
  for (const [unit, size] of units) {
    if (Math.abs(seconds) >= size) return formatter.format(Math.round(seconds / size), unit);
  }
  return formatter.format(seconds, "second");
}

function bindEvents() {
  elements.run.addEventListener("click", runQuery);
  elements.copy.addEventListener("click", async () => {
    await navigator.clipboard.writeText(elements.editor.value);
    const original = elements.copy.textContent;
    elements.copy.textContent = "Copied";
    window.setTimeout(() => { elements.copy.textContent = original; }, 1200);
  });
  elements.editor.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      runQuery();
    }
    if (event.key === "Tab") {
      event.preventDefault();
      const start = elements.editor.selectionStart;
      const end = elements.editor.selectionEnd;
      elements.editor.setRangeText("  ", start, end, "end");
    }
  });
  elements.snapshotSelect.addEventListener("change", () => {
    elements.editor.value = historicalSQL(elements.snapshotSelect.value);
  });
  elements.chartView.addEventListener("click", () => setView("chart"));
  elements.tableView.addEventListener("click", () => setView("table"));
  document.querySelector("[data-scroll-workspace]").addEventListener("click", () => document.querySelector("#workspace").scrollIntoView());
}

async function initialize() {
  renderPresets();
  bindEvents();
  selectPreset(presets[0]);
  await runQuery();
  await loadMetadata();
}

initialize();
