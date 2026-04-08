const PROCESSING_URL = "/processing/stats";
const ANALYZER_STATS_URL = "/analyzer/stats";
const ANALYZER_SPEEDING_URL = "/analyzer/events/speeding";
const ANALYZER_CONGESTION_URL = "/analyzer/events/congestion";
const HEALTH_STATUS_URL = "/health-check/status";
const DASHBOARD_REFRESH_MS = 5000;

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} from ${url}`);
  }
  return await response.json();
}

function randomIndex(max) {
  return Math.floor(Math.random() * max);
}

function setStatus(id, value) {
  const element = document.getElementById(id);
  const normalizedValue = (value ?? "Unknown").toString();
  const statusClass = normalizedValue.toLowerCase();

  element.textContent = normalizedValue;
  element.className = `status-badge status-${statusClass}`;
}

function formatRelativeTime(isoTimestamp) {
  if (!isoTimestamp) {
    return "-";
  }

  const timestamp = new Date(isoTimestamp);
  const diffSeconds = Math.max(0, Math.floor((Date.now() - timestamp.getTime()) / 1000));

  if (Number.isNaN(timestamp.getTime())) {
    return isoTimestamp;
  }

  if (diffSeconds < 5) {
    return "just now";
  }

  if (diffSeconds < 60) {
    return `${diffSeconds} seconds ago`;
  }

  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) {
    return `${diffMinutes} minute${diffMinutes === 1 ? "" : "s"} ago`;
  }

  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours} hour${diffHours === 1 ? "" : "s"} ago`;
  }

  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays} day${diffDays === 1 ? "" : "s"} ago`;
}

function setHealthUpdate(timestamp) {
  const element = document.getElementById("health_last_update");

  if (!timestamp) {
    element.textContent = "-";
    element.removeAttribute("title");
    return;
  }

  element.textContent = formatRelativeTime(timestamp);
  element.title = new Date(timestamp).toLocaleString();
}

async function loadProcessingStats() {
  try {
    const data = await fetchJson(PROCESSING_URL);

    setText("num_speeding_events", data.num_speeding_events ?? "-");
    setText("min_speed_kmh", data.min_speed_kmh ?? "-");
    setText("max_speed_kmh", data.max_speed_kmh ?? "-");
    setText("num_congestion_events", data.num_congestion_events ?? "-");
    setText("max_vehicles_passing", data.max_vehicles_passing ?? "-");
    setText("last_updated", data.last_updated ?? "-");
  } catch (err) {
    console.error("Processing stats error:", err);
    setText("num_speeding_events", "Error");
    setText("min_speed_kmh", "Error");
    setText("max_speed_kmh", "Error");
    setText("num_congestion_events", "Error");
    setText("max_vehicles_passing", "Error");
    setText("last_updated", "Error");
  }
}

async function loadAnalyzerData() {
  try {
    const stats = await fetchJson(ANALYZER_STATS_URL);

    const speedingCount = stats.num_speeding_events ?? 0;
    const congestionCount = stats.num_congestion_events ?? 0;

    setText("analyzer_num_speeding_events", speedingCount);
    setText("analyzer_num_congestion_events", congestionCount);

    if (speedingCount > 0) {
      const sIndex = randomIndex(speedingCount);
      const speedingEvent = await fetchJson(`${ANALYZER_SPEEDING_URL}?index=${sIndex}`);
      document.getElementById("speeding_event").textContent = JSON.stringify(speedingEvent, null, 2);
    } else {
      document.getElementById("speeding_event").textContent = "No speeding events available.";
    }

    if (congestionCount > 0) {
      const cIndex = randomIndex(congestionCount);
      const congestionEvent = await fetchJson(`${ANALYZER_CONGESTION_URL}?index=${cIndex}`);
      document.getElementById("congestion_event").textContent = JSON.stringify(congestionEvent, null, 2);
    } else {
      document.getElementById("congestion_event").textContent = "No congestion events available.";
    }
  } catch (err) {
    console.error("Analyzer error:", err);
    document.getElementById("speeding_event").textContent = "Error loading speeding event.";
    document.getElementById("congestion_event").textContent = "Error loading congestion event.";
    setText("analyzer_num_speeding_events", "Error");
    setText("analyzer_num_congestion_events", "Error");
  }
}

async function loadHealthStatus() {
  try {
    const data = await fetchJson(HEALTH_STATUS_URL);

    setStatus("receiver_status", data.receiver);
    setStatus("storage_status", data.storage);
    setStatus("processing_status", data.processing);
    setStatus("analyzer_status", data.analyzer);
    setHealthUpdate(data.last_update);
  } catch (err) {
    console.error("Health status error:", err);
    setStatus("receiver_status", "Unavailable");
    setStatus("storage_status", "Unavailable");
    setStatus("processing_status", "Unavailable");
    setStatus("analyzer_status", "Unavailable");
    setHealthUpdate(null);
  }
}

async function refreshDashboard() {
  await Promise.allSettled([
    loadProcessingStats(),
    loadAnalyzerData(),
    loadHealthStatus()
  ]);
  setText("browser_updated", new Date().toLocaleString());
}

refreshDashboard();
setInterval(refreshDashboard, DASHBOARD_REFRESH_MS);
