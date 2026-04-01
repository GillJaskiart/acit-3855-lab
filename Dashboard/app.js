const PROCESSING_URL = "/processing/stats";
const ANALYZER_STATS_URL = "/analyzer/stats";
const ANALYZER_SPEEDING_URL = "/analyzer/events/speeding";
const ANALYZER_CONGESTION_URL = "/analyzer/events/congestion";

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

async function refreshDashboard() {
  await loadProcessingStats();
  await loadAnalyzerData();
  setText("browser_updated", new Date().toLocaleString());
}

refreshDashboard();
setInterval(refreshDashboard, 3000);
