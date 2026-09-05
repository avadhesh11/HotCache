// HotCache Interactive Dashboard Client Logic

let telemetryChart;
let ws = null;
const maxDataPoints = 30;

// Chart Data Buffers
const chartLabels = [];
const loadData = [];
const ttlData = [];

document.addEventListener("DOMContentLoaded", () => {
  initChart();
  initWebSocket();
  bindButtons();
});

function initChart() {
  const ctx = document.getElementById("telemetryChart").getContext("2d");
  
  // Fill initial blank history
  for (let i = 0; i < maxDataPoints; i++) {
    chartLabels.push("");
    loadData.push(0);
    ttlData.push(5);
  }

  telemetryChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: chartLabels,
      datasets: [
        {
          label: "Request Load (req/min)",
          data: loadData,
          borderColor: "#818cf8",
          backgroundColor: "rgba(129, 140, 248, 0.15)",
          borderWidth: 2.5,
          tension: 0.35,
          fill: true,
          yAxisID: "yLoad",
          pointRadius: 0,
          pointHoverRadius: 4,
        },
        {
          label: "Active TTL (seconds)",
          data: ttlData,
          borderColor: "#f43f5e",
          backgroundColor: "rgba(244, 63, 94, 0.15)",
          borderWidth: 3,
          stepped: true, // Step chart to clearly show doubling / decay transitions!
          fill: false,
          yAxisID: "yTTL",
          pointRadius: 0,
          pointHoverRadius: 4,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: 300,
        easing: "linear"
      },
      interaction: {
        mode: "index",
        intersect: false,
      },
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.9)",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "rgba(255,255,255,0.1)",
          borderWidth: 1,
        }
      },
      scales: {
        x: {
          display: false,
          grid: { display: false }
        },
        yLoad: {
          type: "linear",
          display: true,
          position: "left",
          title: {
            display: true,
            text: "Req / Min",
            color: "#818cf8",
            font: { size: 11, weight: "bold" }
          },
          grid: {
            color: "rgba(255, 255, 255, 0.05)"
          },
          ticks: {
            color: "#94a3b8",
            font: { family: "JetBrains Mono", size: 10 }
          },
          suggestedMax: 60,
          min: 0,
        },
        yTTL: {
          type: "linear",
          display: true,
          position: "right",
          title: {
            display: true,
            text: "TTL (sec)",
            color: "#f43f5e",
            font: { size: 11, weight: "bold" }
          },
          grid: {
            drawOnChartArea: false
          },
          ticks: {
            color: "#f43f5e",
            font: { family: "JetBrains Mono", size: 10 }
          },
          suggestedMax: 60,
          min: 0,
        }
      }
    }
  });
}

function initWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${window.location.host}/ws/stats`;
  
  const statusEl = document.getElementById("wsStatusText");
  const statusBadge = document.getElementById("wsStatus");

  try {
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      statusEl.textContent = "Live Telemetry Connected";
      statusBadge.style.borderColor = "rgba(16, 185, 129, 0.4)";
    };

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        updateDashboard(payload);
      } catch (e) {
        console.error("Error parsing telemetry frame", e);
      }
    };

    ws.onclose = () => {
      statusEl.textContent = "Reconnecting...";
      statusBadge.style.borderColor = "rgba(239, 68, 68, 0.4)";
      setTimeout(initWebSocket, 2000);
    };

    ws.onerror = () => {
      ws.close();
    };
  } catch (err) {
    console.error("WS connection failed", err);
    setTimeout(initWebSocket, 3000);
  }
}

function updateDashboard(data) {
  const summary = data.summary || {};
  const keys = data.keys || {};
  const traffic = data.traffic_state || {};
  const events = data.events || [];

  // 1. Celebrity Post Specifics
  const celeb = keys["post:celebrity-post"] || { current_ttl: 5, current_load: 0, hits: 0, misses: 0 };
  const celebTtl = celeb.current_ttl || 5;
  const celebLoad = celeb.current_load || 0;

  document.getElementById("celebrityTtlVal").innerHTML = `${celebTtl}<span class="metric-unit">s</span>`;
  document.getElementById("celebrityMultiplier").textContent = `${(celebTtl / 5).toFixed(1)}x Base (5s)`;
  document.getElementById("celebrityLoadVal").textContent = `${celebLoad} req/min`;
  document.getElementById("post1TtlBadge").textContent = `TTL: ${celebTtl}s`;

  // Progress Bar (0 to 60s max)
  const progressPct = Math.min(100, Math.round((celebTtl / 60) * 100));
  document.getElementById("ttlProgressBar").style.width = `${progressPct}%`;

  // 2. DB Metrics & Savings
  const hotcacheDb = summary.db_queries || 0;
  const baselineDb = summary.baseline_db_queries || 0;
  document.getElementById("hotcacheDbHits").textContent = hotcacheDb;
  document.getElementById("baselineDbHits").textContent = baselineDb;
  document.getElementById("compHotcacheDb").textContent = hotcacheDb;
  document.getElementById("compBaselineDb").textContent = baselineDb;

  const totalHits = (summary.hits || 0) + (summary.stale_hits || 0);
  const totalReqs = summary.total_requests || 0;
  const savedQueries = totalHits;
  document.getElementById("dbSavedRatio").textContent = `${savedQueries} queries saved`;

  const hitRatio = totalReqs > 0 ? ((totalHits / totalReqs) * 100).toFixed(1) : "100.0";
  document.getElementById("hitRatioVal").innerHTML = `${hitRatio}<span class="metric-unit">%</span>`;
  document.getElementById("dbSavedPercent").innerHTML = `${hitRatio}<span class="metric-unit">%</span>`;

  document.getElementById("totalReqsVal").textContent = `${totalReqs} total reqs`;
  document.getElementById("hitsVal").textContent = summary.hits || 0;
  document.getElementById("swrHitsVal").textContent = summary.stale_hits || 0;
  document.getElementById("missesVal").textContent = summary.misses || 0;

  // 3. Stampede Locks
  const locksAcquired = summary.stampede_locks_acquired || 0;
  const locksContended = summary.stampede_locks_contended || 0;
  document.getElementById("stampedeLocksVal").textContent = locksAcquired + locksContended;
  document.getElementById("stampedeSavesVal").textContent = locksContended;
  document.getElementById("locksAcquiredVal").textContent = locksAcquired;
  document.getElementById("locksContendedVal").textContent = locksContended;

  // 4. Traffic Pill
  const trafficPill = document.getElementById("trafficStatePill");
  if (traffic.active) {
    trafficPill.textContent = `GENERATING: ${traffic.mode.toUpperCase()}`;
    trafficPill.style.background = "rgba(99, 102, 241, 0.3)";
    trafficPill.style.color = "#a5b4fc";
  } else {
    trafficPill.textContent = "IDLE";
    trafficPill.style.background = "rgba(255, 255, 255, 0.08)";
    trafficPill.style.color = "#cbd5e1";
  }

  // 5. Update Chart Stream
  loadData.shift();
  loadData.push(celebLoad);

  ttlData.shift();
  ttlData.push(celebTtl);

  if (telemetryChart) {
    telemetryChart.update();
  }

  // 6. Update Event Log
  renderEvents(events);
}

function renderEvents(events) {
  const container = document.getElementById("eventLogContainer");
  if (!events || events.length === 0) return;

  container.innerHTML = events.map(ev => {
    const timeStr = new Date(ev.timestamp * 1000).toLocaleTimeString();
    return `<div class="log-entry ${ev.type}">
      <span style="opacity:0.6">[${timeStr}]</span> ${escapeHtml(ev.message)}
    </div>`;
  }).join("");

  container.scrollTop = container.scrollHeight;
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.innerText = text;
  return div.innerHTML;
}

function bindButtons() {
  // Traffic Preset Buttons
  document.getElementById("btnViralSpike").addEventListener("click", () => triggerTraffic("viral_spike"));
  document.getElementById("btnStampede").addEventListener("click", () => triggerTraffic("stampede"));
  document.getElementById("btnCooldown").addEventListener("click", () => triggerTraffic("cooldown"));
  document.getElementById("btnSteady").addEventListener("click", () => triggerTraffic("steady"));
  document.getElementById("btnStopTraffic").addEventListener("click", stopTraffic);

  // Reset Button
  document.getElementById("resetBtn").addEventListener("click", async () => {
    await fetch("/api/metrics/reset", { method: "POST" });
    for (let i = 0; i < maxDataPoints; i++) {
      loadData[i] = 0;
      ttlData[i] = 5;
    }
    if (telemetryChart) telemetryChart.update();
  });

  // Manual Fetch Post Button
  document.getElementById("btnManualFetch").addEventListener("click", async () => {
    const t0 = performance.now();
    const res = await fetch("/posts/celebrity-post");
    const t1 = performance.now();
    const latency = Math.round(t1 - t0);
    document.getElementById("post1Latency").textContent = `Latency: ${latency}ms`;
  });
}

async function triggerTraffic(scenario) {
  await fetch("/api/traffic/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scenario: scenario, target_url: "/posts/celebrity-post" })
  });
}

async function stopTraffic() {
  await fetch("/api/traffic/stop", { method: "POST" });
  const trafficPill = document.getElementById("trafficStatePill");
  if (trafficPill) {
    trafficPill.textContent = "IDLE";
    trafficPill.style.background = "rgba(255, 255, 255, 0.08)";
    trafficPill.style.color = "#cbd5e1";
  }
}
