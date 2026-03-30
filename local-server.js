const http = require("http");
const fs = require("fs");
const path = require("path");

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "local-data");
const LATEST_FILE = path.join(DATA_DIR, "latest_positions.json");
const HISTORY_FILE = path.join(DATA_DIR, "aircraft_history.json");
const HOST = "127.0.0.1";
const PORT = 8787;
const VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json";
const JETAPI_URL = "https://www.jetapi.dev/api";
const LATEST_TTL_MS = 6 * 60 * 60 * 1000;
const HISTORY_TTL_MS = 48 * 60 * 60 * 1000;
const VATSIM_POLL_INTERVAL_MS = 5000;
const VATSIM_CACHE_MAX_AGE_MS = 4000;

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

let latestPositions = Object.create(null);
let aircraftHistory = Object.create(null);
let persistTimer = null;
let lastObservedAt = 0;
let latestVatsimBody = "";
let latestVatsimFetchedAt = 0;
let latestVatsimObservedAt = 0;
let vatsimPollInFlight = null;
let vatsimPollError = null;

fs.mkdirSync(DATA_DIR, { recursive: true });

function loadJsonFile(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function loadPersistedData() {
  latestPositions = loadJsonFile(LATEST_FILE, Object.create(null));
  aircraftHistory = loadJsonFile(HISTORY_FILE, Object.create(null));
  for (const points of Object.values(aircraftHistory)) {
    if (!Array.isArray(points) || !points.length) continue;
    const last = points[points.length - 1];
    if (last && last.observed_at && last.observed_at > lastObservedAt) lastObservedAt = last.observed_at;
  }
  cleanupPersistedData(Date.now());
}

function schedulePersist() {
  if (persistTimer) return;
  persistTimer = setTimeout(() => {
    persistTimer = null;
    fs.writeFileSync(LATEST_FILE, JSON.stringify(latestPositions));
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(aircraftHistory));
  }, 250);
}

function cleanupPersistedData(nowMs) {
  for (const [callsign, row] of Object.entries(latestPositions)) {
    if (!row || !row.observed_at || nowMs - row.observed_at > LATEST_TTL_MS) {
      delete latestPositions[callsign];
    }
  }

  for (const [callsign, points] of Object.entries(aircraftHistory)) {
    const filtered = Array.isArray(points)
      ? points.filter((point) => point && point.observed_at && nowMs - point.observed_at <= HISTORY_TTL_MS)
      : [];
    if (filtered.length) {
      aircraftHistory[callsign] = filtered;
    } else {
      delete aircraftHistory[callsign];
    }
  }
}

function cleanupEndedFlights(activeCallsigns) {
  if (!activeCallsigns.size) return;
  for (const callsign of Object.keys(latestPositions)) {
    if (!activeCallsigns.has(callsign)) {
      delete latestPositions[callsign];
      delete aircraftHistory[callsign];
    }
  }

  for (const callsign of Object.keys(aircraftHistory)) {
    if (!activeCallsigns.has(callsign)) {
      delete aircraftHistory[callsign];
    }
  }
}

function persistRows(rows, observedAt) {
  if (observedAt <= lastObservedAt) return;
  lastObservedAt = observedAt;
  cleanupPersistedData(observedAt);
  cleanupEndedFlights(new Set(rows.map((row) => row.callsign)));

  for (const row of rows) {
    latestPositions[row.callsign] = {
      observed_at: row.observedAt,
      lat: row.lat,
      lon: row.lon,
    };

    const points = aircraftHistory[row.callsign] || [];
    const last = points[points.length - 1];
    if (!last || last.observed_at !== row.observedAt || last.lat !== row.lat || last.lon !== row.lon) {
      points.push({
        observed_at: row.observedAt,
        lat: row.lat,
        lon: row.lon,
        altitude: row.altitude,
        groundspeed: row.groundspeed,
        heading: row.heading,
        squawk: row.squawk,
        aircraft_code: row.aircraftCode,
      });
      aircraftHistory[row.callsign] = points;
    }
  }

  schedulePersist();
}

function sendJson(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Access-Control-Allow-Origin": "*",
  });
  res.end(body);
}

function sendFile(res, filePath) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(err.code === "ENOENT" ? 404 : 500, { "Content-Type": "text/plain; charset=utf-8" });
      res.end(err.code === "ENOENT" ? "Not found" : "Server error");
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, {
      "Content-Type": MIME_TYPES[ext] || "application/octet-stream",
      "Cache-Control": "no-store",
    });
    res.end(data);
  });
}

function mapJetApiResponse(data, registration) {
  const images = data && data.JetPhotos && Array.isArray(data.JetPhotos.Images) ? data.JetPhotos.Images : [];
  return {
    photos: images.map((item, index) => ({
      photoId: item.Link ? String(item.Link).split("/").pop() : `${registration}-${index}`,
      registration: (data.JetPhotos && data.JetPhotos.Reg) || registration || "N/A",
      aircraftType: item.Aircraft || (data.FlightRadar && data.FlightRadar.Aircraft) || "N/A",
      airline: item.Airline || (data.FlightRadar && data.FlightRadar.Airline) || "N/A",
      photographer: item.Photographer || "N/A",
      location: item.Location || "N/A",
      imageUrl: item.Image || null,
      thumbnailUrl: item.Thumbnail || item.Image || null,
      photoPageUrl: item.Link || "N/A",
      photoDate: item.DateTaken || "N/A",
      uploadedDate: item.DateUploaded || "N/A",
      serial: item.Serial || "N/A",
    })),
    count: images.length,
  };
}

function safePathname(urlPath) {
  const decoded = decodeURIComponent(urlPath.split("?")[0]);
  const normalized = path.normalize(decoded).replace(/^(\.\.[/\\])+/, "");
  return normalized === path.sep ? "/index.html" : normalized;
}

function normalizePilotRows(pilots, observedAt) {
  const rows = [];
  for (const pilot of pilots) {
    const callsign = String(pilot.callsign || "").trim().toUpperCase();
    if (!callsign || typeof pilot.latitude !== "number" || typeof pilot.longitude !== "number") continue;
    rows.push({
      callsign,
      observedAt,
      lat: pilot.latitude,
      lon: pilot.longitude,
      altitude: pilot.altitude || null,
      groundspeed: pilot.groundspeed || null,
      heading: pilot.heading || null,
      squawk: pilot.transponder || null,
      aircraftCode: (pilot.flight_plan && (pilot.flight_plan.aircraft_short || pilot.flight_plan.aircraft)) || null,
    });
  }
  return rows;
}

async function fetchAndPersistVatsimData() {
  const upstream = await fetch(VATSIM_URL, {
    headers: { "user-agent": "vtracker-local/1.0" },
  });
  if (!upstream.ok) {
    throw new Error(`upstream_bad_status_${upstream.status}`);
  }

  const body = await upstream.text();
  const data = JSON.parse(body);
  const pilots = Array.isArray(data.pilots) ? data.pilots : [];
  const observedAt = Date.parse((data.general && data.general.update_timestamp) || "") || Date.now();

  persistRows(normalizePilotRows(pilots, observedAt), observedAt);
  latestVatsimBody = body;
  latestVatsimFetchedAt = Date.now();
  latestVatsimObservedAt = observedAt;
  vatsimPollError = null;

  return { body, observedAt };
}

async function ensureVatsimSnapshot(forceRefresh = false) {
  const cacheFresh = latestVatsimBody && (Date.now() - latestVatsimFetchedAt) <= VATSIM_CACHE_MAX_AGE_MS;
  if (!forceRefresh && cacheFresh) {
    return { body: latestVatsimBody, observedAt: latestVatsimObservedAt, cached: true };
  }

  if (vatsimPollInFlight) {
    const result = await vatsimPollInFlight;
    return { ...result, cached: false };
  }

  vatsimPollInFlight = fetchAndPersistVatsimData();
  try {
    const result = await vatsimPollInFlight;
    return { ...result, cached: false };
  } finally {
    vatsimPollInFlight = null;
  }
}

function startVatsimPolling() {
  async function poll() {
    try {
      await ensureVatsimSnapshot(true);
    } catch (error) {
      vatsimPollError = String(error);
      console.error("VATSIM poll failed:", error);
    }
  }

  poll();
  return setInterval(poll, VATSIM_POLL_INTERVAL_MS);
}

async function handleApi(req, res) {
  const url = new URL(req.url, `http://${HOST}:${PORT}`);

  if (url.pathname === "/api/health") {
    sendJson(res, 200, {
      ok: true,
      service: "vtracker-local",
      now: Date.now(),
      vatsim: {
        lastFetchedAt: latestVatsimFetchedAt || null,
        lastObservedAt: latestVatsimObservedAt || null,
        pollError: vatsimPollError,
        polling: Boolean(vatsimPollInFlight),
      },
    });
    return true;
  }

  if (url.pathname === "/api/history") {
    const callsign = String(url.searchParams.get("callsign") || "").trim().toUpperCase();
    const limit = Math.max(50, Math.min(5000, Number(url.searchParams.get("limit") || 2000)));
    const before = Number(url.searchParams.get("before") || 0);
    let points = Array.isArray(aircraftHistory[callsign]) ? aircraftHistory[callsign] : [];
    if (before > 0) points = points.filter((point) => point.observed_at < before);
    if (points.length > limit) points = points.slice(points.length - limit);
    sendJson(res, 200, {
      callsign,
      points,
    });
    return true;
  }

  if (url.pathname === "/api/vatsim") {
    try {
      const { body } = await ensureVatsimSnapshot(false);
      res.writeHead(200, {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Access-Control-Allow-Origin": "*",
      });
      res.end(body);
    } catch (error) {
      sendJson(res, 502, { error: "upstream_fetch_failed", detail: String(error) });
    }
    return true;
  }

  if (url.pathname === "/api/jetphotos") {
    const registration = String(url.searchParams.get("registration") || "").trim().toUpperCase();
    if (!registration) {
      sendJson(res, 400, { error: "registration_required" });
      return true;
    }

    const upstreamUrl = new URL(JETAPI_URL);
    upstreamUrl.searchParams.set("reg", registration);

    try {
      const upstream = await fetch(upstreamUrl, {
        headers: {
          "user-agent": "vtracker-local/1.0",
          "accept": "application/json,text/plain,*/*",
        },
      });
      if (!upstream.ok) {
        sendJson(res, 200, { photos: [], count: 0, unavailable: true, status: upstream.status });
        return true;
      }
      const data = await upstream.json();
      sendJson(res, 200, mapJetApiResponse(data, registration));
    } catch (error) {
      sendJson(res, 200, { photos: [], count: 0, unavailable: true });
    }
    return true;
  }

  return false;
}

const server = http.createServer(async (req, res) => {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("Method not allowed");
    return;
  }

  if (await handleApi(req, res)) return;

  let reqPath = safePathname(req.url || "/");
  if (reqPath === "/") reqPath = "/index.html";

  const filePath = path.join(ROOT, reqPath);
  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("Forbidden");
    return;
  }

  sendFile(res, filePath);
});

loadPersistedData();
const vatsimPollTimer = startVatsimPolling();

server.listen(PORT, HOST, () => {
  console.log(`vtracker local server ready on http://${HOST}:${PORT}`);
});

process.on("SIGINT", () => {
  clearInterval(vatsimPollTimer);
  process.exit(0);
});
