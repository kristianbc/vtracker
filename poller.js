const fs = require("fs");
const path = require("path");

const DEFAULT_VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json";
const DEFAULT_POLL_INTERVAL_MS = 5000;

let lastCleanupAt = 0;
let pollInFlight = false;
let lastObservedAt = 0;
let latestPositionsSeen = new Set();
let lastPersistedPointByCallsign = new Map();
let sessionIdByCallsign = new Map();

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/);
  for (const line of lines) {
    if (!line || /^\s*#/.test(line)) continue;
    const match = line.match(/^\s*([^=]+?)\s*=\s*(.*)\s*$/);
    if (!match) continue;
    const key = match[1];
    let value = match[2];
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (!(key in process.env)) process.env[key] = value;
  }
}

function initEnv() {
  loadEnvFile(path.join(__dirname, ".env"));
  loadEnvFile(path.join(__dirname, ".env.local"));
}

function requireEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) throw new Error(`missing_env_${name}`);
  return value;
}

function getEnv() {
  return {
    VATSIM_URL: String(process.env.VATSIM_URL || DEFAULT_VATSIM_URL).trim(),
    SUPABASE_URL: requireEnv("SUPABASE_URL"),
    SUPABASE_SERVICE_ROLE_KEY: requireEnv("SUPABASE_SERVICE_ROLE_KEY"),
  };
}

function getPollIntervalMs() {
  const raw = Number(process.env.POLL_INTERVAL_MS || process.env.VATSIM_POLL_INTERVAL_MS || DEFAULT_POLL_INTERVAL_MS);
  if (!Number.isFinite(raw)) return DEFAULT_POLL_INTERVAL_MS;
  return Math.max(1000, raw);
}

function getHistoryRetentionMs() {
  const raw = Number(process.env.HISTORY_RETENTION_MS || 12 * 60 * 60 * 1000);
  if (!Number.isFinite(raw)) return 12 * 60 * 60 * 1000;
  return Math.max(60 * 60 * 1000, raw);
}

function getLatestRetentionMs() {
  const raw = Number(process.env.LATEST_RETENTION_MS || 6 * 60 * 60 * 1000);
  if (!Number.isFinite(raw)) return 6 * 60 * 60 * 1000;
  return Math.max(30 * 60 * 1000, raw);
}

function getHistorySampleMs() {
  const raw = Number(process.env.HISTORY_SAMPLE_MS || 30 * 1000);
  if (!Number.isFinite(raw)) return 30 * 1000;
  return Math.max(5 * 1000, raw);
}

function getGroundHistorySampleMs() {
  const raw = Number(process.env.GROUND_HISTORY_SAMPLE_MS || 5 * 1000);
  if (!Number.isFinite(raw)) return 5 * 1000;
  return Math.max(2 * 1000, raw);
}

function getTerminalHistorySampleMs() {
  const raw = Number(process.env.TERMINAL_HISTORY_SAMPLE_MS || 10 * 1000);
  if (!Number.isFinite(raw)) return 10 * 1000;
  return Math.max(5 * 1000, raw);
}

function getLowAltitudeHistorySampleMs() {
  const raw = Number(process.env.LOW_ALTITUDE_HISTORY_SAMPLE_MS || 15 * 1000);
  if (!Number.isFinite(raw)) return 15 * 1000;
  return Math.max(5 * 1000, raw);
}

function getCruiseHistorySampleMs() {
  const raw = Number(process.env.CRUISE_HISTORY_SAMPLE_MS || getHistorySampleMs());
  if (!Number.isFinite(raw)) return getHistorySampleMs();
  return Math.max(15 * 1000, raw);
}

function supabaseBaseUrl(env) {
  return String(env.SUPABASE_URL).replace(/\/+$/, "") + "/rest/v1";
}

function supabaseHeaders(env, extra = {}) {
  const key = String(env.SUPABASE_SERVICE_ROLE_KEY || "");
  return {
    apikey: key,
    Authorization: `Bearer ${key}`,
    ...extra,
  };
}

function buildSupabaseUrl(env, table, query = {}) {
  const url = new URL(`${supabaseBaseUrl(env)}/${table}`);
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") continue;
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

async function supabaseJsonRequest(env, method, table, options = {}) {
  const headers = supabaseHeaders(env, options.headers || {});
  if (options.body !== undefined) headers["Content-Type"] = "application/json";

  const response = await fetch(buildSupabaseUrl(env, table, options.query), {
    method,
    headers,
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    const detail = text ? `: ${text}` : "";
    throw new Error(`supabase_${table}_${method}_${response.status}${detail}`);
  }

  if (response.status === 204) return [];
  const text = await response.text();
  return text ? JSON.parse(text) : [];
}

async function bootstrapActiveSessions(env) {
  if (sessionIdByCallsign.size) return;
  const rows = await supabaseJsonRequest(env, "GET", "latest_positions", {
    query: {
      select: "callsign,session_id",
      limit: 5000,
    },
  });
  for (const row of rows) {
    const callsign = String(row.callsign || "").trim().toUpperCase();
    const sessionId = Number(row.session_id || 0);
    if (callsign && sessionId > 0) sessionIdByCallsign.set(callsign, sessionId);
  }
}

function normalizePilotRows(pilots, observedAt) {
  const rows = [];
  for (const pilot of pilots) {
    const callsign = String(pilot.callsign || "").trim().toUpperCase();
    if (!callsign || typeof pilot.latitude !== "number" || typeof pilot.longitude !== "number") continue;
    rows.push({
      callsign,
      session_id: 0,
      observed_at: observedAt,
      lat: pilot.latitude,
      lon: pilot.longitude,
      altitude: pilot.altitude || null,
      groundspeed: pilot.groundspeed || null,
      heading: pilot.heading || null,
      squawk: pilot.transponder || null,
      aircraft_code: (pilot.flight_plan && (pilot.flight_plan.aircraft_short || pilot.flight_plan.aircraft)) || null,
    });
  }
  return rows;
}

function assignSessionIds(rows, observedAt) {
  for (const row of rows) {
    let sessionId = sessionIdByCallsign.get(row.callsign);
    if (!sessionId) {
      sessionId = observedAt;
      sessionIdByCallsign.set(row.callsign, sessionId);
    }
    row.session_id = sessionId;
  }
}

function angularDifference(a, b) {
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  let diff = Math.abs(a - b) % 360;
  if (diff > 180) diff = 360 - diff;
  return diff;
}

function positionChanged(row, previous) {
  return Number(row.lat) !== Number(previous.lat) || Number(row.lon) !== Number(previous.lon);
}

function getAdaptiveHistorySampleMs(row, previous) {
  const altitude = Number(row.altitude || 0);
  const groundspeed = Number(row.groundspeed || 0);
  const headingChange = previous ? angularDifference(Number(row.heading || 0), Number(previous.heading || 0)) : 0;
  const altitudeChange = previous ? Math.abs(Number(row.altitude || 0) - Number(previous.altitude || 0)) : 0;
  const speedChange = previous ? Math.abs(Number(row.groundspeed || 0) - Number(previous.groundspeed || 0)) : 0;

  if (groundspeed < 50 || altitude < 1000) return getGroundHistorySampleMs();
  if (altitude < 5000) return getTerminalHistorySampleMs();
  if (altitude < 10000) return getLowAltitudeHistorySampleMs();
  if (headingChange >= 8 || altitudeChange >= 500 || speedChange >= 30) return getLowAltitudeHistorySampleMs();
  return getCruiseHistorySampleMs();
}

function shouldPersistHistoryPoint(row) {
  const previous = lastPersistedPointByCallsign.get(row.callsign);
  if (!previous) {
    lastPersistedPointByCallsign.set(row.callsign, {
      observed_at: row.observed_at,
      lat: row.lat,
      lon: row.lon,
      altitude: row.altitude,
      groundspeed: row.groundspeed,
      heading: row.heading,
    });
    return true;
  }

  if (!positionChanged(row, previous)) return false;

  const sampleMs = getAdaptiveHistorySampleMs(row, previous);
  const diffMs = row.observed_at - previous.observed_at;
  const headingChanged = angularDifference(Number(row.heading || 0), Number(previous.heading || 0)) >= 8;
  const altitudeDelta = Math.abs(Number(row.altitude || 0) - Number(previous.altitude || 0));
  const speedDelta = Math.abs(Number(row.groundspeed || 0) - Number(previous.groundspeed || 0));
  const altitude = Number(row.altitude || 0);
  const groundspeed = Number(row.groundspeed || 0);

  // Preserve surface and terminal geometry aggressively: if it moved, keep it.
  if (groundspeed < 80 || altitude < 3000) {
    lastPersistedPointByCallsign.set(row.callsign, {
      observed_at: row.observed_at,
      lat: row.lat,
      lon: row.lon,
      altitude: row.altitude,
      groundspeed: row.groundspeed,
      heading: row.heading,
    });
    return true;
  }

  if (!headingChanged && altitudeDelta < 100 && speedDelta < 5) {
    if (diffMs < 120 * 1000) return false;
  } else if (altitude > 30000) {
    if (diffMs < 30 * 1000) return false;
  } else if (altitude > 20000) {
    if (diffMs < 20 * 1000) return false;
  } else if (altitude > 15000) {
    if (diffMs < 10 * 1000) return false;
  } else if (altitude > 10000) {
    if (diffMs < 7 * 1000) return false;
  } else if (diffMs < sampleMs) {
    return false;
  }

  lastPersistedPointByCallsign.set(row.callsign, {
    observed_at: row.observed_at,
    lat: row.lat,
    lon: row.lon,
    altitude: row.altitude,
    groundspeed: row.groundspeed,
    heading: row.heading,
  });
  return true;
}

function selectTrajectoryRows(rows) {
  const filtered = [];
  for (const row of rows) {
    if (shouldPersistHistoryPoint(row)) filtered.push(row);
  }
  return filtered;
}

async function persistLatestPositions(env, rows) {
  if (!rows.length) return;
  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize).map((row) => ({
      callsign: row.callsign,
      session_id: row.session_id,
      observed_at: row.observed_at,
      lat: row.lat,
      lon: row.lon,
    }));

    await supabaseJsonRequest(env, "POST", "latest_positions", {
      query: { on_conflict: "callsign" },
      body: chunk,
      headers: {
        Prefer: "resolution=merge-duplicates,return=minimal",
      },
    });
  }
}

async function persistTrajectoryPoints(env, rows) {
  if (!rows.length) return;
  const chunkSize = 250;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseJsonRequest(env, "POST", "aircraft_points", {
      query: { on_conflict: "callsign,session_id,observed_at,lat,lon" },
      body: chunk,
      headers: {
        Prefer: "resolution=ignore-duplicates,return=minimal",
      },
    });
  }
}

async function cleanupEndedFlights(env, rows) {
  const currentCallsigns = new Set(rows.map((row) => row.callsign));
  if (!currentCallsigns.size) return;

  if (!latestPositionsSeen.size) {
    latestPositionsSeen = currentCallsigns;
    return;
  }

  const endedCallsigns = [];
  for (const callsign of latestPositionsSeen) {
    if (!currentCallsigns.has(callsign)) endedCallsigns.push(callsign);
  }
  latestPositionsSeen = currentCallsigns;
  for (const callsign of endedCallsigns) {
    lastPersistedPointByCallsign.delete(callsign);
    sessionIdByCallsign.delete(callsign);
  }

  if (!endedCallsigns.length) return;

  const chunkSize = 100;
  for (let i = 0; i < endedCallsigns.length; i += chunkSize) {
    const chunk = endedCallsigns.slice(i, i + chunkSize);
    const filter = `in.(${chunk.map((value) => `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`).join(",")})`;
    await supabaseJsonRequest(env, "DELETE", "latest_positions", {
      query: { callsign: filter },
      headers: { Prefer: "return=minimal" },
    });
  }
}

async function cleanupStaleData(env, nowMs) {
  if (lastCleanupAt && nowMs - lastCleanupAt < 60 * 60 * 1000) return;
  lastCleanupAt = nowMs;

  const staleLatestThreshold = nowMs - getLatestRetentionMs();
  const staleHistoryThreshold = nowMs - getHistoryRetentionMs();

  await supabaseJsonRequest(env, "DELETE", "latest_positions", {
    query: { observed_at: `lt.${staleLatestThreshold}` },
    headers: { Prefer: "return=minimal" },
  });
  await supabaseJsonRequest(env, "DELETE", "aircraft_points", {
    query: { observed_at: `lt.${staleHistoryThreshold}` },
    headers: { Prefer: "return=minimal" },
  });
}

async function ingestSnapshot(env) {
  const upstream = await fetch(env.VATSIM_URL, {
    headers: { "user-agent": "vtracker-poller/1.0" },
  });
  if (!upstream.ok) throw new Error(`upstream_bad_status_${upstream.status}`);

  const data = await upstream.json();
  const pilots = Array.isArray(data.pilots) ? data.pilots : [];
  const observedAt = Date.parse((data.general && data.general.update_timestamp) || "") || Date.now();
  const rows = normalizePilotRows(pilots, observedAt);
  await bootstrapActiveSessions(env);
  assignSessionIds(rows, observedAt);
  const trajectoryRows = selectTrajectoryRows(rows);

  if (observedAt <= lastObservedAt) {
    return { observedAt, rows: rows.length, persisted: false, skipped: true };
  }

  await persistLatestPositions(env, rows);
  await persistTrajectoryPoints(env, trajectoryRows);
  await cleanupEndedFlights(env, rows);
  await cleanupStaleData(env, observedAt);
  lastObservedAt = observedAt;

  return {
    observedAt,
    rows: rows.length,
    persisted: true,
    skipped: false,
    trajectoryRows: trajectoryRows.length,
  };
}

async function runPoll(env) {
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    const result = await ingestSnapshot(env);
    const stamp = new Date(result.observedAt).toISOString();
    if (result.skipped) {
      console.log(`[poller] skipped snapshot ${stamp} rows=${result.rows}`);
    } else {
      console.log(`[poller] persisted snapshot ${stamp} rows=${result.rows} trail_rows=${result.trajectoryRows || 0}`);
    }
  } catch (error) {
    console.error("[poller] ingest failed", error);
  } finally {
    pollInFlight = false;
  }
}

async function main() {
  initEnv();
  const env = getEnv();
  const intervalMs = getPollIntervalMs();

  console.log(`[poller] starting interval=${intervalMs}ms vatsim=${env.VATSIM_URL}`);
  await runPoll(env);
  const timer = setInterval(() => {
    runPoll(env);
  }, intervalMs);

  function shutdown(signal) {
    clearInterval(timer);
    console.log(`[poller] stopping on ${signal}`);
    process.exit(0);
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((error) => {
  console.error("[poller] fatal", error);
  process.exit(1);
});
