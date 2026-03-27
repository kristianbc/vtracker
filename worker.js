const VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json";

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

function json(data, init = {}) {
  const headers = new Headers(init.headers || {});
  headers.set("content-type", "application/json; charset=utf-8");
  for (const [key, value] of Object.entries(corsHeaders())) headers.set(key, value);
  return new Response(JSON.stringify(data), { ...init, headers });
}

let lastPersistStatus = { phase: null, message: null, timestamp: 0 };
let lastCleanupAt = 0;
let schemaReady = false;
const JETAPI_URL = "https://www.jetapi.dev/api";
function recordPersistError(phase, error) {
  lastPersistStatus = {
    phase,
    message: error ? String(error) : null,
    timestamp: Date.now(),
  };
}

async function ensureSchema(db) {
  if (!db || schemaReady) return;
  await db.batch([
    db.prepare(
      `CREATE TABLE IF NOT EXISTS aircraft_points (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        callsign TEXT NOT NULL,
        observed_at INTEGER NOT NULL,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        altitude INTEGER,
        groundspeed INTEGER,
        heading REAL,
        squawk TEXT,
        aircraft_code TEXT
      )`
    ),
    db.prepare(
      `CREATE TABLE IF NOT EXISTS latest_positions (
        callsign TEXT PRIMARY KEY,
        observed_at INTEGER NOT NULL,
        lat REAL NOT NULL,
        lon REAL NOT NULL
      )`
    ),
    db.prepare(
      `CREATE TABLE IF NOT EXISTS ingested_snapshots (
        observed_at INTEGER PRIMARY KEY,
        created_at INTEGER NOT NULL
      )`
    ),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_aircraft_points_callsign_time ON aircraft_points (callsign, observed_at)"),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_aircraft_points_observed_at ON aircraft_points (observed_at)"),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_latest_positions_observed_at ON latest_positions (observed_at)"),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_ingested_snapshots_created_at ON ingested_snapshots (created_at)"),
  ]);
  schemaReady = true;
}

async function cleanupStaleData(db, nowMs) {
  if (!db) return;
  if (lastCleanupAt && nowMs - lastCleanupAt < 60 * 60 * 1000) return;
  lastCleanupAt = nowMs;

  const staleLatestThreshold = nowMs - 6 * 60 * 60 * 1000;
  const staleHistoryThreshold = nowMs - 48 * 60 * 60 * 1000;
  const staleSnapshotThreshold = nowMs - 7 * 24 * 60 * 60 * 1000;

  await db.batch([
    db.prepare("DELETE FROM latest_positions WHERE observed_at < ?1").bind(staleLatestThreshold),
    db.prepare("DELETE FROM aircraft_points WHERE observed_at < ?1").bind(staleHistoryThreshold),
    db.prepare("DELETE FROM ingested_snapshots WHERE created_at < ?1").bind(staleSnapshotThreshold),
  ]);
}

async function deleteFlightsByCallsigns(db, callsigns) {
  if (!db || !callsigns.length) return;
  const chunkSize = 100;
  for (let i = 0; i < callsigns.length; i += chunkSize) {
    const chunk = callsigns.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(", ");
    await db.batch([
      db
        .prepare(`DELETE FROM latest_positions WHERE callsign IN (${placeholders})`)
        .bind(...chunk),
      db
        .prepare(`DELETE FROM aircraft_points WHERE callsign IN (${placeholders})`)
        .bind(...chunk),
    ]);
  }
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

async function cleanupEndedFlights(db, rows) {
  if (!db) return;

  const currentCallsigns = new Set(rows.map((row) => row.callsign));
  if (!currentCallsigns.size) return;
  const result = await db.prepare("SELECT callsign FROM latest_positions").all();
  const endedCallsigns = [];

  for (const entry of result.results || []) {
    const callsign = String(entry.callsign || "").trim().toUpperCase();
    if (callsign && !currentCallsigns.has(callsign)) endedCallsigns.push(callsign);
  }

  await deleteFlightsByCallsigns(db, endedCallsigns);
}

async function claimSnapshotIngest(db, observedAt) {
  if (!db) return false;
  const result = await db
    .prepare("INSERT OR IGNORE INTO ingested_snapshots (observed_at, created_at) VALUES (?1, ?2)")
    .bind(observedAt, Date.now())
    .run();
  return Boolean(result && result.meta && result.meta.changes > 0);
}

function isLocalRequest(request) {
  const hostname = new URL(request.url).hostname;
  return hostname === "127.0.0.1" || hostname === "localhost";
}

async function persistLatestPositions(db, rows) {
  if (!db || !rows.length) return;
  const chunkSize = 20;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const placeholders = [];
    const bindings = [];
    for (const row of chunk) {
      placeholders.push("(?, ?, ?, ?)");
      bindings.push(row.callsign, row.observedAt, row.lat, row.lon);
    }

    await db
      .prepare(
        `INSERT INTO latest_positions (callsign, observed_at, lat, lon)
         VALUES ${placeholders.join(", ")}
         ON CONFLICT(callsign) DO UPDATE SET
           observed_at = excluded.observed_at,
           lat = excluded.lat,
           lon = excluded.lon`
      )
      .bind(...bindings)
      .run();
  }
}

async function persistTrajectoryPoints(db, rows) {
  if (!db || !rows.length) return;
  const chunkSize = 8;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);

    const pointPlaceholders = [];
    const pointBindings = [];
    for (const row of chunk) {
      pointPlaceholders.push("(?, ?, ?, ?, ?, ?, ?, ?, ?)");
      pointBindings.push(
        row.callsign,
        row.observedAt,
        row.lat,
        row.lon,
        row.altitude,
        row.groundspeed,
        row.heading,
        row.squawk,
        row.aircraftCode
      );
    }

    await db
      .prepare(
        `INSERT INTO aircraft_points (callsign, observed_at, lat, lon, altitude, groundspeed, heading, squawk, aircraft_code)
         VALUES ${pointPlaceholders.join(", ")}`
      )
      .bind(...pointBindings)
      .run();
  }
}

async function handleVatsim(request, env, ctx) {
  const sourceUrl = env.VATSIM_URL || VATSIM_URL;
  let upstream;

  try {
    upstream = await fetch(sourceUrl, {
      headers: { "user-agent": "vtracker-worker/1.0" },
      cf: { cacheTtl: 0, cacheEverything: false },
    });
  } catch (error) {
    return json({ error: "upstream_fetch_failed", detail: String(error) }, { status: 502 });
  }

  if (!upstream.ok) {
    return json({ error: "upstream_bad_status", status: upstream.status }, { status: 502 });
  }

  const data = await upstream.json();
  const pilots = Array.isArray(data.pilots) ? data.pilots : [];
  const observedAt = Date.parse((data.general && data.general.update_timestamp) || "") || Date.now();
  const rows = normalizePilotRows(pilots, observedAt);
  const skipPersistence = isLocalRequest(request);

  if (env.DB && !skipPersistence) {
    ctx.waitUntil(
      (async () => {
        await ensureSchema(env.DB).catch((error) => {
          recordPersistError("schema", error);
          console.error("ensureSchema failed", error);
          throw error;
        });
        const shouldPersist = await claimSnapshotIngest(env.DB, observedAt).catch((error) => {
          recordPersistError("snapshot_claim", error);
          console.error("claimSnapshotIngest failed", error);
          throw error;
        });
        if (!shouldPersist) return;

        await persistLatestPositions(env.DB, rows).catch((error) => {
          recordPersistError("latest_positions", error);
          console.error("persistLatestPositions failed", error);
          throw error;
        });
        await persistTrajectoryPoints(env.DB, rows).catch((error) => {
          recordPersistError("trajectory_points", error);
          console.error("persistTrajectoryPoints failed", error);
          throw error;
        });
        await cleanupEndedFlights(env.DB, rows).catch((error) => {
          recordPersistError("ended_flights_cleanup", error);
          console.error("cleanupEndedFlights failed", error);
          throw error;
        });
        await cleanupStaleData(env.DB, observedAt).catch((error) => {
          recordPersistError("cleanup", error);
          console.error("cleanupStaleData failed", error);
          throw error;
        });
      })()
    );
  }

  return json(data, {
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

async function handleHistory(request, env) {
  if (isLocalRequest(request)) {
    return json({
      callsign: String(new URL(request.url).searchParams.get("callsign") || "").trim().toUpperCase(),
      points: [],
    }, {
      headers: {
        "Cache-Control": "no-store, no-cache, must-revalidate",
      },
    });
  }

  if (!env.DB) return json({ error: "database_not_bound" }, { status: 503 });

  const url = new URL(request.url);
  const callsign = String(url.searchParams.get("callsign") || "").trim().toUpperCase();
  const limit = Math.max(50, Math.min(5000, Number(url.searchParams.get("limit") || 2000)));
  const before = Number(url.searchParams.get("before") || 0);

  if (!callsign) return json({ error: "callsign_required" }, { status: 400 });

  const result = before > 0
    ? await env.DB
        .prepare(
          "SELECT observed_at, lat, lon, altitude, groundspeed, heading, squawk, aircraft_code FROM aircraft_points WHERE callsign = ?1 AND observed_at < ?2 ORDER BY observed_at DESC LIMIT ?3"
        )
        .bind(callsign, before, limit)
        .all()
    : await env.DB
        .prepare(
          "SELECT observed_at, lat, lon, altitude, groundspeed, heading, squawk, aircraft_code FROM aircraft_points WHERE callsign = ?1 ORDER BY observed_at DESC LIMIT ?2"
        )
        .bind(callsign, limit)
        .all();

  return json({
    callsign,
    points: (result.results || []).slice().reverse(),
  }, {
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

function handleHealth() {
  return json({
    ok: true,
    service: "vtracker-worker",
    now: Date.now(),
    persist: lastPersistStatus,
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

async function handleJetPhotos(request) {
  const url = new URL(request.url);
  const registration = String(url.searchParams.get("registration") || "").trim().toUpperCase();
  if (!registration) return json({ error: "registration_required" }, { status: 400 });

  const upstreamUrl = new URL(JETAPI_URL);
  upstreamUrl.searchParams.set("reg", registration);

  let upstream;
  try {
    upstream = await fetch(upstreamUrl.toString(), {
      headers: {
        "user-agent": "vtracker-worker/1.0",
        "accept": "application/json,text/plain,*/*",
      },
      cf: { cacheTtl: 3600, cacheEverything: true },
    });
  } catch (error) {
    return json({ photos: [], count: 0, unavailable: true }, {
      headers: {
        "Cache-Control": "public, max-age=300",
      },
    });
  }

  if (!upstream.ok) {
    return json({ photos: [], count: 0, unavailable: true, status: upstream.status }, {
      headers: {
        "Cache-Control": "public, max-age=300",
      },
    });
  }

  const data = await upstream.json();
  return json(mapJetApiResponse(data, registration), {
    headers: {
      "Cache-Control": "public, max-age=3600",
    },
  });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    const url = new URL(request.url);

    if (url.pathname === "/api/health") {
      return handleHealth();
    }

    if (url.pathname === "/api/vatsim") {
      return handleVatsim(request, env, ctx);
    }

    if (url.pathname === "/api/history") {
      return handleHistory(request, env);
    }

    if (url.pathname === "/api/jetphotos") {
      return handleJetPhotos(request);
    }

    return env.ASSETS.fetch(request);
  },
};
