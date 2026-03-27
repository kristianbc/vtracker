const VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json";
let schemaReady = false;

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

async function persistSnapshot(db, pilots, observedAt) {
  if (!db || !pilots.length) return;
  await ensureSchema(db);
  const claim = await db
    .prepare("INSERT OR IGNORE INTO ingested_snapshots (observed_at, created_at) VALUES (?1, ?2)")
    .bind(observedAt, Date.now())
    .run();
  if (!claim || !claim.meta || claim.meta.changes < 1) return;

  const currentCallsigns = new Set();
  for (const pilot of pilots) {
    const callsign = String(pilot.callsign || "").trim().toUpperCase();
    if (!callsign || typeof pilot.latitude !== "number" || typeof pilot.longitude !== "number") continue;
    currentCallsigns.add(callsign);

    const latest = await db
      .prepare("SELECT lat, lon FROM latest_positions WHERE callsign = ?1")
      .bind(callsign)
      .first();

    if (latest && latest.lat === pilot.latitude && latest.lon === pilot.longitude) continue;

    await db.batch([
      db
        .prepare(
          "INSERT INTO aircraft_points (callsign, observed_at, lat, lon, altitude, groundspeed, heading, squawk, aircraft_code) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
        )
        .bind(
          callsign,
          observedAt,
          pilot.latitude,
          pilot.longitude,
          pilot.altitude || null,
          pilot.groundspeed || null,
          pilot.heading || null,
          pilot.transponder || null,
          ((pilot.flight_plan && (pilot.flight_plan.aircraft_short || pilot.flight_plan.aircraft)) || null)
        ),
      db
        .prepare(
          "INSERT INTO latest_positions (callsign, observed_at, lat, lon) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(callsign) DO UPDATE SET observed_at = excluded.observed_at, lat = excluded.lat, lon = excluded.lon"
        )
      .bind(callsign, observedAt, pilot.latitude, pilot.longitude),
    ]);
  }

  if (!currentCallsigns.size) return;

  const existing = await db.prepare("SELECT callsign FROM latest_positions").all();
  const endedCallsigns = [];
  for (const entry of existing.results || []) {
    const callsign = String(entry.callsign || "").trim().toUpperCase();
    if (callsign && !currentCallsigns.has(callsign)) endedCallsigns.push(callsign);
  }

  const chunkSize = 100;
  for (let i = 0; i < endedCallsigns.length; i += chunkSize) {
    const chunk = endedCallsigns.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(", ");
    await db.batch([
      db.prepare(`DELETE FROM latest_positions WHERE callsign IN (${placeholders})`).bind(...chunk),
      db.prepare(`DELETE FROM aircraft_points WHERE callsign IN (${placeholders})`).bind(...chunk),
    ]);
  }
}

export async function onRequestOptions() {
  return new Response(null, { headers: corsHeaders() });
}

export async function onRequestGet(context) {
  const sourceUrl = context.env.VATSIM_URL || VATSIM_URL;
  let upstream;

  try {
    upstream = await fetch(sourceUrl, {
      headers: { "user-agent": "vtracker-pages/1.0" },
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

  if (context.env.DB) {
    context.waitUntil(persistSnapshot(context.env.DB, pilots, observedAt));
  }

  return json(data, {
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}
