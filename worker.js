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

async function persistSnapshot(db, pilots, observedAt) {
  if (!db || !pilots.length) return;

  for (const pilot of pilots) {
    const callsign = String(pilot.callsign || "").trim().toUpperCase();
    if (!callsign || typeof pilot.latitude !== "number" || typeof pilot.longitude !== "number") continue;

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
}

async function handleVatsim(env, ctx) {
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

  if (env.DB) ctx.waitUntil(persistSnapshot(env.DB, pilots, observedAt));

  return json(data, {
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

async function handleHistory(request, env) {
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

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    const url = new URL(request.url);

    if (url.pathname === "/api/health") {
      return json({ ok: true, service: "vtracker-worker", now: Date.now() });
    }

    if (url.pathname === "/api/vatsim") {
      return handleVatsim(env, ctx);
    }

    if (url.pathname === "/api/history") {
      return handleHistory(request, env);
    }

    return env.ASSETS.fetch(request);
  },
};
