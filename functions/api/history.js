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

export async function onRequestOptions() {
  return new Response(null, { headers: corsHeaders() });
}

export async function onRequestGet(context) {
  if (!context.env.DB) {
    return json({ error: "database_not_bound" }, { status: 503 });
  }

  const url = new URL(context.request.url);
  const callsign = String(url.searchParams.get("callsign") || "").trim().toUpperCase();
  const limit = Math.max(50, Math.min(5000, Number(url.searchParams.get("limit") || 2000)));

  if (!callsign) {
    return json({ error: "callsign_required" }, { status: 400 });
  }

  const result = await context.env.DB
    .prepare(
      "SELECT observed_at, lat, lon, altitude, groundspeed, heading, squawk, aircraft_code FROM aircraft_points WHERE callsign = ?1 ORDER BY observed_at DESC LIMIT ?2"
    )
    .bind(callsign, limit)
    .all();

  const points = (result.results || []).slice().reverse();
  return json({
    callsign,
    points,
  }, {
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}
