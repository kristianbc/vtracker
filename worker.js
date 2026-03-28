const VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json";
const JETAPI_URL = "https://www.jetapi.dev/api";

let lastPersistStatus = { phase: null, message: null, timestamp: 0 };
let lastCleanupAt = 0;

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
  };
}

function json(data, init = {}) {
  const headers = new Headers(init.headers || {});
  headers.set("content-type", "application/json; charset=utf-8");
  for (const [key, value] of Object.entries(corsHeaders())) headers.set(key, value);
  return new Response(JSON.stringify(data), { ...init, headers });
}

function recordPersistError(phase, error) {
  lastPersistStatus = {
    phase,
    message: error ? String(error) : null,
    timestamp: Date.now(),
  };
}

function hasSupabaseConfig(env) {
  return Boolean(env && env.SUPABASE_URL && env.SUPABASE_SERVICE_ROLE_KEY);
}

function supabaseBaseUrl(env) {
  return String(env.SUPABASE_URL || "").replace(/\/+$/, "") + "/rest/v1";
}

function supabaseHeaders(env, extra = {}) {
  const key = String(env.SUPABASE_SERVICE_ROLE_KEY || "");
  const headers = {
    apikey: key,
    Authorization: `Bearer ${key}`,
    ...extra,
  };
  return headers;
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

function normalizePilotRows(pilots, observedAt) {
  const rows = [];
  for (const pilot of pilots) {
    const callsign = String(pilot.callsign || "").trim().toUpperCase();
    if (!callsign || typeof pilot.latitude !== "number" || typeof pilot.longitude !== "number") continue;
    rows.push({
      callsign,
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

function isLocalRequest(request) {
  const hostname = new URL(request.url).hostname;
  return hostname === "127.0.0.1" || hostname === "localhost";
}

function quotePostgrestValue(value) {
  return `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function postgrestInFilter(values) {
  return `(${values.map(quotePostgrestValue).join(",")})`;
}

async function claimSnapshotIngest(env, observedAt) {
  if (!hasSupabaseConfig(env)) return false;

  const existing = await supabaseJsonRequest(env, "GET", "ingested_snapshots", {
    query: {
      select: "observed_at",
      observed_at: `eq.${observedAt}`,
      limit: 1,
    },
  });
  if (existing.length) return false;

  try {
    await supabaseJsonRequest(env, "POST", "ingested_snapshots", {
      body: [{ observed_at: observedAt, created_at: Date.now() }],
      headers: {
        Prefer: "return=minimal",
      },
    });
    return true;
  } catch (error) {
    if (String(error).includes("_409")) return false;
    throw error;
  }
}

async function persistLatestPositions(env, rows) {
  if (!rows.length) return;
  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize).map((row) => ({
      callsign: row.callsign,
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
      query: { on_conflict: "callsign,observed_at,lat,lon" },
      body: chunk,
      headers: {
        Prefer: "resolution=ignore-duplicates,return=minimal",
      },
    });
  }
}

async function deleteFlightsByCallsigns(env, callsigns) {
  if (!callsigns.length) return;
  const chunkSize = 100;
  for (let i = 0; i < callsigns.length; i += chunkSize) {
    const chunk = callsigns.slice(i, i + chunkSize);
    const filter = `in.${postgrestInFilter(chunk)}`;
    await supabaseJsonRequest(env, "DELETE", "latest_positions", {
      query: { callsign: filter },
      headers: { Prefer: "return=minimal" },
    });
    await supabaseJsonRequest(env, "DELETE", "aircraft_points", {
      query: { callsign: filter },
      headers: { Prefer: "return=minimal" },
    });
  }
}

async function cleanupEndedFlights(env, rows) {
  const currentCallsigns = new Set(rows.map((row) => row.callsign));
  if (!currentCallsigns.size) return;

  const existing = await supabaseJsonRequest(env, "GET", "latest_positions", {
    query: { select: "callsign", limit: 5000 },
  });

  const endedCallsigns = [];
  for (const entry of existing) {
    const callsign = String(entry.callsign || "").trim().toUpperCase();
    if (callsign && !currentCallsigns.has(callsign)) endedCallsigns.push(callsign);
  }

  await deleteFlightsByCallsigns(env, endedCallsigns);
}

async function cleanupStaleData(env, nowMs) {
  if (lastCleanupAt && nowMs - lastCleanupAt < 60 * 60 * 1000) return;
  lastCleanupAt = nowMs;

  const staleLatestThreshold = nowMs - 6 * 60 * 60 * 1000;
  const staleHistoryThreshold = nowMs - 48 * 60 * 60 * 1000;
  const staleSnapshotThreshold = nowMs - 7 * 24 * 60 * 60 * 1000;

  await supabaseJsonRequest(env, "DELETE", "latest_positions", {
    query: { observed_at: `lt.${staleLatestThreshold}` },
    headers: { Prefer: "return=minimal" },
  });
  await supabaseJsonRequest(env, "DELETE", "aircraft_points", {
    query: { observed_at: `lt.${staleHistoryThreshold}` },
    headers: { Prefer: "return=minimal" },
  });
  await supabaseJsonRequest(env, "DELETE", "ingested_snapshots", {
    query: { created_at: `lt.${staleSnapshotThreshold}` },
    headers: { Prefer: "return=minimal" },
  });
}

async function ingestSnapshot(env) {
  const sourceUrl = env.VATSIM_URL || VATSIM_URL;
  const upstream = await fetch(sourceUrl, {
    headers: { "user-agent": "vtracker-worker/1.0" },
    cf: { cacheTtl: 0, cacheEverything: false },
  });

  if (!upstream.ok) {
    throw new Error(`upstream_bad_status_${upstream.status}`);
  }

  const data = await upstream.json();
  const pilots = Array.isArray(data.pilots) ? data.pilots : [];
  const observedAt = Date.parse((data.general && data.general.update_timestamp) || "") || Date.now();
  const rows = normalizePilotRows(pilots, observedAt);

  if (!hasSupabaseConfig(env)) return data;

  const shouldPersist = await claimSnapshotIngest(env, observedAt);
  if (!shouldPersist) return data;

  await persistLatestPositions(env, rows);
  await persistTrajectoryPoints(env, rows);
  await cleanupEndedFlights(env, rows);
  await cleanupStaleData(env, observedAt);
  return data;
}

async function handleVatsim(request, env, ctx) {
  const skipPersistence = isLocalRequest(request);

  let data;
  try {
    if (skipPersistence) {
      const sourceUrl = env.VATSIM_URL || VATSIM_URL;
      const upstream = await fetch(sourceUrl, {
        headers: { "user-agent": "vtracker-worker/1.0" },
        cf: { cacheTtl: 0, cacheEverything: false },
      });
      if (!upstream.ok) return json({ error: "upstream_bad_status", status: upstream.status }, { status: 502 });
      data = await upstream.json();
    } else {
      data = await ingestSnapshot(env);
    }
  } catch (error) {
    recordPersistError("vatsim_ingest", error);
    return json({ error: "upstream_fetch_failed", detail: String(error) }, { status: 502 });
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

  if (!hasSupabaseConfig(env)) return json({ error: "supabase_not_configured" }, { status: 503 });

  const url = new URL(request.url);
  const callsign = String(url.searchParams.get("callsign") || "").trim().toUpperCase();
  const limit = Math.max(50, Math.min(5000, Number(url.searchParams.get("limit") || 2000)));
  const before = Number(url.searchParams.get("before") || 0);

  if (!callsign) return json({ error: "callsign_required" }, { status: 400 });

  const query = {
    select: "observed_at,lat,lon,altitude,groundspeed,heading,squawk,aircraft_code",
    callsign: `eq.${callsign}`,
    order: "observed_at.desc",
    limit,
  };
  if (before > 0) query.observed_at = `lt.${before}`;

  const points = await supabaseJsonRequest(env, "GET", "aircraft_points", { query });

  return json({
    callsign,
    points: points.slice().reverse(),
  }, {
    headers: {
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

function handleHealth(env) {
  return json({
    ok: true,
    service: "vtracker-worker",
    now: Date.now(),
    storage: {
      provider: hasSupabaseConfig(env) ? "supabase" : "none",
    },
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
        accept: "application/json,text/plain,*/*",
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
      return handleHealth(env);
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
  async scheduled(controller, env, ctx) {
    if (!hasSupabaseConfig(env)) return;
    ctx.waitUntil(
      ingestSnapshot(env).catch((error) => {
        recordPersistError("scheduled_ingest", error);
        console.error("scheduled ingest failed", error);
      })
    );
  },
};
