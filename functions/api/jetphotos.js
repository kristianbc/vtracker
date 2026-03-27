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

const JETAPI_URL = "https://www.jetapi.dev/api";

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

export async function onRequestOptions() {
  return new Response(null, { headers: corsHeaders() });
}

export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const registration = String(url.searchParams.get("registration") || "").trim().toUpperCase();
  if (!registration) return json({ error: "registration_required" }, { status: 400 });

  const upstreamUrl = new URL(JETAPI_URL);
  upstreamUrl.searchParams.set("reg", registration);

  let upstream;
  try {
    upstream = await fetch(upstreamUrl.toString(), {
      headers: {
        "user-agent": "vtracker-pages/1.0",
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
