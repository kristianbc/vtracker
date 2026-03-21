export function onRequestGet() {
  return Response.json({
    ok: true,
    service: "vtracker-pages",
    now: Date.now(),
  });
}
