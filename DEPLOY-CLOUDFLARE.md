# Cloudflare Deploy

This project is prepared for:

- Cloudflare Worker for the frontend and read API
- Supabase for persistent storage
- `poller.js` as the always-on ingest process for production-quality track density

## What gets deployed

- Static frontend: `vatsim-tracker.html`, `index.html`, `img/`
- Worker API routes:
  - `/api/vatsim`
  - `/api/history?callsign=...`
  - `/api/health`
- Supabase schema: `schema.sql`
- Worker entry: `worker.js`
- Poller entry: `poller.js`

## One-time setup

1. Push this folder to GitHub.
2. In Cloudflare, create a Worker from this repo or deploy it with `wrangler deploy`.
3. Create a Supabase project.
4. Run `schema.sql` in Supabase SQL Editor.
5. Set `SUPABASE_SERVICE_ROLE_KEY` as a Cloudflare Worker secret.
6. Set `SUPABASE_URL` in `wrangler.jsonc`.
7. Deploy the worker.
8. Run `poller.js` on an always-on host with:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - optional `POLL_INTERVAL_MS` default `5000`

## What changes in the app

- On Cloudflare, the frontend will use same-origin `/api/vatsim` instead of browser CORS proxies.
- Selecting an aircraft will request `/api/history` and load saved trajectory points from Supabase.
- On `file://`, the page still falls back to the old browser proxy fetch path.

## Notes

- The Worker cron is not enough for local-quality track density on its own.
- Use `poller.js` for continuous ingest in production.
- The frontend still keeps an in-memory copy for rendering, but refresh no longer loses everything once the history has been written to Supabase.
