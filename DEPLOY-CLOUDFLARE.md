# Cloudflare Deploy

This project is prepared for Cloudflare Workers static assets + D1.

## What gets deployed

- Static frontend: `vatsim-tracker.html`, `index.html`, `img/`
- Worker API routes:
  - `/api/vatsim`
  - `/api/history?callsign=...`
  - `/api/health`
- D1 schema: `schema.sql`
- Worker entry: `worker.js`

## One-time setup

1. Push this folder to GitHub.
2. In Cloudflare, create a Worker from this repo or deploy it with `wrangler deploy`.
3. Create a D1 database named `vtracker-db`.
4. Bind the D1 database as `DB`.
5. Run `schema.sql` against that D1 database.
6. Deploy.

## What changes in the app

- On Cloudflare, the frontend will use same-origin `/api/vatsim` instead of browser CORS proxies.
- Selecting an aircraft will request `/api/history` and load saved trajectory points from D1.
- On `file://`, the page still falls back to the old browser proxy fetch path.

## Notes

- History is permanent in D1 until you delete it.
- The frontend still keeps an in-memory copy for rendering, but refresh no longer loses everything once the history has been written to D1.
