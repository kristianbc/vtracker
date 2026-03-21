# Cloudflare Deploy

This project is prepared for Cloudflare Pages + Pages Functions + D1.

## What gets deployed

- Static frontend: `vatsim-tracker.html`, `index.html`, `img/`
- API routes:
  - `/api/vatsim`
  - `/api/history?callsign=...`
  - `/api/health`
- D1 schema: `schema.sql`

## One-time setup

1. Push this folder to GitHub.
2. In Cloudflare, create a new Pages project from that repo.
3. Set:
   - Build command: none
   - Build output directory: `.`
4. Create a D1 database named `vtracker-db`.
5. In your Pages project, add a D1 binding:
   - Variable name: `DB`
   - Database: `vtracker-db`
6. Run `schema.sql` against that D1 database.
7. Redeploy the Pages project.

## What changes in the app

- On Cloudflare, the frontend will use same-origin `/api/vatsim` instead of browser CORS proxies.
- Selecting an aircraft will request `/api/history` and load saved trajectory points from D1.
- On `file://`, the page still falls back to the old browser proxy fetch path.

## Notes

- History is permanent in D1 until you delete it.
- The frontend still keeps an in-memory copy for rendering, but refresh no longer loses everything once the history has been written to D1.
