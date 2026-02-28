# Unified Entrypoint (:3001)

## Goal
Use `http://localhost:3001` as the single browser entrypoint for Web, API, and Admin.

## Port Structure
- Web (Next.js): `localhost:3001` -> container `ivd-web:3000`
- Admin/API (FastAPI): `localhost:8090` -> container `ivd-admin-api:8789`
- Postgres: `localhost:5433` -> container `ivd-postgres:5432`

## Proxy Strategy (Next.js rewrites)
`web/next.config.mjs` rewrites:
- `/api/:path*` -> `${ADMIN_API_INTERNAL_BASE}/api/:path*`
- `/admin/:path*` -> `${ADMIN_API_INTERNAL_BASE}/admin/:path*`
- `/admin` -> `${ADMIN_API_INTERNAL_BASE}/admin`

With Docker defaults:
- `ADMIN_API_INTERNAL_BASE=http://admin-api:8789`

With local `pnpm dev`:
- set `ADMIN_API_INTERNAL_BASE=http://localhost:8090`
- Docker build default is `http://admin-api:8789`

## Environment Variables
`web/.env.example`:
- `NEXT_PUBLIC_API_BASE_URL=/api`
- `ADMIN_API_INTERNAL_BASE=http://localhost:8090`

`docker-compose.yml` (web service):
- `NEXT_PUBLIC_API_BASE_URL=/api`
- `ADMIN_API_INTERNAL_BASE=http://admin-api:8789`

## Frontend Routing
- Feed: `/feed`
- Items: `/feed-items` (alias `/items`)
- Admin shell: `/admin-shell` (iframe wrapper)
- Direct admin still available: `/admin` (rewritten)

## Verification
1. `docker compose up -d --build`
2. Open:
  - `http://localhost:3001/feed`
  - `http://localhost:3001/feed-items`
  - `http://localhost:3001/admin-shell`
  - `http://localhost:3001/admin`
3. Browser Network should show frontend requests to `/api/...` on port `3001` (proxied by Next.js).

## Common Issues
- If `/feed` shows `Failed to fetch`, check `ivd-admin-api` logs and DB migrations.
- If `/admin-shell` is blank, test direct `http://localhost:3001/admin`.
- After config changes, hard refresh browser (`Cmd+Shift+R`).
