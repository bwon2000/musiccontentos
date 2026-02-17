# Music Content Engine API

Backend "Engine API" for the Music Content Intelligence OS. Runs jobs (analyze video, recompute patterns, generate ideas) and writes results to Supabase. The Lovable UI calls this API from the browser.

## Tech

- **Node.js** + **TypeScript**
- **Hono** (lightweight, Vercel-friendly)
- **@supabase/supabase-js** (service role, server-only)
- **Zod** for request validation
- **CORS** enabled for Lovable

## Env

Copy `.env.example` to `.env` and set:

- `SUPABASE_URL` – your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` – service role key (never expose to client)
- `PORT` – optional, default `3000`

## Local run

```bash
cp .env.example .env
# Edit .env with your Supabase URL and service role key.

npm install
npm run dev
```

API base: `http://localhost:3000`

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST   | `/health` | Health check |
| POST   | `/jobs/analyze_video` | Analyze one video (placeholder → video_analysis) |
| POST   | `/jobs/recompute_patterns` | Recompute placeholder patterns (hook_type) |
| POST   | `/jobs/generate_ideas` | Generate N placeholder ideas |

## cURL examples

### Health

```bash
curl -X POST http://localhost:3000/health \
  -H "Content-Type: application/json" \
  -d '{}'
```

Expected: `{"ok":true,"name":"music-content-engine","ts":"..."}`

### Analyze video

Use a real `video_id` from your `videos` table.

```bash
curl -X POST http://localhost:3000/jobs/analyze_video \
  -H "Content-Type: application/json" \
  -d '{"video_id":"YOUR-VIDEO-UUID-HERE"}'
```

Expected (success): `{"ok":true,"job_id":"...","video_id":"..."}`  
Expected (video missing): `404` and `{"ok":false,"error":"Video not found","job_id":"..."}`

### Recompute patterns

```bash
curl -X POST http://localhost:3000/jobs/recompute_patterns \
  -H "Content-Type: application/json" \
  -d '{}'
```

With optional filters:

```bash
curl -X POST http://localhost:3000/jobs/recompute_patterns \
  -H "Content-Type: application/json" \
  -d '{"platform":"tiktok","metric":"views"}'
```

Expected: `{"ok":true,"job_id":"...","patterns_written":N}`

### Generate ideas

```bash
curl -X POST http://localhost:3000/jobs/generate_ideas \
  -H "Content-Type: application/json" \
  -d '{}'
```

Default count is 5. Custom count:

```bash
curl -X POST http://localhost:3000/jobs/generate_ideas \
  -H "Content-Type: application/json" \
  -d '{"count":10}'
```

Expected: `{"ok":true,"job_id":"...","ideas_written":N}`

## Deploy

**Node (Railway, Render, etc.)**

1. Build: `npm run build`
2. Set env: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, optional `PORT`
3. Start: `node dist/server.js` (or `npm start`)

**Vercel**

1. Connect the repo; framework is auto-detected (Hono).
2. Set env in Vercel: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`.
3. Root `index.ts` exports the Hono app as default, so no extra config is needed. Deploy with `vercel` or push to the linked Git branch.

## Database (Supabase)

Assumed tables:

- **videos** – source video rows
- **video_analysis** – one row per video (upserted by analyze_video)
- **jobs** – one row per job (type, status, payload, result, error_message)
- **patterns** – feature_name, feature_value, sample_size, top_freq, bottom_freq, uplift, confidence_score, examples, computed_at
- **ideas** – at least `status`, `created_at`, `updated_at` (optional); placeholder ideas use `status: "draft"`

## Notes

- All job endpoints create a `jobs` row with `status: "running"`, then set `status: "done"` or `status: "error"` with `result` or `error_message`.
- Analyze video uses placeholder content until Gemini extraction is wired in.
- Recompute patterns aggregates by `video_analysis.hook_type` and writes up to 20 rows into `patterns`.
- Generate ideas inserts N rows into `ideas` with `status: "draft"`.
