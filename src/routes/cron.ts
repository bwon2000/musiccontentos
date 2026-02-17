import { Hono } from "hono";
import { supabase } from "../supabase.js";
import {
  EDM_HASHTAGS,
  executeAnalyzeBatch,
  executeIngestVideos,
  executeRecomputePatterns,
} from "./jobs.js";

export const cronRoutes = new Hono();

const CRON_SECRET_HEADER = "x_cron_secret";

/** Check for overlapping run: recent (15 min) job with status=running and (type=ingest_videos or payload.mode=cron) */
async function hasRecentCronRun(): Promise<boolean> {
  const since = new Date(Date.now() - 15 * 60 * 1000).toISOString();
  const { data, error } = await supabase
    .from("jobs")
    .select("id, type, payload")
    .eq("status", "running")
    .gte("created_at", since)
    .limit(20);

  if (error) return false;
  const rows = data ?? [];
  return rows.some((row) => {
    const mode = (row.payload as Record<string, unknown>)?.mode;
    return row.type === "ingest_videos" || mode === "cron";
  });
}

cronRoutes.post("/run", async (c) => {
  const secret = c.req.header(CRON_SECRET_HEADER);
  const expected = process.env.CRON_SECRET;
  if (!expected || secret !== expected) {
    return c.json({ ok: false, error: "Unauthorized" }, 401);
  }

  if (await hasRecentCronRun()) {
    return c.json({ ok: false, error: "already running" }, 409);
  }

  const ingest = await executeIngestVideos(
    {
      platform: "tiktok",
      seeds: { hashtags: EDM_HASHTAGS },
      limit: 200,
    },
    { mode: "cron" }
  );

  const analyze = await executeAnalyzeBatch({ limit: 50 }, { mode: "cron" });

  const patterns = await executeRecomputePatterns(undefined, { mode: "cron" });

  return c.json({
    ok: true,
    ran: "cron",
    ingest: { job_id: ingest.job_id, ...("error" in ingest ? { error: ingest.error } : { total: ingest.total, upserted: ingest.upserted, skipped: ingest.skipped }) },
    analyze: { job_id: analyze.job_id, ...("error" in analyze ? { error: analyze.error } : { selected: analyze.selected, analyzed: analyze.analyzed, errors: analyze.errors }) },
    patterns: { job_id: patterns.job_id, ...("error" in patterns ? { error: patterns.error } : { patterns_written: patterns.patterns_written }) },
  });
});

/** GET /cron/analyze – hourly cron: batch analyze only (no ingestion). Same logic as POST /jobs/analyze_batch with limit 50. */
const DEFAULT_ANALYZE_LIMIT = 50;

cronRoutes.get("/analyze", async (c) => {
  const secret = c.req.header(CRON_SECRET_HEADER);
  const expected = process.env.CRON_SECRET;
  if (!expected || secret !== expected) {
    return c.json({ ok: false, error: "Unauthorized" }, 401);
  }

  const result = await executeAnalyzeBatch({ limit: DEFAULT_ANALYZE_LIMIT });

  if ("error" in result) {
    return c.json(
      { ok: false, job_id: result.job_id, error: result.error },
      500
    );
  }

  return c.json({
    ok: true,
    job_id: result.job_id,
    analyzed: result.analyzed,
    errors: result.errors,
  });
});
