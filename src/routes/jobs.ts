import { Hono } from "hono";
import { z } from "zod";
import { supabase } from "../supabase.js";
import { getDatasetItems, runTikTokHashtagScraper } from "../apify.js";

const analyzeVideoSchema = z.object({
  video_id: z.string().uuid(),
});

const analyzeBatchSchema = z.object({
  limit: z.number().int().min(1).max(200).optional().default(25),
});

const recomputePatternsSchema = z.object({
  platform: z.enum(["tiktok", "instagram", "youtube"]).optional(),
  metric: z.enum(["views", "engagement_rate"]).optional(),
});

const generateIdeasSchema = z.object({
  count: z.number().int().min(1).max(100).optional().default(5),
});

const ingestVideosSchema = z.object({
  platform: z.literal("tiktok"),
  seeds: z.object({
    hashtags: z.array(z.string()).min(1),
  }),
  limit: z.number().int().min(1).max(1000).optional(),
  resultsPerHashtag: z.number().int().min(1).max(100).optional(),
});

const computeBucketsSchema = z.object({
  metric: z.enum(["views", "engagement_rate"]).optional().default("views"),
  platform: z.enum(["tiktok", "instagram", "youtube"]).optional(),
  min_views: z.number().int().min(0).optional().default(0),
});

type JobStatus = "running" | "done" | "error";

async function createJob(
  type: string,
  payload: Record<string, unknown>,
  videoId?: string
): Promise<string> {
  const now = new Date().toISOString();
  const { data, error } = await supabase
    .from("jobs")
    .insert({
      type,
      status: "running",
      video_id: videoId ?? null,
      payload: payload || {},
      result: {},
      error_message: null,
      created_at: now,
      updated_at: now,
    })
    .select("id")
    .single();

  if (error) throw error;
  if (!data?.id) throw new Error("Job insert did not return id");
  return data.id;
}

async function updateJob(
  jobId: string,
  updates: {
    status: JobStatus;
    result?: Record<string, unknown> | null;
    error_message?: string | null;
  }
) {
  const { error } = await supabase
    .from("jobs")
    .update({
      ...updates,
      updated_at: new Date().toISOString(),
    })
    .eq("id", jobId);

  if (error) throw error;
}

/** Shared result types for cron */
export type IngestResult =
  | { job_id: string; total: number; upserted: number; skipped: number }
  | { job_id: string; error: string };
export type AnalyzeBatchResult =
  | { job_id: string; selected: number; analyzed: number; errors: number }
  | { job_id: string; error: string };
export type RecomputePatternsResult =
  | { job_id: string; patterns_written: number }
  | { job_id: string; error: string };
export type ComputeBucketsResult =
  | { job_id: string; total_scored: number; top_count: number; bottom_count: number; updated_count: number }
  | { job_id: string; error: string };

export const EDM_HASHTAGS = [
  "edm", "edmtiktok", "rave", "ravelife", "bassmusic", "dubstep", "dnb", "drumandbass",
  "techno", "housemusic", "deephouse", "progressivehouse", "hardstyle", "trance",
  "melodictechno", "futurebass", "electrohouse",
];

/** Executor: ingest_videos. Used by POST /jobs/ingest_videos and cron. */
export async function executeIngestVideos(
  params: {
    platform: "tiktok";
    seeds: { hashtags: string[] };
    limit?: number;
    resultsPerHashtag?: number;
  },
  options?: { mode?: "cron" }
): Promise<IngestResult> {
  const payload = {
    ...params,
    ...(options?.mode ? { mode: options.mode } : {}),
  };
  let jobId: string | null = null;
  try {
    const apifyToken = process.env.APIFY_TOKEN;
    const apifyActorId = process.env.APIFY_ACTOR_ID ?? "clockworks/tiktok-hashtag-scraper";
    if (!apifyToken) throw new Error("APIFY_TOKEN is not configured");

    jobId = await createJob("ingest_videos", payload);
    const { seeds, limit, resultsPerHashtag } = params;
    const hashtags = seeds.hashtags;

    const runData = await runTikTokHashtagScraper(apifyActorId, apifyToken, {
      hashtags,
      resultsPerPage: resultsPerHashtag ?? 25,
      proxyConfiguration: { useApifyProxy: true, apifyProxyGroups: ["RESIDENTIAL"] },
    }, 120);

    const limitNum = limit ?? 200;
    const items = await getDatasetItems<Record<string, unknown>>(
      runData.defaultDatasetId,
      apifyToken,
      limitNum
    );

    const pulledAt = new Date().toISOString();
    const rowsByUrl = new Map<string, Record<string, unknown>>();
    let skippedNoUrl = 0;
    for (const item of items) {
      const row = normalizeApifyItemToVideo(item, pulledAt);
      if (!row) {
        skippedNoUrl += 1;
        continue;
      }
      if (row.video_url) rowsByUrl.set(String(row.video_url), row);
    }
    const rows = Array.from(rowsByUrl.values());
    const total = items.length;
    const skipped = skippedNoUrl;

    if (rows.length > 0) {
      const { error: upsertError } = await supabase
        .from("videos")
        .upsert(rows, { onConflict: "video_url" });
      if (upsertError) throw upsertError;
    }

    const result = { total, upserted: rows.length, skipped };
    await updateJob(jobId, { status: "done", result });
    return { job_id: jobId, ...result };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown error";
    const truncated = msg.length > 1000 ? msg.slice(0, 1000) + "…" : msg;
    if (jobId) await updateJob(jobId, { status: "error", error_message: truncated }).catch(() => {});
    return { job_id: jobId!, error: msg };
  }
}

/** Executor: analyze_batch. Used by POST /jobs/analyze_batch and cron. */
export async function executeAnalyzeBatch(
  params: { limit: number },
  options?: { mode?: "cron" }
): Promise<AnalyzeBatchResult> {
  const payload = options?.mode
    ? { mode: "cron" as const, limit: params.limit }
    : { mode: "batch" as const, limit: params.limit };
  let jobId: string | null = null;
  try {
    jobId = await createJob("analyze_video", payload);

    const limit = params.limit;
    const fetchSize = Math.min(limit * 2, 500);
    const { data: candidateVideos, error: videosError } = await supabase
      .from("videos")
      .select("id")
      .order("pulled_at", { ascending: false, nullsFirst: false })
      .limit(fetchSize);

    if (videosError) throw videosError;
    const candidateIds = (candidateVideos ?? []).map((v) => v.id).filter(Boolean);
    if (candidateIds.length === 0) {
      await updateJob(jobId, {
        status: "done",
        result: { mode: "batch", selected: 0, analyzed: 0, errors: 0, error_samples: [] },
      });
      return { job_id: jobId, selected: 0, analyzed: 0, errors: 0 };
    }

    const { data: existingAnalysis, error: analysisError } = await supabase
      .from("video_analysis")
      .select("video_id")
      .in("video_id", candidateIds);
    if (analysisError) throw analysisError;

    const existingSet = new Set(
      (existingAnalysis ?? []).map((r) => r.video_id).filter(Boolean)
    );
    const toAnalyze = candidateIds.filter((id) => !existingSet.has(id)).slice(0, limit);
    const selected = toAnalyze.length;

    const placeholderRow = (videoId: string) => {
      const now = new Date().toISOString();
      return {
        video_id: videoId,
        hook_text: "PLACEHOLDER: hook summary",
        hook_type: "placeholder",
        on_screen_text: "PLACEHOLDER: on-screen text",
        format: "placeholder",
        cta_type: "placeholder",
        face_present: null,
        transcript_summary: "PLACEHOLDER: transcript summary",
        extracted_tags: { note: "replace with Gemini later" },
        shots: [],
        analyzed_at: now,
        updated_at: now,
      };
    };

    let analyzed = 0;
    let errors = 0;
    const errorSamples: Array<{ video_id: string; message: string }> = [];
    const maxErrorSamples = 5;

    for (const videoId of toAnalyze) {
      const { error: upsertErr } = await supabase
        .from("video_analysis")
        .upsert(placeholderRow(videoId), { onConflict: "video_id" });
      if (upsertErr) {
        errors += 1;
        if (errorSamples.length < maxErrorSamples)
          errorSamples.push({ video_id: videoId, message: upsertErr.message });
      } else {
        analyzed += 1;
      }
    }

    const result = { mode: "batch", selected, analyzed, errors, error_samples: errorSamples };
    await updateJob(jobId, { status: "done", result });
    return { job_id: jobId, selected, analyzed, errors };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown error";
    const truncated = msg.length > 1000 ? msg.slice(0, 1000) + "…" : msg;
    if (jobId) await updateJob(jobId, { status: "error", error_message: truncated }).catch(() => {});
    return { job_id: jobId!, error: msg };
  }
}

/** Executor: recompute_patterns. Uses top vs bottom performance buckets for uplift. */
export async function executeRecomputePatterns(
  params?: { platform?: string; metric?: string },
  options?: { mode?: "cron" }
): Promise<RecomputePatternsResult> {
  const payload = { ...(params ?? {}), ...(options?.mode ? { mode: options.mode } : {}) };
  let jobId: string | null = null;
  try {
    jobId = await createJob("recompute_patterns", payload);

    const { data: analyses, error: fetchError } = await supabase
      .from("video_analysis")
      .select("video_id, hook_type");
    if (fetchError) throw fetchError;

    const rows = analyses ?? [];
    const videoIds = rows.map((r) => r.video_id).filter(Boolean);
    if (videoIds.length === 0) {
      await updateJob(jobId, { status: "done", result: { patterns_written: 0 } });
      return { job_id: jobId, patterns_written: 0 };
    }

    const { data: videoBuckets } = await supabase
      .from("videos")
      .select("id, performance_bucket")
      .in("id", videoIds);

    const topIds = new Set(
      (videoBuckets ?? []).filter((v) => v.performance_bucket === "top").map((v) => v.id)
    );
    const bottomIds = new Set(
      (videoBuckets ?? []).filter((v) => v.performance_bucket === "bottom").map((v) => v.id)
    );
    const totalTop = topIds.size;
    const totalBottom = bottomIds.size;

    type HookAgg = { topCount: number; bottomCount: number; topVideoIds: string[] };
    const byHookType: Record<string, HookAgg> = {};
    for (const row of rows) {
      const key = row.hook_type ?? "unknown";
      if (!byHookType[key]) byHookType[key] = { topCount: 0, bottomCount: 0, topVideoIds: [] };
      const vid = row.video_id;
      if (topIds.has(vid)) {
        byHookType[key].topCount += 1;
        if (byHookType[key].topVideoIds.length < 5) byHookType[key].topVideoIds.push(vid);
      } else if (bottomIds.has(vid)) {
        byHookType[key].bottomCount += 1;
      }
    }

    const now = new Date().toISOString();
    const patternsToInsert: Array<{
      feature_name: string;
      feature_value: string;
      sample_size: number;
      top_freq: number | null;
      bottom_freq: number | null;
      uplift: number | null;
      confidence_score: number | null;
      examples: unknown;
      computed_at: string;
    }> = [];

    const entries = Object.entries(byHookType)
      .filter(([, agg]) => agg.topCount + agg.bottomCount >= 1)
      .sort((a, b) => b[1].topCount + b[1].bottomCount - (a[1].topCount + a[1].bottomCount))
      .slice(0, 20);

    for (const [hookType, agg] of entries) {
      const topFreq = totalTop > 0 ? agg.topCount / totalTop : null;
      const bottomFreq = totalBottom > 0 ? agg.bottomCount / totalBottom : null;
      const uplift =
        topFreq != null && bottomFreq != null ? topFreq - bottomFreq : null;
      patternsToInsert.push({
        feature_name: "hook_type",
        feature_value: hookType,
        sample_size: agg.topCount + agg.bottomCount,
        top_freq: topFreq ?? 0,
        bottom_freq: bottomFreq ?? 0,
        uplift,
        confidence_score: agg.topCount + agg.bottomCount >= 3 ? 0.8 : 0.5,
        examples: agg.topVideoIds,
        computed_at: now,
      });
    }

    if (patternsToInsert.length > 0) {
      await supabase.from("patterns").delete().eq("feature_name", "hook_type");
      const { error: insertError } = await supabase.from("patterns").insert(patternsToInsert);
      if (insertError) throw insertError;
    }

    await updateJob(jobId, {
      status: "done",
      result: {
        patterns_written: patternsToInsert.length,
        by_hook_type: Object.keys(byHookType).length,
      },
    });
    return { job_id: jobId, patterns_written: patternsToInsert.length };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown error";
    const truncated = msg.length > 1000 ? msg.slice(0, 1000) + "…" : msg;
    if (jobId) await updateJob(jobId, { status: "error", error_message: truncated }).catch(() => {});
    return { job_id: jobId!, error: msg };
  }
}

const BUCKET_UPDATE_CHUNK_SIZE = 100;

/** Executor: compute_buckets. Labels videos into top/mid/bottom by metric, updates engagement_rate. */
export async function executeComputeBuckets(params: {
  metric?: "views" | "engagement_rate";
  platform?: string;
  min_views?: number;
}): Promise<ComputeBucketsResult> {
  const payload = { mode: "compute_buckets" as const, ...params };
  let jobId: string | null = null;
  try {
    jobId = await createJob("recompute_patterns", payload);

    const metric = params.metric ?? "views";
    const minViews = params.min_views ?? 0;
    let query = supabase
      .from("videos")
      .select("id, views, likes, comments_count, shares, engagement_rate, platform");

    if (params.platform) query = query.eq("platform", params.platform);
    const { data: videos, error: fetchError } = await query;
    if (fetchError) throw fetchError;

    const rows = (videos ?? []) as Array<{
      id: string;
      views: number | null;
      likes: number | null;
      comments_count: number | null;
      shares: number | null;
      engagement_rate: number | null;
      platform: string | null;
    }>;

    type Scored = { id: string; engagement_rate: number | null; metricValue: number };
    const scored: Scored[] = [];
    for (const v of rows) {
      const views = Number(v.views) ?? 0;
      const likes = Number(v.likes) ?? 0;
      const comments = Number(v.comments_count) ?? 0;
      const shares = Number(v.shares) ?? 0;
      let engagementRate: number | null = v.engagement_rate != null ? Number(v.engagement_rate) : null;
      if (engagementRate == null && views > 0 && (likes + comments + shares) >= 0) {
        engagementRate = (likes + comments + shares) / views;
      }
      const metricValue = metric === "engagement_rate"
        ? (engagementRate ?? 0)
        : views;
      if (metricValue <= minViews) continue;
      scored.push({ id: v.id, engagement_rate: engagementRate, metricValue });
    }

    scored.sort((a, b) => b.metricValue - a.metricValue);
    const total = scored.length;
    const topCount = Math.max(0, Math.floor(total * 0.2));
    const bottomCount = Math.max(0, Math.floor(total * 0.2));
    const topEnd = topCount;
    const bottomStart = total - bottomCount;

    const updates: Array<{ id: string; engagement_rate: number | null; top_20: boolean; performance_bucket: string }> = [];
    for (let i = 0; i < scored.length; i++) {
      const s = scored[i];
      let performance_bucket: string;
      let top_20: boolean;
      if (i < topEnd) {
        performance_bucket = "top";
        top_20 = true;
      } else if (i >= bottomStart) {
        performance_bucket = "bottom";
        top_20 = false;
      } else {
        performance_bucket = "mid";
        top_20 = false;
      }
      updates.push({
        id: s.id,
        engagement_rate: s.engagement_rate,
        top_20,
        performance_bucket,
      });
    }

    const now = new Date().toISOString();
    let updatedCount = 0;
    for (let i = 0; i < updates.length; i += BUCKET_UPDATE_CHUNK_SIZE) {
      const chunk = updates.slice(i, i + BUCKET_UPDATE_CHUNK_SIZE).map((row) => ({
        id: row.id,
        engagement_rate: row.engagement_rate,
        top_20: row.top_20,
        performance_bucket: row.performance_bucket,
        updated_at: now,
      }));
      const { error } = await supabase.from("videos").upsert(chunk, { onConflict: "id" });
      if (!error) updatedCount += chunk.length;
    }

    const result = {
      total_scored: total,
      top_count: topCount,
      bottom_count: bottomCount,
      updated_count: updatedCount,
    };
    await updateJob(jobId, { status: "done", result });
    return { job_id: jobId, ...result };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown error";
    const truncated = msg.length > 1000 ? msg.slice(0, 1000) + "…" : msg;
    if (jobId) await updateJob(jobId, { status: "error", error_message: truncated }).catch(() => {});
    return { job_id: jobId!, error: msg };
  }
}

export const jobsRoutes = new Hono();

jobsRoutes.post("/analyze_video", async (c) => {
  let jobId: string | null = null;

  try {
    const body = await c.req.json();
    const parsed = analyzeVideoSchema.safeParse(body);
    if (!parsed.success) {
      return c.json(
        { ok: false, error: "Invalid request", details: parsed.error.flatten() },
        400
      );
    }
    const { video_id } = parsed.data;

    // Step 1: Create job row
    const now = new Date().toISOString();
    const { data: jobData, error: jobError } = await supabase
      .from("jobs")
      .insert({
        type: "analyze_video",
        status: "running",
        video_id,
        payload: { video_id },
        result: {},
        error_message: null,
        created_at: now,
        updated_at: now,
      })
      .select("id")
      .single();

    if (jobError) {
      console.error("SUPABASE ERROR:", jobError);
      throw new Error(
        JSON.stringify({
          message: jobError.message,
          details: jobError.details,
          hint: jobError.hint,
          code: jobError.code,
        })
      );
    }
    if (!jobData?.id) throw new Error("Job insert did not return id");
    jobId = jobData.id;
    console.log("Step 1: Job created");

    // Step 2: Fetch video by id
    const { data: video, error: videoError } = await supabase
      .from("videos")
      .select("id")
      .eq("id", video_id)
      .single();

    if (videoError) {
      console.error("SUPABASE ERROR:", videoError);
      throw new Error(
        JSON.stringify({
          message: videoError.message,
          details: videoError.details,
          hint: videoError.hint,
          code: videoError.code,
        })
      );
    }
    if (!video) {
      throw new Error(JSON.stringify({ message: "Video not found", code: "PGRST116" }));
    }
    console.log("Step 2: Video fetched");

    // Step 3: Upsert video_analysis
    const { error: upsertError } = await supabase.from("video_analysis").upsert(
      {
        video_id,
        hook_text: "PLACEHOLDER: hook summary",
        hook_type: "placeholder",
        on_screen_text: "PLACEHOLDER: on-screen text",
        format: "placeholder",
        cta_type: "placeholder",
        face_present: null,
        transcript_summary: "PLACEHOLDER: transcript summary",
        extracted_tags: { note: "replace with Gemini later" },
        shots: [],
        analyzed_at: now,
        updated_at: now,
      },
      { onConflict: "video_id" }
    );

    if (upsertError) {
      console.error("SUPABASE ERROR:", upsertError);
      throw new Error(
        JSON.stringify({
          message: upsertError.message,
          details: upsertError.details,
          hint: upsertError.hint,
          code: upsertError.code,
        })
      );
    }
    console.log("Step 3: Analysis upserted");

    // Step 4: Update job status=done
    const { error: updateJobError } = await supabase
      .from("jobs")
      .update({
        status: "done",
        result: { video_id, wrote_analysis: true },
        updated_at: new Date().toISOString(),
      })
      .eq("id", jobId);

    if (updateJobError) {
      console.error("SUPABASE ERROR:", updateJobError);
      throw new Error(
        JSON.stringify({
          message: updateJobError.message,
          details: updateJobError.details,
          hint: updateJobError.hint,
          code: updateJobError.code,
        })
      );
    }
    console.log("Step 4: Job updated");

    return c.json({ ok: true, job_id: jobId, video_id });
  } catch (err) {
    console.error("FULL ERROR:", err);

    if (jobId) {
      const errMessage = err instanceof Error ? err.message : "Unknown error";
      const truncated = errMessage.length > 1000 ? errMessage.slice(0, 1000) + "…" : errMessage;
      await supabase
        .from("jobs")
        .update({
          status: "error",
          error_message: truncated,
          updated_at: new Date().toISOString(),
        })
        .eq("id", jobId)
        .then(({ error }) => {
          if (error) console.error("Failed to update job to error:", error);
        });
    }

    return c.json(
      {
        ok: false,
        error: "Internal error",
        debug: err instanceof Error ? err.message : String(err),
      },
      500
    );
  }
});

jobsRoutes.post("/analyze_batch", async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = analyzeBatchSchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      { ok: false, error: "Invalid request", details: parsed.error.flatten() },
      400
    );
  }
  const result = await executeAnalyzeBatch({ limit: parsed.data.limit });
  if ("error" in result) {
    return c.json({ ok: false, error: result.error, job_id: result.job_id }, 500);
  }
  return c.json({
    ok: true,
    job_id: result.job_id,
    selected: result.selected,
    analyzed: result.analyzed,
    errors: result.errors,
  });
});

jobsRoutes.post("/recompute_patterns", async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = recomputePatternsSchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      { ok: false, error: "Invalid request", details: parsed.error.flatten() },
      400
    );
  }
  const params = parsed.data as { platform?: string; metric?: string };
  const result = await executeRecomputePatterns(params);
  if ("error" in result) {
    return c.json({ ok: false, error: result.error, job_id: result.job_id }, 500);
  }
  return c.json({
    ok: true,
    job_id: result.job_id,
    patterns_written: result.patterns_written,
  });
});

jobsRoutes.post("/compute_buckets", async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = computeBucketsSchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      { ok: false, error: "Invalid request", details: parsed.error.flatten() },
      400
    );
  }
  const result = await executeComputeBuckets(parsed.data);
  if ("error" in result) {
    return c.json({ ok: false, error: result.error, job_id: result.job_id }, 500);
  }
  return c.json({
    ok: true,
    job_id: result.job_id,
    total_scored: result.total_scored,
    top_count: result.top_count,
    bottom_count: result.bottom_count,
    updated_count: result.updated_count,
  });
});

jobsRoutes.post("/generate_ideas", async (c) => {
  let jobId: string | null = null;

  try {
    const body = await c.req.json().catch(() => ({}));
    const parsed = generateIdeasSchema.safeParse(body);
    if (!parsed.success) {
      return c.json(
        { ok: false, error: "Invalid request", details: parsed.error.flatten() },
        400
      );
    }
    const { count } = parsed.data;
    jobId = await createJob("generate_ideas", { count });

    const now = new Date().toISOString();
    const ideasToInsert = Array.from({ length: count }, () => ({
      status: "draft",
      created_at: now,
      updated_at: now,
    }));

    const { error: insertError } = await supabase
      .from("ideas")
      .insert(ideasToInsert);

    if (insertError) {
      await updateJob(jobId, {
        status: "error",
        error_message: insertError.message,
      });
      return c.json(
        { ok: false, error: insertError.message, job_id: jobId },
        500
      );
    }

    await updateJob(jobId, {
      status: "done",
      result: { ideas_written: count },
    });

    return c.json({
      ok: true,
      job_id: jobId,
      ideas_written: count,
    });
  } catch (err) {
    console.error("generate_ideas error:", err);
    if (jobId) {
      await updateJob(jobId, {
        status: "error",
        error_message: err instanceof Error ? err.message : "Unknown error",
      }).catch(() => {});
    }
    return c.json(
      { ok: false, error: err instanceof Error ? err.message : "Internal error" },
      500
    );
  }
});

/** Map Apify TikTok item to videos row. Returns null if no video_url. */
function normalizeApifyItemToVideo(
  item: Record<string, unknown>,
  pulledAt: string
): Record<string, unknown> | null {
  const videoUrl =
    (item.videoUrl as string) ??
    (item.url as string) ??
    (item.webVideoUrl as string) ??
    (item.link as string);
  if (!videoUrl || typeof videoUrl !== "string") return null;

  const createTime = item.createTime as number | undefined;
  const postDate =
    (item.createTimeISO as string) ||
    (typeof createTime === "number"
      ? new Date(createTime * 1000).toISOString()
      : null);

  return {
    video_url: videoUrl,
    creator_handle:
      (item.authorMeta as Record<string, unknown>)?.name ??
      (item.author as Record<string, unknown>)?.uniqueId ??
      item.authorName ??
      null,
    caption: (item.text as string) ?? (item.caption as string) ?? (item.desc as string) ?? null,
    post_date: postDate,
    views:
      (item.stats as Record<string, unknown>)?.viewsCount ??
      item.playCount ??
      item.viewCount ??
      null,
    likes:
      (item.stats as Record<string, unknown>)?.likesCount ??
      item.diggCount ??
      item.likeCount ??
      null,
    comments_count:
      (item.stats as Record<string, unknown>)?.commentsCount ??
      item.commentCount ??
      null,
    shares:
      (item.stats as Record<string, unknown>)?.sharesCount ?? item.shareCount ?? null,
    saves:
      (item.stats as Record<string, unknown>)?.savedCount ?? item.collectCount ?? null,
    platform: "tiktok",
    pulled_at: pulledAt,
    updated_at: pulledAt,
  };
}

jobsRoutes.post("/ingest_videos", async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = ingestVideosSchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      { ok: false, error: "Invalid request", details: parsed.error.flatten() },
      400
    );
  }
  const { platform, seeds, limit, resultsPerHashtag } = parsed.data;
  const result = await executeIngestVideos({
    platform,
    seeds,
    limit: limit ?? undefined,
    resultsPerHashtag: resultsPerHashtag ?? undefined,
  });
  if ("error" in result) {
    return c.json({ ok: false, error: result.error, job_id: result.job_id }, 500);
  }
  return c.json({
    ok: true,
    job_id: result.job_id,
    total: result.total,
    upserted: result.upserted,
    skipped: result.skipped,
  });
});
