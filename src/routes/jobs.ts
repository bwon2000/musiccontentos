import { Hono } from "hono";
import { z } from "zod";
import { supabase } from "../supabase.js";
import { getDatasetItems, runTikTokHashtagScraper } from "../apify.js";

const analyzeVideoSchema = z.object({
  video_id: z.string().uuid(),
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

jobsRoutes.post("/recompute_patterns", async (c) => {
  let jobId: string | null = null;

  try {
    const body = await c.req.json().catch(() => ({}));
    const parsed = recomputePatternsSchema.safeParse(body);
    if (!parsed.success) {
      return c.json(
        { ok: false, error: "Invalid request", details: parsed.error.flatten() },
        400
      );
    }
    const payload = parsed.data as Record<string, unknown>;
    jobId = await createJob("recompute_patterns", payload);

    const { data: analyses, error: fetchError } = await supabase
      .from("video_analysis")
      .select("video_id, hook_type");

    if (fetchError) {
      await updateJob(jobId, {
        status: "error",
        error_message: fetchError.message,
      });
      return c.json(
        { ok: false, error: fetchError.message, job_id: jobId },
        500
      );
    }

    const rows = analyses ?? [];
    const byHookType: Record<string, { count: number; videoIds: string[] }> = {};
    for (const row of rows) {
      const key = row.hook_type ?? "unknown";
      if (!byHookType[key]) {
        byHookType[key] = { count: 0, videoIds: [] };
      }
      byHookType[key].count += 1;
      byHookType[key].videoIds.push(row.video_id);
    }

    const videoIds = rows.map((r) => r.video_id).filter(Boolean);
    const { data: videoStats } = await supabase
      .from("videos")
      .select("id, views, engagement_rate")
      .in("id", videoIds);

    const viewsByVideo = new Map(
      (videoStats ?? []).map((v) => [v.id, Number(v.views) ?? 0])
    );
    const engagementByVideo = new Map(
      (videoStats ?? []).map((v) => [v.id, Number(v.engagement_rate) ?? 0])
    );
    const overallAvgViews =
      videoStats?.length &&
      videoStats.reduce((s, v) => s + (Number(v.views) ?? 0), 0) / videoStats.length;
    const overallAvgEng =
      videoStats?.length &&
      videoStats.reduce((s, v) => s + (Number(v.engagement_rate) ?? 0), 0) / videoStats.length;

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
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 20);

    for (const [hookType, { count, videoIds: ids }] of entries) {
      const views = ids.map((id) => viewsByVideo.get(id) ?? 0).filter(Boolean);
      const engagements = ids.map((id) => engagementByVideo.get(id) ?? 0).filter(Boolean);
      const avgViews = views.length ? views.reduce((a, b) => a + b, 0) / views.length : null;
      const avgEng = engagements.length
        ? engagements.reduce((a, b) => a + b, 0) / engagements.length
        : null;
      const uplift =
        overallAvgViews && avgViews
          ? (avgViews - overallAvgViews) / overallAvgViews
          : null;

      patternsToInsert.push({
        feature_name: "hook_type",
        feature_value: hookType,
        sample_size: count,
        top_freq: avgViews ?? 0,
        bottom_freq: avgEng ?? 0,
        uplift,
        confidence_score: count >= 3 ? 0.8 : 0.5,
        examples: ids.slice(0, 5),
        computed_at: now,
      });
    }

    if (patternsToInsert.length > 0) {
      await supabase
        .from("patterns")
        .delete()
        .eq("feature_name", "hook_type");
      const { error: insertError } = await supabase
        .from("patterns")
        .insert(patternsToInsert);
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
    }

    await updateJob(jobId, {
      status: "done",
      result: {
        patterns_written: patternsToInsert.length,
        by_hook_type: Object.keys(byHookType).length,
      },
    });

    return c.json({
      ok: true,
      job_id: jobId,
      patterns_written: patternsToInsert.length,
    });
  } catch (err) {
    console.error("recompute_patterns error:", err);
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
  let jobId: string | null = null;

  try {
    const body = await c.req.json().catch(() => ({}));
    const parsed = ingestVideosSchema.safeParse(body);
    if (!parsed.success) {
      return c.json(
        { ok: false, error: "Invalid request", details: parsed.error.flatten() },
        400
      );
    }
    const { platform, seeds, limit, resultsPerHashtag } = parsed.data;

    const apifyToken = process.env.APIFY_TOKEN;
    const apifyActorId =
      process.env.APIFY_ACTOR_ID ?? "clockworks/tiktok-hashtag-scraper";
    if (!apifyToken) {
      return c.json(
        { ok: false, error: "APIFY_TOKEN is not configured" },
        500
      );
    }

    const payload = {
      platform,
      seeds,
      limit: limit ?? undefined,
      resultsPerHashtag: resultsPerHashtag ?? undefined,
    };
    jobId = await createJob("ingest_videos", payload);

    const hashtags = seeds.hashtags;
    const runData = await runTikTokHashtagScraper(
      apifyActorId,
      apifyToken,
      {
        hashtags,
        resultsPerPage: resultsPerHashtag ?? 25,
        proxyConfiguration: {
          useApifyProxy: true,
          apifyProxyGroups: ["RESIDENTIAL"],
        },
      },
      120
    );

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

      if (upsertError) {
        await updateJob(jobId, {
          status: "error",
          error_message: upsertError.message,
        });
        return c.json(
          { ok: false, error: upsertError.message, job_id: jobId },
          500
        );
      }
    }

    const result = { total, upserted: rows.length, skipped };
    await updateJob(jobId, {
      status: "done",
      result,
    });

    return c.json({
      ok: true,
      job_id: jobId,
      ...result,
    });
  } catch (err) {
    console.error("ingest_videos error:", err);
    if (jobId) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      const truncated = msg.length > 1000 ? msg.slice(0, 1000) + "…" : msg;
      await updateJob(jobId, {
        status: "error",
        error_message: truncated,
      }).catch(() => {});
    }
    return c.json(
      { ok: false, error: err instanceof Error ? err.message : "Internal error" },
      500
    );
  }
});
