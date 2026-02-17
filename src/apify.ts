const APIFY_BASE = "https://api.apify.com/v2";

export type ApifyRunInput = {
  hashtags: string[];
  resultsPerPage?: number;
  proxyConfiguration?: {
    useApifyProxy: boolean;
    apifyProxyGroups: string[];
  };
};

export type ApifyRunResponse = {
  data: {
    id: string;
    defaultDatasetId: string;
    status: string;
  };
};

/**
 * Start actor run and wait for finish.
 * Returns run data including defaultDatasetId.
 */
export async function runTikTokHashtagScraper(
  actorId: string,
  token: string,
  input: ApifyRunInput,
  waitForFinishSeconds = 120
): Promise<ApifyRunResponse["data"]> {
  const encodedActorId = encodeURIComponent(actorId);
  const url = `${APIFY_BASE}/acts/${encodedActorId}/runs?token=${encodeURIComponent(token)}&waitForFinish=${waitForFinishSeconds}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Apify run failed (${res.status}): ${text}`);
  }

  const json = (await res.json()) as ApifyRunResponse;
  if (!json?.data?.defaultDatasetId) {
    throw new Error("Apify run response missing defaultDatasetId");
  }
  return json.data;
}

/**
 * Fetch dataset items as JSON.
 */
export async function getDatasetItems<T = Record<string, unknown>>(
  datasetId: string,
  token: string,
  limit = 200
): Promise<T[]> {
  const url = `${APIFY_BASE}/datasets/${datasetId}/items?token=${encodeURIComponent(token)}&clean=true&format=json&limit=${limit}`;

  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Apify dataset fetch failed (${res.status}): ${text}`);
  }

  const data = (await res.json()) as T[];
  return Array.isArray(data) ? data : [];
}
