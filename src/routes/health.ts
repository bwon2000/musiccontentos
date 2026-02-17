import { Hono } from "hono";

export const healthRoutes = new Hono();

healthRoutes.get("/health", (c) =>
  c.json({
    ok: true,
    name: "music-content-engine",
    ts: new Date().toISOString(),
  })
);
healthRoutes.post("/health", (c) =>
  c.json({
    ok: true,
    name: "music-content-engine",
    ts: new Date().toISOString(),
  })
);
