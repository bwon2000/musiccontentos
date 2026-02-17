import "dotenv/config";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import { healthRoutes } from "./routes/health.js";
import { jobsRoutes } from "./routes/jobs.js";

const app = new Hono();

app.use("*", logger());
app.use(
  "*",
  cors({
    origin: (origin) => origin ?? "*",
    allowMethods: ["GET", "POST", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
  })
);

app.route("/", healthRoutes);
app.route("/jobs", jobsRoutes);

app.onError((err, c) => {
  console.error("Unhandled error:", err);
  return c.json({ ok: false, error: "Internal server error" }, 500);
});

export default app;
