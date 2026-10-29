import process from "node:process";
import { bindings, defineConfig, triggers } from "cf/config";

export default defineConfig(({ mode }) => {
  const environment = mode ?? "staging";
  if (environment !== "staging" && environment !== "production") {
    throw new Error("Select the staging or production mode.");
  }

  const production = environment === "production";
  const name =
    process.env.LUCY_TELEMETRY_RESOURCE_PREFIX ||
    (production ? "lucy-telemetry" : "lucy-telemetry-staging");
  const adminHost = production ? "telemetry-admin.lucy.lc" : undefined;

  return {
    worker: {
      name,
      entrypoint: "./build/index.js",
      compatibilityDate: "2026-10-01",
      workersDev: !production,
      previewUrls: false,
      domains: production
        ? [
            { hostname: "telemetry.lucy.lc", production: true, previews: false },
            { hostname: adminHost!, production: true, previews: false },
          ]
        : [],
      env: {
        DB: bindings.d1({ name, id: process.env.LUCY_TELEMETRY_D1_ID }),
        REPORTS: bindings.r2({ name }),
        RECORDS: bindings.queue({ name }),
        CONFIG_JSON: bindings.text(JSON.stringify({ environment, admin_host: adminHost })),
        RETRIEVAL_TOKEN: bindings.secret(),
      },
      triggers: [
        triggers.queue({
          name,
          deadLetterQueue: `${name}-dlq`,
          maxBatchSize: 25,
          maxBatchTimeout: 5,
          maxConcurrency: 5,
          maxRetries: 5,
          retryDelay: 30,
          visibilityTimeoutMs: 30000,
        }),
        triggers.scheduled({ schedule: "7 * * * *" }),
      ],
      observability: { enabled: true, logs: { enabled: true, invocationLogs: false } },
    },
  };
});
