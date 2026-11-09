import { bindings, defineConfig, triggers } from "cf/config";

const accountId = "f4e323f273bab9cb104cbe7b945c58d3";
const databaseId = "c29de72a-8458-4fdb-8619-243acae0d864";

export default defineConfig({
  accountId,
  worker: {
    name: "lucy-telemetry",
    entrypoint: "./build/index.js",
    compatibilityDate: "2026-10-01",
    workersDev: false,
    previewUrls: false,
    placement: { mode: "smart" },
    domains: [{ hostname: "telemetry.lucy.lc", production: true, previews: false }],
    env: {
      DB: bindings.d1({ id: databaseId }),
      REPORTS: bindings.r2({ name: "lucy-telemetry" }),
      RECORDS: bindings.queue({ name: "lucy-telemetry" }),
      CONFIG_JSON: bindings.text(JSON.stringify({ environment: "production" })),
    },
    triggers: [
      triggers.queue({
        name: "lucy-telemetry",
        deadLetterQueue: "lucy-telemetry-dlq",
        maxBatchSize: 25,
        maxBatchTimeout: 5,
        maxConcurrency: 5,
        maxRetries: 5,
        retryDelay: 30,
        visibilityTimeoutMs: 30000,
      }),
      triggers.scheduled({ schedule: "7 * * * *" }),
    ],
    observability: {
      enabled: true,
      logs: { enabled: true, invocationLogs: false },
      issues: { enabled: true },
      traces: { enabled: true },
    },
  },
});
