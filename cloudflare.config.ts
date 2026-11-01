import process from "node:process";
import { bindings, defineConfig, triggers } from "cf/config";

const accountId = "f4e323f273bab9cb104cbe7b945c58d3";
const productionDatabaseId = "c29de72a-8458-4fdb-8619-243acae0d864";

export default defineConfig(({ mode }) => {
  // Vite evaluates the config in `development` mode, which serves the staging profile.
  const environment = mode === undefined || mode === "development" ? "staging" : mode;
  if (environment !== "staging" && environment !== "production") {
    throw new Error("Select the staging or production mode.");
  }

  const production = environment === "production";
  const name =
    process.env.LUCY_TELEMETRY_RESOURCE_PREFIX ||
    (production ? "lucy-telemetry" : "lucy-telemetry-staging");
  const adminHost = production ? "telemetry-admin.lucy.lc" : undefined;

  return {
    accountId,
    worker: {
      name,
      entrypoint: "./build/index.js",
      compatibilityDate: "2026-10-01",
      workersDev: !production,
      previewUrls: false,
      domains:
        adminHost === undefined
          ? []
          : [
              { hostname: "telemetry.lucy.lc", production: true, previews: false },
              { hostname: adminHost, production: true, previews: false },
            ],
      env: {
        DB: bindings.d1({ name, id: production ? productionDatabaseId : undefined }),
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
