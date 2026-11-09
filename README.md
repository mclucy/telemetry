# Lucy telemetry backend

Cloudflare Worker backend for lucy's telemetry service. Rust (`workers-rs`), D1, R2, Queues, and a scheduled cleanup job. The repository is self-contained: one Worker serves ingestion, the queue consumer, and retention cleanup.

## Request flow

```text
POST /v1/records
  validate every record (bounds, UUIDs, RFC3339 timestamps, enums)
  report body -> gzip -> R2 (reports/<report_id>/<sha256>.txt.gz)
  report metadata -> D1 reports row (state=pending)
  events + report metadata -> one durable Queues sendBatch
  response lists only records durably accepted

Queue consumer (at-least-once, idempotent)
  event -> INSERT OR IGNORE event + context snapshots in one atomic D1 batch
  report -> check R2 object metadata, mark reports.state=stored

GET /health
  dependency-free reachability check

Scheduled cleanup (hourly)
  expired events/contexts in bounded D1 batches
  expired reports atomically claimed as state=deleting
  R2 bodies deleted, then metadata tombstoned and removed
```

## HTTP API

All responses set `cache-control: no-store`.

Production ingestion uses `https://telemetry.lucy.lc`. Staging uses its `workers.dev` URL.

### `GET /health`

`200 {"status":"ok","service":"lucy-telemetry"}`. This route touches no binding, so a fast reachability check does not depend on D1, R2, or Queues.

### `POST /v1/records`

Requires `Content-Type: application/json`.

```json
{
  "records": [
    { "id": "<uuid>", "type": "event", "payload": {} },
    { "id": "<uuid>", "type": "report", "payload": {} }
  ]
}
```

Event payload:

| field                              | requirement                                                 |
| ---------------------------------- | ----------------------------------------------------------- |
| `invocation_id`, `installation_id` | UUID                                                        |
| `occurred_at`                      | RFC3339, not more than the allowed clock skew in the future |
| `command`                          | 1–128 bytes, trusted command path without arguments         |
| `lucy_version`, `lucy_commit`      | up to 64 bytes; may be empty when unknown                   |
| `channel`                          | 1–32 bytes                                                  |
| `os`, `arch`                       | 1–64 / 1–32 bytes                                           |
| `duration_ms`                      | optional integer, 0–604800000                               |
| `outcome`                          | `success`, `failure`, or `canceled`                         |
| `error_code`                       | optional, up to 64 bytes (empty is accepted)                |
| `schema_version`                   | integer 1–1000                                              |
| `collection_policy`                | `default` or `opt_in`                                       |
| `contexts`                         | optional array, at most 16                                  |

Context item:

```json
{
  "type": "workspace",
  "status": "present",
  "data": {
    "product": "fabric",
    "product_version": "0.16.9",
    "minecraft_version": "1.21.4",
    "packages": 42
  }
}
```

`type` is `workspace`, `java`, or `agent`. `status` is `present`, `absent`, or `failed`. `data` is required and non-null for `present`, and must be omitted/null for `absent` or `failed`. Serialized `data` is at most 8 KiB.

`data` is a typed allowlist, not arbitrary JSON: unknown fields are rejected. Allowed fields by type (all are optional, but only these names are accepted):

| type        | fields                                                                                                                            |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `workspace` | `product` (1–64 identifier chars), `product_version` and `minecraft_version` (1–64 version chars), `packages` (integer 0–1000000) |
| `java`      | `major` (integer 1–255), `version` (1–64 version chars), `distribution` (1–64 identifier chars)                                   |
| `agent`     | `detected` (boolean)                                                                                                              |

Identifier characters are ASCII alphanumerics and `._-+:`. Version strings contain at least one digit and use ASCII alphanumerics and `.+-_`.

Examples for the other two context types:

```json
{"type":"java","status":"present","data":{"major":21,"version":"21.0.4","distribution":"temurin"}}
{"type":"agent","status":"present","data":{"detected":true}}
```

Report payload:

| field               | requirement                                             |
| ------------------- | ------------------------------------------------------- |
| `report_id`         | UUID; must equal the record `id`                        |
| `installation_id`   | UUID                                                    |
| `invocation_id`     | optional UUID                                           |
| `kind`              | `failure`, `panic`, or `agent_feedback`                 |
| `agent`, `model`    | optional, up to 128 bytes                               |
| `created_at`        | RFC3339, not in the future beyond clock skew            |
| `schema_version`    | integer 1–1000                                          |
| `collection_policy` | `default` or `opt_in`                                   |
| `body`              | sanitized UTF-8 text, non-empty, at most 64 KiB; no NUL |

The client must filter reports before queueing. The backend filters them again before storage. Filtering is not a guarantee that all sensitive data is removed.

Response:

```json
{
  "accepted": ["<uuid>"],
  "rejected": [
    { "id": "<uuid>", "reason": "invalid_field", "field": "payload.command" }
  ],
  "retryable": true
}
```

`accepted` is always present and may be empty. `retryable` appears only on a transient `503`.

| status | meaning                                                                                                                                     |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `200`  | The request was processed; `accepted` lists durable IDs and/or `rejected` lists final per-record decisions.                                 |
| `400`  | The batch itself was malformed, or every record was invalid. A report-ID conflict alone is a per-record `200` rejection, not a batch error. |
| `413`  | Request body exceeds `max_request_bytes`.                                                                                                   |
| `415`  | Missing/unsupported content type or content encoding.                                                                                       |
| `503`  | No ID is reported accepted because report storage or durable queueing failed transiently. Retry the same IDs later.                         |

Acceptance semantics: an event is accepted after its queue message is durably sent. A report is accepted after its body is in R2, pending metadata is in D1, and its queue message is durably sent. If report storage or queueing fails transiently, the whole request returns `503` with `accepted: []` even when some records were already made durable; the client retries the batch and every write is idempotent. Report rows remain `pending` and R2 objects remain until a retry succeeds or the pending grace period expires.

Duplicate IDs are idempotent. A repeated event ID does not create a second row. A stored report with the same body digest is accepted without rewriting R2; a pending retry may rewrite the same body. A different digest returns `report_conflict`.

## Limits and configuration

The deployment writes one `CONFIG_JSON` text binding. Missing configuration uses the safe defaults below. Malformed or out-of-range configuration fails closed with `500 not_configured` rather than silently clamping.

| field                        | default   | accepted range                                   |
| ---------------------------- | --------- | ------------------------------------------------ |
| `environment`                | `unknown` | 1–64 bytes                                       |
| `max_request_bytes`          | 262144    | 1024–1048576                                     |
| `max_records`                | 50        | 1–100                                            |
| `max_record_bytes`           | 98304     | 1024–122880                                      |
| `max_report_body_bytes`      | 65536     | 1–`max_record_bytes`                             |
| `max_context_bytes`          | 8192      | 0–32768                                          |
| `max_contexts`               | 16        | 1–32                                             |
| `event_retention_days`       | 30        | 1–30 (hard privacy ceiling)                      |
| `report_retention_days`      | 14        | 1–14 (hard privacy ceiling)                      |
| `pending_report_grace_hours` | 336       | must equal `report_retention_days * 24` (24–336) |
| `cleanup_batch_size`         | 100       | 1–500                                            |
| `cleanup_max_batches`        | 10        | 1–100                                            |
| `max_clock_skew_seconds`     | 604800    | 0–2592000                                        |

`max_record_bytes` includes the record envelope (`id`, `type`, `payload`) and is measured as compact JSON. Queue messages are independently checked against the Cloudflare Queues per-message ceiling before they are sent.

## Bindings

| binding       | type           | purpose                                    |
| ------------- | -------------- | ------------------------------------------ |
| `DB`          | D1             | events, context snapshots, report metadata |
| `REPORTS`     | R2             | private report body storage                |
| `RECORDS`     | Queue producer | event payloads and report metadata         |
| `CONFIG_JSON` | text           | runtime limits and retention               |

The queue consumer is the same Worker. Its trigger and DLQ are declared in the generated Build Output and applied with `pnpm triggers:staging` or `pnpm triggers:production`.

## Retention and failure behavior

Retention is a privacy policy, not a capacity tuning knob. Events and contexts expire after **30 days**, reports after **14 days**. Expired records are no longer readable; scheduled D1 cleanup and R2 lifecycle deletion are asynchronous. `CONFIG_JSON` rejects longer retention and requires pending grace to match the report retention period.

- **Events/contexts:** the hourly cron deletes expired events in bounded batches; context rows are deleted first in the same atomic D1 batch.
- **Reports:** the cron atomically claims expired `stored` rows and `pending` rows older than the report retention window as `state = deleting`, deletes their R2 objects, then tombstones the IDs and deletes metadata in one D1 transaction. Claimed rows are excluded immediately. If R2 deletion fails, the claim lease expires and the next run retries it. The pending grace equals the report retention, so an accepted report is not cleaned early.
- **Deletion tombstones:** when retention deletes a report, its ID is tombstoned in the same transaction. A delayed queue message or client retry cannot recreate an expired report row or resurrect its body.
- **R2 lifecycle:** an R2 lifecycle rule on the `reports/` prefix is required as a hard backstop for report bodies, including objects orphaned by a crash between R2 put and D1 insert. Its `maxAge` must be at most 14 days (`1209600` seconds); a longer rule would outlive the privacy window.
- **R2 failure:** the report is rejected with `storage_unavailable`; no metadata or queue message is created. R2 is written before D1 and Queues.
- **D1 failure:** the request fails closed; any R2 object without metadata is cleaned by the lifecycle rule.
- **Queue failure:** the response is `503` with `accepted: []`. Report rows are `pending`; retries heal them only while the R2 body still exists. The DLQ captures messages that exhaust consumer retries.
- **Duplicate delivery:** the consumer uses stable IDs and idempotent SQL. Event and context writes are one D1 batch; report metadata is inserted or updated only when the body digest matches.
- **Consumer failure:** the message is retried; after the configured retries it moves to the DLQ. Invalid internal messages also reach the DLQ after retries.

### Pending and DLQ recovery limits

Report recovery is bounded by the retention window measured from the first receipt. Expired reports are hidden immediately; scheduled cleanup deletes their metadata and bodies, with the R2 lifecycle as a backstop. Queue and DLQ retention do not extend the recovery window. Do not restore expired telemetry from backups, D1 time-travel, or a DLQ.

### Monitoring expiry backlog

Cleanup is deliberately bounded: one invocation deletes at most `cleanup_batch_size * cleanup_max_batches` event rows and the same number of report rows. Never replace this with a one-off unbounded sweep; such a delete can exhaust the invocation before it finishes. Instead, compare the expired-row counts below across hourly runs. Run them with the production retention and pending values substituted for the defaults:

```sql
SELECT
  (SELECT COUNT(*) FROM events
     WHERE received_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-30 days'))
    AS expired_events,
  (SELECT COUNT(*) FROM reports
     WHERE state = 'stored'
       AND received_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-14 days'))
    AS expired_reports,
  (SELECT COUNT(*) FROM reports
     WHERE state = 'pending'
       AND received_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-336 hours'))
    AS stale_pending;
```

```sh
cf d1 raw "$D1_DATABASE_ID" --sql "<query above>"
```

Expired counts should return to zero after each hourly run. The cleanup invocation also logs fixed `cleanup_*` lines including the deleted row count and the observed expired backlog; check those logs alongside these queries. A count that keeps growing, or survives two consecutive runs, means cleanup throughput is below intake or the cron is failing: check Worker logs for `cleanup_*` errors, confirm the scheduled trigger, and reduce intake (lower `max_records` / `max_request_bytes` or tighten edge rate limiting) until the backlog drains. Prefer a more frequent cron over a larger batch cap. Sustained expired rows are a privacy-window incident, not just a storage issue.

`reset-id` is a client operation. It rotates the client installation ID and resets client-local statistics; it does not delete data already in D1 or R2. Disabling telemetry purges the client's local queue without rotating the installation ID. Server-side data expires only through the retention policy.

## Privacy and security

- No IP address, `request.cf`, `User-Agent`, or request header is persisted.
- No payload or report body is logged. Logs carry counts, system message IDs, and fixed error categories only.
- R2 is private; no public bucket or custom domain is configured.
- Report bodies are gzip-compressed under keys containing the report UUID and plaintext SHA-256 digest. The consumer checks R2 metadata before promotion.
- The Worker serves one public hostname, `telemetry.lucy.lc`, in production and `workers.dev` in staging.
- Workers observability invocation logs are disabled in the generated Build Output; only application log lines are kept.

## Development

Prerequisites:

```sh
cargo install worker-build --version 0.8.7 --locked
pnpm install --frozen-lockfile
```

`rust-toolchain.toml` pins Rust 1.91.0 and installs the `wasm32-unknown-unknown` target on first use.

Task entrypoints are pnpm scripts in `package.json`:

```sh
pnpm check
pnpm build:staging
```

`pnpm check` runs `cargo check --target wasm32-unknown-unknown`. The build, deploy-check, deploy, and triggers tasks exist as `:staging` and `:production` variants; `build:<mode>` produces the Cloudflare Build Output under `.cloudflare/output/v0`, and `pnpm migrate` applies migrations to the D1 database named by `LUCY_TELEMETRY_D1_ID`.

Deploys carry no secrets: the Worker has no secret bindings, so `pnpm deploy:production` uploads the Build Output as-is.

## Continuous integration

CI runs in GitHub Actions (`.github/workflows/deploy.yml`). Pull requests build the Worker and run a deploy dry run; pushes to `main` deploy the prebuilt Build Output. The workflow installs Rust 1.91, caches `~/.cargo` and `target/`, installs `worker-build` 0.8.7, and deploys the result with `--prebuilt`.

Set `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` as repository secrets for the deploy step. The Workers Builds trigger is removed so a push is not deployed twice.

> **Temporary:** Workers Builds does not provide Rust in its build image and does not cache `~/.cargo` or `target/`, so building there recompiles `worker-build` on every push. Build in Actions until Cloudflare offers native Rust build support or caching. Track [workers-rs#1021](https://github.com/cloudflare/workers-rs/issues/1021) and [workers-rs#1069](https://github.com/cloudflare/workers-rs/pull/1069).
