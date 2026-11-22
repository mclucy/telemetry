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

Production ingestion uses `https://telemetry.lucy.lc`.

### `GET /health`

`200 {"status":"ok","service":"lucy-telemetry"}`. This route touches no binding, so a fast reachability check does not depend on D1, R2, or Queues.

### `POST /v1/records`

Requires `Content-Type: application/json`.

```json
{
  "records": [
    {
      "id": "<uuid>",
      "type": "event",
      "payload": {}
    },
    { "id": "<uuid>", "type": "report", "payload": {} }
  ]
}
```

Event payload:

| field                              | requirement                                                 |
| ---------------------------------- | ----------------------------------------------------------- |
| `invocation_id`, `installation_id` | UUID                                                        |
| `occurred_at`                      | RFC3339, not more than the allowed clock skew in the future |
| `operation`                        | 1–128 bytes, trusted command path without arguments         |
| `lucy_version`, `lucy_commit`      | up to 64 bytes; may be empty when unknown                   |
| `channel`                          | 1–32 bytes                                                  |
| `os`, `arch`                       | 1–64 / 1–32 bytes                                           |
| `duration_ms`                      | optional integer, 0–604800000                               |
| `outcome`                          | `success`, `failure`, or `canceled`                         |
| `error_code`                       | optional, up to 64 bytes (empty is accepted)                |
| `schema_version`                   | integer 1–1000                                              |
| `contexts`                         | optional array, at most 16                                  |

Context item:

```json
{
  "type": "workspace",
  "status": "present",
  "version": 1,
  "data": {
    "product": "fabric",
    "product_version": "0.16.9",
    "minecraft_version": "1.21.4",
    "packages": 42
  }
}
```

`status` is `present`, `absent`, or `failed`. `present` requires non-null `data`. `absent` and `failed` have no data and can include a fixed availability `reason`. The optional `version` identifies the shape of the context independently of the event protocol. Serialized `data` is at most 8 KiB.

The service validates typed fields rather than storing arbitrary JSON:

| type         | fields                                                                                                                                                                          |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `workspace`  | `product` (1–64 identifier chars), `product_version` and `minecraft_version` (1–64 version chars), `packages` (integer 0–1000000)                                               |
| `java`       | `major` (integer 1–255), `version` (1–64 version chars), `distribution` (1–64 identifier chars)                                                                                 |
| `agent`      | `detected` (boolean)                                                                                                                                                            |
| `invocation` | `intent` (`command` or `invalid_usage`) and optional bounded `arguments`                                                                                                        |
| `execution`  | Bounded phase durations and state, `active_phase`, `cancel_reason` or failure category, optional `input_wait_ms` (integer), `files`, and observed HTTP and workspace lock times |

`prompt`, `scale`, `warnings`, `collection`, and `transport` are not collected contexts. A payload may not supply them, and no record stores them.

`active_phase` and each phase key are normalized names matching ASCII `[a-z0-9][a-z0-9_-]{0,63}`. A client replaces ASCII spaces with underscores before serializing; the backend validates the shape rather than a fixed vocabulary.

Slowness is derived from `duration_ms >= 30000` in queries and the dashboard; it is not a wire or stored field.

Invocation arguments:

```json
{
  "intent": "command",
  "arguments": [
    { "name": "core", "kind": "selector", "value": "paper@1.21.4" },
    { "name": "arg-1", "kind": "text" },
    { "name": "yes", "kind": "boolean", "value": true }
  ]
}
```

`arguments` holds at most 32 items. Each `name` is a Cobra flag name or the positional slot `arg-N` (1-based), matching ASCII `[a-z0-9][a-z0-9-]{0,63}`. `kind` is `boolean`, `number`, `selector`, `version`, `text`, or `list`. `value` is omitted for `text` and `list`, and otherwise admits only:

- a boolean;
- a finite JSON number with magnitude at most 9007199254740991;
- a normalized version: 1–4 dot-separated ASCII segments of 1–4 digits, or `[0-9]{2}w[0-9]{2}[a-z]`;
- an exact safe selector: `vanilla`, `paper`, `fabric`, `folia`, `purpur`, `forge`, `neoforge`, `quilt`, `spigot`, `bukkit`, `velocity`, `waterfall`, `modrinth`, `curseforge`, `hangar`, `github`, `release`, `beta`, `alpha`, `stable`, `auto`, `latest`, `none`, `always`, `never`, `off`, `on`, or `snapshot`, optionally refined as `<selector>@<normalized version>` for example `paper@232` or `fabric@0.16.9`.

The client records a value only for a parsed declaration with a normalizable type. It never sends arbitrary text, raw argv, paths, URLs, tokens, environment values, prompt input, or file contents.

Non-argument identifier fields use ASCII alphanumerics and `._-+:`. Non-argument version fields contain at least one digit and use ASCII alphanumerics and `.+-_`. Argument `version` values use the stricter normalized syntax above.

Examples for Java and agent context:

```json
{"type":"java","status":"present","data":{"major":21,"version":"21.0.4","distribution":"temurin"}}
{"type":"agent","status":"present","data":{"detected":true}}
```

Report payload:

| field             | requirement                                             |
| ----------------- | ------------------------------------------------------- |
| `report_id`       | UUID; must equal the record `id`                        |
| `installation_id` | UUID                                                    |
| `invocation_id`   | optional UUID                                           |
| `kind`            | `failure`, `panic`, or `agent_feedback`                 |
| `agent`, `model`  | optional, up to 128 bytes                               |
| `created_at`      | RFC3339, not in the future beyond clock skew            |
| `schema_version`  | integer 1–1000                                          |
| `body`            | sanitized UTF-8 text, non-empty, at most 64 KiB; no NUL |

The client must filter reports before queueing. The backend filters them again before storage. Filtering is not a guarantee that all sensitive data is removed.

Response:

```json
{
  "accepted": ["<uuid>"],
  "rejected": [{ "id": "<uuid>", "reason": "invalid_field", "field": "payload.operation" }],
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

| field                    | default   | accepted range       |
| ------------------------ | --------- | -------------------- |
| `environment`            | `unknown` | 1–64 bytes           |
| `max_request_bytes`      | 262144    | 1024–1048576         |
| `max_records`            | 50        | 1–100                |
| `max_record_bytes`       | 98304     | 1024–122880          |
| `max_report_body_bytes`  | 65536     | 1–`max_record_bytes` |
| `max_context_bytes`      | 8192      | 0–32768              |
| `max_contexts`           | 16        | 1–32                 |
| `cleanup_batch_size`     | 100       | 1–500                |
| `cleanup_max_batches`    | 10        | 1–100                |
| `max_clock_skew_seconds` | 604800    | 0–2592000            |

Record retention is not configuration. Events, their context snapshots, and report bodies expire 30 days after receipt; `CONFIG_JSON` cannot widen that window, and cleanup throughput knobs affect deletion speed only.

`max_record_bytes` includes the record envelope (`id`, `type`, `payload`) and is measured as compact JSON. Queue messages are independently checked against the Cloudflare Queues per-message ceiling before they are sent.

## Bindings

| binding       | type           | purpose                                    |
| ------------- | -------------- | ------------------------------------------ |
| `DB`          | D1             | events, context snapshots, report metadata |
| `REPORTS`     | R2             | private report body storage                |
| `RECORDS`     | Queue producer | event payloads and report metadata         |
| `CONFIG_JSON` | text           | runtime limits                             |

The queue consumer is the same Worker. Its trigger and DLQ are declared in the generated Build Output and applied by `cf deploy`.

## Retention and failure behavior

Retention is a fixed privacy policy, not a capacity tuning knob. Events, their context snapshots, and reports expire **30 days after receipt**. Expired records are no longer readable; scheduled D1 cleanup and R2 lifecycle deletion are asynchronous.

- **Events/contexts:** the hourly cron deletes events received before the 30-day cutoff in bounded batches; context rows are deleted first in the same atomic D1 batch.
- **Reports:** the cron atomically claims `stored` rows older than 30 days and `pending` rows older than the pending grace window as `state = deleting`, deletes their R2 objects, then tombstones the IDs and deletes metadata in one D1 transaction. Claimed rows are excluded immediately. If R2 deletion fails, the claim lease expires and the next run retries it. The pending grace is 14 days, the maximum queue delivery window, so it never exceeds the record lifetime and no message can verify a pending row after that window.
- **Deletion tombstones:** when retention deletes a report, its ID is tombstoned in the same transaction and retained for 15 days, outliving the queue delivery window. A delayed queue message or client retry cannot recreate an expired report row or resurrect its body.
- **R2 lifecycle:** a lifecycle rule on the `reports/` prefix is required as a hard backstop for report bodies, including objects orphaned by a crash between R2 put and D1 insert. `deploy/r2-lifecycle.example.json` sets `maxAge` to 30 days (`2592000` seconds) for that prefix only. Keep it at 30 days: a longer rule would outlive the privacy window, a shorter rule would remove bodies that a pending retry can still verify, and the rule must not cover objects outside `reports/`.
- **R2 failure:** the report is rejected with `storage_unavailable`; no metadata or queue message is created. R2 is written before D1 and Queues.
- **D1 failure:** the request fails closed; any R2 object without metadata is cleaned by the lifecycle rule.
- **Queue failure:** the response is `503` with `accepted: []`. Report rows are `pending`; retries heal them only while the R2 body still exists. The DLQ captures messages that exhaust consumer retries.
- **Duplicate delivery:** the consumer uses stable IDs and idempotent SQL. Event and context writes are one D1 batch; report metadata is inserted or updated only when the body digest matches.
- **Intake pressure:** the hourly ledger counts records denied by the intake cap. These are delivery attempts, not unique lost events. A client may retry and deliver the same record later.
- **Consumer failure:** the message is retried; after the configured retries it moves to the DLQ. Invalid internal messages also reach the DLQ after retries.

### Pending and DLQ recovery limits

Report recovery is bounded by the 30-day retention window measured from first receipt (`received_at`), not event occurrence. A pending row is removed after the 14-day queue delivery window because no message can verify it later. Expired reports are hidden immediately; scheduled cleanup deletes their metadata and bodies, with the R2 lifecycle as a backstop. Queue and DLQ retention do not extend the recovery window. Do not restore expired telemetry from backups, D1 time-travel, or a DLQ.

### Monitoring expiry backlog

Cleanup is deliberately bounded: one invocation deletes at most `cleanup_batch_size * cleanup_max_batches` event rows and the same number of report rows. Never replace this with a one-off unbounded sweep; such a delete can exhaust the invocation before it finishes. Instead, compare the expired-row counts below across hourly runs. The cutoffs match the fixed 30-day record horizon and the 14-day pending grace:

```sql
SELECT
  (SELECT COUNT(*) FROM events
     WHERE received_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-30 days'))
    AS expired_events,
  (SELECT COUNT(*) FROM reports
     WHERE state = 'stored'
       AND received_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-30 days'))
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
- The Worker serves one public hostname, `telemetry.lucy.lc`.
- Workers observability invocation logs are disabled in the generated Build Output; only application log lines are kept.

## Development

Prerequisites: Rust (pinned below), [jq](https://jqlang.github.io/jq/), and pnpm.

```sh
pnpm install --frozen-lockfile
```

`pnpm build` installs the `worker-build` version matching the locked `worker` crate before compiling.

Task entrypoints are pnpm scripts in `package.json`:

```sh
pnpm check
pnpm build
```

`pnpm check` runs `cargo check --target wasm32-unknown-unknown`. `build` produces the Cloudflare Build Output under `.cloudflare/output/v0`, and `deploy` deploys it. `trigger` reapplies trigger configuration without uploading a new version. `pnpm migrate` applies migrations to the D1 database named by `LUCY_TELEMETRY_D1_ID`.

Deploys carry no secrets: the Worker has no secret bindings, so `pnpm deploy` uploads the Build Output as-is.

## Continuous integration

CI runs in GitHub Actions (`.github/workflows/deploy.yml`). Pull requests build the Worker and run a deploy dry run; pushes to `main` deploy the prebuilt Build Output. The workflow installs Rust, caches `~/.cargo` and `target/`, runs `pnpm build` (which matches `worker-build` to the locked `worker` crate), and deploys the result with `--prebuilt`.

Run `pnpm migrate` before deploying the Worker. The consolidated baseline initializes a new D1 database. Use an empty database for this cutover: applying the baseline to a database with `events.command` fails without modifying its telemetry.

This release is a breaking wire cutover, not a dual-protocol rollover. The Worker accepts only the new payload (`operation`, structured invocation arguments, no `collection_policy` or `upload`, and only the retained contexts); it has no shim for the old shape. Released alpha.9 clients still send the legacy format, so their uploads are rejected until they upgrade. Do not treat a production deploy as a safe live cutover: sequence the migration, Worker, dashboard, and client release, and expect an intake gap from clients that have not upgraded.

Set `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` as repository secrets for the deploy step. The Workers Builds trigger is removed so a push is not deployed twice.

> **Temporary:** Workers Builds does not provide Rust in its build image and does not cache `~/.cargo` or `target/`, so building there recompiles `worker-build` on every push. Build in Actions until Cloudflare offers native Rust build support or caching. Track [workers-rs#1021](https://github.com/cloudflare/workers-rs/issues/1021) and [workers-rs#1069](https://github.com/cloudflare/workers-rs/pull/1069).
