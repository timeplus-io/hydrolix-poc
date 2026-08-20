# Timeplus → Hydrolix POC

Streams simulated log events from a local **timeplusd** into **Hydrolix** via its
HTTP streaming-ingest endpoint (`/ingest/event`), using a Timeplus HTTP external stream.

All Timeplus resources live in the `hydrolix_demo` database:

```
hydrolix_demo.logs_source (RANDOM STREAM, 5 eps)
   └─► hydrolix_demo.mv_logs_to_hydrolix (MATERIALIZED VIEW)
          └─► hydrolix_demo.hydrolix_logs (EXTERNAL STREAM, type=http)
                 └─► https://hdx-se-playpen.hydrolix.live/ingest/event  (table=timeplus.logs, transform=timeplus)
```

## Setup

1. Put the Hydrolix service-account token in `.env`: `HYDROLIX_SERVICE_TOKEN=eyJ...` (git-ignored).
2. `docker compose up -d` — starts `timeplus/timeplusd:latest`
   (host ports **18123** SQL/HTTP, **13218** REST ingest, **18463** native; shifted to avoid
   conflicts with other local Timeplus instances).
3. `./setup.sh` — applies `pipeline.sql` (creates the `hydrolix_demo` database and the three
   resources; token is substituted from `.env`). The `default` HTTP user is allowed into
   `hydrolix_demo` via `timeplusd/users.d/hydrolix_demo.yaml`, mounted by compose.

## Verify

```bash
# what timeplusd is emitting
echo "SELECT format_datetime(_tp_time,'%Y-%m-%dT%H:%M:%S.%fZ') ts, * FROM hydrolix_demo.logs_source LIMIT 3" \
  | curl -s 'http://localhost:18123/?default_format=PrettyCompact' --data-binary @-

# what Hydrolix has received (same token works on the ClickHouse-compatible /query endpoint)
./hdx.sh query "SELECT catchall['service'] s, count() FROM timeplus.logs
                WHERE timestamp > now() - INTERVAL 10 MINUTE GROUP BY s FORMAT PrettyCompact"

# manual ingest test
./hdx.sh ingest sample.ndjson
```

## Reading back from Hydrolix (url() + VIEW / TASK)

The HTTP external stream is write-only (`NOT_IMPLEMENTED` on SELECT), so reads use the
`url()` table function against Hydrolix's ClickHouse-compatible `/query` endpoint. `pipeline.sql`
creates two read-path objects:

| Object | What it does |
|---|---|
| `hydrolix_demo.hydrolix_recent` (VIEW) | Ad-hoc pull of the last 5 minutes of `timeplus.logs` with typed columns |
| `hydrolix_demo.pull_hydrolix_stats` (TASK, every 1m) | Pulls per-service/level event count + latency stats for the last minute into stream `hydrolix_demo.hydrolix_service_stats` |

```bash
# ad-hoc query through the view (each SELECT hits Hydrolix)
echo "SELECT service, level, count() AS c, round(avg(latency_ms),1) AS avg_ms
      FROM hydrolix_demo.hydrolix_recent GROUP BY service, level ORDER BY c DESC" \
  | curl -s 'http://localhost:18123/?default_format=PrettyCompact' --data-binary @-

# snapshots accumulated by the scheduled task
echo "SELECT * FROM table(hydrolix_demo.hydrolix_service_stats) ORDER BY pulled_at DESC LIMIT 20" \
  | curl -s 'http://localhost:18123/?default_format=PrettyCompact' --data-binary @-

# task management
echo "SHOW TASKS FROM hydrolix_demo" | curl -s http://localhost:18123/ --data-binary @-
echo "SYSTEM PAUSE TASK hydrolix_demo.pull_hydrolix_stats" | curl -s http://localhost:18123/ --data-binary @-
```

Gotchas for `url()` reads: **Hydrolix caches results by query text**, so a fixed URL returns the same
rows forever (the task stored identical snapshots every minute until this was fixed) — both objects
therefore build the URL with `concat(..., '/* nocache <ms> */')` using `now64(3)`, which is evaluated
on every execution; the inner SQL must be URL-encoded and end with `FORMAT JSONEachRow`;
Hydrolix uses ClickHouse camelCase function names (`toInt32OrZero`, not `to_int32_or_zero`);
the token must go through `headers('Authorization'=...)` (a `?token=` query param is not accepted);
and in this Timeplus version `CREATE TASK` takes `INTO target` *before* `AS`.

## Notes / gotchas

- **Timestamp format**: the Hydrolix `timeplus` transform's primary `timestamp` column is a
  `datetime` with format `2006-01-02T15:04:05.000Z` — it rejects numbers (int64 epoch) and
  strings without milliseconds (HTTP 400). Hence `hydrolix_logs.timestamp` is a `string`
  formatted in the MV. If Hydrolix switches the transform to `epoch_ms`, change the column
  to `int64` and emit `to_unix_timestamp64_milli(_tp_time)`.
- Current Hydrolix schema is catch-all: fields land in `catchall Map(String, Nullable(String))`
  (e.g. `catchall['level']`). Hydrolix will promote them to typed columns later.
- Hydrolix caches query results: `SELECT count() FROM timeplus.logs` may return stale 0;
  add a `WHERE timestamp > now() - INTERVAL ...` predicate when checking.
- Token is a JWT (aud `config-api`) valid until 2027-08-20; it authorizes both ingest and query.

## Teardown

```bash
docker compose down -v
```
