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
