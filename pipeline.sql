-- 0) Dedicated database for all POC resources
CREATE DATABASE IF NOT EXISTS hydrolix_demo;

-- 1) Simulated source: random log events
CREATE RANDOM STREAM IF NOT EXISTS hydrolix_demo.logs_source (
  service     string  DEFAULT ['api','auth','checkout','search','payments'][(rand(1) % 5) + 1],
  level       string  DEFAULT ['INFO','INFO','INFO','WARN','ERROR'][(rand(2) % 5) + 1],
  status      int32   DEFAULT [200,200,200,201,204,301,400,401,404,500,503][(rand(3) % 11) + 1],
  latency_ms  float64 DEFAULT round(rand_exponential(0.02), 2),
  message     string  DEFAULT 'request ' || to_string(rand(4) % 100000) || ' handled'
) SETTINGS eps = 5;

-- 2) Sink: HTTP external stream -> Hydrolix streaming ingest
--    NOTE: timestamp is a string (not int64) because the Hydrolix 'timeplus' transform
--    currently only accepts datetime strings in the form 2006-01-02T15:04:05.000Z.
CREATE EXTERNAL STREAM IF NOT EXISTS hydrolix_demo.hydrolix_logs (
  timestamp   string,
  service     string,
  level       string,
  status      int32,
  latency_ms  float64,
  message     string
)
SETTINGS
  type = 'http',
  url  = 'https://hdx-se-playpen.hydrolix.live/ingest/event?table=timeplus.logs&transform=timeplus',
  data_format = 'JSONEachRow',
  output_format_json_array_of_rows = 1,
  http_header_Content_Type = 'application/json',
  http_header_Authorization = 'Bearer __HYDROLIX_SERVICE_TOKEN__';

-- 3) Pipeline: continuously push source events into the sink
CREATE MATERIALIZED VIEW IF NOT EXISTS hydrolix_demo.mv_logs_to_hydrolix
INTO hydrolix_demo.hydrolix_logs AS
SELECT
  format_datetime(_tp_time, '%Y-%m-%dT%H:%M:%S.%fZ') AS timestamp,
  service, level, status, latency_ms, message
FROM hydrolix_demo.logs_source;

-- 4) Read path: ad-hoc VIEW over Hydrolix's ClickHouse-compatible /query endpoint (bounded pull).
--    The HTTP external stream is write-only, so reads go through the url() table function.
--    Inner SQL is URL-encoded and uses Hydrolix/ClickHouse camelCase function names;
--    Hydrolix only accepts the token via the Authorization header (headers(...) is required).
--    Hydrolix also caches query results by query text, so a per-execution nocache comment is appended.
--    Usage: SELECT service, count() AS c FROM hydrolix_demo.hydrolix_recent GROUP BY service;
CREATE VIEW IF NOT EXISTS hydrolix_demo.hydrolix_recent AS
SELECT * FROM url(
  -- Hydrolix caches results per query text; the nocache suffix makes each execution unique
  concat('https://hdx-se-playpen.hydrolix.live/query?query=SELECT%20timestamp%2C%20catchall%5B%27service%27%5D%20AS%20service%2C%20catchall%5B%27level%27%5D%20AS%20level%2C%20toInt32OrZero%28catchall%5B%27status%27%5D%29%20AS%20status%2C%20toFloat64OrZero%28catchall%5B%27latency_ms%27%5D%29%20AS%20latency_ms%2C%20catchall%5B%27message%27%5D%20AS%20message%20FROM%20timeplus.logs%20WHERE%20timestamp%20%3E%20now%28%29%20-%20INTERVAL%205%20MINUTE%20ORDER%20BY%20timestamp%20DESC%20LIMIT%201000%20FORMAT%20JSONEachRow',
         '%20%2F%2A%20nocache%20', to_string(to_unix_timestamp64_milli(now64(3))), '%20%2A%2F'),
  'JSONEachRow',
  'timestamp string, service string, level string, status int32, latency_ms float64, message string',
  headers('Authorization' = 'Bearer __HYDROLIX_SERVICE_TOKEN__')
);

-- 5) Read path: scheduled TASK that pulls per-service/level stats from Hydrolix every minute
--    and appends them to a local stream. It measures the last *completed* minute that ended
--    >= 60s ago (minute_start), not a rolling 60s window: Hydrolix makes fresh rows visible
--    out of order over ~15-20s, so a rolling window undercounts by 15-25%.
--    Usage: SELECT * FROM table(hydrolix_demo.hydrolix_service_stats) ORDER BY pulled_at DESC;
CREATE STREAM IF NOT EXISTS hydrolix_demo.hydrolix_service_stats (
  pulled_at       datetime64(3, 'UTC'),
  minute_start    datetime('UTC'),
  service         string,
  level           string,
  events          uint64,
  avg_latency_ms  float64,
  max_latency_ms  float64
);

CREATE TASK IF NOT EXISTS hydrolix_demo.pull_hydrolix_stats
SCHEDULE 1m
TIMEOUT 30s
INTO hydrolix_demo.hydrolix_service_stats
AS
SELECT now64(3, 'UTC') AS pulled_at, parse_datetime_best_effort(minute_start) AS minute_start,
       service, level, events, avg_latency_ms, max_latency_ms
FROM url(
  -- Hydrolix caches results per query text; the nocache suffix makes each execution unique
  concat('https://hdx-se-playpen.hydrolix.live/query?query=SELECT%20toStartOfMinute%28now%28%29%29%20-%20INTERVAL%202%20MINUTE%20AS%20minute_start%2C%20catchall%5B%27service%27%5D%20AS%20service%2C%20catchall%5B%27level%27%5D%20AS%20level%2C%20count%28%29%20AS%20events%2C%20avg%28toFloat64OrZero%28catchall%5B%27latency_ms%27%5D%29%29%20AS%20avg_latency_ms%2C%20max%28toFloat64OrZero%28catchall%5B%27latency_ms%27%5D%29%29%20AS%20max_latency_ms%20FROM%20timeplus.logs%20WHERE%20timestamp%20%3E%3D%20toStartOfMinute%28now%28%29%29%20-%20INTERVAL%202%20MINUTE%20AND%20timestamp%20%3C%20toStartOfMinute%28now%28%29%29%20-%20INTERVAL%201%20MINUTE%20GROUP%20BY%20minute_start%2C%20service%2C%20level%20FORMAT%20JSONEachRow',
         '%20%2F%2A%20nocache%20', to_string(to_unix_timestamp64_milli(now64(3))), '%20%2A%2F'),
  'JSONEachRow',
  'minute_start string, service string, level string, events uint64, avg_latency_ms float64, max_latency_ms float64',
  headers('Authorization' = 'Bearer __HYDROLIX_SERVICE_TOKEN__')
);

