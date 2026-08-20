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
