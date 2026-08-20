#!/usr/bin/env bash
# Helper for Hydrolix POC. Usage:
#   ./hdx.sh ingest <file.ndjson|file.json>   # POST to /ingest/event (table timeplus.logs, transform timeplus)
#   ./hdx.sh query "<SQL>"                     # run SQL on /query (ClickHouse-compatible)
set -euo pipefail
cd "$(dirname "$0")"
set -a; . ./.env; set +a
HDX_HOST="${HDX_HOST:-https://hdx-se-playpen.hydrolix.live}"
HDX_TABLE="${HDX_TABLE:-timeplus.logs}"
HDX_TRANSFORM="${HDX_TRANSFORM:-timeplus}"
case "${1:-}" in
  ingest)
    curl -sS -w '\nHTTP %{http_code}\n' -X POST "$HDX_HOST/ingest/event" \
      -H 'Content-Type: application/json' -H "x-hdx-table: $HDX_TABLE" -H "x-hdx-transform: $HDX_TRANSFORM" \
      -H "Authorization: Bearer $HYDROLIX_SERVICE_TOKEN" --data-binary @"$2" ;;
  query)
    # Hydrolix caches results per query text; append a unique comment so every run is fresh.
    curl -sS "$HDX_HOST/query" -H "Authorization: Bearer $HYDROLIX_SERVICE_TOKEN" \
      --data-binary "$2 /* nocache $(date +%s%N) */"; echo ;;
  *) sed -n '2,4p' "$0"; exit 1 ;;
esac
