#!/usr/bin/env bash
# Apply pipeline.sql to the local timeplusd, substituting the Hydrolix token from .env
set -euo pipefail
cd "$(dirname "$0")"
set -a; . ./.env; set +a
export TIMEPLUS_HOST="${TIMEPLUS_HOST:-localhost}" TIMEPLUS_PORT="${TIMEPLUS_PORT:-18123}"
export TIMEPLUS_USER="${TIMEPLUS_USER:-default}" TIMEPLUS_PASSWORD="${TIMEPLUS_PASSWORD:-}"
python3 - <<'PY'
import os, re, sys, urllib.request, base64
sql = open("pipeline.sql").read().replace("__HYDROLIX_SERVICE_TOKEN__", os.environ["HYDROLIX_SERVICE_TOKEN"])
sql = re.sub(r"--[^\n]*", "", sql)
url = f"http://{os.environ['TIMEPLUS_HOST']}:{os.environ['TIMEPLUS_PORT']}/"
auth = base64.b64encode(f"{os.environ['TIMEPLUS_USER']}:{os.environ['TIMEPLUS_PASSWORD']}".encode()).decode()
for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
    name = re.search(r"CREATE .*?IF NOT EXISTS ([\w.]+)", stmt)
    label = name.group(1) if name else stmt[:40]
    req = urllib.request.Request(url, data=stmt.encode(), headers={"Authorization": "Basic " + auth})
    try:
        with urllib.request.urlopen(req) as r:
            print(f"{label:<40} OK")
    except urllib.error.HTTPError as e:
        print(f"{label:<40} FAILED HTTP {e.code}\n{e.read().decode()}"); sys.exit(1)
PY
