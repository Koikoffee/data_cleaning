#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

LOG="logs/etl-$(date +%F).log"
mkdir -p logs

# If there is an old lock that is no longer held by the process -> clean up
if [ -f .etl.lock ] && ! lsof .etl.lock >/dev/null 2>&1; then
  rm -f .etl.lock
fi

# Run pipeline with venv's python, prevent duplicate runs with flock
/usr/bin/flock -n ./.etl.lock -c '
  /home/khoi25/DE/data_cleaning/.venv/bin/python3 -m ETL.pipeline >> '"$LOG"' 2>&1
  /home/khoi25/DE/data_cleaning/.venv/bin/python3 /home/khoi25/DE/data_cleaning/analysis.py >> '"$LOG"' 2>&1
'

echo "$(date '+%F %T') END run_etl (exit=$?)" >> "$LOG"