#!/usr/bin/env bash
set -euo pipefail

VERSION="$(curl -fsS --max-time 10 http://127.0.0.1:8000/ \
  | /opt/pump-hunter/venv/bin/python -c 'import json,sys; print(json.load(sys.stdin).get("version","unknown"))')"

STAMP="$(date -u '+%Y%m%d_%H%M%S')"
REPORT="/home/opc/pump_hunter_${VERSION}_${STAMP}.txt"

{
echo "=================================================="
echo " PUMP HUNTER REPORT"
echo "=================================================="
echo "VERSION: $VERSION"
echo "DATA UTC: $(date -u)"
echo

echo "========== ROOT =========="
curl -sS --max-time 15 http://127.0.0.1:8000/
echo
echo

echo "========== V31 ENGINE =========="
curl -sS --max-time 30 http://127.0.0.1:8000/v31-engine \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== TELEMETRY =========="
curl -sS --max-time 30 "http://127.0.0.1:8000/telemetry?limit=300" \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== SIGNAL ENGINE =========="
curl -sS --max-time 30 http://127.0.0.1:8000/signal-engine \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== SIGNALS =========="
curl -sS --max-time 30 http://127.0.0.1:8000/signals \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== QUALITY =========="
curl -sS --max-time 30 http://127.0.0.1:8000/quality \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== RE-ENTRIES =========="
curl -sS --max-time 30 http://127.0.0.1:8000/re-entries \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== FEED HEALTH =========="
curl -sS --max-time 30 http://127.0.0.1:8000/feed-health \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== COVERAGE =========="
curl -sS --max-time 30 http://127.0.0.1:8000/coverage \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== HISTORY REPORT =========="
curl -sS --max-time 30 http://127.0.0.1:8000/history-report \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== HOT REPORT =========="
curl -sS --max-time 30 http://127.0.0.1:8000/hot-report \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== PERSISTENCE =========="
curl -sS --max-time 30 http://127.0.0.1:8000/persistence \
  | /opt/pump-hunter/venv/bin/python -m json.tool
echo

echo "========== MEMORY =========="
free -h
echo

echo "========== PROCESS =========="
ps -o pid,etime,%cpu,%mem,rss,vsz,cmd -C python
echo

echo "========== SYSTEMD =========="
sudo systemctl status pump-hunter --no-pager -l
echo

echo "========== CURRENT SESSION LOGS =========="
START_TS="$(sudo systemctl show pump-hunter -p ActiveEnterTimestamp --value)"
sudo journalctl -u pump-hunter --since "$START_TS" --no-pager -l
echo

echo "========== CURRENT SESSION ERRORS =========="
sudo journalctl -u pump-hunter --since "$START_TS" --no-pager -l \
  | grep -Ei "error|exception|traceback|failed|warning|oom|killed" || true

echo
echo "=================================================="
echo " KONIEC RAPORTU"
echo "=================================================="

} > "$REPORT" 2>&1

echo
echo "=== RAPORT WYGENEROWANY ==="
ls -lh "$REPORT"

echo
echo "=== WYSYLAM DO GITHUB ==="
/home/opc/.pump-hunter/upload_report.sh "$REPORT"

echo
echo "=============================================="
echo " GOTOWE"
echo "=============================================="
echo "LOCAL:  $REPORT"
echo "GITHUB: Reports/$(basename "$REPORT")"
echo "LATEST: Reports/latest_report.txt"
