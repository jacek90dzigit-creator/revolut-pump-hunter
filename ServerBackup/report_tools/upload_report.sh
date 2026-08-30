#!/usr/bin/env bash
set -euo pipefail

TOKEN_FILE="/home/opc/.pump-hunter/github_token"
REPO="jacek90dzigit-creator/revolut-pump-hunter"
PY="/opt/pump-hunter/venv/bin/python"

if [ ! -f "$TOKEN_FILE" ]; then
  echo "BRAK TOKENU: $TOKEN_FILE"
  exit 1
fi

if [ "$#" -lt 1 ]; then
  echo "Uzycie: $0 /sciezka/do/raportu.txt"
  exit 1
fi

LOCAL_FILE="$1"

if [ ! -f "$LOCAL_FILE" ]; then
  echo "BRAK PLIKU: $LOCAL_FILE"
  exit 1
fi

TOKEN="$(cat "$TOKEN_FILE")"
BASENAME="$(basename "$LOCAL_FILE")"

DATE_DIR="$(date -u '+%Y-%m-%d')"
HOUR="$(date -u '+%H')"

if [ "$HOUR" -lt 12 ]; then
  WINDOW="00-12"
else
  WINDOW="12-24"
fi

REMOTE_FILE="Reports/${DATE_DIR}/${WINDOW}/${BASENAME}"
LATEST_SUMMARY="Reports/latest_summary.json"
LATEST_PATH="Reports/latest_report_path.txt"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

get_sha() {
  local remote_path="$1"

  curl -sS \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/repos/$REPO/contents/$remote_path" \
  | "$PY" -c '
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get("sha", ""))
except Exception:
    print("")
'
}

upload_file() {
  local local_path="$1"
  local remote_path="$2"
  local message="$3"

  local sha
  sha="$(get_sha "$remote_path")"

  local payload_file="$TMP_DIR/payload.json"

  "$PY" - "$local_path" "$message" "$sha" > "$payload_file" <<'PY'
import sys, json, base64

path = sys.argv[1]
message = sys.argv[2]
sha = sys.argv[3]

with open(path, "rb") as f:
    content = base64.b64encode(f.read()).decode("ascii")

payload = {
    "message": message,
    "content": content
}

if sha:
    payload["sha"] = sha

json.dump(payload, sys.stdout)
PY

  local code
  code="$(curl -sS \
    -o "$TMP_DIR/github_response.json" \
    -w "%{http_code}" \
    -X PUT \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -H "Content-Type: application/json" \
    --data-binary "@$payload_file" \
    "https://api.github.com/repos/$REPO/contents/$remote_path")"

  echo "$remote_path -> HTTP $code"

  if [ "$code" != "200" ] && [ "$code" != "201" ]; then
    cat "$TMP_DIR/github_response.json"
    echo
    exit 1
  fi
}

echo "=== TWORZE SUMMARY ==="

SUMMARY_FILE="$TMP_DIR/latest_summary.json"

"$PY" - "$LOCAL_FILE" "$REMOTE_FILE" > "$SUMMARY_FILE" <<'PY'
import sys
import json
import re
from pathlib import Path
from datetime import datetime, timezone

report_path = Path(sys.argv[1])
remote_path = sys.argv[2]

text = report_path.read_text(errors="replace")

summary = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "full_report_path": remote_path,
    "source_file": report_path.name,
    "source_size_bytes": report_path.stat().st_size,
    "version": None,
    "engine": {},
    "signals": {},
    "learner": {},
    "order_flow": {},
    "system": {},
    "errors": []
}

m = re.search(r'"version"\s*:\s*"([^"]+)"', text)
if m:
    summary["version"] = m.group(1)

def grab_int(pattern):
    m = re.search(pattern, text, re.I)
    return int(m.group(1)) if m else None

def grab_float(pattern):
    m = re.search(pattern, text, re.I)
    return float(m.group(1)) if m else None

summary["engine"]["market_context_hours"] = grab_int(
    r'"market_context_hours"\s*:\s*(\d+)'
)

summary["engine"]["sample_every_seconds"] = grab_float(
    r'"sample_every_seconds"\s*:\s*([0-9.]+)'
)

summary["learner"]["samples"] = grab_int(
    r'"samples"\s*:\s*(\d+)'
)

summary["order_flow"]["binance_connected"] = bool(
    re.search(r'"binance_connected"\s*:\s*true', text, re.I)
)

summary["order_flow"]["gate_connected"] = bool(
    re.search(r'"gate_connected"\s*:\s*true', text, re.I)
)

signal_types = [
    "EARLY_MOVE",
    "PUMP",
    "COOLING",
    "EXIT",
    "RE_ENTRY"
]

for signal in signal_types:
    summary["signals"][signal] = len(
        re.findall(r'"signal_type"\s*:\s*"' + re.escape(signal) + r'"', text)
    )

error_lines = []
for line in text.splitlines():
    low = line.lower()
    if any(x in low for x in [
        "traceback",
        "exception",
        "attributeerror",
        "connectionclosed",
        "timeout",
        "failed",
        "error:"
    ]):
        line = line.strip()
        if line and line not in error_lines:
            error_lines.append(line)

summary["errors"] = error_lines[:50]

mem = re.search(
    r'Mem:\s+(\S+)\s+(\S+)\s+(\S+)',
    text
)

if mem:
    summary["system"]["memory_total"] = mem.group(1)
    summary["system"]["memory_used"] = mem.group(2)
    summary["system"]["memory_free"] = mem.group(3)

json.dump(
    summary,
    sys.stdout,
    indent=2,
    ensure_ascii=False
)
PY

PATH_FILE="$TMP_DIR/latest_report_path.txt"
printf '%s\n' "$REMOTE_FILE" > "$PATH_FILE"

echo
echo "=== UPLOAD PELNEGO RAPORTU ==="
upload_file \
  "$LOCAL_FILE" \
  "$REMOTE_FILE" \
  "Pump Hunter full report: $BASENAME"

echo
echo "=== UPDATE latest_summary.json ==="
upload_file \
  "$SUMMARY_FILE" \
  "$LATEST_SUMMARY" \
  "Pump Hunter: update latest summary"

echo
echo "=== UPDATE latest_report_path.txt ==="
upload_file \
  "$PATH_FILE" \
  "$LATEST_PATH" \
  "Pump Hunter: update latest report path"

unset TOKEN

echo
echo "=============================================="
echo " GOTOWE"
echo "=============================================="
echo "FULL:    $REMOTE_FILE"
echo "SUMMARY: $LATEST_SUMMARY"
echo "PATH:    $LATEST_PATH"
