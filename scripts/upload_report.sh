#!/usr/bin/env bash
set -euo pipefail

TOKEN_FILE="/home/opc/.pump-hunter/github_token"
REPO="jacek90dzigit-creator/revolut-pump-hunter"

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
REMOTE_FILE="Reports/$BASENAME"
LATEST_FILE="Reports/latest_report.txt"

upload_file() {
  local local_path="$1"
  local remote_path="$2"
  local message="$3"

  local sha=""
  sha="$(curl -sS \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/repos/$REPO/contents/$remote_path" \
    | /opt/pump-hunter/venv/bin/python -c '
import json,sys
try:
    d=json.load(sys.stdin)
    print(d.get("sha",""))
except Exception:
    print("")
')"

  PAYLOAD_FILE="$(mktemp)"

  /opt/pump-hunter/venv/bin/python - "$local_path" "$message" "$sha" > "$PAYLOAD_FILE" <<'PY'
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

  code="$(curl -sS \
    -o /tmp/ph_github_upload.json \
    -w "%{http_code}" \
    -X PUT \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -H "Content-Type: application/json" \
    --data-binary "@$PAYLOAD_FILE" \
    "https://api.github.com/repos/$REPO/contents/$remote_path")"

  rm -f "$PAYLOAD_FILE"

  echo "$remote_path -> HTTP $code"

  if [ "$code" != "200" ] && [ "$code" != "201" ]; then
    cat /tmp/ph_github_upload.json
    echo
    exit 1
  fi
}

echo "=== UPLOAD RAPORTU ==="
upload_file "$LOCAL_FILE" "$REMOTE_FILE" "Pump Hunter report: $BASENAME"

echo
echo "=== UPDATE latest_report.txt ==="
upload_file "$LOCAL_FILE" "$LATEST_FILE" "Pump Hunter: update latest report"

unset TOKEN

echo
echo "GOTOWE"
echo "Raport: $REMOTE_FILE"
echo "Latest: $LATEST_FILE"
