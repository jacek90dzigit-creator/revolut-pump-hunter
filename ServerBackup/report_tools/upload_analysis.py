#!/opt/pump-hunter/venv/bin/python

import base64
import json
import requests
from pathlib import Path

TOKEN_FILE = Path("/home/opc/.pump-hunter/github_token")
LOCAL_DIR = Path("/home/opc/.pump-hunter/report_analysis")

REPO = "jacek90dzigit-creator/revolut-pump-hunter"
API = f"https://api.github.com/repos/{REPO}/contents"

token = TOKEN_FILE.read_text().strip()

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

files = sorted(LOCAL_DIR.glob("*.json"))

if not files:
    raise SystemExit("BRAK PLIKOW JSON DO WYSŁANIA")

print("=== UPLOAD ANALIZ DO GITHUB ===")

ok = 0

for path in files:
    remote = f"Reports/Analysis/{path.name}"
    url = f"{API}/{remote}"

    r = requests.get(url, headers=headers, timeout=30)

    sha = None

    if r.status_code == 200:
        sha = r.json().get("sha")
    elif r.status_code != 404:
        print(f"ERROR GET {remote}: HTTP {r.status_code}")
        print(r.text[:500])
        continue

    content = base64.b64encode(path.read_bytes()).decode("ascii")

    payload = {
        "message": f"Pump Hunter analysis: {path.name}",
        "content": content,
    }

    if sha:
        payload["sha"] = sha

    r = requests.put(
        url,
        headers=headers,
        json=payload,
        timeout=60,
    )

    if r.status_code in (200, 201):
        print(f"OK: {remote} -> HTTP {r.status_code}")
        ok += 1
    else:
        print(f"ERROR: {remote} -> HTTP {r.status_code}")
        print(r.text[:1000])

print()
print("========================================")
print(" UPLOAD ANALIZ ZAKONCZONY")
print("========================================")
print("Wysłano:", ok)
print("Łącznie:", len(files))

if ok != len(files):
    raise SystemExit(1)
