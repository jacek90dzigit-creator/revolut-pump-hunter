#!/opt/pump-hunter/venv/bin/python

import json
import re
import sys
from pathlib import Path
from datetime import datetime, timezone

REPORT_GLOB = "/home/opc/pump_hunter_*.txt"
OUT_DIR = Path("/home/opc/.pump-hunter/report_analysis")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def first(pattern, text, cast=None):
    m = re.search(pattern, text, re.I | re.M)
    if not m:
        return None
    value = m.group(1)
    if cast:
        try:
            return cast(value)
        except Exception:
            return None
    return value

def count(pattern, text):
    return len(re.findall(pattern, text, re.I))

def analyze(path):
    text = path.read_text(errors="replace")

    result = {
        "analysis_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "report": {
            "name": path.name,
            "size_bytes": path.stat().st_size,
        },
        "engine": {
            "version": first(r'"version"\s*:\s*"([^"]+)"', text),
            "sample_every_seconds": first(
                r'"sample_every_seconds"\s*:\s*([0-9.]+)', text, float
            ),
            "market_context_hours": first(
                r'"market_context_hours"\s*:\s*(\d+)', text, int
            ),
        },
        "learner": {
            "samples": first(r'"samples"\s*:\s*(\d+)', text, int),
        },
        "signals": {},
        "assets": {},
        "flow": {
            "binance_connected": bool(
                re.search(r'"binance_connected"\s*:\s*true', text, re.I)
            ),
            "gate_connected": bool(
                re.search(r'"gate_connected"\s*:\s*true', text, re.I)
            ),
            "no_recent_trades": count(r'NO_RECENT_TRADES', text),
        },
        "events": {
            "timeouts": count(r'timeout|timed out', text),
            "websocket": count(r'websocket|connectionclosed', text),
            "tracebacks": count(r'traceback \(most recent call last\)', text),
            "exceptions": count(r'\bexception\b', text),
            "http_429": count(r'\b429\b', text),
            "http_500": count(r'\b500\b', text),
        },
        "interesting_lines": [],
    }

    signal_names = [
        "EARLY_MOVE",
        "PUMP",
        "COOLING",
        "EXIT",
        "RE_ENTRY",
    ]

    for name in signal_names:
        result["signals"][name] = {
            "signal_type_count": count(
                r'"signal_type"\s*:\s*"' + re.escape(name) + r'"', text
            ),
            "all_mentions": count(r'\b' + re.escape(name) + r'\b', text),
        }

    # Zbieramy aktywa występujące przy signal_type.
    pattern = re.compile(
        r'"asset"\s*:\s*"([^"]+)".{0,1500}?'
        r'"signal_type"\s*:\s*"([^"]+)"',
        re.I | re.S
    )

    for asset, signal in pattern.findall(text):
        asset = asset.upper()
        signal = signal.upper()

        if signal not in signal_names:
            continue

        if asset not in result["assets"]:
            result["assets"][asset] = {
                x: 0 for x in signal_names
            }

        result["assets"][asset][signal] += 1

    # Alternatywna kolejność: signal_type przed asset.
    pattern2 = re.compile(
        r'"signal_type"\s*:\s*"([^"]+)".{0,800}?'
        r'"asset"\s*:\s*"([^"]+)"',
        re.I | re.S
    )

    for signal, asset in pattern2.findall(text):
        asset = asset.upper()
        signal = signal.upper()

        if signal not in signal_names:
            continue

        if asset not in result["assets"]:
            result["assets"][asset] = {
                x: 0 for x in signal_names
            }

        result["assets"][asset][signal] += 1

    # Linie diagnostyczne. Ograniczamy, żeby JSON nie urósł do kolejnego potwora.
    keywords = (
        "traceback",
        "exception",
        "connectionclosed",
        "timeout",
        "429",
        "re_entry",
        "re-entry",
    )

    seen = set()

    for raw in text.splitlines():
        low = raw.lower()

        if not any(k in low for k in keywords):
            continue

        line = raw.strip()

        if not line or line in seen:
            continue

        # Nie uznajemy samego failed_assets: 0 za błąd.
        if re.search(r'"failed_assets"\s*:\s*0', line, re.I):
            continue

        seen.add(line)
        result["interesting_lines"].append(line[:1000])

        if len(result["interesting_lines"]) >= 100:
            break

    # Ranking aktywów według liczby sygnałów.
    ranking = []

    for asset, stats in result["assets"].items():
        total = sum(stats.values())
        ranking.append({
            "asset": asset,
            "total": total,
            **stats,
        })

    ranking.sort(key=lambda x: x["total"], reverse=True)
    result["top_assets"] = ranking[:30]

    return result

reports = sorted(
    Path("/home/opc").glob("pump_hunter_*.txt"),
    key=lambda p: p.stat().st_mtime
)

if not reports:
    print("BRAK RAPORTOW W /home/opc")
    sys.exit(1)

index = []

for path in reports:
    try:
        data = analyze(path)

        out = OUT_DIR / (path.stem + ".json")
        out.write_text(
            json.dumps(data, indent=2, ensure_ascii=False)
        )

        index.append({
            "report": path.name,
            "analysis": out.name,
            "size_bytes": path.stat().st_size,
            "version": data["engine"]["version"],
            "learner_samples": data["learner"]["samples"],
            "signals": {
                k: v["signal_type_count"]
                for k, v in data["signals"].items()
            },
            "events": data["events"],
            "top_assets": data["top_assets"][:10],
        })

        print("OK:", path.name, "->", out.name)

    except Exception as e:
        print("ERROR:", path.name, repr(e))

index_path = OUT_DIR / "all_reports_index.json"
index_path.write_text(
    json.dumps({
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "reports_count": len(index),
        "reports": index,
    }, indent=2, ensure_ascii=False)
)

print()
print("========================================")
print(" ANALIZA RAPORTOW GOTOWA")
print("========================================")
print("Raportow:", len(index))
print("Folder:", OUT_DIR)
print("Index:", index_path)


# ============================================================
# SUMMARY V3.2 EXTENSION
# ============================================================

def extract_int(text, key):
    import re
    patterns = [
        rf'"{re.escape(key)}"\s*:\s*(\d+)',
        rf"'{re.escape(key)}'\s*:\s*(\d+)",
        rf'{re.escape(key)}\s*[:=]\s*(\d+)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            return int(m.group(1))
    return None


def extract_bool(text, key):
    import re
    patterns = [
        rf'"{re.escape(key)}"\s*:\s*(true|false)',
        rf"'{re.escape(key)}'\s*:\s*(True|False)",
        rf'{re.escape(key)}\s*[:=]\s*(true|false)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            return m.group(1).lower() == "true"
    return None


def extract_string(text, key):
    import re
    patterns = [
        rf'"{re.escape(key)}"\s*:\s*"([^"]+)"',
        rf"'{re.escape(key)}'\s*:\s*'([^']+)'",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            return m.group(1)
    return None


def real_runtime_errors(text):
    import re

    bad = []

    checks = {
        "deque_mutated":
            r"RuntimeError:\s*deque mutated during iteration",

        "asgi_exception":
            r"ERROR:\s+Exception in ASGI application",

        "traceback":
            r"Traceback \(most recent call last\)",

        "internal_server_error":
            r'HTTP/[0-9.]+"\s+500 Internal Server Error',

        "task_exception":
            r"Task exception was never retrieved",

        "connection_refused":
            r"ConnectionRefusedError|Connection refused",

        "memory_error":
            r"MemoryError",

        "segfault":
            r"segmentation fault|segfault",
    }

    for name, pattern in checks.items():
        count = len(re.findall(pattern, text, re.I))
        if count:
            bad.append({
                "type": name,
                "count": count,
            })

    return bad


def v32_summary(text):
    return {
        "fusion_evaluated":
            extract_int(text, "fusion_evaluated"),

        "fusion_blocked":
            extract_int(text, "fusion_blocked"),

        "early_blocked":
            extract_int(text, "early_blocked"),

        "pump_blocked":
            extract_int(text, "pump_blocked"),

        "reentry_seen":
            extract_int(text, "reentry_seen"),

        "reentry_accepted":
            extract_int(text, "reentry_accepted"),

        "ws_binance_reconnects":
            extract_int(text, "ws_binance_reconnects"),

        "ws_gate_reconnects":
            extract_int(text, "ws_gate_reconnects"),

        "binance_order_flow_connected":
            extract_bool(text, "binance_connected"),

        "gate_order_flow_connected":
            extract_bool(text, "gate_connected"),

        "btc_regime":
            extract_string(text, "regime"),

        "active_state_count":
            extract_int(text, "active_state_count"),
    }


def clean_false_errors(obj):
    """
    Nie traktujemy failed_assets=0 jako błędu.
    """
    if isinstance(obj, dict):
        for key in list(obj):
            value = obj[key]

            if key == "errors" and isinstance(value, list):
                obj[key] = [
                    x for x in value
                    if not (
                        isinstance(x, str)
                        and "failed_assets" in x
                        and re.search(r'failed_assets[^0-9]*0\b', x)
                    )
                ]

            clean_false_errors(value)

    elif isinstance(obj, list):
        for item in obj:
            clean_false_errors(item)


def enhance_analysis_json(path):
    import json
    import re

    try:
        data = json.loads(path.read_text())
    except Exception:
        return

    report_name = (
        data.get("report", {}).get("name")
        or data.get("source_file")
    )

    if not report_name:
        return

    source = Path("/home/opc") / report_name

    if not source.exists():
        return

    text = source.read_text(errors="replace")

    clean_false_errors(data)

    data["runtime_errors"] = real_runtime_errors(text)
    data["runtime_ok"] = len(data["runtime_errors"]) == 0
    data["v32"] = v32_summary(text)

    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False)
    )


for _json in OUT_DIR.glob("pump_hunter_*.json"):
    enhance_analysis_json(_json)

