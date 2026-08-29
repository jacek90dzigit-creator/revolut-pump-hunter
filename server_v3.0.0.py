import asyncio
import json
import os
import sqlite3
import threading
import time
from collections import deque
from typing import Dict, List, Optional

import requests
import websockets
from fastapi import FastAPI

REVOLUT_TICKERS_URL = "https://revx.revolut.com/api/1.0/public/tickers?region=EEA"
BINANCE_EXCHANGE_INFO_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"
BINANCE_WS_URL = "wss://data-stream.binance.vision:443/ws"
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
GATE_CURRENCY_PAIRS_URL = "https://api.gateio.ws/api/v4/spot/currency_pairs"
GATE_WS_URL = "wss://api.gateio.ws/ws/v4/"
OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
KUCOIN_SYMBOLS_URL = "https://api.kucoin.com/api/v2/symbols"
KUCOIN_BULLET_URL = "https://api.kucoin.com/api/v1/bullet-public"
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
KRAKEN_ASSET_PAIRS_URL = "https://api.kraken.com/0/public/AssetPairs"
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

WHITELIST_REFRESH_SECONDS = 6 * 60 * 60
HISTORY_SECONDS = 2 * 60 * 60 + 5 * 60
BACKFILL_MINUTES = 120
SAMPLE_EVERY_SECONDS = 2.0

WINDOWS = {f"{minute}m": minute * 60 for minute in range(1, 31)}

QUOTE_PRIORITY = ["USDT", "USDC", "USD", "EUR", "FDUSD", "BTC", "ETH"]

# Only safe same-asset ticker rebrands go here.
# AERGO -> HPP was a 1:1 swap. TON -> GRAM is a ticker rebrand.
ASSET_ALIASES = {
    "AERGO": ["AERGO", "HPP"],
    "TON": ["TON", "GRAM"],
}

def canonical_for_exchange_base(base: str, allowed_assets: set) -> Optional[str]:
    base = base.upper()
    if base in allowed_assets:
        return base
    for canonical, aliases in ASSET_ALIASES.items():
        if canonical in allowed_assets and base in aliases:
            return canonical
    return None


# Human-readable names where we have a verified rename/rebrand.
# Other assets safely fall back to their ticker until a dedicated metadata source is added.
ASSET_DISPLAY_NAMES = {
    "AERGO": "House Party Protocol (formerly Aergo)",
    "TON": "Gram (formerly Toncoin)",
}

SOURCE_DISPLAY_NAMES = {
    "BINANCE": "Binance",
    "BYBIT": "Bybit",
    "GATE": "Gate.io",
    "OKX": "OKX",
    "KUCOIN": "KuCoin",
    "COINBASE": "Coinbase",
    "KRAKEN": "Kraken",
}

def asset_display_name(asset: str) -> str:
    symbol = asset.upper()
    return ASSET_DISPLAY_NAMES.get(symbol, symbol)

def source_display_name(source: Optional[str]) -> Optional[str]:
    if source is None:
        return None
    return SOURCE_DISPLAY_NAMES.get(source.upper(), source)

def change_pct(current: Optional[float], start: Optional[float]) -> Optional[float]:
    if current is None or start is None or start <= 0:
        return None
    return round(((current - start) / start) * 100.0, 4)

app = FastAPI(title="Pump Hunter Server", version="3.0.0")

state: Dict[str, object] = {
    "revolut_ok": False,
    "revolut_last_refresh": 0,
    "revolut_pair_count": 0,
    "revolut_asset_count": 0,
    "revolut_last_error": None,
    "assets": [],
    "pairs": [],
    "binance_connected": False,
    "binance_last_event": 0,
    "binance_last_error": None,
    "binance_mapped_assets": 0,
    "binance_symbols": [],
    "bybit_connected": False,
    "bybit_last_event": 0,
    "bybit_last_error": None,
    "bybit_mapped_assets": 0,
    "bybit_symbols": [],
    "gate_connected": False,
    "gate_last_event": 0,
    "gate_last_error": None,
    "gate_mapped_assets": 0,
    "gate_symbols": [],
    "okx_connected": False,
    "okx_last_event": 0,
    "okx_last_error": None,
    "okx_mapped_assets": 0,
    "okx_symbols": [],
    "kucoin_connected": False,
    "kucoin_last_event": 0,
    "kucoin_last_error": None,
    "kucoin_mapped_assets": 0,
    "kucoin_symbols": [],
    "coinbase_connected": False,
    "coinbase_last_event": 0,
    "coinbase_last_error": None,
    "coinbase_mapped_assets": 0,
    "coinbase_symbols": [],
    "kraken_connected": False,
    "kraken_last_event": 0,
    "kraken_last_error": None,
    "kraken_mapped_assets": 0,
    "kraken_symbols": [],
    "total_fast_feed_assets": 0,
    "unmatched_assets": [],
}

price_history: Dict[str, deque] = {}
last_sample_time: Dict[str, float] = {}
latest_prices: Dict[str, float] = {}
live_last_event: Dict[str, float] = {}
activity_last_total: Dict[str, float] = {}
activity_history: Dict[str, deque] = {}
ACTIVITY_HISTORY_SECONDS = 10 * 60
VOLUME_CONFIRM_RATIO = 1.50
VOLUME_STRONG_RATIO = 2.25
VOLUME_RATIO_CAP = 5.0  # compatibility/display cap; raw ratio is preserved separately
DYNAMIC_LOOKBACK_MINUTES = 30
DYNAMIC_RECENT_MINUTES = 5
DYNAMIC_FOCUS_SCORE = 36
DYNAMIC_HOT_SCORE = 56
DYNAMIC_RAPID_SCORE = 76
OUTCOME_MAX_MINUTES = 30

# Pump Hunter 3.0: multi-timescale market context and live order-flow layer.
MARKET_CONTEXT_MAX_SECONDS = 72 * 60 * 60
MARKET_CONTEXT_SAMPLE_SECONDS = 60
MARKET_CONTEXT_DB_RETENTION_SECONDS = 73 * 60 * 60
MARKET_CONTEXT_TARGETS_SECONDS = (
    list(range(60, 31 * 60, 60))
    + list(range(45 * 60, 6 * 60 * 60 + 1, 15 * 60))
    + list(range(6 * 60 * 60 + 30 * 60, 24 * 60 * 60 + 1, 30 * 60))
    + list(range(25 * 60 * 60, 72 * 60 * 60 + 1, 60 * 60))
)
ORDER_FLOW_HISTORY_SECONDS = 10 * 60
ORDER_FLOW_MIN_NOTIONAL = 0.0
EXTENDED_24H_PCT = 80.0
EXTENDED_72H_PCT = 180.0
MEGA_MOVE_72H_PCT = 50.0
MEGA_MOVE_RESET_DRAWDOWN_PCT = -45.0
VOLUME_LOW_BASELINE_FRACTION = 0.10
MIN_PRICE_MOVE_FOR_VOLUME_BONUS_PCT = 0.35

PUMP_MIN_QUALITY_SCORE = 55
EARLY_MIN_QUALITY_SCORE = 50
EARLY_VOLUME_ASSIST_MIN_QUALITY_SCORE = 45
PUMP_LATE_1M_PCT = 5.0
PUMP_LATE_MIN_CONFIRM_3M_PCT = 1.5
PUMP_LATE_MIN_CONFIRM_5M_PCT = 1.5
ENTRY_LATE_1M_PCT = 4.5
ENTRY_LATE_3M_PCT = 7.5
ENTRY_LATE_5M_PCT = 10.0
ENTRY_AVOID_1M_PCT = 7.0
PUMP_EMERGENCY_1M_PCT = 5.0
PUMP_EMERGENCY_MIN_3M_PCT = 0.0
PUMP_EMERGENCY_MIN_5M_PCT = -0.5
REENTRY_MIN_QUALITY_SCORE = 65
REENTRY_CONFIRM_SECONDS = 20
REENTRY_MAX_EXIT_AGE_SECONDS = 20 * 60
COOLING_REENTRY_MIN_QUALITY_SCORE = 60
COOLING_REENTRY_CONFIRM_SECONDS = 10
COOLING_REENTRY_MIN_M1_PCT = 1.25
COOLING_REENTRY_MIN_M3_PCT = 0.25
COOLING_REENTRY_MAX_DRAWDOWN_PCT = -3.0
REENTRY_COOLDOWN_SECONDS = 120
REENTRY_MAX_PER_CYCLE = 2
EXIT_REENTRY_MIN_M1_PCT = 2.0
EXIT_REENTRY_MIN_M3_PCT = 0.75
EXIT_REENTRY_MAX_DRAWDOWN_PCT = -2.0

# Attention radar v3.0.0. FOCUS is deliberately broad, HOT means a live
# accelerating move, and RAPID_HOT is reserved for a genuine fast second gear.
# The important v3.0.0 change is that old 3m/5m gains cannot keep an asset
# RAPID_HOT while its current 1m impulse is already negative.
FOCUS_MIN_1M_PCT = 0.65
FOCUS_MIN_2M_PCT = 1.10
FOCUS_MIN_3M_PCT = 1.60
HOT_MIN_1M_PCT = 1.25
HOT_MIN_2M_PCT = 2.00
HOT_MIN_3M_PCT = 2.75
HOT_MIN_5M_PCT = 4.00
RAPID_HOT_MIN_1M_PCT = 3.25
RAPID_HOT_MIN_2M_PCT = 4.75
RAPID_HOT_MIN_3M_PCT = 6.25
RAPID_HOT_MIN_5M_PCT = 8.50
RAPID_HOT_MIN_CURRENT_1M_PCT = 0.80
RAPID_HOT_PRICE_ONLY_1M_PCT = 4.50
RAPID_HOT_PRICE_ONLY_2M_PCT = 6.50
RAPID_CONFIRM_SECONDS = 20
RAPID_CONFIRM_MIN_CURRENT_1M_PCT = 0.80
RAPID_CONFIRM_MIN_QUALITY_SCORE = 65
HOT_MIN_CURRENT_1M_PCT = 0.20
FOCUS_RELEASE_SECONDS = 75
ATTENTION_COOLDOWN_SECONDS = 3 * 60
HOT_SNAPSHOT_SECONDS = 60
HOT_TRACK_MAX_MINUTES = 12 * 60

# Persistent black-box recorder. SQLite is intentionally local and dependency-free.
DB_PATH = os.getenv("PUMP_HUNTER_DB_PATH", "/opt/pump-hunter/data/pump_hunter.db")
DB_LOCK = threading.RLock()
DB_READY = False
runtime_session_id: Optional[int] = None
backfill_status: Dict[str, object] = {
    "running": False,
    "completed": False,
    "assets_attempted": 0,
    "assets_seeded": 0,
    "candles_loaded": 0,
    "errors": [],
}
latest_sources: Dict[str, str] = {}
signals: deque = deque(maxlen=300)
last_early_alert: Dict[str, float] = {}
last_pump_alert: Dict[str, float] = {}
signal_state: Dict[str, Dict[str, object]] = {}
attention_state: Dict[str, Dict[str, object]] = {}
hot_snapshots: Dict[str, deque] = {}
completed_hot_cycles: deque = deque(maxlen=500)
pending_signal_outcomes: Dict[str, List[Dict[str, object]]] = {}

# Compact multi-timescale context. Only a small tiered set is kept in RAM.
market_context_history: Dict[str, deque] = {}
market_context_last_sample: Dict[str, float] = {}
order_flow_history: Dict[str, deque] = {}
mega_move_state: Dict[str, Dict[str, object]] = {}


def _db_connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_persistence() -> None:
    global DB_READY, runtime_session_id
    with DB_LOCK:
        con = _db_connect()
        try:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS signal_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    asset TEXT,
                    signal_type TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_signal_ts ON signal_events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_signal_asset ON signal_events(asset, ts DESC);

                CREATE TABLE IF NOT EXISTS attention_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    from_level TEXT,
                    to_level TEXT NOT NULL,
                    price REAL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_attention_ts ON attention_events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_attention_asset ON attention_events(asset, ts DESC);

                CREATE TABLE IF NOT EXISTS hot_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_hot_snapshot_ts ON hot_snapshots(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_hot_snapshot_asset ON hot_snapshots(asset, ts DESC);

                CREATE TABLE IF NOT EXISTS hot_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at INTEGER NOT NULL,
                    ended_at INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_hot_cycle_end ON hot_cycles(ended_at DESC);

                CREATE TABLE IF NOT EXISTS telemetry_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source TEXT,
                    price REAL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry_events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_telemetry_asset ON telemetry_events(asset, ts DESC);

                CREATE TABLE IF NOT EXISTS signal_outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_ts INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    observed_at INTEGER NOT NULL,
                    signal_price REAL NOT NULL,
                    observed_price REAL NOT NULL,
                    change_pct REAL,
                    max_gain_pct REAL,
                    max_drawdown_pct REAL,
                    payload_json TEXT NOT NULL,
                    UNIQUE(signal_ts, asset, signal_type, horizon_min)
                );
                CREATE INDEX IF NOT EXISTS idx_outcome_signal ON signal_outcomes(signal_ts DESC, asset);
                CREATE INDEX IF NOT EXISTS idx_outcome_horizon ON signal_outcomes(horizon_min, signal_type);

                CREATE TABLE IF NOT EXISTS market_context_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    asset TEXT NOT NULL,
                    source TEXT,
                    price REAL NOT NULL,
                    UNIQUE(ts, asset)
                );
                CREATE INDEX IF NOT EXISTS idx_market_context_asset_ts
                    ON market_context_snapshots(asset, ts DESC);

                CREATE TABLE IF NOT EXISTS active_attention (
                    asset TEXT PRIMARY KEY,
                    updated_at INTEGER NOT NULL,
                    state_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at INTEGER NOT NULL,
                    stopped_at INTEGER,
                    version TEXT NOT NULL,
                    note TEXT
                );
                """
            )
            cur = con.execute(
                "INSERT INTO runtime_sessions(started_at, version, note) VALUES (?, ?, ?)",
                (int(time.time()), "3.0.0", "startup"),
            )
            runtime_session_id = int(cur.lastrowid)
            con.commit()
            DB_READY = True
        finally:
            con.close()


def _db_insert(table: str, columns: tuple, values: tuple) -> None:
    if not DB_READY:
        return
    placeholders = ",".join("?" for _ in values)
    cols = ",".join(columns)
    with DB_LOCK:
        con = _db_connect()
        try:
            con.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", values)
            con.commit()
        finally:
            con.close()


def persist_signal(item: Dict[str, object]) -> None:
    _db_insert(
        "signal_events",
        ("ts", "asset", "signal_type", "payload_json"),
        (int(item.get("timestamp") or time.time()), str(item.get("asset") or ""),
         str(item.get("type") or ""), json.dumps(item, separators=(",", ":"))),
    )


def persist_attention_transition(asset: str, previous: str, desired: str, price: float,
                                 now: float, payload: Dict[str, object]) -> None:
    _db_insert(
        "attention_events",
        ("ts", "asset", "from_level", "to_level", "price", "payload_json"),
        (int(now), asset, previous, desired, float(price), json.dumps(payload, separators=(",", ":"))),
    )


def persist_hot_snapshot(asset: str, snapshot: Dict[str, object]) -> None:
    _db_insert(
        "hot_snapshots", ("ts", "asset", "payload_json"),
        (int(snapshot.get("timestamp") or time.time()), asset, json.dumps(snapshot, separators=(",", ":"))),
    )


def persist_hot_cycle(item: Dict[str, object]) -> None:
    _db_insert(
        "hot_cycles", ("started_at", "ended_at", "asset", "payload_json"),
        (int(item.get("started_at") or 0), int(item.get("ended_at") or time.time()),
         str(item.get("asset") or ""), json.dumps(item, separators=(",", ":"))),
    )


def persist_active_attention(asset: str, st: Dict[str, object], now: Optional[float] = None) -> None:
    if not DB_READY:
        return
    ts = int(now or time.time())
    serializable = dict(st)
    with DB_LOCK:
        con = _db_connect()
        try:
            con.execute(
                """INSERT INTO active_attention(asset, updated_at, state_json) VALUES (?, ?, ?)
                   ON CONFLICT(asset) DO UPDATE SET updated_at=excluded.updated_at, state_json=excluded.state_json""",
                (asset, ts, json.dumps(serializable, separators=(",", ":"))),
            )
            con.commit()
        finally:
            con.close()


def clear_active_attention(asset: str) -> None:
    if not DB_READY:
        return
    with DB_LOCK:
        con = _db_connect()
        try:
            con.execute("DELETE FROM active_attention WHERE asset=?", (asset,))
            con.commit()
        finally:
            con.close()


def restore_persistent_state() -> Dict[str, int]:
    restored = {"signals": 0, "hot_cycles": 0, "attention": 0, "snapshots": 0}
    if not DB_READY:
        return restored
    with DB_LOCK:
        con = _db_connect()
        try:
            rows = con.execute("SELECT payload_json FROM signal_events ORDER BY id DESC LIMIT 300").fetchall()
            for row in reversed(rows):
                try:
                    signals.appendleft(json.loads(row["payload_json"]))
                    restored["signals"] += 1
                except Exception:
                    pass
            rows = con.execute("SELECT payload_json FROM hot_cycles ORDER BY id DESC LIMIT 500").fetchall()
            for row in reversed(rows):
                try:
                    completed_hot_cycles.appendleft(json.loads(row["payload_json"]))
                    restored["hot_cycles"] += 1
                except Exception:
                    pass
            rows = con.execute("SELECT asset, state_json FROM active_attention").fetchall()
            for row in rows:
                try:
                    asset = str(row["asset"]).upper()
                    st = json.loads(row["state_json"])
                    level = str(st.get("level", "NORMAL"))
                    # Only restore genuinely active tracking. Old NORMAL rows are discarded.
                    if level != "NORMAL":
                        attention_state[asset] = st
                        restored["attention"] += 1
                        if level in ("HOT", "RAPID_WATCH", "RAPID_CONFIRMED"):
                            snaps = con.execute(
                                "SELECT payload_json FROM hot_snapshots WHERE asset=? AND ts>=? ORDER BY ts ASC LIMIT ?",
                                (asset, int(st.get("hot_started_at", 0) or 0), HOT_TRACK_MAX_MINUTES + 30),
                            ).fetchall()
                            dq = deque(maxlen=HOT_TRACK_MAX_MINUTES + 30)
                            for snap in snaps:
                                try:
                                    dq.append(json.loads(snap["payload_json"]))
                                    restored["snapshots"] += 1
                                except Exception:
                                    pass
                            hot_snapshots[asset] = dq
                except Exception:
                    pass
        finally:
            con.close()
    return restored


def persistence_stats() -> Dict[str, object]:
    if not DB_READY:
        return {"ready": False, "path": DB_PATH}
    with DB_LOCK:
        con = _db_connect()
        try:
            counts = {}
            for table in ("signal_events", "attention_events", "hot_snapshots", "hot_cycles", "telemetry_events", "signal_outcomes", "market_context_snapshots", "runtime_sessions"):
                counts[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            last_session = con.execute(
                "SELECT id, started_at, stopped_at, version, note FROM runtime_sessions ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return {
                "ready": True,
                "path": DB_PATH,
                "counts": counts,
                "session": dict(last_session) if last_session else None,
            }
        finally:
            con.close()

SIGNAL_RESET_SECONDS = 15 * 60
COOLING_CONFIRM_SECONDS = 90

# Signal stabilizer / hysteresis.
STATE_MIN_HOLD_SECONDS = {
    "EARLY_MOVE": 20,
    "PUMP": 25,
    "COOLING": 30,
}
PUMP_TO_COOLING_CONFIRM_SECONDS = 12
COOLING_TO_PUMP_CONFIRM_SECONDS = 15
EARLY_TO_COOLING_CONFIRM_SECONDS = 20
REVERSAL_BLOCK_3M_PCT = -2.0
REVERSAL_BLOCK_5M_PCT = -2.5

binance_symbol_to_asset: Dict[str, str] = {}
binance_streams: List[str] = []
bybit_symbol_to_asset: Dict[str, str] = {}
bybit_topics: List[str] = []
gate_symbol_to_asset: Dict[str, str] = {}
gate_pairs: List[str] = []
okx_symbol_to_asset: Dict[str, str] = {}
okx_args: List[dict] = []
kucoin_symbol_to_asset: Dict[str, str] = {}
kucoin_symbols: List[str] = []
coinbase_symbol_to_asset: Dict[str, str] = {}
coinbase_products: List[str] = []
kraken_symbol_to_asset: Dict[str, str] = {}
kraken_pairs: List[str] = []


def pct_change(current: float, old: float) -> float:
    return 0.0 if old <= 0 else ((current - old) / old) * 100.0


def get_old_price(history: deque, target_age_seconds: int, now: float) -> Optional[float]:
    target_time = now - target_age_seconds
    candidate = None
    for ts, price in history:
        if ts <= target_time:
            candidate = price
        else:
            break
    return candidate


def calculate_moves(asset: str, now: float, current_price: float) -> Dict[str, Optional[float]]:
    history = price_history.get(asset)
    if not history:
        return {name: None for name in WINDOWS}

    result: Dict[str, Optional[float]] = {}
    for name, seconds in WINDOWS.items():
        old_price = get_old_price(history, seconds, now)
        result[name] = None if old_price is None else round(
            pct_change(current_price, old_price), 4
        )
    return result


def pump_score(moves: Dict[str, Optional[float]]) -> int:
    score = 0
    m1 = moves.get("1m")
    m3 = moves.get("3m")
    m5 = moves.get("5m")
    m10 = moves.get("10m")
    m30 = moves.get("30m")

    if m1 is not None:
        score += 30 if m1 >= 3 else 24 if m1 >= 2 else 18 if m1 >= 1.5 else 10 if m1 >= 1 else 0
    if m3 is not None:
        score += 24 if m3 >= 4 else 19 if m3 >= 3 else 14 if m3 >= 2 else 8 if m3 >= 1.5 else 0
    if m5 is not None:
        score += 22 if m5 >= 5 else 18 if m5 >= 4 else 13 if m5 >= 3 else 7 if m5 >= 2 else 0
    if m10 is not None:
        score += 14 if m10 >= 7 else 11 if m10 >= 5 else 6 if m10 >= 3 else 0
    if m30 is not None:
        score += 10 if m30 >= 10 else 8 if m30 >= 7 else 5 if m30 >= 5 else 0

    return min(score, 100)



def record_market_activity(
    asset: str,
    now: float,
    cumulative_volume: Optional[float] = None,
    trade_size: Optional[float] = None,
) -> None:
    """Record a source-agnostic activity increment.

    For cumulative 24h volume fields we store only positive deltas.
    For trade tick feeds (KuCoin) we use the latest trade size directly.
    Ratios are calculated within one asset/source, so quote/base units do not
    need to be identical across exchanges.
    """
    delta = 0.0

    if cumulative_volume is not None and cumulative_volume >= 0:
        previous = activity_last_total.get(asset)
        activity_last_total[asset] = cumulative_volume
        if previous is None:
            return
        if cumulative_volume >= previous:
            delta = cumulative_volume - previous
        else:
            # 24h rolling counters can reset/rebase. Do not create a fake spike.
            return
    elif trade_size is not None and trade_size > 0:
        delta = trade_size
    else:
        return

    if delta <= 0:
        return

    history = activity_history.setdefault(asset, deque(maxlen=6000))
    history.append((now, delta))

    cutoff = now - ACTIVITY_HISTORY_SECONDS
    while history and history[0][0] < cutoff:
        history.popleft()


def activity_metrics(asset: str, now: float) -> Dict[str, object]:
    history = activity_history.get(asset)
    if not history:
        return {
            "ready": False,
            "volume_1m": None,
            "volume_prev_1m": None,
            "volume_ratio": None,
            "volume_ratio_raw": None,
            "volume_confirmed": None,
            "volume_strong": None,
            "low_baseline": None,
        }

    current_start = now - 60
    previous_start = now - 120
    current = 0.0
    previous = 0.0
    oldest = now

    for ts, value in history:
        oldest = min(oldest, ts)
        if ts >= current_start:
            current += value
        elif ts >= previous_start:
            previous += value

    ready = oldest <= now - 90
    ratio_raw = None
    ratio = None
    low_baseline = None

    if ready:
        scale = max((current + previous) / 2.0, 1e-12)
        low_baseline = previous < (scale * VOLUME_LOW_BASELINE_FRACTION)

        if previous > 0:
            ratio_raw = current / previous
        elif current > 0:
            ratio_raw = VOLUME_RATIO_CAP
        else:
            ratio_raw = 0.0

        ratio = min(ratio_raw, VOLUME_RATIO_CAP)

    confirmed = None
    strong = None
    if ratio is not None:
        confirmed = (ratio >= VOLUME_CONFIRM_RATIO) and not bool(low_baseline)
        strong = (ratio >= VOLUME_STRONG_RATIO) and not bool(low_baseline)

    return {
        "ready": ready,
        "volume_1m": round(current, 8) if ready else None,
        "volume_prev_1m": round(previous, 8) if ready else None,
        "volume_ratio": round(ratio, 3) if ratio is not None else None,
        "volume_ratio_raw": round(ratio_raw, 3) if ratio_raw is not None else None,
        "volume_confirmed": confirmed,
        "volume_strong": strong,
        "low_baseline": low_baseline,
    }





def _context_bucket_seconds(age_seconds: float) -> int:
    if age_seconds <= 30 * 60:
        return 60
    if age_seconds <= 6 * 60 * 60:
        return 15 * 60
    if age_seconds <= 24 * 60 * 60:
        return 30 * 60
    return 60 * 60


def _compact_market_context(asset: str, now: float) -> None:
    history = market_context_history.get(asset)
    if not history:
        return
    cutoff = now - MARKET_CONTEXT_MAX_SECONDS
    kept = []
    seen = set()
    # Walk newest to oldest so each bucket keeps the newest representative.
    for ts, price, source in reversed(history):
        if ts < cutoff:
            continue
        age = max(0.0, now - ts)
        bucket = _context_bucket_seconds(age)
        bucket_id = int(ts // bucket)
        key = (bucket, bucket_id)
        if key in seen:
            continue
        seen.add(key)
        kept.append((ts, price, source))
    kept.reverse()
    market_context_history[asset] = deque(kept, maxlen=256)


def record_market_context(asset: str, source: str, price: float, now: float) -> None:
    last = market_context_last_sample.get(asset, 0.0)
    if now - last < MARKET_CONTEXT_SAMPLE_SECONDS:
        return
    market_context_last_sample[asset] = now
    history = market_context_history.setdefault(asset, deque(maxlen=256))
    history.append((now, float(price), source))
    _compact_market_context(asset, now)

    # Persist only minute-level context. The DB acts as restart memory; RAM stays compact.
    if DB_READY:
        minute_ts = int(now // 60) * 60
        with DB_LOCK:
            con = _db_connect()
            try:
                con.execute(
                    """INSERT OR REPLACE INTO market_context_snapshots(ts, asset, source, price)
                       VALUES (?, ?, ?, ?)""",
                    (minute_ts, asset, source, float(price)),
                )
                # Cheap periodic retention cleanup, once per ~10 minutes per process.
                if minute_ts % 600 == 0:
                    con.execute(
                        "DELETE FROM market_context_snapshots WHERE ts < ?",
                        (int(now) - MARKET_CONTEXT_DB_RETENTION_SECONDS,),
                    )
                con.commit()
            finally:
                con.close()


def restore_market_context() -> Dict[str, int]:
    restored = {"assets": 0, "rows": 0}
    if not DB_READY:
        return restored
    cutoff = int(time.time()) - MARKET_CONTEXT_DB_RETENTION_SECONDS
    with DB_LOCK:
        con = _db_connect()
        try:
            rows = con.execute(
                """SELECT ts, asset, source, price
                   FROM market_context_snapshots
                   WHERE ts >= ?
                   ORDER BY asset, ts ASC""",
                (cutoff,),
            ).fetchall()
        finally:
            con.close()
    for row in rows:
        asset = str(row["asset"]).upper()
        dq = market_context_history.setdefault(asset, deque(maxlen=256))
        dq.append((float(row["ts"]), float(row["price"]), row["source"]))
        restored["rows"] += 1
    now = time.time()
    for asset in list(market_context_history):
        _compact_market_context(asset, now)
        if market_context_history.get(asset):
            restored["assets"] += 1
            market_context_last_sample[asset] = float(market_context_history[asset][-1][0])
    return restored


def _context_price_at_age(asset: str, now: float, age_seconds: int) -> Optional[float]:
    history = market_context_history.get(asset)
    if not history:
        return None
    target = now - age_seconds
    candidate = None
    for ts, price, _source in history:
        if ts <= target:
            candidate = price
        else:
            break
    return candidate


def market_context_metrics(asset: str, current_price: float, now: float) -> Dict[str, object]:
    history = market_context_history.get(asset)
    changes: Dict[str, Optional[float]] = {}
    for seconds in MARKET_CONTEXT_TARGETS_SECONDS:
        old = _context_price_at_age(asset, now, seconds)
        label = f"{seconds // 60}m" if seconds < 3600 else (
            f"{seconds / 3600:g}h"
        )
        changes[label] = change_pct(current_price, old)

    path = []
    if history:
        path = [(ts, p) for ts, p, _ in history if ts >= now - MARKET_CONTEXT_MAX_SECONDS and p > 0]
    prices = [p for _, p in path]
    peak_price = max(prices) if prices else current_price
    trough_price = min(prices) if prices else current_price
    peak_ts = next((ts for ts, p in reversed(path) if p == peak_price), now) if path else now
    trough_ts = next((ts for ts, p in reversed(path) if p == trough_price), now) if path else now

    ch_1h = changes.get("1h")
    ch_6h = changes.get("6h")
    ch_24h = changes.get("24h")
    ch_72h = changes.get("72h")
    gain_from_trough = change_pct(current_price, trough_price) or 0.0
    drawdown_from_peak = change_pct(current_price, peak_price) or 0.0

    extended = (
        (isinstance(ch_24h, (int, float)) and ch_24h >= EXTENDED_24H_PCT)
        or (isinstance(ch_72h, (int, float)) and ch_72h >= EXTENDED_72H_PCT)
    )
    mega = (
        (isinstance(ch_72h, (int, float)) and ch_72h >= MEGA_MOVE_72H_PCT)
        or gain_from_trough >= MEGA_MOVE_72H_PCT
    )

    return {
        "ready": bool(history and len(history) >= 2),
        "grid": changes,
        "change_1h_pct": ch_1h,
        "change_6h_pct": ch_6h,
        "change_24h_pct": ch_24h,
        "change_72h_pct": ch_72h,
        "peak_72h_price": peak_price,
        "trough_72h_price": trough_price,
        "drawdown_from_72h_peak_pct": round(drawdown_from_peak, 4),
        "gain_from_72h_trough_pct": round(gain_from_trough, 4),
        "hours_since_peak": round(max(0.0, now - peak_ts) / 3600.0, 3),
        "hours_since_trough": round(max(0.0, now - trough_ts) / 3600.0, 3),
        "extended": bool(extended),
        "mega_move": bool(mega),
        "samples_in_ram": len(history) if history else 0,
    }


def relative_strength_metrics(asset: str, current_price: float, now: float) -> Dict[str, object]:
    btc_price = latest_prices.get("BTC")
    if asset == "BTC" or not isinstance(btc_price, (int, float)) or btc_price <= 0:
        return {"ready": False, "benchmark": "BTC"}
    result: Dict[str, object] = {"ready": False, "benchmark": "BTC"}
    for seconds, label in ((60, "1m"), (300, "5m"), (1800, "30m"), (3600, "1h"), (21600, "6h"), (86400, "24h")):
        asset_old = (
            get_old_price(price_history.get(asset, deque()), seconds, now)
            if seconds <= HISTORY_SECONDS
            else _context_price_at_age(asset, now, seconds)
        )
        btc_old = (
            get_old_price(price_history.get("BTC", deque()), seconds, now)
            if seconds <= HISTORY_SECONDS
            else _context_price_at_age("BTC", now, seconds)
        )
        a = change_pct(current_price, asset_old)
        b = change_pct(float(btc_price), btc_old)
        rel = round(a - b, 4) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None
        result[f"asset_{label}_pct"] = a
        result[f"btc_{label}_pct"] = b
        result[f"relative_{label}_pct"] = rel
        if rel is not None:
            result["ready"] = True
    return result


def record_order_flow(asset: str, now: float, side: Optional[str], quantity: Optional[float], price: Optional[float] = None) -> None:
    if not side or not isinstance(quantity, (int, float)) or quantity <= 0:
        return
    normalized = str(side).lower()
    if normalized not in ("buy", "sell"):
        return
    value = float(quantity)
    if isinstance(price, (int, float)) and price > 0:
        value *= float(price)
    if value < ORDER_FLOW_MIN_NOTIONAL:
        return
    dq = order_flow_history.setdefault(asset, deque(maxlen=12000))
    dq.append((now, normalized, value))
    cutoff = now - ORDER_FLOW_HISTORY_SECONDS
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def order_flow_metrics(asset: str, now: float) -> Dict[str, object]:
    dq = order_flow_history.get(asset)
    if not dq:
        return {
            "ready": False, "buy_ratio_1m": None, "buy_ratio_5m": None,
            "buy_notional_1m": None, "sell_notional_1m": None,
        }

    def calc(seconds: int):
        cutoff = now - seconds
        buy = sell = 0.0
        trades = 0
        for ts, side, value in dq:
            if ts < cutoff:
                continue
            trades += 1
            if side == "buy":
                buy += value
            else:
                sell += value
        total = buy + sell
        ratio = (buy / total) if total > 0 else None
        return buy, sell, ratio, trades

    b1, s1, r1, n1 = calc(60)
    b5, s5, r5, n5 = calc(300)
    return {
        "ready": (n1 + n5) > 0,
        "buy_ratio_1m": round(r1, 4) if r1 is not None else None,
        "buy_ratio_5m": round(r5, 4) if r5 is not None else None,
        "buy_notional_1m": round(b1, 4),
        "sell_notional_1m": round(s1, 4),
        "trades_1m": n1,
        "trades_5m": n5,
    }


def mega_move_metrics(
    asset: str,
    current_price: float,
    now: float,
    dynamic: Dict[str, object],
    context: Dict[str, object],
    flow: Dict[str, object],
) -> Dict[str, object]:
    st = mega_move_state.setdefault(asset, {
        "active": False,
        "started_at": None,
        "start_price": None,
        "peak_price": current_price,
        "wave_count": 0,
        "last_stage": "NORMAL",
        "last_hot_at": 0.0,
    })
    st["peak_price"] = max(float(st.get("peak_price") or current_price), current_price)

    dscore = int(dynamic.get("score", 0) or 0)
    velocity = float(dynamic.get("weighted_velocity_pct", 0.0) or 0.0)
    dd72 = float(context.get("drawdown_from_72h_peak_pct", 0.0) or 0.0)
    ch24 = context.get("change_24h_pct")
    ch72 = context.get("change_72h_pct")
    mega_context = bool(context.get("mega_move"))
    extended = bool(context.get("extended"))

    if (dscore >= DYNAMIC_HOT_SCORE or mega_context) and not st["active"]:
        st["active"] = True
        st["started_at"] = now
        st["start_price"] = current_price
        st["peak_price"] = current_price
        st["wave_count"] = 1 if dscore >= DYNAMIC_HOT_SCORE else 0

    hot_now = dscore >= DYNAMIC_HOT_SCORE and velocity > 0
    if st["active"] and hot_now and now - float(st.get("last_hot_at", 0.0) or 0.0) >= 15 * 60:
        st["wave_count"] = int(st.get("wave_count", 0) or 0) + 1
        st["last_hot_at"] = now

    if extended:
        stage = "EXTENDED"
    elif st["active"] and dd72 <= -8.0 and dscore < DYNAMIC_FOCUS_SCORE:
        stage = "DISTRIBUTION"
    elif st["active"] and int(st.get("wave_count", 0) or 0) >= 2 and dscore >= DYNAMIC_FOCUS_SCORE:
        stage = "RE_PUMP"
    elif st["active"] and mega_context and dscore >= DYNAMIC_FOCUS_SCORE:
        stage = "CONTINUATION"
    elif dscore >= DYNAMIC_HOT_SCORE:
        stage = "PUMP"
    elif dscore >= DYNAMIC_FOCUS_SCORE:
        stage = "BUILDING"
    else:
        stage = "NORMAL"

    if st["active"] and dd72 <= MEGA_MOVE_RESET_DRAWDOWN_PCT and dscore < DYNAMIC_FOCUS_SCORE:
        st["active"] = False

    st["last_stage"] = stage
    start_price = st.get("start_price")
    return {
        "active": bool(st.get("active")),
        "stage": stage,
        "started_at": int(st["started_at"]) if st.get("started_at") else None,
        "age_hours": round((now - float(st["started_at"])) / 3600.0, 3) if st.get("started_at") else None,
        "start_price": start_price,
        "gain_from_start_pct": change_pct(current_price, start_price) if isinstance(start_price, (int, float)) else None,
        "peak_price": st.get("peak_price"),
        "wave_count": int(st.get("wave_count", 0) or 0),
        "context_24h_pct": ch24,
        "context_72h_pct": ch72,
        "drawdown_from_72h_peak_pct": context.get("drawdown_from_72h_peak_pct"),
        "buy_ratio_1m": flow.get("buy_ratio_1m"),
    }


def _minute_prices_from_moves(current_price: float, moves: Dict[str, Optional[float]]) -> Dict[int, float]:
    """Reconstruct approximate minute-boundary prices from cumulative lookback moves."""
    out: Dict[int, float] = {0: current_price}
    for minute in range(1, DYNAMIC_LOOKBACK_MINUTES + 1):
        move = moves.get(f"{minute}m")
        if isinstance(move, (int, float)) and (1.0 + float(move) / 100.0) > 0:
            out[minute] = current_price / (1.0 + float(move) / 100.0)
    return out


def dynamic_momentum_metrics(
    asset: str,
    current_price: float,
    moves: Dict[str, Optional[float]],
    now: float,
) -> Dict[str, object]:
    """Dense 1..30m shape model. Newer minute-to-minute returns get more weight."""
    minute_prices = _minute_prices_from_moves(current_price, moves)
    interval_returns: Dict[str, Optional[float]] = {}
    returns: List[float] = []

    for minute in range(1, DYNAMIC_LOOKBACK_MINUTES + 1):
        newer = minute_prices.get(minute - 1)
        older = minute_prices.get(minute)
        value = None
        if isinstance(newer, (int, float)) and isinstance(older, (int, float)) and older > 0:
            value = ((newer - older) / older) * 100.0
            returns.append(value)
        interval_returns[f"{minute}m"] = round(value, 4) if value is not None else None

    recent = [interval_returns.get(f"{m}m") for m in range(1, DYNAMIC_RECENT_MINUTES + 1)]
    recent_vals = [float(x) for x in recent if isinstance(x, (int, float))]
    ten_vals = [
        float(interval_returns[f"{m}m"])
        for m in range(1, 11)
        if isinstance(interval_returns.get(f"{m}m"), (int, float))
    ]

    weights = [0.36, 0.24, 0.17, 0.13, 0.10]
    weighted_velocity = 0.0
    weight_used = 0.0
    for idx, val in enumerate(recent):
        if isinstance(val, (int, float)):
            weighted_velocity += float(val) * weights[idx]
            weight_used += weights[idx]
    if weight_used > 0:
        weighted_velocity /= weight_used

    r1 = interval_returns.get("1m")
    r2 = interval_returns.get("2m")
    r3 = interval_returns.get("3m")
    accel_1 = (float(r1) - float(r2)) if isinstance(r1, (int, float)) and isinstance(r2, (int, float)) else 0.0
    accel_2 = (float(r2) - float(r3)) if isinstance(r2, (int, float)) and isinstance(r3, (int, float)) else 0.0
    acceleration = (0.65 * accel_1) + (0.35 * accel_2)

    positive_5 = sum(1 for x in recent_vals if x > 0)
    positive_10 = sum(1 for x in ten_vals if x > 0)
    persistence_5 = positive_5 / max(1, len(recent_vals))
    persistence_10 = positive_10 / max(1, len(ten_vals))

    volatility_5 = 0.0
    if len(recent_vals) >= 2:
        mean = sum(recent_vals) / len(recent_vals)
        volatility_5 = (sum((x - mean) ** 2 for x in recent_vals) / len(recent_vals)) ** 0.5

    history = price_history.get(asset)
    peak_10m = current_price
    trough_10m = current_price
    if history:
        cutoff = now - 600
        path_prices = [p for ts, p in history if ts >= cutoff and p > 0]
        if path_prices:
            peak_10m = max(path_prices)
            trough_10m = min(path_prices)
    pullback_from_10m_peak = change_pct(current_price, peak_10m) or 0.0
    rebound_from_10m_trough = change_pct(current_price, trough_10m) or 0.0

    m3 = moves.get("3m")
    m5 = moves.get("5m")
    m10 = moves.get("10m")
    m30 = moves.get("30m")

    score = 0.0
    # Current impulse and recency-weighted minute velocity.
    if isinstance(r1, (int, float)):
        score += max(0.0, min(28.0, float(r1) * 11.0))
    score += max(0.0, min(22.0, weighted_velocity * 10.0))
    score += persistence_5 * 14.0
    score += persistence_10 * 8.0
    score += max(-8.0, min(14.0, acceleration * 8.0))

    # Wider context prevents a single candle from dominating the classification.
    if isinstance(m3, (int, float)) and m3 > 0:
        score += min(5.0, float(m3) * 1.2)
    if isinstance(m5, (int, float)) and m5 > 0:
        score += min(4.0, float(m5) * 0.7)
    if isinstance(m10, (int, float)) and m10 > 0:
        score += min(3.0, float(m10) * 0.35)
    if isinstance(m30, (int, float)) and m30 < -6.0:
        score -= 6.0

    # Penalize a move that is already materially below its local peak.
    if pullback_from_10m_peak < -1.0:
        score += max(-18.0, pullback_from_10m_peak * 3.0)
    if isinstance(r1, (int, float)) and float(r1) < -0.5:
        score -= min(18.0, abs(float(r1)) * 8.0)

    score = int(round(max(0.0, min(100.0, score))))

    phase = (
        "RAPID" if score >= DYNAMIC_RAPID_SCORE
        else "HOT" if score >= DYNAMIC_HOT_SCORE
        else "BUILDING" if score >= DYNAMIC_FOCUS_SCORE
        else "QUIET"
    )

    return {
        "score": score,
        "phase": phase,
        "weighted_velocity_pct": round(weighted_velocity, 4),
        "acceleration_pct": round(acceleration, 4),
        "persistence_5": round(persistence_5, 3),
        "persistence_10": round(persistence_10, 3),
        "volatility_5": round(volatility_5, 4),
        "pullback_from_10m_peak_pct": round(pullback_from_10m_peak, 4),
        "rebound_from_10m_trough_pct": round(rebound_from_10m_trough, 4),
        "minute_returns": interval_returns,
    }


def persist_telemetry_event(
    asset: str,
    event_type: str,
    source: Optional[str],
    price: float,
    now: float,
    payload: Dict[str, object],
) -> None:
    _db_insert(
        "telemetry_events",
        ("ts", "asset", "event_type", "source", "price", "payload_json"),
        (int(now), asset, event_type, source, float(price), json.dumps(payload, separators=(",", ":"))),
    )


def register_signal_outcome(signal_item: Dict[str, object]) -> None:
    asset = str(signal_item.get("asset") or "").upper()
    price = signal_item.get("signal_price", signal_item.get("price"))
    ts = int(signal_item.get("timestamp") or time.time())
    if not asset or not isinstance(price, (int, float)) or price <= 0:
        return
    pending_signal_outcomes.setdefault(asset, []).append({
        "signal_ts": ts,
        "signal_type": str(signal_item.get("type") or "UNKNOWN"),
        "signal_price": float(price),
        "max_price": float(price),
        "min_price": float(price),
        "next_horizon": 1,
        "source": signal_item.get("source"),
    })


def update_signal_outcomes(asset: str, price: float, now: float) -> None:
    trackers = pending_signal_outcomes.get(asset)
    if not trackers:
        return
    keep: List[Dict[str, object]] = []
    for tr in trackers:
        tr["max_price"] = max(float(tr.get("max_price", price)), price)
        tr["min_price"] = min(float(tr.get("min_price", price)), price)
        signal_ts = int(tr["signal_ts"])
        next_horizon = int(tr.get("next_horizon", 1))
        elapsed = now - signal_ts

        while next_horizon <= OUTCOME_MAX_MINUTES and elapsed >= next_horizon * 60:
            signal_price = float(tr["signal_price"])
            payload = {
                "signal_ts": signal_ts,
                "asset": asset,
                "signal_type": tr["signal_type"],
                "horizon_min": next_horizon,
                "observed_at": int(now),
                "signal_price": signal_price,
                "observed_price": price,
                "change_pct": change_pct(price, signal_price),
                "max_gain_pct": change_pct(float(tr["max_price"]), signal_price),
                "max_drawdown_pct": change_pct(float(tr["min_price"]), signal_price),
                "source": tr.get("source"),
            }
            if DB_READY:
                with DB_LOCK:
                    con = _db_connect()
                    try:
                        con.execute(
                            """INSERT OR IGNORE INTO signal_outcomes(
                                signal_ts, asset, signal_type, horizon_min, observed_at,
                                signal_price, observed_price, change_pct, max_gain_pct,
                                max_drawdown_pct, payload_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                signal_ts, asset, str(tr["signal_type"]), next_horizon, int(now),
                                signal_price, float(price), payload["change_pct"],
                                payload["max_gain_pct"], payload["max_drawdown_pct"],
                                json.dumps(payload, separators=(",", ":")),
                            ),
                        )
                        con.commit()
                    finally:
                        con.close()
            next_horizon += 1
            tr["next_horizon"] = next_horizon

        if next_horizon <= OUTCOME_MAX_MINUTES:
            keep.append(tr)
    if keep:
        pending_signal_outcomes[asset] = keep
    else:
        pending_signal_outcomes.pop(asset, None)


def quality_metrics(
    moves: Dict[str, Optional[float]],
    momentum_score: int,
    accelerating: bool,
    fading: bool,
    reversal_bounce: bool,
    volume: Dict[str, object],
) -> Dict[str, object]:
    m1, m3, m5, m10 = [moves.get(k) for k in ("1m", "3m", "5m", "10m")]

    available_short = [x for x in (m1, m3, m5) if x is not None]
    positive_short = sum(1 for x in available_short if x > 0)
    negative_short = sum(1 for x in available_short if x < 0)

    trend_aligned = len(available_short) >= 2 and positive_short >= 2
    broad_trend_positive = (
        (m5 is None or m5 >= 0)
        and (m10 is None or m10 >= -0.5)
    )

    score = int(momentum_score)

    if trend_aligned:
        score += 12
    elif len(available_short) >= 2 and positive_short == 1:
        score -= 8

    if broad_trend_positive:
        score += 5
    else:
        score -= 8

    if accelerating:
        score += 8
    if fading:
        score -= 18
    if reversal_bounce:
        score -= 25

    price_impulse = max(
        abs(x) for x in (m1, m3, m5) if x is not None
    ) if any(x is not None for x in (m1, m3, m5)) else 0.0

    volume_ready = bool(volume.get("ready"))
    volume_ratio = volume.get("volume_ratio")
    low_baseline = bool(volume.get("low_baseline")) if volume.get("low_baseline") is not None else False
    volume_bonus_applied = False

    bullish_impulse = max(
        [x for x in (m1, m3, m5) if x is not None and x > 0],
        default=0.0,
    )

    if volume_ready and isinstance(volume_ratio, (int, float)):
        bullish_context = (
            bullish_impulse >= MIN_PRICE_MOVE_FOR_VOLUME_BONUS_PCT
            and (
                (m1 is not None and m1 > 0)
                or (m3 is not None and m3 > 0)
            )
        )

        if bullish_context and not low_baseline:
            if volume_ratio >= VOLUME_STRONG_RATIO:
                score += 18
                volume_bonus_applied = True
            elif volume_ratio >= VOLUME_CONFIRM_RATIO:
                score += 10
                volume_bonus_applied = True
            elif volume_ratio < 0.75:
                score -= 10
        elif volume_ratio < 0.75 and bullish_impulse >= MIN_PRICE_MOVE_FOR_VOLUME_BONUS_PCT:
            score -= 6

    score = max(0, min(100, score))

    if score >= 80:
        label = "VERY_HIGH"
    elif score >= 65:
        label = "HIGH"
    elif score >= 45:
        label = "MEDIUM"
    else:
        label = "LOW"

    return {
        "quality_score": score,
        "quality": label,
        "trend_aligned": trend_aligned,
        "positive_short_windows": positive_short,
        "negative_short_windows": negative_short,
        "broad_trend_positive": broad_trend_positive,
        "price_impulse_pct": round(price_impulse, 4),
        "bullish_impulse_pct": round(bullish_impulse, 4),
        "volume_ready": volume_ready,
        "volume_ratio": volume_ratio,
        "volume_ratio_raw": volume.get("volume_ratio_raw"),
        "volume_confirmed": volume.get("volume_confirmed"),
        "volume_strong": volume.get("volume_strong"),
        "volume_low_baseline": volume.get("low_baseline"),
        "volume_bonus_applied": volume_bonus_applied,
    }



def entry_timing_metrics(
    moves: Dict[str, Optional[float]],
    quality: Dict[str, object],
    accelerating: bool,
    fading: bool,
    reversal_bounce: bool,
) -> Dict[str, object]:
    """Classify whether a fresh long entry is timely or already chasing the move."""
    m1, m3, m5 = [moves.get(k) for k in ("1m", "3m", "5m")]
    quality_score = int(quality.get("quality_score", 0) or 0)
    volume_confirmed = quality.get("volume_confirmed") is True
    volume_strong = quality.get("volume_strong") is True

    reasons: List[str] = []
    overextended_1m = m1 is not None and m1 >= ENTRY_LATE_1M_PCT
    overextended_3m = m3 is not None and m3 >= ENTRY_LATE_3M_PCT
    overextended_5m = m5 is not None and m5 >= ENTRY_LATE_5M_PCT

    # A fast 1m spike needs confirmation from a wider window. This blocks the
    # classic "buy the candle top" case without killing a broad, sustained pump.
    one_minute_spike_unconfirmed = (
        m1 is not None
        and m1 >= PUMP_LATE_1M_PCT
        and (m3 is None or m3 < PUMP_LATE_MIN_CONFIRM_3M_PCT)
        and (m5 is None or m5 < PUMP_LATE_MIN_CONFIRM_5M_PCT)
    )

    if reversal_bounce:
        reasons.append("REVERSAL_BOUNCE")
    if fading:
        reasons.append("FADING")
    if one_minute_spike_unconfirmed:
        reasons.append("UNCONFIRMED_1M_SPIKE")
    if m1 is not None and m1 >= ENTRY_AVOID_1M_PCT:
        reasons.append("EXTREME_1M_EXTENSION")

    avoid = bool(reversal_bounce or fading or one_minute_spike_unconfirmed or (m1 is not None and m1 >= ENTRY_AVOID_1M_PCT))

    late = (
        not avoid
        and (overextended_1m or overextended_3m or overextended_5m)
    )
    if late:
        if overextended_1m:
            reasons.append("1M_EXTENDED")
        if overextended_3m:
            reasons.append("3M_EXTENDED")
        if overextended_5m:
            reasons.append("5M_EXTENDED")

    # Strong confirmation can make a moderately extended move tradable, but it
    # stays labelled LATE so the client can warn against chasing it blindly.
    if avoid:
        entry_status = "AVOID"
    elif late:
        entry_status = "LATE"
    elif quality_score >= PUMP_MIN_QUALITY_SCORE and bool(quality.get("trend_aligned")):
        entry_status = "ENTRY_OK"
    elif quality_score >= EARLY_MIN_QUALITY_SCORE and accelerating:
        entry_status = "ENTRY_OK"
    else:
        entry_status = "WATCH"

    return {
        "entry_status": entry_status,
        "late_entry_risk": bool(late or avoid),
        "late_entry_block": bool(avoid),
        "entry_reasons": reasons,
        "entry_volume_support": bool(volume_confirmed or volume_strong),
    }


def attention_metrics(moves: Dict[str, Optional[float]], volume: Dict[str, object], dynamic: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    """Classify live upside acceleration, with guards against late/stale spikes."""
    m1, m2, m3, m4, m5 = [moves.get(k) for k in ("1m", "2m", "3m", "4m", "5m")]
    vals = [x for x in (m1, m2, m3, m4, m5) if isinstance(x, (int, float))]
    positive = sum(1 for x in vals if x > 0)
    volume_confirmed = volume.get("volume_confirmed") is True
    volume_strong = volume.get("volume_strong") is True
    volume_support = volume_confirmed or volume_strong
    dynamic = dynamic or {}
    dynamic_score = int(dynamic.get("score", 0) or 0)
    dynamic_phase = str(dynamic.get("phase", "QUIET"))
    dynamic_velocity = float(dynamic.get("weighted_velocity_pct", 0.0) or 0.0)
    dynamic_acceleration = float(dynamic.get("acceleration_pct", 0.0) or 0.0)

    acceleration_shape = (
        isinstance(m1, (int, float)) and isinstance(m2, (int, float)) and isinstance(m3, (int, float))
        and m1 > 0.15 and m2 > m1 and m3 > m2
        and (m2 - m1) >= 0.25 and (m3 - m2) >= 0.25
    )
    current_impulse = isinstance(m1, (int, float)) and m1 >= HOT_MIN_CURRENT_1M_PCT
    rapid_current_impulse = isinstance(m1, (int, float)) and m1 >= RAPID_HOT_MIN_CURRENT_1M_PCT
    stale_spike = isinstance(m1, (int, float)) and m1 <= 0.0 and (
        (isinstance(m3, (int, float)) and m3 >= HOT_MIN_3M_PCT)
        or (isinstance(m5, (int, float)) and m5 >= HOT_MIN_5M_PCT)
    )

    # RAPID_HOT should fire earlier than v2.9.2, but only if the move is alive NOW.
    rapid_threshold = (
        (isinstance(m1, (int, float)) and m1 >= RAPID_HOT_MIN_1M_PCT)
        or (isinstance(m2, (int, float)) and m2 >= RAPID_HOT_MIN_2M_PCT)
        or (isinstance(m3, (int, float)) and m3 >= RAPID_HOT_MIN_3M_PCT)
        or (isinstance(m5, (int, float)) and m5 >= RAPID_HOT_MIN_5M_PCT)
    )
    price_shock = (
        (isinstance(m1, (int, float)) and m1 >= RAPID_HOT_PRICE_ONLY_1M_PCT)
        or (isinstance(m2, (int, float)) and m2 >= RAPID_HOT_PRICE_ONLY_2M_PCT and rapid_current_impulse)
    )
    classic_rapid = (
        not stale_spike and positive >= 3 and (
            price_shock
            or (rapid_threshold and rapid_current_impulse and (volume_support or acceleration_shape))
        )
    )
    dynamic_rapid = (
        not stale_spike
        and dynamic_score >= DYNAMIC_RAPID_SCORE
        and dynamic_velocity > 0.20
        and (rapid_current_impulse or dynamic_acceleration > 0.20)
    )
    rapid = classic_rapid or dynamic_rapid

    hot_threshold = (
        (isinstance(m1, (int, float)) and m1 >= HOT_MIN_1M_PCT)
        or (isinstance(m2, (int, float)) and m2 >= HOT_MIN_2M_PCT)
        or (isinstance(m3, (int, float)) and m3 >= HOT_MIN_3M_PCT)
        or (isinstance(m5, (int, float)) and m5 >= HOT_MIN_5M_PCT)
    )
    classic_hot = (
        not stale_spike and positive >= 2 and current_impulse and (
            hot_threshold or (acceleration_shape and volume_support)
        )
    )
    dynamic_hot = (
        not stale_spike
        and dynamic_score >= DYNAMIC_HOT_SCORE
        and dynamic_velocity > 0.05
        and (current_impulse or dynamic_acceleration > 0.10)
    )
    hot = classic_hot or dynamic_hot

    classic_focus = (
        not stale_spike and (
            (isinstance(m1, (int, float)) and m1 >= FOCUS_MIN_1M_PCT)
            or (isinstance(m2, (int, float)) and m2 >= FOCUS_MIN_2M_PCT and (m1 is None or m1 > -0.20))
            or (isinstance(m3, (int, float)) and m3 >= FOCUS_MIN_3M_PCT and (m1 is None or m1 > -0.20))
            or (acceleration_shape and positive >= 3)
        )
    )
    dynamic_focus = (
        not stale_spike
        and dynamic_score >= DYNAMIC_FOCUS_SCORE
        and dynamic_velocity > -0.05
    )
    focus = classic_focus or dynamic_focus

    level = "RAPID_WATCH" if rapid else "HOT" if hot else "FOCUS" if focus else "NORMAL"
    # Diagnostic score, intentionally not used as a hard trading score.
    attention_score = 0
    if isinstance(m1, (int, float)): attention_score += max(0, min(35, int(m1 * 10)))
    if isinstance(m2, (int, float)): attention_score += max(0, min(25, int(m2 * 5)))
    if isinstance(m3, (int, float)): attention_score += max(0, min(20, int(m3 * 3)))
    if acceleration_shape: attention_score += 10
    if volume_support: attention_score += 10
    if stale_spike: attention_score = max(0, attention_score - 35)

    return {
        "level": level,
        "attention_score": min(100, attention_score),
        "acceleration_shape": acceleration_shape,
        "stale_spike": stale_spike,
        "current_impulse": current_impulse,
        "rapid_current_impulse": rapid_current_impulse,
        "positive_1_to_5m": positive,
        "volume_confirmed": volume_confirmed,
        "volume_strong": volume_strong,
        "dynamic_score": dynamic_score,
        "dynamic_phase": dynamic_phase,
        "dynamic_velocity_pct": round(dynamic_velocity, 4),
        "dynamic_acceleration_pct": round(dynamic_acceleration, 4),
    }


def _finish_hot_cycle(asset: str, st: Dict[str, object], now: float, reason: str) -> None:
    started = float(st.get("hot_started_at", 0.0) or 0.0)
    if started <= 0:
        return
    start_price = st.get("hot_start_price")
    peak_price = st.get("hot_peak_price")
    last_price = latest_prices.get(asset)
    item = {
        "asset": asset,
        "started_at": int(started),
        "ended_at": int(now),
        "duration_minutes": round((now - started) / 60.0, 2),
        "start_price": start_price,
        "peak_price": peak_price,
        "end_price": last_price,
        "max_gain_pct": change_pct(peak_price, start_price),
        "end_change_pct": change_pct(last_price, start_price),
        "peak_after_minutes": round((float(st.get("hot_peak_at", started)) - started) / 60.0, 2),
        "max_level": st.get("max_level", st.get("level")),
        "reason": reason,
        "snapshot_count": len(hot_snapshots.get(asset, [])),
    }
    completed_hot_cycles.appendleft(item)
    persist_hot_cycle(item)
    clear_active_attention(asset)


def update_attention_state(asset: str, source: str, source_symbol: str, price: float,
                           moves: Dict[str, Optional[float]], now: float) -> None:
    volume = activity_metrics(asset, now)
    dynamic = dynamic_momentum_metrics(asset, price, moves, now)
    metrics = attention_metrics(moves, volume, dynamic)
    desired = str(metrics["level"])
    st = attention_state.setdefault(asset, {
        "level": "NORMAL", "since": now, "last_transition": now,
        "focus_started_at": 0.0, "hot_started_at": 0.0,
        "hot_start_price": None, "hot_peak_price": None, "hot_peak_at": 0.0,
        "last_snapshot_at": 0.0, "max_level": "NORMAL", "rapid_watch_started_at": 0.0,
    })
    current = str(st.get("level", "NORMAL"))
    rank = {"NORMAL": 0, "FOCUS": 1, "HOT": 2, "RAPID_WATCH": 3, "RAPID_CONFIRMED": 4}
    transitioned = False

    if rank.get(desired, 0) > rank.get(current, 0):
        previous = current
        st["level"] = desired
        st["since"] = now
        st["last_transition"] = now
        if current == "NORMAL":
            st["focus_started_at"] = now
        if desired in ("HOT", "RAPID_WATCH", "RAPID_CONFIRMED") and current not in ("HOT", "RAPID_WATCH", "RAPID_CONFIRMED"):
            st["hot_started_at"] = now
            st["hot_start_price"] = price
            st["hot_peak_price"] = price
            st["hot_peak_at"] = now
            st["last_snapshot_at"] = 0.0
            hot_snapshots[asset] = deque(maxlen=HOT_TRACK_MAX_MINUTES + 30)
        if desired == "RAPID_WATCH":
            st["rapid_watch_started_at"] = now
        if rank.get(desired, 0) > rank.get(str(st.get("max_level", "NORMAL")), 0):
            st["max_level"] = desired
        transitioned = True
        persist_attention_transition(asset, previous, desired, price, now, {
            "moves": {k: moves.get(k) for k in WINDOWS},
            "volume_ratio": volume.get("volume_ratio"),
            "volume_ratio_raw": volume.get("volume_ratio_raw"),
            "attention_score": metrics.get("attention_score"),
            "dynamic": dynamic,
            "stale_spike": metrics.get("stale_spike"),
        })

    # v3.0.0 two-stage rapid confirmation.
    if str(st.get("level", "NORMAL")) == "RAPID_WATCH":
        rapid_age = now - float(st.get("rapid_watch_started_at", now) or now)
        m1_now = moves.get("1m")
        q_now = quality_metrics(
            moves, pump_score(moves), bool(metrics.get("acceleration_shape")),
            False, False, volume
        )
        rapid_alive = (
            isinstance(m1_now, (int, float))
            and m1_now >= RAPID_CONFIRM_MIN_CURRENT_1M_PCT
            and int(q_now.get("quality_score", 0) or 0) >= RAPID_CONFIRM_MIN_QUALITY_SCORE
            and not bool(metrics.get("stale_spike"))
        )
        if rapid_age >= RAPID_CONFIRM_SECONDS and rapid_alive:
            previous = "RAPID_WATCH"
            st["level"] = "RAPID_CONFIRMED"
            st["since"] = now
            st["last_transition"] = now
            st["max_level"] = "RAPID_CONFIRMED"
            transitioned = True
            persist_attention_transition(asset, previous, "RAPID_CONFIRMED", price, now, {
                "reason": "RAPID_CONFIRMED",
                "confirm_seconds": RAPID_CONFIRM_SECONDS,
                "quality_score": q_now.get("quality_score"),
                "moves": {k: moves.get(k) for k in WINDOWS},
                "volume_ratio": volume.get("volume_ratio"),
                "volume_ratio_raw": volume.get("volume_ratio_raw"),
                "dynamic": dynamic,
            })
        elif rapid_age >= RAPID_CONFIRM_SECONDS and not rapid_alive:
            desired = "HOT" if bool(metrics.get("current_impulse")) else desired
    elif str(st.get("level", "NORMAL")) != "RAPID_CONFIRMED":
        st["rapid_watch_started_at"] = 0.0

    current = str(st.get("level", "NORMAL"))

    if rank.get(desired, 0) < rank.get(current, 0):
        quiet_for = now - float(st.get("last_transition", now) or now)
        if current in ("HOT", "RAPID_WATCH", "RAPID_CONFIRMED"):
            m1, m2, m3 = moves.get("1m"), moves.get("2m"), moves.get("3m")
            losing_impulse = (
                isinstance(m1, (int, float)) and m1 <= 0.15
                and (not isinstance(m2, (int, float)) or m2 <= 0.50)
                and isinstance(m3, (int, float)) and m3 <= 0.90
            )
            peak_price = float(st.get("hot_peak_price", price) or price)
            drawdown = change_pct(price, peak_price) or 0.0
            hard_reversal = drawdown <= -4.0 and isinstance(m1, (int, float)) and m1 < 0.0
            if (quiet_for >= ATTENTION_COOLDOWN_SECONDS and losing_impulse) or hard_reversal:
                previous = current
                _finish_hot_cycle(asset, st, now, "HARD_REVERSAL" if hard_reversal else "COOLED")
                next_level = desired if desired == "FOCUS" else "NORMAL"
                st["level"] = next_level
                st["since"] = now
                st["last_transition"] = now
                st["hot_started_at"] = 0.0
                st["hot_start_price"] = None
                st["hot_peak_price"] = None
                st["hot_peak_at"] = 0.0
                st["last_snapshot_at"] = 0.0
                transitioned = True
                persist_attention_transition(asset, previous, next_level, price, now, {
                    "reason": "HARD_REVERSAL" if hard_reversal else "COOLED",
                    "moves": {k: moves.get(k) for k in WINDOWS},
                    "drawdown_from_peak_pct": drawdown,
                })
        elif current == "FOCUS" and quiet_for >= FOCUS_RELEASE_SECONDS:
            previous = current
            st["level"] = desired
            st["since"] = now
            st["last_transition"] = now
            transitioned = True
            persist_attention_transition(asset, previous, desired, price, now, {
                "reason": "FOCUS_RELEASE", "moves": {k: moves.get(k) for k in WINDOWS}
            })

    current = str(st.get("level", "NORMAL"))
    snapshot_saved = False
    if current in ("HOT", "RAPID_WATCH", "RAPID_CONFIRMED"):
        peak = float(st.get("hot_peak_price", price) or price)
        if price >= peak:
            st["hot_peak_price"] = price
            st["hot_peak_at"] = now
        last_snap = float(st.get("last_snapshot_at", 0.0) or 0.0)
        if now - last_snap >= HOT_SNAPSHOT_SECONDS:
            start_price = st.get("hot_start_price")
            peak_price = st.get("hot_peak_price")
            snapshot = {
                "timestamp": int(now),
                "minute": int((now - float(st.get("hot_started_at", now) or now)) // 60),
                "price": price,
                "change_from_hot_pct": change_pct(price, start_price),
                "peak_gain_pct": change_pct(peak_price, start_price),
                "drawdown_from_peak_pct": change_pct(price, peak_price),
                "attention": current,
                "attention_score": metrics.get("attention_score"),
                "signal_state": signal_state.get(asset, {}).get("state", "NORMAL"),
                "volume_ratio": volume.get("volume_ratio"),
                "volume_ratio_raw": volume.get("volume_ratio_raw"),
                "volume_confirmed": volume.get("volume_confirmed"),
                "acceleration_shape": metrics.get("acceleration_shape"),
                "dynamic": dynamic,
                "moves": {k: moves.get(k) for k in WINDOWS},
            }
            hot_snapshots.setdefault(asset, deque(maxlen=HOT_TRACK_MAX_MINUTES + 30)).append(snapshot)
            persist_hot_snapshot(asset, snapshot)
            st["last_snapshot_at"] = now
            snapshot_saved = True

    st["source"] = source
    st["source_symbol"] = source_symbol
    st["current_price"] = price
    st["moves"] = {k: moves.get(k) for k in WINDOWS}
    st["attention_score"] = metrics.get("attention_score")
    st["acceleration_shape"] = metrics["acceleration_shape"]
    st["stale_spike"] = metrics.get("stale_spike")
    st["positive_1_to_5m"] = metrics["positive_1_to_5m"]
    st["volume_ratio"] = volume.get("volume_ratio")
    st["volume_ratio_raw"] = volume.get("volume_ratio_raw")
    st["volume_confirmed"] = volume.get("volume_confirmed")
    st["dynamic_score"] = dynamic.get("score")
    st["dynamic_phase"] = dynamic.get("phase")
    st["dynamic_velocity_pct"] = dynamic.get("weighted_velocity_pct")
    st["dynamic_acceleration_pct"] = dynamic.get("acceleration_pct")
    if current != "NORMAL" and (transitioned or snapshot_saved):
        persist_active_attention(asset, st, now)
    elif current == "NORMAL" and transitioned:
        clear_active_attention(asset)


def detect_signal(asset: str, source: str, source_symbol: str, price: float,
                  moves: Dict[str, Optional[float]], now: float) -> None:
    m1, m3, m5, m10, m30 = [moves.get(k) for k in ("1m", "3m", "5m", "10m", "30m")]
    dynamic = dynamic_momentum_metrics(asset, price, moves, now)
    dynamic_score = int(dynamic.get("score", 0) or 0)
    dynamic_velocity = float(dynamic.get("weighted_velocity_pct", 0.0) or 0.0)
    dynamic_acceleration = float(dynamic.get("acceleration_pct", 0.0) or 0.0)

    early_raw = (
        (m1 is not None and m1 >= 1.5)
        or (m3 is not None and m3 >= 2.0)
        or (m5 is not None and m5 >= 3.0)
    )
    early_raw = early_raw or (
        dynamic_score >= DYNAMIC_FOCUS_SCORE
        and dynamic_velocity > 0.05
        and dynamic_acceleration > -0.30
    )

    pump_raw = (
        (m1 is not None and m1 >= 3.0)
        or (m3 is not None and m3 >= 4.0)
        or (m5 is not None and m5 >= 4.5)
        or (m10 is not None and m10 >= 5.0)
        or (m30 is not None and m30 >= 5.0)
    )
    pump_raw = pump_raw or (
        dynamic_score >= 70
        and dynamic_velocity > 0.20
        and dynamic_acceleration > -0.10
    )

    positive_short = sum(1 for x in (m1, m3, m5) if x is not None and x > 0)
    fading = (
        (m5 is not None and m5 >= 3.0)
        and (m3 is not None and m3 <= 0.0)
        and (m1 is None or m1 < 1.0)
    )
    accelerating = (
        (
            (m1 is not None and m1 >= 1.0)
            and (m3 is None or m3 > 0.0)
            and positive_short >= 2
        )
        or (
            dynamic_score >= DYNAMIC_FOCUS_SCORE
            and dynamic_velocity > 0.10
            and dynamic_acceleration > 0.05
        )
    )

    # Block "bounce after dump" false positives:
    # e.g. +1.5% in 1m while 3m/5m are still heavily negative.
    reversal_bounce = (
        (m1 is not None and m1 >= 1.0)
        and (
            (m3 is not None and m3 <= REVERSAL_BLOCK_3M_PCT)
            or (m5 is not None and m5 <= REVERSAL_BLOCK_5M_PCT)
        )
    )

    score = pump_score(moves)
    momentum_score = score

    if accelerating:
        momentum_score = min(100, momentum_score + 10)
    if fading:
        momentum_score = max(0, momentum_score - 25)
    if reversal_bounce:
        momentum_score = max(0, momentum_score - 30)

    volume = activity_metrics(asset, now)
    market_context = market_context_metrics(asset, price, now)
    relative_strength = relative_strength_metrics(asset, price, now)
    order_flow = order_flow_metrics(asset, now)
    mega_move = mega_move_metrics(asset, price, now, dynamic, market_context, order_flow)

    quality = quality_metrics(
        moves=moves,
        momentum_score=momentum_score,
        accelerating=accelerating,
        fading=fading,
        reversal_bounce=reversal_bounce,
        volume=volume,
    )
    quality_score = int(quality["quality_score"])

    # EARLY should represent a developing move, not just a one-minute rebound.
    early_quality_ok = (
        not reversal_bounce
        and (
            quality_score >= EARLY_MIN_QUALITY_SCORE
            or (
                quality_score >= EARLY_VOLUME_ASSIST_MIN_QUALITY_SCORE
                and quality.get("volume_confirmed") is True
            )
        )
        and (
            bool(quality["trend_aligned"])
            or (m1 is not None and m1 >= 2.5 and (m3 is None or m3 > -0.75))
        )
    )
    entry_timing = entry_timing_metrics(
        moves=moves,
        quality=quality,
        accelerating=accelerating,
        fading=fading,
        reversal_bounce=reversal_bounce,
    )
    # 3.0: an already extremely extended multi-day move is not treated as fresh EARLY.
    if market_context.get("extended") and entry_timing.get("entry_status") == "ENTRY_OK":
        entry_timing["entry_status"] = "LATE"
        entry_timing["late_entry_risk"] = True
        entry_timing.setdefault("entry_reasons", []).append("EXTENDED_24H_72H_CONTEXT")
    late_entry_block = bool(entry_timing["late_entry_block"])

    emergency_pump_ok = (
        m1 is not None
        and m1 >= PUMP_EMERGENCY_1M_PCT
        and (m3 is not None and m3 >= 1.0)
        and (m5 is None or m5 >= PUMP_EMERGENCY_MIN_5M_PCT)
        and quality_score >= 45
        and not reversal_bounce
        and not fading
        and not late_entry_block
    )

    pump_quality_ok = (
        (quality_score >= PUMP_MIN_QUALITY_SCORE and not late_entry_block)
        or emergency_pump_ok
    )

    st = signal_state.setdefault(asset, {
        "state": "NORMAL",
        "started_at": 0.0,
        "last_transition": 0.0,
        "peak_price": price,
        "peak_score": 0,
        "last_price": price,
        "source": source,
        "source_symbol": source_symbol,
        "pending_state": None,
        "pending_since": 0.0,
        "reentry_count": 0,
        "last_reentry_at": 0.0,
        "last_reentry_from": None,
    })

    current = str(st.get("state", "NORMAL"))

    # Finished cycles can reset after a quiet period.
    if current in ("COOLING", "EXIT") and now - float(st.get("last_transition", 0.0)) >= SIGNAL_RESET_SECONDS:
        st.update({
            "state": "NORMAL",
            "started_at": 0.0,
            "last_transition": now,
            "peak_price": price,
            "peak_score": 0,
            "pending_state": None,
            "pending_since": 0.0,
            "reentry_count": 0,
            "last_reentry_at": 0.0,
            "last_reentry_from": None,
        })
        current = "NORMAL"

    st["last_price"] = price
    st["source"] = source
    st["source_symbol"] = source_symbol
    st["peak_price"] = max(float(st.get("peak_price", price)), price)
    st["peak_score"] = max(int(st.get("peak_score", 0)), momentum_score)

    peak_price = float(st.get("peak_price", price))
    drawdown = ((price - peak_price) / peak_price * 100.0) if peak_price > 0 else 0.0
    state_age = now - float(st.get("last_transition", 0.0))

    desired = current
    required_confirm = 0
    reentry_count = int(st.get("reentry_count", 0) or 0)
    last_reentry_at = float(st.get("last_reentry_at", 0.0) or 0.0)
    reentry_cooldown_ok = last_reentry_at <= 0.0 or now - last_reentry_at >= REENTRY_COOLDOWN_SECONDS
    reentry_budget_ok = reentry_count < REENTRY_MAX_PER_CYCLE

    if current == "NORMAL":
        if pump_raw and pump_quality_ok and not fading and not reversal_bounce:
            desired = "PUMP"
        elif early_raw and early_quality_ok and not fading and not reversal_bounce:
            desired = "EARLY_MOVE"

    elif current == "EARLY_MOVE":
        if pump_raw and pump_quality_ok and not fading and not reversal_bounce:
            desired = "PUMP"
        elif state_age >= STATE_MIN_HOLD_SECONDS["EARLY_MOVE"]:
            weakening = (
                fading
                or ((m1 is not None and m1 < -0.5) and (m3 is not None and m3 <= 0.0))
                or (momentum_score <= 5 and drawdown <= -1.5)
            )
            if weakening:
                desired = "COOLING"
                required_confirm = EARLY_TO_COOLING_CONFIRM_SECONDS

    elif current == "PUMP":
        if state_age >= STATE_MIN_HOLD_SECONDS["PUMP"]:
            weakening = (
                fading
                or drawdown <= -2.5
                or ((m1 is not None and m1 < -1.0) and (m3 is not None and m3 <= 0.0))
            )
            if weakening:
                desired = "COOLING"
                required_confirm = PUMP_TO_COOLING_CONFIRM_SECONDS

    elif current == "COOLING":
        if state_age >= STATE_MIN_HOLD_SECONDS["COOLING"]:
            # A genuine second wave after cooling is a RE_ENTRY event.
            recovered = (
                reentry_cooldown_ok
                and reentry_budget_ok
                and pump_raw
                and pump_quality_ok
                and accelerating
                and not fading
                and not reversal_bounce
                and drawdown > COOLING_REENTRY_MAX_DRAWDOWN_PCT
                and quality_score >= COOLING_REENTRY_MIN_QUALITY_SCORE
                and (m1 is not None and m1 >= COOLING_REENTRY_MIN_M1_PCT)
                and (m3 is None or m3 >= COOLING_REENTRY_MIN_M3_PCT)
            )
            if recovered:
                desired = "PUMP"
                required_confirm = COOLING_REENTRY_CONFIRM_SECONDS
            elif state_age >= COOLING_CONFIRM_SECONDS:
                exit_ready = (
                    drawdown <= -3.0
                    or (m3 is not None and m3 < -1.0)
                    or (m5 is not None and m5 < -1.5)
                )
                if exit_ready:
                    desired = "EXIT"

    elif current == "EXIT":
        exit_age = now - float(st.get("last_transition", 0.0))
        reentry_ready = (
            exit_age <= REENTRY_MAX_EXIT_AGE_SECONDS
            and reentry_cooldown_ok
            and reentry_budget_ok
            and pump_raw
            and pump_quality_ok
            and accelerating
            and not fading
            and not reversal_bounce
            and drawdown > EXIT_REENTRY_MAX_DRAWDOWN_PCT
            and quality_score >= REENTRY_MIN_QUALITY_SCORE
            and (m1 is not None and m1 >= EXIT_REENTRY_MIN_M1_PCT)
            and (m3 is not None and m3 >= EXIT_REENTRY_MIN_M3_PCT)
        )
        if reentry_ready:
            desired = "PUMP"
            required_confirm = REENTRY_CONFIRM_SECONDS
        else:
            desired = "EXIT"

    # Confirmation window for noisy reversals.
    if desired != current and required_confirm > 0:
        if st.get("pending_state") != desired:
            st["pending_state"] = desired
            st["pending_since"] = now
            return

        if now - float(st.get("pending_since", now)) < required_confirm:
            return
    else:
        # Any return to the current regime cancels a pending transition.
        if desired == current:
            st["pending_state"] = None
            st["pending_since"] = 0.0
            return

    # Transition accepted.
    previous = current

    if previous == "NORMAL":
        st["started_at"] = now
        st["peak_price"] = price
        st["peak_score"] = momentum_score

    st["state"] = desired
    st["last_transition"] = now
    st["pending_state"] = None
    st["pending_since"] = 0.0

    is_reentry = previous in ("COOLING", "EXIT") and desired == "PUMP"
    if is_reentry:
        st["reentry_count"] = int(st.get("reentry_count", 0) or 0) + 1
        st["last_reentry_at"] = now
        st["last_reentry_from"] = previous

    signal_item = {
        "type": "RE_ENTRY" if is_reentry else desired,
        "engine_state": desired,
        "previous_state": previous,
        "reentry_from": previous if is_reentry else None,
        "asset": asset,
        "asset_name": asset_display_name(asset),
        "source": source,
        "source_name": source_display_name(source),
        "source_symbol": source_symbol,
        "price": price,
        "signal_price": price,
        "pump_score": score,
        "momentum_score": momentum_score,
        "dynamic_momentum_score": dynamic_score,
        "dynamic_phase": dynamic.get("phase"),
        "dynamic_velocity_pct": dynamic.get("weighted_velocity_pct"),
        "dynamic_acceleration_pct": dynamic.get("acceleration_pct"),
        "dynamic_persistence_5": dynamic.get("persistence_5"),
        "dynamic_persistence_10": dynamic.get("persistence_10"),
        "dynamic_volatility_5": dynamic.get("volatility_5"),
        "dynamic_pullback_10m_pct": dynamic.get("pullback_from_10m_peak_pct"),
        "minute_returns": dynamic.get("minute_returns"),
        "market_context": market_context,
        "relative_strength": relative_strength,
        "order_flow": order_flow,
        "mega_move": mega_move,
        "move_stage": mega_move.get("stage"),
        "accelerating": accelerating,
        "fading": fading,
        "reversal_bounce": reversal_bounce,
        "quality_score": quality["quality_score"],
        "quality": quality["quality"],
        "trend_aligned": quality["trend_aligned"],
        "positive_short_windows": quality["positive_short_windows"],
        "broad_trend_positive": quality["broad_trend_positive"],
        "volume_ready": quality["volume_ready"],
        "volume_ratio": quality["volume_ratio"],
        "volume_ratio_raw": quality["volume_ratio_raw"],
        "volume_confirmed": quality["volume_confirmed"],
        "volume_strong": quality["volume_strong"],
        "volume_low_baseline": quality["volume_low_baseline"],
        "volume_bonus_applied": quality["volume_bonus_applied"],
        "price_impulse_pct": quality["price_impulse_pct"],
        "bullish_impulse_pct": quality["bullish_impulse_pct"],
        "pump_quality_ok": pump_quality_ok,
        "emergency_pump_ok": emergency_pump_ok,
        "entry_status": entry_timing["entry_status"],
        "late_entry_risk": entry_timing["late_entry_risk"],
        "late_entry_block": entry_timing["late_entry_block"],
        "entry_reasons": entry_timing["entry_reasons"],
        "reentry_count": int(st.get("reentry_count", 0) or 0),
        "reentry_limit": REENTRY_MAX_PER_CYCLE,
        "reentry_cooldown_seconds": REENTRY_COOLDOWN_SECONDS,
        "last_reentry_at": int(float(st.get("last_reentry_at", 0.0))) if st.get("last_reentry_at") else None,
        "moves": moves,
        "cycle_started_at": int(float(st["started_at"])) if st["started_at"] else None,
        "peak_price": st["peak_price"],
        "peak_score": st["peak_score"],
        "timestamp": int(now),
    }
    signals.appendleft(signal_item)
    persist_signal(signal_item)
    register_signal_outcome(signal_item)
    persist_telemetry_event(
        asset, "SIGNAL_" + str(signal_item.get("type") or "UNKNOWN"),
        source, price, now,
        {
            "moves": {k: moves.get(k) for k in WINDOWS},
            "dynamic": dynamic,
            "volume": volume,
            "market_context": market_context,
            "relative_strength": relative_strength,
            "order_flow": order_flow,
            "mega_move": mega_move,
            "quality": quality,
            "entry_timing": entry_timing,
        },
    )

def store_price(asset: str, source: str, source_symbol: str, price: float) -> None:
    if price <= 0:
        return

    now = time.time()

    # Real live websocket tick.
    live_last_event[asset] = now

    latest_prices[asset] = price
    latest_sources[asset] = source
    record_market_context(asset, source, price, now)
    update_signal_outcomes(asset, price, now)

    history = price_history.setdefault(asset, deque(maxlen=8000))

    if now - last_sample_time.get(asset, 0.0) >= SAMPLE_EVERY_SECONDS:
        history.append((now, price))
        last_sample_time[asset] = now

        cutoff = now - HISTORY_SECONDS
        while history and history[0][0] < cutoff:
            history.popleft()

    moves = calculate_moves(asset, now, price)
    update_attention_state(asset, source, source_symbol, price, moves, now)

    # Signal Engine only reacts to live ticks, using the backfilled history
    # as context for the dense 1m..10m plus 15m/20m/25m/30m windows.
    detect_signal(
        asset=asset,
        source=source,
        source_symbol=source_symbol,
        price=price,
        moves=moves,
        now=now,
    )


def refresh_revolut_whitelist() -> None:
    response = requests.get(
        REVOLUT_TICKERS_URL,
        timeout=20,
        headers={
            "Accept": "application/json",
            "User-Agent": "PumpHunterServer/3.0.0",
        },
    )

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "unknown")
        raise RuntimeError(f"Revolut HTTP 429, Retry-After={retry_after} ms")

    response.raise_for_status()
    tickers = response.json().get("data", [])

    pairs: List[str] = []
    assets = set()

    for ticker in tickers:
        symbol = str(ticker.get("symbol", "")).strip().upper()
        if "/" not in symbol:
            continue
        base, _quote = symbol.split("/", 1)
        if base:
            pairs.append(symbol)
            assets.add(base)

    state["revolut_ok"] = True
    state["revolut_last_refresh"] = int(time.time())
    state["revolut_pair_count"] = len(pairs)
    state["revolut_asset_count"] = len(assets)
    state["assets"] = sorted(assets)
    state["pairs"] = sorted(set(pairs))
    state["revolut_last_error"] = None


def build_binance_mapping() -> None:
    global binance_symbol_to_asset, binance_streams

    response = requests.get(BINANCE_EXCHANGE_INFO_URL, timeout=30)
    response.raise_for_status()

    whitelist = set(state.get("assets", []))
    candidates: Dict[str, Dict[str, str]] = {}

    for item in response.json().get("symbols", []):
        if item.get("status") != "TRADING":
            continue

        base = str(item.get("baseAsset", "")).upper()
        quote = str(item.get("quoteAsset", "")).upper()
        symbol = str(item.get("symbol", "")).upper()

        if quote not in QUOTE_PRIORITY:
            continue
        canonical = canonical_for_exchange_base(base, whitelist)
        if canonical is None:
            continue
        candidates.setdefault(canonical, {})[quote] = symbol

    mapping: Dict[str, str] = {}
    streams: List[str] = []

    for asset in sorted(whitelist):
        quote_map = candidates.get(asset, {})
        selected = next(
            (quote_map[q] for q in QUOTE_PRIORITY if q in quote_map),
            None,
        )
        if selected:
            mapping[selected] = asset
            streams.append(f"{selected.lower()}@miniTicker")

    binance_symbol_to_asset = mapping
    binance_streams = streams
    state["binance_mapped_assets"] = len(mapping)
    state["binance_symbols"] = sorted(mapping.keys())


def fetch_all_bybit_spot_instruments() -> List[dict]:
    items: List[dict] = []
    cursor = ""

    while True:
        params = {
            "category": "spot",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        response = requests.get(
            BYBIT_INSTRUMENTS_URL,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("retCode", 0) != 0:
            raise RuntimeError(
                "Bybit instruments error: "
                + str(payload.get("retMsg", "unknown"))
            )

        result = payload.get("result", {})
        items.extend(result.get("list", []))

        cursor = str(result.get("nextPageCursor", "") or "")
        if not cursor:
            break

    return items


def build_bybit_mapping() -> None:
    global bybit_symbol_to_asset, bybit_topics

    whitelist = set(state.get("assets", []))
    already_mapped = set(binance_symbol_to_asset.values())
    missing = whitelist - already_mapped

    candidates: Dict[str, Dict[str, str]] = {}

    for item in fetch_all_bybit_spot_instruments():
        status = str(item.get("status", ""))
        if status != "Trading":
            continue

        base = str(item.get("baseCoin", "")).upper()
        quote = str(item.get("quoteCoin", "")).upper()
        symbol = str(item.get("symbol", "")).upper()

        if quote not in QUOTE_PRIORITY:
            continue
        canonical = canonical_for_exchange_base(base, missing)
        if canonical is None:
            continue
        candidates.setdefault(canonical, {})[quote] = symbol

    mapping: Dict[str, str] = {}
    topics: List[str] = []

    for asset in sorted(missing):
        quote_map = candidates.get(asset, {})
        selected = next(
            (quote_map[q] for q in QUOTE_PRIORITY if q in quote_map),
            None,
        )
        if selected:
            mapping[selected] = asset
            topics.append(f"tickers.{selected}")

    bybit_symbol_to_asset = mapping
    bybit_topics = topics

    state["bybit_mapped_assets"] = len(mapping)
    state["bybit_symbols"] = sorted(mapping.keys())

    combined = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
    )

    unmatched = sorted(whitelist - combined)

    state["total_fast_feed_assets"] = len(combined)
    state["unmatched_assets"] = unmatched



def build_gate_mapping() -> None:
    global gate_symbol_to_asset, gate_pairs
    whitelist = set(state.get("assets", []))
    already_mapped = set(binance_symbol_to_asset.values()) | set(bybit_symbol_to_asset.values())
    missing = whitelist - already_mapped
    response = requests.get(GATE_CURRENCY_PAIRS_URL, timeout=30)
    response.raise_for_status()
    candidates: Dict[str, Dict[str, str]] = {}
    for item in response.json():
        if str(item.get("trade_status", "")).lower() != "tradable":
            continue
        base = str(item.get("base", "")).upper()
        quote = str(item.get("quote", "")).upper()
        pair_id = str(item.get("id", "")).upper()
        if quote not in QUOTE_PRIORITY:
            continue
        canonical = canonical_for_exchange_base(base, missing)
        if canonical is None:
            continue
        candidates.setdefault(canonical, {})[quote] = pair_id
    mapping: Dict[str, str] = {}
    pairs: List[str] = []
    for asset in sorted(missing):
        quote_map = candidates.get(asset, {})
        selected = next((quote_map[q] for q in QUOTE_PRIORITY if q in quote_map), None)
        if selected:
            mapping[selected] = asset
            pairs.append(selected)
    gate_symbol_to_asset = mapping
    gate_pairs = pairs
    state["gate_mapped_assets"] = len(mapping)
    state["gate_symbols"] = sorted(mapping.keys())
    combined = set(binance_symbol_to_asset.values()) | set(bybit_symbol_to_asset.values()) | set(mapping.values())
    state["total_fast_feed_assets"] = len(combined)
    state["unmatched_assets"] = sorted(whitelist - combined)



def build_okx_mapping() -> None:
    global okx_symbol_to_asset, okx_args

    whitelist = set(state.get("assets", []))
    already_mapped = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
    )
    missing = whitelist - already_mapped

    response = requests.get(
        OKX_INSTRUMENTS_URL,
        params={"instType": "SPOT"},
        timeout=30,
        headers={"Accept": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()

    if str(payload.get("code", "0")) != "0":
        raise RuntimeError(
            "OKX instruments error: "
            + str(payload.get("msg", "unknown"))
        )

    candidates: Dict[str, Dict[str, str]] = {}

    for item in payload.get("data", []):
        if str(item.get("state", "")).lower() != "live":
            continue

        base = str(item.get("baseCcy", "")).upper()
        quote = str(item.get("quoteCcy", "")).upper()
        inst_id = str(item.get("instId", "")).upper()

        if base not in missing or quote not in QUOTE_PRIORITY:
            continue

        candidates.setdefault(base, {})[quote] = inst_id

    mapping: Dict[str, str] = {}
    args: List[dict] = []

    for asset in sorted(missing):
        quote_map = candidates.get(asset, {})
        selected = next(
            (quote_map[q] for q in QUOTE_PRIORITY if q in quote_map),
            None,
        )
        if selected:
            mapping[selected] = asset
            args.append({"channel": "tickers", "instId": selected})

    okx_symbol_to_asset = mapping
    okx_args = args

    state["okx_mapped_assets"] = len(mapping)
    state["okx_symbols"] = sorted(mapping.keys())

    combined = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
        | set(okx_symbol_to_asset.values())
    )

    state["total_fast_feed_assets"] = len(combined)
    state["unmatched_assets"] = sorted(whitelist - combined)



def build_kucoin_mapping() -> None:
    global kucoin_symbol_to_asset, kucoin_symbols

    whitelist = set(state.get("assets", []))
    already_mapped = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
        | set(okx_symbol_to_asset.values())
    )
    missing = whitelist - already_mapped

    response = requests.get(
        KUCOIN_SYMBOLS_URL,
        timeout=30,
        headers={"Accept": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()

    if str(payload.get("code", "")) != "200000":
        raise RuntimeError(
            "KuCoin symbols error: "
            + str(payload.get("msg", payload.get("code", "unknown")))
        )

    candidates: Dict[str, Dict[str, str]] = {}

    for item in payload.get("data", []):
        if not item.get("enableTrading", False):
            continue

        base = str(item.get("baseCurrency", "")).upper()
        quote = str(item.get("quoteCurrency", "")).upper()
        symbol = str(item.get("symbol", "")).upper()

        if quote not in QUOTE_PRIORITY:
            continue
        canonical = canonical_for_exchange_base(base, missing)
        if canonical is None:
            continue
        candidates.setdefault(canonical, {})[quote] = symbol

    mapping: Dict[str, str] = {}
    symbols: List[str] = []

    for asset in sorted(missing):
        quote_map = candidates.get(asset, {})
        selected = next(
            (quote_map[q] for q in QUOTE_PRIORITY if q in quote_map),
            None,
        )
        if selected:
            mapping[selected] = asset
            symbols.append(selected)

    kucoin_symbol_to_asset = mapping
    kucoin_symbols = symbols

    state["kucoin_mapped_assets"] = len(mapping)
    state["kucoin_symbols"] = sorted(mapping.keys())

    combined = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
        | set(okx_symbol_to_asset.values())
        | set(kucoin_symbol_to_asset.values())
    )

    state["total_fast_feed_assets"] = len(combined)
    state["unmatched_assets"] = sorted(whitelist - combined)


def get_kucoin_ws_connection() -> tuple:
    response = requests.post(
        KUCOIN_BULLET_URL,
        timeout=30,
        headers={"Accept": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()

    if str(payload.get("code", "")) != "200000":
        raise RuntimeError(
            "KuCoin bullet error: "
            + str(payload.get("msg", payload.get("code", "unknown")))
        )

    data = payload.get("data", {})
    token = str(data.get("token", ""))
    servers = data.get("instanceServers", [])

    if not token or not servers:
        raise RuntimeError("KuCoin bullet response missing token/server")

    endpoint = str(servers[0].get("endpoint", "")).rstrip("/")
    if not endpoint:
        raise RuntimeError("KuCoin bullet response missing endpoint")

    connect_id = str(int(time.time() * 1000))
    url = f"{endpoint}?token={token}&connectId={connect_id}"

    ping_ms = int(servers[0].get("pingInterval", 18000) or 18000)
    return url, max(5.0, (ping_ms / 1000.0) * 0.75)



def build_coinbase_mapping() -> None:
    global coinbase_symbol_to_asset, coinbase_products

    whitelist = set(state.get("assets", []))
    already = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
        | set(okx_symbol_to_asset.values())
        | set(kucoin_symbol_to_asset.values())
    )
    missing = whitelist - already

    response = requests.get(
        COINBASE_PRODUCTS_URL,
        timeout=30,
        headers={"Accept": "application/json", "User-Agent": "PumpHunterServer/3.0.0"},
    )
    response.raise_for_status()

    candidates: Dict[str, Dict[str, str]] = {}
    for item in response.json():
        status = str(item.get("status", "")).lower()
        if status and status not in ("online",):
            continue
        base = str(item.get("base_currency", "")).upper()
        quote = str(item.get("quote_currency", "")).upper()
        product_id = str(item.get("id", "")).upper()
        if quote not in QUOTE_PRIORITY:
            continue
        canonical = canonical_for_exchange_base(base, missing)
        if canonical is None:
            continue
        candidates.setdefault(canonical, {})[quote] = product_id

    mapping: Dict[str, str] = {}
    products: List[str] = []
    for asset in sorted(missing):
        quote_map = candidates.get(asset, {})
        selected = next((quote_map[q] for q in QUOTE_PRIORITY if q in quote_map), None)
        if selected:
            mapping[selected] = asset
            products.append(selected)

    coinbase_symbol_to_asset = mapping
    coinbase_products = products
    state["coinbase_mapped_assets"] = len(mapping)
    state["coinbase_symbols"] = sorted(mapping.keys())


def build_kraken_mapping() -> None:
    global kraken_symbol_to_asset, kraken_pairs

    whitelist = set(state.get("assets", []))
    already = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
        | set(okx_symbol_to_asset.values())
        | set(kucoin_symbol_to_asset.values())
        | set(coinbase_symbol_to_asset.values())
    )
    missing = whitelist - already

    response = requests.get(KRAKEN_ASSET_PAIRS_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError("Kraken AssetPairs error: " + str(payload["error"]))

    candidates: Dict[str, Dict[str, str]] = {}
    for item in payload.get("result", {}).values():
        wsname = str(item.get("wsname", "") or "").upper()
        if "/" not in wsname:
            continue
        base, quote = wsname.split("/", 1)
        if quote not in QUOTE_PRIORITY:
            continue
        canonical = canonical_for_exchange_base(base, missing)
        if canonical is None:
            continue
        candidates.setdefault(canonical, {})[quote] = wsname

    mapping: Dict[str, str] = {}
    pairs: List[str] = []
    for asset in sorted(missing):
        quote_map = candidates.get(asset, {})
        selected = next((quote_map[q] for q in QUOTE_PRIORITY if q in quote_map), None)
        if selected:
            mapping[selected] = asset
            pairs.append(selected)

    kraken_symbol_to_asset = mapping
    kraken_pairs = pairs
    state["kraken_mapped_assets"] = len(mapping)
    state["kraken_symbols"] = sorted(mapping.keys())

    combined = (
        set(binance_symbol_to_asset.values())
        | set(bybit_symbol_to_asset.values())
        | set(gate_symbol_to_asset.values())
        | set(okx_symbol_to_asset.values())
        | set(kucoin_symbol_to_asset.values())
        | set(coinbase_symbol_to_asset.values())
        | set(kraken_symbol_to_asset.values())
    )
    state["total_fast_feed_assets"] = len(combined)
    state["unmatched_assets"] = sorted(whitelist - combined)



def _seed_history(asset: str, source: str, source_symbol: str, candles: List[tuple]) -> int:
    if not candles:
        return 0
    history = price_history.setdefault(asset, deque(maxlen=8000))
    existing = {int(ts) for ts, _ in history}
    added = 0
    for ts, close in sorted(candles, key=lambda x: x[0]):
        if close <= 0:
            continue
        if int(ts) not in existing:
            history.append((float(ts), float(close)))
            existing.add(int(ts))
            added += 1
    if history:
        latest_ts, latest_price = history[-1]
        latest_prices[asset] = latest_price
        latest_sources[asset] = source
        last_sample_time[asset] = latest_ts
    return added


def _fetch_backfill(source: str, symbol: str) -> List[tuple]:
    now = int(time.time())
    start = now - BACKFILL_MINUTES * 60

    if source == "BINANCE":
        r = requests.get(
            "https://data-api.binance.vision/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "startTime": start * 1000, "limit": BACKFILL_MINUTES + 5},
            timeout=15,
        )
        r.raise_for_status()
        return [(int(x[0]) / 1000.0, float(x[4])) for x in r.json()]

    if source == "BYBIT":
        r = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={"category": "spot", "symbol": symbol, "interval": "1",
                    "start": start * 1000, "end": now * 1000, "limit": BACKFILL_MINUTES + 5},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("retCode") not in (0, None):
            raise RuntimeError(str(payload.get("retMsg")))
        rows = payload.get("result", {}).get("list", [])
        return [(int(x[0]) / 1000.0, float(x[4])) for x in rows]

    if source == "GATE":
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/candlesticks",
            params={"currency_pair": symbol, "interval": "1m", "from": start, "to": now},
            timeout=15,
        )
        r.raise_for_status()
        return [(int(x[0]), float(x[2])) for x in r.json()]

    if source == "KUCOIN":
        r = requests.get(
            "https://api.kucoin.com/api/v1/market/candles",
            params={"type": "1min", "symbol": symbol, "startAt": start, "endAt": now},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("code") != "200000":
            raise RuntimeError(str(payload))
        return [(int(x[0]), float(x[2])) for x in payload.get("data", [])]

    if source == "COINBASE":
        r = requests.get(
            f"https://api.exchange.coinbase.com/products/{symbol}/candles",
            params={"granularity": 60, "start": start, "end": now},
            headers={"Accept": "application/json", "User-Agent": "PumpHunterServer/3.0.0"},
            timeout=15,
        )
        r.raise_for_status()
        return [(int(x[0]), float(x[4])) for x in r.json()]

    if source == "KRAKEN":
        pair = symbol.replace("/", "")
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": pair, "interval": 1, "since": start},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        result = payload.get("result", {})
        rows = next((v for k, v in result.items() if k != "last" and isinstance(v, list)), [])
        return [(int(x[0]), float(x[4])) for x in rows]

    return []


def _asset_routes() -> Dict[str, tuple]:
    routes: Dict[str, tuple] = {}
    # Same priority as mapping: earlier feeds win.
    for source, mapping in (
        ("BINANCE", binance_symbol_to_asset),
        ("BYBIT", bybit_symbol_to_asset),
        ("GATE", gate_symbol_to_asset),
        ("OKX", okx_symbol_to_asset),
        ("KUCOIN", kucoin_symbol_to_asset),
        ("COINBASE", coinbase_symbol_to_asset),
        ("KRAKEN", kraken_symbol_to_asset),
    ):
        for source_symbol, asset in mapping.items():
            routes.setdefault(asset, (source, source_symbol))
    return routes


async def historical_backfill() -> None:
    backfill_status.update({
        "running": True, "completed": False, "assets_attempted": 0,
        "assets_seeded": 0, "candles_loaded": 0, "errors": [],
    })
    routes = _asset_routes()
    sem = asyncio.Semaphore(10)

    async def one(asset: str, route: tuple) -> None:
        source, symbol = route
        backfill_status["assets_attempted"] += 1
        async with sem:
            try:
                candles = await asyncio.to_thread(_fetch_backfill, source, symbol)
                added = _seed_history(asset, source, symbol, candles)
                if added:
                    backfill_status["assets_seeded"] += 1
                    backfill_status["candles_loaded"] += added
            except Exception as exc:
                errors = backfill_status["errors"]
                if len(errors) < 50:
                    errors.append({"asset": asset, "source": source, "symbol": symbol, "error": str(exc)[:180]})

    await asyncio.gather(*(one(asset, route) for asset, route in routes.items()))
    backfill_status["running"] = False
    backfill_status["completed"] = True


async def rebuild_mappings() -> None:
    try:
        build_binance_mapping()
        state["binance_last_error"] = None
    except Exception as exc:
        state["binance_last_error"] = f"mapping: {exc}"

    try:
        build_bybit_mapping()
        state["bybit_last_error"] = None
    except Exception as exc:
        state["bybit_last_error"] = f"mapping: {exc}"

    try:
        build_gate_mapping()
        state["gate_last_error"] = None
    except Exception as exc:
        state["gate_last_error"] = f"mapping: {exc}"

    try:
        build_okx_mapping()
        state["okx_last_error"] = None
    except Exception as exc:
        state["okx_last_error"] = f"mapping: {exc}"

    try:
        build_kucoin_mapping()
        state["kucoin_last_error"] = None
    except Exception as exc:
        state["kucoin_last_error"] = f"mapping: {exc}"

    try:
        build_coinbase_mapping()
        state["coinbase_last_error"] = None
    except Exception as exc:
        state["coinbase_last_error"] = f"mapping: {exc}"

    try:
        build_kraken_mapping()
        state["kraken_last_error"] = None
    except Exception as exc:
        state["kraken_last_error"] = f"mapping: {exc}"


async def whitelist_refresh_loop() -> None:
    while True:
        try:
            refresh_revolut_whitelist()
        except Exception as exc:
            state["revolut_ok"] = False
            state["revolut_last_error"] = str(exc)

        await rebuild_mappings()
        await asyncio.sleep(WHITELIST_REFRESH_SECONDS)


async def binance_fast_feed_loop() -> None:
    while True:
        try:
            if not binance_streams:
                await asyncio.sleep(10)
                continue

            state["binance_connected"] = False

            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=2_000_000,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "method": "SUBSCRIBE",
                            "params": binance_streams,
                            "id": 1,
                        }
                    )
                )

                state["binance_connected"] = True
                state["binance_last_error"] = None

                async for raw in ws:
                    message = json.loads(raw)

                    if "result" in message and message.get("id") == 1:
                        continue

                    if message.get("e") != "24hrMiniTicker":
                        continue

                    symbol = str(message.get("s", "")).upper()
                    asset = binance_symbol_to_asset.get(symbol)

                    if not asset:
                        continue

                    try:
                        price = float(message.get("c", 0))
                    except (TypeError, ValueError):
                        continue

                    now_event = time.time()
                    state["binance_last_event"] = int(now_event)
                    try:
                        cumulative_activity = float(message.get("q", 0))
                    except (TypeError, ValueError):
                        cumulative_activity = None
                    record_market_activity(asset, now_event, cumulative_volume=cumulative_activity)

                    store_price(
                        asset=asset,
                        source="BINANCE",
                        source_symbol=symbol,
                        price=price,
                    )

        except Exception as exc:
            state["binance_connected"] = False
            state["binance_last_error"] = str(exc)
            await asyncio.sleep(5)


async def bybit_fast_feed_loop() -> None:
    while True:
        try:
            if not bybit_topics:
                await asyncio.sleep(10)
                continue

            state["bybit_connected"] = False

            async with websockets.connect(
                BYBIT_WS_URL,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=2_000_000,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "op": "subscribe",
                            "args": bybit_topics,
                        }
                    )
                )

                state["bybit_connected"] = True
                state["bybit_last_error"] = None

                async for raw in ws:
                    message = json.loads(raw)

                    topic = str(message.get("topic", ""))
                    if not topic.startswith("tickers."):
                        continue

                    data = message.get("data", {})
                    if not isinstance(data, dict):
                        continue

                    symbol = str(data.get("symbol", "")).upper()
                    asset = bybit_symbol_to_asset.get(symbol)

                    if not asset:
                        continue

                    try:
                        price = float(data.get("lastPrice", 0))
                    except (TypeError, ValueError):
                        continue

                    now_event = time.time()
                    state["bybit_last_event"] = int(now_event)
                    try:
                        cumulative_activity = float(data.get("turnover24h", 0))
                    except (TypeError, ValueError):
                        cumulative_activity = None
                    record_market_activity(asset, now_event, cumulative_volume=cumulative_activity)

                    store_price(
                        asset=asset,
                        source="BYBIT",
                        source_symbol=symbol,
                        price=price,
                    )

        except Exception as exc:
            state["bybit_connected"] = False
            state["bybit_last_error"] = str(exc)
            await asyncio.sleep(5)



async def gate_fast_feed_loop() -> None:
    while True:
        try:
            if not gate_pairs:
                await asyncio.sleep(10)
                continue
            state["gate_connected"] = False
            async with websockets.connect(
                GATE_WS_URL, ping_interval=20, ping_timeout=60,
                close_timeout=10, max_size=2_000_000
            ) as ws:
                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "spot.tickers",
                    "event": "subscribe",
                    "payload": gate_pairs,
                }))
                state["gate_connected"] = True
                state["gate_last_error"] = None
                async for raw in ws:
                    message = json.loads(raw)
                    if message.get("channel") != "spot.tickers" or message.get("event") != "update":
                        continue
                    data = message.get("result", {})
                    if not isinstance(data, dict):
                        continue
                    pair = str(data.get("currency_pair", "")).upper()
                    asset = gate_symbol_to_asset.get(pair)
                    if not asset:
                        continue
                    try:
                        price = float(data.get("last", 0))
                    except (TypeError, ValueError):
                        continue
                    now_event = time.time()
                    state["gate_last_event"] = int(now_event)
                    try:
                        cumulative_activity = float(data.get("quote_volume", 0))
                    except (TypeError, ValueError):
                        cumulative_activity = None
                    record_market_activity(asset, now_event, cumulative_volume=cumulative_activity)
                    store_price(asset, "GATE", pair, price)
        except Exception as exc:
            state["gate_connected"] = False
            state["gate_last_error"] = str(exc)
            await asyncio.sleep(5)



async def okx_fast_feed_loop() -> None:
    while True:
        try:
            if not okx_args:
                await asyncio.sleep(10)
                continue

            state["okx_connected"] = False

            async with websockets.connect(
                OKX_WS_URL,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=2_000_000,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "op": "subscribe",
                            "args": okx_args,
                        }
                    )
                )

                state["okx_connected"] = True
                state["okx_last_error"] = None

                async for raw in ws:
                    message = json.loads(raw)

                    if message.get("event") in ("subscribe", "error"):
                        if message.get("event") == "error":
                            state["okx_last_error"] = (
                                f'{message.get("code")}: {message.get("msg")}'
                            )
                        continue

                    arg = message.get("arg", {})
                    if not isinstance(arg, dict):
                        continue
                    if arg.get("channel") != "tickers":
                        continue

                    inst_id = str(arg.get("instId", "")).upper()
                    asset = okx_symbol_to_asset.get(inst_id)
                    if not asset:
                        continue

                    data = message.get("data", [])
                    if not isinstance(data, list) or not data:
                        continue

                    item = data[0]
                    if not isinstance(item, dict):
                        continue

                    try:
                        price = float(item.get("last", 0))
                    except (TypeError, ValueError):
                        continue

                    now_event = time.time()
                    state["okx_last_event"] = int(now_event)
                    try:
                        cumulative_activity = float(item.get("volCcy24h", 0))
                    except (TypeError, ValueError):
                        cumulative_activity = None
                    record_market_activity(asset, now_event, cumulative_volume=cumulative_activity)

                    store_price(
                        asset=asset,
                        source="OKX",
                        source_symbol=inst_id,
                        price=price,
                    )

        except Exception as exc:
            state["okx_connected"] = False
            state["okx_last_error"] = str(exc)
            await asyncio.sleep(5)



async def kucoin_fast_feed_loop() -> None:
    while True:
        try:
            if not kucoin_symbols:
                await asyncio.sleep(10)
                continue

            state["kucoin_connected"] = False

            ws_url, app_ping_seconds = await asyncio.to_thread(
                get_kucoin_ws_connection
            )

            async with websockets.connect(
                ws_url,
                ping_interval=None,
                close_timeout=10,
                max_size=2_000_000,
            ) as ws:
                topic = "/market/ticker:" + ",".join(kucoin_symbols)

                await ws.send(
                    json.dumps(
                        {
                            "id": str(int(time.time() * 1000)),
                            "type": "subscribe",
                            "topic": topic,
                            "response": True,
                        }
                    )
                )

                state["kucoin_connected"] = True
                state["kucoin_last_error"] = None

                while True:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=app_ping_seconds,
                        )
                    except asyncio.TimeoutError:
                        await ws.send(
                            json.dumps(
                                {
                                    "id": str(int(time.time() * 1000)),
                                    "type": "ping",
                                }
                            )
                        )
                        continue

                    message = json.loads(raw)

                    if message.get("type") in ("welcome", "ack", "pong"):
                        continue

                    if message.get("type") != "message":
                        continue

                    subject = str(message.get("subject", "")).upper()
                    topic_name = str(message.get("topic", "")).upper()

                    symbol = ""
                    if subject in kucoin_symbol_to_asset:
                        symbol = subject
                    else:
                        for candidate in kucoin_symbol_to_asset:
                            if candidate in topic_name:
                                symbol = candidate
                                break

                    asset = kucoin_symbol_to_asset.get(symbol)
                    if not asset:
                        continue

                    data = message.get("data", {})
                    if not isinstance(data, dict):
                        continue

                    try:
                        price = float(data.get("price", 0))
                    except (TypeError, ValueError):
                        continue

                    now_event = time.time()
                    state["kucoin_last_event"] = int(now_event)
                    try:
                        trade_size = float(data.get("size", 0))
                    except (TypeError, ValueError):
                        trade_size = None
                    record_market_activity(asset, now_event, trade_size=trade_size)
                    record_order_flow(
                        asset=asset,
                        now=now_event,
                        side=data.get("side"),
                        quantity=trade_size,
                        price=price,
                    )

                    store_price(
                        asset=asset,
                        source="KUCOIN",
                        source_symbol=symbol,
                        price=price,
                    )

        except Exception as exc:
            state["kucoin_connected"] = False
            state["kucoin_last_error"] = str(exc)
            await asyncio.sleep(5)



async def coinbase_fast_feed_loop() -> None:
    while True:
        try:
            if not coinbase_products:
                await asyncio.sleep(10)
                continue
            state["coinbase_connected"] = False
            async with websockets.connect(
                COINBASE_WS_URL, ping_interval=20, ping_timeout=60,
                close_timeout=10, max_size=2_000_000
            ) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": coinbase_products,
                    "channel": "ticker",
                }))
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "channel": "heartbeats",
                }))
                state["coinbase_connected"] = True
                state["coinbase_last_error"] = None

                async for raw in ws:
                    message = json.loads(raw)
                    if message.get("channel") != "ticker":
                        continue
                    for event in message.get("events", []):
                        for ticker in event.get("tickers", []):
                            product_id = str(ticker.get("product_id", "")).upper()
                            asset = coinbase_symbol_to_asset.get(product_id)
                            if not asset:
                                continue
                            try:
                                price = float(ticker.get("price", 0))
                            except (TypeError, ValueError):
                                continue
                            now_event = time.time()
                            state["coinbase_last_event"] = int(now_event)
                            try:
                                cumulative_activity = float(ticker.get("volume_24_h", 0))
                            except (TypeError, ValueError):
                                cumulative_activity = None
                            record_market_activity(asset, now_event, cumulative_volume=cumulative_activity)
                            store_price(asset, "COINBASE", product_id, price)
        except Exception as exc:
            state["coinbase_connected"] = False
            state["coinbase_last_error"] = str(exc)
            await asyncio.sleep(5)


async def kraken_fast_feed_loop() -> None:
    while True:
        try:
            if not kraken_pairs:
                await asyncio.sleep(10)
                continue
            state["kraken_connected"] = False
            async with websockets.connect(
                KRAKEN_WS_URL, ping_interval=20, ping_timeout=60,
                close_timeout=10, max_size=2_000_000
            ) as ws:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {
                        "channel": "ticker",
                        "symbol": kraken_pairs,
                        "snapshot": True,
                    }
                }))
                state["kraken_connected"] = True
                state["kraken_last_error"] = None

                async for raw in ws:
                    message = json.loads(raw)
                    if message.get("channel") != "ticker":
                        continue
                    data = message.get("data", [])
                    if not isinstance(data, list):
                        continue
                    for ticker in data:
                        symbol = str(ticker.get("symbol", "")).upper()
                        asset = kraken_symbol_to_asset.get(symbol)
                        if not asset:
                            continue
                        try:
                            price = float(ticker.get("last", 0))
                        except (TypeError, ValueError):
                            continue
                        now_event = time.time()
                        state["kraken_last_event"] = int(now_event)
                        raw_volume = ticker.get("volume")
                        try:
                            if isinstance(raw_volume, list):
                                cumulative_activity = float(raw_volume[-1]) if raw_volume else None
                            else:
                                cumulative_activity = float(raw_volume) if raw_volume is not None else None
                        except (TypeError, ValueError):
                            cumulative_activity = None
                        record_market_activity(asset, now_event, cumulative_volume=cumulative_activity)
                        store_price(asset, "KRAKEN", symbol, price)
        except Exception as exc:
            state["kraken_connected"] = False
            state["kraken_last_error"] = str(exc)
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event() -> None:
    try:
        init_persistence()
        restored = restore_persistent_state()
        restored["market_context"] = restore_market_context()
        state["persistence_restored"] = restored
        state["persistence_error"] = None
    except Exception as exc:
        state["persistence_error"] = str(exc)

    try:
        refresh_revolut_whitelist()
    except Exception as exc:
        state["revolut_ok"] = False
        state["revolut_last_error"] = str(exc)

    await rebuild_mappings()

    asyncio.create_task(whitelist_refresh_loop())
    asyncio.create_task(historical_backfill())
    asyncio.create_task(binance_fast_feed_loop())
    asyncio.create_task(bybit_fast_feed_loop())
    asyncio.create_task(gate_fast_feed_loop())
    asyncio.create_task(okx_fast_feed_loop())
    asyncio.create_task(kucoin_fast_feed_loop())
    asyncio.create_task(coinbase_fast_feed_loop())
    asyncio.create_task(kraken_fast_feed_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if not DB_READY or runtime_session_id is None:
        return
    try:
        with DB_LOCK:
            con = _db_connect()
            try:
                con.execute(
                    "UPDATE runtime_sessions SET stopped_at=?, note=? WHERE id=?",
                    (int(time.time()), "graceful_shutdown", int(runtime_session_id)),
                )
                con.commit()
            finally:
                con.close()
    except Exception:
        pass


@app.get("/")
def root() -> Dict[str, object]:
    return {
        "name": "Pump Hunter Server",
        "version": "3.0.0",
        "status": "running",
    }


@app.get("/status")
def status() -> Dict[str, object]:
    return {
        "server": "running",
        "revolut_ok": state["revolut_ok"],
        "asset_count": state["revolut_asset_count"],
        "pair_count": state["revolut_pair_count"],
        "revolut_last_error": state["revolut_last_error"],
        "binance_connected": state["binance_connected"],
        "binance_mapped_assets": state["binance_mapped_assets"],
        "binance_last_error": state["binance_last_error"],
        "bybit_connected": state["bybit_connected"],
        "bybit_mapped_assets": state["bybit_mapped_assets"],
        "bybit_last_error": state["bybit_last_error"],
        "gate_connected": state["gate_connected"],
        "gate_mapped_assets": state["gate_mapped_assets"],
        "gate_last_error": state["gate_last_error"],
        "okx_connected": state["okx_connected"],
        "okx_mapped_assets": state["okx_mapped_assets"],
        "okx_last_error": state["okx_last_error"],
        "kucoin_connected": state["kucoin_connected"],
        "kucoin_mapped_assets": state["kucoin_mapped_assets"],
        "kucoin_last_error": state["kucoin_last_error"],
        "coinbase_connected": state["coinbase_connected"],
        "coinbase_mapped_assets": state["coinbase_mapped_assets"],
        "coinbase_last_error": state["coinbase_last_error"],
        "kraken_connected": state["kraken_connected"],
        "kraken_mapped_assets": state["kraken_mapped_assets"],
        "kraken_last_error": state["kraken_last_error"],
        "total_fast_feed_assets": state["total_fast_feed_assets"],
        "unmatched_assets": len(state["unmatched_assets"]),
        "signal_count": len(signals),
        "persistence": persistence_stats(),
        "persistence_error": state.get("persistence_error"),
        "persistence_restored": state.get("persistence_restored"),
    }


@app.get("/coverage")
def coverage() -> Dict[str, object]:
    return {
        "revolut_assets": state["revolut_asset_count"],
        "binance_assets": state["binance_mapped_assets"],
        "bybit_assets": state["bybit_mapped_assets"],
        "gate_assets": state["gate_mapped_assets"],
        "okx_assets": state["okx_mapped_assets"],
        "kucoin_assets": state["kucoin_mapped_assets"],
        "coinbase_assets": state["coinbase_mapped_assets"],
        "kraken_assets": state["kraken_mapped_assets"],
        "total_fast_feed_assets": state["total_fast_feed_assets"],
        "unmatched_count": len(state["unmatched_assets"]),
        "unmatched_assets": state["unmatched_assets"],
    }


@app.get("/feed-health")
def feed_health(stale_after: int = 120) -> Dict[str, object]:
    now = time.time()
    whitelist_assets = [str(a).upper() for a in state.get("assets", [])]

    priced_assets = []
    missing_price_assets = []
    stale_assets = []
    source_counts: Dict[str, int] = {}

    for asset in whitelist_assets:
        price = latest_prices.get(asset)
        source = latest_sources.get(asset)

        if price is None:
            missing_price_assets.append(asset)
            continue

        priced_assets.append(asset)

        if source:
            source_counts[source] = source_counts.get(source, 0) + 1

        history = price_history.get(asset)
        last_tick = history[-1][0] if history else None
        if last_tick is not None and now - last_tick > max(1, stale_after):
            stale_assets.append(
                {
                    "asset": asset,
                    "asset_name": asset_display_name(asset),
                    "source": source,
                    "source_name": source_display_name(source),
                    "last_tick_age_seconds": round(now - last_tick, 1),
                }
            )

    live_assets = sorted(a for a in whitelist_assets if a in live_last_event)
    live_stale = []
    for asset in live_assets:
        age = now - live_last_event[asset]
        if age > max(1, stale_after):
            live_stale.append({
                "asset": asset,
                "asset_name": asset_display_name(asset),
                "source": latest_sources.get(asset),
                "source_name": source_display_name(latest_sources.get(asset)),
                "last_live_tick_age_seconds": round(age, 1),
            })

    return {
        "mapped_assets": state.get("total_fast_feed_assets", 0),
        "whitelist_assets": len(whitelist_assets),
        "priced_assets": len(priced_assets),
        "missing_price_count": len(missing_price_assets),
        "missing_price_assets": missing_price_assets,
        "live_assets": len(live_assets),
        "never_live_count": len(whitelist_assets) - len(live_assets),
        "never_live_assets": sorted(set(whitelist_assets) - set(live_assets)),
        "stale_after_seconds": max(1, stale_after),
        "stale_count": len(live_stale),
        "stale_assets": live_stale,
        "source_price_counts": source_counts,
        "backfill": dict(backfill_status),
        "signal_count": len(signals),
    }


@app.get("/whitelist")
def whitelist() -> Dict[str, object]:
    return {
        "asset_count": state["revolut_asset_count"],
        "assets": state["assets"],
    }


@app.get("/signals")
def get_signals(limit: int = 50) -> Dict[str, object]:
    safe_limit = max(1, min(limit, 200))
    enriched = []

    for raw in list(signals)[:safe_limit]:
        item = dict(raw)
        asset = str(item.get("asset", "")).upper()
        signal_price = item.get("signal_price", item.get("price"))
        current_price = latest_prices.get(asset)

        item["asset_name"] = asset_display_name(asset)
        item["source_name"] = source_display_name(item.get("source"))
        item["signal_price"] = signal_price
        item["current_price"] = current_price
        item["change_since_signal_pct"] = change_pct(current_price, signal_price)
        item["current_source"] = latest_sources.get(asset)
        item["current_source_name"] = source_display_name(latest_sources.get(asset))
        enriched.append(item)

    return {
        "count": len(signals),
        "signals": enriched,
    }


@app.get("/quality")
def quality_overview(limit: int = 30) -> Dict[str, object]:
    now = time.time()
    rows = []

    for asset in [str(a).upper() for a in state.get("assets", [])]:
        price = latest_prices.get(asset)
        if price is None:
            continue
        moves = calculate_moves(asset, now, price)

        m1, m3, m5 = [moves.get(k) for k in ("1m", "3m", "5m")]
        positive_short = sum(1 for x in (m1, m3, m5) if x is not None and x > 0)
        fading = (
            (m5 is not None and m5 >= 3.0)
            and (m3 is not None and m3 <= 0.0)
            and (m1 is None or m1 < 1.0)
        )
        accelerating = (
            (m1 is not None and m1 >= 1.0)
            and (m3 is None or m3 > 0.0)
            and positive_short >= 2
        )
        reversal = (
            (m1 is not None and m1 >= 1.0)
            and (
                (m3 is not None and m3 <= REVERSAL_BLOCK_3M_PCT)
                or (m5 is not None and m5 <= REVERSAL_BLOCK_5M_PCT)
            )
        )
        momentum = pump_score(moves)
        if accelerating:
            momentum = min(100, momentum + 10)
        if fading:
            momentum = max(0, momentum - 25)
        if reversal:
            momentum = max(0, momentum - 30)

        qm = quality_metrics(
            moves, momentum, accelerating, fading, reversal,
            activity_metrics(asset, now)
        )
        timing = entry_timing_metrics(moves, qm, accelerating, fading, reversal)
        rows.append({
            "asset": asset,
            "asset_name": asset_display_name(asset),
            "source": latest_sources.get(asset),
            "source_name": source_display_name(latest_sources.get(asset)),
            "price": price,
            "state": signal_state.get(asset, {}).get("state", "NORMAL"),
            "quality_score": qm["quality_score"],
            "quality": qm["quality"],
            "entry_status": timing["entry_status"],
            "late_entry_risk": timing["late_entry_risk"],
            "entry_reasons": timing["entry_reasons"],
            "trend_aligned": qm["trend_aligned"],
            "price_impulse_pct": qm["price_impulse_pct"],
            "bullish_impulse_pct": qm["bullish_impulse_pct"],
            "volume_ratio": qm["volume_ratio"],
            "volume_ratio_raw": qm["volume_ratio_raw"],
            "volume_confirmed": qm["volume_confirmed"],
            "volume_low_baseline": qm["volume_low_baseline"],
            "volume_bonus_applied": qm["volume_bonus_applied"],
            "moves": moves,
        })

    rows.sort(key=lambda x: x["quality_score"], reverse=True)
    safe_limit = max(1, min(limit, 100))
    return {"count": len(rows), "top": rows[:safe_limit]}


@app.get("/re-entries")
def re_entries(limit: int = 50) -> Dict[str, object]:
    safe_limit = max(1, min(limit, 200))
    rows = [sig for sig in signals if sig.get("type") == "RE_ENTRY"]
    return {
        "count": len(rows),
        "re_entries": rows[:safe_limit],
    }


@app.get("/signal-engine")
def signal_engine() -> Dict[str, object]:
    counts = {"NORMAL": 0, "EARLY_MOVE": 0, "PUMP": 0, "COOLING": 0, "EXIT": 0}
    active = []
    whitelist_assets = [str(a).upper() for a in state.get("assets", [])]

    for asset in whitelist_assets:
        st = signal_state.get(asset)
        current = str(st.get("state", "NORMAL")) if st else "NORMAL"
        counts[current] = counts.get(current, 0) + 1
        if st and current != "NORMAL":
            active.append({
                "asset": asset,
                "asset_name": asset_display_name(asset),
                "state": current,
                "source": st.get("source"),
                "source_name": source_display_name(st.get("source")),
                "source_symbol": st.get("source_symbol"),
                "started_at": int(float(st.get("started_at", 0))) if st.get("started_at") else None,
                "last_transition": int(float(st.get("last_transition", 0))),
                "peak_price": st.get("peak_price"),
                "peak_score": st.get("peak_score"),
                "current_price": latest_prices.get(asset),
                "pending_state": st.get("pending_state"),
                "pending_since": int(float(st.get("pending_since", 0))) if st.get("pending_since") else None,
            })

    active.sort(key=lambda x: x.get("last_transition") or 0, reverse=True)
    reentry_count = sum(1 for sig in signals if sig.get("type") == "RE_ENTRY")

    attention_counts = {"NORMAL": 0, "FOCUS": 0, "HOT": 0, "RAPID_WATCH": 0, "RAPID_CONFIRMED": 0}
    for asset in whitelist_assets:
        level = str(attention_state.get(asset, {}).get("level", "NORMAL"))
        attention_counts[level] = attention_counts.get(level, 0) + 1

    return {
        "states": counts,
        "active_count": len(active),
        "reentry_count": reentry_count,
        "attention": attention_counts,
        "hot_active_count": attention_counts.get("HOT", 0) + attention_counts.get("RAPID_WATCH", 0) + attention_counts.get("RAPID_CONFIRMED", 0),
        "active": active,
    }



@app.get("/telemetry")
def telemetry_overview(limit: int = 100) -> Dict[str, object]:
    safe_limit = max(1, min(limit, 500))
    if not DB_READY:
        return {"version": "3.0.0", "error": "database_not_ready", "persistence": persistence_stats()}
    with DB_LOCK:
        con = _db_connect()
        try:
            telemetry_count = int(con.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0])
            outcome_count = int(con.execute("SELECT COUNT(*) FROM signal_outcomes").fetchone()[0])
            horizon_rows = con.execute(
                """SELECT horizon_min, COUNT(*) AS n,
                          AVG(change_pct) AS avg_change,
                          AVG(max_gain_pct) AS avg_max_gain,
                          AVG(max_drawdown_pct) AS avg_max_drawdown
                   FROM signal_outcomes
                   GROUP BY horizon_min ORDER BY horizon_min"""
            ).fetchall()
            recent = con.execute(
                """SELECT signal_ts, asset, signal_type, horizon_min, observed_at,
                          change_pct, max_gain_pct, max_drawdown_pct
                   FROM signal_outcomes ORDER BY id DESC LIMIT ?""",
                (safe_limit,),
            ).fetchall()
            return {
                "version": "3.0.0",
                "dynamic_engine": {
                    "windows": [f"{m}m" for m in range(1, 31)],
                    "focus_score": DYNAMIC_FOCUS_SCORE,
                    "hot_score": DYNAMIC_HOT_SCORE,
                    "rapid_score": DYNAMIC_RAPID_SCORE,
                    "outcome_horizons_minutes": list(range(1, OUTCOME_MAX_MINUTES + 1)),
                    "market_context_max_hours": 72,
                    "market_context_grid": "tiered",
                    "relative_strength_benchmark": "BTC",
                },
                "counts": {
                    "telemetry_events": telemetry_count,
                    "signal_outcomes": outcome_count,
                    "pending_outcome_trackers": sum(len(v) for v in pending_signal_outcomes.values()),
                },
                "horizons": [dict(r) for r in horizon_rows],
                "recent_outcomes": [dict(r) for r in recent],
            }
        finally:
            con.close()


@app.get("/market-context/{asset}")
def market_context_endpoint(asset: str) -> Dict[str, object]:
    symbol = asset.upper()
    price = latest_prices.get(symbol)
    now = time.time()
    if not isinstance(price, (int, float)) or price <= 0:
        return {"version": "3.0.0", "asset": symbol, "error": "price_not_available"}
    moves = calculate_moves(symbol, now, price)
    dynamic = dynamic_momentum_metrics(symbol, price, moves, now)
    context = market_context_metrics(symbol, price, now)
    flow = order_flow_metrics(symbol, now)
    relative = relative_strength_metrics(symbol, price, now)
    mega = mega_move_metrics(symbol, price, now, dynamic, context, flow)
    return {
        "version": "3.0.0",
        "asset": symbol,
        "price": price,
        "source": latest_sources.get(symbol),
        "dynamic": dynamic,
        "market_context": context,
        "relative_strength": relative,
        "order_flow": flow,
        "mega_move": mega,
    }


@app.get("/v3-engine")
def v3_engine_overview() -> Dict[str, object]:
    now = time.time()
    stage_counts: Dict[str, int] = {}
    active = []
    for asset, price in list(latest_prices.items()):
        if not isinstance(price, (int, float)) or price <= 0:
            continue
        moves = calculate_moves(asset, now, price)
        dynamic = dynamic_momentum_metrics(asset, price, moves, now)
        context = market_context_metrics(asset, price, now)
        flow = order_flow_metrics(asset, now)
        mega = mega_move_metrics(asset, price, now, dynamic, context, flow)
        stage = str(mega.get("stage") or "NORMAL")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        if stage != "NORMAL":
            active.append({
                "asset": asset,
                "price": price,
                "stage": stage,
                "dynamic_score": dynamic.get("score"),
                "change_24h_pct": context.get("change_24h_pct"),
                "change_72h_pct": context.get("change_72h_pct"),
                "drawdown_from_72h_peak_pct": context.get("drawdown_from_72h_peak_pct"),
                "relative_1h_pct": relative_strength_metrics(asset, price, now).get("relative_1h_pct"),
                "buy_ratio_1m": flow.get("buy_ratio_1m"),
                "wave_count": mega.get("wave_count"),
            })
    active.sort(key=lambda x: (x.get("dynamic_score") or 0), reverse=True)
    return {
        "version": "3.0.0",
        "architecture": {
            "live_websocket": True,
            "fast_price_history_sample_seconds": SAMPLE_EVERY_SECONDS,
            "momentum_windows": "1m..30m",
            "market_context": "1m..30m/1m; 45m..6h/15m; 6.5h..24h/30m; 25h..72h/1h",
            "benchmark": "BTC",
            "order_flow": "live where exchange feed exposes aggressor side",
            "stages": ["NORMAL", "BUILDING", "PUMP", "CONTINUATION", "RE_PUMP", "EXTENDED", "DISTRIBUTION"],
        },
        "stage_counts": stage_counts,
        "active_count": len(active),
        "active": active[:100],
    }


@app.get("/persistence")
def persistence_overview() -> Dict[str, object]:
    return persistence_stats()


@app.get("/history-report")
def history_report(limit: int = 100) -> Dict[str, object]:
    safe_limit = max(1, min(limit, 500))
    if not DB_READY:
        return {"version": "3.0.0", "persistence": persistence_stats(), "error": "database_not_ready"}
    with DB_LOCK:
        con = _db_connect()
        try:
            def load_payloads(sql: str, params: tuple = ()) -> List[Dict[str, object]]:
                rows = con.execute(sql, params).fetchall()
                out = []
                for row in rows:
                    try:
                        out.append(json.loads(row["payload_json"]))
                    except Exception:
                        pass
                return out
            signals_db = load_payloads(
                "SELECT payload_json FROM signal_events ORDER BY id DESC LIMIT ?", (safe_limit,)
            )
            attention_rows = con.execute(
                "SELECT ts, asset, from_level, to_level, price, payload_json FROM attention_events ORDER BY id DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
            attention = []
            for row in attention_rows:
                try:
                    meta = json.loads(row["payload_json"])
                except Exception:
                    meta = {}
                attention.append({
                    "timestamp": row["ts"], "asset": row["asset"],
                    "from": row["from_level"], "to": row["to_level"], "price": row["price"],
                    **meta,
                })
            cycles = load_payloads(
                "SELECT payload_json FROM hot_cycles ORDER BY id DESC LIMIT ?", (safe_limit,)
            )
            sessions = [dict(r) for r in con.execute(
                "SELECT id, started_at, stopped_at, version, note FROM runtime_sessions ORDER BY id DESC LIMIT 20"
            ).fetchall()]
            return {
                "version": "3.0.0",
                "persistence": persistence_stats(),
                "signals": signals_db,
                "attention_transitions": attention,
                "completed_hot_cycles": cycles,
                "signal_outcomes": [dict(r) for r in con.execute(
                    """SELECT signal_ts, asset, signal_type, horizon_min, observed_at,
                              signal_price, observed_price, change_pct, max_gain_pct, max_drawdown_pct
                       FROM signal_outcomes ORDER BY id DESC LIMIT ?""",
                    (safe_limit,),
                ).fetchall()],
                "runtime_sessions": sessions,
            }
        finally:
            con.close()


@app.get("/hot")
def hot_overview(limit: int = 50) -> Dict[str, object]:
    rows = []
    for asset, st in attention_state.items():
        level = str(st.get("level", "NORMAL"))
        if level == "NORMAL":
            continue
        start_price = st.get("hot_start_price")
        peak_price = st.get("hot_peak_price")
        current_price = latest_prices.get(asset)
        rows.append({
            "asset": asset,
            "asset_name": asset_display_name(asset),
            "attention": level,
            "source": st.get("source"),
            "source_name": source_display_name(st.get("source")),
            "current_price": current_price,
            "moves": st.get("moves", {}),
            "volume_ratio": st.get("volume_ratio"),
            "volume_confirmed": st.get("volume_confirmed"),
            "acceleration_shape": st.get("acceleration_shape"),
            "attention_score": st.get("attention_score"),
            "stale_spike": st.get("stale_spike"),
            "hot_start_price": start_price,
            "hot_gain_pct": change_pct(current_price, start_price),
            "hot_peak_price": peak_price,
            "hot_peak_gain_pct": change_pct(peak_price, start_price),
            "drawdown_from_hot_peak_pct": change_pct(current_price, peak_price),
            "snapshot_count": len(hot_snapshots.get(asset, [])),
            "signal_state": signal_state.get(asset, {}).get("state", "NORMAL"),
            "since": int(float(st.get("since", 0))) if st.get("since") else None,
        })
    rank = {"RAPID_CONFIRMED": 4, "RAPID_WATCH": 3, "HOT": 2, "FOCUS": 1}
    rows.sort(key=lambda x: (rank.get(str(x.get("attention")), 0), x.get("hot_peak_gain_pct") or -999), reverse=True)
    safe_limit = max(1, min(limit, 200))
    return {"count": len(rows), "hot": rows[:safe_limit]}


@app.get("/hot/{asset}")
def hot_detail(asset: str) -> Dict[str, object]:
    symbol = asset.upper()
    st = attention_state.get(symbol)
    if not st:
        return {"asset": symbol, "attention": "NORMAL", "snapshots": []}
    return {
        "asset": symbol,
        "asset_name": asset_display_name(symbol),
        "attention": st.get("level", "NORMAL"),
        "state": dict(st),
        "snapshots": list(hot_snapshots.get(symbol, [])),
    }


@app.get("/hot-report")
def hot_report(limit: int = 100) -> Dict[str, object]:
    safe_limit = max(1, min(limit, 500))
    active = hot_overview(limit=safe_limit)
    return {
        "version": "3.0.0",
        "active": active,
        "completed_count": len(completed_hot_cycles),
        "completed": list(completed_hot_cycles)[:safe_limit],
        "rules": {
            "focus": {"1m": FOCUS_MIN_1M_PCT, "2m": FOCUS_MIN_2M_PCT, "3m": FOCUS_MIN_3M_PCT},
            "hot": {"1m": HOT_MIN_1M_PCT, "2m": HOT_MIN_2M_PCT, "3m": HOT_MIN_3M_PCT, "5m": HOT_MIN_5M_PCT},
            "rapid_hot": {"1m": RAPID_HOT_MIN_1M_PCT, "2m": RAPID_HOT_MIN_2M_PCT, "3m": RAPID_HOT_MIN_3M_PCT, "5m": RAPID_HOT_MIN_5M_PCT, "min_current_1m": RAPID_HOT_MIN_CURRENT_1M_PCT, "confirm_seconds": RAPID_CONFIRM_SECONDS, "confirm_min_quality": RAPID_CONFIRM_MIN_QUALITY_SCORE},
            "snapshot_seconds": HOT_SNAPSHOT_SECONDS,
        },
    }


@app.get("/report")
def compact_report(limit: int = 200) -> Dict[str, object]:
    safe_limit = max(1, min(limit, 300))
    items = list(signals)[:safe_limit]
    type_counts: Dict[str, int] = {}
    by_type: Dict[str, List[dict]] = {
        "EARLY_MOVE": [], "PUMP": [], "RE_ENTRY": [], "COOLING": [], "EXIT": []
    }

    for raw in items:
        kind = str(raw.get("type", "UNKNOWN"))
        type_counts[kind] = type_counts.get(kind, 0) + 1
        if kind not in by_type:
            continue
        asset = str(raw.get("asset", "")).upper()
        signal_price = raw.get("signal_price", raw.get("price"))
        current_price = latest_prices.get(asset)
        by_type[kind].append({
            "asset": asset,
            "quality_score": raw.get("quality_score"),
            "entry_status": raw.get("entry_status"),
            "signal_price": signal_price,
            "current_price": current_price,
            "change_since_signal_pct": change_pct(current_price, signal_price),
            "reentry_from": raw.get("reentry_from"),
            "reentry_count": raw.get("reentry_count", 0),
            "timestamp": raw.get("timestamp"),
        })

    def performance(rows: List[dict]) -> Dict[str, object]:
        vals = [r["change_since_signal_pct"] for r in rows if isinstance(r.get("change_since_signal_pct"), (int, float))]
        if not vals:
            return {"samples": 0, "positive": 0, "negative": 0, "avg_pct": None, "median_pct": None}
        ordered = sorted(vals)
        n = len(ordered)
        median = ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
        return {
            "samples": n,
            "positive": sum(1 for v in vals if v > 0),
            "negative": sum(1 for v in vals if v < 0),
            "avg_pct": round(sum(vals) / n, 4),
            "median_pct": round(median, 4),
        }

    return {
        "version": "3.0.0",
        "signals_considered": len(items),
        "type_counts": type_counts,
        "performance": {k: performance(v) for k, v in by_type.items()},
        "re_entries": by_type["RE_ENTRY"][:20],
        "pumps": by_type["PUMP"][:30],
        "early_moves": by_type["EARLY_MOVE"][:30],
        "attention": {
            "active": hot_overview(limit=50),
            "completed_hot_cycles": len(completed_hot_cycles),
            "recent_completed": list(completed_hot_cycles)[:20],
        },
        "rules": {
            "pump_min_quality": PUMP_MIN_QUALITY_SCORE,
            "early_min_quality": EARLY_MIN_QUALITY_SCORE,
            "cooling_reentry_min_quality": COOLING_REENTRY_MIN_QUALITY_SCORE,
            "exit_reentry_min_quality": REENTRY_MIN_QUALITY_SCORE,
            "reentry_cooldown_seconds": REENTRY_COOLDOWN_SECONDS,
            "reentry_max_per_cycle": REENTRY_MAX_PER_CYCLE,
            "hot_snapshot_seconds": HOT_SNAPSHOT_SECONDS,
            "dynamic_engine": {
                "focus_score": DYNAMIC_FOCUS_SCORE,
                "hot_score": DYNAMIC_HOT_SCORE,
                "rapid_score": DYNAMIC_RAPID_SCORE,
                "windows": "1m..30m",
                "outcomes": "1m..30m",
                "market_context": "to_72h_tiered",
                "relative_strength": "BTC",
                "move_stage": "v3_mega_move",
            },
            "focus_thresholds": {"1m": FOCUS_MIN_1M_PCT, "2m": FOCUS_MIN_2M_PCT, "3m": FOCUS_MIN_3M_PCT},
            "hot_thresholds": {"1m": HOT_MIN_1M_PCT, "2m": HOT_MIN_2M_PCT, "3m": HOT_MIN_3M_PCT, "5m": HOT_MIN_5M_PCT},
            "rapid_hot_thresholds": {"1m": RAPID_HOT_MIN_1M_PCT, "2m": RAPID_HOT_MIN_2M_PCT, "3m": RAPID_HOT_MIN_3M_PCT, "5m": RAPID_HOT_MIN_5M_PCT, "min_current_1m": RAPID_HOT_MIN_CURRENT_1M_PCT, "confirm_seconds": RAPID_CONFIRM_SECONDS, "confirm_min_quality": RAPID_CONFIRM_MIN_QUALITY_SCORE},
        },
    }


@app.get("/signal/{index}")
def signal_detail(index: int) -> Dict[str, object]:
    items = list(signals)
    if index < 0 or index >= len(items):
        return {"error": "signal_not_found", "index": index}

    item = dict(items[index])
    asset = str(item.get("asset", "")).upper()
    signal_price = item.get("signal_price", item.get("price"))
    current_price = latest_prices.get(asset)

    item["asset_name"] = asset_display_name(asset)
    item["source_name"] = source_display_name(item.get("source"))
    item["signal_price"] = signal_price
    item["current_price"] = current_price
    item["change_since_signal_pct"] = change_pct(current_price, signal_price)
    item["current_source"] = latest_sources.get(asset)
    item["current_source_name"] = source_display_name(latest_sources.get(asset))
    return item


@app.get("/asset/{asset}")
def asset_status(asset: str) -> Dict[str, object]:
    symbol = asset.upper()
    price = latest_prices.get(symbol)
    source = latest_sources.get(symbol)
    now = time.time()

    moves = (
        calculate_moves(symbol, now, price)
        if price
        else {name: None for name in WINDOWS}
    )

    volume = activity_metrics(symbol, now)
    current_quality = quality_metrics(
        moves=moves,
        momentum_score=pump_score(moves),
        accelerating=False,
        fading=False,
        reversal_bounce=False,
        volume=volume,
    )
    current_timing = entry_timing_metrics(
        moves=moves,
        quality=current_quality,
        accelerating=False,
        fading=False,
        reversal_bounce=False,
    )
    current_dynamic = dynamic_momentum_metrics(symbol, price, moves, now) if price else {}
    current_context = market_context_metrics(symbol, price, now) if price else {}
    current_relative = relative_strength_metrics(symbol, price, now) if price else {}
    current_flow = order_flow_metrics(symbol, now)
    current_mega = mega_move_metrics(symbol, price, now, current_dynamic, current_context, current_flow) if price else {}

    return {
        "asset": symbol,
        "asset_name": asset_display_name(symbol),
        "price": price,
        "source": source,
        "source_name": source_display_name(source),
        "moves": moves,
        "pump_score": pump_score(moves),
        "quality_score": current_quality["quality_score"],
        "quality": current_quality["quality"],
        "entry_status": current_timing["entry_status"],
        "late_entry_risk": current_timing["late_entry_risk"],
        "entry_reasons": current_timing["entry_reasons"],
        "bullish_impulse_pct": current_quality["bullish_impulse_pct"],
        "volume_ratio": current_quality["volume_ratio"],
        "volume_ratio_raw": current_quality["volume_ratio_raw"],
        "volume_confirmed": current_quality["volume_confirmed"],
        "volume_low_baseline": current_quality["volume_low_baseline"],
        "volume_bonus_applied": current_quality["volume_bonus_applied"],
        "signal_state": signal_state.get(symbol, {}).get("state", "NORMAL"),
        "signal_cycle": signal_state.get(symbol),
        "attention": attention_state.get(symbol, {}).get("level", "NORMAL"),
        "attention_cycle": attention_state.get(symbol),
        "hot_snapshots": list(hot_snapshots.get(symbol, []))[-30:],
        "dynamic": current_dynamic,
        "market_context": current_context,
        "relative_strength": current_relative,
        "order_flow": current_flow,
        "mega_move": current_mega,
        "move_stage": current_mega.get("stage"),
        "tracked": symbol in price_history,
    }
