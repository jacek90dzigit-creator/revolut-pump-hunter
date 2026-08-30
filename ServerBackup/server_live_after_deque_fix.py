"""
Pump Hunter Server 3.1.2
Compatibility wrapper for Pump Hunter 3.0.0.

3.1 goals:
- 72h market-context warmup on demand for assets that enter FOCUS/HOT or an active signal state.
- BTC context warmup as benchmark.
- Live trade-side order flow for Binance and Gate.io for currently interesting assets.
- Composite quality score with context, relative-strength and order-flow adjustments.
- Anti-saturation guard so quality=100 requires several independent confirmations.
- Stricter, slower re-entry rules.
- Richer lifecycle labels: FRESH_MOVE, FIRST_WAVE, PULLBACK, SECOND_WAVE,
  CONTINUATION, LATE_EXTENSION, DISTRIBUTION.
- Shadow outcome learner from the existing signal_outcomes database.
- New /v31-engine and /learner endpoints.

Deployment requirement:
Keep server_v3.0.0.py next to this file, or set:
PUMP_HUNTER_V30_PATH=/absolute/path/server_v3.0.0.py
"""

import asyncio
import importlib.util
import json
import math
import os
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import websockets


def _safe_deque_snapshot_v32(dq, retries=10):
    """Stable read-only snapshot used only by Engine 3.2 read paths."""
    if not dq:
        return []

    last = None

    for attempt in range(retries):
        try:
            return list(dq)
        except RuntimeError as exc:
            if "deque mutated during iteration" not in str(exc):
                raise

            last = exc

            if attempt >= 2:
                time.sleep(0)

    if last:
        raise last

    return []


VERSION = "3.2.0"

# ---------------------------------------------------------------------------
# Load 3.0 as the stable base. 3.1 patches selected functions and adds workers.
# ---------------------------------------------------------------------------

_THIS_DIR = Path(__file__).resolve().parent
_V30_CANDIDATES = [
    os.getenv("PUMP_HUNTER_V30_PATH"),
    str(_THIS_DIR / "server_v3.0.0.py"),
    "/home/opc/server_v3.0.0.py",
    "/opt/pump-hunter/server_v3.0.0.py",
]
_V30_PATH = next((p for p in _V30_CANDIDATES if p and Path(p).exists()), None)
if not _V30_PATH:
    raise RuntimeError(
        "Pump Hunter 3.1 needs server_v3.0.0.py. "
        "Place it next to server_v3.1.0.py or set PUMP_HUNTER_V30_PATH."
    )

_spec = importlib.util.spec_from_file_location("pump_hunter_v30_base", _V30_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load Pump Hunter 3.0 base from {_V30_PATH}")
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)

app = base.app
app.version = VERSION

# ---------------------------------------------------------------------------
# 3.1 configuration
# ---------------------------------------------------------------------------

CONTEXT_WARMUP_HOURS = 72
CONTEXT_WARMUP_INTERVAL = "15m"
CONTEXT_WARMUP_RETRY_SECONDS = 15 * 60
CONTEXT_WARMUP_MIN_AGE_SECONDS = 70 * 60 * 60
CONTEXT_WARMUP_CONCURRENCY = 3

FLOW_TRACK_REFRESH_SECONDS = 5
FLOW_RECONNECT_SECONDS = 20
FLOW_MAX_ASSETS = 24
FLOW_MIN_TRADES_1M = 5
FLOW_BULLISH_RATIO = 0.62
FLOW_BEARISH_RATIO = 0.38

# Context starts affecting quality before the very-high "extended" thresholds
# in 3.0. This catches late entries in assets already far into a multi-hour move.
CONTEXT_24H_SOFT_EXTENSION = 25.0
CONTEXT_24H_HARD_EXTENSION = 60.0
CONTEXT_72H_SOFT_EXTENSION = 60.0
CONTEXT_72H_HARD_EXTENSION = 140.0
CONTEXT_DISTRIBUTION_DRAWDOWN = -8.0

# Re-entry is deliberately stricter in 3.1. A second wave should prove itself.
base.REENTRY_MIN_QUALITY_SCORE = 78
base.REENTRY_CONFIRM_SECONDS = 30
base.REENTRY_COOLDOWN_SECONDS = 180
base.REENTRY_MAX_PER_CYCLE = 2
base.EXIT_REENTRY_MIN_M1_PCT = 2.25
base.EXIT_REENTRY_MIN_M3_PCT = 1.00
base.EXIT_REENTRY_MAX_DRAWDOWN_PCT = -1.75

base.COOLING_REENTRY_MIN_QUALITY_SCORE = 74
base.COOLING_REENTRY_CONFIRM_SECONDS = 20
base.COOLING_REENTRY_MIN_M1_PCT = 1.50
base.COOLING_REENTRY_MIN_M3_PCT = 0.50
base.COOLING_REENTRY_MAX_DRAWDOWN_PCT = -2.50

v31_state: Dict[str, object] = {
    "version": VERSION,
    "base_version": "3.0.0",
    "base_path": _V30_PATH,
    "started_at": int(time.time()),
    "context_warmup": {
        "running_assets": [],
        "completed_assets": 0,
        "failed_assets": 0,
        "last_errors": [],
    },
    "order_flow": {
        "binance_connected": False,
        "gate_connected": False,
        "binance_last_error": None,
        "gate_last_error": None,
        "tracked_assets": [],
        "last_event": 0,
    },
    "learner": {
        "mode": "SHADOW",
        "last_refresh": 0,
        "samples": 0,
    },
}

_context_warmup_last_try: Dict[str, float] = {}
_context_warmup_tasks: Dict[str, asyncio.Task] = {}
_eval_ctx: Dict[str, object] = {}
_learner_cache: Dict[str, object] = {"generated_at": 0, "rows": [], "summary": {}}

# Preserve selected 3.0 functions before patching.
_quality_v30 = base.quality_metrics
_mega_move_v30 = base.mega_move_metrics
_detect_signal_v30 = base.detect_signal
_persist_signal_v30 = base.persist_signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(value) -> Optional[float]:
    return float(value) if isinstance(value, (int, float)) and math.isfinite(float(value)) else None


def _relabel_quality(score: int) -> str:
    if score >= 80:
        return "VERY_HIGH"
    if score >= 65:
        return "HIGH"
    if score >= 45:
        return "MEDIUM"
    return "LOW"


def _tracked_assets() -> List[str]:
    """Return the highest-priority assets for expensive 3.1 workers."""
    candidates = []

    for asset, st in base.attention_state.items():
        level = str(st.get("level", "NORMAL"))
        if level in ("FOCUS", "HOT", "RAPID_WATCH", "RAPID_CONFIRMED"):
            rank = {
                "RAPID_CONFIRMED": 100,
                "RAPID_WATCH": 90,
                "HOT": 80,
                "FOCUS": 60,
            }.get(level, 0)
            candidates.append((rank, asset))

    for asset, st in base.signal_state.items():
        state = str(st.get("state", "NORMAL"))
        if state != "NORMAL":
            rank = {
                "PUMP": 95,
                "EARLY_MOVE": 75,
                "COOLING": 55,
                "EXIT": 35,
            }.get(state, 25)
            candidates.append((rank, asset))

    # Recent signal assets remain interesting for a little while.
    now = time.time()
    for sig in _safe_deque_snapshot_v32(base.signals)[:100]:
        ts = float(sig.get("timestamp", 0) or 0)
        if now - ts <= 30 * 60:
            candidates.append((45, str(sig.get("asset", "")).upper()))

    # BTC is always useful as relative-strength benchmark.
    candidates.append((110, "BTC"))

    best: Dict[str, int] = {}
    for rank, asset in candidates:
        if asset:
            best[asset] = max(best.get(asset, 0), rank)

    return [a for a, _ in sorted(best.items(), key=lambda x: x[1], reverse=True)[:FLOW_MAX_ASSETS]]


def _route_for_asset(asset: str) -> Optional[Tuple[str, str]]:
    routes = base._asset_routes()
    return routes.get(asset)


def _context_age_seconds(asset: str, now: Optional[float] = None) -> float:
    now = now or time.time()
    dq = base.market_context_history.get(asset)
    if not dq:
        return 0.0
    try:
        oldest_ts = float(dq[0][0])
    except Exception:
        return 0.0
    return max(0.0, now - oldest_ts)


# ---------------------------------------------------------------------------
# 72h on-demand context warmup
# ---------------------------------------------------------------------------

def _fetch_context_72h(source: str, symbol: str) -> List[Tuple[int, float]]:
    now = int(time.time())
    start = now - CONTEXT_WARMUP_HOURS * 3600
    timeout = 15
    rows: List[Tuple[int, float]] = []

    def windows_24h():
        windows = []
        cursor = start
        step = 24 * 3600
        while cursor < now:
            end = min(cursor + step, now)
            windows.append((cursor, end))
            cursor = end
        return windows

    if source == "BINANCE":
        rows = []
        for wstart, wend in windows_24h():
            r = requests.get(
                "https://data-api.binance.vision/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": "15m",
                    "startTime": wstart * 1000,
                    "endTime": wend * 1000,
                    "limit": 1000,
                },
                timeout=timeout,
            )
            r.raise_for_status()
            rows.extend((int(x[0]) // 1000, float(x[4])) for x in r.json())

    elif source == "BYBIT":
        r = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={
                "category": "spot",
                "symbol": symbol,
                "interval": "15",
                "start": start * 1000,
                "limit": 300,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json().get("result", {}).get("list", [])
        rows = [(int(x[0]) // 1000, float(x[4])) for x in data]

    elif source == "GATE":
        rows = []
        for wstart, wend in windows_24h():
            r = requests.get(
                "https://api.gateio.ws/api/v4/spot/candlesticks",
                params={
                    "currency_pair": symbol,
                    "interval": "15m",
                    "from": wstart,
                    "to": wend,
                    "limit": 1000,
                },
                timeout=timeout,
            )
            r.raise_for_status()
            # Gate spot candle: [timestamp, quote_volume, close, high, low, open, ...]
            rows.extend((int(float(x[0])), float(x[2])) for x in r.json())

    elif source == "OKX":
        r = requests.get(
            "https://www.okx.com/api/v5/market/history-candles",
            params={"instId": symbol, "bar": "15m", "limit": "300"},
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        rows = [(int(x[0]) // 1000, float(x[4])) for x in data]

    elif source == "KUCOIN":
        r = requests.get(
            "https://api.kucoin.com/api/v1/market/candles",
            params={
                "symbol": symbol,
                "type": "15min",
                "startAt": start,
                "endAt": now,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        # KuCoin candle: [time, open, close, high, low, volume, turnover]
        rows = [(int(x[0]), float(x[2])) for x in data]

    elif source == "COINBASE":
        start_iso = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
        end_iso = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        r = requests.get(
            f"https://api.exchange.coinbase.com/products/{symbol}/candles",
            params={"granularity": 900, "start": start_iso, "end": end_iso},
            timeout=timeout,
        )
        r.raise_for_status()
        # Coinbase: [time, low, high, open, close, volume]
        rows = [(int(x[0]), float(x[4])) for x in r.json()]

    elif source == "KRAKEN":
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": symbol, "interval": 15, "since": start},
            timeout=timeout,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        result = payload.get("result", {})
        data = next((v for k, v in result.items() if k != "last" and isinstance(v, list)), [])
        rows = [(int(x[0]), float(x[4])) for x in data]

    normalized = {}
    for ts, price in rows:
        if ts > 0 and price > 0:
            normalized[int(ts)] = float(price)

    return sorted(normalized.items())


def _persist_context_points(asset: str, source: str, points: List[Tuple[int, float]]) -> None:
    if not points or not base.DB_READY:
        return
    with base.DB_LOCK:
        con = base._db_connect()
        try:
            con.executemany(
                """INSERT OR IGNORE INTO market_context_snapshots(ts, asset, source, price)
                   VALUES (?, ?, ?, ?)""",
                [(int(ts), asset, source, float(price)) for ts, price in points],
            )
            con.commit()
        finally:
            con.close()


async def _warm_context_asset(asset: str) -> None:
    route = _route_for_asset(asset)
    if not route:
        return
    source, symbol = route
    running = v31_state["context_warmup"]["running_assets"]
    if asset not in running:
        running.append(asset)

    try:
        points = await asyncio.to_thread(_fetch_context_72h, source, symbol)
        if len(points) < 8:
            raise RuntimeError(f"only {len(points)} context candles returned")

        dq = base.market_context_history.setdefault(asset, deque(maxlen=512))
        dq_snapshot = _safe_deque_snapshot_v32(dq)
        existing = {int(float(x[0])) for x in dq_snapshot}
        merged = list(dq_snapshot)
        for ts, price in points:
            if int(ts) not in existing:
                merged.append((float(ts), float(price), source))

        merged.sort(key=lambda x: x[0])
        dq.clear()
        for item in merged[-512:]:
            dq.append(item)

        base._compact_market_context(asset, time.time())
        if dq:
            base.market_context_last_sample[asset] = float(dq[-1][0])

        await asyncio.to_thread(_persist_context_points, asset, source, points)
        v31_state["context_warmup"]["completed_assets"] += 1

    except Exception as exc:
        v31_state["context_warmup"]["failed_assets"] += 1
        errors = v31_state["context_warmup"]["last_errors"]
        errors.append({"asset": asset, "error": str(exc)[:180], "ts": int(time.time())})
        del errors[:-20]
    finally:
        if asset in running:
            running.remove(asset)
        _context_warmup_tasks.pop(asset, None)


async def context_warmup_manager() -> None:
    sem = asyncio.Semaphore(CONTEXT_WARMUP_CONCURRENCY)

    async def guarded(asset: str):
        async with sem:
            await _warm_context_asset(asset)

    while True:
        try:
            now = time.time()
            for asset in _tracked_assets():
                if _context_age_seconds(asset, now) >= CONTEXT_WARMUP_MIN_AGE_SECONDS:
                    continue
                if now - _context_warmup_last_try.get(asset, 0.0) < CONTEXT_WARMUP_RETRY_SECONDS:
                    continue
                if asset in _context_warmup_tasks:
                    continue
                _context_warmup_last_try[asset] = now
                task = asyncio.create_task(guarded(asset))
                _context_warmup_tasks[asset] = task
        except Exception:
            pass
        await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Live trade-side order flow for Binance and Gate.io
# ---------------------------------------------------------------------------

def _desired_binance_trade_streams() -> Dict[str, str]:
    tracked = set(_tracked_assets())
    out = {}
    for symbol, asset in base.binance_symbol_to_asset.items():
        if asset in tracked:
            out[f"{symbol.lower()}@aggTrade"] = asset
    return out


async def binance_order_flow_loop() -> None:
    while True:
        try:
            streams = _desired_binance_trade_streams()
            if not streams:
                v31_state["order_flow"]["binance_connected"] = False
                await asyncio.sleep(FLOW_TRACK_REFRESH_SECONDS)
                continue

            async with websockets.connect(
                base.BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=2_000_000,
            ) as ws:
                await ws.send(json.dumps({
                    "method": "SUBSCRIBE",
                    "params": list(streams.keys()),
                    "id": 3101,
                }))
                v31_state["order_flow"]["binance_connected"] = True
                v31_state["order_flow"]["binance_last_error"] = None
                connected_at = time.time()

                while True:
                    if time.time() - connected_at >= FLOW_RECONNECT_SECONDS:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    msg = json.loads(raw)
                    if msg.get("e") != "aggTrade":
                        continue

                    symbol = str(msg.get("s", "")).upper()
                    asset = base.binance_symbol_to_asset.get(symbol)
                    if not asset:
                        continue
                    try:
                        price = float(msg.get("p", 0))
                        qty = float(msg.get("q", 0))
                    except (TypeError, ValueError):
                        continue
                    # Binance "m": buyer is maker. Aggressive side is therefore sell.
                    side = "sell" if msg.get("m") is True else "buy"
                    now_event = time.time()
                    base.record_order_flow(asset, now_event, side, qty, price)
                    v31_state["order_flow"]["last_event"] = int(now_event)

        except Exception as exc:
            v31_state["order_flow"]["binance_connected"] = False
            v31_state["order_flow"]["binance_last_error"] = str(exc)[:180]
            await asyncio.sleep(5)


def _desired_gate_pairs() -> Dict[str, str]:
    tracked = set(_tracked_assets())
    out = {}
    for pair, asset in base.gate_symbol_to_asset.items():
        if asset in tracked:
            out[pair] = asset
    return out


async def gate_order_flow_loop() -> None:
    while True:
        try:
            pairs = _desired_gate_pairs()
            if not pairs:
                v31_state["order_flow"]["gate_connected"] = False
                await asyncio.sleep(FLOW_TRACK_REFRESH_SECONDS)
                continue

            async with websockets.connect(
                base.GATE_WS_URL,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=2_000_000,
            ) as ws:
                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "spot.trades",
                    "event": "subscribe",
                    "payload": list(pairs.keys()),
                }))
                v31_state["order_flow"]["gate_connected"] = True
                v31_state["order_flow"]["gate_last_error"] = None
                connected_at = time.time()

                while True:
                    if time.time() - connected_at >= FLOW_RECONNECT_SECONDS:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    msg = json.loads(raw)
                    if msg.get("channel") != "spot.trades" or msg.get("event") != "update":
                        continue

                    item = msg.get("result", {})
                    if not isinstance(item, dict):
                        continue
                    pair = str(item.get("currency_pair", "")).upper()
                    asset = base.gate_symbol_to_asset.get(pair)
                    if not asset:
                        continue
                    try:
                        price = float(item.get("price", 0))
                        qty = float(item.get("amount", 0))
                    except (TypeError, ValueError):
                        continue
                    side = str(item.get("side", "")).lower()
                    now_event = time.time()
                    base.record_order_flow(asset, now_event, side, qty, price)
                    v31_state["order_flow"]["last_event"] = int(now_event)

        except Exception as exc:
            v31_state["order_flow"]["gate_connected"] = False
            v31_state["order_flow"]["gate_last_error"] = str(exc)[:180]
            await asyncio.sleep(5)


async def flow_state_loop() -> None:
    while True:
        v31_state["order_flow"]["tracked_assets"] = _tracked_assets()
        await asyncio.sleep(FLOW_TRACK_REFRESH_SECONDS)


# ---------------------------------------------------------------------------
# Composite 3.1 quality model
# ---------------------------------------------------------------------------

def quality_metrics_v31(
    moves,
    momentum_score,
    accelerating,
    fading,
    reversal_bounce,
    volume,
):
    q = dict(_quality_v30(
        moves=moves,
        momentum_score=momentum_score,
        accelerating=accelerating,
        fading=fading,
        reversal_bounce=reversal_bounce,
        volume=volume,
    ))

    asset = str(_eval_ctx.get("asset") or "")
    price = _eval_ctx.get("price")
    now = float(_eval_ctx.get("now") or time.time())

    components = {
        "v30_score": int(q.get("quality_score", 0) or 0),
        "context": 0,
        "relative_strength": 0,
        "order_flow": 0,
        "anti_saturation": 0,
    }

    independent = 0
    if q.get("trend_aligned") is True:
        independent += 1
    if q.get("volume_confirmed") is True:
        independent += 1

    context = {}
    relative = {}
    flow = {}
    if asset and isinstance(price, (int, float)) and price > 0:
        context = base.market_context_metrics(asset, float(price), now)
        relative = base.relative_strength_metrics(asset, float(price), now)
        flow = base.order_flow_metrics(asset, now)

    ch24 = _pct(context.get("change_24h_pct"))
    ch72 = _pct(context.get("change_72h_pct"))
    dd72 = _pct(context.get("drawdown_from_72h_peak_pct"))

    # Multi-hour extension protection. This is intentionally gradual.
    if ch24 is not None:
        if ch24 >= CONTEXT_24H_HARD_EXTENSION:
            components["context"] -= 18
        elif ch24 >= CONTEXT_24H_SOFT_EXTENSION:
            components["context"] -= 8

    if ch72 is not None:
        if ch72 >= CONTEXT_72H_HARD_EXTENSION:
            components["context"] -= 18
        elif ch72 >= CONTEXT_72H_SOFT_EXTENSION:
            components["context"] -= 7

    # Falling materially from a 72h peak while momentum is trying to bounce
    # is more likely distribution than a fresh pump.
    if dd72 is not None and dd72 <= CONTEXT_DISTRIBUTION_DRAWDOWN:
        components["context"] -= 8

    rel5 = _pct(relative.get("relative_5m_pct"))
    rel1h = _pct(relative.get("relative_1h_pct"))
    rs_signal = 0
    if rel5 is not None:
        if rel5 >= 1.0:
            rs_signal += 5
        elif rel5 <= -1.0:
            rs_signal -= 6
    if rel1h is not None:
        if rel1h >= 2.0:
            rs_signal += 4
        elif rel1h <= -2.0:
            rs_signal -= 5
    components["relative_strength"] = max(-10, min(8, rs_signal))
    if components["relative_strength"] >= 4:
        independent += 1

    trades1 = int(flow.get("trades_1m", 0) or 0)
    buy1 = _pct(flow.get("buy_ratio_1m"))
    buy5 = _pct(flow.get("buy_ratio_5m"))
    flow_signal = 0
    if trades1 >= FLOW_MIN_TRADES_1M and buy1 is not None:
        if buy1 >= FLOW_BULLISH_RATIO:
            flow_signal += 8
            independent += 1
        elif buy1 <= FLOW_BEARISH_RATIO:
            flow_signal -= 12

    if buy5 is not None:
        if buy5 >= 0.60:
            flow_signal += 3
        elif buy5 <= 0.40:
            flow_signal -= 5
    components["order_flow"] = max(-15, min(11, flow_signal))

    score = int(q.get("quality_score", 0) or 0)
    score += components["context"]
    score += components["relative_strength"]
    score += components["order_flow"]

    # Anti-saturation: quality=100 in 3.0 was too easy. Very high confidence
    # now needs several independent confirmations, not a single strong impulse.
    if score >= 95:
        if independent <= 1:
            components["anti_saturation"] = -15
            score = min(score, 84)
        elif independent == 2:
            components["anti_saturation"] = -7
            score = min(score, 91)
        elif independent == 3:
            score = min(score, 97)

    score = max(0, min(100, int(round(score))))
    q["quality_score"] = score
    q["quality"] = _relabel_quality(score)
    q["v31_components"] = components
    q["v31_independent_confirmations"] = independent
    q["v31_context_ready"] = bool(context.get("ready"))
    q["v31_flow_ready"] = bool(flow.get("ready"))
    q["v31_relative_strength_ready"] = bool(relative.get("ready"))
    return q


# ---------------------------------------------------------------------------
# Lifecycle / move-stage model
# ---------------------------------------------------------------------------

def mega_move_metrics_v31(asset, current_price, now, dynamic, context, flow):
    out = dict(_mega_move_v30(asset, current_price, now, dynamic, context, flow))

    dscore = int(dynamic.get("score", 0) or 0)
    velocity = float(dynamic.get("weighted_velocity_pct", 0.0) or 0.0)
    ch24 = _pct(context.get("change_24h_pct"))
    ch72 = _pct(context.get("change_72h_pct"))
    dd72 = _pct(context.get("drawdown_from_72h_peak_pct")) or 0.0
    buy1 = _pct(flow.get("buy_ratio_1m"))

    stage = "NORMAL"
    reasons: List[str] = []

    hard_extended = (
        (ch24 is not None and ch24 >= CONTEXT_24H_HARD_EXTENSION)
        or (ch72 is not None and ch72 >= CONTEXT_72H_HARD_EXTENSION)
    )
    soft_extended = (
        (ch24 is not None and ch24 >= CONTEXT_24H_SOFT_EXTENSION)
        or (ch72 is not None and ch72 >= CONTEXT_72H_SOFT_EXTENSION)
    )

    sell_pressure = buy1 is not None and buy1 <= FLOW_BEARISH_RATIO
    buy_pressure = buy1 is not None and buy1 >= FLOW_BULLISH_RATIO

    recent_reaccel = (
        dd72 <= -2.0
        and dscore >= base.DYNAMIC_HOT_SCORE
        and velocity > 0
        and buy_pressure
        and int(flow.get("trades_1m", 0) or 0) >= FLOW_MIN_TRADES_1M
    )

    if recent_reaccel:
        stage = "SECOND_WAVE"
        reasons.append("REACCELERATION_BELOW_PRIOR_PEAK")
        reasons.append("BUY_FLOW_SUPPORT")
    elif dd72 <= -10.0 and dscore < base.DYNAMIC_FOCUS_SCORE:
        stage = "DISTRIBUTION"
        reasons.append("BELOW_72H_PEAK_AND_MOMENTUM_WEAK")
    elif hard_extended:
        stage = "LATE_EXTENSION"
        reasons.append("MULTI_HOUR_HARD_EXTENSION")
    elif dd72 <= -4.0 and dscore < base.DYNAMIC_HOT_SCORE:
        stage = "PULLBACK"
        reasons.append("PULLBACK_FROM_72H_PEAK")
    elif soft_extended and dscore >= base.DYNAMIC_HOT_SCORE:
        stage = "CONTINUATION"
        reasons.append("EXTENDED_BUT_STILL_ACCELERATING")
    elif dscore >= base.DYNAMIC_HOT_SCORE:
        stage = "FIRST_WAVE"
        reasons.append("LIVE_HOT_MOMENTUM")
    elif dscore >= base.DYNAMIC_FOCUS_SCORE:
        stage = "FRESH_MOVE"
        reasons.append("BUILDING_MOMENTUM")

    if sell_pressure and stage in ("SECOND_WAVE", "CONTINUATION", "FIRST_WAVE"):
        reasons.append("SELL_FLOW_WARNING")

    out["stage"] = stage
    out["stage_reasons"] = reasons
    out["soft_extended"] = bool(soft_extended)
    out["hard_extended"] = bool(hard_extended)
    out["flow_buy_pressure"] = bool(buy_pressure)
    out["flow_sell_pressure"] = bool(sell_pressure)
    return out


# ---------------------------------------------------------------------------
# Detection wrapper and signal enrichment
# ---------------------------------------------------------------------------

def detect_signal_v31(asset, source, source_symbol, price, moves, now):
    _eval_ctx.clear()
    _eval_ctx.update({
        "asset": asset,
        "source": source,
        "source_symbol": source_symbol,
        "price": price,
        "moves": moves,
        "now": now,
    })
    try:
        return _detect_signal_v30(asset, source, source_symbol, price, moves, now)
    finally:
        _eval_ctx.clear()


def persist_signal_v31(signal_item):
    asset = str(signal_item.get("asset", "")).upper()
    price = signal_item.get("signal_price", signal_item.get("price"))
    now = float(signal_item.get("timestamp", time.time()) or time.time())

    if asset and isinstance(price, (int, float)):
        context = base.market_context_metrics(asset, float(price), now)
        flow = base.order_flow_metrics(asset, now)
        relative = base.relative_strength_metrics(asset, float(price), now)
        signal_item["v31"] = {
            "context_age_hours": round(_context_age_seconds(asset, now) / 3600.0, 3),
            "context_ready_72h": _context_age_seconds(asset, now) >= 70 * 3600,
            "order_flow": flow,
            "relative_strength": relative,
            "market_context": context,
            "learner_mode": "SHADOW",
        }

    return _persist_signal_v30(signal_item)


# Activate patched functions inside the imported base module.
base.quality_metrics = quality_metrics_v31
base.mega_move_metrics = mega_move_metrics_v31
base.detect_signal = detect_signal_v31
base.persist_signal = persist_signal_v31


# ---------------------------------------------------------------------------
# Shadow learner
# ---------------------------------------------------------------------------

def _quality_band(score: Optional[int]) -> str:
    if score is None:
        return "UNKNOWN"
    if score >= 90:
        return "90-100"
    if score >= 80:
        return "80-89"
    if score >= 70:
        return "70-79"
    if score >= 60:
        return "60-69"
    if score >= 50:
        return "50-59"
    return "<50"


def _build_learner_snapshot() -> Dict[str, object]:
    if not base.DB_READY:
        return {"ready": False, "reason": "DB_NOT_READY", "rows": []}

    with base.DB_LOCK:
        con = base._db_connect()
        try:
            outcome_rows = con.execute(
                """SELECT signal_ts, asset, signal_type, horizon_min,
                          change_pct, max_gain_pct, max_drawdown_pct
                   FROM signal_outcomes
                   WHERE horizon_min IN (5, 10, 15, 30)
                   ORDER BY id DESC
                   LIMIT 5000"""
            ).fetchall()

            signal_rows = con.execute(
                """SELECT ts, asset, signal_type, payload_json
                   FROM signal_events
                   ORDER BY id DESC
                   LIMIT 1500"""
            ).fetchall()
        finally:
            con.close()

    signal_meta = {}
    for row in signal_rows:
        key = (int(row["ts"]), str(row["asset"]), str(row["signal_type"]))
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            payload = {}
        signal_meta[key] = {
            "quality_score": payload.get("quality_score"),
            "entry_status": payload.get("entry_status"),
            "move_stage": payload.get("move_stage"),
        }

    groups: Dict[Tuple[str, int, str], Dict[str, float]] = {}
    for row in outcome_rows:
        key_meta = (int(row["signal_ts"]), str(row["asset"]), str(row["signal_type"]))
        meta = signal_meta.get(key_meta, {})
        band = _quality_band(meta.get("quality_score"))
        key = (str(row["signal_type"]), int(row["horizon_min"]), band)
        g = groups.setdefault(key, {
            "n": 0,
            "positive": 0,
            "gain2": 0,
            "gain5": 0,
            "sum_change": 0.0,
            "sum_max_gain": 0.0,
            "sum_drawdown": 0.0,
        })
        ch = float(row["change_pct"] or 0.0)
        mg = float(row["max_gain_pct"] or 0.0)
        md = float(row["max_drawdown_pct"] or 0.0)
        g["n"] += 1
        g["positive"] += 1 if ch > 0 else 0
        g["gain2"] += 1 if mg >= 2.0 else 0
        g["gain5"] += 1 if mg >= 5.0 else 0
        g["sum_change"] += ch
        g["sum_max_gain"] += mg
        g["sum_drawdown"] += md

    rows = []
    for (signal_type, horizon, band), g in groups.items():
        n = int(g["n"])
        if n <= 0:
            continue
        rows.append({
            "signal_type": signal_type,
            "horizon_min": horizon,
            "quality_band": band,
            "n": n,
            "positive_rate": round(g["positive"] / n, 4),
            "max_gain_2pct_rate": round(g["gain2"] / n, 4),
            "max_gain_5pct_rate": round(g["gain5"] / n, 4),
            "avg_change_pct": round(g["sum_change"] / n, 4),
            "avg_max_gain_pct": round(g["sum_max_gain"] / n, 4),
            "avg_max_drawdown_pct": round(g["sum_drawdown"] / n, 4),
        })

    rows.sort(key=lambda x: (x["signal_type"], x["horizon_min"], x["quality_band"]))
    return {
        "ready": True,
        "mode": "SHADOW",
        "generated_at": int(time.time()),
        "samples": len(outcome_rows),
        "rows": rows,
    }


async def learner_loop() -> None:
    global _learner_cache
    while True:
        try:
            snap = await asyncio.to_thread(_build_learner_snapshot)
            _learner_cache = snap
            v31_state["learner"]["last_refresh"] = int(time.time())
            v31_state["learner"]["samples"] = int(snap.get("samples", 0) or 0)
        except Exception:
            pass
        await asyncio.sleep(5 * 60)


# ---------------------------------------------------------------------------
# Startup extension
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_v31() -> None:
    # Base startup has already initialized DB, whitelist and mappings.
    try:
        if base.DB_READY and base.runtime_session_id is not None:
            with base.DB_LOCK:
                con = base._db_connect()
                try:
                    con.execute(
                        "UPDATE runtime_sessions SET version=?, note=? WHERE id=?",
                        (VERSION, "startup_v31", int(base.runtime_session_id)),
                    )
                    con.commit()
                finally:
                    con.close()
    except Exception:
        pass

    asyncio.create_task(context_warmup_manager())
    asyncio.create_task(binance_order_flow_loop())
    asyncio.create_task(gate_order_flow_loop())
    asyncio.create_task(flow_state_loop())
    asyncio.create_task(learner_loop())


# ---------------------------------------------------------------------------
# Replace root route so the live version is unambiguous.
# ---------------------------------------------------------------------------

# Remove only the original GET / route from 3.0.
for route in list(app.router.routes):
    if getattr(route, "path", None) == "/" and "GET" in getattr(route, "methods", set()):
        try:
            app.router.routes.remove(route)
        except ValueError:
            pass

@app.get("/")
def root_v311():
    return {
        "name": "Pump Hunter Server",
        "version": VERSION,
        "status": "running",
        "base": "3.0.0",
        "engine": "hybrid-v312",
    }


@app.get("/v31-engine")
def v31_engine():
    now = time.time()
    tracked = _tracked_assets()
    context_rows = []
    for asset in tracked[:30]:
        price = base.latest_prices.get(asset)
        if not isinstance(price, (int, float)):
            continue
        context = base.market_context_metrics(asset, float(price), now)
        flow = base.order_flow_metrics(asset, now)
        relative = base.relative_strength_metrics(asset, float(price), now)
        context_rows.append({
            "asset": asset,
            "source": base.latest_sources.get(asset),
            "attention": base.attention_state.get(asset, {}).get("level", "NORMAL"),
            "signal_state": base.signal_state.get(asset, {}).get("state", "NORMAL"),
            "context_age_hours": round(_context_age_seconds(asset, now) / 3600.0, 3),
            "context_ready": context.get("ready"),
            "change_24h_pct": context.get("change_24h_pct"),
            "change_72h_pct": context.get("change_72h_pct"),
            "drawdown_from_72h_peak_pct": context.get("drawdown_from_72h_peak_pct"),
            "relative_5m_pct": relative.get("relative_5m_pct"),
            "relative_1h_pct": relative.get("relative_1h_pct"),
            "flow_ready": flow.get("ready"),
            "flow_status": (
                "READY" if flow.get("ready") else
                "NO_RECENT_TRADES" if asset in set(v31_state["order_flow"].get("tracked_assets", [])) else
                "NOT_TRACKED"
            ),
            "buy_ratio_1m": flow.get("buy_ratio_1m"),
            "trades_1m": flow.get("trades_1m"),
        })

    return {
        "version": VERSION,
        "base_version": "3.0.0",
        "sample_every_seconds": base.SAMPLE_EVERY_SECONDS,
        "market_context_hours": CONTEXT_WARMUP_HOURS,
        "market_context_mode": "LIVE_PLUS_ON_DEMAND_72H",
        "order_flow_mode": "FOCUS_HOT_ACTIVE_ONLY",
        "quality_model": "COMPOSITE_ANTI_SATURATION_V311",
        "reentry_model": "STRICT_SECOND_WAVE_CONFIRMED",
        "learner_mode": "SHADOW",
        "tracked_assets": tracked,
        "workers": v31_state,
        "assets": context_rows,
    }


# Replace inherited /telemetry endpoint so reported version matches live wrapper.
for route in list(app.router.routes):
    if getattr(route, "path", None) == "/telemetry" and "GET" in getattr(route, "methods", set()):
        try:
            app.router.routes.remove(route)
        except ValueError:
            pass

@app.get("/telemetry")
def telemetry_v312(limit: int = 100):
    payload = base.telemetry_overview(limit=limit)
    if isinstance(payload, dict):
        payload = dict(payload)
        payload["version"] = VERSION
    return payload


@app.get("/learner")
def learner():
    return _learner_cache


@app.get("/market-context-v31/{asset}")
def market_context_v31(asset: str):
    asset = asset.upper()
    price = base.latest_prices.get(asset)
    if not isinstance(price, (int, float)):
        return {"asset": asset, "ready": False, "reason": "NO_LIVE_PRICE"}

    now = time.time()
    context = base.market_context_metrics(asset, float(price), now)
    dynamic = base.dynamic_momentum_metrics(
        asset,
        float(price),
        base.calculate_moves(asset, now, float(price)),
        now,
    )
    flow = base.order_flow_metrics(asset, now)
    relative = base.relative_strength_metrics(asset, float(price), now)
    lifecycle = mega_move_metrics_v31(asset, float(price), now, dynamic, context, flow)

    return {
        "asset": asset,
        "price": price,
        "source": base.latest_sources.get(asset),
        "context_age_hours": round(_context_age_seconds(asset, now) / 3600.0, 3),
        "context": context,
        "relative_strength": relative,
        "order_flow": flow,
        "lifecycle": lifecycle,
        "warmup_running": asset in _context_warmup_tasks,
    }

# ===========================================================================
# PUMP HUNTER 3.2.0 FUSION ENGINE
# ===========================================================================

VERSION = "3.2.0"
app.version = VERSION

# ---------------------------------------------------------------------------
# 3.2 tuning
# ---------------------------------------------------------------------------

# 3.1.2 praktycznie zabiło RE_ENTRY. 3.2 przywraca je ostrożnie.
base.REENTRY_MIN_QUALITY_SCORE = 70
base.REENTRY_CONFIRM_SECONDS = 20
base.REENTRY_COOLDOWN_SECONDS = 120
base.REENTRY_MAX_PER_CYCLE = 2
base.EXIT_REENTRY_MIN_M1_PCT = 1.25
base.EXIT_REENTRY_MIN_M3_PCT = 0.45
base.EXIT_REENTRY_MAX_DRAWDOWN_PCT = -2.25

base.COOLING_REENTRY_MIN_QUALITY_SCORE = 68
base.COOLING_REENTRY_CONFIRM_SECONDS = 15
base.COOLING_REENTRY_MIN_M1_PCT = 0.90
base.COOLING_REENTRY_MIN_M3_PCT = 0.30
base.COOLING_REENTRY_MAX_DRAWDOWN_PCT = -3.00

# WebSocket nie ma się bez sensu odnawiać co 20 sekund.
FLOW_RECONNECT_SECONDS = 15 * 60

FUSION_EARLY_MIN = 58
FUSION_PUMP_MIN = 66
FUSION_REENTRY_MIN = 64
FUSION_HARD_BLOCK = 42

V32_STATE_TTL = 6 * 3600

v32_asset_state = {}
v32_stats = {
    "fusion_evaluated": 0,
    "fusion_blocked": 0,
    "early_blocked": 0,
    "pump_blocked": 0,
    "reentry_seen": 0,
    "reentry_accepted": 0,
    "ws_binance_reconnects": 0,
    "ws_gate_reconnects": 0,
}

# Zachowujemy aktywny persist z 3.1.2.
_persist_signal_v31_active = base.persist_signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _v32_num(v, default=None):
    try:
        if v is None:
            return default
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default


def _v32_clip(v, lo, hi):
    return max(lo, min(hi, v))


def _v32_signal_type(item):
    return str(
        item.get("signal_type")
        or item.get("type")
        or item.get("state")
        or ""
    ).upper()


def _v32_asset(item):
    return str(item.get("asset") or "").upper()


def _v32_price(item, asset):
    for key in ("signal_price", "price", "current_price"):
        x = _v32_num(item.get(key))
        if x and x > 0:
            return x
    x = _v32_num(base.latest_prices.get(asset))
    return x


def _v32_moves_from_payload(item):
    out = {}

    candidates = [
        item.get("moves"),
        item.get("change"),
        item.get("changes"),
        item.get("movement"),
    ]

    for obj in candidates:
        if isinstance(obj, dict):
            for k in ("1m", "2m", "3m", "5m", "10m", "15m", "30m"):
                if k in obj:
                    out[k] = _v32_num(obj.get(k))

    for k in ("1m", "2m", "3m", "5m", "10m", "15m", "30m"):
        if k not in out:
            for key in (
                k,
                f"change_{k}_pct",
                f"asset_{k}_pct",
                f"relative_{k}_pct",
            ):
                if key in item:
                    out[k] = _v32_num(item.get(key))
                    break

    return out


def _v32_recent_prices(asset, limit=30):
    dq = base.price_history.get(asset)
    if not dq:
        return []

    vals = []
    try:
        rows = _safe_deque_snapshot_v32(dq)[-limit:]
    except Exception:
        return []

    for row in rows:
        price = None

        if isinstance(row, (tuple, list)):
            if len(row) >= 2:
                price = _v32_num(row[1])
        elif isinstance(row, dict):
            price = _v32_num(
                row.get("price")
                or row.get("close")
                or row.get("value")
            )

        if price and price > 0:
            vals.append(price)

    return vals


def _v32_rsi(asset, period=14):
    prices = _v32_recent_prices(asset, period + 8)

    if len(prices) < period + 1:
        return None

    prices = prices[-(period + 1):]

    gains = []
    losses = []

    for a, b in zip(prices[:-1], prices[1:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss <= 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def _v32_structure(asset):
    prices = _v32_recent_prices(asset, 24)

    if len(prices) < 8:
        return {
            "ready": False,
            "trend": "UNKNOWN",
            "breakout": False,
            "squeeze": False,
        }

    short = prices[-5:]
    medium = prices[-12:] if len(prices) >= 12 else prices

    sma5 = sum(short) / len(short)
    sma12 = sum(medium) / len(medium)

    last = prices[-1]
    prev_high = max(prices[:-1])

    mean = sum(medium) / len(medium)
    variance = sum((x - mean) ** 2 for x in medium) / len(medium)
    std = math.sqrt(variance)

    width_pct = ((4 * std) / mean * 100.0) if mean > 0 else None

    trend = (
        "UP"
        if last >= sma5 >= sma12
        else "DOWN"
        if last <= sma5 <= sma12
        else "MIXED"
    )

    return {
        "ready": True,
        "trend": trend,
        "breakout": bool(last > prev_high),
        "squeeze": bool(width_pct is not None and width_pct <= 3.0),
        "band_width_pct": round(width_pct, 4) if width_pct is not None else None,
        "sma5": sma5,
        "sma12": sma12,
    }


def _v32_btc_regime(now):
    price = base.latest_prices.get("BTC")

    if not isinstance(price, (int, float)) or price <= 0:
        return {
            "ready": False,
            "regime": "UNKNOWN",
            "score": 0,
        }

    relative = base.relative_strength_metrics("BTC", float(price), now)
    context = base.market_context_metrics("BTC", float(price), now)

    ch1h = _v32_num(
        context.get("change_1h_pct")
        or relative.get("asset_1h_pct")
        or relative.get("relative_1h_pct")
    )

    ch24 = _v32_num(context.get("change_24h_pct"))

    score = 0

    if ch1h is not None:
        if ch1h >= 1.0:
            score += 6
        elif ch1h <= -1.0:
            score -= 8

    if ch24 is not None:
        if ch24 >= 2.0:
            score += 4
        elif ch24 <= -3.0:
            score -= 6

    if score >= 5:
        regime = "RISK_ON"
    elif score <= -7:
        regime = "RISK_OFF"
    else:
        regime = "NEUTRAL"

    return {
        "ready": True,
        "regime": regime,
        "score": score,
        "change_1h_pct": ch1h,
        "change_24h_pct": ch24,
    }


def _v32_volume_component(asset, now):
    try:
        volume = base.activity_metrics(asset, now)
    except Exception:
        volume = {}

    ratio = _v32_num(
        volume.get("volume_ratio_raw")
        or volume.get("volume_ratio")
    )

    confirmed = volume.get("volume_confirmed") is True
    strong = volume.get("volume_strong") is True

    score = 0

    if ratio is not None:
        if ratio >= 5.0:
            score = 18
        elif ratio >= 3.0:
            score = 15
        elif ratio >= 2.0:
            score = 11
        elif ratio >= 1.4:
            score = 7
        elif ratio < 0.7:
            score = -5

    if confirmed:
        score += 3

    if strong:
        score += 3

    return {
        "score": _v32_clip(score, -8, 20),
        "ratio": ratio,
        "confirmed": confirmed,
        "strong": strong,
    }


def _v32_flow_component(asset, now):
    try:
        flow = base.order_flow_metrics(asset, now)
    except Exception:
        flow = {}

    buy1 = _v32_num(flow.get("buy_ratio_1m"))
    buy5 = _v32_num(flow.get("buy_ratio_5m"))
    trades = int(flow.get("trades_1m", 0) or 0)

    score = 0

    if trades >= 5 and buy1 is not None:
        if buy1 >= 0.72:
            score += 17
        elif buy1 >= 0.62:
            score += 12
        elif buy1 >= 0.55:
            score += 5
        elif buy1 <= 0.35:
            score -= 18
        elif buy1 <= 0.42:
            score -= 10

    if buy5 is not None:
        if buy5 >= 0.62:
            score += 4
        elif buy5 <= 0.38:
            score -= 5

    return {
        "score": _v32_clip(score, -20, 20),
        "buy_ratio_1m": buy1,
        "buy_ratio_5m": buy5,
        "trades_1m": trades,
        "ready": bool(flow.get("ready")),
    }


def _v32_momentum_component(asset, item):
    moves = _v32_moves_from_payload(item)

    m1 = _v32_num(moves.get("1m"))
    m3 = _v32_num(moves.get("3m"))
    m5 = _v32_num(moves.get("5m"))

    rsi = _v32_rsi(asset)

    score = 0

    if m1 is not None:
        if 0.25 <= m1 <= 3.0:
            score += 6
        elif m1 > 6.0:
            score -= 8
        elif m1 < -1.0:
            score -= 7

    if m3 is not None:
        if 0.5 <= m3 <= 6.0:
            score += 6
        elif m3 < -2.0:
            score -= 6

    if m5 is not None:
        if 0.8 <= m5 <= 10.0:
            score += 5
        elif m5 < -3.0:
            score -= 5

    if rsi is not None:
        if 52 <= rsi <= 72:
            score += 8
        elif 72 < rsi <= 82:
            score += 3
        elif rsi > 88:
            score -= 10
        elif rsi < 35:
            score -= 5

    acceleration = False

    if all(x is not None for x in (m1, m3, m5)):
        acceleration = (
            m1 > 0
            and m3 > m1
            and m5 > m3
        )

        if acceleration:
            score += 5

    return {
        "score": _v32_clip(score, -20, 25),
        "rsi14": rsi,
        "m1": m1,
        "m3": m3,
        "m5": m5,
        "acceleration": acceleration,
    }


def _v32_context_component(asset, price, now):
    if not price:
        return {"score": 0}

    try:
        context = base.market_context_metrics(asset, price, now)
        relative = base.relative_strength_metrics(asset, price, now)
    except Exception:
        context = {}
        relative = {}

    ch24 = _v32_num(context.get("change_24h_pct"))
    ch72 = _v32_num(context.get("change_72h_pct"))
    dd = _v32_num(context.get("drawdown_from_72h_peak_pct"))
    rel5 = _v32_num(relative.get("relative_5m_pct"))
    rel1h = _v32_num(relative.get("relative_1h_pct"))

    score = 0

    if rel5 is not None:
        if rel5 >= 1.0:
            score += 5
        elif rel5 <= -1.0:
            score -= 5

    if rel1h is not None:
        if rel1h >= 2.0:
            score += 5
        elif rel1h <= -2.0:
            score -= 5

    if ch24 is not None:
        if ch24 > 70:
            score -= 12
        elif ch24 > 35:
            score -= 5

    if ch72 is not None:
        if ch72 > 150:
            score -= 12
        elif ch72 > 80:
            score -= 5

    if dd is not None and dd <= -10:
        score -= 8

    return {
        "score": _v32_clip(score, -20, 15),
        "change_24h_pct": ch24,
        "change_72h_pct": ch72,
        "drawdown_72h_pct": dd,
        "relative_5m_pct": rel5,
        "relative_1h_pct": rel1h,
    }


def _v32_structure_component(asset):
    structure = _v32_structure(asset)

    score = 0

    if structure.get("ready"):
        trend = structure.get("trend")

        if trend == "UP":
            score += 8
        elif trend == "DOWN":
            score -= 10

        if structure.get("breakout"):
            score += 8

        if structure.get("squeeze"):
            score += 4

    structure["score"] = _v32_clip(score, -12, 20)
    return structure


def _v32_fusion(item):
    asset = _v32_asset(item)
    now = float(item.get("timestamp") or time.time())
    price = _v32_price(item, asset)

    quality = int(item.get("quality_score", 0) or 0)

    momentum = _v32_momentum_component(asset, item)
    volume = _v32_volume_component(asset, now)
    flow = _v32_flow_component(asset, now)
    context = _v32_context_component(asset, price, now)
    structure = _v32_structure_component(asset)
    btc = _v32_btc_regime(now)

    # 30 pkt jakości bazowej.
    base_quality = _v32_clip(quality * 0.30, 0, 30)

    score = (
        base_quality
        + momentum["score"]
        + volume["score"]
        + flow["score"]
        + context["score"]
        + structure["score"]
    )

    if btc.get("regime") == "RISK_ON":
        score += 4
    elif btc.get("regime") == "RISK_OFF":
        score -= 8

    # Anti-fakeout.
    fakeout_reasons = []

    if (
        momentum.get("m1") is not None
        and momentum["m1"] >= 5.0
        and not volume.get("confirmed")
        and (flow.get("buy_ratio_1m") or 0) < 0.55
    ):
        score -= 18
        fakeout_reasons.append("FAST_SPIKE_WITHOUT_VOLUME_FLOW")

    if (
        structure.get("trend") == "DOWN"
        and not structure.get("breakout")
    ):
        score -= 10
        fakeout_reasons.append("BEARISH_STRUCTURE")

    if momentum.get("rsi14") is not None and momentum["rsi14"] >= 90:
        score -= 8
        fakeout_reasons.append("RSI_EXTREME")

    score = int(round(_v32_clip(score, 0, 100)))

    if score >= 82:
        grade = "A"
    elif score >= 72:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 48:
        grade = "D"
    else:
        grade = "F"

    return {
        "score": score,
        "grade": grade,
        "quality_base": quality,
        "momentum": momentum,
        "volume": volume,
        "order_flow": flow,
        "context": context,
        "structure": structure,
        "btc_regime": btc,
        "fakeout_reasons": fakeout_reasons,
    }


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

def _v32_phase_for_signal(sig_type):
    return {
        "EARLY_MOVE": "BUILDING",
        "PUMP": "IMPULSE",
        "COOLING": "PULLBACK",
        "EXIT": "DISTRIBUTION",
        "RE_ENTRY": "SECOND_WAVE",
    }.get(sig_type, "NORMAL")


def _v32_update_state(asset, sig_type, fusion, now):
    st = v32_asset_state.setdefault(asset, {
        "phase": "NORMAL",
        "previous_phase": None,
        "since": now,
        "last_signal": None,
        "last_signal_at": 0,
        "cycle_started_at": 0,
        "reentries": 0,
        "peak_fusion": 0,
    })

    old = str(st.get("phase", "NORMAL"))
    new = _v32_phase_for_signal(sig_type)

    if new != old:
        st["previous_phase"] = old
        st["phase"] = new
        st["since"] = now

    if sig_type == "EARLY_MOVE" and old in ("NORMAL", "DISTRIBUTION"):
        st["cycle_started_at"] = now
        st["reentries"] = 0

    if sig_type == "RE_ENTRY":
        st["reentries"] = int(st.get("reentries", 0)) + 1

    st["last_signal"] = sig_type
    st["last_signal_at"] = now
    st["peak_fusion"] = max(
        int(st.get("peak_fusion", 0)),
        int(fusion.get("score", 0)),
    )

    return st


def _v32_state_guard(asset, sig_type, fusion, now):
    st = v32_asset_state.get(asset, {})

    previous = str(st.get("phase", "NORMAL"))
    score = int(fusion.get("score", 0))

    allow = True
    reason = "OK"

    if sig_type == "EARLY_MOVE":
        if score < FUSION_EARLY_MIN:
            allow = False
            reason = "EARLY_LOW_FUSION"

    elif sig_type == "PUMP":
        if score < FUSION_PUMP_MIN:
            allow = False
            reason = "PUMP_LOW_FUSION"

    elif sig_type == "RE_ENTRY":
        v32_stats["reentry_seen"] += 1

        valid_previous = previous in (
            "PULLBACK",
            "DISTRIBUTION",
            "IMPULSE",
        )

        if score < FUSION_REENTRY_MIN:
            allow = False
            reason = "REENTRY_LOW_FUSION"
        elif not valid_previous:
            allow = False
            reason = "REENTRY_INVALID_PHASE"
        else:
            v32_stats["reentry_accepted"] += 1

    # Nie blokujemy EXIT/COOLING. One są potrzebne do zarządzania cyklem.

    if score < FUSION_HARD_BLOCK and sig_type in ("EARLY_MOVE", "PUMP", "RE_ENTRY"):
        allow = False
        reason = "HARD_FUSION_BLOCK"

    return allow, reason


def _v32_cleanup_states(now):
    dead = []

    for asset, st in v32_asset_state.items():
        last = float(st.get("last_signal_at", 0) or 0)

        if last and now - last > V32_STATE_TTL:
            dead.append(asset)

    for asset in dead:
        v32_asset_state.pop(asset, None)


# ---------------------------------------------------------------------------
# Signal wrapper
# ---------------------------------------------------------------------------

def persist_signal_v32(signal_item):
    asset = _v32_asset(signal_item)
    sig_type = _v32_signal_type(signal_item)
    now = float(signal_item.get("timestamp") or time.time())

    if not asset or not sig_type:
        return _persist_signal_v31_active(signal_item)

    fusion = _v32_fusion(signal_item)

    signal_item["v32"] = {
        "fusion": fusion,
        "state_before": dict(v32_asset_state.get(asset, {})),
        "engine": "FUSION_V32",
    }

    v32_stats["fusion_evaluated"] += 1

    allow, reason = _v32_state_guard(
        asset,
        sig_type,
        fusion,
        now,
    )

    signal_item["v32"]["allowed"] = allow
    signal_item["v32"]["decision_reason"] = reason

    if not allow:
        v32_stats["fusion_blocked"] += 1

        if sig_type == "EARLY_MOVE":
            v32_stats["early_blocked"] += 1

        if sig_type == "PUMP":
            v32_stats["pump_blocked"] += 1

        return None

    st = _v32_update_state(
        asset,
        sig_type,
        fusion,
        now,
    )

    signal_item["v32"]["state_after"] = dict(st)

    _v32_cleanup_states(now)

    return _persist_signal_v31_active(signal_item)


base.persist_signal = persist_signal_v32


# ---------------------------------------------------------------------------
# Robust WebSocket workers with exponential backoff
# ---------------------------------------------------------------------------

async def binance_order_flow_loop():
    backoff = 2

    while True:
        try:
            streams = _desired_binance_trade_streams()

            if not streams:
                v31_state["order_flow"]["binance_connected"] = False
                await asyncio.sleep(FLOW_TRACK_REFRESH_SECONDS)
                continue

            async with websockets.connect(
                base.BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=45,
                close_timeout=8,
                max_size=2_000_000,
            ) as ws:

                await ws.send(json.dumps({
                    "method": "SUBSCRIBE",
                    "params": list(streams.keys()),
                    "id": 3201,
                }))

                v31_state["order_flow"]["binance_connected"] = True
                v31_state["order_flow"]["binance_last_error"] = None

                backoff = 2
                connected_at = time.time()

                while True:
                    if time.time() - connected_at >= FLOW_RECONNECT_SECONDS:
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        try:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=10)
                            continue
                        except Exception:
                            raise

                    msg = json.loads(raw)

                    if msg.get("e") != "aggTrade":
                        continue

                    symbol = str(msg.get("s", "")).upper()
                    asset = base.binance_symbol_to_asset.get(symbol)

                    if not asset:
                        continue

                    try:
                        price = float(msg.get("p", 0))
                        qty = float(msg.get("q", 0))
                    except (TypeError, ValueError):
                        continue

                    side = "sell" if msg.get("m") is True else "buy"
                    evt = time.time()

                    base.record_order_flow(
                        asset,
                        evt,
                        side,
                        qty,
                        price,
                    )

                    v31_state["order_flow"]["last_event"] = int(evt)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            v31_state["order_flow"]["binance_connected"] = False
            v31_state["order_flow"]["binance_last_error"] = str(exc)[:180]

            v32_stats["ws_binance_reconnects"] += 1

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def gate_order_flow_loop():
    backoff = 2

    while True:
        try:
            pairs = _desired_gate_pairs()

            if not pairs:
                v31_state["order_flow"]["gate_connected"] = False
                await asyncio.sleep(FLOW_TRACK_REFRESH_SECONDS)
                continue

            async with websockets.connect(
                base.GATE_WS_URL,
                ping_interval=20,
                ping_timeout=45,
                close_timeout=8,
                max_size=2_000_000,
            ) as ws:

                await ws.send(json.dumps({
                    "time": int(time.time()),
                    "channel": "spot.trades",
                    "event": "subscribe",
                    "payload": list(pairs.keys()),
                }))

                v31_state["order_flow"]["gate_connected"] = True
                v31_state["order_flow"]["gate_last_error"] = None

                backoff = 2
                connected_at = time.time()

                while True:
                    if time.time() - connected_at >= FLOW_RECONNECT_SECONDS:
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        try:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=10)
                            continue
                        except Exception:
                            raise

                    msg = json.loads(raw)

                    if (
                        msg.get("channel") != "spot.trades"
                        or msg.get("event") != "update"
                    ):
                        continue

                    item = msg.get("result", {})

                    if not isinstance(item, dict):
                        continue

                    pair = str(
                        item.get("currency_pair", "")
                    ).upper()

                    asset = base.gate_symbol_to_asset.get(pair)

                    if not asset:
                        continue

                    try:
                        price = float(item.get("price", 0))
                        qty = float(item.get("amount", 0))
                    except (TypeError, ValueError):
                        continue

                    side = str(item.get("side", "")).lower()
                    evt = time.time()

                    base.record_order_flow(
                        asset,
                        evt,
                        side,
                        qty,
                        price,
                    )

                    v31_state["order_flow"]["last_event"] = int(evt)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            v31_state["order_flow"]["gate_connected"] = False
            v31_state["order_flow"]["gate_last_error"] = str(exc)[:180]

            v32_stats["ws_gate_reconnects"] += 1

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Replace root route and expose 3.2 diagnostics
# ---------------------------------------------------------------------------

for route in list(app.router.routes):
    if (
        getattr(route, "path", None) == "/"
        and "GET" in getattr(route, "methods", set())
    ):
        try:
            app.router.routes.remove(route)
        except ValueError:
            pass


@app.get("/")
def root_v320():
    return {
        "name": "Pump Hunter Server",
        "version": VERSION,
        "status": "running",
        "base": "3.0.0",
        "engine": "fusion-v32",
    }


@app.get("/v32-engine")
def v32_engine():
    now = time.time()

    active = []

    for asset, st in sorted(
        v32_asset_state.items(),
        key=lambda kv: float(
            kv[1].get("last_signal_at", 0) or 0
        ),
        reverse=True,
    )[:50]:
        active.append({
            "asset": asset,
            **st,
        })

    return {
        "version": VERSION,
        "engine": "FUSION_V32",
        "fusion_thresholds": {
            "early": FUSION_EARLY_MIN,
            "pump": FUSION_PUMP_MIN,
            "reentry": FUSION_REENTRY_MIN,
            "hard_block": FUSION_HARD_BLOCK,
        },
        "reentry": {
            "quality": base.REENTRY_MIN_QUALITY_SCORE,
            "confirm_seconds": base.REENTRY_CONFIRM_SECONDS,
            "cooldown_seconds": base.REENTRY_COOLDOWN_SECONDS,
            "exit_min_1m": base.EXIT_REENTRY_MIN_M1_PCT,
            "exit_min_3m": base.EXIT_REENTRY_MIN_M3_PCT,
        },
        "stats": dict(v32_stats),
        "order_flow": {
            "binance_connected": v31_state["order_flow"].get("binance_connected"),
            "gate_connected": v31_state["order_flow"].get("gate_connected"),
            "binance_last_error": v31_state["order_flow"].get("binance_last_error"),
            "gate_last_error": v31_state["order_flow"].get("gate_last_error"),
            "last_event": v31_state["order_flow"].get("last_event"),
            "reconnect_interval_seconds": FLOW_RECONNECT_SECONDS,
        },
        "btc_regime": _v32_btc_regime(now),
        "active_state_count": len(v32_asset_state),
        "active_states": active,
        "learner": v31_state.get("learner", {}),
    }

