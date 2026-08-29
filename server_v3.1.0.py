"""
Pump Hunter Server 3.1.0
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

VERSION = "3.1.0"

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
CONTEXT_WARMUP_MIN_AGE_SECONDS = 20 * 60 * 60
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
    for sig in list(base.signals)[:100]:
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

    if source == "BINANCE":
        r = requests.get(
            "https://data-api.binance.vision/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "15m",
                "startTime": start * 1000,
                "limit": 300,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        rows = [(int(x[0]) // 1000, float(x[4])) for x in r.json()]

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
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/candlesticks",
            params={
                "currency_pair": symbol,
                "interval": "15m",
                "from": start,
                "to": now,
                "limit": 300,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        # Gate spot candle: [timestamp, quote_volume, close, high, low, open, ...]
        rows = [(int(float(x[0])), float(x[2])) for x in r.json()]

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
        existing = {int(float(x[0])) for x in dq}
        merged = list(dq)
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

    if dd72 <= -10.0 and dscore < base.DYNAMIC_FOCUS_SCORE:
        stage = "DISTRIBUTION"
        reasons.append("BELOW_72H_PEAK_AND_MOMENTUM_WEAK")
    elif hard_extended:
        stage = "LATE_EXTENSION"
        reasons.append("MULTI_HOUR_HARD_EXTENSION")
    elif dd72 <= -4.0 and dscore < base.DYNAMIC_HOT_SCORE:
        stage = "PULLBACK"
        reasons.append("PULLBACK_FROM_72H_PEAK")
    elif dd72 <= -2.0 and dscore >= base.DYNAMIC_HOT_SCORE and velocity > 0:
        stage = "SECOND_WAVE"
        reasons.append("REACCELERATION_BELOW_PRIOR_PEAK")
        if buy_pressure:
            reasons.append("BUY_FLOW_SUPPORT")
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
def root_v31():
    return {
        "name": "Pump Hunter Server",
        "version": VERSION,
        "status": "running",
        "base": "3.0.0",
        "engine": "hybrid-v31",
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
        "quality_model": "COMPOSITE_ANTI_SATURATION",
        "reentry_model": "STRICT_SECOND_WAVE",
        "learner_mode": "SHADOW",
        "tracked_assets": tracked,
        "workers": v31_state,
        "assets": context_rows,
    }


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
