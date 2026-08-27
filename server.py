import asyncio
import json
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

WINDOWS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "10m": 600,
    "30m": 1800,
}

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

app = FastAPI(title="Pump Hunter Server", version="2.8.2")

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
VOLUME_RATIO_CAP = 5.0
VOLUME_LOW_BASELINE_FRACTION = 0.10
MIN_PRICE_MOVE_FOR_VOLUME_BONUS_PCT = 0.35

PUMP_MIN_QUALITY_SCORE = 45
PUMP_EMERGENCY_1M_PCT = 5.0
PUMP_EMERGENCY_MIN_3M_PCT = 0.0
PUMP_EMERGENCY_MIN_5M_PCT = -0.5
REENTRY_MIN_QUALITY_SCORE = 65
REENTRY_CONFIRM_SECONDS = 20
REENTRY_MAX_EXIT_AGE_SECONDS = 20 * 60
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



def detect_signal(asset: str, source: str, source_symbol: str, price: float,
                  moves: Dict[str, Optional[float]], now: float) -> None:
    m1, m3, m5, m10, m30 = [moves.get(k) for k in ("1m", "3m", "5m", "10m", "30m")]

    early_raw = (
        (m1 is not None and m1 >= 1.5)
        or (m3 is not None and m3 >= 2.0)
        or (m5 is not None and m5 >= 3.0)
    )
    pump_raw = (
        (m1 is not None and m1 >= 3.0)
        or (m3 is not None and m3 >= 4.0)
        or (m5 is not None and m5 >= 4.5)
        or (m10 is not None and m10 >= 5.0)
        or (m30 is not None and m30 >= 5.0)
    )

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
        quality_score >= 45
        and not reversal_bounce
        and (
            bool(quality["trend_aligned"])
            or (m1 is not None and m1 >= 2.5 and (m3 is None or m3 > -0.75))
        )
    )
    emergency_pump_ok = (
        m1 is not None
        and m1 >= PUMP_EMERGENCY_1M_PCT
        and (m3 is None or m3 >= PUMP_EMERGENCY_MIN_3M_PCT)
        and (m5 is None or m5 >= PUMP_EMERGENCY_MIN_5M_PCT)
        and not reversal_bounce
        and not fading
    )

    pump_quality_ok = (
        quality_score >= PUMP_MIN_QUALITY_SCORE
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
            # Strong recovery required to re-enter PUMP.
            recovered = (
                pump_raw
                and pump_quality_ok
                and accelerating
                and not fading
                and not reversal_bounce
                and drawdown > -2.0
                and momentum_score >= 45
            )
            if recovered:
                desired = "PUMP"
                required_confirm = COOLING_TO_PUMP_CONFIRM_SECONDS
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
            and pump_raw
            and accelerating
            and not fading
            and not reversal_bounce
            and quality_score >= REENTRY_MIN_QUALITY_SCORE
            and (m1 is not None and m1 >= 1.5)
            and (m3 is None or m3 > 0.0)
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

    signals.appendleft({
        "type": "RE_ENTRY" if previous == "EXIT" and desired == "PUMP" else desired,
        "engine_state": desired,
        "previous_state": previous,
        "asset": asset,
        "asset_name": asset_display_name(asset),
        "source": source,
        "source_name": source_display_name(source),
        "source_symbol": source_symbol,
        "price": price,
        "signal_price": price,
        "pump_score": score,
        "momentum_score": momentum_score,
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
        "moves": moves,
        "cycle_started_at": int(float(st["started_at"])) if st["started_at"] else None,
        "peak_price": st["peak_price"],
        "peak_score": st["peak_score"],
        "timestamp": int(now),
    })

def store_price(asset: str, source: str, source_symbol: str, price: float) -> None:
    if price <= 0:
        return

    now = time.time()

    # Real live websocket tick.
    live_last_event[asset] = now

    latest_prices[asset] = price
    latest_sources[asset] = source

    history = price_history.setdefault(asset, deque(maxlen=8000))

    if now - last_sample_time.get(asset, 0.0) >= SAMPLE_EVERY_SECONDS:
        history.append((now, price))
        last_sample_time[asset] = now

        cutoff = now - HISTORY_SECONDS
        while history and history[0][0] < cutoff:
            history.popleft()

    moves = calculate_moves(asset, now, price)

    # Signal Engine only reacts to live ticks, using the backfilled history
    # as context for the 1m/3m/5m/10m/30m windows.
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
            "User-Agent": "PumpHunterServer/2.8.2",
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
        headers={"Accept": "application/json", "User-Agent": "PumpHunterServer/2.8.2"},
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
            headers={"Accept": "application/json", "User-Agent": "PumpHunterServer/2.8.2"},
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


@app.get("/")
def root() -> Dict[str, object]:
    return {
        "name": "Pump Hunter Server",
        "version": "2.8.2",
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
        rows.append({
            "asset": asset,
            "asset_name": asset_display_name(asset),
            "source": latest_sources.get(asset),
            "source_name": source_display_name(latest_sources.get(asset)),
            "price": price,
            "state": signal_state.get(asset, {}).get("state", "NORMAL"),
            "quality_score": qm["quality_score"],
            "quality": qm["quality"],
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
    return {
        "states": counts,
        "active_count": len(active),
        "active": active,
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
        "bullish_impulse_pct": current_quality["bullish_impulse_pct"],
        "volume_ratio": current_quality["volume_ratio"],
        "volume_ratio_raw": current_quality["volume_ratio_raw"],
        "volume_confirmed": current_quality["volume_confirmed"],
        "volume_low_baseline": current_quality["volume_low_baseline"],
        "volume_bonus_applied": current_quality["volume_bonus_applied"],
        "signal_state": signal_state.get(symbol, {}).get("state", "NORMAL"),
        "signal_cycle": signal_state.get(symbol),
        "tracked": symbol in price_history,
    }
