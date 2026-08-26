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

WHITELIST_REFRESH_SECONDS = 6 * 60 * 60
HISTORY_SECONDS = 31 * 60
SAMPLE_EVERY_SECONDS = 2.0

WINDOWS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "10m": 600,
    "30m": 1800,
}

QUOTE_PRIORITY = ["USDT", "USDC", "FDUSD", "BTC", "ETH"]

app = FastAPI(title="Pump Hunter Server", version="2.4")

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
    "total_fast_feed_assets": 0,
    "unmatched_assets": [],
}

price_history: Dict[str, deque] = {}
last_sample_time: Dict[str, float] = {}
latest_prices: Dict[str, float] = {}
latest_sources: Dict[str, str] = {}
signals: deque = deque(maxlen=300)
last_early_alert: Dict[str, float] = {}
last_pump_alert: Dict[str, float] = {}

binance_symbol_to_asset: Dict[str, str] = {}
binance_streams: List[str] = []
bybit_symbol_to_asset: Dict[str, str] = {}
bybit_topics: List[str] = []
gate_symbol_to_asset: Dict[str, str] = {}
gate_pairs: List[str] = []
okx_symbol_to_asset: Dict[str, str] = {}
okx_args: List[dict] = []


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


def detect_signal(
    asset: str,
    source: str,
    source_symbol: str,
    price: float,
    moves: Dict[str, Optional[float]],
    now: float,
) -> None:
    m1 = moves.get("1m")
    m3 = moves.get("3m")
    m5 = moves.get("5m")
    m10 = moves.get("10m")
    m30 = moves.get("30m")

    early = (
        (m1 is not None and m1 >= 1.5)
        or (m3 is not None and m3 >= 2.0)
        or (m5 is not None and m5 >= 3.0)
    )

    pump = (
        (m1 is not None and m1 >= 3.0)
        or (m3 is not None and m3 >= 4.0)
        or (m5 is not None and m5 >= 4.5)
        or (m10 is not None and m10 >= 5.0)
        or (m30 is not None and m30 >= 5.0)
    )

    score = pump_score(moves)

    if pump:
        if now - last_pump_alert.get(asset, 0) < 900:
            return
        last_pump_alert[asset] = now
        kind = "PUMP"
    elif early:
        if now - last_early_alert.get(asset, 0) < 300:
            return
        last_early_alert[asset] = now
        kind = "EARLY_MOVE"
    else:
        return

    signals.appendleft(
        {
            "type": kind,
            "asset": asset,
            "source": source,
            "source_symbol": source_symbol,
            "price": price,
            "pump_score": score,
            "moves": moves,
            "timestamp": int(now),
        }
    )


def store_price(asset: str, source: str, source_symbol: str, price: float) -> None:
    if price <= 0:
        return

    now = time.time()
    latest_prices[asset] = price
    latest_sources[asset] = source

    history = price_history.setdefault(asset, deque(maxlen=2000))

    if now - last_sample_time.get(asset, 0.0) >= SAMPLE_EVERY_SECONDS:
        history.append((now, price))
        last_sample_time[asset] = now
        cutoff = now - HISTORY_SECONDS
        while history and history[0][0] < cutoff:
            history.popleft()

    moves = calculate_moves(asset, now, price)
    detect_signal(asset, source, source_symbol, price, moves, now)


def refresh_revolut_whitelist() -> None:
    response = requests.get(
        REVOLUT_TICKERS_URL,
        timeout=20,
        headers={
            "Accept": "application/json",
            "User-Agent": "PumpHunterServer/2.4",
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

        if base not in whitelist or quote not in QUOTE_PRIORITY:
            continue

        candidates.setdefault(base, {})[quote] = symbol

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

        if base not in missing or quote not in QUOTE_PRIORITY:
            continue

        candidates.setdefault(base, {})[quote] = symbol

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
        if base not in missing or quote not in QUOTE_PRIORITY:
            continue
        candidates.setdefault(base, {})[quote] = pair_id
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

                    state["binance_last_event"] = int(time.time())

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

                    state["bybit_last_event"] = int(time.time())

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
                    state["gate_last_event"] = int(time.time())
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

                    state["okx_last_event"] = int(time.time())

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


@app.on_event("startup")
async def startup_event() -> None:
    try:
        refresh_revolut_whitelist()
    except Exception as exc:
        state["revolut_ok"] = False
        state["revolut_last_error"] = str(exc)

    await rebuild_mappings()

    asyncio.create_task(whitelist_refresh_loop())
    asyncio.create_task(binance_fast_feed_loop())
    asyncio.create_task(bybit_fast_feed_loop())
    asyncio.create_task(gate_fast_feed_loop())
    asyncio.create_task(okx_fast_feed_loop())


@app.get("/")
def root() -> Dict[str, object]:
    return {
        "name": "Pump Hunter Server",
        "version": "2.4",
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
        "total_fast_feed_assets": state["total_fast_feed_assets"],
        "unmatched_count": len(state["unmatched_assets"]),
        "unmatched_assets": state["unmatched_assets"],
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
    return {
        "count": len(signals),
        "signals": list(signals)[:safe_limit],
    }


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

    return {
        "asset": symbol,
        "price": price,
        "source": source,
        "moves": moves,
        "pump_score": pump_score(moves),
        "tracked": symbol in price_history,
    }
