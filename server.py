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

app = FastAPI(title="Pump Hunter Server", version="2.6.2")

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
            "asset_name": asset_display_name(asset),
            "source": source,
            "source_name": source_display_name(source),
            "source_symbol": source_symbol,
            "price": price,
            "signal_price": price,
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
            "User-Agent": "PumpHunterServer/2.5",
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
        headers={"Accept": "application/json", "User-Agent": "PumpHunterServer/2.6.2"},
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
        latest_source_symbols[asset] = source_symbol
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
            headers={"Accept": "application/json", "User-Agent": "PumpHunterServer/2.6.2"},
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

                    state["kucoin_last_event"] = int(time.time())

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
                            state["coinbase_last_event"] = int(time.time())
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
                        state["kraken_last_event"] = int(time.time())
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
        "version": "2.6.2",
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

    return {
        "asset": symbol,
        "asset_name": asset_display_name(symbol),
        "price": price,
        "source": source,
        "source_name": source_display_name(source),
        "moves": moves,
        "pump_score": pump_score(moves),
        "tracked": symbol in price_history,
    }
