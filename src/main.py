"""KuCoin Market Collector — actor Apify privato.

Raccoglie solo dati PUBBLICI di KuCoin Futures (nessuna API key necessaria):
- mode "snapshot": fotografia di tutti i perpetual attivi (prezzi, funding,
  open interest, volumi) + rilevamento nuovi listing. Da schedulare ogni 15 min.
- mode "backfill": storico candele + storico funding dei contratti listati
  negli ultimi N giorni, per testare subito l'ipotesi "nuovi listing".
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import httpx
from apify import Actor

FUTURES_BASE = "https://api-futures.kucoin.com"

SNAPSHOT_DATASET = "kucoin-futures-snapshots"
LISTINGS_DATASET = "kucoin-new-listings"
BACKFILL_DATASET = "kucoin-listing-backfill"
STATE_STORE = "kucoin-collector-state"

# Campi del contratto che salviamo in ogni snapshot (il resto è rumore).
CONTRACT_FIELDS = [
    "symbol", "baseCurrency", "quoteCurrency", "settleCurrency", "isInverse",
    "status", "firstOpenDate", "multiplier", "maxLeverage", "tickSize",
    "markPrice", "indexPrice", "lastTradePrice",
    "fundingFeeRate", "predictedFundingFeeRate", "nextFundingRateTime",
    "openInterest", "volumeOf24h", "turnoverOf24h", "priceChgPct",
    "highPrice", "lowPrice",
]


def now_ms() -> int:
    return int(time.time() * 1000)


def iso(ms: int | None) -> str | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


# Il endpoint del funding ha un limite più stretto: tutte le richieste del run
# passano da qui, distanziate di almeno THROTTLE[path] secondi.
THROTTLE = {"/api/v1/contract/funding-rates": 0.35, "/api/v1/kline/query": 0.08}
_throttle_lock: dict[str, asyncio.Lock] = {}
_throttle_last: dict[str, float] = {}


async def _wait_turn(path: str) -> None:
    gap = THROTTLE.get(path)
    if not gap:
        return
    lock = _throttle_lock.setdefault(path, asyncio.Lock())
    async with lock:
        wait = _throttle_last.get(path, 0) + gap - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        _throttle_last[path] = time.monotonic()


async def get_json(client: httpx.AsyncClient, path: str, params: dict | None = None, retries: int = 8):
    """GET con retry su 429/5xx ed errori di rete. Ritorna il campo `data`.
    Sui 429 rispetta l'header gw-ratelimit-reset di KuCoin (ms al reset)."""
    delay = 1.0
    for attempt in range(1, retries + 1):
        await _wait_turn(path)
        wait = delay
        try:
            r = await client.get(FUTURES_BASE + path, params=params)
            if r.status_code == 429:
                try:
                    wait = max(delay, int(r.headers.get("gw-ratelimit-reset", 0)) / 1000 + 0.5)
                except ValueError:
                    pass
                raise httpx.HTTPStatusError("HTTP 429", request=r.request, response=r)
            if r.status_code >= 500:
                raise httpx.HTTPStatusError(f"HTTP {r.status_code}", request=r.request, response=r)
            r.raise_for_status()
            body = r.json()
            if str(body.get("code")) == "429000":
                raise RuntimeError("KuCoin rate limit (429000)")
            if str(body.get("code")) != "200000":
                raise RuntimeError(f"KuCoin error {body.get('code')}: {body.get('msg')}")
            return body.get("data")
        except (httpx.HTTPError, RuntimeError) as e:
            if attempt == retries:
                raise
            if attempt >= 3:  # i primi tentativi falliti sono normali, non sporcare il log
                Actor.log.warning(f"{path} tentativo {attempt} fallito ({e}), riprovo tra {wait:.0f}s")
            await asyncio.sleep(wait)
            delay = min(delay * 2, 30)


async def fetch_contracts(client: httpx.AsyncClient, usdt_only: bool) -> list[dict]:
    data = await get_json(client, "/api/v1/contracts/active") or []
    contracts = [c for c in data if c.get("status", "Open") == "Open"]
    if usdt_only:
        contracts = [c for c in contracts if c.get("quoteCurrency") == "USDT" and not c.get("isInverse")]
    return contracts


# ---------------------------------------------------------------- snapshot

async def run_snapshot(client: httpx.AsyncClient, inp: dict) -> None:
    ts = now_ms()
    contracts = await fetch_contracts(client, inp.get("usdtOnly", True))
    Actor.log.info(f"Contratti attivi: {len(contracts)}")

    rows = [{k: c.get(k) for k in CONTRACT_FIELDS} for c in contracts]

    # Un solo item per run con tutti i contratti dentro: 1 scrittura invece
    # di ~500, così il costo di storage su Apify resta trascurabile.
    snapshots = await Actor.open_dataset(name=SNAPSHOT_DATASET)
    await snapshots.push_data({
        "ts": ts,
        "tsIso": iso(ts),
        "count": len(rows),
        "contracts": rows,
    })

    # --- rilevamento nuovi listing
    state = await Actor.open_key_value_store(name=STATE_STORE)
    known: dict = await state.get_value("known_symbols") or {}
    first_run = not known
    lookback_ms = inp.get("newListingLookbackHours", 48) * 3600 * 1000

    new_items = []
    for c in contracts:
        sym = c["symbol"]
        if sym in known:
            continue
        known[sym] = c.get("firstOpenDate") or ts
        first_open = c.get("firstOpenDate") or 0
        # Al primo avvio segnaliamo solo i contratti davvero recenti.
        if first_run and ts - first_open > lookback_ms:
            continue
        new_items.append({
            "detectedAt": ts,
            "detectedAtIso": iso(ts),
            "symbol": sym,
            "firstOpenDate": first_open,
            "firstOpenDateIso": iso(first_open),
            "minutesSinceOpen": round((ts - first_open) / 60000, 1) if first_open else None,
            **{k: c.get(k) for k in CONTRACT_FIELDS if k != "symbol"},
        })

    if new_items:
        listings = await Actor.open_dataset(name=LISTINGS_DATASET)
        await listings.push_data(new_items)
        Actor.log.info(f"Nuovi listing rilevati: {[i['symbol'] for i in new_items]}")

    await state.set_value("known_symbols", known)
    await Actor.set_value("OUTPUT", {
        "mode": "snapshot", "ts": iso(ts), "contracts": len(rows),
        "newListings": [i["symbol"] for i in new_items], "firstRun": first_run,
    })


# ---------------------------------------------------------------- backfill

async def fetch_klines(client: httpx.AsyncClient, symbol: str, granularity: int, start: int, end: int) -> list[list]:
    """Candele [time, open, high, low, close, volume] paginando a finestre."""
    step = granularity * 60 * 1000 * 200  # 200 candele per richiesta
    out: dict[int, list] = {}
    t = start
    while t < end:
        t2 = min(t + step, end)
        data = await get_json(client, "/api/v1/kline/query", {
            "symbol": symbol, "granularity": granularity, "from": t, "to": t2,
        }) or []
        for row in data:
            out[int(row[0])] = row
        t = t2
        await asyncio.sleep(0.15)
    return [out[k] for k in sorted(out)]


FUNDING_PAGE_CAP = 100  # KuCoin restituisce al massimo 100 righe per richiesta


async def _funding_window(client: httpx.AsyncClient, symbol: str, t: int, t2: int,
                          out: dict[int, dict], depth: int = 0) -> None:
    data = await get_json(client, "/api/v1/contract/funding-rates", {
        "symbol": symbol, "from": t, "to": t2,
    }) or []
    # Finestra piena = risultati probabilmente troncati (contratti con funding
    # ogni 1-4h): la dividiamo a metà e richiediamo le due parti.
    if len(data) >= FUNDING_PAGE_CAP and t2 - t > 2 * 3600 * 1000 and depth < 12:
        mid = (t + t2) // 2
        await _funding_window(client, symbol, t, mid, out, depth + 1)
        await _funding_window(client, symbol, mid, t2, out, depth + 1)
        return
    for row in data:
        tp = int(row.get("timepoint") or row.get("timePoint") or 0)
        out[tp] = {"timepoint": tp, "fundingRate": row.get("fundingRate")}


async def fetch_funding_history(client: httpx.AsyncClient, symbol: str, start: int, end: int) -> list[dict]:
    window = 30 * 24 * 3600 * 1000  # 90 righe a funding 8h; le finestre piene vengono divise
    out: dict[int, dict] = {}
    t = start
    while t < end:
        t2 = min(t + window, end)
        try:
            await _funding_window(client, symbol, t, t2, out)
        except Exception as e:  # endpoint non critico: non bloccare il backfill
            Actor.log.warning(f"Funding history {symbol} incompleta: {e}")
            break
        t = t2
    return [out[k] for k in sorted(out)]


DAY_MS = 24 * 3600 * 1000
REFERENCE_SYMBOL = "XBTUSDTM"


async def push_backfill_item(ds, item: dict) -> None:
    # Named dataset = archivio; default dataset = tab "Output" del run,
    # da cui si esporta direttamente il risultato di questo backfill.
    await ds.push_data(item)
    await Actor.push_data(item)


async def run_backfill(client: httpx.AsyncClient, inp: dict) -> None:
    window_days = inp.get("eventWindowDays", 0)
    if window_days and window_days > 0:
        await run_event_backfill(client, inp)
    else:
        await run_full_backfill(client, inp)


async def run_event_backfill(client: httpx.AsyncClient, inp: dict) -> None:
    """Per ogni listing salva solo la finestra evento: dettaglio fine nei primi
    giorni + candele più larghe fino a fine finestra. Più BTC come riferimento
    per segmentare per regime di mercato."""
    ts = now_ms()
    days = inp.get("backfillDays", 730)
    window_days = inp.get("eventWindowDays", 30)
    fine_g = inp.get("fineGranularityMinutes", 15)
    fine_days = inp.get("fineWindowDays", 7)
    coarse_g = inp.get("coarseGranularityMinutes", 60)
    max_symbols = inp.get("maxBackfillSymbols", 1000)
    cutoff = ts - days * DAY_MS

    contracts = await fetch_contracts(client, inp.get("usdtOnly", True))
    recent = sorted(
        (c for c in contracts if (c.get("firstOpenDate") or 0) >= cutoff),
        key=lambda c: c["firstOpenDate"],
        reverse=True,
    )[:max_symbols]
    Actor.log.info(f"Contratti listati negli ultimi {days} giorni: {len(recent)} "
                   f"(finestra {window_days}g, {fine_g}m per {fine_days}g, poi {coarse_g}m)")

    ds = await Actor.open_dataset(name=BACKFILL_DATASET)

    if inp.get("includeBtcReference", True):
        start = cutoff - DAY_MS  # copre anche il contesto del listing più vecchio
        btc = await fetch_klines(client, REFERENCE_SYMBOL, coarse_g, start, ts)
        await push_backfill_item(ds, {
            "symbol": REFERENCE_SYMBOL, "reference": True,
            "granularityMinutes": coarse_g, "candles": btc, "collectedAt": iso(ts),
        })
        Actor.log.info(f"Riferimento {REFERENCE_SYMBOL}: {len(btc)} candele")

    sem = asyncio.Semaphore(max(1, inp.get("concurrency", 4)))
    done = 0

    async def process(c: dict) -> None:
        nonlocal done
        sym, first_open = c["symbol"], c["firstOpenDate"]
        fine_end = min(first_open + fine_days * DAY_MS, ts)
        window_end = min(first_open + window_days * DAY_MS, ts)
        async with sem:
            try:
                fine = await fetch_klines(client, sym, fine_g, first_open, fine_end)
                coarse = await fetch_klines(client, sym, coarse_g, first_open, window_end)
                funding = await fetch_funding_history(client, sym, first_open, window_end)
            except Exception as e:
                Actor.log.warning(f"{sym}: backfill fallito ({e}), salto")
                return
        await push_backfill_item(ds, {
            "symbol": sym,
            "reference": False,
            "baseCurrency": c.get("baseCurrency"),
            "firstOpenDate": first_open,
            "firstOpenDateIso": iso(first_open),
            "windowComplete": window_end - first_open >= window_days * DAY_MS,
            "maxLeverage": c.get("maxLeverage"),
            "multiplier": c.get("multiplier"),
            "turnoverOf24h": c.get("turnoverOf24h"),
            "fineGranularityMinutes": fine_g,
            "candlesFine": fine,          # [time, open, high, low, close, volume]
            "coarseGranularityMinutes": coarse_g,
            "candles": coarse,
            "funding": funding,
            "collectedAt": iso(ts),
        })
        done += 1
        Actor.log.info(f"[{done}/{len(recent)}] {sym}: {len(fine)} fini, {len(coarse)} orarie, {len(funding)} funding")

    await asyncio.gather(*(process(c) for c in recent))

    await Actor.set_value("OUTPUT", {"mode": "backfill", "symbols": done, "days": days,
                                     "eventWindowDays": window_days})


async def run_full_backfill(client: httpx.AsyncClient, inp: dict) -> None:
    ts = now_ms()
    days = inp.get("backfillDays", 120)
    granularity = inp.get("klineGranularityMinutes", 60)
    max_symbols = inp.get("maxBackfillSymbols", 300)
    cutoff = ts - days * 24 * 3600 * 1000

    contracts = await fetch_contracts(client, inp.get("usdtOnly", True))
    recent = sorted(
        (c for c in contracts if (c.get("firstOpenDate") or 0) >= cutoff),
        key=lambda c: c["firstOpenDate"],
        reverse=True,
    )[:max_symbols]
    Actor.log.info(f"Contratti listati negli ultimi {days} giorni: {len(recent)}")

    ds = await Actor.open_dataset(name=BACKFILL_DATASET)
    done = 0
    for c in recent:
        sym, first_open = c["symbol"], c["firstOpenDate"]
        try:
            candles = await fetch_klines(client, sym, granularity, first_open, ts)
            funding = await fetch_funding_history(client, sym, first_open, ts)
        except Exception as e:
            Actor.log.warning(f"{sym}: backfill fallito ({e}), salto")
            continue
        item = {
            "symbol": sym,
            "baseCurrency": c.get("baseCurrency"),
            "firstOpenDate": first_open,
            "firstOpenDateIso": iso(first_open),
            "granularityMinutes": granularity,
            "candles": candles,          # [time, open, high, low, close, volume]
            "funding": funding,
            "collectedAt": iso(ts),
        }
        await push_backfill_item(ds, item)
        done += 1
        Actor.log.info(f"[{done}/{len(recent)}] {sym}: {len(candles)} candele, {len(funding)} funding")

    await Actor.set_value("OUTPUT", {"mode": "backfill", "symbols": done, "days": days})


# ---------------------------------------------------------------- history

HISTORY_DATASET = "kucoin-history"
DEFAULT_HISTORY_SYMBOLS = [
    "XBTUSDTM", "ETHUSDTM", "SOLUSDTM", "BNBUSDTM", "XRPUSDTM", "DOGEUSDTM", "ADAUSDTM",
    "AVAXUSDTM", "LINKUSDTM", "DOTUSDTM", "LTCUSDTM", "TRXUSDTM", "BCHUSDTM", "NEARUSDTM",
    "ATOMUSDTM", "UNIUSDTM", "ETCUSDTM", "FILUSDTM", "APTUSDTM", "ARBUSDTM", "OPUSDTM",
    "SUIUSDTM", "INJUSDTM", "TONUSDTM",
]


async def run_history(client: httpx.AsyncClient, inp: dict) -> None:
    """Storico lungo di una lista di perpetual (per strategie trend following)."""
    ts = now_ms()
    days = inp.get("historyDays", 1825)
    granularity = inp.get("historyGranularityMinutes", 240)
    wanted = inp.get("historySymbols") or DEFAULT_HISTORY_SYMBOLS
    start_all = ts - days * DAY_MS

    active = {c["symbol"]: c for c in await fetch_contracts(client, usdt_only=True)}
    missing = [s for s in wanted if s not in active]
    if missing:
        Actor.log.warning(f"Non attivi su KuCoin, salto: {missing}")
    symbols = [s for s in wanted if s in active]

    ds = await Actor.open_dataset(name=HISTORY_DATASET)
    sem = asyncio.Semaphore(max(1, inp.get("concurrency", 4)))
    done = 0

    async def process(sym: str) -> None:
        nonlocal done
        c = active[sym]
        start = max(start_all, c.get("firstOpenDate") or start_all)
        async with sem:
            try:
                candles = await fetch_klines(client, sym, granularity, start, ts)
                funding = await fetch_funding_history(client, sym, start, ts)
            except Exception as e:
                Actor.log.warning(f"{sym}: storico non scaricato ({e})")
                return
        item = {"symbol": sym, "baseCurrency": c.get("baseCurrency"), "firstOpenDate": c.get("firstOpenDate"),
                "granularityMinutes": granularity, "candles": candles, "funding": funding,
                "collectedAt": iso(ts)}
        await ds.push_data(item)
        await Actor.push_data(item)
        done += 1
        first = iso(candles[0][0]) if candles else None
        Actor.log.info(f"[{done}/{len(symbols)}] {sym}: {len(candles)} candele da {first}, {len(funding)} funding")

    await asyncio.gather(*(process(s) for s in symbols))
    await Actor.set_value("OUTPUT", {"mode": "history", "symbols": done, "missing": missing})


# ---------------------------------------------------------------- entry

async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}
        mode = inp.get("mode", "snapshot")

        proxy_url = None
        if inp.get("proxyConfiguration", {}).get("useApifyProxy"):
            proxy_cfg = await Actor.create_proxy_configuration(actor_proxy_input=inp["proxyConfiguration"])
            proxy_url = await proxy_cfg.new_url() if proxy_cfg else None

        async with httpx.AsyncClient(timeout=30, proxy=proxy_url,
                                     headers={"User-Agent": "kucoin-market-collector/1.0"}) as client:
            if mode == "backfill":
                await run_backfill(client, inp)
            elif mode == "history":
                await run_history(client, inp)
            elif mode == "unlocks":
                from .unlocks import run_unlocks
                await run_unlocks(client, inp, fetch_contracts, fetch_klines,
                                  fetch_funding_history, iso, now_ms)
            else:
                await run_snapshot(client, inp)
