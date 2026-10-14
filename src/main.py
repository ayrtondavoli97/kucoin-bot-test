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


async def get_json(client: httpx.AsyncClient, path: str, params: dict | None = None, retries: int = 5):
    """GET con retry su 429/5xx ed errori di rete. Ritorna il campo `data`."""
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            r = await client.get(FUTURES_BASE + path, params=params)
            if r.status_code == 429 or r.status_code >= 500:
                raise httpx.HTTPStatusError(f"HTTP {r.status_code}", request=r.request, response=r)
            r.raise_for_status()
            body = r.json()
            if str(body.get("code")) != "200000":
                raise RuntimeError(f"KuCoin error {body.get('code')}: {body.get('msg')}")
            return body.get("data")
        except (httpx.HTTPError, RuntimeError) as e:
            if attempt == retries:
                raise
            Actor.log.warning(f"{path} tentativo {attempt} fallito ({e}), riprovo tra {delay:.0f}s")
            await asyncio.sleep(delay)
            delay *= 2


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


async def fetch_funding_history(client: httpx.AsyncClient, symbol: str, start: int, end: int) -> list[dict]:
    window = 30 * 24 * 3600 * 1000
    out: dict[int, dict] = {}
    t = start
    while t < end:
        t2 = min(t + window, end)
        try:
            data = await get_json(client, "/api/v1/contract/funding-rates", {
                "symbol": symbol, "from": t, "to": t2,
            }) or []
        except Exception as e:  # endpoint non critico: non bloccare il backfill
            Actor.log.warning(f"Funding history {symbol} non disponibile: {e}")
            return [out[k] for k in sorted(out)]
        for row in data:
            tp = int(row.get("timepoint") or row.get("timePoint") or 0)
            out[tp] = {"timepoint": tp, "fundingRate": row.get("fundingRate")}
        t = t2
        await asyncio.sleep(0.15)
    return [out[k] for k in sorted(out)]


async def run_backfill(client: httpx.AsyncClient, inp: dict) -> None:
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
        await ds.push_data({
            "symbol": sym,
            "baseCurrency": c.get("baseCurrency"),
            "firstOpenDate": first_open,
            "firstOpenDateIso": iso(first_open),
            "granularityMinutes": granularity,
            "candles": candles,          # [time, open, high, low, close, volume]
            "funding": funding,
            "collectedAt": iso(ts),
        })
        done += 1
        Actor.log.info(f"[{done}/{len(recent)}] {sym}: {len(candles)} candele, {len(funding)} funding")

    await Actor.set_value("OUTPUT", {"mode": "backfill", "symbols": done, "days": days})


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
            else:
                await run_snapshot(client, inp)
