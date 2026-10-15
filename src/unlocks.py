"""Modalità "unlocks": eventi di sblocco token (DefiLlama, dataset pubblici) incrociati
con i perpetual KuCoin, più candele orarie e funding attorno a ogni evento.

Output:
- dataset `kucoin-unlock-events`: un item per sblocco passato con prezzi attorno
  (anche nel dataset del run, per esportarlo dalla tab Output);
- dataset `kucoin-upcoming-unlocks`: sblocchi futuri sui token con perpetual KuCoin
  (servirà al bot per sapere cosa arriva);
- record `UNLOCKS_REPORT` nel key-value store del run: conteggi e protocolli scartati.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict

import httpx
from apify import Actor

DATASETS = "https://defillama-datasets.llama.fi"
COINGECKO_LIST = "https://api.coingecko.com/api/v3/coins/list"

EVENTS_DATASET = "kucoin-unlock-events"
UPCOMING_DATASET = "kucoin-upcoming-unlocks"

H = 3600 * 1000
DAY = 24 * H

# Categorie standard di DefiLlama -> gruppo
CATEGORY_GROUP = {
    "insiders": "insiders",
    "privatesale": "investors",
    "publicsale": "public",
    "noncirculating": "ecosystem",
    "airdrop": "community",
    "farming": "community",
    "liquidity": "liquidity",
}
# Parole chiave nell'etichetta dell'allocazione -> gruppo (più specifico)
LABEL_KEYWORDS = [
    ("team", r"team|core contributor|contributor|founder|employee|advisor|staff|developer"),
    ("investors", r"investor|backer|seed|private|strategic|series|venture|vc\b|early support|pre-?sale"),
    ("ecosystem", r"ecosystem|treasury|foundation|reserve|dao|grant|development fund|protocol develop"),
    ("community", r"community|airdrop|reward|incentive|staking|mining|farming|user|retro|launchpool"),
    ("public", r"public|ico|ido|sale"),
    ("liquidity", r"liquidity|market making|exchange"),
]


def label_group(label: str, cat_of_label: dict[str, str]) -> str:
    low = label.lower()
    for group, pattern in LABEL_KEYWORDS:
        if re.search(pattern, low):
            return group
    cat = cat_of_label.get(label)
    if cat:
        return CATEGORY_GROUP.get(cat.lower().replace("_", ""), "other")
    return "other"


async def get_any(client: httpx.AsyncClient, url: str, headers: dict | None = None, retries: int = 4):
    delay = 2.0
    for attempt in range(1, retries + 1):
        try:
            r = await client.get(url, headers=headers)
            if r.status_code == 404:
                return None
            if r.status_code == 429 or r.status_code >= 500:
                raise httpx.HTTPStatusError(f"HTTP {r.status_code}", request=r.request, response=r)
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPError, ValueError) as e:
            if attempt == retries:
                Actor.log.warning(f"{url}: {e}")
                return None
            await asyncio.sleep(delay)
            delay *= 2


def find_series(data: dict) -> list[dict]:
    for key in ("documentedData", "realTimeData"):
        block = data.get(key)
        if isinstance(block, dict) and isinstance(block.get("data"), list) and block["data"]:
            return block["data"]
    if isinstance(data.get("data"), list):
        return data["data"]
    return []


def find_categories(data: dict) -> dict[str, str]:
    """{etichetta: categoria} se il file contiene la mappa delle categorie."""
    out = {}
    for block in (data, data.get("documentedData") or {}, data.get("metadata") or {}):
        cats = block.get("categories") if isinstance(block, dict) else None
        if isinstance(cats, dict):
            for cat, labels in cats.items():
                for lab in labels or []:
                    out[str(lab)] = str(cat)
    return out


def find_gecko_id(data: dict, slug: str) -> tuple[str, bool]:
    meta = data.get("metadata") or {}
    for v in (meta.get("token"), data.get("token"), data.get("gecko_id"), meta.get("gecko_id")):
        if isinstance(v, str) and v:
            return v.split(":", 1)[-1], True
    return slug, False


def extract_events(series: list[dict], cat_of_label: dict[str, str], min_pct: float,
                   merge_days: int) -> list[dict]:
    """Eventi = giorni in cui il totale sbloccato cresce di almeno min_pct
    rispetto al circolante del giorno prima. Giorni vicini vengono uniti."""
    per_label: dict[str, dict[int, float]] = {}
    all_ts: set[int] = set()
    for s in series:
        label = str(s.get("label", "?"))
        pts = {}
        for p in s.get("data") or []:
            try:
                pts[int(p["timestamp"])] = float(p.get("unlocked") or 0)
            except (KeyError, TypeError, ValueError):
                continue
        if pts:
            per_label[label] = pts
            all_ts.update(pts)
    if not per_label:
        return []
    ts_sorted = sorted(all_ts)
    last: dict[str, float] = defaultdict(float)
    prev_total = 0.0
    raw = []
    for t in ts_sorted:
        incs = {}
        for label, pts in per_label.items():
            if t in pts:
                inc = pts[t] - last[label]
                last[label] = pts[t]
                if inc > 0:
                    incs[label] = inc
        total = sum(last.values())
        inc_total = sum(incs.values())
        if prev_total > 0 and inc_total >= min_pct * prev_total:
            raw.append({"t": t * 1000, "amount": inc_total, "circ_before": prev_total,
                        "total_after": total, "by_label": incs})
        prev_total = total
    final_total = sum(last.values())

    merged: list[dict] = []
    for ev in raw:
        if merged and ev["t"] - merged[-1]["t_last"] <= merge_days * DAY:
            m = merged[-1]
            m["amount"] += ev["amount"]
            m["total_after"] = ev["total_after"]
            m["t_last"] = ev["t"]
            for k, v in ev["by_label"].items():
                m["by_label"][k] = m["by_label"].get(k, 0) + v
        else:
            merged.append({**ev, "t_last": ev["t"], "by_label": dict(ev["by_label"])})

    out = []
    for m in merged:
        groups: dict[str, float] = defaultdict(float)
        for label, v in m["by_label"].items():
            groups[label_group(label, cat_of_label)] += v
        out.append({
            "unlockTime": m["t"],
            "unlockTimeLast": m["t_last"],
            "amount": m["amount"],
            "pctOfCirculating": m["amount"] / m["circ_before"] if m["circ_before"] else None,
            "pctOfTrackedSupply": m["amount"] / final_total if final_total else None,
            "byLabel": {k: round(v, 4) for k, v in sorted(m["by_label"].items(), key=lambda kv: -kv[1])},
            "groupShares": {k: round(v / m["amount"], 4) for k, v in groups.items()},
            "mainGroup": max(groups, key=groups.get) if groups else None,
        })
    return out


def kucoin_index(contracts: list[dict]) -> dict[str, dict]:
    """simbolo token (minuscolo) -> contratto KuCoin. Gestisce XBT e i prefissi 1000x."""
    idx = {}
    for c in contracts:
        base = (c.get("baseCurrency") or c["symbol"][:-5]).upper()
        if base == "XBT":
            base = "BTC"
        m = re.match(r"^(1000000|100000|10000|1000)(.+)$", base)
        if m:
            idx.setdefault(m.group(2).lower(), {**c, "_scale": int(m.group(1))})
        idx.setdefault(base.lower(), {**c, "_scale": 1})
    return idx


async def run_unlocks(client: httpx.AsyncClient, inp: dict, fetch_contracts, fetch_klines,
                      fetch_funding_history, iso, now_ms) -> None:
    ts = now_ms()
    min_pct = inp.get("unlockMinPctCirculating", 1.0) / 100
    merge_days = inp.get("unlockMergeDays", 3)
    pre_days = inp.get("unlockWindowBeforeDays", 30)
    post_days = inp.get("unlockWindowAfterDays", 30)
    since = ts - inp.get("unlockLookbackDays", 730) * DAY
    conc = max(1, inp.get("concurrency", 4))

    contracts = await fetch_contracts(client, True)
    kidx = kucoin_index(contracts)
    Actor.log.info(f"Perpetual KuCoin USDT attivi: {len(contracts)}")

    headers = {"x-cg-demo-api-key": inp["coingeckoApiKey"]} if inp.get("coingeckoApiKey") else None
    gecko = await get_any(client, COINGECKO_LIST, headers=headers) or []
    gecko_symbol = {g["id"]: g["symbol"].lower() for g in gecko if g.get("id") and g.get("symbol")}
    Actor.log.info(f"CoinGecko: {len(gecko_symbol)} token mappati")

    slugs = await get_any(client, f"{DATASETS}/emissionsProtocolsList") or []
    if inp.get("unlockMaxProtocols"):
        slugs = slugs[: inp["unlockMaxProtocols"]]
    Actor.log.info(f"Protocolli con dati di sblocco su DefiLlama: {len(slugs)}")

    events_ds = await Actor.open_dataset(name=EVENTS_DATASET)
    upcoming_ds = await Actor.open_dataset(name=UPCOMING_DATASET)
    sem = asyncio.Semaphore(conc)
    report = defaultdict(int)
    unmatched: list[str] = []
    logged_keys = 0

    async def handle_protocol(slug: str) -> None:
        nonlocal logged_keys
        async with sem:
            data = await get_any(client, f"{DATASETS}/emissions/{slug}")
        if not isinstance(data, dict):
            report["protocol_no_data"] += 1
            return
        if logged_keys < 3:
            logged_keys += 1
            meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
            Actor.log.info(f"[debug] {slug} chiavi: {list(data)[:12]} metadata: {list(meta)[:12]}")
        gid, gid_found = find_gecko_id(data, slug)
        sym = gecko_symbol.get(gid)
        contract = kidx.get(sym) if sym else None
        if not contract:
            report["protocol_no_kucoin_perp"] += 1
            if sym:
                unmatched.append(f"{slug}:{sym}")
            return
        series = find_series(data)
        events = extract_events(series, find_categories(data), min_pct, merge_days)
        report["protocol_matched"] += 1
        report["gecko_id_from_slug"] += 0 if gid_found else 1
        first_open = contract.get("firstOpenDate") or 0

        for ev in events:
            t = ev["unlockTime"]
            base_item = {
                "protocol": slug, "geckoId": gid, "tokenSymbol": sym,
                "kucoinSymbol": contract["symbol"], "priceScale": contract["_scale"],
                "perpFirstOpen": first_open, "unlockTimeIso": iso(t), **ev,
            }
            if t > ts:
                await upcoming_ds.push_data(base_item)
                report["events_upcoming"] += 1
                continue
            if t < since:
                report["events_too_old"] += 1
                continue
            if first_open and first_open > t - 14 * DAY:
                report["events_before_perp"] += 1
                continue
            start, end = t - pre_days * DAY, min(t + post_days * DAY, ts)
            async with sem:
                try:
                    candles = await fetch_klines(client, contract["symbol"], 60, start, end)
                    funding = await fetch_funding_history(client, contract["symbol"], start, end)
                except Exception as e:
                    Actor.log.warning(f"{contract['symbol']} @ {iso(t)}: prezzi non scaricati ({e})")
                    report["events_price_error"] += 1
                    continue
            item = {**base_item, "windowComplete": end >= t + post_days * DAY,
                    "candles": candles, "funding": funding}
            await events_ds.push_data(item)
            await Actor.push_data(item)
            report["events_with_prices"] += 1
            Actor.log.info(f"{contract['symbol']} {iso(t)[:10]} +{(ev['pctOfCirculating'] or 0) * 100:.1f}% "
                           f"circolante ({ev['mainGroup']}): {len(candles)} candele")

    await asyncio.gather(*(handle_protocol(s) for s in slugs))
    out = {**report, "unmatched_sample": unmatched[:50], "min_pct_circulating": min_pct}
    await Actor.set_value("UNLOCKS_REPORT", out)
    Actor.log.info(f"Fine: {dict(report)}")


async def discover_unlock_events(client: httpx.AsyncClient, inp: dict, contracts: list[dict], iso) -> list[dict]:
    """Tutti gli sblocchi (passati e futuri) dei token con perpetual KuCoin, senza prezzi.
    Usata dall'executor paper per sapere quali sblocchi stanno arrivando."""
    min_pct = inp.get("unlockMinPctCirculating", 1.0) / 100
    merge_days = inp.get("unlockMergeDays", 3)
    kidx = kucoin_index(contracts)
    headers = {"x-cg-demo-api-key": inp["coingeckoApiKey"]} if inp.get("coingeckoApiKey") else None
    gecko = await get_any(client, COINGECKO_LIST, headers=headers) or []
    gecko_symbol = {g["id"]: g["symbol"].lower() for g in gecko if g.get("id") and g.get("symbol")}
    slugs = await get_any(client, f"{DATASETS}/emissionsProtocolsList") or []
    if not gecko_symbol or not slugs:
        raise RuntimeError(f"Calendario sblocchi non disponibile (CoinGecko {len(gecko_symbol)}, DefiLlama {len(slugs)})")
    sem = asyncio.Semaphore(max(1, inp.get("concurrency", 4)))
    found: list[dict] = []

    async def one(slug: str) -> None:
        async with sem:
            data = await get_any(client, f"{DATASETS}/emissions/{slug}")
        if not isinstance(data, dict):
            return
        gid, _ = find_gecko_id(data, slug)
        sym = gecko_symbol.get(gid)
        contract = kidx.get(sym) if sym else None
        if not contract:
            return
        for ev in extract_events(find_series(data), find_categories(data), min_pct, merge_days):
            found.append({"protocol": slug, "geckoId": gid, "tokenSymbol": sym,
                          "kucoinSymbol": contract["symbol"], "perpFirstOpen": contract.get("firstOpenDate") or 0,
                          "unlockTimeIso": iso(ev["unlockTime"]), **ev})

    await asyncio.gather(*(one(s) for s in slugs))
    return sorted(found, key=lambda e: e["unlockTime"])
