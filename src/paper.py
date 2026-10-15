"""Executor in PAPER TRADING per la strategia sugli sblocchi di token.

Nessun ordine reale: nessuna API key, solo dati pubblici. Da schedulare ogni ora.

Regola (decisa prima del test in avanti, non va ritoccata durante):
- evento: sblocco >= 1% del circolante su un token con perpetual KuCoin USDT;
- entrata: SHORT sul token dal primo run dopo (sblocco - 14 giorni), più LONG BTC per lo
  stesso nozionale (copertura dal mercato);
- uscita: primo run dopo (sblocco + 14 giorni);
- filtri: volume 24h del perpetual >= minTurnover24h, perpetual quotato da almeno 7 giorni;
- dimensione: positionPct dell'equity per posizione, al massimo maxOpenPositions aperte;
- stop di emergenza: se il token sale oltre +emergencyStopPct dall'entrata si chiude subito
  (non c'era nel backtest: è una protezione contro i pump estremi tipo COAI).

Costi simulati: fee taker 0,06% + slippage (0,15% token, 0,05% BTC) a ogni apertura e
chiusura; funding reale incassato/pagato su entrambe le gambe.

Stato (key-value store `kucoin-paper-unlocks`):  STATE (posizioni, equity), UNLOCKS_CACHE.
Dataset: `kucoin-paper-unlocks-trades` (trade chiusi), `kucoin-paper-unlocks-equity` (equity a ogni run).
"""

from __future__ import annotations


import httpx
from apify import Actor

H = 3600 * 1000
DAY = 24 * H
BTC = "XBTUSDTM"

STORE = "kucoin-paper-unlocks"
TRADES_DS = "kucoin-paper-unlocks-trades"
EQUITY_DS = "kucoin-paper-unlocks-equity"

TAKER = 0.0006
SLIP_ALT = 0.0015
SLIP_BTC = 0.0005


def price_of(c: dict | None) -> float | None:
    if not c:
        return None
    for k in ("lastTradePrice", "markPrice"):
        try:
            v = float(c.get(k) or 0)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    return None


async def notify(client: httpx.AsyncClient, inp: dict, text: str) -> None:
    token, chat = inp.get("telegramBotToken"), inp.get("telegramChatId")
    if not token or not chat:
        return
    try:
        await client.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat, "text": text, "disable_web_page_preview": True})
    except httpx.HTTPError as e:
        Actor.log.warning(f"Telegram non raggiunto: {e}")


async def run_paper_unlocks(client: httpx.AsyncClient, inp: dict, fetch_contracts, fetch_funding_history,
                            iso, now_ms) -> None:
    from .unlocks import discover_unlock_events

    now = int(inp.get("simulateNowMs") or now_ms())   # simulateNowMs: solo per test
    entry_days = inp.get("entryDaysBefore", 14)
    exit_days = inp.get("exitDaysAfter", 14)
    pos_pct = inp.get("positionPct", 4) / 100
    max_open = inp.get("maxOpenPositions", 25)
    min_turnover = inp.get("minTurnover24h", 1_000_000)
    stop_pct = inp.get("emergencyStopPct", 100) / 100

    store = await Actor.open_key_value_store(name=STORE)
    state = await store.get_value("STATE") or {
        "createdAt": now, "createdAtIso": iso(now), "capital": float(inp.get("paperCapital", 10_000)),
        "realized": 0.0, "positions": {}, "handled": {}, "closedTrades": 0, "wins": 0,
    }
    if inp.get("resetPaperState"):
        Actor.log.warning("resetPaperState: stato azzerato")
        state = {"createdAt": now, "createdAtIso": iso(now), "capital": float(inp.get("paperCapital", 10_000)),
                 "realized": 0.0, "positions": {}, "handled": {}, "closedTrades": 0, "wins": 0}

    contracts = await fetch_contracts(client, True)
    by_sym = {c["symbol"]: c for c in contracts}
    btc_px = price_of(by_sym.get(BTC))
    if not btc_px:
        raise RuntimeError("Prezzo BTC non disponibile, run annullato senza modifiche")

    # --- calendario sblocchi: aggiornato al massimo ogni cacheHours
    cache = await store.get_value("UNLOCKS_CACHE") or {}
    if now - cache.get("ts", 0) > inp.get("cacheHours", 12) * H or not cache.get("events"):
        events = await discover_unlock_events(client, inp, contracts, iso)
        cache = {"ts": now, "events": [e for e in events if e["unlockTime"] > now - (exit_days + 2) * DAY]}
        await store.set_value("UNLOCKS_CACHE", cache)
        Actor.log.info(f"Calendario sblocchi aggiornato: {len(cache['events'])} eventi rilevanti")
    events = cache["events"]

    trades_ds = await Actor.open_dataset(name=TRADES_DS)
    positions: dict = state["positions"]

    # --- 1) funding maturato sulle posizioni aperte
    for key, p in positions.items():
        start = p["fundingUntil"]
        if now - start < H:
            continue
        try:
            fa = await fetch_funding_history(client, p["symbol"], start, now)
            fb = await fetch_funding_history(client, BTC, start, now)
        except Exception as e:
            Actor.log.warning(f"{key}: funding non aggiornato ({e})")
            continue
        alt = sum(float(f["fundingRate"] or 0) for f in fa if start < f["timepoint"] <= now)
        btc = sum(float(f["fundingRate"] or 0) for f in fb if start < f["timepoint"] <= now)
        # short sul token incassa il funding positivo, long BTC lo paga
        p["funding"] += p["notional"] * (alt - btc)
        p["fundingUntil"] = now

    # --- 2) chiusure: scadenza, stop di emergenza, contratto sparito
    def close(key: str, reason: str, px: float | None) -> dict:
        p = positions.pop(key)
        px = px or p["lastPx"]
        exit_alt = px * (1 + SLIP_ALT)                       # ricompro il token un po' più caro
        exit_btc = btc_px * (1 - SLIP_BTC)                   # rivendo BTC un po' più basso
        pnl_alt = p["notional"] * (p["entryFill"] - exit_alt) / p["entryFill"]
        pnl_btc = p["notional"] * (exit_btc / p["btcEntryFill"] - 1)
        fees = p["notional"] * TAKER * 2 * 2                 # 2 gambe x (apertura + chiusura)
        pnl = pnl_alt + pnl_btc + p["funding"] - fees
        state["realized"] += pnl
        state["closedTrades"] += 1
        state["wins"] += 1 if pnl > 0 else 0
        return {**p, "closedAt": now, "closedAtIso": iso(now), "exitReason": reason, "exitPx": px,
                "btcExitPx": btc_px, "pnlAlt": pnl_alt, "pnlBtc": pnl_btc, "fees": fees,
                "pnl": pnl, "returnOnNotional": pnl / p["notional"]}

    closed = []
    for key in list(positions):
        p = positions[key]
        c = by_sym.get(p["symbol"])
        px = price_of(c)
        if px:
            p["lastPx"] = px
        if not c:
            closed.append(close(key, "contratto_non_più_attivo", None))
        elif px and px >= p["entryPx"] * (1 + stop_pct):
            closed.append(close(key, "stop_emergenza", px))
        elif now >= p["exitDue"]:
            closed.append(close(key, "scadenza", px))
    for t in closed:
        await trades_ds.push_data(t)
        Actor.log.info(f"CHIUSA {t['symbol']} ({t['exitReason']}): {t['pnl']:+.2f} USDT ({t['returnOnNotional']*100:+.1f}%)")
        await notify(client, inp, f"[PAPER] Chiusa short {t['symbol']} ({t['exitReason']}): "
                                  f"{t['pnl']:+.2f} USDT ({t['returnOnNotional']*100:+.1f}%)")

    # --- 3) aperture
    def equity_now() -> float:
        unreal = 0.0
        for p in positions.values():
            px = p["lastPx"]
            unreal += p["notional"] * (p["entryFill"] - px) / p["entryFill"]
            unreal += p["notional"] * (btc_px / p["btcEntryFill"] - 1)
            unreal += p["funding"] - p["notional"] * TAKER * 2
        return state["capital"] + state["realized"] + unreal

    handled: dict = state["handled"]
    for ev in events:
        key = f"{ev['kucoinSymbol']}:{ev['unlockTime']}"
        if key in handled or key in positions:
            continue
        tu = ev["unlockTime"]
        if not (tu - entry_days * DAY <= now < tu):
            continue
        sym = ev["kucoinSymbol"]
        c = by_sym.get(sym)
        px = price_of(c)
        reason = None
        if not c or not px:
            reason = "contratto_non_attivo"
        elif now - (ev.get("perpFirstOpen") or 0) < 7 * DAY:
            reason = "perpetual_troppo_nuovo"
        elif float(c.get("turnoverOf24h") or 0) < min_turnover:
            reason = "volume_basso"
        elif any(p["symbol"] == sym for p in positions.values()):
            reason = "già_short_sul_token"
        elif len(positions) >= max_open:
            reason = "max_posizioni"
        elif now > tu - (entry_days - 2) * DAY:
            reason = "entrata_in_ritardo"     # il run è arrivato tardi: non si insegue
        if reason:
            handled[key] = reason
            Actor.log.info(f"Saltato {key} ({ev['unlockTimeIso'][:10]}): {reason}")
            continue
        notional = round(equity_now() * pos_pct, 2)
        positions[key] = {
            "key": key, "symbol": sym, "protocol": ev["protocol"], "unlockTime": tu,
            "unlockTimeIso": ev["unlockTimeIso"], "pctOfCirculating": ev.get("pctOfCirculating"),
            "mainGroup": ev.get("mainGroup"), "groupShares": ev.get("groupShares"),
            "openedAt": now, "openedAtIso": iso(now), "exitDue": tu + exit_days * DAY,
            "exitDueIso": iso(tu + exit_days * DAY), "notional": notional,
            "entryPx": px, "entryFill": px * (1 - SLIP_ALT), "lastPx": px,
            "btcEntryPx": btc_px, "btcEntryFill": btc_px * (1 + SLIP_BTC),
            "funding": 0.0, "fundingUntil": now,
        }
        handled[key] = "aperta"
        Actor.log.info(f"APERTA short {sym} {notional:.2f} USDT @ {px} (sblocco {ev['unlockTimeIso'][:10]}, "
                       f"{(ev.get('pctOfCirculating') or 0)*100:.1f}% circolante, {ev.get('mainGroup')})")
        await notify(client, inp, f"[PAPER] Aperta short {sym} {notional:.0f} USDT @ {px} + long BTC. "
                                  f"Sblocco {ev['unlockTimeIso'][:10]} "
                                  f"({(ev.get('pctOfCirculating') or 0)*100:.1f}% circolante, {ev.get('mainGroup')})")

    # pulizia: dimentica gli eventi gestiti ormai passati da tempo
    state["handled"] = {k: v for k, v in handled.items() if int(k.rsplit(":", 1)[1]) > now - 60 * DAY}

    eq = equity_now()
    snap = {"ts": now, "tsIso": iso(now), "equity": round(eq, 2), "realized": round(state["realized"], 2),
            "openPositions": len(positions), "closedTrades": state["closedTrades"],
            "winRate": round(state["wins"] / state["closedTrades"], 3) if state["closedTrades"] else None,
            "returnSinceStart": round(eq / state["capital"] - 1, 4)}
    state["lastRun"] = now
    state["lastRunIso"] = iso(now)
    state["summary"] = snap
    await store.set_value("STATE", state)
    await (await Actor.open_dataset(name=EQUITY_DS)).push_data(snap)
    await Actor.set_value("OUTPUT", {**snap, "positions": list(positions.values())})
    upcoming = [e for e in events if now < e["unlockTime"] - entry_days * DAY < now + 7 * DAY]
    Actor.log.info(f"Equity {eq:.2f} USDT ({snap['returnSinceStart']*100:+.2f}%), {len(positions)} aperte, "
                   f"{state['closedTrades']} chiuse. Prossime entrate entro 7 giorni: "
                   f"{[e['kucoinSymbol'] + ' ' + e['unlockTimeIso'][:10] for e in upcoming][:10]}")
