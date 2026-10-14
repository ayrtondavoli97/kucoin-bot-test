"""Studio eventi sui nuovi listing dei perpetual KuCoin Futures.

Input: export JSON del backfill a finestra evento (mode=backfill, eventWindowDays>0).
Uso:
    python analysis/listing_study.py backfill.json out_dir/

Produce in out_dir:
    listings.csv        una riga per contratto: classe, liquidità, regime BTC, rendimenti
    grid_results.csv    tutte le regole short testate (in-sample / out-of-sample / totale)
    best_trades.csv     i trade della regola scelta
    summary.json        numeri principali
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

H = 3600 * 1000
DAY = 24 * H

# ------------------------------------------------------------------ classi
# KuCoin dal 2026-03 lista anche azioni, ETF, materie prime e pre-IPO.
# Questi NON seguono la dinamica "nuovo token" e vanno esclusi.
NON_CRYPTO = set("""
PENG VST SMH APP SOFI TMF MRNA BWET NKE KIOXIA CSOPSAMSUNG2L CSOPSKHYNIX2L BNC BYD GPRO IONQ MARA
MEITUAN PDD KUAISHOU SHEINHKD HANMI KO SAMSUNGEM ZHONGJI GIGADEV SHAZ XIAOMIHKD TENCENTHKD POPMART
TENCENT MINIMAX MUU ZHIPU CRWD SOXS KORU SKHY SNXX MVLL TTWO STRC CIEN KLAC LRCX SMCI SONY ALAB POET
TER ADBE UVXY STXX DKNG EWZ GME RIVN XLE ZM SPCX AMAT AXTI CRDO BX IWM HYUNDAI SAMSUNG CRM SKHYNIX
HPE IREN ONDS NOW ANTHROPIC NVO LUNR SLX DELL MP OPENAI RGTI RDW IBM NOK INFQ APLD ASTS BE VRT ASML
OKLO RDDT NBIS SQQQ XOM BRKB FLNC JPM V WMT AAOI GLW MCD TQQQ SOXL CSCO DIS HD UBER CBRS MRVL COHR
RKLB WDC HIMS LITE QCOM ARM USAR BMNR LLY COST AMD CRWV AVGO BABA NFLX MSFT ORCL MU SNDK AAPL QQQ SPY
TSM BZ NATGAS CL GOOGL NVDA META PAYP EWJ EWY AMZN INTC PLTR COIN CRCL HOOD MSTR TSLA COPPER XPD XPT
XAG AGPU CYPH SKUU INTW KSTR BOT RAM BSP O LYTE QNTX BBX DRAM BILL
""".split())
# Non sono "nuovi token": stablecoin, oro tokenizzato, indici, token dell'exchange.
SPECIAL = {"USDG", "XAUT", "ETHBTC", "KCS"}
# Dopo l'arrivo delle azioni (2026-03-01) tengo solo i nomi che so essere crypto.
STOCKS_START = pd.Timestamp("2026-03-01", tz="UTC").value // 10**6
KNOWN_CRYPTO_AFTER = set("""
HAJIMI CASHCAT BICO XDC GRVT 10000REKT GRAM BASED EDGE CFG MANTRA MOONSHOT AIGENSYN GENIUS
""".split())


def classify(base: str, first_open: int) -> str:
    if base in SPECIAL:
        return "special"
    if base in NON_CRYPTO:
        return "tradfi"
    if first_open >= STOCKS_START and base not in KNOWN_CRYPTO_AFTER:
        return "uncertain"
    return "crypto"


# ------------------------------------------------------------------ dati
def to_df(rows: list) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["t", "o", "h", "l", "c", "v", "q"])
    df = pd.DataFrame([r[:7] for r in rows])
    df.columns = ["t", "o", "h", "l", "c", "v", "q"][: df.shape[1]]
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.drop_duplicates("t").sort_values("t").reset_index(drop=True)


def load(path: str):
    items = json.load(open(path, encoding="utf-8"))
    btc = next(x for x in items if x.get("reference"))
    contracts = [x for x in items if not x.get("reference")]
    return to_df(btc["candles"]), contracts


def btc_regime(btc: pd.DataFrame, t: int) -> dict:
    past = btc[btc.t <= t]
    if len(past) < 720:
        return {"btc_above_sma30": np.nan, "btc_ret30": np.nan}
    last = past.c.iloc[-1]
    sma = past.c.iloc[-720:].mean()
    return {"btc_above_sma30": bool(last > sma), "btc_ret30": last / past.c.iloc[-720] - 1}


def candle_ending_by(hourly: pd.DataFrame, t: int) -> int | None:
    """Indice dell'ultima candela oraria che si chiude entro t (i listing non sono
    sempre allineati all'ora). None se la candela più vicina è vecchia di oltre 1h."""
    i = int(np.searchsorted(hourly.t.values, t - H, side="right")) - 1
    if i < 0 or hourly.t.iloc[i] < t - 2 * H:
        return None
    return i


def candle_closing_from(hourly: pd.DataFrame, t: int) -> int | None:
    """Indice della prima candela oraria che si chiude a t o subito dopo (entro 1h).
    Usata per entrate e orizzonti: con listing non allineati all'ora si entra al
    massimo un'ora dopo il momento teorico, mai prima."""
    i = int(np.searchsorted(hourly.t.values, t - H, side="left"))
    if i >= len(hourly) or hourly.t.iloc[i] + H - t >= H:
        return None
    return i


# ------------------------------------------------------------------ evento
HORIZONS = {"1h": 1, "4h": 4, "24h": 24, "3d": 72, "7d": 168, "14d": 336, "30d": 720}


def event_row(x: dict, btc: pd.DataFrame) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    base = x["symbol"][:-5]
    t0 = x["firstOpenDate"]
    fine, hourly = to_df(x.get("candlesFine", [])), to_df(x["candles"])
    row = {
        "symbol": x["symbol"], "base": base, "first_open": t0,
        "first_open_iso": x["firstOpenDateIso"], "class": classify(base, t0),
        "window_complete": x.get("windowComplete"), "n_fine": len(fine), "n_hourly": len(hourly),
    }
    if len(fine) < 4 or len(hourly) < 25:
        row["usable"] = False
        return row, fine, hourly
    row["usable"] = True
    p0 = fine.c.iloc[0]  # prezzo a listing + 15 minuti
    row["p0"] = p0
    row["first15_range"] = fine.h.iloc[0] / fine.l.iloc[0] - 1
    first24 = hourly[hourly.t < t0 + DAY]
    row["turnover_24h"] = first24.q.sum() if "q" in first24 else np.nan
    row["pump_max_24h"] = first24.h.max() / p0 - 1
    row["dump_min_24h"] = first24.l.min() / p0 - 1
    row["hours_to_24h_peak"] = (first24.t.iloc[first24.h.argmax()] - t0) / H
    for name, hrs in HORIZONS.items():
        i = candle_closing_from(hourly, t0 + hrs * H) if hrs < 720 else candle_ending_by(hourly, t0 + hrs * H)
        row[f"ret_{name}"] = hourly.c.iloc[i] / p0 - 1 if i is not None else np.nan
    row["max_up_30d"] = hourly.h.max() / p0 - 1
    row.update(btc_regime(btc, t0))
    return row, fine, hourly


# ------------------------------------------------------------------ backtest
TAKER = 0.0006


def short_trade(row: dict, hourly: pd.DataFrame, funding: list, entry_h: int, hold_h: int,
                stop: float | None, slip: float) -> dict | None:
    t0 = row["first_open"]
    ie = candle_closing_from(hourly, t0 + entry_h * H)
    ix = candle_ending_by(hourly, t0 + (entry_h + hold_h) * H)
    if ie is None or ix is None or ix <= ie:
        return None
    entry = hourly.c.iloc[ie]
    entry_t = int(hourly.t.iloc[ie] + H)
    path = hourly.iloc[ie + 1: ix + 1]
    exit_px, exit_reason, exit_time = hourly.c.iloc[ix], "time", int(hourly.t.iloc[ix] + H)
    if stop is not None:
        level = entry * (1 + stop)
        hit = path[path.h >= level]
        if len(hit):
            bar = hit.iloc[0]
            # se apre già sopra lo stop (gap), esci all'apertura
            exit_px = max(level, bar.o)
            exit_reason, exit_time = "stop", int(bar.t + H)
    entry_fill = entry * (1 - slip)                 # vendi un po' più in basso
    exit_fill = exit_px * (1 + slip * (2 if exit_reason == "stop" else 1))  # ricompri più in alto
    gross = (entry_fill - exit_fill) / entry_fill
    fees = TAKER * (1 + exit_fill / entry_fill)
    fund = sum(float(f["fundingRate"] or 0) for f in funding
               if entry_t < f["timepoint"] <= exit_time)  # funding positivo = lo short incassa
    return {"symbol": row["symbol"], "first_open": t0, "entry_px": entry, "exit_px": exit_px,
            "exit_reason": exit_reason, "gross": gross, "fees": fees, "funding": fund,
            "net": gross - fees + fund}


def stats(r: pd.Series) -> dict:
    if len(r) == 0:
        return {"n": 0}
    gains, losses = r[r > 0].sum(), -r[r < 0].sum()
    return {"n": int(len(r)), "mean": r.mean(), "median": r.median(), "win": (r > 0).mean(),
            "pf": gains / losses if losses > 0 else np.inf, "worst": r.min(), "best": r.max()}


def bootstrap_ci(r: np.ndarray, n: int = 5000, seed: int = 7) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    means = rng.choice(r, size=(n, len(r)), replace=True).mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def main(path: str, out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    btc, contracts = load(path)

    rows, series = [], {}
    for x in contracts:
        row, fine, hourly = event_row(x, btc)
        rows.append(row)
        series[x["symbol"]] = (hourly, x.get("funding", []))
    ev = pd.DataFrame(rows)
    ev.to_csv(out / "listings.csv", index=False)

    crypto = ev[(ev["class"] == "crypto") & ev.usable].copy()
    min_turnover = 1_000_000
    crypto["liquid"] = crypto.turnover_24h >= min_turnover

    # --- griglia di regole short
    entries, holds = [1, 4, 24], [72, 168, 336, 672]
    stops = [0.15, 0.3, 0.5, 1.0, None]
    split_t = crypto.first_open.median()
    grid = []
    trades_by_rule = {}
    for slip in (0.001, 0.003):
        for e in entries:
            for hd in holds:
                for s in stops:
                    tr = []
                    for _, row in crypto[crypto.liquid].iterrows():
                        hourly, funding = series[row.symbol]
                        t = short_trade(row, hourly, funding, e, hd, s, slip)
                        if t:
                            tr.append(t)
                    tdf = pd.DataFrame(tr)
                    if tdf.empty:
                        continue
                    key = (slip, e, hd, s)
                    trades_by_rule[key] = tdf
                    ins = tdf[tdf.first_open < split_t].net
                    oos = tdf[tdf.first_open >= split_t].net
                    rec = {"slip": slip, "entry_h": e, "hold_h": hd, "stop": s if s is not None else "none"}
                    for tag, r in (("all", tdf.net), ("ins", ins), ("oos", oos)):
                        for k, v in stats(r).items():
                            rec[f"{tag}_{k}"] = v
                    grid.append(rec)
    grid = pd.DataFrame(grid)
    grid.to_csv(out / "grid_results.csv", index=False)

    # scelta SOLO sull'in-sample (listing più vecchi), slippage base
    base = grid[grid.slip == 0.001].sort_values("ins_mean", ascending=False)
    best = base.iloc[0]
    key = (0.001, int(best.entry_h), int(best.hold_h), None if best.stop == "none" else float(best.stop))
    bt = trades_by_rule[key]
    bt.to_csv(out / "best_trades.csv", index=False)
    stress = trades_by_rule[(0.003,) + key[1:]]
    oos = bt[bt.first_open >= split_t].net.values

    # percorso mediano del prezzo dopo il listing (crypto liquide)
    days = list(range(0, 31))
    paths = []
    for _, row in crypto[crypto.liquid].iterrows():
        hourly = series[row.symbol][0]
        p = []
        for dd in days:
            i = candle_ending_by(hourly, row.first_open + max(dd * 24, 1) * H)
            p.append(hourly.c.iloc[i] / row.p0 - 1 if i is not None else np.nan)
        paths.append(p)
    paths = np.array(paths, dtype=float)

    def q(a, p):
        return [float(np.nanpercentile(a[:, i], p)) for i in range(a.shape[1])]

    by_regime = {}
    for flag, g in crypto[crypto.liquid].groupby("btc_above_sma30"):
        by_regime["btc_sopra_sma30" if flag else "btc_sotto_sma30"] = {
            "n": int(len(g)), "ret_7d_median": float(g.ret_7d.median()),
            "ret_30d_median": float(g.ret_30d.median()),
            "share_down_30d": float((g.ret_30d < 0).mean()),
        }

    summary = {
        "contracts_total": int(len(ev)),
        "by_class": ev["class"].value_counts().to_dict(),
        "crypto_usable": int(len(crypto)),
        "crypto_liquid": int(crypto.liquid.sum()),
        "listing_range": [str(crypto.first_open_iso.min()), str(crypto.first_open_iso.max())],
        "split_date": pd.to_datetime(split_t, unit="ms", utc=True).isoformat(),
        "event_liquid": {
            col: {"median": float(crypto[crypto.liquid][col].median()),
                  "share_neg": float((crypto[crypto.liquid][col] < 0).mean()),
                  "n": int(crypto[crypto.liquid][col].notna().sum())}
            for col in [f"ret_{k}" for k in HORIZONS]
        },
        "pump_max_24h": {"median": float(crypto[crypto.liquid].pump_max_24h.median()),
                         "p90": float(crypto[crypto.liquid].pump_max_24h.quantile(0.9)),
                         "max": float(crypto[crypto.liquid].pump_max_24h.max())},
        "max_up_30d": {"p90": float(crypto[crypto.liquid].max_up_30d.quantile(0.9)),
                       "share_over_100pct": float((crypto[crypto.liquid].max_up_30d > 1).mean())},
        "by_regime": by_regime,
        "best_rule_chosen_in_sample": {k: (v if not isinstance(v, (np.floating, np.integer)) else float(v))
                                       for k, v in best.items()},
        "best_rule_oos": stats(pd.Series(oos)),
        "best_rule_oos_ci95_mean": bootstrap_ci(oos) if len(oos) > 5 else None,
        "best_rule_all_stress_slip03": stats(stress.net),
        "best_rule_exit_reasons": bt.exit_reason.value_counts().to_dict(),
        "best_rule_funding_mean": float(bt.funding.mean()),
        "rules_tested": int(len(base)),
        "rules_positive_oos_share": float((base.oos_mean > 0).mean()),
        "path_days": days, "path_median": q(paths, 50), "path_p25": q(paths, 25), "path_p75": q(paths, 75),
    }
    json.dump(summary, open(out / "summary.json", "w"), indent=2, default=float)
    print(json.dumps({k: v for k, v in summary.items() if not k.startswith("path")}, indent=2, default=float))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "backfill.json",
         sys.argv[2] if len(sys.argv) > 2 else "analysis_out")
