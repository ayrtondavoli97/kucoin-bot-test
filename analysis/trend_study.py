"""Backtest trend following sui perpetual KuCoin principali.

Input: export JSON della modalità `history` del collector.
Uso:
    python analysis/trend_study.py history.json out_dir/

Regole (poche e classiche, per non adattarsi al passato):
- TSMOM:   segno del rendimento degli ultimi L giorni            L = 30, 60, 90, 180
- EMA:     media veloce sopra/sotto la lenta                     (10,50) (20,100) (50,200)
- BREAKOUT: rottura del massimo/minimo di N giorni, uscita a N/2 N = 20, 55, 100
Ognuna in versione solo long e long/short.

Portafoglio: stesso rischio per ogni asset (volatilità obiettivo 50% annuo per asset,
mai più di 1x per asset), diviso per il numero di asset disponibili quel giorno:
esposizione lorda massima 1x, nessuna leva. Segnale calcolato a chiusura del giorno t,
posizione tenuta il giorno t+1 (niente lookahead). Costi: fee taker + slippage su ogni
variazione di posizione; funding reale pagato/incassato ogni giorno.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

TAKER = 0.0006
SLIPPAGE = 0.0005          # perpetual molto liquidi
TARGET_VOL = 0.50          # per asset, annualizzata
VOL_WINDOW = 30
BAND = 0.25                # non ribilanciare se la variazione relativa è sotto il 25%
MIN_HISTORY = 200          # giorni minimi prima di usare un asset (serve per EMA 200)


# ------------------------------------------------------------------ dati
def load(path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    items = json.load(open(path, encoding="utf-8"))
    closes, funding = {}, {}
    for it in items:
        sym = it["symbol"].replace("USDTM", "").replace("XBT", "BTC")
        c = pd.DataFrame([r[:6] for r in it["candles"]], columns=["t", "o", "h", "l", "c", "v"])
        if c.empty:
            continue
        c["day"] = pd.to_datetime(c.t, unit="ms", utc=True).dt.floor("D")
        daily = c.sort_values("t").groupby("day").c.last().astype(float)
        closes[sym] = daily.iloc[:-1]  # l'ultimo giorno è incompleto
        f = pd.DataFrame(it.get("funding") or [])
        if not f.empty:
            f["day"] = pd.to_datetime(f.timepoint, unit="ms", utc=True).dt.floor("D")
            funding[sym] = f.groupby("day").fundingRate.sum().astype(float)
    px = pd.DataFrame(closes).sort_index()
    fd = pd.DataFrame(funding).reindex(px.index).reindex(columns=px.columns).fillna(0.0)
    return px, fd


# ------------------------------------------------------------------ segnali
def sig_tsmom(px: pd.DataFrame, L: int, ls: bool) -> pd.DataFrame:
    r = px / px.shift(L) - 1
    s = np.sign(r)
    return s if ls else s.clip(lower=0)


def sig_ema(px: pd.DataFrame, fast: int, slow: int, ls: bool) -> pd.DataFrame:
    f = px.ewm(span=fast, adjust=False, min_periods=fast).mean()
    s_ = px.ewm(span=slow, adjust=False, min_periods=slow).mean()
    s = np.sign(f - s_)
    return s if ls else s.clip(lower=0)


def sig_breakout(px: pd.DataFrame, N: int, ls: bool) -> pd.DataFrame:
    hi = px.rolling(N).max().shift(1)
    lo = px.rolling(N).min().shift(1)
    hi_x = px.rolling(N // 2).max().shift(1)
    lo_x = px.rolling(N // 2).min().shift(1)
    out = pd.DataFrame(0.0, index=px.index, columns=px.columns)
    for col in px.columns:
        pos = 0.0
        p, h, l, hx, lx = (px[col].values, hi[col].values, lo[col].values, hi_x[col].values, lo_x[col].values)
        res = np.zeros(len(p))
        for i in range(len(p)):
            if np.isnan(p[i]) or np.isnan(h[i]):
                pos = 0.0
            elif pos > 0 and p[i] < lx[i]:
                pos = 0.0
            elif pos < 0 and p[i] > hx[i]:
                pos = 0.0
            if pos == 0 and not np.isnan(h[i]):
                if p[i] > h[i]:
                    pos = 1.0
                elif ls and p[i] < l[i]:
                    pos = -1.0
            res[i] = pos
        out[col] = res
    return out


def strategies() -> dict[str, callable]:
    s = {}
    for ls in (False, True):
        tag = "LS" if ls else "L"
        for L in (30, 60, 90, 180):
            s[f"TSMOM{L}_{tag}"] = lambda px, L=L, ls=ls: sig_tsmom(px, L, ls)
        for f, sl in ((10, 50), (20, 100), (50, 200)):
            s[f"EMA{f}-{sl}_{tag}"] = lambda px, f=f, sl=sl, ls=ls: sig_ema(px, f, sl, ls)
        for N in (20, 55, 100):
            s[f"BRK{N}_{tag}"] = lambda px, N=N, ls=ls: sig_breakout(px, N, ls)
    # Ensemble: media di TUTTI i segnali della stessa famiglia (nessuna scelta sul passato).
    for tag in ("L", "LS"):
        members = [k for k in s if k.endswith("_" + tag)]
        s[f"ENSEMBLE_{tag}"] = lambda px, members=members, s=dict(s): sum(s[m](px).fillna(0) for m in members) / len(members)
    return s


# ------------------------------------------------------------------ portafoglio
def apply_band(target: pd.DataFrame) -> pd.DataFrame:
    w = target.values.copy()
    held = np.zeros(w.shape[1])
    out = np.zeros_like(w)
    for i in range(len(w)):
        t = np.nan_to_num(w[i])
        change = (np.sign(t) != np.sign(held)) | (np.abs(t - held) > BAND * np.abs(t))
        held = np.where(change, t, held)
        out[i] = held
    return pd.DataFrame(out, index=target.index, columns=target.columns)


def run(px: pd.DataFrame, fd: pd.DataFrame, signal: pd.DataFrame) -> pd.DataFrame:
    ret = px.pct_change()
    vol = ret.rolling(VOL_WINDOW).std() * np.sqrt(365)
    age = px.notna().cumsum()
    eligible = (age >= MIN_HISTORY) & vol.notna() & (vol > 0)
    n_active = eligible.sum(axis=1).replace(0, np.nan)
    scale = (TARGET_VOL / vol).clip(upper=1.0)
    target = (signal * scale).where(eligible, 0.0).div(n_active, axis=0).fillna(0.0)
    w = apply_band(target)
    held = w.shift(1).fillna(0.0)                      # posizione decisa ieri, tenuta oggi
    pnl_price = (held * ret.fillna(0.0)).sum(axis=1)
    pnl_funding = -(held * fd).sum(axis=1)             # funding > 0: i long pagano, gli short incassano
    turnover = (w - w.shift(1).fillna(0.0)).abs().sum(axis=1).shift(1).fillna(0.0)
    cost = turnover * (TAKER + SLIPPAGE)
    out = pd.DataFrame({"price": pnl_price, "funding": pnl_funding, "cost": -cost,
                        "gross_exposure": held.abs().sum(axis=1), "turnover": turnover})
    out["net"] = out.price + out.funding + out.cost
    return out


def metrics(r: pd.Series, extra: pd.DataFrame | None = None) -> dict:
    r = r.dropna()
    if len(r) < 30:
        return {}
    eq = (1 + r).cumprod()
    years = len(r) / 365
    cagr = eq.iloc[-1] ** (1 / years) - 1 if eq.iloc[-1] > 0 else -1.0
    vol = r.std() * np.sqrt(365)
    dd = (eq / eq.cummax() - 1).min()
    m = {"days": len(r), "cagr": cagr, "vol": vol, "sharpe": r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0,
         "max_dd": dd, "calmar": cagr / abs(dd) if dd < 0 else np.nan, "total": eq.iloc[-1] - 1}
    if extra is not None:
        e = extra.loc[r.index]
        m.update({"exposure": e.gross_exposure.mean(), "turnover_yr": e.turnover.sum() / years,
                  "cost_yr": -e.cost.sum() / years, "funding_yr": e.funding.sum() / years})
    return m


def main(path: str, out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    px, fd = load(path)
    start = px.notna().sum(axis=1)
    start = start[start >= 3].index.min() + pd.Timedelta(days=MIN_HISTORY)
    px_eval_index = px.index[px.index >= start]
    split = px_eval_index[len(px_eval_index) // 2]

    rows, curves, results = [], {}, {}
    for name, fn in strategies().items():
        res = run(px, fd, fn(px)).loc[start:]
        results[name] = res
        curves[name] = (1 + res.net).cumprod()
        row = {"strategy": name}
        for tag, sl in (("all", res), ("ins", res.loc[:split - pd.Timedelta(days=1)]), ("oos", res.loc[split:])):
            for k, v in metrics(sl.net, sl).items():
                row[f"{tag}_{k}"] = v
        rows.append(row)

    # benchmark
    ret = px.pct_change().loc[start:]
    bench = {"BTC_buyhold": ret["BTC"] if "BTC" in ret else ret.iloc[:, 0],
             "EW_buyhold": ret.where(px.notna().cumsum().loc[start:] >= MIN_HISTORY).mean(axis=1)}
    for name, r in bench.items():
        curves[name] = (1 + r.fillna(0)).cumprod()
        row = {"strategy": name}
        for tag, sl in (("all", r), ("ins", r.loc[:split - pd.Timedelta(days=1)]), ("oos", r.loc[split:])):
            for k, v in metrics(sl).items():
                row[f"{tag}_{k}"] = v
        rows.append(row)

    table = pd.DataFrame(rows)
    table.to_csv(out / "strategies.csv", index=False)
    pd.DataFrame(curves).to_csv(out / "equity_curves.csv")

    strat = table[~table.strategy.str.contains("buyhold")]
    best = strat.sort_values("ins_sharpe", ascending=False).iloc[0]
    best_res = results[best.strategy]
    yearly = best_res.net.groupby(best_res.index.year).apply(lambda r: (1 + r).prod() - 1)
    summary = {
        "assets": list(px.columns), "eval_start": str(start.date()), "split": str(split.date()),
        "end": str(px.index.max().date()), "strategies_tested": int(len(strat)),
        "share_oos_sharpe_positive": float((strat.oos_sharpe > 0).mean()),
        "median_oos_sharpe": float(strat.oos_sharpe.median()),
        "best_in_sample": best.strategy,
        "best_ins": {k[4:]: float(best[k]) for k in best.index if k.startswith("ins_")},
        "best_oos": {k[4:]: float(best[k]) for k in best.index if k.startswith("oos_")},
        "best_yearly": {int(k): float(v) for k, v in yearly.items()},
        "benchmarks": table[table.strategy.str.contains("buyhold")].set_index("strategy")[
            ["all_cagr", "all_sharpe", "all_max_dd", "oos_sharpe", "oos_max_dd"]].to_dict("index"),
    }
    json.dump(summary, open(out / "summary.json", "w"), indent=2, default=float)
    cols = ["strategy", "ins_sharpe", "oos_sharpe", "oos_cagr", "oos_max_dd", "all_sharpe", "all_cagr",
            "all_max_dd", "all_exposure", "all_cost_yr", "all_funding_yr"]
    pd.set_option("display.width", 220)
    print(table[cols].sort_values("ins_sharpe", ascending=False).round(3).to_string(index=False))
    print(json.dumps({k: v for k, v in summary.items() if k != "assets"}, indent=2, default=float))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "history.json",
         sys.argv[2] if len(sys.argv) > 2 else "trend_out")
