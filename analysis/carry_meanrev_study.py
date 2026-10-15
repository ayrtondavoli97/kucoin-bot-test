"""Due strategie "di range/rendimento" sui perpetual principali KuCoin.

Input: export JSON della modalità `history` del collector.
Uso:
    python analysis/carry_meanrev_study.py history.json out_dir/

A) CARRY SUL FUNDING (delta neutral: long spot + short perpetual sullo stesso nozionale)
   Si incassa il funding quando è positivo; il prezzo si annulla tra le due gambe.
   Regola: entra quando la media del funding degli ultimi N giorni, annualizzata, supera
   T_in; esci quando scende sotto T_out. Costi a ogni entrata/uscita su entrambe le gambe
   (spot taker 0,10% + perp taker 0,06% + slippage 0,05% x 2). Capitale diviso in parti
   uguali tra gli asset disponibili; la quota non investita resta in cassa a 0%.
   Non modellati: variazioni della base perp-spot, rendimento della cassa, rischio exchange.

B) MEAN REVERSION FILTRATA (proxy di un grid bot) su candele 4h
   z = (prezzo - media 20 candele) / deviazione 20 candele. Long se z < -Z, short se z > Z,
   uscita quando z torna a 0 o dopo 30 candele. Filtri provati: nessuno, volatilità prevista
   bassa, mercato laterale (efficiency ratio basso), entrambi.
   Costi: taker 0,06% + slippage 0,05% per lato, funding reale sui perpetual.

Per entrambe: scelta della regola sulla prima metà del periodo, verifica sulla seconda.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

YEAR_DAYS = 365
CARRY_COST_SIDE = 0.0010 + 0.0006 + 0.0005 * 2   # per lato (entrata o uscita), entrambe le gambe
MR_COST_SIDE = 0.0006 + 0.0005
BARS_PER_YEAR = 6 * 365


def load(path: str):
    items = json.load(open(path, encoding="utf-8"))
    closes, funding = {}, {}
    for it in items:
        sym = it["symbol"].replace("USDTM", "").replace("XBT", "BTC")
        c = pd.DataFrame([r[:5] for r in it["candles"]], columns=["t", "o", "h", "l", "c"])
        c["dt"] = pd.to_datetime(c.t, unit="ms", utc=True)
        closes[sym] = c.set_index("dt").c.astype(float).sort_index()
        f = pd.DataFrame(it.get("funding") or [])
        if not f.empty:
            f["dt"] = pd.to_datetime(f.timepoint, unit="ms", utc=True)
            funding[sym] = f.set_index("dt").fundingRate.astype(float).sort_index()
    return pd.DataFrame(closes).sort_index(), funding


def perf(r: pd.Series, per_year: int) -> dict:
    r = r.dropna()
    if len(r) < 30:
        return {}
    eq = (1 + r).cumprod()
    years = len(r) / per_year
    dd = (eq / eq.cummax() - 1).min()
    return {"ann_return": float(eq.iloc[-1] ** (1 / years) - 1), "ann_vol": float(r.std() * np.sqrt(per_year)),
            "sharpe": float(r.mean() / r.std() * np.sqrt(per_year)) if r.std() > 0 else 0.0,
            "max_dd": float(dd), "total": float(eq.iloc[-1] - 1)}


# ------------------------------------------------------------------ A) carry
def carry_daily(funding: dict) -> pd.DataFrame:
    return pd.DataFrame({s: f.groupby(f.index.floor("D")).sum() for s, f in funding.items()}).sort_index()


def run_carry(fd: pd.DataFrame, n_days: int | None, t_in: float, t_out: float) -> pd.DataFrame:
    avail = fd.notna()
    n_avail = avail.sum(axis=1).replace(0, np.nan)
    if n_days is None:                      # sempre dentro
        state = avail.astype(float)
    else:
        trail = fd.rolling(n_days, min_periods=n_days).mean()
        ann = trail * YEAR_DAYS             # funding giornaliero medio -> annuo
        st = np.zeros(fd.shape)
        held = np.zeros(fd.shape[1])
        a = ann.values
        for i in range(len(fd)):
            row = np.nan_to_num(a[i], nan=-1)
            held = np.where(held == 1, (row >= t_out).astype(float), (row > t_in).astype(float))
            st[i] = held
        state = pd.DataFrame(st, index=fd.index, columns=fd.columns)
    w = state.div(n_avail, axis=0).fillna(0)
    held = w.shift(1).fillna(0)             # deciso ieri, incassa oggi
    pnl = (held * fd.fillna(0)).sum(axis=1)
    turnover = (w - w.shift(1).fillna(0)).abs().sum(axis=1).shift(1).fillna(0)
    cost = turnover * CARRY_COST_SIDE
    return pd.DataFrame({"funding": pnl, "cost": -cost, "net": pnl - cost, "invested": held.sum(axis=1)})


# ------------------------------------------------------------------ B) mean reversion
def efficiency_ratio(p: pd.DataFrame, n: int) -> pd.DataFrame:
    change = (p - p.shift(n)).abs()
    path = p.diff().abs().rolling(n).sum()
    return change / path


def run_meanrev(px: pd.DataFrame, fund_bar: pd.DataFrame, Z: float, filt: str) -> pd.DataFrame:
    ma = px.rolling(20).mean()
    sd = px.rolling(20).std()
    z = (px - ma) / sd
    ret = px.pct_change()
    ewv = ret.pow(2).ewm(alpha=0.06).mean().pow(0.5)                 # volatilità prevista (EWMA)
    vol_ok = ewv < ewv.rolling(6 * 90, min_periods=6 * 30).median()
    er_ok = efficiency_ratio(px, 30) < 0.3
    ok = {"nessuno": pd.DataFrame(True, index=px.index, columns=px.columns),
          "vol": vol_ok, "laterale": er_ok, "vol+laterale": vol_ok & er_ok}[filt]
    pos = np.zeros(px.shape)
    zv, okv = z.values, ok.values
    cur = np.zeros(px.shape[1])
    age = np.zeros(px.shape[1])
    for i in range(len(px)):
        zi = zv[i]
        for j in range(px.shape[1]):
            if np.isnan(zi[j]):
                cur[j] = 0
                continue
            if cur[j] != 0:
                age[j] += 1
                if (cur[j] > 0 and zi[j] >= 0) or (cur[j] < 0 and zi[j] <= 0) or age[j] >= 30:
                    cur[j] = 0
            if cur[j] == 0 and okv[i, j]:
                if zi[j] < -Z:
                    cur[j], age[j] = 1, 0
                elif zi[j] > Z:
                    cur[j], age[j] = -1, 0
        pos[i] = cur
    n = px.notna().sum(axis=1).replace(0, np.nan)
    w = pd.DataFrame(pos, index=px.index, columns=px.columns).div(n, axis=0).fillna(0)
    held = w.shift(1).fillna(0)
    pnl = (held * ret.fillna(0)).sum(axis=1) - (held * fund_bar).sum(axis=1)
    turnover = (w - w.shift(1).fillna(0)).abs().sum(axis=1).shift(1).fillna(0)
    cost = turnover * MR_COST_SIDE
    return pd.DataFrame({"net": pnl - cost, "gross": pnl, "cost": -cost, "exposure": held.abs().sum(axis=1)})


def main(path: str, out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    px, funding = load(path)
    summary = {}

    # ---- A) carry
    fd = carry_daily(funding)
    fd = fd.loc[fd.index >= fd.index.min() + pd.Timedelta(days=1)]
    split = fd.index[len(fd) // 2]
    rows = []
    configs = [("sempre", None, 0, 0)] + [(f"N{n}_in{int(ti*100)}_out{int(to*100)}", n, ti, to)
                                          for n in (3, 7, 14) for ti in (0.05, 0.10, 0.20) for to in (0.0, ti / 2)]
    results = {}
    for name, n, ti, to in configs:
        res = run_carry(fd, n, ti, to)
        results[name] = res
        row = {"rule": name}
        for tag, sl in (("all", res), ("ins", res.loc[:split]), ("oos", res.loc[split:])):
            for k, v in perf(sl.net, YEAR_DAYS).items():
                row[f"{tag}_{k}"] = v
            row[f"{tag}_invested"] = float(sl.invested.mean())
            row[f"{tag}_cost_yr"] = float(-sl.cost.sum() / (len(sl) / YEAR_DAYS))
        rows.append(row)
    carry = pd.DataFrame(rows)
    carry.to_csv(out / "carry_rules.csv", index=False)
    best = carry[carry.rule != "sempre"].sort_values("ins_sharpe", ascending=False).iloc[0]
    bres = results[best.rule]
    yearly = bres.net.groupby(bres.index.year).apply(lambda r: (1 + r).prod() - 1)
    always = results["sempre"]
    summary["carry"] = {
        "period": [str(fd.index.min().date()), str(fd.index.max().date())], "split": str(split.date()),
        "best_rule_in_sample": best.rule,
        "best": {k: float(best[k]) for k in best.index if k != "rule"},
        "best_yearly": {int(k): float(v) for k, v in yearly.items()},
        "always_on": {k: float(v) for k, v in carry[carry.rule == "sempre"].iloc[0].items() if k != "rule"},
        "always_on_yearly": {int(k): float(v) for k, v in
                             always.net.groupby(always.index.year).apply(lambda r: (1 + r).prod() - 1).items()},
        "share_rules_oos_positive": float((carry.oos_ann_return > 0).mean()),
    }

    # ---- B) mean reversion
    fbar = pd.DataFrame({s: funding[s].groupby(funding[s].index.floor("4h")).sum().reindex(px.index).fillna(0)
                         for s in px.columns if s in funding}).reindex(columns=px.columns).fillna(0)
    split_b = px.index[len(px) // 2]
    rows, mres = [], {}
    for Z in (1.5, 2.0, 2.5):
        for filt in ("nessuno", "vol", "laterale", "vol+laterale"):
            res = run_meanrev(px, fbar, Z, filt).iloc[6 * 90:]
            mres[(Z, filt)] = res
            row = {"Z": Z, "filter": filt}
            for tag, sl in (("all", res), ("ins", res.loc[:split_b]), ("oos", res.loc[split_b:])):
                for k, v in perf(sl.net, BARS_PER_YEAR).items():
                    row[f"{tag}_{k}"] = v
                row[f"{tag}_gross_ann"] = float(sl.gross.mean() * BARS_PER_YEAR)
                row[f"{tag}_cost_ann"] = float(-sl.cost.mean() * BARS_PER_YEAR)
                row[f"{tag}_exposure"] = float(sl.exposure.mean())
            rows.append(row)
    mr = pd.DataFrame(rows)
    mr.to_csv(out / "meanrev_rules.csv", index=False)
    bm = mr.sort_values("ins_sharpe", ascending=False).iloc[0]
    summary["meanrev"] = {
        "period": [str(px.index.min().date()), str(px.index.max().date())], "split": str(split_b.date()),
        "best_in_sample": {"Z": float(bm.Z), "filter": bm["filter"]},
        "best": {k: float(bm[k]) for k in bm.index if k not in ("Z", "filter")},
        "share_rules_oos_positive": float((mr.oos_sharpe > 0).mean()),
        "filter_effect_oos_sharpe": mr.groupby("filter").oos_sharpe.mean().to_dict(),
    }
    json.dump(summary, open(out / "summary.json", "w"), indent=2, default=float)
    pd.set_option("display.width", 250)
    print(carry[["rule", "ins_ann_return", "ins_sharpe", "oos_ann_return", "oos_sharpe", "oos_max_dd",
                 "all_ann_return", "all_invested", "all_cost_yr"]].round(4).to_string(index=False))
    print(mr[["Z", "filter", "ins_sharpe", "oos_sharpe", "oos_ann_return", "oos_max_dd", "all_gross_ann",
              "all_cost_ann", "all_exposure"]].round(3).to_string(index=False))
    print(json.dumps(summary, indent=1, default=float))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "history.json",
         sys.argv[2] if len(sys.argv) > 2 else "carry_mr_out")
