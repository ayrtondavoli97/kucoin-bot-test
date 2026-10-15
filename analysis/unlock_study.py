"""Studio eventi sugli sblocchi di token (DefiLlama) per i perpetual KuCoin.

Input: export della modalità `unlocks` + export della modalità `history` (per BTC).
Uso:
    python analysis/unlock_study.py unlocks.json history.json out_dir/

Domande:
1. Il prezzo scende prima/dopo lo sblocco, al netto del mercato (BTC)?
2. Conta a chi va lo sblocco (team/investitori vs ecosistema/community) e quanto è grande?
3. Primo sblocco "isolato" vs rate ricorrenti?
4. Una regola short semplice regge fuori campione, con fee, slippage e funding?
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

H = 3600 * 1000
DAY = 24 * H
TAKER = 0.0006
SLIP = 0.0015            # mid cap: più slippage dei major
INSIDER_GROUPS = {"team", "investors", "insiders"}
DAYS = [-30, -14, -7, -3, -1, 0, 1, 3, 7, 14, 29]


def load_btc(history_path: str) -> pd.Series:
    items = json.load(open(history_path, encoding="utf-8"))
    btc = next(x for x in items if x["symbol"] == "XBTUSDTM")
    c = pd.DataFrame([r[:5] for r in btc["candles"]], columns=["t", "o", "h", "l", "c"])
    return c.set_index("t").c.astype(float).sort_index()


def price_at(t_idx: np.ndarray, closes: np.ndarray, t: int, max_gap: int) -> float:
    """Close dell'ultima candela iniziata prima di t (entro max_gap ms)."""
    i = int(np.searchsorted(t_idx, t, side="left")) - 1
    if i < 0 or t - t_idx[i] > max_gap:
        return np.nan
    return float(closes[i])


def build_events(unlocks_path: str, btc: pd.Series) -> tuple[pd.DataFrame, dict]:
    items = json.load(open(unlocks_path, encoding="utf-8"))
    btc_t, btc_c = btc.index.values, btc.values
    rows, series = [], {}
    for k, x in enumerate(items):
        c = pd.DataFrame([r[:5] for r in x["candles"]], columns=["t", "o", "h", "l", "c"]).astype(float)
        c = c.drop_duplicates("t").sort_values("t").reset_index(drop=True)
        if len(c) < 24 * 20:
            continue
        tu = int(x["unlockTime"])
        tt, cc = c.t.values.astype(np.int64), c.c.values
        row = {"id": k, "symbol": x["kucoinSymbol"], "protocol": x["protocol"], "t_unlock": tu,
               "date": x["unlockTimeIso"][:10], "pct_circ": x.get("pctOfCirculating") or np.nan,
               "group": x.get("mainGroup"), "shares": x.get("groupShares") or {}}
        row["insider_share"] = sum(v for g, v in row["shares"].items() if g in INSIDER_GROUPS)
        for d in DAYS:
            row[f"p{d}"] = price_at(tt, cc, tu + d * DAY, 2 * H)
            row[f"b{d}"] = price_at(btc_t, btc_c, tu + d * DAY, 5 * H)
        f = pd.DataFrame(x.get("funding") or [])
        series[k] = (c, f)
        rows.append(row)
    ev = pd.DataFrame(rows)
    # ricorrente = c'è stato un altro sblocco dello stesso token nei 45 giorni prima
    ev = ev.sort_values(["symbol", "t_unlock"])
    prev = ev.groupby("symbol").t_unlock.shift(1)
    ev["recurring"] = (ev.t_unlock - prev) <= 45 * DAY
    ev["insider"] = ev.insider_share >= 0.5
    ev["size"] = pd.cut(ev.pct_circ * 100, [0, 2, 5, 10, 1000], labels=["1-2%", "2-5%", "5-10%", ">10%"])
    return ev.sort_values("t_unlock").reset_index(drop=True), series


def windows(ev: pd.DataFrame) -> pd.DataFrame:
    """Rendimenti grezzi e al netto di BTC (beta 1) su finestre standard."""
    w = {"pre30_to_-1": (-30, -1), "pre14_to_-1": (-14, -1), "pre7_to_0": (-7, 0),
         "post0_to_7": (0, 7), "post0_to_14": (0, 14), "post0_to_29": (0, 29)}
    out = ev.copy()
    for name, (a, b) in w.items():
        r = ev[f"p{b}"] / ev[f"p{a}"] - 1
        rb = ev[f"b{b}"] / ev[f"b{a}"] - 1
        out[f"raw_{name}"] = r
        out[f"abn_{name}"] = r - rb
    return out


def summarize(df: pd.DataFrame, cols: list[str]) -> dict:
    out = {"n": int(len(df))}
    for c in cols:
        s = df[c].dropna()
        if len(s) < 5:
            continue
        se = s.std() / np.sqrt(len(s))
        out[c] = {"median": float(s.median()), "mean": float(s.mean()), "t_mean": float(s.mean() / se) if se else 0,
                  "share_neg": float((s < 0).mean()), "n": int(len(s))}
    return out


def short_trade(ev_row, c: pd.DataFrame, f: pd.DataFrame, btc: pd.Series, entry_d: int, exit_d: int,
                stop: float | None, hedge: bool) -> dict | None:
    tu = ev_row.t_unlock
    t_in, t_out = tu + entry_d * DAY, tu + exit_d * DAY
    tt = c.t.values.astype(np.int64)
    i_in = int(np.searchsorted(tt, t_in, side="left")) - 1
    i_out = int(np.searchsorted(tt, t_out, side="left")) - 1
    if i_in < 0 or i_out <= i_in or t_in - tt[i_in] > 2 * H or t_out - tt[i_out] > 2 * H:
        return None
    entry, exit_px, reason, t_exit = c.c.iloc[i_in], c.c.iloc[i_out], "time", t_out
    if stop is not None:
        path = c.iloc[i_in + 1: i_out + 1]
        hit = path[path.h >= entry * (1 + stop)]
        if len(hit):
            bar = hit.iloc[0]
            exit_px, reason, t_exit = max(entry * (1 + stop), bar.o), "stop", int(bar.t + H)
    e_fill = entry * (1 - SLIP)
    x_fill = exit_px * (1 + SLIP * (2 if reason == "stop" else 1))
    gross = (e_fill - x_fill) / e_fill
    fees = TAKER * (1 + x_fill / e_fill)
    fund = 0.0
    if not f.empty:
        m = (f.timepoint > t_in) & (f.timepoint <= t_exit)
        fund = float(pd.to_numeric(f.loc[m, "fundingRate"], errors="coerce").fillna(0).sum())
    net = gross - fees + fund
    if hedge:  # long BTC per lo stesso nozionale: elimina il movimento di mercato
        b_in = btc.asof(t_in)
        b_out = btc.asof(t_exit)
        if not (np.isfinite(b_in) and np.isfinite(b_out)):
            return None
        net += (b_out / b_in - 1) - 2 * (TAKER + 0.0005)
    return {"id": ev_row.id, "t": tu, "net": net, "gross": gross, "funding": fund, "reason": reason}


def stats(r: pd.Series) -> dict:
    if len(r) < 5:
        return {"n": int(len(r))}
    g, l = r[r > 0].sum(), -r[r < 0].sum()
    return {"n": int(len(r)), "mean": float(r.mean()), "median": float(r.median()), "win": float((r > 0).mean()),
            "pf": float(g / l) if l > 0 else np.inf, "worst": float(r.min()),
            "t_mean": float(r.mean() / (r.std() / np.sqrt(len(r)))) if r.std() > 0 else 0.0}


def main(unlocks_path: str, history_path: str, out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    btc = load_btc(history_path)
    ev, series = build_events(unlocks_path, btc)
    ev = windows(ev)
    ev.drop(columns=["shares"]).to_csv(out / "unlock_events.csv", index=False)
    cols = [c for c in ev.columns if c.startswith("abn_") or c.startswith("raw_")]

    groups = {
        "tutti": ev,
        "insider (team/investitori >=50%)": ev[ev.insider],
        "non insider": ev[~ev.insider],
        "isolati (primo dopo 45g)": ev[~ev.recurring],
        "ricorrenti": ev[ev.recurring],
        "insider + isolati": ev[ev.insider & ~ev.recurring],
        "insider + >=5% circolante": ev[ev.insider & (ev.pct_circ >= 0.05)],
    }
    for lab in ev["size"].cat.categories:
        groups[f"dimensione {lab}"] = ev[ev["size"] == lab]
    event_summary = {k: summarize(v, cols) for k, v in groups.items()}

    # --- regole short: scelte sulla prima metà degli eventi, verificate sulla seconda
    split_t = ev.t_unlock.median()
    filters = {
        "tutti": lambda e: e,
        "insider": lambda e: e[e.insider],
        "insider_isolati": lambda e: e[e.insider & ~e.recurring],
        "insider_>=5%": lambda e: e[e.insider & (e.pct_circ >= 0.05)],
        ">=5%": lambda e: e[e.pct_circ >= 0.05],
    }
    grid, trades = [], {}
    for fname, fil in filters.items():
        sub = fil(ev)
        for entry_d in (-14, -7, -3, -1):
            for exit_d in (0, 3, 7, 14):
                for stop in (None, 0.2, 0.4):
                    for hedge in (False, True):
                        tr = []
                        for _, row in sub.iterrows():
                            c, f = series[row.id]
                            t = short_trade(row, c, f, btc, entry_d, exit_d, stop, hedge)
                            if t:
                                tr.append(t)
                        tdf = pd.DataFrame(tr)
                        if len(tdf) < 10:
                            continue
                        key = (fname, entry_d, exit_d, stop, hedge)
                        trades[key] = tdf
                        rec = {"filter": fname, "entry_d": entry_d, "exit_d": exit_d,
                               "stop": "none" if stop is None else stop, "hedge_btc": hedge}
                        for tag, r in (("all", tdf.net), ("ins", tdf[tdf.t < split_t].net),
                                       ("oos", tdf[tdf.t >= split_t].net)):
                            for k, v in stats(r).items():
                                rec[f"{tag}_{k}"] = v
                        grid.append(rec)
    grid = pd.DataFrame(grid)
    grid.to_csv(out / "unlock_rules.csv", index=False)
    ranked = grid[grid.ins_n >= 15].sort_values("ins_mean", ascending=False)
    best = ranked.iloc[0]

    summary = {
        "events": int(len(ev)), "symbols": int(ev.symbol.nunique()),
        "range": [ev.date.min(), ev.date.max()], "split": pd.to_datetime(split_t, unit="ms").date().isoformat(),
        "event_summary": event_summary,
        "rules_tested": int(len(grid)),
        "share_rules_oos_positive": float((grid.oos_mean > 0).mean()),
        "best_in_sample": {k: (float(v) if isinstance(v, (np.floating, np.integer)) else v) for k, v in best.items()},
        "top5_in_sample": ranked.head(5)[["filter", "entry_d", "exit_d", "stop", "hedge_btc", "ins_n", "ins_mean",
                                          "oos_n", "oos_mean", "oos_pf", "oos_t_mean"]].to_dict("records"),
    }
    json.dump(summary, open(out / "summary.json", "w"), indent=2, default=float)
    print(json.dumps(summary, indent=1, default=float))


if __name__ == "__main__":
    main(*sys.argv[1:4])
