# KuCoin Market Collector (privato)

Actor Apify che raccoglie solo dati **pubblici** di KuCoin Futures. Nessuna API key.

## Modalità
- `snapshot` — tutti i perpetual USDT attivi: mark/index/last price, funding attuale e previsto,
  open interest, volumi 24h. Rileva i nuovi listing. **Schedulare ogni 15 min.**
- `backfill` — per i contratti listati negli ultimi N giorni. Con `eventWindowDays` > 0 salva solo
  la finestra dopo il listing (candele fini nei primi giorni + orarie fino a fine finestra + funding)
  più BTC come riferimento di regime. Con `eventWindowDays` = 0 scarica tutto lo storico.
  **Lanciare a mano una volta.**

- `unlocks` — sblocchi di token da DefiLlama (dataset pubblici) incrociati con i perpetual KuCoin:
  per ogni sblocco ≥ soglia scarica candele orarie e funding prima/dopo. Gli sblocchi futuri vanno
  in `kucoin-upcoming-unlocks`.

- `history` — storico lungo (default 5 anni, candele 4h) + funding dei 24 perpetual principali,
  per il backtest trend following (`analysis/trend_study.py`).

- `paper_unlocks` — **paper trading** della strategia sblocchi (nessun ordine reale, nessuna API key).
  Short sul token da 14 giorni prima a 14 giorni dopo ogni sblocco >= 1% del circolante, coperto con
  long BTC, 4% dell'equity per posizione, max 25 posizioni, stop di emergenza a +100%.
  Stato in `kucoin-paper-unlocks` (STATE), trade chiusi in `kucoin-paper-unlocks-trades`,
  equity a ogni run in `kucoin-paper-unlocks-equity`. **Schedulare ogni ora.**

## Storage (named, persistente)
| Nome | Contenuto |
|---|---|
| `kucoin-futures-snapshots` | 1 item per run, campo `contracts` con tutti i perpetual |
| `kucoin-new-listings` | 1 item per ogni nuovo contratto rilevato |
| `kucoin-listing-backfill` | 1 item per contratto: `candles` [time, open, high, low, close, volume] + `funding` |
| `kucoin-collector-state` (KV) | simboli già visti |

## Deploy
```
apify login
apify push
```
Poi in Console: Schedules → nuovo schedule `*/15 * * * *` con input `{"mode": "snapshot"}`.
L'actor resta privato finché non lo pubblichi sullo Store.

## Export per l'analisi
Console → Storage → Datasets → dataset → Export (JSON o CSV), poi allegare il file in chat.

## Esecuzione in locale (Windows / PowerShell)
```
pip install -r requirements.txt
mkdir storage\key_value_stores\default -Force
# salva l'input JSON in storage\key_value_stores\default\INPUT.json
python -m src
python tools\export_local.py backfill.json
```
