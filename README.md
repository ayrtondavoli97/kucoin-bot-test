# KuCoin Market Collector (privato)

Actor Apify che raccoglie solo dati **pubblici** di KuCoin Futures. Nessuna API key.

## Modalità
- `snapshot` — tutti i perpetual USDT attivi: mark/index/last price, funding attuale e previsto,
  open interest, volumi 24h. Rileva i nuovi listing. **Schedulare ogni 15 min.**
- `backfill` — per i contratti listati negli ultimi N giorni scarica candele dal giorno del
  listing + storico funding. **Lanciare a mano una volta.**

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
