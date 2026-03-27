# RoboOtec.ai – MVP (Public Data + Free Tools)

Tento repozitář obsahuje MVP architekturu a první vývojový scaffold pro plně autonomní prediktivní AI trading platformu. MVP je **striktně postavené na veřejných datech a bezplatných nástrojích**.

## Rychlý přehled
- `robootec_mvp_architecture.md` – architektura
- `robootec_mvp_architecture.html` – čitelný online náhled
- `apps/api` – API (FastAPI)
- `apps/worker` – ingest + backtest pipeline (worker)
- `apps/dashboard` – jednoduchý HTML dashboard (přes API endpoint)
- `config` – konfigurace datových zdrojů
- `infra` – infrastruktura (DB/Redis)

## Lokální náhled architektury
Spusť lokální server:

```bash
python3 -m http.server 8000
```

Otevři:
- `http://localhost:8000/robootec_mvp_architecture.html`

## Status
Základní scaffold je připraven. Další kroky:
1. Datové konektory (public/free)
2. Backtest engine
3. Risk a portfolio layer
4. Paper trading simulátor
5. Dashboard

## Public data ingest (Stooq)
Stooq poskytuje CSV download historických dat bez klíče.

```bash
# stáhne celý seznam symbolů z config/markets.json
python3 -m apps.worker.main --task download

# stáhne jeden symbol
python3 -m apps.worker.main --task download --download_symbol aapl.us
```

Poznámka: ověřte licenční podmínky Stooq pro komerční použití.

## Backtest (CSV)
CSV musí mít sloupce: `date,open,high,low,close,volume`

Příklad:
```bash
python3 -m apps.worker.main --task backtest --csv /path/to/data.csv --strategy sma_cross
```

Pokud `--csv` neuvedeš, backtest si stáhne data pro symbol (default `aapl.us`) ze Stooq.

## Dashboard
Spusť API:
```bash
uvicorn apps.api.main:app --reload
```

Otevři:
- `http://localhost:8000/dashboard`

## Bezpečnost a důvěrnost
Tato dokumentace je důvěrná. Nepublikovat mimo schválené prostředí.
