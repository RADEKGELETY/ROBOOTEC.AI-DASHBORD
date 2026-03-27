import argparse
from datetime import datetime, timedelta, date
from pathlib import Path

from apps.worker.providers.alpaca import fetch_stock_bars, fetch_crypto_orderbooks
from apps.worker.providers.fred import fetch_series_observations
from apps.worker.providers.ecb import fetch_series
from apps.worker.providers.gdelt import fetch_latest_gkg_url
from apps.worker.data.csv_loader import load_ohlcv_csv
from apps.worker.data.stooq_cache import save_symbol_csv, cache_dir
from apps.worker.providers.alpha_vantage_intraday import download_equity_intraday_csv, download_fx_intraday_csv
from apps.worker.backtest.engine import run_backtest
from apps.worker.backtest.strategies import get_strategy, get_all_strategies, get_tuned_strategies
from apps.worker.backtest.predictor import HistoricalOutcomeFilter
from apps.shared.storage import write_json, read_json
from apps.shared.config import load_markets


def filter_last_months(candles, months=6):
    if not candles:
        return candles
    candles = sorted(candles, key=lambda c: c.timestamp)
    end = candles[-1].timestamp
    start = end - timedelta(days=30 * months)
    return [c for c in candles if c.timestamp >= start]


def period_info(candles):
    if not candles:
        return None, None, None
    candles = sorted(candles, key=lambda c: c.timestamp)
    start = candles[0].timestamp.date().isoformat()
    end = candles[-1].timestamp.date().isoformat()
    days = (candles[-1].timestamp - candles[0].timestamp).days
    return start, end, days


def _months_between(start: date, end: date) -> list[str]:
    months = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return months


def _save_alpha_intraday(symbol: str, interval: str, start: date, end: date, asset: str, output_symbol: str) -> Path:
    out = cache_dir() / f"{output_symbol}_{interval}.csv"
    rows = []
    if asset == "fx":
        from_symbol, to_symbol = symbol[:3].upper(), symbol[3:].upper()
        text = download_fx_intraday_csv(from_symbol, to_symbol, interval=interval)
        rows = text.splitlines()
    else:
        months = _months_between(start, end)
        for i, month in enumerate(months):
            text = download_equity_intraday_csv(symbol.upper(), interval=interval, month=month)
            lines = text.splitlines()
            if not lines:
                continue
            if i > 0 and lines:
                lines = lines[1:]
            rows.extend(lines)
    out.write_text("\n".join(rows), encoding="utf-8")
    return out


def _symbol_group(markets: dict, symbol: str) -> str | None:
    for group, symbols in markets.get("symbols", {}).items():
        if symbol in symbols:
            return group
    return None


def _resolve_provider(markets: dict, symbol: str) -> tuple[str, str, str]:
    group = _symbol_group(markets, symbol)
    providers = markets.get("providers", {})
    provider_cfg = providers.get(group, {})
    provider = provider_cfg.get("provider", markets.get("source", "stooq"))
    asset = provider_cfg.get("asset", "stock")
    symbol_map = markets.get("symbol_map", {})
    provider_symbol = symbol_map.get(symbol, symbol)
    return provider, asset, provider_symbol


def _normalize_interval(provider: str, interval: str) -> str:
    if provider == "alpha_vantage":
        if interval.endswith("m") and not interval.endswith("min"):
            return interval.replace("m", "min")
        if interval.endswith("h"):
            hours = int(interval[:-1])
            return f"{hours * 60}min"
    return interval


def main():
    parser = argparse.ArgumentParser(description="RoboOtec MVP worker")
    parser.add_argument("--task", required=True, choices=["market", "macro", "news", "news_calendar", "crypto", "backtest", "download", "backtest_batch"])
    parser.add_argument("--symbols", default="AAPL")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--timeframe", default="1Day")
    parser.add_argument("--feed", default="iex")
    parser.add_argument("--crypto_symbols", default="BTC/USD")
    parser.add_argument("--csv", help="Path to OHLCV CSV with columns: date,open,high,low,close,volume")
    parser.add_argument("--strategy", default="sma_cross_10_30")
    parser.add_argument("--strategies", default="all")
    parser.add_argument("--predict", default="on", choices=["on", "off"])
    parser.add_argument("--intraday", default="on", choices=["on", "off"])
    parser.add_argument("--interval")
    parser.add_argument("--keywords", help="Comma-separated keywords for news calendar")
    parser.add_argument("--fred_series", default="CPIAUCSL")
    parser.add_argument("--ecb_flow", default="EXR")
    parser.add_argument("--ecb_key", default="D.USD.EUR.SP00.A")
    parser.add_argument("--download_symbol", help="Download a single symbol from Stooq")
    parser.add_argument("--batch_symbols", help="Comma-separated symbol list for batch backtest")
    args = parser.parse_args()

    if args.task == "market":
        end = args.end or datetime.utcnow().isoformat() + "Z"
        start = args.start or (datetime.utcnow() - timedelta(days=30)).isoformat() + "Z"
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        data = fetch_stock_bars(symbols, start=start, end=end, timeframe=args.timeframe, feed=args.feed)
        print(f"Fetched stock bars for {symbols}: keys={list(data.keys())}")

    if args.task == "macro":
        fred = fetch_series_observations(args.fred_series)
        ecb = fetch_series(args.ecb_flow, args.ecb_key)
        print(f"Fetched FRED series {args.fred_series}: {len(fred.get('observations', []))} points")
        print(f"Fetched ECB series {args.ecb_flow}/{args.ecb_key}: {len(ecb.splitlines())} lines")

    if args.task == "news":
        url = fetch_latest_gkg_url()
        print(f"Latest GDELT GKG file: {url}")

    if args.task == "news_calendar":
        from apps.worker.news.red_news import build_and_save_red_news

        start = date.fromisoformat(args.start) if args.start else (date.today() - timedelta(days=180))
        end = date.fromisoformat(args.end) if args.end else date.today()
        keywords = [k.strip() for k in (args.keywords or "TRUMP,FED,POWELL,FOMC").split(",") if k.strip()]
        out = build_and_save_red_news(start, end, keywords)
        print(f"Red news calendar saved: {out}")

    if args.task == "crypto":
        symbols = [s.strip() for s in args.crypto_symbols.split(",") if s.strip()]
        data = fetch_crypto_orderbooks(symbols)
        print(f"Fetched crypto orderbooks for {symbols}: keys={list(data.keys())}")

    if args.task == "download":
        markets = load_markets()
        interval = markets.get("interval", "d")
        if args.download_symbol:
            out = save_symbol_csv(args.download_symbol, interval=interval)
            print(f"Downloaded {args.download_symbol} to {out}")
        else:
            symbols = []
            for group in markets.get("symbols", {}).values():
                symbols.extend(group)
            for sym in symbols:
                out = save_symbol_csv(sym, interval=interval)
                print(f"Downloaded {sym} to {out}")

    if args.task == "backtest":
        if not args.csv:
            markets = load_markets()
            interval = args.interval or (markets.get("intraday_interval") if args.intraday == "on" else markets.get("interval", "d"))
            symbol = args.download_symbol or "aapl.us"
            provider, asset, provider_symbol = _resolve_provider(markets, symbol)
            if interval == markets.get("interval", "d"):
                provider = markets.get("source", "stooq")
                asset = "stock"
                provider_symbol = symbol
            interval = _normalize_interval(provider, interval)
            end_dt = datetime.fromisoformat(args.end) if args.end else datetime.utcnow()
            start_dt = datetime.fromisoformat(args.start) if args.start else end_dt - timedelta(days=180)
            if provider == "alpha_vantage" and interval.endswith("min"):
                cached = _save_alpha_intraday(provider_symbol, interval, start_dt.date(), end_dt.date(), asset, symbol)
            else:
                cached = save_symbol_csv(symbol, interval=interval, start=start_dt, end=end_dt, provider=provider, asset=asset)
            args.csv = str(cached)
        candles = filter_last_months(load_ohlcv_csv(args.csv), months=6)
        start, end, days = period_info(candles)
        initial_cash = 100000.0
        strategy = get_strategy(args.strategy)
        predictor = HistoricalOutcomeFilter() if args.predict == "on" else None
        metrics, trades = run_backtest(candles, strategy, initial_cash=initial_cash, predictor=predictor)
        payload = {
            "strategy": strategy.name,
            "symbol": args.download_symbol or "CSV",
            "initial_cash": initial_cash,
            "start": start,
            "end": end,
            "period_days": days,
            "metrics": metrics.__dict__,
            "trades": [t.__dict__ for t in trades],
        }
        path = write_json("last_backtest.json", payload)
        all_backtests = read_json("backtests.json", default=[])
        all_backtests.append(payload)
        write_json("backtests.json", all_backtests)
        print(f"Backtest saved to {path}")

    if args.task == "backtest_batch":
        markets = load_markets()
        interval = args.interval or (markets.get("intraday_interval") if args.intraday == "on" else markets.get("interval", "d"))
        rg_map = {
            "rg_nasdaq": "nasdaq_top10",
            "rg_gold": "commodities",
            "rg_btc": "crypto",
            "rg_eurusd": "fx",
        }
        if args.batch_symbols:
            symbols = [s.strip() for s in args.batch_symbols.split(",") if s.strip()]
        else:
            symbols = []
            for group in markets.get("symbols", {}).values():
                symbols.extend(group)

        if args.strategies == "all":
            strategies = get_all_strategies()
        elif args.strategies == "tuned":
            strategies = get_tuned_strategies()
        else:
            strategies = [get_strategy(s.strip()) for s in args.strategies.split(",") if s.strip()]

        all_backtests = []
        initial_cash = 100000.0

        end_dt = datetime.fromisoformat(args.end) if args.end else datetime.utcnow()
        start_dt = datetime.fromisoformat(args.start) if args.start else end_dt - timedelta(days=180)

        for sym in symbols:
            provider, asset, provider_symbol = _resolve_provider(markets, sym)
            if interval == markets.get("interval", "d"):
                provider = markets.get("source", "stooq")
                asset = "stock"
                provider_symbol = sym
            interval_use = _normalize_interval(provider, interval)
            if provider == "alpha_vantage" and interval_use.endswith("min"):
                csv_path = _save_alpha_intraday(provider_symbol, interval_use, start_dt.date(), end_dt.date(), asset, sym)
            else:
                csv_path = save_symbol_csv(sym, interval=interval_use, start=start_dt, end=end_dt, provider=provider, asset=asset)
            candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
            start, end, days = period_info(candles)
            for strat in strategies:
                if strat.name in rg_map:
                    allowed_group = rg_map[strat.name]
                    allowed_symbols = markets.get("symbols", {}).get(allowed_group, [])
                    if sym not in allowed_symbols:
                        continue
                predictor = HistoricalOutcomeFilter() if args.predict == "on" else None
                metrics, trades = run_backtest(candles, strat, initial_cash=initial_cash, predictor=predictor)
                payload = {
                    "strategy": strat.name,
                    "symbol": sym,
                    "initial_cash": initial_cash,
                    "start": start,
                    "end": end,
                    "period_days": days,
                    "metrics": metrics.__dict__,
                    "trades": [t.__dict__ for t in trades],
                }
                all_backtests.append(payload)
                write_json("last_backtest.json", payload)
                print(f"Backtest {sym} / {strat.name} done")

        write_json("backtests.json", all_backtests)
        print(f"Batch backtest saved ({len(all_backtests)} total)")

    print("Done", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
