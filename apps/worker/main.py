import argparse
from datetime import datetime, timedelta

from apps.worker.providers.alpaca import fetch_stock_bars, fetch_crypto_orderbooks
from apps.worker.providers.fred import fetch_series_observations
from apps.worker.providers.ecb import fetch_series
from apps.worker.providers.gdelt import fetch_latest_gkg_url
from apps.worker.data.csv_loader import load_ohlcv_csv
from apps.worker.data.stooq_cache import save_symbol_csv
from apps.worker.backtest.engine import run_backtest
from apps.worker.backtest.strategies import get_strategy, get_all_strategies
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


def main():
    parser = argparse.ArgumentParser(description="RoboOtec MVP worker")
    parser.add_argument("--task", required=True, choices=["market", "macro", "news", "crypto", "backtest", "download", "backtest_batch"])
    parser.add_argument("--symbols", default="AAPL")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--timeframe", default="1Day")
    parser.add_argument("--feed", default="iex")
    parser.add_argument("--crypto_symbols", default="BTC/USD")
    parser.add_argument("--csv", help="Path to OHLCV CSV with columns: date,open,high,low,close,volume")
    parser.add_argument("--strategy", default="sma_cross_10_30")
    parser.add_argument("--strategies", default="all")
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
            interval = markets.get("interval", "d")
            symbol = args.download_symbol or "aapl.us"
            cached = save_symbol_csv(symbol, interval=interval)
            args.csv = str(cached)
        candles = filter_last_months(load_ohlcv_csv(args.csv), months=6)
        start, end, days = period_info(candles)
        initial_cash = 100000.0
        strategy = get_strategy(args.strategy)
        metrics, trades = run_backtest(candles, strategy, initial_cash=initial_cash)
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
        interval = markets.get("interval", "d")
        if args.batch_symbols:
            symbols = [s.strip() for s in args.batch_symbols.split(",") if s.strip()]
        else:
            symbols = []
            for group in markets.get("symbols", {}).values():
                symbols.extend(group)

        if args.strategies == "all":
            strategies = get_all_strategies()
        else:
            strategies = [get_strategy(s.strip()) for s in args.strategies.split(",") if s.strip()]

        all_backtests = []
        initial_cash = 100000.0

        for sym in symbols:
            csv_path = save_symbol_csv(sym, interval=interval)
            candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
            start, end, days = period_info(candles)
            for strat in strategies:
                metrics, trades = run_backtest(candles, strat, initial_cash=initial_cash)
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
