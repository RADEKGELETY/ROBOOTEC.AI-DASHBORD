import argparse
from datetime import datetime, timedelta

from apps.worker.providers.alpaca import fetch_stock_bars, fetch_crypto_orderbooks
from apps.worker.providers.fred import fetch_series_observations
from apps.worker.providers.ecb import fetch_series
from apps.worker.providers.gdelt import fetch_latest_gkg_url
from apps.worker.data.csv_loader import load_ohlcv_csv
from apps.worker.data.stooq_cache import save_symbol_csv
from apps.worker.backtest.engine import run_backtest
from apps.worker.backtest.strategies import get_strategy
from apps.shared.storage import write_json
from apps.shared.config import load_markets


def main():
    parser = argparse.ArgumentParser(description="RoboOtec MVP worker")
    parser.add_argument("--task", required=True, choices=["market", "macro", "news", "crypto", "backtest", "download"])
    parser.add_argument("--symbols", default="AAPL")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--timeframe", default="1Day")
    parser.add_argument("--feed", default="iex")
    parser.add_argument("--crypto_symbols", default="BTC/USD")
    parser.add_argument("--csv", help="Path to OHLCV CSV with columns: date,open,high,low,close,volume")
    parser.add_argument("--strategy", default="sma_cross")
    parser.add_argument("--fred_series", default="CPIAUCSL")
    parser.add_argument("--ecb_flow", default="EXR")
    parser.add_argument("--ecb_key", default="D.USD.EUR.SP00.A")
    parser.add_argument("--download_symbol", help="Download a single symbol from Stooq")
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
            # Try to use cached Stooq CSV
            markets = load_markets()
            interval = markets.get("interval", "d")
            symbol = args.download_symbol or "aapl.us"
            cached = save_symbol_csv(symbol, interval=interval)
            args.csv = str(cached)
        candles = load_ohlcv_csv(args.csv)
        strategy = get_strategy(args.strategy)
        metrics, trades = run_backtest(candles, strategy)
        payload = {
            "strategy": strategy.name,
            "symbol": "CSV",
            "metrics": metrics.__dict__,
            "trades": [t.__dict__ for t in trades],
        }
        path = write_json("last_backtest.json", payload)
        print(f"Backtest saved to {path}")

    print("Done", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
