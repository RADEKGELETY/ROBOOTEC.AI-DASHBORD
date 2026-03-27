from fastapi import FastAPI
from datetime import datetime
from typing import List

from apps.shared.models import Strategy, Signal, BacktestResult, HealthStatus
from apps.shared.storage import read_json

app = FastAPI(title="RoboOtec.ai MVP API", version="0.1.0")


@app.get("/health", response_model=HealthStatus)
def health():
    return HealthStatus(status="ok", time=datetime.utcnow())


@app.get("/strategies", response_model=List[Strategy])
def list_strategies():
    return [
        Strategy(id="strat-001", name="Trend Following", market="NASDAQ", timeframe="1h"),
        Strategy(id="strat-002", name="Mean Reversion", market="EUR/USD", timeframe="15m"),
    ]


@app.get("/signals", response_model=List[Signal])
def list_signals():
    now = datetime.utcnow()
    return [
        Signal(
            id="sig-001",
            strategy_id="strat-001",
            symbol="AAPL",
            side="BUY",
            confidence=0.67,
            timestamp=now,
            entry=185.4,
            stop_loss=181.2,
            take_profit=194.0,
        )
    ]


@app.get("/backtests", response_model=List[BacktestResult])
def list_backtests():
    return [
        BacktestResult(
            id="bt-001",
            strategy_id="strat-001",
            symbol="AAPL",
            start=datetime(2025, 9, 1),
            end=datetime(2026, 3, 1),
            win_rate=0.62,
            profit_factor=1.55,
            max_drawdown=0.11,
            sharpe=1.2,
        )
    ]

@app.get("/backtests/latest")
def latest_backtest():
    data = read_json("last_backtest.json", default={})
    return data


@app.get("/dashboard")
def dashboard():
    # Minimal HTML dashboard (no external deps)
    html = """
    <!doctype html>
    <html lang="cs">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>RoboOtec MVP Dashboard</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 24px; background: #0f1115; color: #e8edf4; }
        .card { background: #151a21; padding: 16px; border-radius: 12px; margin-bottom: 16px; }
        h1, h2 { margin: 0 0 12px; }
        code { background: #0b1322; padding: 2px 6px; border-radius: 6px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
      </style>
    </head>
    <body>
      <h1>RoboOtec MVP Dashboard</h1>
      <div class="card">
        <h2>Latest Backtest</h2>
        <div id="backtest">Loading...</div>
      </div>
      <script>
        fetch('/backtests/latest').then(r => r.json()).then(data => {
          if (!data || !data.metrics) {
            document.getElementById('backtest').innerText = 'No backtest yet.';
            return;
          }
          const m = data.metrics;
          document.getElementById('backtest').innerHTML = `
            <div><strong>Strategy:</strong> ${data.strategy}</div>
            <div class=\"grid\">
              <div>Win rate: ${ (m.win_rate * 100).toFixed(2) }%</div>
              <div>Profit factor: ${ m.profit_factor.toFixed(2) }</div>
              <div>Max drawdown: ${ (m.max_drawdown * 100).toFixed(2) }%</div>
              <div>Total return: ${ (m.total_return * 100).toFixed(2) }%</div>
            </div>
          `;
        });
      </script>
    </body>
    </html>
    """
    return html


@app.post("/ingest/run")
def run_ingest():
    # Placeholder: trigger worker pipeline
    return {"status": "accepted", "time": datetime.utcnow().isoformat()}
