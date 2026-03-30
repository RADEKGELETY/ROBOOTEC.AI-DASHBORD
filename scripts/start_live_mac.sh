#!/bin/bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env.live"

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE"
  echo "Create it with:"
  echo "ALPACA_API_KEY_ID=YOUR_KEY"
  echo "ALPACA_API_SECRET_KEY=YOUR_SECRET"
  echo "ALPACA_TRADE_BASE_URL=https://paper-api.alpaca.markets"
  exit 1
fi

# Load env
set -a
source "$ENV_FILE"
set +a

cd "$ROOT_DIR"

# Start live worker in background
PYTHONPATH=. python3 apps/worker/live/worker.py &
WORKER_PID=$!

cleanup() {
  echo ""
  echo "Stopping live worker..."
  kill "$WORKER_PID" >/dev/null 2>&1 || true
  exit 0
}
trap cleanup INT TERM

echo "Live worker started (PID $WORKER_PID)."
echo "Publishing live dashboard every 30 seconds. Press Ctrl+C to stop."

LIVE_JSON="$ROOT_DIR/docs/data/live.json"

while true; do
  PYTHONPATH=. python3 scripts/publish_live_dashboard.py
  if [ -f "$LIVE_JSON" ]; then
    echo "OK: live.json updated at $LIVE_JSON"
  fi
  sleep 30
done
