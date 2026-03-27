from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class Strategy(BaseModel):
    id: str
    name: str
    market: str
    timeframe: str
    budget_usd: float = Field(default=100000.0)
    risk_level: str = Field(default="balanced")


class Signal(BaseModel):
    id: str
    strategy_id: str
    symbol: str
    side: str  # BUY/SELL/NO_TRADE
    confidence: float
    timestamp: datetime
    entry: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class BacktestResult(BaseModel):
    id: str
    strategy_id: str
    symbol: str
    start: datetime
    end: datetime
    win_rate: float
    profit_factor: float
    max_drawdown: float
    sharpe: Optional[float] = None


class HealthStatus(BaseModel):
    status: str
    time: datetime
