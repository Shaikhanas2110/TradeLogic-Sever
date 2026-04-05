from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from tradingview_ta import TA_Handler, Interval
from typing import Dict, Any
import uvicorn
from datetime import datetime
import yfinance as yf
import pandas as pd
import numpy as np
from demo1 import backtest_rsi_trend_confluence
from flask import Flask, request, jsonify

app = FastAPI(title="TradingView Strategy Engine")

# Enable CORS for Flutter
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or specify your frontend URL, e.g. ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# STRATEGY LOGIC (Same as before, but optimized)
# =============================================================================


class StrategyRequest(BaseModel):
    symbol: str
    exchange: str = "NSE"
    interval: str = "15m"
    strategy: str = "RSI Fibonacci"
    params: Dict[str, Any] = {}


# All strategies here...
STRATEGIES = {
    "RSI Overbought/Oversold": lambda ind, p: (
        (
            "🟢 BUY"
            if ind.get("RSI", 50) < p.get("oversold", 30)
            else (
                "🔴 SELL" if ind.get("RSI", 50) > p.get("overbought", 70) else "⚪ HOLD"
            )
        ),
        f"RSI: {ind.get('RSI', 0):.1f}",
        70,
    ),
    "MACD Crossover": lambda ind, p: (
        (
            "🟢 BUY"
            if ind.get("MACD.macd", 0) > ind.get("MACD.signal", 0)
            else (
                "🔴 SELL"
                if ind.get("MACD.macd", 0) < ind.get("MACD.signal", 0)
                else "⚪ HOLD"
            )
        ),
        f"MACD: {ind.get('MACD.macd', 0):.4f}",
        65,
    ),
    "EMA Crossover": lambda ind, p: (
        (
            "🟢 BUY"
            if ind.get(f"EMA{p.get('fast', 20)}", 0)
            > ind.get(f"EMA{p.get('slow', 50)}", 0)
            else "🔴 SELL"
        ),
        f"EMA crossover",
        60,
    ),
    "RSI Fibonacci": lambda ind, p: strategy_rsi_fibonacci(ind, p),
    "Multi-Indicator": lambda ind, p: strategy_multi_indicator(ind, p),
}


def strategy_rsi_fibonacci(ind, params):
    rsi = ind.get("RSI", 50)
    rsi_prev = ind.get("RSI[1]", 50)
    ema20 = ind.get("EMA20", 0)
    ema50 = ind.get("EMA50", 0)
    close = ind.get("close", 0)
    adx = ind.get("ADX", 0)

    if not all([close, ema20, ema50]):
        return "⚪ HOLD", "No data", 0

    strong_up = ema20 > ema50 and close > ema20
    rsi_rising = rsi > rsi_prev if rsi_prev else False

    sensitivity = params.get("sensitivity", "Medium")

    if sensitivity == "Low":
        if rsi < 35 and ema20 > ema50:
            return "🟢 BUY", f"RSI({rsi:.1f}) oversold + uptrend", 55
        if rsi > 65 and ema20 < ema50:
            return "🔴 SELL", f"RSI({rsi:.1f}) overbought + downtrend", 55

    elif sensitivity == "Medium":
        if rsi < 30 and rsi_rising and strong_up:
            return "🟢 BUY", f"RSI({rsi:.1f}) bounce + strong uptrend", 75
        if rsi > 70 and rsi < rsi_prev and ema20 < ema50 and close < ema20:
            return "🔴 SELL", f"RSI({rsi:.1f}) drop + strong downtrend", 75

    else:  # High
        if rsi < 25 and rsi_rising and strong_up and adx > 25:
            return "🟢 BUY", f"RSI({rsi:.1f}) extreme + ADX({adx:.0f})", 90
        if rsi > 75 and rsi < rsi_prev and ema20 < ema50 and adx > 25:
            return "🔴 SELL", f"RSI({rsi:.1f}) extreme + ADX({adx:.0f})", 90

    return "⚪ HOLD", f"RSI: {rsi:.1f}", 0


def strategy_multi_indicator(ind, params):
    score = 0
    rsi = ind.get("RSI", 50)
    macd = ind.get("MACD.macd", 0)
    macd_sig = ind.get("MACD.signal", 0)
    ema20 = ind.get("EMA20", 0)
    ema50 = ind.get("EMA50", 0)
    stoch_k = ind.get("Stoch.K", 50)
    adx = ind.get("ADX", 0)

    if rsi < 30:
        score += 2
    elif rsi < 40:
        score += 1
    elif rsi > 70:
        score -= 2
    elif rsi > 60:
        score -= 1

    if macd and macd_sig:
        if macd > macd_sig:
            score += 1
        else:
            score -= 1

    if ema20 and ema50:
        if ema20 > ema50:
            score += 1
        else:
            score -= 1

    if stoch_k < 20:
        score += 1
    elif stoch_k > 80:
        score -= 1

    if adx and adx > 25:
        if ind.get("ADX+DI", 0) > ind.get("ADX-DI", 0):
            score += 1
        else:
            score -= 1

    if score >= 4:
        return "🟢 STRONG BUY", f"Score: {score}", 85
    elif score >= 2:
        return "🟢 BUY", f"Score: {score}", 65
    elif score <= -4:
        return "🔴 STRONG SELL", f"Score: {score}", 85
    elif score <= -2:
        return "🔴 SELL", f"Score: {score}", 65
    return "⚪ HOLD", f"Score: {score}", 0


def bt_rsi_fibonacci_flow(row, prev_row, params):
    """
    EXACT 1:1 PORT OF YOUR RSI FIBONACCI FLOW PINE SCRIPT
    No changes, no approximations.
    """

    # Extract values
    close = row["close"]
    high = row["high"]
    low = row["low"]

    rsi = row["RSI"]
    rsi_prev = prev_row["RSI"]

    swing_high = row["swing_high"]
    swing_low = row["swing_low"]
    fib_trend_up = row["fib_trend_up"]

    price_range = abs(swing_high - swing_low)

    # Calculate Fib levels exactly like Pine Script
    if fib_trend_up:
        fib382 = swing_low + price_range * 0.382
        fib50 = swing_low + price_range * 0.5
        fib618 = swing_low + price_range * 0.618
    else:
        fib382 = swing_high - price_range * 0.382
        fib50 = swing_high - price_range * 0.5
        fib618 = swing_high - price_range * 0.618

    # RSI Conditions
    rsi_oversold = rsi < 30
    rsi_overbought = rsi > 70
    rsi_rising = rsi > rsi_prev
    rsi_falling = rsi < rsi_prev

    # Price Zone Conditions
    in_entry_zone = (close >= fib382) and (close <= fib618)

    # Confluence Signal - EXACT SAME AS PINE SCRIPT
    bull_confluence = rsi_oversold and in_entry_zone and fib_trend_up
    bear_confluence = rsi_overbought and in_entry_zone and not fib_trend_up

    strong_bull = bull_confluence and rsi_rising
    strong_bear = bear_confluence and rsi_falling

    # Return Signal
    if strong_bull:
        return "BUY"
    elif strong_bear:
        return "SELL"

    return "HOLD"


# =============================================================================
# API ENDPOINTS
# =============================================================================


@app.post("/analyze")
async def analyze_stock(request: StrategyRequest):
    try:
        # Map interval string to TradingView interval
        interval_map = {
            "1m": Interval.INTERVAL_1_MINUTE,
            "5m": Interval.INTERVAL_5_MINUTES,
            "15m": Interval.INTERVAL_15_MINUTES,
            "30m": Interval.INTERVAL_30_MINUTES,
            "1h": Interval.INTERVAL_1_HOUR,
            "4h": Interval.INTERVAL_4_HOURS,
            "1d": Interval.INTERVAL_1_DAY,
        }

        handler = TA_Handler(
            symbol=request.symbol,
            screener=(
                "india"
                if request.exchange in ["NSE", "BSE"]
                else "crypto" if request.exchange == "BINANCE" else "america"
            ),
            exchange=request.exchange,
            interval=interval_map.get(request.interval, Interval.INTERVAL_15_MINUTES),
        )

        analysis = handler.get_analysis()
        ind = analysis.indicators

        # Run strategy
        signal, reason, confidence = STRATEGIES[request.strategy](ind, request.params)

        return {
            "success": True,
            "symbol": request.symbol,
            "signal": signal,
            "reason": reason,
            "confidence": confidence,
            "current_price": ind.get("close", 0),
            "indicators": {
                "RSI": ind.get("RSI", 0),
                "EMA20": ind.get("EMA20", 0),
                "EMA50": ind.get("EMA50", 0),
                "MACD": ind.get("MACD.macd", 0),
                "ADX": ind.get("ADX", 0),
            },
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/strategies")
async def get_strategies():
    """Return list of available strategies"""
    return {"strategies": list(STRATEGIES.keys()), "count": len(STRATEGIES)}


@app.get("/")
async def root():
    return {"message": "TradingView Strategy API is running"}


# =============================================================================
# RUN SERVER
# =============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
