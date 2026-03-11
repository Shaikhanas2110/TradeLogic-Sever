import json
from flask import Flask, Response, request, jsonify
import threading
import time
from flask_cors import CORS
import upstox_client
from Strategy_option_websocket_n7 import get_ltp, get_ltp_price
import lux_osc_matrix
from helper_upstox import placeOrder
import pandas as pd
import numpy as np
import requests
import os
import sys
import pytz
import time
from datetime import datetime, date, timedelta, time as dt_time
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta, time
import time as time_module
import math
import upstox_login

IST = pytz.timezone("Asia/Kolkata")

minute_data = {}

app = Flask(__name__)
CORS(app)
algo_states = {}
algo_threads = {}
last_signal = {}
trade_logs = []
lux_signals = {}
portfolio = {}
df = pd.read_csv("111.csv")
SYMBOL_MAP = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "RELIANCE": "NSE_EQ|INE002A01018",
}
TOKEN_FILE = "upstox_access_token.txt"


# =========================
# CORE LOGIC
# =========================


def fetch_raw_candles(instrument_key):
    access_token = get_upstox_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    now = datetime.now()
    market_open_time = time(9, 15)
    market_close_time = time(15, 30)
    today = now.date()

    if market_open_time <= now.time() <= market_close_time and today.weekday() < 5:
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
        response = requests.get(url, headers=headers)
        data = response.json()
        return data.get("data", {}).get("candles", [])

    # historical fallback
    chart_date = today
    if today.weekday() == 5:
        chart_date -= timedelta(days=1)
    elif today.weekday() == 6:
        chart_date -= timedelta(days=2)
    elif now.time() < market_open_time:
        chart_date -= timedelta(days=1)

    while True:
        date_str = chart_date.strftime("%Y-%m-%d")
        url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{date_str}/{date_str}"
        response = requests.get(url, headers=headers)
        data = response.json()
        raw = data.get("data", {}).get("candles", [])
        if raw:
            return raw
        chart_date -= timedelta(days=1)


# def get_upstox_token():
#     if not os.path.exists(TOKEN_FILE):
#         raise Exception("Access token file not found. Run login script first.")
#     with open(TOKEN_FILE, "r") as f:
#         token = f.read().strip()
#     if not token:
#         raise Exception("Access token is empty.")
#     return token

def get_upstox_token():
    # 1. Try environment variable first (Railway)
    token = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
    if token:
        return token
    
    # 2. Fallback to file (local dev)
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            token = f.read().strip()
        if token:
            return token
    
    raise Exception("No access token found. Set UPSTOX_ACCESS_TOKEN env variable.")


def convert_to_dataframe(raw):
    if not raw:
        return None

    df = pd.DataFrame(raw)

    df = df.iloc[:, :6]
    df.columns = ["time", "open", "high", "low", "close", "volume"]

    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True)

    return df


def process_candles(raw_candles):

    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)

    chart_result = []
    lux_result = []

    for candle in raw_candles:
        t, o, h, l, c, v = candle[:6]

        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        dt = dt.astimezone()

        candle_time = dt.time()

        if MARKET_OPEN <= candle_time <= MARKET_CLOSE:

            # ⭐ Chart format (OLD)
            chart_result.append({"time": dt.strftime("%H:%M"), "price": c})

            # ⭐ Lux format (OHLC)
            lux_result.append(
                {
                    "time": dt.strftime("%H:%M"),
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                }
            )

    chart_result = sorted(chart_result, key=lambda x: x["time"])
    lux_result = sorted(lux_result, key=lambda x: x["time"])

    # ⭐ Return BOTH
    return jsonify({"chart": chart_result, "ohlc": lux_result})


def get_chart_date():
    now = datetime.now()
    today = now.date()

    market_open = now.replace(hour=9, minute=0, second=0)
    market_close = now.replace(hour=15, minute=30, second=0)

    # Weekend
    if today.weekday() == 5:
        return today - timedelta(days=1)
    elif today.weekday() == 6:
        return today - timedelta(days=2)

    # Before market open
    if now < market_open:
        return today - timedelta(days=1)

    # After market close
    if now > market_close:
        return today

    # Market hours
    return today


def get_last_trading_day():
    today = datetime.now().date()

    # If today is Saturday (5) or Sunday (6)
    if today.weekday() == 5:  # Saturday
        return today - timedelta(days=1)  # Friday
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)  # Friday
    else:
        return today


def exit_trade(symbol, ltp, reason):
    state = algo_states[symbol]
    rule = state["rule"]

    placeOrder(
        inst=symbol,
        t_type="SELL",
        qty=rule["quantity"],
        order_type="MARKET",
        price=0,
        variety="regular",
        papertrading=1,
    )

    engine.update_portfolio_sell(symbol, rule["quantity"], ltp)

    state["running"] = False

    trade_logs.append(
        {
            "symbol": symbol,
            "type": "SELL",
            "price": price,
            "time": time.time(),
            "reason": reason,
        }
    )

    print(symbol, reason)


def get_all_equity_stocks():
    stocks = df[(df["exchange"] == "NSE_EQ") & (df["instrument_type"] == "EQUITY")]
    result = []
    for _, row in stocks.iterrows():
        result.append(
            {
                "symbol": row["tradingsymbol"],
                "name": row["name"],
                "exchange": row["exchange"],
                "price": str(row["last_price"]) if row["last_price"] else "0",
                "instrument_key": row["instrument_key"],
            }
        )

    return result


def calculate_ema(prices, period):
    ema_vals = []
    k = 2 / (period + 1)

    for i, price in enumerate(prices):
        if i == 0:
            ema_vals.append(price)
        else:
            ema_vals.append(price * k + ema_vals[-1] * (1 - k))

    return ema_vals


def calculate_rsi(prices, period=14):
    prices = np.array(prices)
    deltas = np.diff(prices)

    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = np.mean(gains[:period]) if len(gains) >= period else 0
    avg_loss = np.mean(losses[:period]) if len(losses) >= period else 0

    rsi = [None] * period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi.append(100 - (100 / (1 + rs)))

    return rsi


def calculate_vwap(prices, volumes):
    vwap = []
    cumulative_pv = 0
    cumulative_vol = 0

    for p, v in zip(prices, volumes):
        cumulative_pv += p * v
        cumulative_vol += v
        vwap.append(cumulative_pv / cumulative_vol if cumulative_vol != 0 else p)

    return vwap


def get_mock_candles(symbol):
    prices = []

    # simulate last 100 prices
    base = get_ltp(symbol)  # your existing LTP function

    for i in range(100):
        base += np.random.uniform(-1, 1)
        prices.append(round(base, 2))

    volumes = np.random.randint(100, 1000, size=len(prices))

    return prices, volumes


def check_ema_signal(df):
    if len(df) < 25:
        return None

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    # BUY crossover
    if prev["ema9"] < prev["ema21"] and curr["ema9"] > curr["ema21"]:
        return "BUY"

    # SELL crossover
    if prev["ema9"] > prev["ema21"] and curr["ema9"] < curr["ema21"]:
        return "SELL"

    return None


def is_market_open():
    now = datetime.now(IST).time()
    return dt_time(9, 0) <= now <= dt_time(15, 30)


def minute_price_collector(symbol):
    MIN_CANDLES_NEEDED = 100

    while True:
        try:
            now = datetime.now(IST)
            today_str = now.strftime("%Y-%m-%d")

            if not is_market_open():
                time_module.sleep(30)
                continue

            minute_key = now.strftime("%H:%M")

            ltp = get_ltp(symbol)

            if ltp is None:
                print(f"{symbol} LTP fetch failed")
                time_module.sleep(5)
                continue

            # Initialize nested dict structure
            if symbol not in minute_data:
                minute_data[symbol] = {}

            if today_str not in minute_data[symbol]:
                minute_data[symbol][today_str] = {}

            # Save minute close
            minute_data[symbol][today_str][minute_key] = ltp

            print(f"Saved minute: {symbol} {minute_key} {ltp}")

            # ─────────────────────────────────────
            # Compute Lux signals
            # ─────────────────────────────────────
            current_day_data = minute_data[symbol].get(today_str, {})

            if len(current_day_data) >= MIN_CANDLES_NEEDED:
                try:
                    closes = list(current_day_data.values())

                    df = pd.DataFrame(
                        {
                            "close": closes,
                            "open": closes,
                            "high": closes,
                            "low": closes,
                            "volume": [1000] * len(closes),
                        }
                    )

                    params = {
                        "mL": 7,
                        "sL": 3,
                        "sT": "SMA",
                        "mfL": 35,
                        "mfS": 6,
                        "rsF": 4,
                    }

                    signal_result = lux_osc_matrix.compute_lux_osc_signals(
                        df, params=params
                    )

                    lux_signals[symbol] = {
                        **signal_result,
                        "computed_at": now.isoformat(),
                        "candle_count": len(closes),
                    }

                    print(
                        f"Computed Lux signal for {symbol}: "
                        f"{signal_result.get('signal', 'neutral')} "
                        f"(candles: {len(closes)})"
                    )

                except Exception as calc_err:
                    print(f"Lux compute error for {symbol}: {calc_err}")

            # Wait for next minute
            time_module.sleep(60)

        except Exception as e:
            print(f"Minute Collector Error ({symbol}): {e}")
            time_module.sleep(10)


# def run_algo(symbol):
#     try:
#         state = engine.algo_states.get(symbol)
#         if not state:
#             return

#         rule = state.get("rule", {})

#         # FIX: Extract and convert values once at the start.
#         # This prevents the "float() ... not 'function'" error during the loop.
#         try:
#             target_sell_price = float(rule.get("sell_price", 0.0))
#             stop_loss_price = float(rule.get("stop_loss", 0.0))
#             trigger_buy_price = float(rule.get("buy_price", 0.0))
#             trade_qty = int(rule.get("quantity", 0))
#             exchange_name = rule.get("exchange", "NSE")
#         except (TypeError, ValueError) as e:
#             print(f"[{symbol}] Config Error: Invalid number in rules: {e}")
#             state["running"] = False
#             return

#         print(
#             f"[{symbol}] Algo Started. Target: {target_sell_price}, SL: {stop_loss_price}"
#         )

#         while state.get("running", False):
#             try:
#                 # Get current price
#                 ltp = get_ltp_price(symbol)

#                 if ltp is None:
#                     time_module.sleep(1)
#                     continue

#                 # ===================== BUY LOGIC =====================
#                 if not state.get("bought", False) and ltp >= trigger_buy_price:
#                     # Place Real/Paper Order
#                     try:
#                         # placeOrder(...)
#                         pass
#                     except Exception:
#                         pass

#                     # Update Portfolio Wallet
#                     success, msg = engine.update_portfolio_buy(
#                         symbol=symbol,
#                         exchange=exchange_name,
#                         qty=trade_qty,
#                         price=ltp,
#                     )

#                     if success:
#                         state["bought"] = True
#                         state["entry_price"] = ltp
#                         print(f"[{symbol}] BUY EXECUTED @ {ltp}")
#                     else:
#                         print(f"[{symbol}] BUY FAILED: {msg}")
#                         state["running"] = False  # Stop if no money

#                 # ===================== SELL LOGIC =====================
#                 elif state.get("bought", False):
#                     # Target Hit
#                     if ltp >= target_sell_price:
#                         print(f"[{symbol}] TARGET HIT @ {ltp}")
#                         engine.update_portfolio_sell(symbol, trade_qty, ltp)
#                         state["running"] = False
#                         break

#                     # Stop Loss Hit
#                     elif ltp <= stop_loss_price:
#                         print(f"[{symbol}] STOP LOSS HIT @ {ltp}")
#                         engine.update_portfolio_sell(symbol, trade_qty, ltp)
#                         state["running"] = False
#                         break

#                 time_module.sleep(1)
#             except Exception as inner_error:
#                 print(f"[{symbol}] Loop Error: {inner_error}")
#                 time_module.sleep(2)

#     except Exception as outer_error:
#         print(f"[{symbol}] Fatal Error: {outer_error}")


def run_algo(symbol):
    try:
        state = engine.algo_states.get(symbol)
        if not state:
            return

        rule = state.get("rule", {})

        try:
            target_sell_price = float(rule.get("sell_price", 0.0))
            stop_loss_price = float(rule.get("stop_loss", 0.0))
            trigger_buy_price = float(rule.get("buy_price", 0.0))
            trade_qty = int(rule.get("quantity", 0))
            exchange_name = rule.get("exchange", "NSE")
        except (TypeError, ValueError) as e:
            print(f"[{symbol}] Config Error: Invalid number in rules: {e}")
            state["running"] = False
            return

        print(
            f"[{symbol}] Algo Started. Target: {target_sell_price}, SL: {stop_loss_price}"
        )

        while state.get("running", False):
            try:
                # ── FIX: get_ltp_price returns (ltp, prev_close) tuple now ──
                # We only need ltp here, so unpack and discard prev_close
                result = get_ltp_price(symbol)
                if result is None or result[0] is None:
                    time_module.sleep(1)
                    continue
                ltp, _ = result  # ← unpack tuple, ignore prev_close

                # ── BUY LOGIC ─────────────────────────────────────────────
                if not state.get("bought", False) and ltp >= trigger_buy_price:
                    try:
                        # placeOrder(...)
                        pass
                    except Exception:
                        pass

                    success, msg = engine.update_portfolio_buy(
                        symbol=symbol,
                        exchange=exchange_name,
                        qty=trade_qty,
                        price=ltp,
                    )

                    if success:
                        state["bought"] = True
                        state["entry_price"] = ltp
                        print(f"[{symbol}] BUY EXECUTED @ {ltp}")
                    else:
                        print(f"[{symbol}] BUY FAILED: {msg}")
                        state["running"] = False

                # ── SELL LOGIC ────────────────────────────────────────────
                elif state.get("bought", False):
                    if ltp >= target_sell_price:
                        print(f"[{symbol}] TARGET HIT @ {ltp}")
                        engine.update_portfolio_sell(symbol, trade_qty, ltp)
                        state["running"] = False
                        break

                    elif ltp <= stop_loss_price:
                        print(f"[{symbol}] STOP LOSS HIT @ {ltp}")
                        engine.update_portfolio_sell(symbol, trade_qty, ltp)
                        state["running"] = False
                        break

                time_module.sleep(1)

            except Exception as inner_error:
                print(f"[{symbol}] Loop Error: {inner_error}")
                time_module.sleep(2)

    except Exception as outer_error:
        print(f"[{symbol}] Fatal Error: {outer_error}")


def check_trading_hours():
    try:
        ist = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist)
        current_time = now_ist.time()

        # Define open/close times as simple time objects
        # 5:30 AM
        open_time = time(5, 30, 0)
        # 11:59:59 PM (Avoids the 24:00:00 error)
        close_time = time(23, 59, 59)

        return open_time <= current_time <= close_time
    except Exception as e:
        print(f"Time check error: {e}")
        # Default to True so users aren't locked out of their accounts if clock fails
        return True


trade_logs = []


# ==========================================
#  STATE MANAGEMENT & INITIAL BALANCE
# ==========================================
class TradingEngine:
    def __init__(self):
        self.initial_balance = 100000.0
        self.current_balance = self.initial_balance
        self.portfolio = {}
        self.algo_states = {}

    def sync_to_firebase(self):
        pass  # Add Firebase code here

    def update_portfolio_buy(self, symbol, exchange, qty, price):
        cost = float(qty) * float(price)

        if self.current_balance < cost:
            return (
                False,
                f"Insufficient Funds! Required: ${cost:.2f}, Available: ${self.current_balance:.2f}",
            )

        self.current_balance -= cost

        if symbol in self.portfolio:
            old_qty = self.portfolio[symbol]["quantity"]
            old_avg = self.portfolio[symbol]["avg_price"]
            new_qty = old_qty + qty
            new_avg = ((old_qty * old_avg) + (qty * price)) / new_avg

            self.portfolio[symbol]["quantity"] = new_qty
            self.portfolio[symbol]["avg_price"] = round(new_avg, 2)
        else:
            self.portfolio[symbol] = {
                "symbol": symbol,
                "exchange": exchange,
                "quantity": qty,
                "avg_price": round(price, 2),
            }

        self.sync_to_firebase()
        return True, "Trade Executed Successfully"

    def update_portfolio_sell(self, symbol, qty, sell_price):
        if symbol not in self.portfolio:
            return False, "No position found!"

        current_qty = self.portfolio[symbol]["quantity"]
        if current_qty < qty:
            return False, "Not enough quantity to sell!"

        revenue = float(qty) * float(sell_price)
        self.current_balance += revenue

        self.portfolio[symbol]["quantity"] -= qty

        if self.portfolio[symbol]["quantity"] <= 0:
            del self.portfolio[symbol]

        self.sync_to_firebase()
        return True, "Sell Executed Successfully"

    def get_account_summary(self):
        return {
            "cash_available": round(self.current_balance, 2),
            "portfolio": list(self.portfolio.values()),
        }


engine = TradingEngine()
algo_threads = {}


# ==========================================
#  ROUTES & ENDPOINTS
# ==========================================
@app.route("/minute_data/<instrument_key>")
def get_minute_data(instrument_key):

    access_token = get_upstox_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    now = datetime.now()

    market_open_time = time(9, 15)
    market_close_time = time(15, 30)

    today = now.date()
    # 🔹 Decide which endpoint to use
    if market_open_time <= now.time() <= market_close_time and today.weekday() < 5:
        # MARKET RUNNING → USE INTRADAY
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
    else:
        # MARKET CLOSED OR WEEKEND → USE HISTORICAL

        # Find last trading day
        chart_date = today

        if today.weekday() == 5:  # Saturday
            chart_date = today - timedelta(days=1)
        elif today.weekday() == 6:  # Sunday
            chart_date = today - timedelta(days=2)

        # If before market open → use previous day
        elif now.time() < market_open_time:
            chart_date = today - timedelta(days=1)

        # 🔁 Holiday fallback loop
        while True:
            date_str = chart_date.strftime("%Y-%m-%d")

            url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{date_str}/{date_str}"

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                return jsonify({"error": response.text}), 500

            data = response.json()
            raw_candles = data.get("data", {}).get("candles", [])

            if raw_candles:
                break

            chart_date = chart_date - timedelta(days=1)

        # Process historical candles

        return process_candles(raw_candles)

    # 🔥 If intraday endpoint used
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return jsonify({"error": response.text}), 500

    data = response.json()
    raw_candles = data.get("data", {}).get("candles", [])

    return process_candles(raw_candles)


@app.route("/init_account")
def init_account():
    # Reset to default 100k if new user
    engine.reset_account()
    return jsonify({"status": "ok", "msg": "Account Initialized"})


@app.route("/signal/<symbol>")
def get_signal(symbol):
    return jsonify(last_signal.get(symbol, {}))


@app.route("/trade_logs/<symbol>")
def get_trade_logs(symbol):
    logs = [t for t in trade_logs if t["symbol"] == symbol]
    return jsonify(logs)


@app.route("/watchlist", methods=["GET"])
def watchlist():
    return jsonify(get_all_equity_stocks())


@app.route("/price/<symbol>")
def price(symbol):
    try:
        ltp = get_ltp(symbol)
        return jsonify({"price": ltp})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/ltp/<instrument_key>")
def ltp(instrument_key):
    price = get_ltp(instrument_key)
    return jsonify({"price": price})


@app.route("/ltp1/<symbol>")
def ltp_route(symbol):
    try:
        ltp_val, prev_close = get_ltp_price(symbol)
        if ltp_val is None:
            return (
                jsonify({"error": "Symbol not found", "ltp": 0.0, "prev_close": 0.0}),
                404,
            )
        return (
            jsonify(
                {
                    "ltp": round(float(ltp_val), 2),
                    "prev_close": round(float(prev_close), 2),
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e), "ltp": 0.0, "prev_close": 0.0}), 500

@app.route("/portfolio", methods=["GET"])
def get_portfolio():

    try:
        portfolio_list = list(engine.portfolio.values())

        return jsonify(portfolio_list), 200
    except Exception as e:
        print(f"Error fetching portfolio: {e}")
        return jsonify({"error": "Could not fetch portfolio"}), 500


@app.route("/account_status", methods=["GET"])
def get_account_summary_api():
    return jsonify(engine.get_account_summary()), 200


@app.route("/buy_order", methods=["POST"])
def buy_endpoint():
    try:
        data = request.json or {}
        symbol = data.get("symbol")
        qty = int(data.get("qty", 0))
        price = float(data.get("price", 0.0))

        if not symbol or qty <= 0:
            return jsonify({"success": False, "msg": "Invalid Symbol or Qty"}), 400

        if not check_trading_hours():
            success, msg = engine.update_portfolio_buy(symbol, "NSE", qty, price)
            return jsonify(
                {
                    "success": success,
                    "msg": msg,
                    "balance": engine.current_balance,
                    "note": "Market CLOSED. Simulation Mode.",
                }
            ), (200 if success else 400)

        # Real Order Logic
        try:
            placeOrder(
                inst=symbol,
                t_type="BUY",
                qty=qty,
                order_type="MARKET",
                price=0,
                variety="regular",
                papertrading=1,
            )
        except Exception as e:
            return jsonify({"error": str(e)}), 400

        success, msg = engine.update_portfolio_buy(symbol, "NSE", qty, price)
        return (
            jsonify(
                {"success": success, "msg": msg, "balance": engine.current_balance}
            ),
            200,
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/sell_order", methods=["POST"])
def sell_endpoint():
    try:
        data = request.json or {}
        symbol = data.get("symbol", "UNKNOWN")

        # 1. Safe parsing of numbers
        try:
            qty = int(data.get("qty", 0))
            # Get sell_price from Flutter, default to 0.0
            sell_price = float(data.get("sell_price", 0.0))
        except (ValueError, TypeError) as e:
            return (
                jsonify({"success": False, "error": f"Invalid number format: {e}"}),
                400,
            )

        if symbol == "UNKNOWN" or qty <= 0:
            return (
                jsonify(
                    {"success": False, "error": "Missing symbol or invalid quantity"}
                ),
                400,
            )

        # 2. Check trading hours (Logging only, don't block the logic)
        if not check_trading_hours():
            print(f"[{datetime.now()}] INFO: Selling {symbol} outside standard hours.")

        # 3. Process the sell in your TradingEngine
        # This deducts the qty and adds the cash to balance
        success, msg = engine.update_portfolio_sell(symbol, qty, sell_price)

        # 4. Return result to Flutter
        # Python Booleans MUST be Capitalized (True/False)
        return jsonify(
            {
                "success": success,
                "msg": msg,
                "balance": round(engine.current_balance, 2),
            }
        ), (200 if success else 400)

    except Exception as e:
        # Log the full error to your terminal so you can see it
        print(f"CRITICAL ERROR in sell_endpoint: {e}")
        # Always use Capital 'False' here
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/reset_account", methods=["POST"])
def reset_account():
    engine.current_balance = 100000.0
    engine.portfolio = {}
    return jsonify({"status": "success", "message": "Account Reset"}), 200


@app.route("/stop_algo", methods=["POST"])
def stop_algo():
    data = request.json or {}
    symbol = data.get("symbol")

    if symbol in engine.algo_states:
        state = engine.algo_states[symbol]

        # If the algo was running and HAD BOUGHT shares, we should sell them now
        if state.get("running") and state.get("bought"):
            print(f"[{symbol}] Manual Stop: Liquidating position...")

            # Get current price to calculate recovery cash
            current_price = get_ltp_price(symbol) or state.get("entry_price", 0)
            qty = state.get("rule", {}).get("quantity", 0)

            # Update Portfolio: Sell the shares and add cash back to balance
            success, msg = engine.update_portfolio_sell(symbol, qty, current_price)
            print(f"[{symbol}] Liquidated: {msg}")

        # Stop the loop
        engine.algo_states[symbol]["running"] = False
        return jsonify(
            {
                "status": "stopped",
                "symbol": symbol,
                "portfolio_updated": state.get("bought", False),
            }
        )

    return jsonify({"error": "algo not found"}), 400


@app.route("/start_algo", methods=["POST"])
def start_algo():
    data = request.json
    symbol = data.get("symbol")

    if not symbol:
        return jsonify({"error": "Symbol required"}), 400

    if symbol in engine.algo_states and engine.algo_states[symbol].get("running"):
        return jsonify({"status": "already running", "symbol": symbol})

    engine.algo_states[symbol] = {
        "running": True,
        "rule": data,
        "bought": False,
        "entry_price": 0,
    }

    thread = threading.Thread(target=run_algo, args=(symbol,), daemon=True)
    algo_threads[symbol] = thread
    thread.start()

    return jsonify({"status": "algo started", "symbol": symbol, "rule": data})


@app.route("/upstox/login-url", methods=["GET"])
def get_login_url():
    return jsonify({"url": upstox_login.auth_url()})


@app.route("/upstox/exchange", methods=["POST"])
def post_exchange():
    code = (request.json or {}).get("code")
    if not code:
        return jsonify({"error": "code missing"}), 400
    try:
        return jsonify(upstox_login.exchange_code(code))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def restart_server():
    # Add a small delay if necessary
    os.execl(sys.executable, sys.executable, *sys.argv)


# @app.route("/callback", methods=["GET"])
# def callback():
#     code = request.args.get("code")
#     print("Task finished, restarting...")
#     print(
#         f"<h3>Login OK</h3><p>You can copy this code back to the app: <code>{code}</code></p>"
#     )
#     threading.Thread(target=restart_server).start()


@app.route("/callback", methods=["GET"])
def callback():
    code = request.args.get("code")
    if not code:
        return "No code received", 400
    # Save token logic here
    return f"<h3>Login OK</h3><p>Code: <code>{code}</code></p>", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4000, debug=False)
    
