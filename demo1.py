import pandas as pd
import pandas_ta as pta
import numpy as np


def backtest_rsi_trend_confluence(
    symbol: str,
    start_date: str,
    end_date: str,
    timeframe: str,
    initial_capital: float,
    rsi_length: int = 21,
    ema_fast: int = 50,
    ema_slow: int = 200,
    stop_loss_pct: float = 6.0,
    take_profit_pct: float = 12.0,
    trail_pct: float = 3.0,
    position_size_pct: float = 30.0,
    trade_type: str = "Both",
    use_session_filter: bool = False,
    session_start: int = 0,
    session_end: int = 23,
    breakeven_pct: float = 4.0,
):
    import yfinance as yf

    interval_map = {
        "1h": "1h",
        "4h": "1h",  # download 1h and resample
        "1d": "1d",
    }

    interval = interval_map.get(timeframe, "1h")
    df = yf.download(symbol, start=start_date, end=end_date, interval=interval)

    if df.empty:
        return {"error": "No data downloaded"}

    # Normalize columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [col.lower() for col in df.columns]

    if timeframe == "4h":
        df = (
            df.resample("4H")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna()
        )

    df.reset_index(inplace=True)

    for col in df.columns:
        if str(col).lower() in ["date", "datetime"]:
            df.rename(columns={col: "datetime"}, inplace=True)
            break

    if "datetime" not in df.columns:
        df.rename(columns={df.columns[0]: "datetime"}, inplace=True)

    # =============================
    # INDICATORS
    # =============================

    df.ta.rsi(close="close", length=rsi_length, append=True)
    df.rename(columns={f"RSI_{rsi_length}": "rsi"}, inplace=True)

    df.ta.ema(close="close", length=ema_fast, append=True)
    df.ta.ema(close="close", length=ema_slow, append=True)
    df.rename(
        columns={
            f"EMA_{ema_fast}": "ema_fast",
            f"EMA_{ema_slow}": "ema_slow",
        },
        inplace=True,
    )

    df.ta.atr(high="high", low="low", close="close", length=14, append=True)
    df.rename(columns={"ATRr_14": "atr"}, inplace=True)

    df["atr_sma"] = df["atr"].rolling(20).mean()
    df.dropna(inplace=True)

    # =============================
    # TREND REGIME
    # =============================

    df["macro_uptrend"] = df["close"] > df["ema_slow"]
    df["macro_downtrend"] = df["close"] < df["ema_slow"]

    df["strong_uptrend"] = df["ema_fast"] > df["ema_slow"]
    df["strong_downtrend"] = df["ema_fast"] < df["ema_slow"]

    # Volatility filter (real one)
    df["good_volatility"] = df["atr"] > df["atr_sma"]

    # =============================
    # SCORE-BASED ENTRY
    # =============================

    df["long_score"] = 0
    df["short_score"] = 0

    # Long scoring
    df.loc[df["macro_uptrend"], "long_score"] += 1
    df.loc[df["strong_uptrend"], "long_score"] += 1
    df.loc[df["rsi"] < 35, "long_score"] += 1
    df.loc[df["good_volatility"], "long_score"] += 1

    # Short scoring
    df.loc[df["macro_downtrend"], "short_score"] += 1
    df.loc[df["strong_downtrend"], "short_score"] += 1
    df.loc[df["rsi"] > 65, "short_score"] += 1
    df.loc[df["good_volatility"], "short_score"] += 1

    df["go_long"] = df["long_score"] >= 3
    df["go_short"] = df["short_score"] >= 3

    # =============================
    # SESSION FILTER
    # =============================

    if use_session_filter:
        df["hour"] = pd.to_datetime(df["datetime"]).dt.hour
        df["in_session"] = (df["hour"] >= session_start) & (df["hour"] <= session_end)
        df["go_long"] &= df["in_session"]
        df["go_short"] &= df["in_session"]

    # =============================
    # BACKTEST ENGINE
    # =============================

    capital = initial_capital
    position = 0
    entry_price = 0
    stop_loss = 0
    take_profit = 0
    position_size = 0
    breakeven_triggered = False

    trades = []
    equity_curve = []
    current_trade = None

    for i in range(len(df)):
        row = df.iloc[i]
        price = row["close"]

        # Update equity
        if position == 1:
            equity_curve.append(capital + (price - entry_price) * position_size)
        elif position == -1:
            equity_curve.append(capital + (entry_price - price) * position_size)
        else:
            equity_curve.append(capital)

        # Entry
        if position == 0:
            if row["go_long"] and trade_type in ["Long Only", "Both"]:
                position = 1
                entry_price = price
                position_size = (capital * (position_size_pct / 100)) / price
                stop_loss = price * (1 - stop_loss_pct / 100)
                take_profit = price * (1 + take_profit_pct / 100)
                breakeven_triggered = False
                current_trade = {
                    "entry_date": row["datetime"],
                    "entry_price": price,
                    "type": "long",
                }

            elif row["go_short"] and trade_type in ["Short Only", "Both"]:
                position = -1
                entry_price = price
                position_size = (capital * (position_size_pct / 100)) / price
                stop_loss = price * (1 + stop_loss_pct / 100)
                take_profit = price * (1 - take_profit_pct / 100)
                breakeven_triggered = False
                current_trade = {
                    "entry_date": row["datetime"],
                    "entry_price": price,
                    "type": "short",
                }

        # Manage Long
        elif position == 1:
            if not breakeven_triggered and price > entry_price * (
                1 + breakeven_pct / 100
            ):
                stop_loss = entry_price
                breakeven_triggered = True

            if price <= stop_loss or price >= take_profit:
                pnl = (price - entry_price) * position_size
                capital += pnl
                trades.append(
                    {
                        **current_trade,
                        "exit_date": row["datetime"],
                        "exit_price": price,
                        "pnl": pnl,
                    }
                )
                position = 0
                continue

            new_trail = price * (1 - trail_pct / 100)
            if new_trail > stop_loss:
                stop_loss = new_trail

        # Manage Short
        elif position == -1:
            if not breakeven_triggered and price < entry_price * (
                1 - breakeven_pct / 100
            ):
                stop_loss = entry_price
                breakeven_triggered = True

            if price >= stop_loss or price <= take_profit:
                pnl = (entry_price - price) * position_size
                capital += pnl
                trades.append(
                    {
                        **current_trade,
                        "exit_date": row["datetime"],
                        "exit_price": price,
                        "pnl": pnl,
                    }
                )
                position = 0
                continue

            new_trail = price * (1 + trail_pct / 100)
            if new_trail < stop_loss:
                stop_loss = new_trail

    # =============================
    # STATS
    # =============================

    final_capital = equity_curve[-1]
    net_profit = final_capital - initial_capital
    net_profit_pct = (net_profit / initial_capital) * 100

    trades_df = pd.DataFrame(trades)

    if len(trades_df) > 0:
        total_trades = len(trades_df)
        wins = trades_df[trades_df["pnl"] > 0]
        losses = trades_df[trades_df["pnl"] < 0]

        win_rate = len(wins) / total_trades * 100
        gross_profit = wins["pnl"].sum()
        gross_loss = abs(losses["pnl"].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        max_dd = (
            (pd.Series(equity_curve).cummax() - pd.Series(equity_curve))
            / pd.Series(equity_curve).cummax()
            * 100
        ).max()
    else:
        total_trades = win_rate = profit_factor = max_dd = 0

    return {
        "initial_capital": initial_capital,
        "final_capital": float(final_capital),
        "net_profit_pct": float(net_profit_pct),
        "total_trades": int(total_trades),
        "win_rate": float(win_rate),
        "profit_factor": float(profit_factor),
        "max_drawdown": float(max_dd),
        "trades": trades,
    }
