import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ================================
# MOVING AVERAGES & TECHNICAL INDICATORS
# ================================

def calculate_sma(df, period=20, column='close'):
    """
    Calculate Simple Moving Average
    Args:
        df: DataFrame with OHLC data
        period: Number of periods for SMA (default 20)
        column: Column to calculate SMA on (default 'close')
    Returns:
        Series with SMA values
    """
    return df[column].rolling(window=period, min_periods=period).mean()

def calculate_ema(df, period=20, column='close'):
    """
    Calculate Exponential Moving Average
    Args:
        df: DataFrame with OHLC data
        period: Number of periods for EMA
        column: Column to calculate EMA on
    Returns:
        Series with EMA values
    """
    return df[column].ewm(span=period, adjust=False).mean()

def calculate_multiple_sma(df, periods=[9, 20, 50]):
    """
    Calculate multiple SMAs at once
    Args:
        df: DataFrame with OHLC data
        periods: List of periods for SMA calculation
    Returns:
        DataFrame with additional SMA columns
    """
    df_copy = df.copy()
    for period in periods:
        df_copy[f'SMA_{period}'] = calculate_sma(df_copy, period)
    return df_copy

def calculate_rsi(df, period=14):
    """
    Calculate RSI (Relative Strength Index)
    Args:
        df: DataFrame with OHLC data
        period: Period for RSI calculation (default 14)
    Returns:
        Series with RSI values
    """
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ================================
# CANDLE PATTERN ANALYSIS
# ================================

def analyze_candle_properties(df):
    """
    Add candle analysis properties to DataFrame
    Returns DataFrame with additional columns for candle analysis
    """
    df_copy = df.copy()

    # Basic candle properties
    df_copy['body'] = abs(df_copy['close'] - df_copy['open'])
    df_copy['upper_shadow'] = df_copy['high'] - df_copy[['open', 'close']].max(axis=1)
    df_copy['lower_shadow'] = df_copy[['open', 'close']].min(axis=1) - df_copy['low']
    df_copy['total_range'] = df_copy['high'] - df_copy['low']
    df_copy['is_bullish'] = (df_copy['close'] > df_copy['open']).astype(int)
    df_copy['is_bearish'] = (df_copy['close'] < df_copy['open']).astype(int)

    # Ratios for pattern detection
    df_copy['body_ratio'] = df_copy['body'] / df_copy['total_range']
    df_copy['upper_shadow_ratio'] = df_copy['upper_shadow'] / df_copy['total_range']
    df_copy['lower_shadow_ratio'] = df_copy['lower_shadow'] / df_copy['total_range']

    return df_copy

def detect_doji(df, threshold=0.1):
    """
    Detect Doji patterns (small body relative to range)
    Args:
        df: DataFrame with candle properties
        threshold: Maximum body ratio to consider as Doji
    Returns:
        Series with Doji signals (1 for Doji, 0 for normal)
    """
    return (df['body_ratio'] <= threshold).astype(int)

def detect_hammer(df, body_threshold=0.3, shadow_threshold=2.0):
    """
    Detect Hammer patterns
    Args:
        df: DataFrame with candle properties
        body_threshold: Maximum body ratio
        shadow_threshold: Minimum lower shadow to body ratio
    Returns:
        Series with Hammer signals
    """
    hammer_condition = (
            (df['body_ratio'] <= body_threshold) &
            (df['lower_shadow'] >= shadow_threshold * df['body']) &
            (df['upper_shadow'] <= df['body'] * 0.5)
    )
    return hammer_condition.astype(int)

def detect_shooting_star(df, body_threshold=0.3, shadow_threshold=2.0):
    """
    Detect Shooting Star patterns
    Args:
        df: DataFrame with candle properties
        body_threshold: Maximum body ratio
        shadow_threshold: Minimum upper shadow to body ratio
    Returns:
        Series with Shooting Star signals
    """
    shooting_star_condition = (
            (df['body_ratio'] <= body_threshold) &
            (df['upper_shadow'] >= shadow_threshold * df['body']) &
            (df['lower_shadow'] <= df['body'] * 0.5)
    )
    return shooting_star_condition.astype(int)

def detect_engulfing_pattern(df):
    """
    Detect Bullish and Bearish Engulfing patterns
    Returns:
        Tuple of (bullish_engulfing, bearish_engulfing) Series
    """
    # Bullish Engulfing: Current bullish candle engulfs previous bearish candle
    bullish_engulfing = (
            (df['is_bullish'] == 1) &
            (df['is_bearish'].shift(1) == 1) &
            (df['open'] < df['close'].shift(1)) &
            (df['close'] > df['open'].shift(1))
    ).astype(int)

    # Bearish Engulfing: Current bearish candle engulfs previous bullish candle
    bearish_engulfing = (
            (df['is_bearish'] == 1) &
            (df['is_bullish'].shift(1) == 1) &
            (df['open'] > df['close'].shift(1)) &
            (df['close'] < df['open'].shift(1))
    ).astype(int)

    return bullish_engulfing, bearish_engulfing

# ================================
# TREND ANALYSIS & REVERSALS
# ================================

def detect_trend_direction(df, sma_short=9, sma_long=20):
    """
    Detect trend direction using moving averages
    Args:
        df: DataFrame with OHLC data
        sma_short: Short period SMA
        sma_long: Long period SMA
    Returns:
        Series with trend direction (1=uptrend, -1=downtrend, 0=sideways)
    """
    df_copy = df.copy()
    df_copy['SMA_short'] = calculate_sma(df_copy, sma_short)
    df_copy['SMA_long'] = calculate_sma(df_copy, sma_long)

    trend = np.where(
        df_copy['SMA_short'] > df_copy['SMA_long'], 1,  # Uptrend
        np.where(df_copy['SMA_short'] < df_copy['SMA_long'], -1, 0)  # Downtrend or Sideways
    )

    return pd.Series(trend, index=df.index)

def detect_trend_reversal(df, lookback=5):
    """
    Detect potential trend reversals using multiple signals
    Args:
        df: DataFrame with analyzed candle data
        lookback: Number of periods to look back for trend confirmation
    Returns:
        Dict with bullish and bearish reversal signals
    """
    # Add required indicators
    df_analyzed = analyze_candle_properties(df)
    df_analyzed['RSI'] = calculate_rsi(df_analyzed)
    df_analyzed['SMA_20'] = calculate_sma(df_analyzed, 20)
    df_analyzed['trend'] = detect_trend_direction(df_analyzed)

    # Detect patterns
    df_analyzed['doji'] = detect_doji(df_analyzed)
    df_analyzed['hammer'] = detect_hammer(df_analyzed)
    df_analyzed['shooting_star'] = detect_shooting_star(df_analyzed)
    bullish_eng, bearish_eng = detect_engulfing_pattern(df_analyzed)
    df_analyzed['bullish_engulfing'] = bullish_eng
    df_analyzed['bearish_engulfing'] = bearish_eng

    # Bullish reversal conditions
    bullish_reversal = (
            (df_analyzed['trend'].rolling(lookback).mean() < 0) &  # Previous downtrend
            (
                    (df_analyzed['hammer'] == 1) |
                    (df_analyzed['bullish_engulfing'] == 1) |
                    ((df_analyzed['doji'] == 1) & (df_analyzed['RSI'] < 30))
            ) &
            (df_analyzed['close'] > df_analyzed['SMA_20'])  # Price above SMA
    ).astype(int)

    # Bearish reversal conditions
    bearish_reversal = (
            (df_analyzed['trend'].rolling(lookback).mean() > 0) &  # Previous uptrend
            (
                    (df_analyzed['shooting_star'] == 1) |
                    (df_analyzed['bearish_engulfing'] == 1) |
                    ((df_analyzed['doji'] == 1) & (df_analyzed['RSI'] > 70))
            ) &
            (df_analyzed['close'] < df_analyzed['SMA_20'])  # Price below SMA
    ).astype(int)

    return {
        'bullish_reversal': bullish_reversal,
        'bearish_reversal': bearish_reversal,
        'analysis_df': df_analyzed
    }

# ================================
# BREAKOUT DETECTION
# ================================

def calculate_support_resistance(df, window=20, min_touches=2):
    """
    Calculate dynamic support and resistance levels
    Args:
        df: DataFrame with OHLC data
        window: Rolling window for level calculation
        min_touches: Minimum touches to confirm level
    Returns:
        DataFrame with support and resistance levels
    """
    df_copy = df.copy()

    # Rolling highs and lows
    df_copy['rolling_high'] = df_copy['high'].rolling(window=window).max()
    df_copy['rolling_low'] = df_copy['low'].rolling(window=window).min()

    # Dynamic support (recent lows)
    df_copy['support'] = df_copy['low'].rolling(window=window//2).min()

    # Dynamic resistance (recent highs)
    df_copy['resistance'] = df_copy['high'].rolling(window=window//2).max()

    return df_copy

def detect_breakouts(df, volume_multiplier=1.5, breakout_threshold=0.002):
    """
    Detect breakout signals with volume confirmation
    Args:
        df: DataFrame with OHLC data
        volume_multiplier: Volume should be X times average for confirmation
        breakout_threshold: Minimum % move to consider breakout
    Returns:
        Dict with breakout signals and analysis
    """
    # Calculate support/resistance and volume average
    df_sr = calculate_support_resistance(df)
    df_sr['avg_volume'] = df_sr['volume'].rolling(window=20).mean()
    df_sr['volume_ratio'] = df_sr['volume'] / df_sr['avg_volume']

    # Breakout conditions
    resistance_breakout = (
            (df_sr['close'] > df_sr['resistance']) &
            (df_sr['close'] / df_sr['resistance'] - 1 > breakout_threshold) &
            (df_sr['volume_ratio'] > volume_multiplier)
    ).astype(int)

    support_breakdown = (
            (df_sr['close'] < df_sr['support']) &
            (1 - df_sr['close'] / df_sr['support'] > breakout_threshold) &
            (df_sr['volume_ratio'] > volume_multiplier)
    ).astype(int)

    return {
        'resistance_breakout': resistance_breakout,
        'support_breakdown': support_breakdown,
        'support_levels': df_sr['support'],
        'resistance_levels': df_sr['resistance'],
        'analysis_df': df_sr
    }

# ================================
# INTEGRATED ANALYSIS FUNCTION
# ================================

def comprehensive_market_analysis(df, symbol=""):
    """
    Comprehensive analysis combining all indicators
    Args:
        df: DataFrame with OHLC data from getHistorical functions
        symbol: Symbol name for reference
    Returns:
        Dict with complete analysis and trading signals
    """
    print(f"Analyzing {symbol}...")

    # 1. Add multiple SMAs
    df_analyzed = calculate_multiple_sma(df, [9, 20, 50])

    # 2. Add candle analysis
    df_analyzed = analyze_candle_properties(df_analyzed)

    # 3. Add technical indicators
    df_analyzed['RSI'] = calculate_rsi(df_analyzed)
    df_analyzed['EMA_12'] = calculate_ema(df_analyzed, 12)
    df_analyzed['EMA_26'] = calculate_ema(df_analyzed, 26)

    # 4. Detect patterns
    df_analyzed['doji'] = detect_doji(df_analyzed)
    df_analyzed['hammer'] = detect_hammer(df_analyzed)
    df_analyzed['shooting_star'] = detect_shooting_star(df_analyzed)
    bullish_eng, bearish_eng = detect_engulfing_pattern(df_analyzed)
    df_analyzed['bullish_engulfing'] = bullish_eng
    df_analyzed['bearish_engulfing'] = bearish_eng

    # 5. Trend analysis
    reversal_analysis = detect_trend_reversal(df_analyzed)

    # 6. Breakout analysis
    breakout_analysis = detect_breakouts(df_analyzed)

    # 7. Generate trading signals
    latest_data = df_analyzed.iloc[-1]
    current_signals = {
        'timestamp': latest_data.name,
        'close_price': latest_data['close'],
        'sma_20': latest_data['SMA_20'],
        'rsi': latest_data['RSI'],
        'trend_signal': 'BULLISH' if latest_data['close'] > latest_data['SMA_20'] else 'BEARISH',
        'reversal_signal': 'BULLISH_REVERSAL' if reversal_analysis['bullish_reversal'].iloc[-1] else
        'BEARISH_REVERSAL' if reversal_analysis['bearish_reversal'].iloc[-1] else 'NONE',
        'breakout_signal': 'RESISTANCE_BREAKOUT' if breakout_analysis['resistance_breakout'].iloc[-1] else
        'SUPPORT_BREAKDOWN' if breakout_analysis['support_breakdown'].iloc[-1] else 'NONE',
        'pattern_detected': []
    }

    # Check for patterns in latest candle
    if latest_data['doji'] == 1:
        current_signals['pattern_detected'].append('DOJI')
    if latest_data['hammer'] == 1:
        current_signals['pattern_detected'].append('HAMMER')
    if latest_data['shooting_star'] == 1:
        current_signals['pattern_detected'].append('SHOOTING_STAR')
    if latest_data['bullish_engulfing'] == 1:
        current_signals['pattern_detected'].append('BULLISH_ENGULFING')
    if latest_data['bearish_engulfing'] == 1:
        current_signals['pattern_detected'].append('BEARISH_ENGULFING')

    return {
        'analyzed_df': df_analyzed,
        'current_signals': current_signals,
        'reversal_analysis': reversal_analysis,
        'breakout_analysis': breakout_analysis
    }

# ================================
# INTEGRATION WITH EXISTING UPSTOX FUNCTIONS
# ================================

def analyze_and_trade(symbol, interval=30, duration=5, quantity=1, papertrading=0):
    """
    Complete analysis and trading function that integrates with existing Upstox functions
    Args:
        symbol: Trading symbol (e.g., "NIFTY2550818000CE")
        interval: Time interval for analysis
        duration: Number of days of historical data
        quantity: Trading quantity
        papertrading: 0 for paper trading, 1 for real trading
    Returns:
        Dict with analysis results and trade execution status
    """
    try:
        # 1. Get historical data using existing function
        historical_data = getHistorical(symbol, interval, duration)

        if historical_data.empty:
            return {'error': 'No historical data available'}

        # 2. Perform comprehensive analysis
        analysis = comprehensive_market_analysis(historical_data, symbol)

        # 3. Trading decision logic
        signals = analysis['current_signals']
        trade_executed = False
        trade_details = {}

        # Strong bullish signals
        if (signals['trend_signal'] == 'BULLISH' and
                (signals['reversal_signal'] == 'BULLISH_REVERSAL' or
                 signals['breakout_signal'] == 'RESISTANCE_BREAKOUT') and
                signals['rsi'] < 70):

            print(f"Strong BULLISH signal detected for {symbol}")
            order_id = placeOrder(symbol, "BUY", quantity, "MARKET", 0, "regular", papertrading)
            trade_executed = True
            trade_details = {'action': 'BUY', 'order_id': order_id, 'reason': 'Bullish reversal/breakout'}

        # Strong bearish signals
        elif (signals['trend_signal'] == 'BEARISH' and
              (signals['reversal_signal'] == 'BEARISH_REVERSAL' or
               signals['breakout_signal'] == 'SUPPORT_BREAKDOWN') and
              signals['rsi'] > 30):

            print(f"Strong BEARISH signal detected for {symbol}")
            order_id = placeOrder(symbol, "SELL", quantity, "MARKET", 0, "regular", papertrading)
            trade_executed = True
            trade_details = {'action': 'SELL', 'order_id': order_id, 'reason': 'Bearish reversal/breakdown'}

        return {
            'symbol': symbol,
            'analysis': analysis,
            'trade_executed': trade_executed,
            'trade_details': trade_details,
            'timestamp': datetime.now()
        }

    except Exception as e:
        print(f"Error in analyze_and_trade for {symbol}: {e}")
        return {'error': str(e)}

# ================================
# USAGE EXAMPLES
# ================================

def scan_multiple_instruments(instruments, interval=30, duration=5):
    """
    Scan multiple instruments for trading opportunities
    Args:
        instruments: List of symbols to scan
        interval: Time interval for analysis
        duration: Historical data duration
    Returns:
        List of analysis results for all instruments
    """
    results = []

    for instrument in instruments:
        try:
            print(f"\n=== Scanning {instrument} ===")
            result = analyze_and_trade(instrument, interval, duration, papertrading=0)
            results.append(result)

            # Print summary
            if 'analysis' in result:
                signals = result['analysis']['current_signals']
                print(f"Price: {signals['close_price']:.2f}")
                print(f"SMA20: {signals['sma_20']:.2f}")
                print(f"RSI: {signals['rsi']:.2f}")
                print(f"Trend: {signals['trend_signal']}")
                print(f"Patterns: {', '.join(signals['pattern_detected']) if signals['pattern_detected'] else 'None'}")

        except Exception as e:
            print(f"Error scanning {instrument}: {e}")
            results.append({'symbol': instrument, 'error': str(e)})

    return results