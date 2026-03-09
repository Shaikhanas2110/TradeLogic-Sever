import pandas as pd
import ta
import yfinance as yf

# Define the NSE stock symbol (example: NIFTY50 index)
symbol = "^NSEI"  # Use actual stock symbols like 'TCS.NS', 'INFY.NS' for individual stocks

# Fetch historical data from Yahoo Finance
df = yf.download(symbol, period="6mo", interval="1d")  # Last 6 months of daily data

# Calculate Moving Average of Volume (for comparison)
df["Volume_MA"] = df["Volume"].rolling(window=20).mean()

# Identify High-Volume Days
df["High_Volume"] = df["Volume"] > df["Volume_MA"] * 1.5  # Stocks with 50% higher volume than average

# Filter days with high volume
high_volume_days = df[df["High_Volume"]]

# Display results
print(high_volume_days[["Close", "Volume"]])
