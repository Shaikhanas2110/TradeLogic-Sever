# ================= LUX OSCILLATOR ENGINE =================
import pandas as pd
import numpy as np


def lux_oscillator(df, mL=7, sL=3, mfL=35, mfS=6, rsF=4):

    # BASIC CHECK
    if df is None or len(df) < 50:
        return None

    # SAFE COPY + SORT
    df = df.copy()
    df = df.sort_index()

    # HYPER WAVE
    hi = df["high"].rolling(mL, min_periods=1).max()
    lo = df["low"].rolling(mL, min_periods=1).min()
    av = (df["high"] + df["low"]) / 2

    # Prevent zero division
    range_ = (hi - lo).replace(0, np.nan)

    base = (df["close"] - (hi + lo + av) / 3) / range_ * 100
    base = base.replace([np.inf, -np.inf], np.nan).fillna(0)

    # Oscillator
    lin = base.rolling(mL, min_periods=1).mean()
    osc_sig = lin.ewm(span=sL, adjust=False).mean()
    osc_sgD = osc_sig.ewm(span=2, adjust=False).mean()

    # MFI
    tp = (df["high"] + df["low"] + df["close"]) / 3
    rmf = tp * df["volume"]

    # FIX → preserve index
    pos = pd.Series(np.where(tp > tp.shift(1), rmf, 0), index=df.index)
    neg = pd.Series(np.where(tp < tp.shift(1), rmf, 0), index=df.index)

    pos = pos.rolling(mfL, min_periods=1).sum()
    neg = neg.rolling(mfL, min_periods=1).sum()

    # Prevent zero division
    mfi_ratio = pos / neg.replace(0, np.nan)
    mfi = 100 - (100 / (1 + mfi_ratio))
    mfi = mfi.fillna(50)
    mfi = mfi.rolling(mfS, min_periods=1).mean() - 50

    # REVERSAL
    vMA = df["volume"].rolling(7, min_periods=1).mean()
    rsi = vMA.diff().fillna(0).rolling(7, min_periods=1).mean()

    tMj = df["volume"] > vMA * (1 + rsF / 10)
    tMn = (df["volume"] > vMA * (rsF / 10)) & (~tMj)

    mjBR = tMj & (osc_sig > rsF) & (mfi > 0)
    mjBL = tMj & (osc_sig < -rsF) & (mfi < 0)

    mnBR = tMn & (osc_sig > 20) & (osc_sig > osc_sgD) & (rsi > 0)
    mnBL = tMn & (osc_sig < -20) & (osc_sig < osc_sgD) & (rsi < 0)

    # FINAL SIGNAL
    last = -1
    signal = "HOLD"

    if mjBL.iloc[last] or mnBL.iloc[last]:
        signal = "BUY"

    if mjBR.iloc[last] or mnBR.iloc[last]:
        signal = "SELL"

    # print(osc_sig.iloc[-1, mfi.iloc[-1]])
    
    return {
        "signal": signal,
        "osc": round(float(osc_sig.iloc[last]), 2),
        "mfi": round(float(mfi.iloc[last]), 2),
    }
