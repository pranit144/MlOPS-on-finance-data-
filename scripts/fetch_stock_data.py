import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
TICKER    = "AAPL"          # Change to any stock symbol
PERIOD    = "1mo"           # Last 1 month of data
INTERVAL  = "1d"            # Daily candles
FILE_PATH = "data/stock_data.csv"

# ─────────────────────────────────────────────
# 1. Download latest data from Yahoo Finance
# ─────────────────────────────────────────────
print(f"[{datetime.now()}] Fetching data for {TICKER} ...")

raw = yf.download(
    tickers=TICKER,
    period=PERIOD,
    interval=INTERVAL,
    auto_adjust=True,
    progress=False,
    multi_level_index=False,   # yfinance >=0.2.52 — flat columns for single ticker
)

# Fallback: if multi-level columns still present, flatten them
if isinstance(raw.columns, pd.MultiIndex):
    raw.columns = [col[0] for col in raw.columns]

raw.reset_index(inplace=True)

# Rename 'Datetime' → 'Date' if interval < 1d returns Datetime column
if "Datetime" in raw.columns:
    raw.rename(columns={"Datetime": "Date"}, inplace=True)

# Keep only canonical OHLCV columns
raw = raw[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()

# Normalise Date to YYYY-MM-DD string so dedup works regardless of timezone
raw["Date"] = pd.to_datetime(raw["Date"]).dt.strftime("%Y-%m-%d")

# Round price columns to 4 decimal places
for col in ["Open", "High", "Low", "Close"]:
    raw[col] = pd.to_numeric(raw[col], errors="coerce").round(4)

raw["Volume"] = pd.to_numeric(raw["Volume"], errors="coerce").astype("Int64")

print(f"  → Downloaded {len(raw)} rows.")

# ─────────────────────────────────────────────
# 2. Append to existing CSV (or create fresh)
# ─────────────────────────────────────────────
os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

if os.path.exists(FILE_PATH) and os.path.getsize(FILE_PATH) > 0:
    old_data = pd.read_csv(FILE_PATH)
    combined = pd.concat([old_data, raw], ignore_index=True)
    combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    combined.sort_values("Date", inplace=True, ignore_index=True)
    combined.to_csv(FILE_PATH, index=False)
    print(f"  → Appended. Total rows in CSV: {len(combined)}")
else:
    raw.to_csv(FILE_PATH, index=False)
    print(f"  → Created new file with {len(raw)} rows.")

print(f"[{datetime.now()}] ✅ Data updated successfully!")
print(f"   📄 File: {FILE_PATH}")
