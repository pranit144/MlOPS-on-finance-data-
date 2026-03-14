import argparse
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf


DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "BRK-B", "JPM", "V",
    "JNJ", "PG", "XOM", "BAC", "WMT", "DIS", "MA", "ADBE", "CRM", "NFLX"
]


def normalise_ticker(ticker):
    return ticker.strip().upper().replace(".", "-")


def read_tickers_from_file(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    return [normalise_ticker(t) for t in lines]


def effective_ticker_list(args):
    if args.ticker_list:
        return [normalise_ticker(t) for t in args.ticker_list.split(",") if t.strip()]

    file_tickers = read_tickers_from_file("data/tickers.txt")
    if file_tickers:
        return file_tickers

    return DEFAULT_TICKERS


def transform_dataframe(df):
    if df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "Date"})

    if "Date" not in df.columns:
        df.reset_index(inplace=True)

    df = df.rename(columns={"Adj Close": "Adj_Close"})
    df["Date"] = pd.to_datetime(df["Date"])

    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")

    # convert Date to string consistently for storage and dedupe
    if (df["Date"].dt.time == pd.Timestamp("00:00").time()).all():
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    else:
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d %H:%M")

    return df


def fetch_ticker_data_period(ticker, period="5d", interval="1d"):
    raw = yf.download(
        tickers=ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    if raw is None or raw.empty:
        return None

    return transform_dataframe(raw)


def fetch_ticker_data_1y_1m(ticker):
    # yfinance generally returns 1m data for max ~7 days in one call
    # so we chunk 1 year into 7-day windows and combine
    end = pd.Timestamp.utcnow()
    start = end - pd.DateOffset(years=1)
    chunk_days = 7

    segments = []
    cursor = start

    while cursor < end:
        slice_end = min(cursor + pd.Timedelta(days=chunk_days), end)
        print(f"   - fetching window {cursor.date()} → {slice_end.date()}")

        raw = yf.download(
            tickers=ticker,
            start=cursor.strftime("%Y-%m-%d"),
            end=slice_end.strftime("%Y-%m-%d"),
            interval="1m",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        if raw is not None and not raw.empty:
            segments.append(raw)
        else:
            print(f"     ⚠ no data in this segment {cursor.date()} -> {slice_end.date()}")

        cursor = slice_end + pd.Timedelta(minutes=1)

    if not segments:
        return None

    combined = pd.concat(segments)
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.sort_index()

    df = transform_dataframe(combined)
    if df is None or df.empty:
        return None

    return df


def append_or_create_csv(path, data, dedupe_key="Date"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        existing = pd.read_csv(path)
        combined = pd.concat([existing, data], ignore_index=True)
        combined.drop_duplicates(subset=[dedupe_key], keep="last", inplace=True)
        combined = combined.sort_values(dedupe_key, ignore_index=True)
        combined.to_csv(path, index=False)
        return len(combined)

    data.to_csv(path, index=False)
    return len(data)


def main():
    parser = argparse.ArgumentParser(description="yfinance stock data fetcher")
    parser.add_argument("--ticker-list", type=str, default="",
                        help="Comma-separated tickers (e.g. AAPL,MSFT); if empty, reads data/tickers.txt or default list")
    parser.add_argument("--period", type=str, default="5d", help="yfinance period to fetch (e.g. 5d, 1mo, 1y)")
    parser.add_argument("--interval", type=str, default="1d", help="yfinance interval (e.g. 1m, 5m, 1d)")
    parser.add_argument("--one-year-1m", action="store_true", help="Fetch full 1-year 1-minute data using chunked requests")
    parser.add_argument("--output-dir", type=str, default="data", help="Destination folder for output CSVs")
    args = parser.parse_args()

    tickers = effective_ticker_list(args)
    if not tickers:
        raise SystemExit("No tickers found to fetch. Create data/tickers.txt or pass --ticker-list.")

    summary = {"fetched": 0, "failed": 0}

    if args.one_year_1m:
        output_main = os.path.join(args.output_dir, "one_year_data")
        os.makedirs(output_main, exist_ok=True)
        print(f"[{datetime.now()}] Starting 1-year, 1-minute fetch for {len(tickers)} tickers...")

        for ticker in tickers:
            print(f"[{datetime.now()}] Fetching 1yr 1m {ticker} ...")
            df = fetch_ticker_data_1y_1m(ticker)
            if df is None or df.empty:
                print(f"  ✖ No 1yr 1m data for {ticker}; skip")
                summary["failed"] += 1
                continue

            out_path = os.path.join(output_main, f"{ticker.replace('/', '_')}_1y_1m.csv")
            total_rows = append_or_create_csv(out_path, df, dedupe_key="Date")
            print(f"  ✓ {ticker} saved to {out_path} (rows={total_rows})")
            summary["fetched"] += 1

        print(f"[{datetime.now()}] Done. fetched={summary['fetched']}, failed={summary['failed']}")
        return

    out_dir = args.output_dir
    os.makedirs(out_dir, exist_ok=True)
    print(f"[{datetime.now()}] Starting fetch for {len(tickers)} tickers with period={args.period}, interval={args.interval}")

    for ticker in tickers:
        print(f"[{datetime.now()}] Fetching {ticker} ...")
        df = fetch_ticker_data_period(ticker, period=args.period, interval=args.interval)
        if df is None or df.empty:
            print(f"  ✖ No data for {ticker}; skipping")
            summary["failed"] += 1
            continue

        out_path = os.path.join(out_dir, f"{ticker.replace('/', '_')}.csv")
        total_rows = append_or_create_csv(out_path, df, dedupe_key="Date")
        print(f"  ✓ {ticker} saved to {out_path} (rows={total_rows})")
        summary["fetched"] += 1

    print(f"[{datetime.now()}] Done. fetched={summary['fetched']}, failed={summary['failed']}")


if __name__ == "__main__":
    main()
