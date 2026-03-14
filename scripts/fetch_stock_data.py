import argparse
import os
from datetime import datetime

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


def format_output_path(ticker):
    filename = f"{ticker.replace('/', '_')}.csv"
    return os.path.join("data", filename)


def transform_dataframe(df):
    if df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "Date"})

    if "Date" not in df.columns:
        df.reset_index(inplace=True)

    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")
    return df


def fetch_ticker_data(ticker, period="5d", interval="1d"):
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


def append_or_create_csv(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        existing = pd.read_csv(path)
        combined = pd.concat([existing, data], ignore_index=True)
        combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        combined.sort_values("Date", inplace=True, ignore_index=True)
        combined.to_csv(path, index=False)
        return len(combined)

    data.to_csv(path, index=False)
    return len(data)


def main():
    parser = argparse.ArgumentParser(description="Daily stock data fetcher for yfinance tickers")
    parser.add_argument("--ticker-list", type=str, default="",
                        help="Comma-separated tickers (e.g. AAPL,MSFT); if empty, reads data/tickers.txt or default list")
    parser.add_argument("--period", type=str, default="5d", help="yfinance period to fetch; recommended 2d/5d")
    parser.add_argument("--interval", type=str, default="1d", help="yfinance interval")
    args = parser.parse_args()

    tickers = effective_ticker_list(args)
    if not tickers:
        raise SystemExit("No tickers found to fetch. Create data/tickers.txt or pass --ticker-list.")

    summary = {"fetched": 0, "failed": 0}

    print(f"[{datetime.now()}] Starting fetch for {len(tickers)} tickers: {', '.join(tickers)}")

    for ticker in tickers:
        print(f"[{datetime.now()}] Fetching {ticker} ...")
        df = fetch_ticker_data(ticker, period=args.period, interval=args.interval)
        if df is None or df.empty:
            print(f"  ✖ No data for {ticker}; skipping")
            summary["failed"] += 1
            continue

        out_path = format_output_path(ticker)
        total_rows = append_or_create_csv(out_path, df)
        print(f"  ✓ {ticker} saved to {out_path} (rows={total_rows})")
        summary["fetched"] += 1

    print(f"[{datetime.now()}] Done. fetched={summary['fetched']}, failed={summary['failed']}")


if __name__ == "__main__":
    main()
