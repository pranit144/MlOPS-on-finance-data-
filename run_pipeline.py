"""
run_pipeline.py
───────────────
Quick local runner — mimics exactly what GitHub Actions does.
Run with:  python run_pipeline.py
"""

import subprocess
import sys

steps = [
    ("Install dependencies",  [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]),
    # Standard daily refresh (1 week for the set of tickers)
    ("Fetch stock data (daily)", [sys.executable, "scripts/fetch_stock_data.py", "--period", "5d", "--interval", "1d"]),

    # Uncomment to run full 1-year 1-minute fetch (expensive and may take time) for all tickers
    # ("Fetch 1-year 1-minute stock data", [sys.executable, "scripts/fetch_stock_data.py", "--one-year-1m"]),
]

for name, cmd in steps:
    print(f"\n{'─'*50}")
    print(f"▶  {name}")
    print(f"{'─'*50}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\n❌ Step '{name}' failed with exit code {result.returncode}")
        sys.exit(result.returncode)

print("\n✅ Pipeline completed successfully!")
print("   Check  data/stock_data.csv  for the output.")
