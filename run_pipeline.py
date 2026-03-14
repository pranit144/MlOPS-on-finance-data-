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
    ("Fetch stock data",      [sys.executable, "scripts/fetch_stock_data.py", "--period", "5d", "--interval", "1d"]),
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
