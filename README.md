# 📈 Stock Data Pipeline — GitHub Actions MLOps

A **100% free**, automated stock data pipeline powered by **GitHub Actions** and **yfinance**.

---

## 🏗️ Architecture

```
yfinance API  →  Python Script  →  CSV Dataset  →  Git Commit  →  GitHub Repo
                                                                      ↑
                                          Monthly Scheduler (GitHub Actions)
```

---

## 📁 Project Structure

```
kproject_ML_OPS/
│
├── data/
│   └── stock_data.csv          ← Auto-generated & auto-updated
│
├── scripts/
│   └── fetch_stock_data.py     ← Fetches & appends stock data
│
├── .github/
│   └── workflows/
│       └── pipeline.yml        ← GitHub Actions automation
│
├── requirements.txt
└── README.md
```

---

## ⚙️ How It Works

| Step | Action |
|------|--------|
| 1 | GitHub Actions triggers on **every 15th of the month** at 00:00 UTC |
| 2 | Python script runs — calls **yfinance** to download the last month's AAPL data |
| 3 | New data is **appended** to `data/stock_data.csv` (duplicates removed by Date) |
| 4 | Updated CSV is **committed & pushed** back to the repo automatically |

---

## 🚀 Setup Steps

1. **Fork or clone** this repository to your GitHub account.
2. No secrets needed — the default `GITHUB_TOKEN` handles push permissions.
3. Go to **Actions** tab → enable workflows if prompted.
4. (Optional) **Manual run**: Actions → *Stock Data Pipeline* → *Run workflow*.

---

## 🔧 Configuration

Edit `scripts/fetch_stock_data.py` to change:

```python
TICKER   = "AAPL"   # Any Yahoo Finance symbol: TSLA, MSFT, GOOGL ...
PERIOD   = "1mo"    # Data window per run
INTERVAL = "1d"     # Candle interval: 1d, 1h, 15m ...
```

---

## 📊 Sample Data

```
Date,Open,High,Low,Close,Volume
2026-02-15,180.0,182.0,178.0,181.0,5000000
2026-02-16,181.0,183.0,179.0,182.0,5100000
```

---

## 🛣️ MLOps Upgrade Roadmap

```
Step 1  ✅ Data Fetch (this project)
Step 2  🔜 Data Validation
Step 3  🔜 Feature Engineering
Step 4  🔜 Model Training (scikit-learn / XGBoost)
Step 5  🔜 Model Registry (MLflow)
Step 6  🔜 API Deployment (FastAPI)
```

---

## 🛠️ Run Locally

```bash
pip install -r requirements.txt
python scripts/fetch_stock_data.py
```
