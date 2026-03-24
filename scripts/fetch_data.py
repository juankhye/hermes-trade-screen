"""
Hermès Trade Screen — Data Fetcher
Fetches live market data and writes to data/live-data.json
Run locally or via GitHub Actions on a schedule.
"""
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Data output path
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "live-data.json"


def fetch_yahoo_finance():
    """Fetch stock data using yfinance as fallback, or urllib if unavailable."""
    symbols = {
        "RMS.PA": "Hermes",
        "MC.PA": "LVMH",
        "KER.PA": "Kering",
        "CFR.SW": "Richemont",
        "BRBY.L": "Burberry",
        "BC.MI": "Cucinelli",
        "BZ=F": "Brent Crude",
        "GC=F": "Gold",
        "GB.L": "Global Blue",
        "EURUSD=X": "EUR/USD",
    }

    results = {}

    # Try yfinance first
    try:
        import yfinance as yf

        for sym, name in symbols.items():
            try:
                ticker = yf.Ticker(sym)
                hist = ticker.history(period="6mo")
                if hist.empty:
                    continue
                info = ticker.fast_info
                closes = hist["Close"].tolist()
                results[sym] = {
                    "name": name,
                    "price": round(closes[-1], 2) if closes else None,
                    "prevClose": round(closes[-2], 2) if len(closes) > 1 else None,
                    "currency": getattr(info, "currency", "EUR"),
                    "closes": [round(c, 2) for c in closes],
                    "dates": [d.strftime("%Y-%m-%d") for d in hist.index],
                }
                print(f"  [OK] {sym} ({name}): {results[sym]['price']}")
            except Exception as e:
                print(f"  [FAIL] {sym} ({name}): {e}")
        return results
    except ImportError:
        pass

    # Fallback: urllib direct to Yahoo Finance API
    import urllib.request
    import ssl

    ctx = ssl.create_default_context()
    headers = {"User-Agent": "Mozilla/5.0"}

    for sym, name in symbols.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=6mo&interval=1d"
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            result = data["chart"]["result"][0]
            meta = result["meta"]
            closes = [c for c in result["indicators"]["quote"][0]["close"] if c is not None]
            timestamps = result["timestamp"]
            dates = [datetime.utcfromtimestamp(t).strftime("%Y-%m-%d") for t in timestamps]
            results[sym] = {
                "name": name,
                "price": round(meta["regularMarketPrice"], 2),
                "prevClose": round(meta["chartPreviousClose"], 2),
                "currency": meta.get("currency", "EUR"),
                "closes": [round(c, 2) for c in closes],
                "dates": dates[: len(closes)],
            }
            print(f"  [OK] {sym} ({name}): {results[sym]['price']}")
        except Exception as e:
            print(f"  [FAIL] {sym} ({name}): {e}")

    return results


def fetch_exchange_rates():
    """Fetch FX rates."""
    import urllib.request

    rates = {}
    try:
        url = "https://api.exchangerate-api.com/v4/latest/EUR"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        rates["EUR_CNY"] = round(data["rates"].get("CNY", 0), 4)
        rates["EUR_USD"] = round(data["rates"].get("USD", 0), 4)
        rates["EUR_GBP"] = round(data["rates"].get("GBP", 0), 4)
        print(f"  [OK] FX: EUR/CNY={rates['EUR_CNY']}, EUR/USD={rates['EUR_USD']}")
    except Exception as e:
        print(f"  [FAIL] FX rates: {e}")
    return rates


def fetch_akshare_china():
    """Fetch China macro data via akshare."""
    china_data = {}
    try:
        import akshare as ak

        # China Consumer Goods Retail Sales
        try:
            df = ak.macro_china_consumer_goods_retail()
            if df is not None and not df.empty:
                recent = df.tail(12)
                china_data["retail_sales"] = {
                    "dates": recent.iloc[:, 0].astype(str).tolist(),
                    "values": recent.iloc[:, 1].astype(float).tolist(),
                }
                print(f"  [OK] China Retail Sales: {len(china_data['retail_sales']['values'])} points")
        except Exception as e:
            print(f"  [FAIL] China Retail Sales: {e}")

        # CSI 300 Index
        try:
            df = ak.stock_zh_index_daily_em(symbol="sh000300")
            if df is not None and not df.empty:
                recent = df.tail(120)
                china_data["csi300"] = {
                    "dates": recent["date"].astype(str).tolist(),
                    "closes": recent["close"].astype(float).round(2).tolist(),
                }
                print(f"  [OK] CSI 300: {len(china_data['csi300']['closes'])} points")
        except Exception as e:
            print(f"  [FAIL] CSI 300: {e}")

    except ImportError:
        print("  [WARN] akshare not available, skipping China data")

    return china_data


def main():
    print("=" * 60)
    print("Hermes Trade Screen - Data Fetcher")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    output = {
        "updated_at": datetime.now().isoformat(),
        "stocks": {},
        "fx": {},
        "china": {},
    }

    print("\n[1/3] Fetching stock data...")
    output["stocks"] = fetch_yahoo_finance()

    print("\n[2/3] Fetching exchange rates...")
    output["fx"] = fetch_exchange_rates()

    print("\n[3/3] Fetching China macro data...")
    output["china"] = fetch_akshare_china()

    # Write output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n[OK] Data written to {OUTPUT_FILE}")
    print(f"  Stocks: {len(output['stocks'])} symbols")
    print(f"  FX rates: {len(output['fx'])} pairs")
    print(f"  China data: {len(output['china'])} series")


if __name__ == "__main__":
    main()
