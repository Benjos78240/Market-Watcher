from eodhd import APIClient
import os
from dotenv import load_dotenv
from pathlib import Path
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from scripts.utils.log_checker import check_and_alert_log
from datetime import datetime, timedelta

# Charger la clé API
load_dotenv(str(Path.home() / "market-watcher/config/.env"))
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
api = APIClient(EODHD_API_KEY)

def get_or_create_asset_id(ticker):
    conn = get_oracle_connection()
    cur = conn.cursor()
    logger.info(f"🔎 Recherche asset_id pour {ticker}")
    cur.execute("SELECT asset_id FROM assets WHERE ticker = :ticker", {"ticker": ticker})
    row = cur.fetchone()
    if row:
        asset_id = row[0]
        logger.info(f"✅ asset_id {asset_id} trouvé pour {ticker}")
    else:
        asset_id_var = cur.var(int)
        logger.info(f"➕ Création d'un nouvel asset pour {ticker}")
        cur.execute(
            "INSERT INTO assets (ticker) VALUES (:ticker) RETURNING asset_id INTO :asset_id",
            {"ticker": ticker, "asset_id": asset_id_var}
        )
        asset_id = asset_id_var.getvalue()[0]
        logger.info(f"✅ asset_id {asset_id} créé pour {ticker}")
        conn.commit()
    cur.close()
    conn.close()
    return asset_id

def update_user_watchlist_asset_id(ticker, asset_id):
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE user_watchlist SET asset_id = :asset_id WHERE ticker = :ticker AND (asset_id IS NULL OR asset_id <> :asset_id)",
        {"asset_id": asset_id, "ticker": ticker}
    )
    logger.info(f"🔄 Mise à jour user_watchlist: {ticker} -> asset_id {asset_id}")
    conn.commit()
    cur.close()
    conn.close()

def update_asset_status(asset_id, status):
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE assets SET last_extract_status = :status, last_extract_attempt = SYSDATE WHERE asset_id = :asset_id",
        {"status": status, "asset_id": asset_id}
    )
    conn.commit()
    cur.close()
    conn.close()

def update_asset_dates(asset_id):
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            MIN(price_date), 
            MAX(price_date)
        FROM prices
        WHERE asset_id = :asset_id
    """, {"asset_id": asset_id})
    row = cur.fetchone()
    date_min, date_max = row if row else (None, None)
    cur.execute("""
        UPDATE assets
        SET date_min = :date_min, date_max = :date_max
        WHERE asset_id = :asset_id
    """, {"date_min": date_min, "date_max": date_max, "asset_id": asset_id})
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🗓️ Dates min/max mises à jour pour asset_id {asset_id}: {date_min} / {date_max}")

def fetch_daily_data_eodhd(ticker, years=10):
    logger.info(f"🔎 Extraction daily EODHD pour {ticker} (max {years} ans)")
    try:
        date_to = datetime.today()
        date_from = date_to - timedelta(days=years*365)
        data = api.get_eod_historical_stock_market_data(
            symbol=ticker,
            period='d',
            order='a',
            from_date=date_from.strftime("%Y-%m-%d"),
            to_date=date_to.strftime("%Y-%m-%d")
        )
        results = []
        if data and isinstance(data, list):
            for entry in data:
                results.append({
                    "date": entry["date"],
                    "open": float(entry["open"]),
                    "close": float(entry["close"]),
                    "high": float(entry["high"]),
                    "low": float(entry["low"]),
                    "volume": float(entry["volume"]),
                    "dividend_amount": float(entry.get("dividend", 0)),
                    "split_coefficient": float(entry.get("split", 1))
                })
        logger.info(f"📈 {len(results)} lignes extraites pour {ticker}")
        return results
    except Exception as e:
        logger.error(f"❌ Erreur extraction EODHD pour {ticker}: {e}")
        return []

def fetch_last_daily_data_eodhd(ticker):
    logger.info(f"🔎 Extraction dernière daily EODHD pour {ticker}")
    try:
        data = api.get_eod_historical_stock_market_data(
            symbol=ticker,
            period='d',
            order='d'  # décroissant : la plus récente d'abord
        )
        if data and isinstance(data, list) and len(data) > 0:
            entry = data[0]
            logger.info(f"📈 Dernière ligne extraite pour {ticker}: {entry['date']}")
            return [{
                "date": entry["date"],
                "open": float(entry["open"]),
                "close": float(entry["close"]),
                "high": float(entry["high"]),
                "low": float(entry["low"]),
                "volume": float(entry["volume"]),
                "dividend_amount": float(entry.get("dividend", 0)),
                "split_coefficient": float(entry.get("split", 1))
            }]
        return []
    except Exception as e:
        logger.error(f"❌ Erreur extraction EODHD pour {ticker}: {e}")
        return []

def insert_prices(asset_id, ticker, prices):
    conn = get_oracle_connection()
    cur = conn.cursor()
    total = len(prices)
    for idx, p in enumerate(prices, 1):
        price_date = datetime.strptime(p["date"], "%Y-%m-%d")
        cur.execute("""
            MERGE INTO prices dst
            USING (SELECT :asset_id AS asset_id, :price_date AS price_date FROM dual) src
            ON (dst.asset_id = src.asset_id AND dst.price_date = src.price_date)
            WHEN MATCHED THEN
                UPDATE SET open_value = :open, close_value = :close, high_value = :high, low_value = :low, volume = :volume,
                           dividend_amount = :dividend, split_coefficient = :split, source = 4
            WHEN NOT MATCHED THEN
                INSERT (asset_id, price_date, open_value, close_value, high_value, low_value, volume, dividend_amount, split_coefficient, source)
                VALUES (:asset_id, :price_date, :open, :close, :high, :low, :volume, :dividend, :split, 4)
        """, {
            "asset_id": asset_id,
            "price_date": price_date,
            "open": p["open"],
            "close": p["close"],
            "high": p.get("high"),
            "low": p.get("low"),
            "volume": p.get("volume"),
            "dividend": p.get("dividend_amount"),
            "split": p.get("split_coefficient")
        })
        if idx % 500 == 0 or idx == total:
            logger.info(f"⏳ Progression {ticker}: {idx}/{total} lignes insérées")
    conn.commit()
    cur.close()
    conn.close()

def get_unique_tickers():
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT ticker FROM assets
        WHERE asset_type IS NULL OR asset_type <> 'CRYPTO'
    """)
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tickers

def batch_extract_and_insert():
    tickers =get_unique_tickers()#get_unique_tickers() ["IES.XETRA","2FE.XETRA","CRIN.XETRA"]  # Ou get_unique_tickers()
    for ticker in tickers:
        asset_id = get_or_create_asset_id(ticker)
        update_user_watchlist_asset_id(ticker, asset_id)
        prices = fetch_last_daily_data_eodhd(ticker)#fetch_last_daily_data_eodhd(ticker)  # <-- ici fetch_last_daily_data_eodhd(ticker)
        if prices:
            insert_prices(asset_id, ticker, prices)
            logger.info(f"✅ Dernière donnée insérée pour {ticker}")
            update_asset_status(asset_id, "OK")
        else:
            update_asset_status(asset_id, "ERROR")
        update_asset_dates(asset_id)

if __name__ == "__main__":
    batch_extract_and_insert()
    # Calcul des indicateurs après extraction
    from scripts.utils.maj_indicateurs_last_obs import update_last_obs_all_assets
    update_last_obs_all_assets('STOCK')
    update_last_obs_all_assets('ETF')
    check_and_alert_log("extraction_eodhd_", "extraction_eodhd_")