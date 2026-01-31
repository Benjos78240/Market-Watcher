import os
from pathlib import Path
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from scripts.utils.log_checker import check_and_alert_log
import time
import requests
from datetime import datetime

# Charger la clé API
load_dotenv(str(Path.home() / "market-watcher/config/.env"))
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

BATCH_SIZE = 5  # Taille du lot pour les requêtes par lot

def get_unique_tickers():
    conn = get_oracle_connection()
    cur = conn.cursor()
    # Priorise ceux en LIMIT ou ERROR, puis les autres
    cur.execute("""
        SELECT ticker FROM assets
        WHERE asset_type IS NULL OR asset_type <> 'CRYPTO'
        ORDER BY 
            CASE 
                WHEN last_extract_status = 'LIMIT' THEN 1
                WHEN last_extract_status = 'ERROR' THEN 2
                ELSE 3
            END, last_extract_attempt
    """)
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tickers

def get_asset_id(ticker):
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("SELECT asset_id FROM assets WHERE ticker = :ticker", {"ticker": ticker})
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

def fetch_daily_data(ticker):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}"
    logger.info(f"📡 Requête Alpha Vantage pour {ticker} : {url}")
    try:
        response = requests.get(url)
        logger.info(f"🌐 Statut HTTP {response.status_code} pour {ticker}")
        data = response.json()
        if "Error Message" in data:
            logger.error(f"❌ Alpha Vantage retourne une erreur pour {ticker}: {data['Error Message']}")
            update_asset_status(asset_id, "ERROR")
            return []
        if "Note" in data or "Information" in data:
            logger.warning(f"⏳ Alpha Vantage limite atteinte pour {ticker}: {data.get('Note') or data.get('Information')}")
            update_asset_status(asset_id, "LIMIT")
            return []
        ts = data.get("Time Series (Daily)", {})
        if not ts:
            logger.warning(f"⚠️ Pas de données 'Time Series (Daily)' pour {ticker}. Réponse brute: {data}")
        results = []
        for date_str, values in ts.items():
            results.append({
                "date": date_str,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),  # <-- Correction ici
                # Les champs suivants n'existent pas dans TIME_SERIES_DAILY
                "dividend": 0.0,
                "split": 1.0
            })
        logger.info(f"📈 {len(results)} lignes extraites pour {ticker}")
        return results
    except Exception as e:
        logger.error(f"❌ Erreur extraction Alpha Vantage pour {ticker}: {e}")
        return []

def insert_prices(asset_id, ticker, prices):
    conn = get_oracle_connection()
    cur = conn.cursor()
    for p in prices:
        price_date = datetime.strptime(p["date"], "%Y-%m-%d")
        cur.execute("""
            MERGE INTO prices dst
            USING (SELECT :asset_id AS asset_id, :price_date AS price_date FROM dual) src
            ON (dst.asset_id = src.asset_id AND dst.price_date = src.price_date)
            WHEN MATCHED THEN
                UPDATE SET close_value = :close, volume = :volume, source = 2
            WHEN NOT MATCHED THEN
                INSERT (asset_id, price_date, open_value, high_value, low_value, close_value, volume, dividend_amount, split_coefficient, source)
                VALUES (:asset_id, :price_date, :open, :high, :low, :close, :volume, :dividend, :split, 2)
        """, {
            "asset_id": asset_id,
            "price_date": price_date,
            "open": p["open"],
            "high": p["high"],
            "low": p["low"],
            "close": p["close"],
            "volume": p["volume"],
            "dividend": p["dividend"],
            "split": p["split"]
        })
    conn.commit()
    cur.close()
    conn.close()

def get_or_create_asset_id(ticker):
    """Récupère l'asset_id pour un ticker, le crée dans assets si besoin."""
    conn = get_oracle_connection()
    cur = conn.cursor()
    logger.info(f"🔎 Recherche asset_id pour {ticker}")
    cur.execute("SELECT asset_id FROM assets WHERE ticker = :ticker", {"ticker": ticker})
    row = cur.fetchone()
    if row:
        asset_id = row[0]
        logger.info(f"✅ asset_id {asset_id} trouvé pour {ticker}")
    else:
        # Création de l'asset
        asset_id_var = cur.var(int)
        logger.info(f"➕ Création d'un nouvel asset pour {ticker}")
        cur.execute(
            "INSERT INTO assets (ticker) VALUES (:ticker) RETURNING asset_id INTO :asset_id",
            {"ticker": ticker, "asset_id": asset_id_var}
        )
        asset_id = asset_id_var.getvalue()[0]  # <-- Correction ici
        logger.info(f"✅ asset_id {asset_id} créé pour {ticker}")
        conn.commit()
    cur.close()
    conn.close()
    return asset_id

def update_user_watchlist_asset_id(ticker, asset_id):
    """Met à jour user_watchlist pour associer asset_id au ticker si absent."""
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
    """Met à jour date_min et date_max dans assets à partir de prices."""
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

def batch_extract_and_insert():
    tickers = get_unique_tickers()
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        for ticker in batch:
            asset_id = get_or_create_asset_id(ticker)
            update_user_watchlist_asset_id(ticker, asset_id)
            prices = fetch_daily_data(ticker)
            if prices:
                insert_prices(asset_id, ticker, prices)
                logger.info(f"✅ Données insérées pour {ticker}")
            update_asset_status(asset_id, "OK")
            update_asset_dates(asset_id)  # <-- Ajout ici
            time.sleep(12)  # Respecte la limite Alpha Vantage
        logger.info(f"Batch {i//BATCH_SIZE+1} traité.")

if __name__ == "__main__":
    batch_extract_and_insert()
    check_and_alert_log("extraction_alpha_vantage_", "extraction_alpha_vantage_")