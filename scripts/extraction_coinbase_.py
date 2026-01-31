import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import time

from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from scripts.utils.log_checker import check_and_alert_log


# Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.expanduser('~/market-watcher/config/.env'))

SOURCE_COINBASE = 1

def get_crypto_pairs():
    """Récupère les paires CRYPTO à extraire depuis la table assets."""
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("SELECT asset_id, ticker FROM assets WHERE asset_type = 'CRYPTO'")
    pairs = cur.fetchall()
    cur.close()
    conn.close()
    return pairs

def get_or_create_asset_id(ticker):
    """Récupère ou crée un asset_id pour une paire."""
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("SELECT asset_id FROM assets WHERE ticker = :ticker", {"ticker": ticker})
    row = cur.fetchone()
    if row:
        asset_id = row[0]
        logger.info(f"✅ asset_id {asset_id} trouvé pour {ticker}")
    else:
        asset_id_var = cur.var(int)
        logger.info(f"➕ Création d'un nouvel asset pour {ticker}")
        cur.execute(
            "INSERT INTO assets (ticker, asset_type, source) VALUES (:ticker, 'CRYPTO', :source) RETURNING asset_id INTO :asset_id",
            {"ticker": ticker, "source": SOURCE_COINBASE, "asset_id": asset_id_var}
        )
        asset_id = asset_id_var.getvalue()[0]
        conn.commit()
        logger.info(f"✅ asset_id {asset_id} créé pour {ticker}")
    cur.close()
    conn.close()
    return asset_id

def update_user_watchlist_asset_id(ticker, asset_id):
    """Met à jour user_watchlist pour associer asset_id au ticker."""
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

def fetch_coinbase_ohlc(pair, granularity=21600):
    """Récupère toutes les données OHLC pour une paire avec la granularité choisie (ex: 6h)."""
    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity={granularity}"
    logger.info(f"📡 Requête Coinbase OHLC pour {pair} : {url}")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logger.warning(f"⚠️ Pas de données OHLC pour {pair}")
            return []
        ohlc_list = []
        for ohlc in data:
            utc_dt = datetime.fromtimestamp(ohlc[0], tz=pytz.UTC)
            paris_dt = utc_dt.astimezone(pytz.timezone("Europe/Paris"))
            ohlc_list.append({
                "price_date": paris_dt,
                "open": ohlc[3],
                "high": ohlc[2],
                "low": ohlc[1],
                "close": ohlc[4],
                "price_value": ohlc[4],
                "volume": ohlc[5]
            })
        return ohlc_list
    except Exception as e:
        logger.error(f"❌ Erreur extraction Coinbase pour {pair}: {e}")
        return []

def fetch_coinbase_last_6h(pair):
    """Récupère la dernière bougie 6h pour une paire."""
    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=21600"
    logger.info(f"📡 Requête Coinbase OHLC 6h pour {pair} : {url}")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logger.warning(f"⚠️ Pas de données OHLC pour {pair}")
            return None
        ohlc = data[0]  # La bougie la plus récente
        utc_dt = datetime.fromtimestamp(ohlc[0], tz=pytz.UTC)
        paris_dt = utc_dt.astimezone(pytz.timezone("Europe/Paris"))
        return {
            "price_date": paris_dt,
            "open": ohlc[3],
            "high": ohlc[2],
            "low": ohlc[1],
            "close": ohlc[4],
            "price_value": ohlc[4],
            "volume": ohlc[5]
        }
    except Exception as e:
        logger.error(f"❌ Erreur extraction Coinbase pour {pair}: {e}")
        return None

def insert_price(asset_id, pair, ohlc):
    """Insère la donnée dans la table prices."""
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO prices dst
        USING (SELECT :asset_id AS asset_id, :price_date AS price_date FROM dual) src
        ON (dst.asset_id = src.asset_id AND dst.price_date = src.price_date)
        WHEN MATCHED THEN
            UPDATE SET price_value = :price_value, open_value = :open, high_value = :high, low_value = :low, close_value = :close, volume = :volume, source = :source
        WHEN NOT MATCHED THEN
            INSERT (asset_id, price_date, price_value, open_value, high_value, low_value, close_value, volume, source)
            VALUES (:asset_id, :price_date, :price_value, :open, :high, :low, :close, :volume, :source)
    """, {
        "asset_id": asset_id,
        "price_date": ohlc["price_date"],
        "price_value": ohlc["price_value"],
        "open": ohlc["open"],
        "high": ohlc["high"],
        "low": ohlc["low"],
        "close": ohlc["close"],
        "volume": ohlc["volume"],
        "source": SOURCE_COINBASE
    })
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"✅ Données insérées pour {pair} (asset_id={asset_id})")

def update_asset_dates_and_status(asset_id, ohlc_list, status):
    """Met à jour date_min, date_max, last_extract_status, last_extract_attempt dans assets."""
    now = datetime.now(pytz.timezone("Europe/Paris"))
    conn = get_oracle_connection()
    cur = conn.cursor()
    # Récupère date_min et date_max depuis la table prices
    cur.execute("""
        SELECT MIN(price_date), MAX(price_date)
        FROM prices
        WHERE asset_id = :asset_id
    """, {"asset_id": asset_id})
    row = cur.fetchone()
    date_min, date_max = row if row else (None, None)
    cur.execute("""
        UPDATE assets
        SET date_min = :date_min,
            date_max = :date_max,
            last_extract_status = :status,
            last_extract_attempt = :attempt
        WHERE asset_id = :asset_id
    """, {
        "date_min": date_min,
        "date_max": date_max,
        "status": status,
        "attempt": now,
        "asset_id": asset_id
    })
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"🗓️ assets mis à jour pour asset_id={asset_id} ({date_min} → {date_max}, status={status})")

def batch_extract_and_insert():
    pairs = get_crypto_pairs()
    now = datetime.now(pytz.timezone("Europe/Paris"))
    last_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    for asset_id, pair in pairs:
        if not asset_id:
            asset_id = get_or_create_asset_id(pair)
        update_user_watchlist_asset_id(pair, asset_id)
        ohlc = fetch_coinbase_ohlc_for_hour(pair, last_hour)
        status = "OK" if ohlc else "ERROR"
        if ohlc:
            insert_price(asset_id, pair, ohlc)
            update_asset_dates_and_status(asset_id, [ohlc], status)
        else:
            update_asset_dates_and_status(asset_id, [], status)

def fetch_coinbase_ohlc_for_hour(pair, target_dt, max_retries=5, retry_delay=30):
    """Récupère la bougie 1h pour une heure précise (target_dt = datetime Europe/Paris) avec retry."""
    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=3600"
    for attempt in range(max_retries):
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        found = False
        for ohlc in data:
            utc_dt = datetime.fromtimestamp(ohlc[0], tz=pytz.UTC)
            paris_dt = utc_dt.astimezone(pytz.timezone("Europe/Paris"))
            # On compare l'heure exacte (sans les minutes/secondes)
            if paris_dt.replace(minute=0, second=0, microsecond=0) == target_dt.replace(minute=0, second=0, microsecond=0):
                return {
                    "price_date": paris_dt,
                    "open": ohlc[3],
                    "high": ohlc[2],
                    "low": ohlc[1],
                    "close": ohlc[4],
                    "price_value": ohlc[4],
                    "volume": ohlc[5]
                }
        if attempt < max_retries - 1:
            logger.warning(f"Bougie {pair} {target_dt} non trouvée, retry dans {retry_delay}s...")
            time.sleep(retry_delay)
    logger.error(f"Bougie {pair} {target_dt} non trouvée après {max_retries} tentatives.")
    return None

if __name__ == "__main__":
    batch_extract_and_insert()
    from scripts.utils.maj_indicateurs_last_obs import update_last_obs_all_assets
    update_last_obs_all_assets('CRYPTO')
    check_and_alert_log("extraction_coinbase_", "extraction_coinbase_")