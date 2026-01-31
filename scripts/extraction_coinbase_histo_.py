import os
import requests
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz

from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger

# Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.expanduser('~/market-watcher/config/.env'))

SOURCE_COINBASE = 1
SOURCE_BINANCE = 4

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
            "INSERT INTO assets (ticker, asset_type) VALUES (:ticker, 'CRYPTO') RETURNING asset_id INTO :asset_id",
            {"ticker": ticker, "asset_id": asset_id_var}
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

def fetch_coinbase_last_1h(pair):
    """Récupère la dernière bougie 1h pour une paire."""
    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=3600"
    logger.info(f"📡 Requête Coinbase OHLC 1h pour {pair} : {url}")
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

def fetch_coinbase_ohlc_1h(pair):
    """Récupère toutes les bougies 1h disponibles pour une paire."""
    url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=3600"
    logger.info(f"📡 Requête Coinbase OHLC 1h pour {pair} : {url}")
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

def insert_price_binance(asset_id, pair, ohlc):
    """Insère la donnée dans la table prices (source Binance)."""
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
            "source": SOURCE_BINANCE
    })
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"✅ Données Binance insérées pour {pair} (asset_id={asset_id})")

def update_asset_dates_and_status(asset_id, ohlc_list, status):
    """Met à jour date_min, date_max, last_extract_status, last_extract_attempt dans assets."""
    if not ohlc_list:
        return
    dates = [ohlc["price_date"] for ohlc in ohlc_list]
    date_min = min(dates)
    date_max = max(dates)
    now = datetime.now(pytz.timezone("Europe/Paris"))
    conn = get_oracle_connection()
    cur = conn.cursor()
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
    for asset_id, pair in pairs:
        if not asset_id:
            asset_id = get_or_create_asset_id(pair)
        update_user_watchlist_asset_id(pair, asset_id)
        ohlc_list = fetch_coinbase_ohlc_1h(pair)
        status = "OK" if ohlc_list else "ERROR"
        if ohlc_list:
            for ohlc in ohlc_list:
                insert_price(asset_id, pair, ohlc)
            update_asset_dates_and_status(asset_id, ohlc_list, status)
        else:
            update_asset_dates_and_status(asset_id, [], status)

def fetch_binance_ohlc_1h(symbol, start_dt, end_dt):
    logger.info("🔄 Démarrage de l'extraction des donnée Binance")
    url = "https://api.binance.com/api/v3/klines"
    interval = "1h"
    limit = 1000
    all_ohlc = []
    max_span = timedelta(days=365)

    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + max_span, end_dt)
        start_ts = int(current_start.timestamp() * 1000)
        end_ts = int(current_end.timestamp() * 1000)
        while start_ts < end_ts:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ts,
                "endTime": min(start_ts + limit * 3600 * 1000, end_ts),
                "limit": limit
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            for ohlc in data:
                all_ohlc.append({
                    "price_date": datetime.fromtimestamp(ohlc[0] / 1000, pytz.UTC),
                    "open": float(ohlc[1]),
                    "high": float(ohlc[2]),
                    "low": float(ohlc[3]),
                    "close": float(ohlc[4]),
                    "price_value": float(ohlc[4]),
                    "volume": float(ohlc[5])
                })
            start_ts = data[-1][0] + 3600 * 1000
            time.sleep(0.2)
        current_start = current_end
    logger.info("🔄 Fin de l'extraction des donnée Binance")
    return all_ohlc



def get_missing_date_ranges(asset_id, start_dt, end_dt):
    """
    Retourne une liste de tuples (start, end) pour les plages horaires manquantes dans prices.
    """
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT price_date FROM prices
        WHERE asset_id = :asset_id
        AND price_date BETWEEN :start_dt AND :end_dt
        ORDER BY price_date
    """, {"asset_id": asset_id, "start_dt": start_dt, "end_dt": end_dt})
    existing_dates = set([row[0].replace(minute=0, second=0, microsecond=0) for row in cur.fetchall()])
    cur.close()
    conn.close()

    # Génère toutes les heures attendues
    all_hours = []
    dt = start_dt
    while dt <= end_dt:
        all_hours.append(dt.replace(minute=0, second=0, microsecond=0))
        dt += timedelta(hours=1)

    missing = sorted(set(all_hours) - existing_dates)
    # Regroupe les dates manquantes en plages continues
    ranges = []
    if missing:
        range_start = missing[0]
        prev = missing[0]
        for d in missing[1:]:
            if d - prev > timedelta(hours=1):
                ranges.append((range_start, prev))
                range_start = d
            prev = d
        ranges.append((range_start, prev))
    return ranges

def batch_extract_and_insert_binance():
    pairs = get_crypto_pairs()
    pairs = [(asset_id, pair) for asset_id, pair in pairs if pair in ("XRP-EUR", "XRPEUR")]
    for asset_id, pair in pairs:
        if not asset_id:
            asset_id = get_or_create_asset_id(pair)  # Ajoute cette ligne !
        binance_symbol = pair.replace("-", "").upper()
        start_dt = datetime(2024, 10, 18, tzinfo=pytz.UTC)
        end_dt = datetime(2025, 10, 18, tzinfo=pytz.UTC)
        missing_ranges = get_missing_date_ranges(asset_id, start_dt, end_dt)
        for range_start, range_end in missing_ranges:
            logger.info(f"⏳ Complétion Binance {binance_symbol} du {range_start} au {range_end}")
            ohlc_list = fetch_binance_ohlc_1h(binance_symbol, range_start, range_end)
            logger.info(f"📊 {len(ohlc_list)} bougies récupérées pour {binance_symbol} ({range_start} → {range_end})")
            for ohlc in ohlc_list:
                insert_price_binance(asset_id, pair, ohlc)

def fetch_with_retry_coinbase(pair, target_dt, max_attempts=5, delay=30):
    """Essaie de récupérer la bougie manquante via Coinbase avec plusieurs tentatives."""
    for attempt in range(1, max_attempts + 1):
        ohlc_list = fetch_coinbase_ohlc_1h(pair)
        for ohlc in ohlc_list:
            if ohlc["price_date"] == target_dt:
                logger.info(f"✅ Bougie {pair} {target_dt} récupérée à la tentative {attempt}.")
                return ohlc
        logger.warning(f"Bougie {pair} {target_dt} non trouvée, retry dans {delay}s... (tentative {attempt}/{max_attempts})")
        time.sleep(delay)
    logger.error(f"Bougie {pair} {target_dt} non trouvée après {max_attempts} tentatives.")
    return None

if __name__ == "__main__":
    # Extraction historique sur Binance pour la paire XRP-EUR
    batch_extract_and_insert_binance()