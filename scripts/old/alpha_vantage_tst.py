import os
from pathlib import Path
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger


# 1. Charger la clé API Alpha Vantage depuis le fichier .env
load_dotenv(str(Path.home() / "market-watcher/config/.env"))
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def get_unique_assets():
    """
    2. Récupère la liste des actifs uniques à traiter (asset_id, asset_name, asset_type)
    depuis la table user_watchlist.
    Cela permet d'extraire chaque actif une seule fois, même s'il est suivi par plusieurs utilisateurs.
    """
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT asset_id, asset_name, asset_type FROM user_watchlist")
    assets = cur.fetchall()
    cur.close()
    conn.close()
    return assets

conn = get_oracle_connection()
if not conn:
    logger.error("❌ Connexion à la base Oracle impossible.")
    exit(1)
else:
    logger.info("✅ Connexion à la base Oracle OK.")
conn.close()


assets = get_unique_assets()
logger.info(f"Actifs trouvés dans user_watchlist : {assets}")
if not assets:
    logger.error("❌ Aucun actif trouvé dans user_watchlist. Vérifie que la table n'est pas vide.")
    exit(1)

assets = [a for a in assets if a[2] and a[2].upper() in ("STOCK", "ETF")]
logger.info(f"Actifs filtrés (STOCK/ETF) : {assets}")
if not assets:
    logger.error("❌ Aucun actif de type STOCK ou ETF trouvé. Vérifie la colonne asset_type.")
    exit(1)

def insert_prices(asset_id, df):
    """
    Insère les prix historiques dans la table prices pour un asset donné,
    en gérant l'absence de certaines colonnes (ex: 'adjusted close').
    """
    conn = get_oracle_connection()
    cur = conn.cursor()
    for date, row in df.iterrows():
        try:
            # Utilise 'adjusted close' si dispo, sinon 'close'
            #close :Cest le prix de clôture brut de l’actif à la fin
            #adjusted close :C’est le prix de clôture ajusté pour refléter les dividendes, splits, et autres opérations sur titres.
            price_value = float(row["adjusted close"]) if "adjusted close" in row else float(row["close"])
            cur.execute("""
                INSERT INTO prices (
                    asset_id, price_date, price_value, volume, source,
                    open_value, high_value, low_value, close_value, dividend_amount, split_coefficient
                )
                VALUES (
                    :asset_id, TO_TIMESTAMP(:price_date, 'YYYY-MM-DD'), :price_value, :volume, :source,
                    :open_value, :high_value, :low_value, :close_value, :dividend_amount, :split_coefficient
                )
            """, {
                "asset_id": asset_id,
                "price_date": date.strftime("%Y-%m-%d"),
                "price_value": price_value,
                "volume": int(row.get("volume", 0)),
                "source": "ALPHA_VANTAGE",
                "open_value": float(row.get("open", 0)),
                "high_value": float(row.get("high", 0)),
                "low_value": float(row.get("low", 0)),
                "close_value": float(row.get("close", 0)),
                "dividend_amount": float(row.get("dividend amount", 0)),
                "split_coefficient": float(row.get("split coefficient", 1))
            })
        except Exception as e:
            logger.error(f"❌ Erreur insertion prix {asset_id} {date}: {e}")
    conn.commit()
    cur.close()
    conn.close()



if __name__ == "__main__":
    # 5. Initialisation de l'API Alpha Vantage
    ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas')

test_asset = assets[10]
logger.info(f"Test extraction Alpha Vantage pour {test_asset[0]}")
try:
    data, _ =ts.get_daily(symbol=test_asset[0], outputsize='compact')
    data.columns = [col.split('. ')[1] for col in data.columns]
    logger.info(f"Colonnes récupérées : {data.columns}")
    logger.info(f"Premières lignes :\n{data.head()}")
    insert_prices(test_asset[0], data)
    logger.info(f"✅ Insertion dans la table prices terminée pour {test_asset[0]}")
except Exception as e:
    logger.error(f"❌ Erreur Alpha Vantage : {e}")
    exit(1)
    


