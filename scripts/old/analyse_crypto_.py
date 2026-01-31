import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.log_checker import check_and_alert_log
from scripts.utils.logger import logger  # <-- Ajout ici
from pathlib import Path
from dotenv import load_dotenv
import requests

load_dotenv(str(Path.home() / "market-watcher/config/.env"))

def get_assets():
    logger.info("Chargement des actifs CRYPTO depuis la base...")
    conn = get_oracle_connection()
    df = pd.read_sql("""
        SELECT asset_id, ticker, asset_name, asset_type
        FROM assets
        WHERE asset_type = 'CRYPTO'
    """, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    logger.info(f"{len(df)} actifs chargés.")
    return df

def get_prices(asset_id):
    logger.info(f"Chargement des prix pour asset_id={asset_id}...")
    conn = get_oracle_connection()
    df = pd.read_sql(f"""
        SELECT price_date, price_value as price
        FROM prices
        WHERE asset_id = {asset_id} AND price_value IS NOT NULL
        ORDER BY price_date
    """, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    logger.info(f"{len(df)} prix chargés pour asset_id={asset_id}.")
    return df

def compute_rsi(prices, window=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def send_discord_message(webhook_url, message):
    max_length = 1900  # On laisse un peu de marge pour le formatage
    for i in range(0, len(message), max_length):
        chunk = message[i:i+max_length]
        data = {"content": f"```md\n{chunk}\n```"}
        response = requests.post(webhook_url, json=data)
        if response.status_code != 204:
            logger.error(f"Erreur Discord: {response.status_code} - {response.text}")

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1389304198658785330/j07N_glKcSM7bFqUdGC9K3vQRFkwVtw34Eyf9xSbAwgQveEcziQKE1OaVrPATzrW_kyA"

def format_table(df, titre):
    if df.empty:
        logger.info(f"Aucun résultat pour {titre}.")
        return f"**{titre}**\nAucun résultat.\n"
    else:
        df = df.copy()
        header = " | ".join(f"{col:20}" for col in df.columns)
        lines = [header]
        for _, row in df.iterrows():
            line = " | ".join(f"{str(val):20}"[:20] for val in row)
            lines.append(line)
        logger.info(f"{len(df)} lignes formatées pour {titre}.")
        return f"**{titre}**\n" + "```\n" + "\n".join(lines) + "\n```\n"

if __name__ == "__main__":
    logger.info("Début du script analyse_crypto_.py")
    assets = get_assets()
    achats, ventes, conserver = [], [], []
    for _, row in assets.iterrows():
        df = get_prices(row["asset_id"])
        ma_window = 50  # Pour la crypto, MA50
        if len(df) < max(20, ma_window, 14):
            logger.warning(f"Pas assez d'historique pour {row['asset_name']} (asset_id={row['asset_id']})")
            continue
        df["ma"] = df["price"].rolling(ma_window).mean()
        df["rsi14"] = compute_rsi(df["price"], 14)
        last = df.iloc[-1]
        # Signal achat
        if last["rsi14"] < 30 and last["price"] < last["ma"]:
            achats.append({
                "asset_name": row["asset_name"],
                "asset_type": row["asset_type"],
                "dernier_prix": last["price"],
                "date": last["price_date"],
                "rsi": last["rsi14"],
                "ma": last["ma"]
            })
            logger.info(f"Signal achat détecté pour {row['asset_name']}")
        # Signal vente
        elif last["rsi14"] > 70 and last["price"] > last["ma"]:
            ventes.append({
                "asset_name": row["asset_name"],
                "asset_type": row["asset_type"],
                "dernier_prix": last["price"],
                "date": last["price_date"],
                "rsi": last["rsi14"],
                "ma": last["ma"]
            })
            logger.info(f"Signal vente détecté pour {row['asset_name']}")
        # À conserver
        elif 30 <= last["rsi14"] <= 70:
            conserver.append({
                "asset_name": row["asset_name"],
                "asset_type": row["asset_type"],
                "dernier_prix": last["price"],
                "date": last["price_date"],
                "rsi": last["rsi14"],
                "ma": last["ma"]
            })
            logger.info(f"À conserver : {row['asset_name']}")

    df_achats = pd.DataFrame(achats)
    df_ventes = pd.DataFrame(ventes)
    df_conserver = pd.DataFrame(conserver)

    logger.info("Préparation du message Discord.")
    message = (
        format_table(df_achats, "Signaux d'achat") +
        format_table(df_ventes, "Signaux de vente") +
        format_table(df_conserver, "Titres à conserver")
    )

    send_discord_message(DISCORD_WEBHOOK_URL, message)
    logger.info("Message Discord envoyé.")
    logger.info("Fin du script analyse_crypto_.py")
    check_and_alert_log("analyse_crypto_", "analyse_crypto_")
