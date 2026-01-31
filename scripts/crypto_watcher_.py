import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from pathlib import Path
from dotenv import load_dotenv
from scripts.utils.log_checker import check_and_alert_log
from scripts.utils.logger import logger
from scripts.utils.discord_manager import DiscordManager  # Import du DiscordManager
import os

load_dotenv(str(Path.home() / "market-watcher/config/.env"))

# Configuration Discord
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_CRYPTO_WATCHER")

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

def generate_crypto_signals_message(achats, ventes, conserver):
    """Génère un message Discord formaté pour les signaux crypto"""
    
    message = "🚀 **SIGNALEMENT CRYPTO - Analyse RSI/MA50**\n\n"
    
    # Section ACHAT
    if achats:
        message += "🟢 **SIGNAUX D'ACHAT** (RSI < 30 & Prix < MA50)\n"
        for achat in achats:
            message += (
                f"• **{achat['asset_name']}**\n"
                f"  Prix: {achat['dernier_prix']:.2f}€ | "
                f"RSI: {achat['rsi']:.1f} | "
                f"MA50: {achat['ma']:.2f}€\n"
            )
        message += "\n"
    
    # Section VENTE
    if ventes:
        message += "🔴 **SIGNAUX DE VENTE** (RSI > 70 & Prix > MA50)\n"
        for vente in ventes:
            message += (
                f"• **{vente['asset_name']}**\n"
                f"  Prix: {vente['dernier_prix']:.2f}€ | "
                f"RSI: {vente['rsi']:.1f} | "
                f"MA50: {vente['ma']:.2f}€\n"
            )
        message += "\n"
    
    # Section CONSERVER
    if conserver:
        message += "🟡 **À CONSERVER** (RSI entre 30-70)\n"
        for conserve in conserver:
            message += (
                f"• **{conserve['asset_name']}**\n"
                f"  Prix: {conserve['dernier_prix']:.2f}€ | "
                f"RSI: {conserve['rsi']:.1f} | "
                f"MA50: {conserve['ma']:.2f}€\n"
            )
        message += "\n"
    
    # Message si aucun signal
    if not achats and not ventes and not conserver:
        message += "📊 Aucun signal détecté pour le moment.\n"
    
    message += f"\n⏰ Dernière mise à jour: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    return message

if __name__ == "__main__":
    logger.info("Début du script crypto_watcher.py")
    
    # Initialisation du DiscordManager
    discord_manager = DiscordManager(DISCORD_WEBHOOK_URL)
    
    assets = get_assets()
    achats, ventes, conserver = [], [], []
    
    for _, row in assets.iterrows():
        df = get_prices(row["asset_id"])
        ma_window = 50  # MA50 pour la crypto
        
        if len(df) < max(20, ma_window, 14):
            logger.warning(f"Pas assez d'historique pour {row['asset_name']} (asset_id={row['asset_id']})")
            continue
            
        df["ma"] = df["price"].rolling(ma_window).mean()
        df["rsi14"] = compute_rsi(df["price"], 14)
        last = df.iloc[-1]
        
        # Vérification des valeurs NaN
        if pd.isna(last["rsi14"]) or pd.isna(last["ma"]):
            logger.warning(f"Valeurs manquantes pour {row['asset_name']}")
            continue
            
        # Signal achat
        if last["rsi14"] < 30 and last["price"] < last["ma"]:
            achats.append({
                "asset_name": row["asset_name"],
                "ticker": row["ticker"],
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
                "ticker": row["ticker"],
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
                "ticker": row["ticker"],
                "dernier_prix": last["price"],
                "date": last["price_date"],
                "rsi": last["rsi14"],
                "ma": last["ma"]
            })
            logger.info(f"À conserver : {row['asset_name']}")

    # Génération du message
    message = generate_crypto_signals_message(achats, ventes, conserver)

    # Envoi via DiscordManager uniquement s'il y a un signal d'achat ou de vente
    if achats or ventes:
        discord_manager.send_detailed_report("crypto_watcher", message)
        logger.info("Message Discord envoyé.")
    else:
        logger.info("Aucun signal d'achat ou de vente détecté, aucun message envoyé.")

    logger.info("Fin du script crypto_watcher_.py")
    check_and_alert_log("crypto_watcher_", "crypto_watcher_")
