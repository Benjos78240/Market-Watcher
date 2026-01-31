import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from pathlib import Path
from dotenv import load_dotenv
from datetime import timedelta, datetime
import numpy as np
import requests
from scripts.utils.log_checker import check_and_alert_log
from scripts.utils.logger import logger  # Ajout du logger

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1389304198658785330/j07N_glKcSM7bFqUdGC9K3vQRFkwVtw34Eyf9xSbAwgQveEcziQKE1OaVrPATzrW_kyA"

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
    df["price_date"] = pd.to_datetime(df["price_date"])
    logger.info(f"{len(df)} prix chargés pour asset_id={asset_id}.")
    return df

def compute_evolution(df, ref_date, days):
    target_date = ref_date - timedelta(days=days)
    past = df[df["price_date"] <= target_date]
    if past.empty:
        logger.warning(f"Pas de données pour le calcul d'évolution à {days} jours.")
        return np.nan
    past_price = past.iloc[-1]["price"]
    last_price = df.iloc[-1]["price"]
    return ((last_price - past_price) / past_price) * 100 if past_price != 0 else np.nan

def evolution_message(evol):
    if pd.isna(evol):
        return "Pas de données"
    elif evol > 5:
        return f"🚀 +{evol:.2f}% (forte hausse)"
    elif evol > 1:
        return f"📈 +{evol:.2f}%"
    elif evol < -5:
        return f"🔻 {evol:.2f}% (forte baisse)"
    elif evol < -1:
        return f"📉 {evol:.2f}%"
    else:
        return f"➖ {evol:.2f}% (stable)"

def send_discord_message(webhook_url, message):
    max_length = 1900
    for i in range(0, len(message), max_length):
        chunk = message[i:i+max_length]
        data = {"content": f"{chunk}"}
        response = requests.post(webhook_url, json=data)
        if response.status_code != 204:
            logger.error(f"Erreur Discord: {response.status_code} - {response.text}")

def format_table(df):
    df = df.copy()
    for col in ["1j", "5j", "1m", "3m"]:
        df[col] = df[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")
    header = "| " + " | ".join(df.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    rows = ["| " + " | ".join(map(str, row)) + " |" for row in df.values]
    table = "\n".join([header, separator] + rows)
    return f"```markdown\n{table}\n```"

if __name__ == "__main__":
    logger.info("Début du script evol_crypto_.py")
    assets = get_assets()
    results = []
    now = datetime.now()
    for _, row in assets.iterrows():
        df = get_prices(row["asset_id"])
        if len(df) < 2:
            logger.warning(f"Pas assez de données pour {row['asset_name']} (asset_id={row['asset_id']})")
            continue
        evol_1d = compute_evolution(df, df.iloc[-1]["price_date"], 1)
        evol_5d = compute_evolution(df, df.iloc[-1]["price_date"], 5)
        evol_1m = compute_evolution(df, df.iloc[-1]["price_date"], 30)
        evol_3m = compute_evolution(df, df.iloc[-1]["price_date"], 90)
        logger.info(
            f"{row['asset_name']} ({row['ticker']}): 1j={evol_1d:.2f}%, 5j={evol_5d:.2f}%, 1m={evol_1m:.2f}%, 3m={evol_3m:.2f}%"
        )
        results.append({
            "asset_name": row["asset_name"],
            "ticker": row["ticker"],
            "1j": evol_1d,
            "5j": evol_5d,
            "1m": evol_1m,
            "3m": evol_3m
        })

    df_res = pd.DataFrame(results)
    moy_1j = df_res["1j"].mean()
    moy_5j = df_res["5j"].mean()
    moy_1m = df_res["1m"].mean()
    moy_3m = df_res["3m"].mean()

    logger.info("Calcul des moyennes globales terminé.")

    # Message résumé
    resume = (
        "**Évolution moyenne de l'ensemble des cryptos suivies :**\n"
        f"- 1 jour : {evolution_message(moy_1j)}\n"
        f"- 5 jours : {evolution_message(moy_5j)}\n"
        f"- 1 mois : {evolution_message(moy_1m)}\n"
        f"- 3 mois : {evolution_message(moy_3m)}\n"
    )

    # Tableau détaillé
    table = format_table(df_res[["asset_name", "ticker", "1j", "5j", "1m", "3m"]])

    # Message par crypto (optionnel)
    details = ""
    for _, row in df_res.iterrows():
        details += (
            f"**{row['asset_name']}** ({row['ticker']})\n"
            f"  1j : {evolution_message(row['1j'])}\n"
            f"  5j : {evolution_message(row['5j'])}\n"
            f"  1m : {evolution_message(row['1m'])}\n"
            f"  3m : {evolution_message(row['3m'])}\n"
            "-----------------------------\n"
        )

    logger.info("Préparation du message Discord.")
    message = (
        f"{resume}\n"
        f"**Tableau récapitulatif :**\n"
        f"{table}\n"
        f"**Détail par crypto :**\n"
        f"{details}"
    )

    send_discord_message(DISCORD_WEBHOOK_URL, message)
    logger.info("Message Discord envoyé.")
    logger.info("Fin du script evol_crypto_.py")
    check_and_alert_log("evol_crypto_", "evol_crypto_")