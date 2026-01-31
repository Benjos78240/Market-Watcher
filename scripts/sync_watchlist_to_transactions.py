import datetime
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
import time
import pandas as pd
from pathlib import Path
import requests
import os

def sync_watchlist_to_transactions(date_exec=None):
    conn = get_oracle_connection()
    cur = conn.cursor()
    logger.info("🔗 Connexion Oracle pour synchronisation user_watchlist → user_transactions")
    
    # Récupère tous les actifs suivis
    cur.execute("SELECT discord_user, ticker, NVL(QTE, 0), asset_id FROM user_watchlist")
    watchlist = cur.fetchall()
    now = date_exec if date_exec else datetime.datetime.now()
    for discord_user, ticker, qte, asset_id in watchlist:
        # Cherche la dernière quantité connue dans user_transactions
        cur.execute("""
            SELECT qte, prix, dt FROM user_transactions
            WHERE discord_user = :u AND ticker = :t
            ORDER BY dt DESC FETCH FIRST 1 ROWS ONLY
        """, {"u": discord_user, "t": ticker})
        last = cur.fetchone()
        last_qte = last[0] if last else None
        last_prix = last[1] if last and len(last) > 1 else None
        # Récupère le dernier prix de l'actif
        cur.execute("""
            SELECT close_value FROM prices
            WHERE asset_id = :aid AND close_value IS NOT NULL
            ORDER BY price_date DESC FETCH FIRST 1 ROWS ONLY
        """, {"aid": asset_id})
        last_price = cur.fetchone()
        prix = last_price[0] if last_price else None
        # Calcul de l'évolution (variation de quantité, de prix et performance relative)
        if last_qte is None or float(last_qte) != float(qte):
            # Cas spécial : passage de >0 à 0 (vente totale ou retrait)
            if float(qte) == 0 and (last_qte is not None and float(last_qte) == 0):
                # Déjà à 0, on ne réinsère pas
                continue
            type_mvt = "ACHAT" if float(qte) > 0 else "VENTE"
            qte_diff = float(qte) if last_qte is None else float(qte) - float(last_qte)
            evolution_prix = 0
            evolution_prix_relative = 0
            if last_prix is not None and prix is not None and float(last_prix) != 0:
                evolution_prix = (float(prix) - float(last_prix)) * float(qte)
                evolution_prix_relative = (float(prix) - float(last_prix)) / float(last_prix)
            cur.execute(
                "INSERT INTO user_transactions (discord_user, ticker, asset_id, dt, qte, type_mvt, prix, comm, evolution, evolution_prix, evolution_prix_relative) VALUES (:u, :t, :aid, :d, :q, :tm, :p, :c, :evo, :evop, :evopr)",
                {"u": discord_user, "t": ticker, "aid": asset_id, "d": now, "q": qte_diff, "tm": type_mvt, "p": prix, "c": "Sync auto", "evo": qte_diff, "evop": evolution_prix, "evopr": evolution_prix_relative}
            )
            logger.info(f"📝 Mouvement inséré pour {discord_user} {ticker} : ΔQTE={qte_diff} (nouvelle QTE={qte}) | Prix={prix} | ΔPrix={evolution_prix} | Perf={evolution_prix_relative:.2%}")
        else:
            # Si la quantité ne change pas, duplique la ligne avec la nouvelle date
            # Sauf si QTE=0 (on ne duplique pas les lignes à 0)
            if float(qte) == 0:
                continue
            evolution_prix = 0
            evolution_prix_relative = 0
            if last_prix is not None and prix is not None and float(last_prix) != 0:
                evolution_prix = (float(prix) - float(last_prix)) * float(qte)
                evolution_prix_relative = (float(prix) - float(last_prix)) / float(last_prix)
            cur.execute(
                "INSERT INTO user_transactions (discord_user, ticker, asset_id, dt, qte, type_mvt, prix, comm, evolution, evolution_prix, evolution_prix_relative) VALUES (:u, :t, :aid, :d, :q, :tm, :p, :c, :evo, :evop, :evopr)",
                {"u": discord_user, "t": ticker, "aid": asset_id, "d": now, "q": qte, "tm": "SUIVI", "p": prix, "c": "Sync auto (pas de changement)", "evo": 0, "evop": evolution_prix, "evopr": evolution_prix_relative}
            )
            logger.info(f"🔁 Mouvement dupliqué pour {discord_user} {ticker} : QTE={qte} | Prix={prix} | ΔPrix={evolution_prix} | Perf={evolution_prix_relative:.2%}")
    conn.commit()
    # Suppression des doublons sur le jour (on garde la plus récente)
    logger.info("Nettoyage des doublons sur la date (jour) dans user_transactions...")
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM user_transactions ut
        WHERE ut.ROWID NOT IN (
            SELECT rid FROM (
                SELECT ROWID as rid,
                       ROW_NUMBER() OVER (PARTITION BY discord_user, ticker, TRUNC(dt) ORDER BY dt DESC) as rn
                FROM user_transactions
            ) WHERE rn = 1
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ Synchronisation terminée et doublons supprimés (on garde la ligne la plus récente par user/ticker/jour)")

if __name__ == "__main__":
    #yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    #sync_watchlist_to_transactions(date_exec=yesterday)

    today = datetime.datetime.now()
    sync_watchlist_to_transactions(date_exec=today)
