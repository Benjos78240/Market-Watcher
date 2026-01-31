import time
import pandas as pd
from pathlib import Path
import requests
import os
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from scripts.search_ticker_eodhd import is_valid_eodhd_ticker
from scripts.utils.search_ticker_coinbase import search_pair_coinbase
from scripts.extraction_eodhd_hist import fetch_daily_data_eodhd, insert_prices, update_asset_dates
from scripts.extraction_coinbase_histo_ import fetch_coinbase_ohlc_1h, insert_price, update_asset_dates_and_status


DATA_DIR = Path.home() / "market-watcher/data"

def insert_user_watchlist(csv_path, discord_user):
    try:
        df = pd.read_csv(str(csv_path), sep=";", encoding="latin1")
    except Exception:
        df = pd.read_csv(str(csv_path), sep=",", encoding="utf-8")
    if df.empty or "ticker" not in df.columns:
        logger.warning(f"⚠️ Aucun ticker chargé ou colonne 'ticker' manquante dans {csv_path.name} !")
        return []
    df = df.drop_duplicates(subset=["ticker"])
    conn = get_oracle_connection()
    logger.info(f"🔗 Connexion Oracle pour mise à jour user_watchlist ({discord_user})")
    cur = conn.cursor()
    tickers_to_validate = {}
    corrections = []
    for idx, row in df.iterrows():
        ticker = str(row["ticker"]).strip()
        asset_type = row["type"] if "type" in df.columns else row.get("asset_type", None)
        asset_name = row["nom"] if "nom" in df.columns else None
        # Gestion de la quantité
        qte = row["QTE"] if "QTE" in df.columns else 0
        try:
            qte = float(str(qte).replace(",", ".")) if str(qte).strip() != '' else None
        except Exception:
            qte = None
        # Vérifie si déjà présent
        cur.execute("SELECT QTE FROM user_watchlist WHERE discord_user = :discord_user AND ticker = :ticker", {"discord_user": discord_user, "ticker": ticker})
        existing = cur.fetchone()
        # Correction/validation pour CRYPTO
        if asset_type and asset_type.upper() == "CRYPTO":
            symbol, official_name = search_pair_coinbase(ticker)
            if symbol is None:
                logger.warning(f"⚠️ Paire crypto '{ticker}' non trouvée sur Coinbase, ignorée.")
                continue
            if symbol != ticker or (official_name and official_name != asset_name):
                corrections.append({"old_ticker": ticker, "new_ticker": symbol, "new_name": official_name})
            ticker = symbol
            asset_name = official_name
        else:
            tickers_to_validate[ticker] = {"asset_name": asset_name, "asset_type": asset_type}

        try:
            asset_id = get_or_create_asset_id(ticker, asset_name, asset_type)
            if existing:
                # Si la quantité a changé, on met à jour
                old_qte = existing[0] if existing[0] is not None else 0
                if float(old_qte) != float(qte):
                    cur.execute(
                        "UPDATE user_watchlist SET QTE = :qte WHERE discord_user = :discord_user AND ticker = :ticker",
                        {"qte": qte, "discord_user": discord_user, "ticker": ticker}
                    )
                    logger.info(f"🔄 Quantité mise à jour pour {ticker} ({asset_type}) : {old_qte} → {qte} pour {discord_user}")
                continue
            cur.execute(
                "INSERT INTO user_watchlist (discord_user, ticker, asset_name, asset_type, asset_id, QTE) VALUES (:discord_user, :ticker, :name, :type, :asset_id, :qte)",
                {"discord_user": discord_user, "ticker": ticker, "name": asset_name, "type": asset_type, "asset_id": asset_id, "qte": qte}
            )
            logger.info(f"✅ Ajouté à user_watchlist : {ticker} ({asset_type}) pour {discord_user} (QTE={qte})")
        except Exception as e:
            logger.error(f"❌ [{discord_user}] Erreur ajout {ticker} : {e}")
    conn.commit()
    cur.close()
    conn.close()
    return tickers_to_validate, corrections

def remove_missing_from_watchlist(csv_path, discord_user):
    logger.info(f"Début suppression des ticker non-suivis {discord_user}...")
    try:
        df = pd.read_csv(str(csv_path), sep=";", encoding="latin1")
    except Exception:
        df = pd.read_csv(str(csv_path), sep=",", encoding="utf-8")
    if df.empty or "ticker" not in df.columns:
        logger.warning(f"⚠️ Aucun ticker chargé ou colonne 'ticker' manquante dans {csv_path.name} !")
        return

    tickers_csv = set(df["ticker"].astype(str).str.strip())
    conn = get_oracle_connection()
    logger.info(f"🔗 Connexion Oracle pour vérification des tickers à supprimer dans user_watchlist ({discord_user})")
    cur = conn.cursor()
    cur.execute("SELECT ticker FROM user_watchlist WHERE discord_user = :discord_user", {"discord_user": discord_user})
    tickers_db = set(row[0] for row in cur.fetchall())
    to_remove = tickers_db - tickers_csv

    if not to_remove:
        logger.info(f"✅ Aucun ticker à supprimer pour {discord_user}.")
        cur.close()
        conn.close()
        return

    for ticker in to_remove:
        cur.execute(
            "DELETE FROM user_watchlist WHERE discord_user = :discord_user AND ticker = :ticker",
            {"discord_user": discord_user, "ticker": ticker}
        )
        logger.info(f"🗑️ [{discord_user}] Supprimé de la base : {ticker}")

    conn.commit()
    cur.close()
    conn.close()

def validate_and_update_tickers(tickers_to_validate):
    # Actions/ETF
    conn = get_oracle_connection()
    logger.info("🔗 Connexion Oracle pour validation des actifs type : actions(Stock) & ETF) dans la table Assets")
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ticker FROM user_watchlist WHERE asset_type IS NULL OR asset_type <> 'CRYPTO'")
    all_tickers = set(row[0] for row in cur.fetchall())
    cur.execute("SELECT ticker FROM assets")
    existing_assets = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()
    tickers_to_insert = {t: v for t, v in tickers_to_validate.items() if t not in existing_assets}

    corrections = []

    for ticker, info in tickers_to_insert.items():
        asset_name = info.get("asset_name")
        asset_type = info.get("asset_type")
        if is_valid_eodhd_ticker(ticker):
            conn = get_oracle_connection()
            logger.info("🔗 Connexion Oracle pour insertion des actifs type : actions(Stock & ETF) dans la table Assets")
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO assets (ticker, asset_name, asset_type) VALUES (:ticker, :name, :type) RETURNING asset_id INTO :asset_id",
                {"ticker": ticker, "name": asset_name, "type": asset_type, "asset_id": cur.var(int)}
            )
            asset_id = cur.getvalue("asset_id")
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Ticker '{ticker}' ajouté à assets (validé EODHD)")
        else:
            logger.warning(f"⚠️ Ticker '{ticker}' non valide sur EODHD, ignoré.")

    # Crypto
    conn = get_oracle_connection()
    logger.info("🔗 Connexion Oracle pour pour validation des actifs type : Crypto dans la table Assets")
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ticker, asset_name FROM user_watchlist WHERE asset_type = 'CRYPTO'")
    crypto_tickers = set((row[0], row[1]) for row in cur.fetchall())
    cur.execute("SELECT ticker FROM assets WHERE asset_type = 'CRYPTO'")
    existing_crypto_assets = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()
    tickers_to_validate = [t for t, _ in crypto_tickers if t not in existing_crypto_assets]

    for ticker in tickers_to_validate:
        symbol, official_name = search_pair_coinbase(ticker)
        if symbol is None:
            logger.warning(f"⚠️ Paire crypto '{ticker}' non trouvée sur Coinbase.")
            continue
        conn = get_oracle_connection()
        logger.info("🔗 Connexion Oracle pour insértion des actifs type : Crypto dans la table Assets")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO assets (ticker, asset_name, asset_type) VALUES (:ticker, :name, :type)",
            {"ticker": symbol, "name": official_name, "type": "CRYPTO"}
        )
        conn.commit()
        cur.close()
        conn.close()
        if symbol != ticker:
            logger.info(f"Correction de la paire '{ticker}' → '{symbol}' ({official_name})")
            corrections.append({"old_ticker": ticker, "new_ticker": symbol, "new_name": official_name})

    # Applique les corrections dans la base
    if corrections:
        conn = get_oracle_connection()
        logger.info("🔗 Connexion Oracle pour corrections user_watchlist/assets")
        cur = conn.cursor()
        cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        for corr in corrections:
            cur.execute(
                "UPDATE user_watchlist SET ticker = :new_ticker, asset_name = :new_name WHERE ticker = :old_ticker",
                {"new_ticker": corr["new_ticker"], "new_name": corr["new_name"], "old_ticker": corr["old_ticker"]}
            )
            cur.execute(
                "UPDATE assets SET ticker = :new_ticker, asset_name = :new_name WHERE ticker = :old_ticker",
                {
                    "new_ticker": corr["new_ticker"],
                    "new_name": corr["new_name"],
                    "old_ticker": corr["old_ticker"]
                }
            )
        conn.commit()
        cur.close()
        conn.close()

    # Applique les corrections dans tous les CSV
    for csv_file in DATA_DIR.glob("watch_pf_*.csv"):
        try:
            df = pd.read_csv(str(csv_file), sep=";", encoding="latin1")
        except Exception:
            df = pd.read_csv(str(csv_file), sep=",", encoding="utf-8")
        for corr in corrections:
            df.loc[df["ticker"] == corr["old_ticker"], "ticker"] = corr["new_ticker"]
            if "nom" in df.columns:
                df.loc[df["ticker"] == corr["new_ticker"], "nom"] = corr["new_name"]
            else:
                df["nom"] = df.apply(
                    lambda row: corr["new_name"] if row["ticker"] == corr["new_ticker"] else row.get("nom", ""), axis=1
                )
        df.to_csv(csv_file, sep=";", index=False, encoding="utf-8")
        logger.info(f"💾 CSV {csv_file.name} mis à jour avec les tickers validés.")

def get_or_create_asset_id(ticker, asset_name=None, asset_type=None):
    conn = get_oracle_connection()
    cur = conn.cursor()
    cur.execute("SELECT asset_id FROM assets WHERE ticker = :ticker", {"ticker": ticker})
    row = cur.fetchone()
    if row:
        asset_id = row[0]
    else:
        # Prépare la variable de sortie
        asset_id_var = cur.var(int)
        cur.execute(
            "INSERT INTO assets (ticker, asset_name, asset_type) VALUES (:ticker, :name, :type) RETURNING asset_id INTO :asset_id",
            {"ticker": ticker, "name": asset_name, "type": asset_type, "asset_id": asset_id_var}
        )
        asset_id = asset_id_var.getvalue()
        if isinstance(asset_id, list):
            asset_id = asset_id[0]
        conn.commit()
        # Extraction historique pour les nouveaux actifs
        if asset_type and asset_type.upper() in ("STOCK", "ETF"):
            try:
                prices = fetch_daily_data_eodhd(ticker, years=10)
                if prices:
                    insert_prices(asset_id, ticker, prices)
                    update_asset_dates(asset_id)
                    logger.info(f"✅ Extraction historique auto pour {ticker} (asset_id={asset_id}) terminée.")
                else:
                    logger.warning(f"⚠️ Extraction historique auto pour {ticker} (asset_id={asset_id}) : aucune donnée récupérée.")
            except Exception as e:
                logger.error(f"❌ Erreur extraction historique auto pour {ticker} (asset_id={asset_id}): {e}")
        elif asset_type and asset_type.upper() == "CRYPTO":
            try:
                ohlc_list = fetch_coinbase_ohlc_1h(ticker)
                status = "OK" if ohlc_list else "ERROR"
                if ohlc_list:
                    for ohlc in ohlc_list:
                        insert_price(asset_id, ticker, ohlc)
                    update_asset_dates_and_status(asset_id, ohlc_list, status)
                    logger.info(f"✅ Extraction historique auto CRYPTO pour {ticker} (asset_id={asset_id}) terminée.")
                else:
                    update_asset_dates_and_status(asset_id, [], status)
                    logger.warning(f"⚠️ Extraction historique auto CRYPTO pour {ticker} (asset_id={asset_id}) : aucune donnée récupérée.")
            except Exception as e:
                logger.error(f"❌ Erreur extraction historique auto CRYPTO pour {ticker} (asset_id={asset_id}): {e}")
    cur.close()
    conn.close()
    return asset_id

if __name__ == "__main__":
    all_to_validate = {}
    all_corrections = []
    for csv_file in DATA_DIR.glob("watch_pf_*.csv"):
        discord_user = csv_file.stem.replace("watch_pf_", "")
        tickers, corrections = insert_user_watchlist(csv_file, discord_user)
        remove_missing_from_watchlist(csv_file, discord_user)
        all_to_validate.update(tickers)
        all_corrections.extend(corrections)
    validate_and_update_tickers(all_to_validate)