from dotenv import load_dotenv
from pathlib import Path
import cx_Oracle
import os
from scripts.utils.logger import logger

def get_oracle_connection():
    # Charge les variables d'environnement depuis le .env
    load_dotenv("/home/ufcbu/market-watcher/config/.env")

    user = os.getenv("ORACLE_USER")
    pwd = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    # Vérifications
    if not user:
        logger.error("❌ Oracle User non chargé ! Vérifiez votre fichier .env.")
        return None
    if not pwd:
        logger.error("❌ Oracle Password non chargé ! Vérifiez votre fichier .env.")
        return None
    if not dsn:
        logger.error("❌ Oracle DSN non chargé ! Vérifiez votre fichier .env.")
        return None

    try:
        conn = cx_Oracle.connect(user, pwd, dsn)
        logger.info("✅ Connexion Oracle réussie !")
        return conn
    except Exception as e:
        logger.error(f"❌ Erreur de connexion Oracle : {e}")
        return None

# Test du module si exécuté directement
if __name__ == "__main__":
    conn = get_oracle_connection()
    if conn:
        print("Test OK : Connexion Oracle établie.")
        conn.close()
    else:
        print("Test KO : Connexion Oracle impossible.")