import time
import pandas as pd
from pathlib import Path
import requests
import os
from dotenv import load_dotenv
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger

load_dotenv(str(Path.home() / "market-watcher/config/.env"))
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def search_ticker_alpha_vantage(keyword):
    """
    Recherche un ticker via Alpha Vantage à partir d'un mot-clé.
    Retourne le symbole exact et le nom si trouvé, sinon (None, None).
    """
    ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
    if not ALPHA_VANTAGE_KEY:
        logger.error("Clé API Alpha Vantage manquante dans les variables d'environnement.")
        return None, None
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=SYMBOL_SEARCH"
        f"&keywords={keyword}"
        f"&apikey={ALPHA_VANTAGE_KEY}"
    )
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
    except Exception as e:
        logger.error(f"Erreur lors de la requête Alpha Vantage : {e}")
        return None, None

    if "Note" in data:
        logger.warning(f"⏳ Limite Alpha Vantage atteinte : {data['Note']}")
        return None, None

    matches = data.get("bestMatches", [])
    if not matches:
        logger.warning(f"Réponse brute Alpha Vantage pour '{keyword}': {data}")
        return None, None

    symbol = matches[0].get("1. symbol")
    name = matches[0].get("2. name")
    return symbol, name
