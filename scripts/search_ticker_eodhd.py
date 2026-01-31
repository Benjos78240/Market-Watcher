from eodhd import APIClient
import os
from dotenv import load_dotenv
from pathlib import Path
from scripts.utils.logger import logger

load_dotenv(str(Path.home() / "market-watcher/config/.env"))
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
api = APIClient(EODHD_API_KEY)

def is_valid_eodhd_ticker(ticker):
    """
    Vérifie si le ticker est valide sur EODHD en tentant d'extraire la dernière donnée daily.
    Retourne True si au moins une donnée est trouvée, False sinon.
    """
    try:
        data = api.get_eod_historical_stock_market_data(
            symbol=ticker,
            period='d',
            order='d'
        )
        if data and isinstance(data, list) and len(data) > 0:
            logger.info(f"✅ Ticker {ticker} est valide sur EODHD ({data[0]['date']})")
            return True
        else:
            logger.warning(f"❌ Ticker {ticker} est invalide ou absent sur EODHD")
            return False
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du ticker EODHD : {e}")
        return False

# Exemple d'utilisation
#if __name__ == "__main__":
 #   ticker = "PINR.PA"
  #  is_valid = is_valid_eodhd_ticker(ticker)
   # print(f"Ticker {ticker} valide sur EODHD ? {is_valid}")
