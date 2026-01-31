from eodhd import APIClient
import os
from dotenv import load_dotenv
from pathlib import Path

# Charger la clé API
load_dotenv(str(Path.home() / "market-watcher/config/.env"))
EODHD_API_KEY = os.getenv("EODHD_API_KEY")
api = APIClient(EODHD_API_KEY)

def print_fundamentals(ticker):
    data = api.get_fundamentals_data(ticker)
    if not data:
        print(f"Aucune donnée fondamentale pour {ticker}")
        return

    # Extraction des principaux indicateurs
    try:
        pe = data.get("General", {}).get("PE")
        pb = data.get("General", {}).get("PB")
        dividend_yield = data.get("General", {}).get("DividendYield")
        payout_ratio = data.get("General", {}).get("DividendPayoutRatio")
        debt_ebitda = data.get("Highlights", {}).get("DebtToEBITDA")
        roe = data.get("Highlights", {}).get("ReturnOnEquity")

        print(f"Ticker: {ticker}")
        print(f"P/E: {pe}")
        print(f"P/B: {pb}")
        print(f"Dividend Yield: {dividend_yield}")
        print(f"Payout Ratio: {payout_ratio}")
        print(f"Dette/EBITDA: {debt_ebitda}")
        print(f"ROE: {roe}")
    except Exception as e:
        print(f"Erreur lors de l'extraction des fondamentaux: {e}")

if __name__ == "__main__":
    # Exemple avec un ticker CAC40
    print_fundamentals("AI.PA")