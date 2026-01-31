from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.search_ticker_eodhd import is_valid_eodhd_ticker
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(str(Path.home() / "market-watcher/config/.env"))

cac40_tickers = [
    "AC.PA", "AIR.PA", "AI.PA", "MT.AS", "CS.PA", "BNP.PA", "EN.PA", "CAP.PA", "CA.PA", "ACA.PA",
    "BN.PA", "DSY.PA", "EDEN.PA", "ENGI.PA", "EL.PA", "ERF.PA", "RMS.PA", "KER.PA", "LR.PA", "OR.PA",
    "MC.PA", "ML.PA", "ORA.PA", "RI.PA", "PUB.PA", "RNO.PA", "SAF.PA", "SGO.PA", "SAN.PA", "SU.PA",
    "GLE.PA", "STLAP.PA", "STM.PA", "TEP.PA", "HO.PA", "TTE.PA", "URW.PA", "VIE.PA", "DG.PA"
]

# Optionnel : dictionnaire ticker -> nom entreprise
cac40_names = {
    "AC.PA": "Accor", "AIR.PA": "Airbus", "AI.PA": "Air Liquide", "MT.AS": "ArcelorMittal", "CS.PA": "AXA",
    "BNP.PA": "BNP Paribas", "EN.PA": "Bouygues", "CAP.PA": "Capgemini", "CA.PA": "Carrefour", "ACA.PA": "Crédit Agricole",
    "BN.PA": "Danone", "DSY.PA": "Dassault Systèmes", "EDEN.PA": "Edenred", "ENGI.PA": "Engie", "EL.PA": "EssilorLuxottica",
    "ERF.PA": "Eurofins Scientific", "RMS.PA": "Hermès", "KER.PA": "Kering", "LR.PA": "Legrand", "OR.PA": "L'Oréal",
    "MC.PA": "LVMH", "ML.PA": "Michelin", "ORA.PA": "Orange", "RI.PA": "Pernod Ricard", "PUB.PA": "Publicis Groupe",
    "RNO.PA": "Renault", "SAF.PA": "Safran", "SGO.PA": "Saint-Gobain", "SAN.PA": "Sanofi", "SU.PA": "Schneider Electric",
    "GLE.PA": "Société Générale", "STLAP.PA": "Stellantis", "STM.PA": "STMicroelectronics", "TEP.PA": "Teleperformance",
    "HO.PA": "Thales", "TTE.PA": "TotalEnergies", "URW.PA": "Unibail-Rodamco-Westfield", "VIE.PA": "Veolia Environnement",
    "DG.PA": "Vinci"
}

conn = get_oracle_connection()
cur = conn.cursor()
cur.execute("SELECT ticker FROM assets")
db_tickers = set(row[0] for row in cur.fetchall())

missing = [t for t in cac40_tickers if t not in db_tickers]

if missing:
    print("Tickers CAC40 manquants dans assets :")
    for t in missing:
        print("-", t)
        # Vérifie sur EODHD
        if is_valid_eodhd_ticker(t):
            print(f"  ✅ Ticker {t} existe sur EODHD, insertion dans assets...")
            asset_name = cac40_names.get(t)
            cur.execute(
                "INSERT INTO assets (ticker, asset_name, asset_type) VALUES (:ticker, :name, :type)",
                {"ticker": t, "name": asset_name, "type": "STOCK"}
            )
            conn.commit()
        else:
            print(f"  ❌ Ticker {t} n'existe pas sur EODHD, non inséré.")
else:
    print("Tous les tickers CAC40 sont présents dans assets.")

cur.close()
conn.close()