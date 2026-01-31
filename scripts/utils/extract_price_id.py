import pandas as pd
from pathlib import Path
from scripts.utils.bd_oracle_connection import get_oracle_connection
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt

DATA_DIR = Path.home() / "market-watcher/data"
OUTPUT_FILE = DATA_DIR / "prices_asset_305.csv"

# Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.expanduser('~/market-watcher/config/.env'))


def extract_prices_asset():
    conn = get_oracle_connection()
    query = """
        SELECT * FROM prices WHERE asset_id = 305
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Export terminé : {OUTPUT_FILE}")

def plot_prices_asset():
    # Chemin vers ton fichier exporté
    csv_path = Path.home() / "market-watcher/data/prices_asset_305.csv"

    # Lecture du CSV
    df = pd.read_csv(csv_path)

    # Conversion de la colonne date
    df['PRICE_DATE'] = pd.to_datetime(df['PRICE_DATE'])

    # Tri par date (optionnel mais conseillé)
    df = df.sort_values('PRICE_DATE')

    # Tracé du graphique
    plt.figure(figsize=(12, 6))
    plt.plot(df['PRICE_DATE'], df['CLOSE_VALUE'], label='Cours')
    plt.xlabel('Date')
    plt.ylabel('Prix')
    plt.title('Evolution du prix (asset_id=305)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(DATA_DIR / "outputs/graphique/prices_asset_305.png")
    print("✅ Graphique sauvegardé dans outputs/graphique/prices_asset_305.png")
    plt.show()

if __name__ == "__main__":
    extract_prices_asset()
    plot_prices_asset()