import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger

def compute_positions(discord_user=None):
    conn = get_oracle_connection()
    cur = conn.cursor()
    logger.info("🔗 Connexion Oracle pour calcul des positions courantes")
    # Récupère toutes les transactions
    if discord_user:
        cur.execute("""
            SELECT DISCORD_USER, TICKER, QTE, TYPE_MVT, DT
            FROM user_transactions
            WHERE DISCORD_USER = :discord_user
        """, {"discord_user": discord_user})
    else:
        cur.execute("""
            SELECT DISCORD_USER, TICKER, QTE, TYPE_MVT, DT
            FROM user_transactions
        """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        logger.warning("Aucune transaction trouvée.")
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["DISCORD_USER", "TICKER", "QTE", "TYPE_MVT", "DT"])
    # Calcule la position courante : somme des QTE (ACHAT = +, VENTE = -)
    def signed_qte(row):
        if row["TYPE_MVT"] and str(row["TYPE_MVT"]).upper().startswith("V"):  # VENTE
            return -abs(row["QTE"] or 0)
        return row["QTE"] or 0
    df["QTE_SIGNED"] = df.apply(signed_qte, axis=1)
    positions = df.groupby(["DISCORD_USER", "TICKER"]).agg({"QTE_SIGNED": "sum"}).reset_index()
    positions = positions.rename(columns={"QTE_SIGNED": "QTE_COURANTE"})
    logger.info(f"Positions courantes calculées :\n{positions}")
    return positions

if __name__ == "__main__":
    positions = compute_positions()
    print(positions)
