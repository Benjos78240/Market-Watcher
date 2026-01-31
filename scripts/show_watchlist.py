from scripts.utils.bd_oracle_connection import get_oracle_connection

def show_watchlist():
    conn = get_oracle_connection()
    if not conn:
        print("Connexion Oracle impossible.")
        return
    cur = conn.cursor()
    cur.execute("SELECT discord_user, asset_id, asset_name, asset_type FROM user_watchlist ORDER BY discord_user, asset_id")
    for row in cur.fetchall():
        print(row)
    cur.close()
    conn.close()

if __name__ == "__main__":
    show_watchlist()