import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger

def compute_rsi(series, window=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def update_indicators_for_asset(asset_id, asset_type, min_obs_rsi=14, min_obs_ma52=52, min_obs_ma104=104):
    conn = get_oracle_connection()
    query = """
        SELECT price_id, price_date, close_value
        FROM prices
        WHERE asset_id = :asset_id AND close_value IS NOT NULL
        ORDER BY price_date
    """
    df = pd.read_sql(query, conn, params={"asset_id": asset_id})
    df.columns = [c.lower() for c in df.columns]
    if df.empty:
        logger.info(f"Aucune donnée pour asset_id={asset_id}")
        conn.close()
        return 0
    df = df.sort_values('price_date').reset_index(drop=True)
    # Pour les cryptos, regrouper par jour et prendre le dernier close
    if asset_type == 'CRYPTO':
        df['date'] = pd.to_datetime(df['price_date']).dt.date
        df = df.groupby('date', as_index=False).last()
        price_col = 'close_value'
    else:
        price_col = 'close_value'
    # Fenêtres standards
    ma52 = 52
    ma104 = 104
    rsi_window = 14
    min_periods_ma = max(1, int(ma52 * 0.3))
    # Calcul indicateurs si assez d'observations
    if len(df) >= rsi_window:
        df['rsi'] = compute_rsi(df[price_col], window=rsi_window)
    if len(df) >= ma52:
        df['ma52'] = df[price_col].rolling(window=ma52, min_periods=min_periods_ma).mean()
    if len(df) >= ma104:
        df['ma104'] = df[price_col].rolling(window=ma104, min_periods=52).mean()
    # Mise à jour en base
    updated = 0
    for _, row in df.iterrows():
        cur = conn.cursor()
        cur.execute("""
            UPDATE prices SET rsi = :rsi, ma52 = :ma52, ma104 = :ma104 WHERE price_id = :price_id
        """, {
            "rsi": float(row['rsi']) if 'rsi' in row and pd.notnull(row['rsi']) else None,
            "ma52": float(row['ma52']) if 'ma52' in row and pd.notnull(row['ma52']) else None,
            "ma104": float(row['ma104']) if 'ma104' in row and pd.notnull(row['ma104']) else None,
            "price_id": int(row['price_id'])
        })
        updated += 1
        cur.close()
    conn.commit()
    conn.close()
    logger.info(f"{updated} lignes mises à jour pour asset_id={asset_id}")
    return updated

def update_all_assets(asset_type_filter=None):
    conn = get_oracle_connection()
    query = "SELECT asset_id, asset_type FROM assets"
    if asset_type_filter:
        query += " WHERE asset_type = :atype"
        assets = pd.read_sql(query, conn, params={"atype": asset_type_filter})
    else:
        assets = pd.read_sql(query, conn)
    assets.columns = [c.lower() for c in assets.columns]
    conn.close()
    total = 0
    for _, row in assets.iterrows():
        total += update_indicators_for_asset(row['asset_id'], row['asset_type'])
    logger.info(f"Total lignes mises à jour : {total}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        update_all_assets(sys.argv[1])
    else:
        update_all_assets()
