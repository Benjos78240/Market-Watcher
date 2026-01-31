from fastapi import FastAPI, Query
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd

app = FastAPI()

# Endpoint /api/system-metrics
@app.get("/api/system-metrics")
def get_system_metrics(
    start: str = Query(None, description="Start date YYYY-MM-DD HH:MM:SS"),
    end: str = Query(None, description="End date YYYY-MM-DD HH:MM:SS")
):
    try:
        conn = get_oracle_connection()
        if not conn:
            return {"error": "Connexion Oracle impossible"}
        
        cur = conn.cursor()
        query = """
            SELECT measured_at, host_id, cpu_percent, memory_percent, 
                   disk_usage, network_in, network_out, load_average
            FROM system_metrics
            WHERE 1=1
        """
        params = {}
        if start:
            query += " AND measured_at >= TO_DATE(:start_date, 'YYYY-MM-DD HH24:MI:SS')"
            params["start_date"] = start
        if end:
            query += " AND measured_at <= TO_DATE(:end_date, 'YYYY-MM-DD HH24:MI:SS')"
            params["end_date"] = end
        if not start and not end:
            query += " AND measured_at >= SYSDATE - 1/24"
        
        query += " ORDER BY measured_at DESC FETCH FIRST 100 ROWS ONLY"
        cur.execute(query, params)
        rows = cur.fetchall()
        
        metrics = []
        for row in rows:
            # Format timestamp UNIX en millisecondes (Grafana préfère ça)
            if hasattr(row[0], 'timestamp'):
                timestamp = int(row[0].timestamp() * 1000)
            else:
                # Si ce n'est pas un objet datetime
                dt = datetime.strptime(str(row[0]), "%Y-%m-%d %H:%M:%S")
                timestamp = int(dt.timestamp() * 1000)
            
            metrics.append({
                "time": timestamp, # Champ principal pour Grafana
                "measured_at_iso": row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                "host_id": str(row[1]),
                "cpu_percent": float(row[2] or 0),
                "memory_percent": float(row[3] or 0),
                "disk_usage": float(row[4] or 0),
                "network_in": float(row[5] or 0),
                "network_out": float(row[6] or 0),
                "load_average": float(row[7] or 0)
            })
        
        cur.close()
        conn.close()
        return metrics
        
    except Exception as e:
        logger.error(f"[system_metrics] Erreur: {e}")
        return {"error": str(e)}

@app.get("/api/positions")
def get_positions(
    start: str = Query(None, description="Start date YYYY-MM-DD"),
    end: str = Query(None, description="End date YYYY-MM-DD")
):
    try:
        conn = get_oracle_connection()
        if not conn:
            return {"error": "Connexion Oracle impossible"}
        cur = conn.cursor()
        max_years = 0.5  # 6 months max
        today = datetime.now().date()
        default_start = today - timedelta(days=int(365*max_years))
        start_date = start if start else default_start.strftime('%Y-%m-%d')
        end_date = end if end else today.strftime('%Y-%m-%d')
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        if (end_dt - start_dt).days > int(365*max_years):
            start_dt = end_dt - timedelta(days=int(365*max_years))
        # Requête : un point par ticker/jour, pour la crypto uniquement la dernière observation du jour
        query = '''
SELECT * FROM (
        SELECT
            a.asset_name,
            a.ticker,
            p.price_date,
            p.close_value,
            p.rsi,
            p.ma52,
            p.ma104,
            LAG(p.close_value) OVER (PARTITION BY a.ticker ORDER BY p.price_date) AS prev_close_value,
            (p.close_value - LAG(p.close_value) OVER (PARTITION BY a.ticker ORDER BY p.price_date)) / NULLIF(LAG(p.close_value) OVER (PARTITION BY a.ticker ORDER BY p.price_date), 0) AS evolution_prix,
            a.asset_type,
            ROW_NUMBER() OVER (
                PARTITION BY a.asset_type, a.asset_id, TRUNC(p.price_date)
                ORDER BY p.price_date DESC
            ) as rn
        FROM assets a
        JOIN prices p ON a.asset_id = p.asset_id
        WHERE p.price_date >= TO_DATE(:start_dt, 'YYYY-MM-DD')
            AND p.price_date <= TO_DATE(:end_dt, 'YYYY-MM-DD')
)
WHERE (asset_type = 'CRYPTO' AND rn = 1)
     OR (asset_type <> 'CRYPTO')
ORDER BY ticker, price_date
        '''
        cur.execute(query, {"start_dt": start_dt.strftime('%Y-%m-%d'), "end_dt": end_dt.strftime('%Y-%m-%d')})
        rows = cur.fetchall()
        columns = [desc[0].lower() for desc in cur.description]
        data = []
        for row in rows:
            d = dict(zip(columns, row))
            # Supprimer uniquement les champs techniques inutiles
            for k in ['rn', 'asset_type', 'prev_close_value']:
                d.pop(k, None)
            # Les champs rsi, ma52, ma104 sont conservés s'ils existent dans la requête SQL
            data.append(d)
        cur.close()
        conn.close()
        return data
    except Exception as e:
        logger.error(f"[positions] Erreur: {e}")
        return {"error": str(e)}

@app.get("/api/correlation_matrix")
def correlation_matrix(discord_user: str = Query(..., description="Utilisateur Discord")):
    """
    Retourne la matrice de corrélation des actifs détenus (qte>0) pour un utilisateur Discord.
    """
    try:
        conn = get_oracle_connection()
        if not conn:
            return {"error": "Connexion Oracle impossible"}
        # Récupérer la liste des actifs détenus (qte>0)
        query_assets = '''
            SELECT uw.asset_id, uw.asset_name, uw.asset_type, uw.ticker
            FROM user_watchlist uw
            WHERE uw.discord_user = :discord_user AND NVL(uw.qte,0) > 0
        '''
        assets = pd.read_sql(query_assets, conn, params={"discord_user": discord_user})
        assets.columns = [c.lower() for c in assets.columns]
        if assets.empty:
            return {"error": "Aucun actif trouvé pour cet utilisateur"}
        # Pour chaque actif, récupérer la série de prix (close_value ou daily max pour crypto)
        price_dfs = []
        for _, row in assets.iterrows():
            if row['asset_type'] == 'CRYPTO':
                query_price = f'''
                    SELECT TRUNC(price_date) as price_date, MAX(close_value) as close_value
                    FROM prices
                    WHERE asset_id = {row['asset_id']} AND close_value IS NOT NULL
                    GROUP BY TRUNC(price_date)
                    ORDER BY TRUNC(price_date)
                '''
            else:
                query_price = f'''
                    SELECT price_date, close_value
                    FROM prices
                    WHERE asset_id = {row['asset_id']} AND close_value IS NOT NULL
                    ORDER BY price_date
                '''
            df = pd.read_sql(query_price, conn)
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                df = df.rename(columns={"close_value": row['asset_name']})
                df['price_date'] = pd.to_datetime(df['price_date'])
                df = df.set_index('price_date')
                price_dfs.append(df[[row['asset_name']]])
        conn.close()
        if not price_dfs:
            return {"error": "Aucune donnée de prix trouvée pour les actifs de cet utilisateur"}
        # Fusionner toutes les séries sur la date
        prices = pd.concat(price_dfs, axis=1, join='outer').sort_index()
        # Calculer la matrice de corrélation (corrélation de Pearson)
        corr_matrix = prices.corr().round(3)
        # Pour Grafana, retourner sous forme de liste de dicts (source, target, value)
        result = []
        for col in corr_matrix.columns:
            for idx in corr_matrix.index:
                if col != idx:
                    result.append({"source": col, "target": idx, "value": corr_matrix.loc[idx, col]})
        return result
    except Exception as e:
        logger.error(f"[correlation_matrix] Erreur: {e}")
        return {"error": str(e)}
    
from scripts.compute_positions import compute_positions

@app.get("/api/portfolio_summary")
def portfolio_summary(discord_user: str = Query(..., description="Utilisateur Discord")):
    """Retourne un résumé du portefeuille: valeur totale, P&L journalier, positions et cashflow mensuel."""
    try:
        conn = get_oracle_connection()
        if not conn:
            return {"error": "Connexion Oracle impossible"}

        # 1) calculer positions courantes via compute_positions (utilise user_transactions)
        positions_df = compute_positions(discord_user)
        if positions_df.empty:
            return {"total_value": 0, "pnl_today": 0, "positions": [], "cashflow_month": []}

        # conserver les positions non nulles
        positions_df = positions_df[positions_df['QTE_COURANTE'] != 0]

        cur = conn.cursor()
        total_value = 0.0
        pnl_today = 0.0
        positions_out = []

        for _, r in positions_df.iterrows():
            ticker = r['TICKER']
            qte = float(r['QTE_COURANTE'])
            # retrouver asset_id et asset_name
            cur.execute("SELECT asset_id, asset_name FROM assets WHERE ticker = :t FETCH FIRST 1 ROWS ONLY", {"t": ticker})
            row = cur.fetchone()
            if not row:
                continue
            asset_id = row[0]
            asset_name = row[1]

            # récupérer les 2 dernières closes
            cur.execute("""
                SELECT close_value, price_date FROM prices
                WHERE asset_id = :aid AND close_value IS NOT NULL
                ORDER BY price_date DESC FETCH FIRST 2 ROWS ONLY
            """, {"aid": asset_id})
            price_rows = cur.fetchall()
            if not price_rows:
                last_price = None
                prev_price = None
            else:
                last_price = float(price_rows[0][0]) if price_rows[0][0] is not None else None
                prev_price = float(price_rows[1][0]) if len(price_rows) > 1 and price_rows[1][0] is not None else last_price

            value = qte * (last_price or 0)
            total_value += value
            pnl_today += qte * ((last_price or 0) - (prev_price or 0))

            positions_out.append({
                "ticker": ticker,
                "asset_id": asset_id,
                "asset_name": asset_name,
                "qte": qte,
                "last_price": last_price,
                "value": value
            })

        # cashflow mensuel (achat = négatif, vente = positif)
        cur.execute("""
            SELECT TO_CHAR(dt, 'YYYY-MM') as mon,
                   SUM(CASE WHEN UPPER(NVL(type_mvt,'')) LIKE 'V%' THEN NVL(prix,0)*NVL(qte,0) ELSE -NVL(prix,0)*NVL(qte,0) END) as cash
            FROM user_transactions
            WHERE discord_user = :u
            GROUP BY TO_CHAR(dt, 'YYYY-MM')
            ORDER BY mon DESC
        """, {"u": discord_user})
        cash_rows = cur.fetchall()
        cashflow_month = [{"month": r[0], "cashflow": float(r[1] or 0)} for r in cash_rows]

        cur.close()
        conn.close()

        return {
            "total_value": total_value,
            "pnl_today": pnl_today,
            "positions": positions_out,
            "cashflow_month": cashflow_month
        }
    except Exception as e:
        logger.error(f"[portfolio_summary] Erreur: {e}")
        return {"error": str(e)}