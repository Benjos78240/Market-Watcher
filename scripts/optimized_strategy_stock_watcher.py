import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from skopt import gp_minimize
from skopt.space import Integer, Real
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.log_checker import check_and_alert_log
from scripts.utils.logger import logger
from scripts.utils.report_generator import generate_report
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv(os.path.expanduser('~/market-watcher/config/.env'))

# --- 1. Récupération des données ---
def get_cac40_assets():
    """Récupère les tickers CAC40 depuis la base Oracle"""
    conn = get_oracle_connection()
    query = """
        SELECT a.asset_id, a.ticker, a.asset_name 
        FROM assets a
        JOIN indices i ON a.asset_id = i.asset_id
        WHERE i.indice = 'CAC40'
    """
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    conn.close()
    return df

def get_historical_data(asset_id, start_date, end_date):
    """Récupère les données historiques pour un asset"""
    conn = get_oracle_connection()
    query = f"""
        SELECT price_date, close_value as price, volume
        FROM prices
        WHERE asset_id = {asset_id}
        AND price_date BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD') 
                          AND TO_DATE('{end_date}', 'YYYY-MM-DD')
        ORDER BY price_date
    """
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    conn.close()
    return df

# --- 2. Calcul des indicateurs ---
def compute_technical_indicators(df, ma_window):
    """Calcule les indicateurs techniques avec une fenêtre dynamique"""
    df[f'ma{ma_window}'] = df['price'].rolling(ma_window).mean()
    
    # RSI
    delta = df['price'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # Volume
    df['vol_ma20'] = df['volume'].rolling(20).mean()
    
    return df.dropna()

# --- 3. Optimisation des paramètres ---
def optimize_strategy(asset_id):
    """Trouve les meilleurs paramètres pour un asset"""
    # Espace de recherche
    space = [
        Integer(20, 40, name='rsi_buy'),
        Integer(60, 80, name='rsi_sell'),
        Integer(50, 200, name='ma_window'),
        Real(1.0, 3.0, name='vol_mult')
    ]
    
    # Fonction à optimiser
    def evaluate(params):
        rsi_buy, rsi_sell, ma_window, vol_mult = params
        
        # Données d'entraînement (70% de la période)
        train_start = (datetime.now() - timedelta(days=1825)).strftime('%Y-%m-%d')
        train_end = (datetime.now() - timedelta(days=548)).strftime('%Y-%m-%d')
        df = get_historical_data(asset_id, train_start, train_end)
        df = compute_technical_indicators(df, ma_window)
        
        # Simulation
        df['signal'] = 0
        buy_cond = (
            (df['rsi'] < rsi_buy) & 
            (df['price'] > df[f'ma{ma_window}']) & 
            (df['volume'] > vol_mult * df['vol_ma20'])
        )
        sell_cond = (df['rsi'] > rsi_sell)
        
        df.loc[buy_cond, 'signal'] = 1
        df.loc[sell_cond, 'signal'] = -1
        
        # Rendement annualisé
        returns = df['price'].pct_change()[df['signal'] == 1].sum()
        return -returns  # Minimiser l'opposé du rendement
    
    # Optimisation
    result = gp_minimize(
        evaluate,
        space,
        n_calls=30,
        random_state=42,
        verbose=True
    )
    
    return result.x  # Meilleurs paramètres

# --- 4. Backtest avec paramètres optimisés ---
def backtest_optimized(asset_id, params, start_date, end_date):
    """Backtest avec les paramètres optimisés"""
    rsi_buy, rsi_sell, ma_window, vol_mult = params
    df = get_historical_data(asset_id, start_date, end_date)
    df = compute_technical_indicators(df, ma_window)
    
    # Génération des signaux
    df['signal'] = 0
    buy_cond = (
        (df['rsi'] < rsi_buy) & 
        (df['price'] > df[f'ma{ma_window}']) & 
        (df['volume'] > vol_mult * df['vol_ma20'])
    )
    sell_cond = (df['rsi'] > rsi_sell)
    
    df.loc[buy_cond, 'signal'] = 1
    df.loc[sell_cond, 'signal'] = -1
    
    # Simulation des trades
    trades = []
    position = 0
    entry_price = 0
    
    for _, row in df.iterrows():
        if row['signal'] == 1 and position == 0:
            position = 1
            entry_price = row['price']
        elif row['signal'] == -1 and position == 1:
            trades.append({
                'buy_date': row['price_date'],
                'sell_date': row['price_date'],
                'return': (row['price'] - entry_price) / entry_price
            })
            position = 0
    
    return trades

# --- 5. Workflow Principal ---
if __name__ == "__main__":
    logger.info("Début de l'optimisation")
    
    # Paramètres
    assets = get_cac40_assets()
    validation_start = (datetime.now() - timedelta(days=548)).strftime('%Y-%m-%d')  # Derniers 18 mois
    validation_end = datetime.now().strftime('%Y-%m-%d')
    
    all_results = []
    
    for _, asset in assets.iterrows():
        try:
            # Optimisation
            best_params = optimize_strategy(asset['asset_id'])
            logger.info(f"Meilleurs paramètres pour {asset['ticker']}: RSI={best_params[0]}/{best_params[1]}, MM={best_params[2]}j, Vol x{best_params[3]:.1f}")
            
            # Validation
            trades = backtest_optimized(asset['asset_id'], best_params, validation_start, validation_end)
            if trades:
                avg_return = np.mean([t['return'] for t in trades])
                all_results.append({
                    'ticker': asset['ticker'],
                    'name': asset['asset_name'],
                    'params': best_params,
                    'avg_return': avg_return,
                    'n_trades': len(trades)
                })
                
        except Exception as e:
            logger.error(f"Erreur sur {asset['ticker']} : {str(e)}")
    
    # Génération du rapport
    if all_results:
        df_results = pd.DataFrame(all_results)
        os.makedirs('data/outputs/backtest_results', exist_ok=True)
        df_results.to_csv('data/outputs/backtest_results/optimized_results.csv', index=False)
        generate_report(df_results)  # Génère un PDF avec matplotlib
        logger.info("Rapport généré dans /data/reports/")
check_and_alert_log("optimized_strategy_stock_watcher", "optimized_strategy_stock_watcher")