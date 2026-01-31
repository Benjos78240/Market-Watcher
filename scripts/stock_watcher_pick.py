import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.logger import logger
from datetime import datetime, timedelta
from dotenv import load_dotenv
import numpy as np
import os

# Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.expanduser('~/market-watcher/config/.env'))

def get_cac40_assets():
    """Récupère les tickers CAC40 depuis la base"""
    conn = get_oracle_connection()
    query = """
        SELECT a.asset_id, a.ticker, a.asset_name 
        FROM assets a
        JOIN indices i ON a.asset_id = i.asset_id
        WHERE i.indice = 'CAC40'
    """
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]  # Ajoute cette ligne !
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
    df.columns = [c.lower() for c in df.columns]  # Ajoute cette ligne !
    conn.close()
    return df

def compute_technical_indicators(df):
    """Calcule tous les indicateurs techniques"""
    # Moyennes mobiles
    df['ma20'] = df['price'].rolling(20).mean()
    df['ma50'] = df['price'].rolling(50).mean()
    df['ma200'] = df['price'].rolling(200).mean()
    
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

def generate_signals(df):
    """Génère les signaux d'achat/vente"""
    df['signal'] = 0  # 0: neutre, 1: achat, -1: vente
    
    # Conditions d'achat
    buy_condition = (
        (df['rsi'] < 30) &
        (df['price'] > df['ma200']) &  # Prix au-dessus MM200
        (df['volume'] > df['vol_ma20'])  # Volume supérieur à la moyenne
    )
    
    # Conditions de vente
    sell_condition = (
        (df['rsi'] > 70) |
        (df['price'] < df['ma50'])  # Prix sous MM50
    )
    
    df.loc[buy_condition, 'signal'] = 1
    df.loc[sell_condition, 'signal'] = -1
    
    return df

def backtest_strategy():
    """Backtest complet de la stratégie"""
    logger.info("Début du backtest")
    
    # Paramètres
    start_date = (datetime.now() - timedelta(days=1825)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    initial_capital = 10000
    position_size = 0.1  # 10% du capital par position
    
    # Récupération des assets
    assets = get_cac40_assets()
    if assets.empty:
        logger.error("Aucun actif CAC40 trouvé")
        return
    
    portfolio = {}
    results = []
    
    for _, asset in assets.iterrows():
        try:
            # Récupération des données
            df = get_historical_data(asset['asset_id'], start_date, end_date)
            if len(df) < 200:
                continue
                
            # Calcul des indicateurs
            df = compute_technical_indicators(df)
            df = generate_signals(df)
            
            # Simulation des trades
            position = 0
            entry_price = 0
            trade_results = []
            
            for i, row in df.iterrows():
                if row['signal'] == 1 and position == 0:  # Achat
                    position = (initial_capital * position_size) / row['price']
                    entry_price = row['price']
                    trade_results.append({
                        'date': row['price_date'],
                        'action': 'BUY',
                        'price': row['price'],
                        'shares': position
                    })
                elif row['signal'] == -1 and position > 0:  # Vente
                    trade_results.append({
                        'date': row['price_date'],
                        'action': 'SELL',
                        'price': row['price'],
                        'shares': position,
                        'return': (row['price'] - entry_price) / entry_price
                    })
                    position = 0
                    
            if trade_results:
                results.append({
                    'ticker': asset['ticker'],
                    'name': asset['asset_name'],
                    'trades': trade_results,
                    'total_return': sum(t['return'] for t in trade_results if 'return' in t)
                })
                
        except Exception as e:
            logger.error(f"Erreur pour {asset['ticker']}: {str(e)}")
    
    # Analyse des résultats
    if results:
        total_return = sum(r['total_return'] for r in results) / len(results)
        best_trade = max(results, key=lambda x: x['total_return'])
        worst_trade = min(results, key=lambda x: x['total_return'])
        
        logger.info(f"\nRésultats du backtest:")
        logger.info(f"Rendement moyen: {total_return:.2%}")
        logger.info(f"Meilleur trade: {best_trade['name']} ({best_trade['total_return']:.2%})")
        logger.info(f"Pire trade: {worst_trade['name']} ({worst_trade['total_return']:.2%})")
        
        # Export des résultats pour analyse
        pd.DataFrame(results).to_csv('backtest_results.csv', index=False)
    else:
        logger.info("Aucun trade généré pendant la période")

if __name__ == "__main__":
    backtest_strategy()
