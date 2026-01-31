import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import os

def generate_report(results_df):
    """Génère un rapport PDF avec les résultats"""
    os.makedirs('data/outputs/reports', exist_ok=True)
    
    with PdfPages('data/outputs/reports/strategy_report.pdf') as pdf:
        # Page 1 : Performances globales
        plt.figure(figsize=(10, 6))
        plt.bar(results_df['ticker'], results_df['avg_return'] * 100)
        plt.title("Rendement moyen par action")
        plt.ylabel("Rendement (%)")
        plt.xticks(rotation=45)
        pdf.savefig()
        plt.close()
        
        # Page 2 : Détail des paramètres optimaux
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.axis('off')
        table = ax.table(
            cellText=results_df[['ticker', 'params', 'n_trades']].values,
            colLabels=['Ticker', 'Paramètres (RSI buy/sell, MA, Vol)', 'Nb Trades'],
            loc='center'
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        pdf.savefig()
        plt.close()
