import pandas as pd
from scripts.utils.logger import logger

class PortfolioTracker:
    def __init__(self):
        self.portfolio_data = pd.DataFrame(columns=['user_id', 'ticker', 'performance', 'date'])
        logger.info("PortfolioTracker initialisé.")

    def update_user_portfolio(self, user_id, assets):
        """Met à jour les performances du portefeuille"""
        date = pd.Timestamp.now()
        new_data = pd.DataFrame([{
            'user_id': user_id,
            'ticker': asset['ticker'],
            'performance': asset['performance'],
            'date': date
        } for asset in assets])

        self.portfolio_data = pd.concat([self.portfolio_data, new_data])
        logger.info(f"Portefeuille mis à jour pour {user_id} avec {len(assets)} actifs.")

    def generate_portfolio_report(self, user_id):
        """Génère un rapport de performance"""
        user_data = self.portfolio_data[self.portfolio_data['user_id'] == user_id]

        if user_data.empty:
            logger.warning(f"Aucune donnée de portefeuille pour {user_id}.")
            return None

        report = "📊 **Votre performance globale**\n"
        avg_perf = user_data['performance'].mean()
        report += f"➡️ Moyenne: {avg_perf:+.2f}%\n\n"

        # Top 3 des meilleures performances
        top = user_data.nlargest(3, 'performance')
        report += "🏆 **Top 3**\n"
        for _, row in top.iterrows():
            report += f"- {row['ticker']}: {row['performance']:+.2f}%\n"

        logger.info(f"Rapport de portefeuille généré pour {user_id}.")
        return report
