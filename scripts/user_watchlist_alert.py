import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.discord_manager import DiscordManager
from scripts.utils.portfolio_tracker import PortfolioTracker
from scripts.utils.logger import logger
from scripts.utils.log_checker import check_and_alert_log
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv
import os

# Configuration
load_dotenv(os.path.expanduser('~/market-watcher/config/.env'))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_STOCK_WATCHER")

class WatchlistAnalyzer:
    def __init__(self):
        self.discord = DiscordManager(DISCORD_WEBHOOK_URL)
        self.portfolio = PortfolioTracker()

    def get_user_watchlists(self):
        """Récupère les watchlists par utilisateur"""
        conn = get_oracle_connection()
        query = """
            SELECT 
                DISCORD_ID, 
                DISCORD_USER,
                ASSET_ID, 
                TICKER, 
                ASSET_NAME,
                ASSET_TYPE
            FROM USER_WATCHLIST
            WHERE ASSET_TYPE IN ('STOCK', 'ETF', 'CRYPTO')
            ORDER BY ASSET_TYPE, TICKER
        """
        df = pd.read_sql(query, conn)
        conn.close()
        df.columns = [c.lower() for c in df.columns]
        return df.groupby('discord_id')

    def analyze_asset(self, asset_id, asset_type):
        """Version optimisée avec gestion complète des cryptos"""
        conn = None
        try:
            conn = get_oracle_connection()
            if asset_type == 'CRYPTO':
                query = f"""
                    SELECT 
                        TRUNC(price_date) as price_date,
                        MAX(price_value) as price,
                        SUM(volume) as volume
                    FROM prices
                    WHERE asset_id = {asset_id}
                    AND price_value IS NOT NULL
                    AND price_date >= SYSDATE - 400
                    GROUP BY TRUNC(price_date)
                    ORDER BY TRUNC(price_date) ASC
                """
            else:
                query = f"""
                    SELECT 
                        price_date,
                        close_value as price,
                        volume
                    FROM prices
                    WHERE asset_id = {asset_id}
                    AND close_value IS NOT NULL
                    AND price_date >= SYSDATE - 400
                    ORDER BY price_date ASC
                """

            df = pd.read_sql(query, conn)
            df.columns = [c.lower() for c in df.columns]
            df = df.sort_values('price_date', ascending=True).reset_index(drop=True)
            df = df.dropna(subset=['price'])
            df = df[df['price'] > 0]

            if len(df) < 14:
                logger.warning(f"Données insuffisantes ({len(df)} points) pour {asset_id}")
                return None

            # Calcul des indicateurs
            ma_window = min(52, len(df))
            rsi_window = min(14, len(df)-1)
            vol_window = min(20, len(df))
            min_periods_ma = max(1, int(ma_window * 0.3))
            min_periods_vol = max(1, int(vol_window * 0.5))

            try:
                df['ma52'] = df['price'].rolling(window=ma_window, min_periods=min_periods_ma).mean()
                df['ma104'] = df['price'].rolling(window=104, min_periods=52).mean()
                delta = df['price'].diff()
                gain = delta.where(delta > 0, 0)
                loss = -delta.where(delta < 0, 0)
                avg_gain = gain.rolling(rsi_window).mean()
                avg_loss = loss.rolling(rsi_window).mean()
                rs = avg_gain / (avg_loss + 1e-10)
                df['rsi'] = 100 - (100 / (1 + rs))
                df['vol_ma20'] = df['volume'].rolling(window=vol_window, min_periods=min_periods_vol).mean()
            except Exception as calc_error:
                logger.error(f"Erreur calcul indicateurs: {str(calc_error)}")
                return None

            last = df.iloc[-1]
            result = {
                'price': last['price'],
                'rsi': float(last['rsi']) if not pd.isna(last['rsi']) else None,
                'ma52': float(last['ma52']) if not pd.isna(last['ma52']) else None,
                'ma104': float(last['ma104']) if 'ma104' in last and not pd.isna(last['ma104']) else None,
                'volume_ratio': (float(last['volume']) / last['vol_ma20']) if (last['vol_ma20'] > 0) else None,
                'evolution': self.compute_evolution(df)
            }
            logger.debug(f"Résultats pour {asset_id}: {result}")
            return result

        except Exception as e:
            logger.error(f"Erreur majeure sur asset {asset_id}: {str(e)}", exc_info=True)
            return None
        finally:
            if conn:
                conn.close()

    def compute_rsi(self, prices, window=14):
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window).mean()
        avg_loss = loss.rolling(window).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def compute_evolution(self, df):
        def safe_change(days):
            try:
                if df.empty:
                    return None
                last_price = df.iloc[-1]['price']
                last_date = df.iloc[-1]['price_date']
                target_date = last_date - pd.Timedelta(days=days)
                # Cherche la ligne la plus proche de target_date
                past_rows = df[df['price_date'] <= target_date]
                if past_rows.empty:
                    return None
                past_price = past_rows.iloc[-1]['price']
                if past_price == 0:
                    return None
                change = (last_price - past_price) / past_price
                return float(change * 100)
            except Exception:
                return None

        return {
            '1j': safe_change(1),
            '5j': safe_change(5),
            '1m': safe_change(30),
            '3m': safe_change(90),
            '6m': safe_change(180),
            '1a': safe_change(365)
        }

    def generate_signal(self, data):
        """Génère un signal de trading avec nouveaux seuils"""
        if (
            data is None or
            data['rsi'] is None or
            data['price'] is None or
            data['ma52'] is None
        ):
            return None

        signal = None
        if data['rsi'] < 30 and data['price'] > data['ma52']:
            signal = 'BUY'
        elif data['rsi'] > 70:
            signal = 'SELL'
        return signal

    def generate_detailed_report(self, discord_id, group):
        # 1. Récupération des données
        asset_reports = []
        for _, row in group.iterrows():
            analysis = self.analyze_asset(row['asset_id'], row['asset_type'])
            if analysis:
                asset_reports.append({
                    'ticker': row['ticker'],
                    'name': row['asset_name'],
                    'type': row['asset_type'],
                    'price': analysis['price'],
                    'ma52': analysis['ma52'],
                    'ma104': analysis.get('ma104', None),
                    'rsi': analysis['rsi'],
                    'evolution': analysis['evolution'],
                    'signal': self.generate_signal(analysis)
                })

        # 2. Génération du rapport complet
        full_report = self._generate_full_report(asset_reports, discord_id)
        
        # 3. Envoi simple - le DiscordManager gère le découpage
        self.discord.send_detailed_report(discord_id, full_report)

    def _generate_full_report(self, assets, discord_id):
        """Génère le rapport complet en une seule string"""
        report = f"<@{discord_id}> 📊 Rapport\n\n"
        report += self._generate_summary_section(assets) or ""
        report += self._generate_top_performers_section(assets) or ""
        
        # Sections détaillées par type
        sections_by_type = {}
        for asset in sorted(assets, key=lambda x: x['type']):
            asset_type = asset['type']
            if asset_type not in sections_by_type:
                sections_by_type[asset_type] = []
            asset_text = (
                f"\n**{asset['ticker']} - {asset['name']}**\n"
                f"▫️ Prix: {asset['price']:.2f}€\n"
                f"▫️ MA52: {asset['ma52']:.2f}€\n"
                f"▫️ RSI: {asset['rsi']:.1f} ({self._get_signal_text(asset)})\n"
                f"▫️ 1j: {self._format_evolution(asset['evolution']['1j'])}\n"
                f"▫️ 1m: {self._format_evolution(asset['evolution']['1m'])}\n"
                f"▫️ 3m: {self._format_evolution(asset['evolution']['3m'])}\n"
                f"▫️ 1an: {self._format_evolution(asset['evolution']['1a'])}\n"
            )
            sections_by_type[asset_type].append(asset_text)
        for asset_type, asset_texts in sections_by_type.items():
            report += f"\n🔷 **{asset_type.upper()}**\n"
            report += "".join(asset_texts)
        return report

    def _get_signal_text(self, asset):
        """Retourne le texte du signal"""
        if asset['rsi'] < 30 and asset['price'] < asset['ma52']:
            return "🟢 ACHETER"
        elif asset['rsi'] > 70:
            return "🔴 VENDRE"
        else:
            return "🟡 CONSERVER"

    def _generate_summary_section(self, assets):
        """Synthèse globale en 1 bloc compact"""
        df = pd.DataFrame(assets)
        if df.empty:
            return None

        section = "🔷 **SYNTHÈSE GLOBALE**\n"
        # Performance par type
        for asset_type, group in df.groupby('type'):
            perf_1m = [e.get('1m', 0) for e in group['evolution'] if e.get('1m') is not None]
            perf_1y = [e.get('1a', 0) for e in group['evolution'] if e.get('1a') is not None]
            avg_1m = sum(perf_1m) / len(perf_1m) if perf_1m else 0
            avg_1y = sum(perf_1y) / len(perf_1y) if perf_1y else 0
            section += (
                f"▫️ **{asset_type}**\n"
                f"   - 1 mois: {self._format_percent(avg_1m)}\n"
                f"   - 1 an: {self._format_percent(avg_1y)}\n"
            )

        # Ajout de la performance globale tous types confondus
        all_perf_1m = [e.get('1m', 0) for e in df['evolution'] if e.get('1m') is not None]
        all_perf_1y = [e.get('1a', 0) for e in df['evolution'] if e.get('1a') is not None]
        avg_all_1m = sum(all_perf_1m) / len(all_perf_1m) if all_perf_1m else 0
        avg_all_1y = sum(all_perf_1y) / len(all_perf_1y) if all_perf_1y else 0
        section += (
            f"\n▫️ **GLOBAL**\n"
            f"   - 1 mois: {self._format_percent(avg_all_1m)}\n"
            f"   - 1 an: {self._format_percent(avg_all_1y)}\n"
        )
        return section

    def _generate_top_performers_section(self, assets):
        """Top 5 sur 1 mois et 1 an"""
        def get_top(period, limit=5):
            valid = [a for a in assets if a['evolution'].get(period) is not None]
            return sorted(valid, key=lambda x: x['evolution'][period], reverse=True)[:limit]

        section = "\n🏆 **TOP PERFORMERS**\n"
        # Top 1 mois
        section += "▫️ **1 mois**\n"
        for i, asset in enumerate(get_top('1m'), 1):
            section += f"   {i}. {asset['ticker']}: {self._format_percent(asset['evolution']['1m'])}\n"
        # Top 1 an
        section += "\n▫️ **1 an**\n"
        for i, asset in enumerate(get_top('1a'), 1):
            section += f"   {i}. {asset['ticker']}: {self._format_percent(asset['evolution']['1a'])}\n"
        return section

    def _generate_asset_detail_sections(self, assets):
        """Détail par actif groupé par type"""
        sections = []
        current_type = None
        for asset in sorted(assets, key=lambda x: x['type']):
            # Nouvelle section par type d'actif
            if asset['type'] != current_type:
                current_type = asset['type']
                sections.append(f"\n🔷 **{current_type.upper()}**\n")
            # Signal RSI
            if asset['rsi'] < 30 and asset['price'] < asset['ma52']:
                signal = "🟢 ACHETER"
            elif asset['rsi'] > 70:
                signal = "🔴 VENDRE"
            else:
                signal = "🟡 CONSERVER"
            # Formatage détaillé
            sections[-1] += (
                f"\n**{asset['ticker']} - {asset['name']}**\n"
                f"▫️ Prix: {asset['price']:.2f}€\n"
                f"▫️ MA52: {asset['ma52']:.2f}€\n"
                f"▫️ RSI: {asset['rsi']:.1f} ({signal})\n"
                f"▫️ 1j: {self._format_evolution(asset['evolution']['1j'])}\n"
                f"▫️ 1m: {self._format_evolution(asset['evolution']['1m'])}\n"
                f"▫️ 3m: {self._format_evolution(asset['evolution']['3m'])}\n"
                f"▫️ 1an: {self._format_evolution(asset['evolution']['1a'])}\n"
            )
        return sections

    def _format_percent(self, value):
        """Formatage des pourcentages"""
        if value is None:
            return "N/A"
        emoji = "🚀" if value > 15 else "📈" if value > 5 else "🟢" if value > 0 else "🔴"
        return f"{emoji} {value:+.2f}%"

    def _format_evolution(self, value):
        """Formatage cohérent avec tes screenshots"""
        if value is None:
            return "N/A"
        if value > 15:
            comment = "(forte hausse)"
        elif value > 5:
            comment = "(hausse)"
        elif value < -15:
            comment = "(forte baisse)"
        elif value < -5:
            comment = "(baisse)"
        else:
            comment = "(stable)"
        return f"{self._format_percent(value)} {comment}"

    def run(self):
        """Version simplifiée sans doublons"""
        user_groups = list(self.get_user_watchlists())
        for discord_id, group in user_groups:
            try:
                # Envoi du rapport unique
                detailed_report = self.generate_detailed_report(discord_id, group)
                if detailed_report:
                    self.discord.send_detailed_report(discord_id, detailed_report)
            except Exception as e:
                logger.error(f"ERREUR utilisateur {discord_id}: {str(e)}")
                continue



if __name__ == "__main__":
    try:
        logger.info("Démarrage du script UserWatchlistAlert")
        analyzer = WatchlistAnalyzer()
        analyzer.run()
    except Exception as e:
        logger.error(f"Erreur critique: {str(e)}")
        raise

check_and_alert_log("user_watchlist_alert", "user_watchlist_alert")