import requests
from scripts.utils.logger import logger

class DiscordManager:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self.MAX_MESSAGE_LENGTH = 2000

    def send_user_alert(self, discord_id, alerts):
        """Version optimisée de votre format préféré"""
        discord_id = str(discord_id)
        message = f"<@{discord_id}> 🌟 **Signaux Trading**\n\n"

        for alert in alerts:
            # Détermination du signal
            if alert['signal'] == 'BUY':
                signal_str = "🟢 ACHAT"
            elif alert['signal'] == 'SELL':
                signal_str = "🔴 VENTE"
            else:
                signal_str = "⚪ CONSERVER"

            # Construction du message
            message += (
                f"**{alert['ticker']} - {alert['name']}**\n"
                f"{signal_str} | Prix: {alert['price']:.2f}€ | RSI: {alert['rsi']:.1f}\n"
                f"📈 Evolution: "
                f"1j: {self._format_evolution(alert['evolution']['1j'])}, "
                f"1m: {self._format_evolution(alert['evolution']['1m'])}, "
                f"1an: {self._format_evolution(alert['evolution']['1a'])}\n\n"
            )

        # Utilisation de la nouvelle méthode d'envoi sécurisée
        self._send_safe(message)

    def send_detailed_report(self, discord_id, report):
        """Envoie un rapport avec gestion automatique de la longueur"""
        discord_id = str(discord_id)
        self._send_safe(report)

    def _format_evolution(self, value):
        """Formatage cohérent avec vos préférences"""
        if value is None:
            return "N/A"
        emoji = "🚀" if value > 5 else "📈" if value > 0 else "📉"
        return f"{emoji}{value:+.2f}%"

    def _send_safe(self, content):
        """Méthode d'envoi sécurisée avec découpage intelligent"""
        if len(content) <= self.MAX_MESSAGE_LENGTH:
            self._send(content)
            return
        
        # Découpage intelligent par lignes
        lines = content.split('\n')
        current_chunk = ""
        
        for line in lines:
            # Vérifie si l'ajout de cette ligne dépasse la limite
            test_chunk = current_chunk + "\n" + line if current_chunk else line
            
            if len(test_chunk) > self.MAX_MESSAGE_LENGTH:
                # Envoie le chunk actuel s'il n'est pas vide
                if current_chunk:
                    self._send(current_chunk)
                    current_chunk = line  # Commence un nouveau chunk avec la ligne actuelle
                else:
                    # La ligne seule est trop longue, on la tronque
                    truncated_line = line[:self.MAX_MESSAGE_LENGTH - 3] + "..."
                    self._send(truncated_line)
                    current_chunk = ""
            else:
                current_chunk = test_chunk
        
        # Envoie le dernier chunk
        if current_chunk:
            self._send(current_chunk)

    def _send(self, content):
        """Méthode d'envoi basique avec vérification finale"""
        # Assurance finale (ne devrait plus être nécessaire avec _send_safe)
        if len(content) > self.MAX_MESSAGE_LENGTH:
            content = content[:self.MAX_MESSAGE_LENGTH - 3] + "..."
        
        try:
            response = requests.post(
                self.webhook_url,
                json={"content": content},
                timeout=10
            )
            if response.status_code not in [200, 204]:
                logger.error(f"Erreur Discord: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Erreur envoi Discord: {str(e)}")
        logger.info(f"Message envoyé à Discord ({len(content)} caractères)")