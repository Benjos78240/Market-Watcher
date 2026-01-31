

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.utils.logger import logger

import re
from datetime import datetime, timedelta
import requests
from scripts.utils.log_checker import check_and_alert_log

# Utilise le même webhook Discord que log_checker
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1391519148076695613/CdDIRTgzkhYGnqe11a1nLTficoUVbR9mQrL2u0TOPMYLgCj5jLJwuSmZb2lKDgbV7MUU"
AUTH_LOG = "/var/log/auth.log"
STATE_FILE = Path(__file__).parent / "last_auth_check.txt"

# Expressions régulières pour connexions SSH
SSH_SUCCESS = re.compile(r"Accepted (password|publickey) for (\w+) from ([\d.]+) port (\d+)")
SSH_FAIL = re.compile(r"Failed (password|publickey) for (\w+|invalid user \w+) from ([\d.]+) port (\d+)")


def get_last_position():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            pos = f.read()
            return int(pos) if pos.isdigit() else 0
    return 0

def save_last_position(pos):
    with open(STATE_FILE, "w") as f:
        f.write(str(pos))

def monitor_auth_log():
    alerts = []
    fail_count = 0
    whitelist_ips = {"88.191.209.171"}  # Ajoute ici d'autres IP autorisées si besoin
    try:
        with open(AUTH_LOG, "r") as f:
            f.seek(0, 2)
            end_pos = f.tell()
            last_pos = get_last_position()
            if last_pos > end_pos:
                last_pos = 0  # log rotated
            f.seek(last_pos)
            for line in f:
                if SSH_SUCCESS.search(line):
                    m = SSH_SUCCESS.search(line)
                    user, ip = m.group(2), m.group(3)
                    if ip not in whitelist_ips:
                        alerts.append(f"✅ Connexion SSH réussie : {user} depuis {ip}")
                        logger.info(f"Connexion SSH réussie : {user} depuis {ip}")
                    else:
                        logger.info(f"Connexion SSH whitelist : {user} depuis {ip}")
                elif SSH_FAIL.search(line):
                    fail_count += 1
                    logger.warning(f"Échec SSH détecté dans auth.log")
            save_last_position(f.tell())
        # Notifier seulement si attaque massive (>10 échecs sur la période)
        if fail_count > 10:
            alerts.append(f"❗️ {fail_count} échecs SSH détectés depuis le dernier contrôle.")
            logger.error(f"{fail_count} échecs SSH détectés depuis le dernier contrôle.")
    except Exception as e:
        alerts.append(f"Erreur lecture auth.log : {e}")
        logger.error(f"Erreur lecture auth.log : {e}")
    return alerts

def send_discord_alerts(alerts):
    if not alerts:
        return
    subject = f"🚨 Surveillance SSH {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    body = "\n".join(alerts)
    data = {"content": f"**{subject}**\n{body}"}
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        if response.status_code != 204:
            print(f"Erreur Discord: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Erreur envoi Discord: {e}")

if __name__ == "__main__":
    alerts = monitor_auth_log()
    send_discord_alerts(alerts)
    # Ajout de la vérification des logs système (ex: logs/ssh_access_monitor_*)
    check_and_alert_log("ssh_access_monitor", "ssh_access_monitor", hours=1)
