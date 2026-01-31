import os
import requests
from datetime import datetime, timedelta
import glob
from pathlib import Path

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1391519148076695613/CdDIRTgzkhYGnqe11a1nLTficoUVbR9mQrL2u0TOPMYLgCj5jLJwuSmZb2lKDgbV7MUU"
LOG_DIR = Path(__file__).parent.parent.parent / "logs"

def check_and_alert_log(script_name, prefix, hours=1):
    now = datetime.now()
    problems = []
    for h in range(hours):
        dt = now - timedelta(hours=h)
        pattern = f"{prefix}_{dt.strftime('%Y%m%d_%H')}*.log"
        log_files = sorted(
            glob.glob(os.path.join(LOG_DIR, pattern)),
            reverse=True
        )
        if not log_files:
            problems.append(f"❌ Log manquant pour **{script_name}** : {pattern}")
            continue
        logpath = log_files[0]  # Prend le plus récent
        with open(logpath, "r") as f:
            content = f.read()
            if "ERROR" in content or "❌" in content:
                problems.append(f"❌ Erreur détectée dans **{script_name}** ({os.path.basename(logpath)})")
    # Envoi d'une alerte uniquement s'il y a des problèmes
    if problems:
        subject = f"🚨 Problème {script_name}"
        body = "\n".join(problems)
        data = {"content": f"**{subject}**\n{body}"}
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        if response.status_code != 204:
            print(f"Erreur Discord: {response.status_code} - {response.text}")