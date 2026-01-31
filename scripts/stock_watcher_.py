import pandas as pd
from scripts.utils.bd_oracle_connection import get_oracle_connection
from scripts.utils.log_checker import check_and_alert_log
from pathlib import Path
from dotenv import load_dotenv
import requests
import numpy as np
from datetime import timedelta
import collections

load_dotenv(str(Path.home() / "market-watcher/config/.env"))

def get_assets_user_watchlist():
    conn = get_oracle_connection()
    df = pd.read_sql("""
        SELECT DISTINCT a.asset_id, a.ticker, a.asset_name, a.asset_type
        FROM assets a
        JOIN user_watchlist uw ON a.asset_id = uw.asset_id
        WHERE a.asset_type IN ('STOCK', 'ETF')
    """, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df

def get_prices(asset_id):
    conn = get_oracle_connection()
    df = pd.read_sql(f"""
        SELECT price_date, close_value as price, volume
        FROM prices
        WHERE asset_id = {asset_id} AND close_value IS NOT NULL
        ORDER BY price_date
    """, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df

def compute_rsi(prices, window=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_ma(series, window):
    return series.rolling(window).mean()

def compute_bollinger_bands(series, window=20, num_std=2):
    ma = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = ma + num_std * std
    lower = ma - num_std * std
    return lower, upper

def compute_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd, signal_line

def send_discord_message(webhook_url, message):
    max_length = 1900  # On laisse un peu de marge pour le formatage
    for i in range(0, len(message), max_length):
        chunk = message[i:i+max_length]
        data = {"content": f"```md\n{chunk}\n```"}
        response = requests.post(webhook_url, json=data)
        if response.status_code != 204:
            print(f"Erreur Discord: {response.status_code} - {response.text}")

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1389316827900936193/vs-rrPKrNEWuxaKIre5rTOqR19cTmKsM7fqlcCJTwpRrQIS3nIUHbDqOhTRQxAyAhoMu"

def compute_evolutions(df, last_idx):
    evol = {}
    last_price = df.iloc[last_idx]["price"]
    date_last = df.iloc[last_idx]["price_date"]

    def get_past_price(days):
        target_date = date_last - timedelta(days=days)
        past = df[df["price_date"] <= target_date]
        if not past.empty:
            return past.iloc[-1]["price"]
        return np.nan

    for label, days in [("1j", 1), ("5j", 5), ("3m", 90), ("1a", 365), ("3a", 365*3), ("5a", 365*5)]:
        past_price = get_past_price(days)
        evol[label] = ((last_price - past_price) / past_price * 100) if past_price and not np.isnan(past_price) else None
    return evol

def format_evol(evol):
    txt = ""
    for label in ["1j", "5j", "3m", "1a", "3a", "5a"]:
        val = evol.get(label)
        if val is None:
            txt += f"{label}: N/A  "
        else:
            emoji = "🚀" if val > 3 else "📉" if val < -3 else "⏸️"
            txt += f"{label}: {val:+.2f}% {emoji}  "
    return txt.strip()

def format_signaux(signaux, titre):
    if not signaux:
        return f"**{titre}**\nAucun signal détecté.\n"
    msg = f"**{titre} :**\n"
    for s in signaux:
        msg += (
            f"**{s['asset_name']}** ({s['asset_type']})\n"
            f"  Prix : {s['dernier_prix']:.2f} | Date : {s['date']}\n"
            f"  RSI : {s['rsi']:.2f} | MA200 : {s['ma200']:.2f}\n"
            f"  Bollinger : [{s['boll_lower']:.2f} ; {s['boll_upper']:.2f}]\n"
            f"  MACD : {s['macd']:.2f} / {s['macd_signal']:.2f}\n"
            f"  Volume : {s['volume']:.0f} (Moy20j : {s['vol_ma20']:.0f})\n"
            f"  Evol : {format_evol(s['evol'])}\n"
            "-----------------------------\n"
        )
    return msg

def format_conserver(conserver):
    if not conserver:
        return ""
    msg = "**À conserver :**\n"
    for s in conserver:
        msg += (
            f"• {s['asset_name']} ({s['asset_type']})\n"
            f"   1j : {evolution_message(s['evol']['1j'])}\n"
            f"   5j : {evolution_message(s['evol']['5j'])}\n"
            f"   3m : {evolution_message(s['evol']['3m'])}\n"
            f"   1a : {evolution_message(s['evol']['1a'])}\n"
            f"   3a : {evolution_message(s['evol']['3a'])}\n"
            f"   5a : {evolution_message(s['evol']['5a'])}\n"
            "-----------------------------\n"
        )
    return msg

def evolution_message(evol):
    if pd.isna(evol):
        return "Pas de données"
    elif evol > 5:
        return f"🚀 +{evol:.2f}% (forte hausse)"
    elif evol > 1:
        return f"📈 +{evol:.2f}%"
    elif evol < -5:
        return f"🔻 {evol:.2f}% (forte baisse)"
    elif evol < -1:
        return f"📉 {evol:.2f}%"
    else:
        return f"➖ {evol:.2f}% (stable)"

def synthese_evol(user_evol):
    msg = ""
    for user, evols in user_evol.items():
        msg += f"**Évolution moyenne de la watchlist {user} :**\n"
        for label, emoji in [("1j", "1 jour"), ("5j", "5 jours"), ("3m", "3 mois"), ("1a", "1 an"), ("3a", "3 ans"), ("5a", "5 ans")]:
            vals = evols[label]
            if vals:
                avg = np.nanmean(vals)
                msg += f"- {emoji} : {evolution_message(avg)}\n"
            else:
                msg += f"- {emoji} : N/A\n"
        msg += "\n"
    return msg

if __name__ == "__main__":
    assets = get_assets_user_watchlist()
    achats, ventes, conserver = [], [], []
    for _, row in assets.iterrows():
        df = get_prices(row["asset_id"])
        if len(df) < 200 or df["price"].isnull().all():
            continue

        df["ma200"] = compute_ma(df["price"], 200)
        df["rsi14"] = compute_rsi(df["price"], 14)
        df["vol_ma20"] = compute_ma(df["volume"], 20) if "volume" in df.columns else np.nan
        df["volume"] = df.get("volume", np.nan)
        lower, upper = compute_bollinger_bands(df["price"])
        df["boll_lower"] = lower
        df["boll_upper"] = upper
        macd, macd_signal = compute_macd(df["price"])
        df["macd"] = macd
        df["macd_signal"] = macd_signal

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        achat = (
            (last["rsi14"] < 30) and
            (last["price"] < last["ma200"]) and
            (last.get("volume", 0) > last.get("vol_ma20", 0)) and
            (last["price"] <= last["boll_lower"]) and
            (prev["macd"] < prev["macd_signal"]) and (last["macd"] > last["macd_signal"])
        )
        vente = (
            (last["rsi14"] > 70) and
            (last["price"] > last["ma200"]) and
            (last.get("volume", 0) > last.get("vol_ma20", 0)) and
            (last["price"] >= last["boll_upper"]) and
            (prev["macd"] > prev["macd_signal"]) and (last["macd"] < last["macd_signal"])
        )

        evol = compute_evolutions(df, df.index[-1])
        row_evol = {
            "asset_name": row["asset_name"],
            "asset_type": row["asset_type"],
            "evol": evol,
            "discord_user": row.get("discord_user", "Tous")
        }
        if achat:
            achats.append(row_evol)
        elif vente:
            ventes.append(row_evol)
        else:
            conserver.append(row_evol)

    # Regroupe les évolutions par utilisateur
    user_evol = collections.defaultdict(lambda: {"1j": [], "5j": [], "3m": [], "1a": [], "3a": [], "5a": []})

    for row in achats + ventes + conserver:
        user = row.get("discord_user", "Tous")
        evol = row.get("evol", {})
        for k in user_evol[user]:
            if evol.get(k) is not None:
                user_evol[user][k].append(evol[k])

    message = (
        synthese_evol(user_evol) +
        format_signaux(achats, "Signaux d'achat") +
        format_signaux(ventes, "Signaux de vente") +
        format_conserver(conserver)
    )

    send_discord_message(DISCORD_WEBHOOK_URL, message)
    check_and_alert_log("stock_watcher_", "stock_watcher_")