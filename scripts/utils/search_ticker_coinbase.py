import requests
from scripts.utils.logger import logger

COINBASE_API = "https://api.exchange.coinbase.com"

def search_pair_coinbase(pair: str):
    """
    Vérifie si une paire (ex: BTC-EUR) existe sur Coinbase.
    Retourne (PAIR, NOM_CRYPTO) ou (None, None) si non trouvée.
    """
    url_products = f"{COINBASE_API}/products"
    url_currencies = f"{COINBASE_API}/currencies"
    logger.info(f"🔎 Vérification de la paire {pair} sur Coinbase...")
    try:
        resp = requests.get(url_products, timeout=5)
        resp.raise_for_status()
        products = resp.json()
        for prod in products:
            if prod.get("id", "").upper() == pair.upper():
                base = prod.get("base_currency", "")
                quote = prod.get("quote_currency", "")
                logger.info(f"✅ Paire trouvée : {prod['id']} ({base}/{quote})")
                # Cherche le nom officiel de la crypto (base_currency)
                resp2 = requests.get(url_currencies, timeout=5)
                resp2.raise_for_status()
                for cur in resp2.json():
                    if cur.get("id", "").upper() == base.upper():
                        name = cur.get("name", "")
                        logger.info(f"ℹ️ Nom officiel pour {base}: {name}")
                        return prod["id"], name
                # Si pas trouvé, retourne juste le code
                logger.warning(f"Nom officiel non trouvé pour {base}")
                return prod["id"], base
        logger.warning(f"❌ Paire {pair} non trouvée sur Coinbase.")
        return None, None
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de la paire {pair} : {e}")
        return None, None

# Exemple de test
#if __name__ == "__main__":
 #   for pair in ["BTC-EUR", "ETH-USD", "SOL-EUR", "ADA-USD", "FAKE-EUR"]:
  #      symbol, name = search_pair_coinbase(pair)
   #     if symbol:
    #        print(f"✅ {pair} trouvé : {symbol} - {name}")
     #   else:
      #      print(f"❌ {pair} non trouvé sur Coinbase")