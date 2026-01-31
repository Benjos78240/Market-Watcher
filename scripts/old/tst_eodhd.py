from eodhd import APIClient
import os
from dotenv import load_dotenv
from pathlib import Path
import time
from datetime import datetime

load_dotenv(str(Path.home() / "market-watcher/config/.env"))

api_key = os.getenv("EODHD_API_KEY")
api = APIClient(api_key)

resp = api.get_eod_historical_stock_market_data(
    symbol="SAF.PA",
    period="d",
    from_date="2024-07-01",
    to_date="2024-07-07",
    order="a"
)
print(resp)