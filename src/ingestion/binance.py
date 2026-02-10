import requests
from datetime import datetime, timedelta

def get_btc_data():
    symbol = "BTCUSDT"
    interval = "1h"
    limit = 1000

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=365 * 5)

    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    url = (
        "https://api.binance.com/api/v3/klines"
        f"?symbol={symbol}&interval={interval}&startTime={start_ms}&endTime={end_ms}&limit={limit}"
    )

    r = requests.get(url)

    r.raise_for_status()
    return r.json()

def insert_to_db():
    data = get_btc_data()
    

if __name__ == "__main__":
    data = get_btc_data()

