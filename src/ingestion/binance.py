from decimal import Decimal
from datetime import datetime, timedelta
import requests
from sqlalchemy import text
from math import log
from src.db.db import get_engine

def get_btc_data(start_ms):
    symbol = "BTCUSDT"
    interval = "1h"
    limit = 1000

    url = (
        "https://api.binance.com/api/v3/klines"
        f"?symbol={symbol}&interval={interval}&startTime={start_ms}&limit={limit}"
    )

    r = requests.get(url)

    r.raise_for_status()
    return r.json()

def insert_to_db(rows):
    if not rows:
        return 0

    symbol = "BTCUSDT"
    interval = "1h"
    insert_sql = """
        INSERT INTO candles (
            symbol, interval, open_time, open, high, low, close, volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, interval, open_time) DO NOTHING
    """

    values = []
    for row in rows:
        open_time = datetime.utcfromtimestamp(row[0] / 1000.0)
        values.append(
            (
                symbol,
                interval,
                open_time,
                Decimal(row[1]),
                Decimal(row[2]),
                Decimal(row[3]),
                Decimal(row[4]),
                Decimal(row[5]),
            )
        )

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(insert_sql), values)

    return len(values)

def calculate_hourly_returns(close_t, close_t_1):
    return log(close_t) - log(close_t_1)
    

if __name__ == "__main__":
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=365 * 5)

    start_ms = int(start_time.timestamp() * 1000)
    while True:
        rows = get_btc_data(start_ms)
        if not rows:
            break
        inserted = insert_to_db(rows)
        start_ms = rows[-1][0] + 1
