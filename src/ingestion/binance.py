import os
from decimal import Decimal
from datetime import datetime, timedelta
import requests
import psycopg

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

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

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

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, values)
        conn.commit()

    return len(values)





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
