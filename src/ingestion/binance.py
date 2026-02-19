import os
from decimal import Decimal
from datetime import datetime, timedelta
from math import log
import requests
from sqlalchemy import text
from src.db.db import get_engine

INTERVAL = os.getenv("BINANCE_INTERVAL", "5m")


def _interval_ms(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    if unit == "m":
        return value * 60 * 1000
    if unit == "h":
        return value * 60 * 60 * 1000
    if unit == "d":
        return value * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported interval: {interval}")

def get_btc_data(start_ms):
    symbol = "BTCUSDT"
    interval = INTERVAL
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
    interval = INTERVAL
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
        conn.exec_driver_sql(insert_sql, values)

    return len(values)

def calculate_hourly_returns(close_t, close_t_1):
    return log(close_t) - log(close_t_1)

def get_latest_open_time_ms():
    engine = get_engine()
    q = text("""
      SELECT MAX(open_time) AS max_open
      FROM candles
      WHERE symbol = 'BTCUSDT' AND interval = :interval
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"interval": INTERVAL}).mappings().first()
    if row and row["max_open"]:
        return int(row["max_open"].timestamp() * 1000)
    return None

def filter_closed_rows(rows):
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    interval_ms = _interval_ms(INTERVAL)
    return [row for row in rows if row[0] + interval_ms <= now_ms]
    

if __name__ == "__main__":
    latest_ms = get_latest_open_time_ms()
    if latest_ms is None:
        end_time = datetime.utcnow()
        history_days = int(os.getenv("HISTORY_DAYS", "30"))
        start_time = end_time - timedelta(days=history_days)
        start_ms = int(start_time.timestamp() * 1000)
    else:
        start_ms = latest_ms + 1

    while True:
        rows = get_btc_data(start_ms)
        if not rows:
            break
        rows = filter_closed_rows(rows)
        if not rows:
            break
        inserted = insert_to_db(rows)
        start_ms = rows[-1][0] + 1
        if inserted == 0 and latest_ms is not None:
            break
