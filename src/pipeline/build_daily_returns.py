from sqlalchemy import text
from src.db.db import get_engine

SQL_CREATE = """
CREATE TABLE IF NOT EXISTS returns_1d (
  symbol TEXT NOT NULL,
  day DATE NOT NULL,
  close NUMERIC NOT NULL,
  r NUMERIC,
  PRIMARY KEY (symbol, day)
);
"""

SQL_INSERT = """
WITH daily_close AS (
    SELECT
        symbol,
        DATE(open_time) AS day,
        close,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, DATE(open_time)
            ORDER BY open_time DESC
        ) AS rn
    FROM candles
    WHERE interval = '5m'
),
daily_close_dedup AS (
    SELECT symbol, day, close
    FROM daily_close
    WHERE rn = 1
),
daily_returns AS (
    SELECT
        symbol,
        day,
        close,
        LN(close) - LN(LAG(close) OVER (PARTITION BY symbol ORDER BY day)) AS r
    FROM daily_close_dedup
)
INSERT INTO returns_1d (symbol, day, close, r)
SELECT symbol, day, close, r
FROM daily_returns
ON CONFLICT (symbol, day) DO NOTHING;
"""

def main() -> None:
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE))
        conn.execute(text(SQL_INSERT))

if __name__ == "__main__":
    main()
