from sqlalchemy import text
from src.db.db import get_engine

SQL_CREATE = """
CREATE TABLE IF NOT EXISTS returns_5m (
  symbol TEXT NOT NULL,
  time TIMESTAMPTZ NOT NULL,
  close NUMERIC NOT NULL,
  r NUMERIC,
  PRIMARY KEY (symbol, time)
);
"""

SQL_INSERT = """
INSERT INTO returns_5m (symbol, time, close, r)
SELECT
  symbol,
  open_time AS time,
  close,
  LN(close) - LN(LAG(close) OVER (PARTITION BY symbol ORDER BY open_time)) AS r
FROM candles
WHERE interval = '5m'
ON CONFLICT (symbol, time) DO NOTHING;
"""

def main() -> None:
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE))
        conn.execute(text(SQL_INSERT))

if __name__ == "__main__":
    main()
