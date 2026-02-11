from sqlalchemy import create_engine, text
import os

SQL_CREATE = """
CREATE TABLE IF NOT EXISTS returns_1h (
  symbol TEXT NOT NULL,
  time TIMESTAMPTZ NOT NULL,
  close NUMERIC NOT NULL,
  r NUMERIC,
  PRIMARY KEY (symbol, time)
);
"""

SQL_INSERT = """
INSERT INTO returns_1h (symbol, time, close, r)
SELECT
  symbol,
  open_time AS time,
  close,
  LN(close) - LN(LAG(close) OVER (PARTITION BY symbol ORDER BY open_time)) AS r
FROM candles
WHERE interval = '1h'
ON CONFLICT (symbol, time) DO NOTHING;
"""

def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE))
        conn.execute(text(SQL_INSERT))

if __name__ == "__main__":
    main()
