CREATE TABLE IF NOT EXISTS candles (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  open_time TIMESTAMPTZ NOT NULL,
  open NUMERIC NOT NULL,
  high NUMERIC NOT NULL,
  low NUMERIC NOT NULL,
  close NUMERIC NOT NULL,
  volume NUMERIC NOT NULL,
  PRIMARY KEY (symbol, interval, open_time)
);

CREATE TABLE IF NOT EXISTS returns_1h (
  symbol TEXT NOT NULL,
  time TIMESTAMPTZ NOT NULL,
  close NUMERIC NOT NULL,
  r NUMERIC NOT NULL,
  PRIMARY KEY (symbol, time)
);

CREATE TABLE IF NOT EXISTS returns_1d (
  symbol TEXT NOT NULL,
  day DATE NOT NULL,
  close NUMERIC NOT NULL,
  r NUMERIC NOT NULL,
  PRIMARY KEY (symbol, day)
);