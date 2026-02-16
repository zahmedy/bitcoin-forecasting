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

CREATE TABLE IF NOT EXISTS model_artifacts (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  freq TEXT NOT NULL,
  target TEXT NOT NULL,
  trained_at TIMESTAMPTZ NOT NULL,
  artifact BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS predictions (
  symbol TEXT NOT NULL,
  freq TEXT NOT NULL,
  target TEXT NOT NULL,
  predicted_for TIMESTAMPTZ NOT NULL,
  yhat NUMERIC NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, freq, target, predicted_for)
);
