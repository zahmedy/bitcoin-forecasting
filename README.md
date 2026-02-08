# bitcoin-forecasting
Forecast BTC price for next day or next hour 

```
data/
  raw/
    btc_1h.parquet
  processed/
    btc_1h_features.parquet
    btc_1d_features.parquet

src/
  ingestion/
    binance.py
  features/
    returns.py
    rolling.py
  backtest/
    walk_forward.py
```