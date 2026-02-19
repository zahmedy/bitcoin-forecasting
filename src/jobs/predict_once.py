import os
import pickle
import pandas as pd
from sqlalchemy import create_engine, text
from arch import arch_model

DB_URL = os.environ["DATABASE_URL"]
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL, pool_pre_ping=True)

def load_recent_returns(n=500) -> pd.DataFrame:
    q = text("""
      SELECT time, r
      FROM returns_5m
      WHERE symbol='BTCUSDT'
      ORDER BY time DESC
      LIMIT :n
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"n": n}).mappings().all()
    df = pd.DataFrame(rows).sort_values("time")
    if df.empty or "r" not in df.columns:
        return pd.DataFrame(columns=["time", "r"])
    df["r"] = pd.to_numeric(df["r"], errors="coerce")
    df = df.dropna(subset=["r"])
    return df.dropna().reset_index(drop=True)

def main():
    df = load_recent_returns(1000)
    if df.empty:
        print("No returns available for prediction")
        return

    am = arch_model(df["r"], mean="Zero", vol="GARCH", p=1, q=1, dist="normal")
    res = am.fit(disp="off")
    f = res.forecast(horizon=1, reindex=False)
    var = float(f.variance.iloc[-1, 0])
    yhat = var ** 0.5

    # predict for next 5m (last time + 5m)
    last_time = df["time"].iloc[-1]
    q_ins = text("""
      INSERT INTO predictions (symbol, freq, target, predicted_for, yhat)
      VALUES ('BTCUSDT', '5m', 'abs_return', :pred_for, :yhat)
      ON CONFLICT (symbol, freq, target, predicted_for) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(q_ins, {"pred_for": last_time + pd.Timedelta(minutes=5), "yhat": yhat})

    artifact = pickle.dumps({"model_type": "garch", "model": res})
    with engine.begin() as conn:
        conn.execute(
            text("""
              INSERT INTO model_artifacts (symbol, freq, target, trained_at, artifact)
              VALUES ('BTCUSDT', '5m', 'abs_return', now(), :artifact)
            """),
            {"artifact": artifact},
        )

    print("predicted_for", (last_time + pd.Timedelta(minutes=5)).isoformat(), "yhat", yhat)

import time
from datetime import datetime, timezone

def sleep_until_next_hour():
    now = datetime.now(timezone.utc)
    next_hour = (now.replace(minute=0, second=0, microsecond=0)
                 + pd.Timedelta(hours=1))
    sleep_seconds = (next_hour - now).total_seconds()
    time.sleep(max(0, sleep_seconds))

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print("prediction error:", e)
        sleep_until_next_hour()
