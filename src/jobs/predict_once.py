import os
import pickle
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.environ["DATABASE_URL"]
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL, pool_pre_ping=True)

def load_latest_artifact():
    q = text("""
      SELECT artifact
      FROM model_artifacts
      WHERE symbol='BTCUSDT' AND freq='1h' AND target='abs_return'
      ORDER BY trained_at DESC
      LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(q).mappings().first()
    if not row:
        raise RuntimeError("No model artifact found. Run trainer first.")
    return pickle.loads(row["artifact"])

def load_recent_returns(n=50) -> pd.DataFrame:
    q = text("""
      SELECT time, r
      FROM returns_1h
      WHERE symbol='BTCUSDT'
      ORDER BY time DESC
      LIMIT :n
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"n": n}).mappings().all()
    df = pd.DataFrame(rows).sort_values("time")
    df["r"] = pd.to_numeric(df["r"], errors="coerce")
    df = df.dropna(subset=["r"])
    df["abs_r"] = df["r"].abs()
    return df.dropna()

def make_latest_features(df: pd.DataFrame):
    # need at least 10 points for roll_std10
    abs_r = df["abs_r"]
    r = df["r"]

    feats = {
        "lag0": float(abs_r.iloc[-1]),
        "lag1": float(abs_r.iloc[-2]),
        "lag2": float(abs_r.iloc[-3]),
        "lag3": float(abs_r.iloc[-4]),
        "roll_mean5": float(abs_r.iloc[-5:].mean()),
        "roll_std10": float(r.iloc[-10:].std()),
    }
    return feats

def main():
    art = load_latest_artifact()
    scaler = art["scaler"]
    model = art["model"]
    cols = art["feature_cols"]

    df = load_recent_returns(60)
    feats = make_latest_features(df)

    X = [[feats[c] for c in cols]]
    Xs = scaler.transform(X)
    yhat = float(model.predict(Xs)[0])

    # predict for next hour (last time + 1h)
    last_time = df["time"].iloc[-1]
    q_ins = text("""
      INSERT INTO predictions (symbol, freq, target, predicted_for, yhat)
      VALUES ('BTCUSDT', '1h', 'abs_return', :pred_for, :yhat)
      ON CONFLICT (symbol, freq, target, predicted_for) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(q_ins, {"pred_for": last_time + pd.Timedelta(hours=1), "yhat": yhat})

    print("predicted_for", (last_time + pd.Timedelta(hours=1)).isoformat(), "yhat", yhat)

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

