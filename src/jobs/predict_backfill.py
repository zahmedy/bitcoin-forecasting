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


def load_returns_1h() -> pd.DataFrame:
    q = text("""
      SELECT time, r
      FROM returns_1h
      WHERE symbol = 'BTCUSDT'
      ORDER BY time
    """)
    with engine.begin() as conn:
        rows = conn.execute(q).mappings().all()
    df = pd.DataFrame(rows)
    df["r"] = pd.to_numeric(df["r"], errors="coerce")
    df = df.dropna(subset=["r"]).reset_index(drop=True)
    df["abs_r"] = df["r"].abs()
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for k in [0, 1, 2, 3]:
        df[f"lag{k}"] = df["abs_r"].shift(k)
    df["roll_mean5"] = df["abs_r"].rolling(5).mean()
    df["roll_std10"] = df["r"].rolling(10).std()
    df = df.dropna().reset_index(drop=True)
    return df


def main(hours: int = 50) -> None:
    art = load_latest_artifact()
    scaler = art["scaler"]
    model = art["model"]
    cols = art["feature_cols"]

    df = build_features(load_returns_1h())
    if df.empty:
        print("No returns available for backfill")
        return

    df = df.tail(hours)
    X = df[cols].to_numpy()
    Xs = scaler.transform(X)
    yhats = model.predict(Xs)

    rows = []
    for i, row in df.iterrows():
        predicted_for = row["time"] + pd.Timedelta(hours=1)
        rows.append(
            {
                "pred_for": predicted_for,
                "yhat": float(yhats[df.index.get_loc(i)]),
            }
        )

    q_ins = text("""
      INSERT INTO predictions (symbol, freq, target, predicted_for, yhat)
      VALUES ('BTCUSDT', '1h', 'abs_return', :pred_for, :yhat)
      ON CONFLICT (symbol, freq, target, predicted_for) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(q_ins, rows)

    print(f"backfilled {len(rows)} predictions")


if __name__ == "__main__":
    main()
