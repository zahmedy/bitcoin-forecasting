import os
import pickle
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

DB_URL = os.environ["DATABASE_URL"]
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL, pool_pre_ping=True)

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
    df = df.dropna(subset=["r"])
    df["abs_r"] = df["r"].abs()
    return df.dropna()

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    # target: next-hour abs return
    df = df.copy()
    df["y"] = df["abs_r"].shift(-1)
    for k in [0, 1, 2, 3]:
        df[f"lag{k}"] = df["abs_r"].shift(k)
    df["roll_mean5"] = df["abs_r"].rolling(5).mean()
    df["roll_std10"] = df["r"].rolling(10).std()
    df = df.dropna().reset_index(drop=True)
    return df

def main():
    df = build_features(load_returns_1h())

    feature_cols = ["lag0", "lag1", "lag2", "lag3", "roll_mean5", "roll_std10"]
    X = df[feature_cols].to_numpy()
    y = df["y"].to_numpy()

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    model = LinearRegression()
    model.fit(Xs, y)

    artifact = pickle.dumps({"scaler": scaler, "model": model, "feature_cols": feature_cols})

    with engine.begin() as conn:
        conn.execute(
            text("""
              INSERT INTO model_artifacts (symbol, freq, target, trained_at, artifact)
              VALUES ('BTCUSDT', '1h', 'abs_return', now(), :artifact)
            """),
            {"artifact": artifact},
        )

    print(f"trained rows={len(df)} saved artifact bytes={len(artifact)}")

if __name__ == "__main__":
    main()
