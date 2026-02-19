import os
import pickle
import pandas as pd
from sqlalchemy import create_engine, text
from arch import arch_model

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
    df = df.dropna(subset=["r"]).reset_index(drop=True)
    return df


def main(hours: int = 50) -> None:
    df = load_returns_1h()
    if df.empty:
        print("No returns available for backfill")
        return

    am = arch_model(df["r"], mean="Zero", vol="GARCH", p=1, q=1, dist="normal")
    res = am.fit(disp="off")
    f = res.forecast(horizon=1, reindex=False)
    var = f.variance.iloc[:, 0]
    sigma = var.pow(0.5)
    df = df.copy()
    df["yhat"] = sigma.values
    df = df.tail(hours)

    rows = []
    for _, row in df.iterrows():
        predicted_for = row["time"] + pd.Timedelta(hours=1)
        rows.append(
            {
                "pred_for": predicted_for,
                "yhat": float(row["yhat"]),
            }
        )

    q_ins = text("""
      INSERT INTO predictions (symbol, freq, target, predicted_for, yhat)
      VALUES ('BTCUSDT', '1h', 'abs_return', :pred_for, :yhat)
      ON CONFLICT (symbol, freq, target, predicted_for) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(q_ins, rows)

    artifact = pickle.dumps({"model_type": "garch", "model": res})
    with engine.begin() as conn:
        conn.execute(
            text("""
              INSERT INTO model_artifacts (symbol, freq, target, trained_at, artifact)
              VALUES ('BTCUSDT', '1h', 'abs_return', now(), :artifact)
            """),
            {"artifact": artifact},
        )

    print(f"backfilled {len(rows)} predictions")


if __name__ == "__main__":
    main()
