from src.db.db import get_engine
from sqlalchemy import text
import pandas as pd

def get_hourly_df():
    
    try:
        engine = get_engine()

        returns_query = """
            SELECT symbol, time, close, r
            FROM returns_1h
            ORDER BY time;
        """

        with engine.begin() as conn:
            result = conn.execute(text(returns_query))

            data = result.fetchall()

            if not data:
                return None

            df = pd.DataFrame(data, columns=result.keys())
            df["abs_r"] = df["r"].abs()
            df["y"] = df["abs_r"].shift(-1)
            df["lag0"] = df["abs_r"].shift(0)
            df["lag1"] = df["abs_r"].shift(1)
            df["lag2"] = df["abs_r"].shift(2)
            df["lag3"] = df["abs_r"].shift(3)

            return df.dropna(subset=["y", 
                                     "lag0", 
                                     "lag1", 
                                     "lag2", 
                                     "lag3"])
    except Exception:
        raise

if __name__ == "__main__":
    df = get_hourly_df()
