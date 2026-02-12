from db.db import get_engine
from sqlalchemy import text
import pandas as pd


def get_hourly_df():
    
    try:
        engine = get_engine()

        returns_query = """
            SELECT * FROM returns_1d
        """

        with engine.begin() as conn:
            result = conn.execute(text(returns_query))

            data = result.fetchall()

            if data:
                df = pd.DataFrame(data, columns=result.keys())
                df["y"] = df["r"].shift(-1)
                df["lag0"] = df["r"].shift(0)
                df["lag1"] = df["r"].shift(1)
                df["lag2"] = df["r"].shift(2)
                df["lag3"] = df["r"].shift(3)

                return df
            
            else:
                print(f"\nNo data found in the database")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    get_hourly_df()