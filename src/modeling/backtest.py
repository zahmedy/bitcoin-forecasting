from src.modeling.dataset import get_hourly_df
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error


def train():
    df = get_hourly_df()

    if df is None or df.empty:
        print("No data available for backtest")
        return None
    
    y = df["y"]
    X = df.drop(columns=["y", "time", "symbol"])
    scaler = StandardScaler()
    linear = LinearRegression()

    rows_count = X.shape[0]

    split_index = int(0.7 * rows_count)

    baseline_errors = []
    linear_errors = []

    for t in range(split_index, rows_count):
        X_train = X[:t]
        y_train = y[:t]

        X_test = X[t:t+1]
        y_test = y.iloc[t:t+1]

        scaler.fit(X_train)
        X_train = scaler.transform(X_train)
        X_test = scaler.transform(X_test)

        linear.fit(X_train, y_train)
        y_hat = linear.predict(X_test)

        baseline_hat = [0.0]
        baseline_errors.append(mean_absolute_error(y_test, baseline_hat))
        linear_errors.append(mean_absolute_error(y_test, y_hat))

    baseline_mae = sum(baseline_errors) / len(baseline_errors)
    linear_mae = sum(linear_errors) / len(linear_errors)

    print(f"Baseline MAE (hourly): {baseline_mae}")
    print(f"Linear MAE (hourly): {linear_mae}")

if __name__ == "__main__":
    train()
