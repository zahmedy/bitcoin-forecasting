import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

DB_URL = os.environ["DATABASE_URL"]
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL, pool_pre_ping=True)

app = FastAPI()

PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>BTC Volatility Live</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }
    .card { padding: 16px; border: 1px solid #ddd; border-radius: 12px; max-width: 640px; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; }
    .k { color: #666; font-size: 12px; }
    .v { font-size: 22px; font-weight: 700; }
    .small { color: #666; font-size: 13px; }
    code { background: #f6f6f6; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <h2>BTC Live Volatility Forecast (next hour)</h2>
  <div class="card">
    <div class="row">
      <div>
        <div class="k">Latest close</div>
        <div class="v" id="price">—</div>
        <div class="small" id="price_time">—</div>
      </div>
      <div>
        <div class="k">Predicted |return|</div>
        <div class="v" id="yhat">—</div>
        <div class="small" id="pred_for">—</div>
      </div>
    </div>
    <p class="small">Auto-refresh every <code>30s</code>.</p>
    <p class="small" id="status"></p>
  </div>

<script>
async function refresh() {
  try {
    const r = await fetch("/v1/latest");
    const j = await r.json();

    document.getElementById("price").textContent = j.latest_close ?? "—";
    document.getElementById("price_time").textContent = j.latest_close_time ?? "—";

    document.getElementById("yhat").textContent = j.yhat ?? "—";
    document.getElementById("pred_for").textContent = j.predicted_for ?? "—";

    document.getElementById("status").textContent =
      "Updated: " + new Date().toISOString();
  } catch (e) {
    document.getElementById("status").textContent =
      "Error: " + (e?.message ?? String(e));
  }
}
refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(PAGE)

@app.get("/v1/latest")
def latest():
    # Latest candle close (hourly)
    q_candle = text("""
      SELECT open_time, close
      FROM candles
      WHERE symbol = 'BTCUSDT' AND interval = '1h'
      ORDER BY open_time DESC
      LIMIT 1
    """)

    # Latest prediction
    q_pred = text("""
      SELECT predicted_for, yhat
      FROM predictions
      WHERE symbol = 'BTCUSDT' AND freq = '1h' AND target = 'abs_return'
      ORDER BY predicted_for DESC
      LIMIT 1
    """)

    with engine.begin() as conn:
        c = conn.execute(q_candle).mappings().first()
        try:
            p = conn.execute(q_pred).mappings().first()
        except ProgrammingError:
            p = None

    return {
        "latest_close_time": (c["open_time"].isoformat() if c else None),
        "latest_close": (str(c["close"]) if c else None),
        "predicted_for": (p["predicted_for"].isoformat() if p else None),
        "yhat": (str(p["yhat"]) if p else None),
    }

def main():
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)

if __name__ == "__main__":
    main()
