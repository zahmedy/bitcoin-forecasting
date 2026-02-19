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
        <div class="k">Expected move</div>
        <div class="v" id="yhat">—</div>
        <div class="small" id="pred_for">—</div>
      </div>
    </div>
    <p class="small">Auto-refresh every <code>30s</code>.</p>
    <p class="small" id="status"></p>
  </div>
  <div style="height:16px"></div>
<div class="card">
  <div class="k">Last 48h: Actual |return| vs Predicted |return|</div>
  <canvas id="chart" width="600" height="240" style="width:100%; max-width:640px;"></canvas>
  <p class="small">Actual = solid line. Predicted = dashed line.</p>
</div>


<script>
function drawLine(ctx, points, dashed=false, color="#111") {
  if (points.length < 2) return;
  ctx.save();
  ctx.strokeStyle = color;
  if (dashed) ctx.setLineDash([6, 4]);
  ctx.beginPath();
  ctx.moveTo(points[0].x, points[0].y);
  for (let i = 1; i < points.length; i++) ctx.lineTo(points[i].x, points[i].y);
  ctx.stroke();
  ctx.restore();
}

function scalePoints(series, w, h, pad, yMax, tMin, tMax) {
  const n = series.length;
  if (n === 0) return [];
  const span = Math.max(1, tMax - tMin);
  return series.map((p, i) => {
    const t = new Date(p.t).getTime();
    const x = pad + ((t - tMin) / span) * (w - 2*pad);
    const y = pad + (1 - (p.v / yMax)) * (h - 2*pad);
    return {x, y};
  });
}

async function refreshChart() {
  const hours = 48;
  const [aRes, pRes] = await Promise.all([
    fetch(`/v1/series/abs_returns?hours=${hours}`),
    fetch(`/v1/series/predictions?hours=${hours}`)
  ]);
  const actual = await aRes.json();
  const pred = await pRes.json();

  const canvas = document.getElementById("chart");
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height, pad = 18;

  // clear
  ctx.clearRect(0,0,w,h);

  // choose a shared y scale
  const maxA = actual.reduce((m,p)=>Math.max(m,p.v), 0);
  const maxP = pred.reduce((m,p)=>Math.max(m,p.v), 0);
  const yMax = Math.max(1e-9, maxA, maxP) * 1.1;

  // time range (extend 1h beyond latest actual)
  const tActual = actual.map(p => new Date(p.t).getTime());
  const tPred = pred.map(p => new Date(p.t).getTime());
  const tMin = Math.min(...tActual, ...tPred);
  const lastActual = tActual.length ? Math.max(...tActual) : Date.now();
  const tMax = Math.max(lastActual + 3600 * 1000, ...tPred, lastActual);

  // axes
  ctx.beginPath();
  ctx.moveTo(pad, pad);
  ctx.lineTo(pad, h - pad);
  ctx.lineTo(w - pad, h - pad);
  ctx.stroke();

  // lines
  const aPts = scalePoints(actual, w, h, pad, yMax, tMin, tMax);
  const pPts = scalePoints(pred, w, h, pad, yMax, tMin, tMax);
  drawLine(ctx, aPts, false, "#1f4fd6");
  drawLine(ctx, pPts, true, "#e0672f");

  // label yMax
  ctx.fillStyle = "#111";
  ctx.fillText(`yMax≈${yMax.toFixed(4)}`, pad + 6, pad + 10);
}

async function refresh() {
  try {
    const r = await fetch("/v1/latest");
    const j = await r.json();

    const latestClose = j.latest_close != null ? Number(j.latest_close) : null;
    document.getElementById("price").textContent = latestClose != null
      ? `$${latestClose.toFixed(2)}`
      : "—";
    document.getElementById("price_time").textContent = j.latest_close_time ?? "—";
    const yhat = j.yhat != null ? Number(j.yhat) : null;
    const price = j.latest_close != null ? Number(j.latest_close) : null;
    const move = (yhat != null && price != null) ? price * yhat : null;
    document.getElementById("yhat").textContent = move != null
      ? `~$${move.toFixed(0)} next hour`
      : "—";
    document.getElementById("pred_for").textContent = j.predicted_for ?? "—";

    await refreshChart();

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

@app.get("/v1/series/abs_returns")
def series_abs_returns(hours: int = 48):
    q = text("""
      SELECT time, ABS(r) AS abs_r
      FROM returns_1h
      WHERE symbol='BTCUSDT'
        AND time >= now() - (:hours || ' hours')::interval
      ORDER BY time
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"hours": hours}).mappings().all()
    return [{"t": r["time"].isoformat(), "v": float(r["abs_r"])} for r in rows]

@app.get("/v1/series/predictions")
def series_predictions(hours: int = 48):
    q = text("""
      SELECT predicted_for, yhat
      FROM predictions
      WHERE symbol='BTCUSDT' AND freq='1h' AND target='abs_return'
        AND predicted_for >= now() - (:hours || ' hours')::interval
      ORDER BY predicted_for
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"hours": hours}).mappings().all()
    return [{"t": r["predicted_for"].isoformat(), "v": float(r["yhat"])} for r in rows]

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
