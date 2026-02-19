import os
import statistics
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
    @import url("https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap");
  </style>
  <style>
    :root {
      --bg: #f6f7fb;
      --card: #ffffff;
      --ink: #0d1015;
      --muted: #5f6b7a;
      --accent: #1f4fd6;
      --accent-2: #e0672f;
      --border: #e3e8f0;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 28px;
      font-family: "Space Grotesk", system-ui, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 600px at 10% -10%, #dfe8ff 0%, transparent 60%),
        radial-gradient(800px 500px at 100% 0%, #ffe9d8 0%, transparent 55%),
        var(--bg);
    }
    .wrap { max-width: 980px; margin: 0 auto; }
    h1 { font-size: 32px; margin: 0 0 6px 0; }
    .sub { color: var(--muted); margin: 0 0 16px 0; }
    .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-bottom: 14px; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: 0 1px 0 rgba(15, 23, 42, 0.02);
    }
    .k { color: var(--muted); font-size: 12px; letter-spacing: 0.04em; text-transform: uppercase; }
    .v { font-size: 24px; font-weight: 700; margin-top: 6px; }
    .mono { font-family: "IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, monospace; }
    .small { color: var(--muted); font-size: 12px; margin-top: 6px; }
    .badge {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 6px 10px; border-radius: 999px;
      background: #eef2ff; color: #253b80; font-size: 12px; font-weight: 600;
      border: 1px solid #d6deff;
    }
    .badge.high { background: #ffe8e1; border-color: #ffd1c1; color: #7b2c14; }
    .badge.low { background: #e7f7ef; border-color: #c7efd9; color: #1e5a3a; }
    .row { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
    .chart-card { padding: 16px 18px 10px; }
    canvas { width: 100%; height: 220px; border: 1px solid var(--border); border-radius: 12px; }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="row" style="margin-bottom: 8px;">
      <div>
        <h1>BTC Volatility Forecast</h1>
        <p class="sub">Next‑hour expected move and recent regime context.</p>
      </div>
      <div class="badge" id="regime_badge">Vol regime: —</div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="k">Latest close</div>
        <div class="v mono" id="price">—</div>
        <div class="small mono" id="price_time">—</div>
      </div>
      <div class="card">
        <div class="k">Expected move (1σ)</div>
        <div class="v" id="move">—</div>
        <div class="small" id="move_pct">—</div>
      </div>
      <div class="card">
        <div class="k">Next‑hour range</div>
        <div class="v mono" id="range_68">—</div>
        <div class="small mono" id="range_95">—</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="k">Last hour |return|</div>
        <div class="v" id="last_abs_move">—</div>
        <div class="small" id="last_abs_return">—</div>
      </div>
      <div class="card">
        <div class="k">Realized vol (24h)</div>
        <div class="v" id="rv24">—</div>
        <div class="small" id="rv24_pct">—</div>
      </div>
      <div class="card">
        <div class="k">Realized vol (7d)</div>
        <div class="v" id="rv7d">—</div>
        <div class="small" id="rv7d_pct">—</div>
      </div>
    </div>

    <div class="card chart-card">
      <div class="k">Last 48h: Actual |return| vs Predicted |return|</div>
      <canvas id="chart" width="900" height="220"></canvas>
      <p class="small">Actual = solid line. Predicted = dashed line.</p>
      <p class="small" id="status"></p>
    </div>
  </div>


<script>
const fmtUsd = (v, d=0) =>
  v == null ? "—" : `$${Number(v).toLocaleString("en-US", {minimumFractionDigits:d, maximumFractionDigits:d})}`;
const fmtPct = (v, d=2) =>
  v == null ? "—" : `${(Number(v) * 100).toFixed(d)}%`;

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

    document.getElementById("price").textContent = fmtUsd(j.latest_close, 2);
    document.getElementById("price_time").textContent = j.latest_close_time ?? "—";

    document.getElementById("move").textContent = j.expected_move != null
      ? `${fmtUsd(j.expected_move, 0)} next hour`
      : "—";
    document.getElementById("move_pct").textContent = j.expected_move_pct != null
      ? `${fmtPct(j.expected_move_pct / 100, 2)} | for ${j.predicted_for ?? "—"}`
      : "—";

    document.getElementById("range_68").textContent =
      (j.range_68_low != null && j.range_68_high != null)
        ? `${fmtUsd(j.range_68_low, 0)} – ${fmtUsd(j.range_68_high, 0)}`
        : "—";
    document.getElementById("range_95").textContent =
      (j.range_95_low != null && j.range_95_high != null)
        ? `95%: ${fmtUsd(j.range_95_low, 0)} – ${fmtUsd(j.range_95_high, 0)}`
        : "—";

    document.getElementById("last_abs_move").textContent = j.last_abs_move != null
      ? fmtUsd(j.last_abs_move, 0)
      : "—";
    document.getElementById("last_abs_return").textContent = j.last_abs_return != null
      ? `|r| = ${fmtPct(j.last_abs_return, 3)}`
      : "—";

    document.getElementById("rv24").textContent = j.rv24_move != null
      ? fmtUsd(j.rv24_move, 0)
      : "—";
    document.getElementById("rv24_pct").textContent = j.rv24_std != null
      ? `σ ≈ ${fmtPct(j.rv24_std, 3)} (24h)`
      : "—";

    document.getElementById("rv7d").textContent = j.rv7d_move != null
      ? fmtUsd(j.rv7d_move, 0)
      : "—";
    document.getElementById("rv7d_pct").textContent = j.rv7d_std != null
      ? `σ ≈ ${fmtPct(j.rv7d_std, 3)} (7d)`
      : "—";

    const badge = document.getElementById("regime_badge");
    if (j.vol_regime) {
      badge.textContent = `Vol regime: ${j.vol_regime}` + (j.vol_percentile != null ? ` (${Math.round(j.vol_percentile * 100)}th pct)` : "");
      badge.classList.remove("high", "low");
      if (j.vol_regime === "High") badge.classList.add("high");
      if (j.vol_regime === "Low") badge.classList.add("low");
    } else {
      badge.textContent = "Vol regime: —";
      badge.classList.remove("high", "low");
    }

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

    q_last_r = text("""
      SELECT time, r
      FROM returns_1h
      WHERE symbol = 'BTCUSDT'
      ORDER BY time DESC
      LIMIT 1
    """)
    q_recent_r = text("""
      SELECT r
      FROM returns_1h
      WHERE symbol = 'BTCUSDT'
        AND time >= now() - (:hours || ' hours')::interval
      ORDER BY time
    """)
    q_pred_hist = text("""
      SELECT yhat
      FROM predictions
      WHERE symbol = 'BTCUSDT' AND freq = '1h' AND target = 'abs_return'
        AND predicted_for >= now() - (:hours || ' hours')::interval
      ORDER BY predicted_for
    """)

    with engine.begin() as conn:
        c = conn.execute(q_candle).mappings().first()
        r_last = conn.execute(q_last_r).mappings().first()
        r24 = conn.execute(q_recent_r, {"hours": 24}).mappings().all()
        r7d = conn.execute(q_recent_r, {"hours": 168}).mappings().all()
        try:
            p = conn.execute(q_pred).mappings().first()
            p_hist = conn.execute(q_pred_hist, {"hours": 168}).mappings().all()
        except ProgrammingError:
            p = None
            p_hist = []

    def _to_float(x):
        return float(x) if x is not None else None

    def _stdev(values):
        if len(values) < 2:
            return None
        return statistics.pstdev(values)

    price = _to_float(c["close"]) if c else None
    yhat = _to_float(p["yhat"]) if p else None

    expected_move = price * yhat if price is not None and yhat is not None else None
    range_68_low = price - expected_move if expected_move is not None else None
    range_68_high = price + expected_move if expected_move is not None else None
    range_95_low = price - (1.96 * expected_move) if expected_move is not None else None
    range_95_high = price + (1.96 * expected_move) if expected_move is not None else None

    r_last_val = _to_float(r_last["r"]) if r_last else None
    last_abs_return = abs(r_last_val) if r_last_val is not None else None
    last_abs_move = price * last_abs_return if price is not None and last_abs_return is not None else None

    r24_vals = [_to_float(r["r"]) for r in r24 if r["r"] is not None]
    r7d_vals = [_to_float(r["r"]) for r in r7d if r["r"] is not None]
    rv24_std = _stdev(r24_vals)
    rv7d_std = _stdev(r7d_vals)
    rv24_move = price * rv24_std if price is not None and rv24_std is not None else None
    rv7d_move = price * rv7d_std if price is not None and rv7d_std is not None else None

    yhat_hist = [_to_float(row["yhat"]) for row in p_hist if row["yhat"] is not None]
    vol_percentile = None
    if yhat is not None and yhat_hist:
        count = sum(1 for v in yhat_hist if v <= yhat)
        vol_percentile = count / len(yhat_hist)
    if vol_percentile is None:
        vol_regime = None
    elif vol_percentile >= 0.7:
        vol_regime = "High"
    elif vol_percentile <= 0.3:
        vol_regime = "Low"
    else:
        vol_regime = "Normal"

    return {
        "latest_close_time": (c["open_time"].isoformat() if c else None),
        "latest_close": price,
        "predicted_for": (p["predicted_for"].isoformat() if p else None),
        "yhat": yhat,
        "expected_move": expected_move,
        "expected_move_pct": (yhat * 100 if yhat is not None else None),
        "range_68_low": range_68_low,
        "range_68_high": range_68_high,
        "range_95_low": range_95_low,
        "range_95_high": range_95_high,
        "last_abs_return": last_abs_return,
        "last_abs_move": last_abs_move,
        "rv24_std": rv24_std,
        "rv24_move": rv24_move,
        "rv7d_std": rv7d_std,
        "rv7d_move": rv7d_move,
        "vol_percentile": vol_percentile,
        "vol_regime": vol_regime,
    }

def main():
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)

if __name__ == "__main__":
    main()
