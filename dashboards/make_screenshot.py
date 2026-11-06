"""Genera dashboards/dashboard_screenshot.png con las MISMAS consultas SQL del dashboard (sql/kpis.sql)."""
import sqlite3, sys
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
db = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "storage" / "happiness.db"
c = sqlite3.connect(db)
f = pd.read_sql_query("""SELECT f.*, c.country_name, d.year FROM fact_predictions f
    JOIN dim_country c USING(country_id) JOIN dim_date d USING(date_id)""", c)
st = pd.read_sql_query("SELECT processing_status s, COUNT(*) n FROM raw_happiness_events GROUP BY 1", c)

fig = plt.figure(figsize=(16, 9)); fig.suptitle("World Happiness — streaming predictions dashboard", fontsize=17, fontweight="bold")
kp = [("Predictions", f"{len(f):,}"), ("MAE", f"{f.abs_prediction_error.mean():.3f}"),
      ("RMSE", f"{(f.prediction_error**2).mean()**.5:.3f}"), ("Bias (pred-actual)", f"{f.prediction_error.mean():+.3f}")]
for i, (k, v) in enumerate(kp):
    a = fig.add_axes([0.05 + i * 0.235, 0.80, 0.21, 0.10]); a.axis("off")
    a.text(.5, .62, v, ha="center", fontsize=26, fontweight="bold", color="#1f4e79"); a.text(.5, .12, "KPI 1 · " + k if i == 1 else k, ha="center", fontsize=11)
    a.add_patch(plt.Rectangle((0, 0), 1, 1, fill=False, ec="#bbb", transform=a.transAxes))
g = fig.add_gridspec(2, 3, left=.06, right=.97, bottom=.07, top=.74, hspace=.42, wspace=.3)
a = fig.add_subplot(g[0, 0]); t = f.groupby("country_name").size().sort_values(ascending=False).head(12)[::-1]
a.barh(t.index, t.values, color="#4c78a8"); a.set_title("KPI 2 · Predictions by country (top 12)")
a = fig.add_subplot(g[0, 1]); sc = a.scatter(f.actual_score, f.predicted_score, c=f.year, cmap="viridis", s=14, alpha=.75)
lim = [f.actual_score.min(), f.actual_score.max()]; a.plot(lim, lim, "r--"); a.set_title("KPI 3 · Predicted vs actual"); a.set_xlabel("actual"); a.set_ylabel("predicted"); plt.colorbar(sc, ax=a, label="year")
a = fig.add_subplot(g[0, 2]); y = f.groupby("year")[["actual_score", "predicted_score"]].mean()
a.plot(y.index, y.actual_score, "o-", label="actual"); a.plot(y.index, y.predicted_score, "s--", label="predicted"); a.legend(); a.set_title("KPI 4 · Trend by observation year"); a.set_xticks(y.index)
a = fig.add_subplot(g[1, :2]); m = f.assign(minute=f.event_time.str[11:16]).groupby("minute").abs_prediction_error.mean()
a.plot(m.index, m.values, "o-", color="#e45756"); a.set_title("KPI 4 · Streaming trend: MAE per event-minute"); a.tick_params(axis="x", rotation=60)
a = fig.add_subplot(g[1, 2]); a.bar(st.s, st.n, color=["#54a24b" if s == "VALID" else "#e45756" for s in st.s]); a.set_title("Pipeline quality (raw table status)"); a.tick_params(axis="x", rotation=25)
out = Path(__file__).parent / "dashboard_screenshot.png"; fig.savefig(out, dpi=110); print(out)
