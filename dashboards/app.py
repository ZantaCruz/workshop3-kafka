"""Dashboard Streamlit — consulta SIEMPRE la base de datos (no los CSV).
    streamlit run dashboards/app.py                         # usa storage/happiness.db
    HAPPINESS_DB=storage/happiness_demo.db streamlit run dashboards/app.py
"""
import os
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DB = Path(os.environ.get("HAPPINESS_DB", ROOT / "storage" / "happiness.db"))

st.set_page_config(page_title="Happiness streaming predictions", layout="wide")
st.title("🌍 World Happiness — predicciones en streaming")
st.caption(f"Fuente: tabla `fact_predictions` en `{DB.name}` (SQLite). Refresca automáticamente al recargar.")

if not DB.exists():
    st.error(f"No existe {DB}. Ejecuta primero el consumidor (o tests/test_pipeline.py --keep-db ...).")
    st.stop()


@st.cache_data(ttl=5)
def q(sql: str) -> pd.DataFrame:
    with sqlite3.connect(DB) as c:
        return pd.read_sql_query(sql, c)


facts = q("""SELECT f.*, c.country_name, d.year FROM fact_predictions f
             JOIN dim_country c USING(country_id) JOIN dim_date d USING(date_id)""")
status = q("SELECT processing_status, COUNT(*) AS events FROM raw_happiness_events GROUP BY 1")
if facts.empty:
    st.warning("Aún no hay predicciones."); st.stop()

years = sorted(facts["year"].unique())
sel = st.sidebar.multiselect("Año de observación", years, default=years)
facts = facts[facts["year"].isin(sel)]

n = len(facts)
mae = facts["abs_prediction_error"].mean()
rmse = (facts["prediction_error"] ** 2).mean() ** 0.5
c1, c2, c3, c4 = st.columns(4)
c1.metric("Predicciones", f"{n:,}")
c2.metric("KPI 1 · Error absoluto medio (MAE)", f"{mae:.3f}")
c3.metric("RMSE", f"{rmse:.3f}")
c4.metric("Sesgo (pred − real)", f"{facts['prediction_error'].mean():+.3f}")

a, b = st.columns(2)
with a:
    st.subheader("KPI 2 · Predicciones por país (top 20)")
    byc = facts.groupby("country_name").size().sort_values(ascending=False).head(20).reset_index(name="predicciones")
    st.plotly_chart(px.bar(byc, x="predicciones", y="country_name", orientation="h").update_yaxes(autorange="reversed"),
                    width="stretch")
with b:
    st.subheader("KPI 3 · Predicho vs real")
    fig = px.scatter(facts, x="actual_score", y="predicted_score", color="year", hover_name="country_name", opacity=.7)
    lo, hi = facts[["actual_score", "predicted_score"]].min().min(), facts[["actual_score", "predicted_score"]].max().max()
    fig.add_shape(type="line", x0=lo, y0=lo, x1=hi, y1=hi, line=dict(dash="dash", color="red"))
    st.plotly_chart(fig, width="stretch")

st.subheader("KPI 4 · Tendencia de las predicciones en el tiempo")
t1, t2 = st.columns(2)
facts["minute"] = facts["event_time"].str[:16]
trend = facts.groupby("minute").agg(predicciones=("prediction_id", "count"), mae=("abs_prediction_error", "mean")).reset_index()
with t1:
    st.markdown("**En tiempo de streaming** (minuto de emisión del evento)")
    st.plotly_chart(px.line(trend, x="minute", y="mae", markers=True, labels={"mae": "MAE"}), width="stretch")
with t2:
    st.markdown("**Por año de observación** (score real vs predicho promedio)")
    ty = facts.groupby("year")[["actual_score", "predicted_score"]].mean().reset_index().melt("year")
    st.plotly_chart(px.line(ty, x="year", y="value", color="variable", markers=True), width="stretch")

st.subheader("Calidad del pipeline (tabla raw)")
q1, q2 = st.columns(2)
q1.dataframe(status, hide_index=True)
reasons = q("""SELECT processing_status,
       CASE WHEN status_detail LIKE 'duplicate event_id%' THEN 'duplicate event_id' ELSE status_detail END AS motivo,
       COUNT(*) AS events
       FROM raw_happiness_events WHERE processing_status NOT IN ('VALID','RECEIVED')
       GROUP BY 1, 2 ORDER BY events DESC LIMIT 15""")
q2.markdown("**Motivos de rechazo**"); q2.dataframe(reasons, hide_index=True)
