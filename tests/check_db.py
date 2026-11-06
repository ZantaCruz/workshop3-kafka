"""Resumen de evidencia desde la BD:  python tests/check_db.py [ruta_db]"""
import sqlite3, sys
from pathlib import Path
db = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "storage" / "happiness.db"
c = sqlite3.connect(db)
q = lambda s: c.execute(s).fetchall()
print("Eventos por estado:", q("SELECT processing_status, COUNT(*) FROM raw_happiness_events GROUP BY 1"))
print("Motivos de rechazo:")
for r in q("""SELECT processing_status,
        CASE WHEN status_detail LIKE 'duplicate event_id%' THEN 'duplicate event_id' ELSE status_detail END, COUNT(*)
        FROM raw_happiness_events WHERE processing_status NOT IN ('VALID','RECEIVED') GROUP BY 1,2 ORDER BY 3 DESC LIMIT 15"""):
    print("  ", r)
n_fact, mae = q("SELECT COUNT(*), ROUND(AVG(abs_prediction_error),3) FROM fact_predictions")[0]
distinct = q("SELECT COUNT(DISTINCT r.event_id) FROM fact_predictions f JOIN raw_happiness_events r USING(raw_event_id)")[0][0]
print(f"fact_predictions: {n_fact} filas | event_id distintos: {distinct} | MAE: {mae}")
print("Sin duplicados en el hecho:", n_fact == distinct)
print("Mensajes por partición:", q("SELECT kafka_partition, COUNT(*) FROM raw_happiness_events GROUP BY 1"))
