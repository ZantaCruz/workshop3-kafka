-- KPI 1. Error de predicción promedio
SELECT ROUND(AVG(abs_prediction_error), 4) AS mae,
       ROUND(AVG(prediction_error), 4)     AS bias_mean_signed_error,
       ROUND(SQRT(AVG(prediction_error * prediction_error)), 4) AS rmse,
       COUNT(*) AS n_predictions
FROM fact_predictions;

-- KPI 2. Predicciones por país (y error medio)
SELECT c.country_name, COUNT(*) AS predictions,
       ROUND(AVG(f.predicted_score), 3) AS avg_predicted,
       ROUND(AVG(f.abs_prediction_error), 3) AS mae
FROM fact_predictions f JOIN dim_country c USING (country_id)
GROUP BY c.country_name ORDER BY predictions DESC, c.country_name;

-- KPI 3. Predicho vs real (detalle para scatter)
SELECT c.country_name, d.year, f.actual_score, f.predicted_score, f.prediction_error
FROM fact_predictions f
JOIN dim_country c USING (country_id) JOIN dim_date d USING (date_id);

-- KPI 4a. Tendencia por año de observación
SELECT d.year, COUNT(*) AS n, ROUND(AVG(f.actual_score), 3) AS avg_actual,
       ROUND(AVG(f.predicted_score), 3) AS avg_predicted, ROUND(AVG(f.abs_prediction_error), 3) AS mae
FROM fact_predictions f JOIN dim_date d USING (date_id)
GROUP BY d.year ORDER BY d.year;

-- KPI 4b. Tendencia en tiempo de streaming (por minuto de emisión)
SELECT SUBSTR(event_time, 1, 16) AS minute, COUNT(*) AS predictions,
       ROUND(AVG(abs_prediction_error), 3) AS mae
FROM fact_predictions GROUP BY minute ORDER BY minute;

-- Calidad del pipeline: eventos por estado en la tabla raw
SELECT processing_status, COUNT(*) AS events FROM raw_happiness_events GROUP BY processing_status;

-- Detalle de rechazos
SELECT raw_event_id, event_id, processing_status, status_detail FROM raw_happiness_events
WHERE processing_status NOT IN ('VALID','RECEIVED') ORDER BY raw_event_id;

-- Trazabilidad: predicción -> evento original
SELECT f.prediction_id, r.raw_event_id, r.kafka_partition, r.kafka_offset, r.raw_payload, f.predicted_score
FROM fact_predictions f JOIN raw_happiness_events r USING (raw_event_id) LIMIT 10;
