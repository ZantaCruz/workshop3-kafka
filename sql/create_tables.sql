-- Workshop 3 · Esquema analítico (SQLite). Idempotente: se puede ejecutar varias veces.
PRAGMA foreign_keys = ON;

-- RAW / staging: el mensaje Kafka tal como llegó + estado de procesamiento (no es una dimensión)
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    kafka_topic       TEXT    NOT NULL,
    kafka_partition   INTEGER NOT NULL,
    kafka_offset      INTEGER NOT NULL,
    kafka_key         TEXT,
    raw_payload       TEXT    NOT NULL,          -- valor original, sin modificar
    event_id          TEXT,                      -- extraído solo si el JSON lo permite
    received_at       TEXT    NOT NULL,          -- ISO-8601 cuando el consumidor lo recibió
    processing_status TEXT    NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN ('RECEIVED','VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR')),
    status_detail     TEXT,                      -- motivo del rechazo / error
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)   -- un mensaje Kafka se guarda una sola vez (reprocesos)
);
CREATE INDEX IF NOT EXISTS ix_raw_status   ON raw_happiness_events(processing_status);
CREATE INDEX IF NOT EXISTS ix_raw_event_id ON raw_happiness_events(event_id);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL UNIQUE
);

-- Dimensión de fecha a nivel de año de observación (date_id = AAAA0101)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INTEGER PRIMARY KEY,
    full_date TEXT    NOT NULL,
    year      INTEGER NOT NULL UNIQUE,
    decade    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_event_id         INTEGER NOT NULL UNIQUE REFERENCES raw_happiness_events(raw_event_id),
    country_id           INTEGER NOT NULL REFERENCES dim_country(country_id),
    date_id              INTEGER NOT NULL REFERENCES dim_date(date_id),
    actual_score         REAL    NOT NULL,
    predicted_score      REAL    NOT NULL,
    prediction_error     REAL    NOT NULL,       -- predicho - real (con signo)
    abs_prediction_error REAL    NOT NULL,
    event_time           TEXT    NOT NULL,       -- cuándo el productor emitió el evento
    prediction_timestamp TEXT    NOT NULL,       -- cuándo el consumidor generó la predicción
    model_name           TEXT
);
CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS ix_fact_date    ON fact_predictions(date_id);
