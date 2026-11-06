"""Lógica de negocio del consumidor, separada de Kafka para poder probarla sin broker.

Flujo de process_message():
  1. Guardar el mensaje ORIGINAL en raw_happiness_events (status RECEIVED).
  2. Validar contrato  -> INVALID_SCHEMA (estructura/tipos) | INVALID_VALUES (rangos, fecha, duplicado).
  3. Predecir con el pipeline entrenado -> PREDICTION_ERROR si falla.
  4. Cargar dim_country / dim_date / fact_predictions y marcar VALID.
Nunca lanza excepciones por datos malos: un evento inválido se registra y se omite.
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_SQL = ROOT / "sql" / "create_tables.sql"

# ---------------- Contrato del evento ----------------
REQUIRED_STRINGS = ("event_id", "country", "event_time")
FEATURE_FIELDS = ("gdp", "family", "health", "freedom", "generosity", "corruption")
NUMERIC_FIELDS = FEATURE_FIELDS + ("actual_happiness_score",)
REQUIRED_FIELDS = REQUIRED_STRINGS + ("year",) + NUMERIC_FIELDS
# Rangos plausibles (holgados respecto a lo observado 2015-2019: max gdp 2.1, family 1.6, health 1.1...)
RANGES = {
    "gdp": (0.0, 3.0), "family": (0.0, 2.5), "health": (0.0, 1.5), "freedom": (0.0, 1.0),
    "generosity": (0.0, 1.0), "corruption": (0.0, 1.0), "actual_happiness_score": (0.0, 10.0),
}
YEAR_RANGE = (2000, 2100)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def connect(db_path: Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))
    conn.commit()
    return conn


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_schema(event) -> str | None:
    """Estructura y tipos. Devuelve el motivo o None si es válido."""
    if not isinstance(event, dict):
        return "payload is not a JSON object"
    for f in REQUIRED_FIELDS:
        if f not in event:
            return f"missing field: {f}"
        if event[f] is None:
            return f"null field: {f}"
    for f in REQUIRED_STRINGS:
        if not isinstance(event[f], str):
            return f"{f} must be a string"
        if not event[f].strip():
            return f"empty field: {f}"
    if not isinstance(event["year"], int) or isinstance(event["year"], bool):
        return "year must be an integer"
    for f in NUMERIC_FIELDS:
        if not _is_number(event[f]):
            return f"{f} must be numeric (got {type(event[f]).__name__})"
    return None


def validate_values(event, conn: sqlite3.Connection) -> str | None:
    """Valores de negocio: finitos, en rango, fecha ISO y event_id no duplicado."""
    for f in NUMERIC_FIELDS:
        v = event[f]
        if not math.isfinite(v):
            return f"{f} is not finite"
        lo, hi = RANGES[f]
        if not lo <= v <= hi:
            return f"{f}={v} out of range [{lo}, {hi}]"
    if not YEAR_RANGE[0] <= event["year"] <= YEAR_RANGE[1]:
        return f"year={event['year']} out of range {YEAR_RANGE}"
    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        return "event_time is not a valid ISO-8601 timestamp"
    dup = conn.execute(
        "SELECT 1 FROM raw_happiness_events WHERE event_id = ? AND processing_status = 'VALID'",
        (event["event_id"],),
    ).fetchone()
    if dup:
        return f"duplicate event_id: {event['event_id']}"
    return None


class Predictor:
    """Envuelve el Pipeline serializado y garantiza el MISMO orden de columnas que en entrenamiento."""

    def __init__(self, model_path: Path):
        import joblib
        self.model = joblib.load(model_path)
        self.features = list(self.model.feature_names_in_)
        self.name = type(self.model[-1]).__name__ if hasattr(self.model, "__getitem__") else type(self.model).__name__

    def missing_features(self, event) -> list[str]:
        return [f for f in self.features if f not in event]

    def predict(self, event) -> float:
        row = pd.DataFrame([[event[f] for f in self.features]], columns=self.features)
        return float(self.model.predict(row)[0])


def _set_status(conn, raw_id, status, detail=None):
    conn.execute(
        "UPDATE raw_happiness_events SET processing_status = ?, status_detail = ? WHERE raw_event_id = ?",
        (status, detail, raw_id),
    )
    conn.commit()


def store_raw(conn, topic, partition, offset, key, value: bytes | None):
    """Guarda el mensaje original. Devuelve raw_event_id, o None si ese offset ya estaba guardado (replay)."""
    payload = (value or b"").decode("utf-8", errors="replace")
    event_id = None
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict) and isinstance(obj.get("event_id"), str):
            event_id = obj["event_id"]
    except ValueError:
        pass
    cur = conn.execute(
        """INSERT OR IGNORE INTO raw_happiness_events
           (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload, event_id, received_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (topic, partition, offset, key, payload, event_id, now_iso()),
    )
    conn.commit()
    return cur.lastrowid if cur.rowcount else None


def _get_or_create_country(conn, name: str) -> int:
    conn.execute("INSERT OR IGNORE INTO dim_country(country_name) VALUES (?)", (name,))
    return conn.execute("SELECT country_id FROM dim_country WHERE country_name = ?", (name,)).fetchone()[0]


def _get_or_create_date(conn, year: int) -> int:
    date_id = year * 10000 + 101
    conn.execute(
        "INSERT OR IGNORE INTO dim_date(date_id, full_date, year, decade) VALUES (?, ?, ?, ?)",
        (date_id, f"{year}-01-01", year, year // 10 * 10),
    )
    return date_id


def process_message(conn, predictor: Predictor, topic, partition, offset, key, value: bytes | None) -> str:
    """Procesa un mensaje y devuelve su estado final. Es idempotente por (topic, partition, offset)."""
    raw_id = store_raw(conn, topic, partition, offset, key, value)
    if raw_id is None:
        return "SKIPPED_REPLAY"

    # 1) Parseo + contrato de esquema
    try:
        event = json.loads((value or b"").decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        _set_status(conn, raw_id, "INVALID_SCHEMA", f"invalid JSON: {exc}")
        return "INVALID_SCHEMA"
    reason = validate_schema(event)
    if reason:
        _set_status(conn, raw_id, "INVALID_SCHEMA", reason)
        return "INVALID_SCHEMA"

    # 2) Features que exige el modelo (además del contrato)
    missing = predictor.missing_features(event)
    if missing:
        _set_status(conn, raw_id, "INVALID_SCHEMA", f"missing model features: {missing}")
        return "INVALID_SCHEMA"

    # 3) Valores
    reason = validate_values(event, conn)
    if reason:
        _set_status(conn, raw_id, "INVALID_VALUES", reason)
        return "INVALID_VALUES"

    # 4) Predicción (nunca debe tumbar el pipeline)
    try:
        predicted = predictor.predict(event)
        if not math.isfinite(predicted):
            raise ValueError("model returned a non-finite prediction")
    except Exception as exc:  # noqa: BLE001
        _set_status(conn, raw_id, "PREDICTION_ERROR", f"{type(exc).__name__}: {exc}")
        return "PREDICTION_ERROR"

    # 5) Carga atómica: dimensiones + hecho + estado VALID
    actual = float(event["actual_happiness_score"])
    try:
        with conn:
            country_id = _get_or_create_country(conn, event["country"].strip())
            date_id = _get_or_create_date(conn, event["year"])
            conn.execute(
                """INSERT INTO fact_predictions
                   (raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error,
                    abs_prediction_error, event_time, prediction_timestamp, model_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (raw_id, country_id, date_id, actual, predicted, predicted - actual, abs(predicted - actual),
                 event["event_time"], now_iso(), predictor.name),
            )
            conn.execute(
                "UPDATE raw_happiness_events SET processing_status='VALID', status_detail=NULL WHERE raw_event_id=?",
                (raw_id,),
            )
    except sqlite3.Error as exc:
        _set_status(conn, raw_id, "PREDICTION_ERROR", f"load failed: {exc}")
        return "PREDICTION_ERROR"
    return "VALID"
