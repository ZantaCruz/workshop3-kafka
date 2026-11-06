"""Prueba del consumidor SIN broker: reproduce mensajes (partition/offset) y verifica estados y BD.
Ejecutar:  python tests/test_pipeline.py [--keep-db storage/happiness_demo.db]
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "kafka"))
from processing import Predictor, connect, process_message  # noqa: E402
from producer import build_event, invalid_samples  # noqa: E402  (no abre conexión al importar)


def run(db_path, n_valid=None):
    if str(db_path) != ":memory:" and Path(db_path).exists():
        Path(db_path).unlink()
    conn, pred = connect(db_path), Predictor(ROOT / "models" / "model.pkl")
    df = pd.read_csv(ROOT / "data" / "streaming" / "happiness_stream.csv")
    if n_valid:
        df = df.head(n_valid)
    offset, stats = {0: 0, 1: 0, 2: 0}, {}

    def send(key, value, p=None):
        p = p if p is not None else hash(key or "") % 3
        st = process_message(conn, pred, "happiness-events", p, offset[p], key, value.encode() if isinstance(value, str) else value)
        offset[p] += 1
        stats[st] = stats.get(st, 0) + 1
        return st

    base = pd.Timestamp("2026-10-30 15:30:00")
    last_event = None
    for i, (_, row) in enumerate(df.iterrows(), 1):
        ev = build_event(row, i)
        ev["event_time"] = (base + pd.Timedelta(seconds=i)).isoformat()
        last_event = ev
        send(ev["country"], json.dumps(ev))
    for k, bad in invalid_samples(9000):
        send(k, bad)
    # extras: evento duplicado, NaN, bool como número, campo extra de fuga (debe ignorarse), valor None
    send(last_event["country"], json.dumps(last_event))                                    # duplicado event_id
    send("X", '{"event_id":"E9100","country":"X","year":2019,"gdp":NaN,"family":1,"health":1,"freedom":0.5,"generosity":0.1,"corruption":0.1,"actual_happiness_score":5,"event_time":"2026-10-30T15:30:00"}')
    send("X", json.dumps({**last_event, "event_id": "E9101", "freedom": True}))
    send("X", json.dumps({**last_event, "event_id": "E9102", "corruption": None}))
    send("X", json.dumps({**last_event, "event_id": "E9103", "dystopia_residual": 2.1}))   # campo extra: válido
    send("X", "[1,2,3]")
    class Boom:
        features = pred.features; name = "Boom"
        missing_features = staticmethod(lambda e: [])
        def predict(self, e): raise RuntimeError("modelo roto")
    stats["PREDICTION_ERROR"] = stats.get("PREDICTION_ERROR", 0) + (process_message(conn, Boom(), "happiness-events", 1, 99999, "X", json.dumps({**last_event, "event_id": "E9200"}).encode()) == "PREDICTION_ERROR")
    # replay del mismo offset: no debe duplicar
    st_replay = process_message(conn, pred, "happiness-events", 0, 0, "x", b"{}")
    return conn, stats, st_replay


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--keep-db"); a = ap.parse_args()
    conn, stats, replay = run(a.keep_db or ":memory:")
    print("Estados:", stats, "| replay ->", replay)
    n_valid = len(pd.read_csv(ROOT / "data" / "streaming" / "happiness_stream.csv"))
    assert stats["VALID"] == n_valid + 1, stats            # todo el dataset + el evento con campo extra
    assert stats["INVALID_SCHEMA"] >= 6 and stats["INVALID_VALUES"] >= 4, stats
    assert replay == "SKIPPED_REPLAY" and stats["PREDICTION_ERROR"] == 1
    nf = conn.execute("SELECT COUNT(*) FROM fact_predictions").fetchone()[0]
    nv = conn.execute("SELECT COUNT(*) FROM raw_happiness_events WHERE processing_status='VALID'").fetchone()[0]
    assert nf == nv == stats["VALID"], (nf, nv)
    total_raw = conn.execute("SELECT COUNT(*) FROM raw_happiness_events").fetchone()[0]
    assert total_raw == sum(v for k, v in stats.items()), "todo mensaje debe quedar en raw"
    print("Motivos de rechazo:")
    for r in conn.execute("SELECT processing_status, status_detail FROM raw_happiness_events WHERE processing_status NOT IN ('VALID','RECEIVED')"):
        print("  ", r)
    print("OK: todos los asserts pasaron.")
