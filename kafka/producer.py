"""Productor: lee el dataset histórico preparado fila por fila y lo publica como eventos en Kafka.

Uso:
    python kafka/producer.py                       # todo el dataset, 0.5 s entre eventos
    python kafka/producer.py --subset test --delay 0.2 --limit 50
    python kafka/producer.py --inject-invalid      # añade eventos defectuosos para demostrar la validación
"""
import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "streaming" / "happiness_stream.csv"
TOPIC = "happiness-events"


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3, replication: int = 1) -> None:
    """Crea el topic (3 particiones, RF=1) si no existe. Reproducible sin comandos manuales."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
    for _, fut in futures.items():
        try:
            fut.result()
            print(f"Topic '{topic}' creado ({partitions} particiones, RF={replication}).")
        except Exception as exc:  # carrera con otro proceso: no es fatal
            print(f"Aviso creando topic: {exc}")


def build_event(row, n: int) -> dict:
    """Formato EXACTO pedido por el taller. event_time = momento en que el productor emite el evento."""
    return {
        "event_id": f"E{n:04d}",
        "country": row["country"],
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now().isoformat(timespec="seconds"),
    }


def invalid_samples(start: int) -> list[tuple[str, str]]:
    """(key, valor crudo) con defectos intencionales, uno por cada estado de rechazo."""
    t = datetime.now().isoformat(timespec="seconds")
    ok = dict(country="Testland", year=2019, gdp=1.0, family=1.0, health=0.8, freedom=0.4,
              generosity=0.2, corruption=0.1, actual_happiness_score=5.5, event_time=t)
    bad = [
        ("Testland", json.dumps({**{k: v for k, v in ok.items() if k != "gdp"}, "event_id": f"E{start:04d}"})),         # falta campo
        ("Testland", json.dumps({**ok, "event_id": f"E{start+1:04d}", "family": "alto"})),                              # tipo inválido
        ("Testland", json.dumps({**ok, "event_id": f"E{start+2:04d}", "gdp": -4.2})),                                   # fuera de rango
        ("Testland", json.dumps({**ok, "event_id": f"E{start+3:04d}", "event_time": "ayer por la tarde"})),             # fecha inválida
        ("Testland", json.dumps({**ok, "event_id": f"E{start+4:04d}", "country": ""})),                                 # país vacío
        ("Testland", "esto {no es json"),                                                                                # JSON roto
    ]
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    ap.add_argument("--delay", type=float, default=0.5, help="segundos entre eventos (configurable)")
    ap.add_argument("--subset", choices=["all", "test", "train"], default="all")
    ap.add_argument("--limit", type=int, default=None, help="máximo de eventos a enviar")
    ap.add_argument("--inject-invalid", action="store_true", help="intercala eventos defectuosos de demostración")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    if args.subset != "all":
        df = df[df["split"] == args.subset]
    if args.limit:
        df = df.head(args.limit)

    ensure_topic(args.bootstrap, args.topic)
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})
    delivered = {"ok": 0, "err": 0}

    def on_delivery(err, msg):
        if err is not None:
            delivered["err"] += 1
            print(f"  ✗ entrega fallida: {err}")
        else:
            delivered["ok"] += 1

    print(f"Productor iniciado: {len(df)} eventos -> topic '{args.topic}' (key = country, delay = {args.delay}s)")
    n = 0
    extra = invalid_samples(start=9000) if args.inject_invalid else []
    inject_every = max(1, len(df) // (len(extra) + 1)) if extra else None
    try:
        for _, row in df.iterrows():
            n += 1
            event = build_event(row, n)
            producer.produce(args.topic, key=event["country"], value=json.dumps(event), on_delivery=on_delivery)
            producer.poll(0)
            print(f"Sent {event['event_id']} | key={event['country']:<28} year={event['year']} score={event['actual_happiness_score']}")
            if extra and n % inject_every == 0:
                key, bad = extra.pop(0)
                producer.produce(args.topic, key=key, value=bad, on_delivery=on_delivery)
                print(f"Sent INVALID (demo) | {bad[:70]}")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    finally:
        producer.flush()
        print(f"Productor terminado. Entregados={delivered['ok']} errores={delivered['err']}")


if __name__ == "__main__":
    main()
