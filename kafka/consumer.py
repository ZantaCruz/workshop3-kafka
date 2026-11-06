"""Consumidor: inferencia en tiempo real + carga en base de datos.

Grupo: happiness-ml-team · Topic: happiness-events
Pipeline por mensaje: raw -> validar contrato -> features (mismo orden del entrenamiento) -> model.pkl -> fact_predictions.

Los offsets se confirman DESPUÉS de persistir (at-least-once); un replay no duplica filas porque
raw_happiness_events es único por (topic, partition, offset).
"""
import argparse
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

from processing import Predictor, connect, process_message

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "happiness-events"
GROUP = "happiness-ml-team"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--group", default=GROUP)
    ap.add_argument("--db", type=Path, default=ROOT / "storage" / "happiness.db")
    ap.add_argument("--model", type=Path, default=ROOT / "models" / "model.pkl")
    ap.add_argument("--idle-exit", type=int, default=0,
                    help="si >0, termina tras N segundos sin mensajes (útil para demos/CI)")
    args = ap.parse_args()

    predictor = Predictor(args.model)
    conn = connect(args.db)
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,      # commit manual tras guardar en BD
    })
    consumer.subscribe([args.topic])
    counts, idle = {}, 0
    print(f"Consumidor '{args.group}' esperando eventos en '{args.topic}'... (modelo: {predictor.name}, BD: {args.db})")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                if args.idle_exit and idle >= args.idle_exit:
                    print("Sin mensajes nuevos: saliendo.")
                    break
                continue
            idle = 0
            if msg.error():
                print(f"Error de Kafka: {msg.error()}")
                continue
            key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            status = process_message(conn, predictor, msg.topic(), msg.partition(), msg.offset(), key, msg.value())
            consumer.commit(message=msg, asynchronous=False)
            counts[status] = counts.get(status, 0) + 1
            print(f"[p{msg.partition()} o{msg.offset():>5}] key={str(key):<26} -> {status:<16} | acumulado: {counts}")
    except KeyboardInterrupt:
        print("\nConsumidor detenido por el usuario.")
    except KafkaException as exc:
        print(f"Error fatal de Kafka: {exc}")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
