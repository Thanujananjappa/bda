"""Stream simulated student events to Kafka (attendance, assignment, assessment)."""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATASET_PATH, KAFKA_BOOTSTRAP, KAFKA_TOPIC  # noqa: E402
from pipeline.event_schema import weekly_events_for_student  # noqa: E402


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = ";" if sample.count(";") > sample.count(",") else ","
        return list(csv.DictReader(f, delimiter=delimiter))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weeks", type=int, default=52, help="Simulate N weekly batches")
    parser.add_argument("--limit-students", type=int, default=0, help="Cap students (0=all)")
    parser.add_argument("--delay", type=float, default=0.02, help="Delay between messages")
    args = parser.parse_args()

    if not DATASET_PATH.exists():
        print(f"Missing dataset. Run: python scripts/download_dataset.py")
        sys.exit(1)

    rows = load_rows(DATASET_PATH)
    if args.limit_students:
        rows = rows[: args.limit_students]

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        print(f"Kafka not reachable at {KAFKA_BOOTSTRAP}. Start: docker compose up -d")
        sys.exit(1)

    rng = random.Random(42)
    sent = 0
    for week in range(1, args.weeks + 1):
        print(f"Week {week}/{args.weeks}...")
        for idx, row in enumerate(rows, start=1):
            events = weekly_events_for_student(row, idx, week, rng)
            for ev in events:
                producer.send(KAFKA_TOPIC, value=ev, key=ev["student_id"].encode())
                sent += 1
                if args.delay:
                    time.sleep(args.delay)
        producer.flush()
        print(f"  flushed week {week} ({sent} total messages)")

    producer.close()
    print(f"Done. Published {sent} events to topic '{KAFKA_TOPIC}'.")


if __name__ == "__main__":
    main()
