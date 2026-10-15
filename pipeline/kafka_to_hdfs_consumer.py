"""Consume Kafka student-events and persist JSON lines to HDFS (or local mirror)."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import HDFS_RAW_PATH, KAFKA_BOOTSTRAP, KAFKA_TOPIC  # noqa: E402
from pipeline.hdfs_client import HdfsClient  # noqa: E402

_running = True


def _stop(*_: object) -> None:
    global _running
    _running = False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages (0 = run until Ctrl+C)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    hdfs = HdfsClient()
    hdfs.mkdirs(HDFS_RAW_PATH)
    storage = "HDFS" if not hdfs._use_local else "local mirror"
    print(f"Writing to {storage} under {HDFS_RAW_PATH}")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="student-events-hdfs-writer",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
    except NoBrokersAvailable:
        print(f"Kafka not reachable at {KAFKA_BOOTSTRAP}")
        sys.exit(1)

    count = 0
    print(f"Consuming topic '{KAFKA_TOPIC}'...")
    for msg in consumer:
        if not _running:
            break
        record = msg.value
        path = hdfs.append_json_line(HDFS_RAW_PATH, record)
        count += 1
        if count % 500 == 0:
            print(f"  stored {count} events (last: {path})")
        if args.max_messages and count >= args.max_messages:
            break

    consumer.close()
    print(f"Stopped. Wrote {count} events.")


if __name__ == "__main__":
    main()
