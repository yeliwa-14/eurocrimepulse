#!/usr/bin/env python3
"""
EuroCrimePulse — Kafka Producer

Reads JSONL files from a generated batch directory and publishes each record
to the appropriate Kafka topic.

Architecture:
    streaming_generator.py  (generates batch files)
        |
        v
    kafka_producer.py  (THIS SCRIPT — sends to Kafka)
        |
        v
    Kafka topics:
        eurocrimepulse.police
        eurocrimepulse.court
        eurocrimepulse.corrections

Usage:
    pip install kafka-python          # one-time setup
    python3 kafka_producer.py  <batch_dir>  [--bootstrap localhost:9092]

The batch directory must have the structure produced by the generator:
    batch_<id>/
        police/   *.jsonl
        court/    *.jsonl
        corrections/ *.jsonl
"""

import argparse
import json
import signal
import sys
import time
from pathlib import Path

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    sys.exit(
        "ERROR: kafka-python is required.\n"
        "Install with:  pip install kafka-python"
    )


# ---------------------------------------------------------------------------
# Topic and key configuration
# ---------------------------------------------------------------------------
TOPIC_MAP = {
    "police": "eurocrimepulse.police",
    "court": "eurocrimepulse.court",
    "corrections": "eurocrimepulse.corrections",
}

# Business-key field used as the Kafka message key for each source.
KEY_FIELD_MAP = {
    "police": "crime_id",
    "court": "case_id",
    "corrections": "record_id",
}


# ---------------------------------------------------------------------------
# Producer logic
# ---------------------------------------------------------------------------
def produce_batch(batch_dir: str, bootstrap_servers: str = "localhost:9092"):
    """Read all JSONL files in *batch_dir* and publish to Kafka."""
    batch_path = Path(batch_dir)
    if not batch_path.exists():
        print(f"ERROR: batch directory not found: {batch_path}", file=sys.stderr)
        sys.exit(1)

    # ---- Connect to Kafka ----
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(
                v, ensure_ascii=False
            ).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            linger_ms=10,
            batch_size=16384,
        )
    except NoBrokersAvailable:
        print(
            f"ERROR: cannot connect to Kafka at {bootstrap_servers}. "
            "Is the broker running?",
            file=sys.stderr,
        )
        sys.exit(1)

    total_counts = {}
    error_count = 0

    for source_type in ("police", "court", "corrections"):
        topic = TOPIC_MAP[source_type]
        key_field = KEY_FIELD_MAP[source_type]
        source_dir = batch_path / source_type

        if not source_dir.exists():
            print(
                f"WARNING: no '{source_type}' directory in batch – skipping",
                file=sys.stderr,
            )
            continue

        count = 0
        for jsonl_file in sorted(source_dir.glob("*.jsonl")):
            with open(jsonl_file, "r", encoding="utf-8") as fh:
                for line_no, raw_line in enumerate(fh, 1):
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue

                    # --- Parse ---
                    try:
                        record = json.loads(raw_line)
                    except json.JSONDecodeError as exc:
                        print(
                            f"  WARN {jsonl_file.name}:{line_no}: "
                            f"invalid JSON – {exc}",
                            file=sys.stderr,
                        )
                        error_count += 1
                        continue

                    # --- Send ---
                    message_key = record.get(key_field)
                    try:
                        future = producer.send(
                            topic, value=record, key=message_key
                        )
                        # Block on each send so delivery errors are visible.
                        future.get(timeout=30)
                        count += 1
                    except KafkaError as exc:
                        print(
                            f"  ERROR {jsonl_file.name}:{line_no}: "
                            f"Kafka send failed – {exc}",
                            file=sys.stderr,
                        )
                        error_count += 1

        total_counts[source_type] = count

    # ---- Flush & close ----
    producer.flush(timeout=30)
    producer.close(timeout=10)

    # ---- Report ----
    print()
    print("=== Kafka produce summary ===")
    for source_type, cnt in total_counts.items():
        print(f"  {source_type:15s}  {cnt:>8d} records  →  {TOPIC_MAP[source_type]}")
    if error_count:
        print(f"\n  ⚠ {error_count} delivery/parse errors (see stderr)")
    else:
        print("\n  ✓ all records delivered successfully")

    return total_counts, error_count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

running = True
STATE_FILE = "producer_state.json"


def handle_sig(signum, frame):
    global running
    running = False


def load_state(outdir):
    path = Path(outdir) / STATE_FILE
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {"produced": []}
    return {"produced": []}


def save_state(outdir, state):
    path = Path(outdir) / STATE_FILE
    path.write_text(
        json.dumps(state, indent=2),
        encoding="utf-8",
    )


def publish_existing_batches(outdir, bootstrap):
    state = load_state(outdir)
    produced = set(state.get("produced", []))

    batches = sorted(
        p for p in Path(outdir).iterdir()
        if p.is_dir() and p.name.startswith("batch_")
    )

    for batch in batches:
        if batch.name in produced:
            continue

        print(f"[producer] publishing {batch}", flush=True)
        produce_batch(
            str(batch),
            bootstrap_servers=bootstrap,
        )

        produced.add(batch.name)
        save_state(outdir, {"produced": sorted(produced)})


def watch_and_produce(outdir, bootstrap, poll_interval):
    state = load_state(outdir)
    produced = set(state.get("produced", []))

    print(
        f"[producer] watching {outdir} "
        f"(poll_interval={poll_interval}s)",
        flush=True,
    )

    while running:
        batches = sorted(
            p for p in Path(outdir).iterdir()
            if p.is_dir() and p.name.startswith("batch_")
        )

        for batch in batches:
            if batch.name in produced:
                continue

            try:
                print(f"[producer] publishing {batch}", flush=True)
                produce_batch(
                    str(batch),
                    bootstrap_servers=bootstrap,
                )
                produced.add(batch.name)
                save_state(
                    outdir,
                    {"produced": sorted(produced)},
                )
            except Exception as exc:
                print(
                    f"[producer] ERROR publishing {batch}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        for _ in range(max(0, poll_interval)):
            if not running:
                break
            time.sleep(1)


def parse_args():
    p = argparse.ArgumentParser(
        description="EuroCrimePulse Kafka producer with continuous mode"
    )
    p.add_argument(
        "--batch-dir",
        help="Publish a single batch directory.",
    )
    p.add_argument(
        "--outdir",
        default="./landing",
        help="Landing directory containing batch_* directories.",
    )
    p.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap server(s).",
    )
    p.add_argument(
        "--continuous",
        action="store_true",
        help="Watch for and publish new batches continuously.",
    )
    p.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Polling interval in seconds.",
    )
    return p.parse_args()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    args = parse_args()
    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    if args.batch_dir:
        produce_batch(
            args.batch_dir,
            bootstrap_servers=args.bootstrap,
        )
    elif args.continuous:
        watch_and_produce(
            args.outdir,
            args.bootstrap,
            args.poll_interval,
        )
        print("Producer stopped", flush=True)
    else:
        publish_existing_batches(
            args.outdir,
            args.bootstrap,
        )
