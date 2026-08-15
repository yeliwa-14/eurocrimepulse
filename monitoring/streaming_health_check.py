#!/usr/bin/env python3

import os
import subprocess
import sys
import time
import json


def run(cmd, timeout=10):
    try:
        res = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except Exception as e:
        return 2, "", str(e)


def kafka_topics(brokers):
    cmd = os.environ.get(
        "KAFKA_TOPICS_CMD",
        f"/usr/local/kafka/bin/kafka-topics.sh "
        f"--bootstrap-server {brokers} --list",
    )
    rc, out, err = run(cmd)
    return {
        "rc": rc,
        "topics": out.splitlines() if out else [],
        "err": err,
    }


def hdfs_file_count(path):
    rc, out, err = run(
        f"hdfs dfs -count -q {path}"
    )

    if rc != 0:
        return None, err

    try:
        parts = out.split()
        # hdfs -count -q returns:
        # QUOTA REMAINING_QUOTA SPACE_QUOTA REMAINING_SPACE FILE_COUNT CONTENT_SIZE PATH
        return int(parts[4]), ""
    except Exception:
        return None, out


def hdfs_gold_counts(path):
    counts = {}

    for layer in ("police", "court", "corrections"):
        count, err = hdfs_file_count(
            f"{path}/{layer}"
        )
        counts[layer] = {
            "files": count,
            "err": err,
        }

    return counts


def main():
    brokers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    gold_path = os.getenv(
        "EUROCRIMEPULSE_GOLD_BASE",
        "hdfs://localhost:9000/eurocrimepulse/gold",
    )

    wait_seconds = int(
        os.getenv(
            "STREAMING_HEALTH_WAIT_SECONDS",
            "30",
        )
    )

    print("=== EuroCrimePulse Streaming Health Check ===")

    # --------------------------------------------------------
    # 1. Kafka
    # --------------------------------------------------------
    kafka = kafka_topics(brokers)

    print(
        json.dumps(
            kafka,
            indent=2,
        )
    )

    if kafka["rc"] != 0:
        print("ERROR: Kafka is unavailable.")
        sys.exit(2)

    required_topics = {
        "eurocrimepulse.police",
        "eurocrimepulse.court",
        "eurocrimepulse.corrections",
    }

    if not required_topics.issubset(
        set(kafka["topics"])
    ):
        print("ERROR: Required Kafka topics are missing.")
        sys.exit(2)

    # --------------------------------------------------------
    # 2. Capture initial HDFS counts
    # --------------------------------------------------------
    before = hdfs_gold_counts(gold_path)

    print("Gold BEFORE:")
    print(json.dumps(before, indent=2))

    # --------------------------------------------------------
    # 3. Wait for streaming output to change
    # --------------------------------------------------------
    print(
        f"Waiting {wait_seconds}s for streaming output..."
    )

    time.sleep(wait_seconds)

    after = hdfs_gold_counts(gold_path)

    print("Gold AFTER:")
    print(json.dumps(after, indent=2))

    # --------------------------------------------------------
    # 4. Verify actual growth
    # --------------------------------------------------------
    growth = {}

    for layer in (
        "police",
        "court",
        "corrections",
    ):
        b = before[layer]["files"]
        a = after[layer]["files"]

        if b is None or a is None:
            growth[layer] = None
        else:
            growth[layer] = a - b

    print("Growth:")
    print(json.dumps(growth, indent=2))

    if not any(
        isinstance(v, int) and v > 0
        for v in growth.values()
    ):
        print(
            "ERROR: Streaming services are reachable, "
            "but no new Gold files were produced."
        )
        sys.exit(2)

    print("OK: Streaming is actively producing data.")
    sys.exit(0)


if __name__ == "__main__":
    main()
