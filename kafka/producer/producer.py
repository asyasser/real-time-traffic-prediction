
import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer

DATA_PATH = "../../data/metr_la_TEST_raw.parquet"
SENSOR_IDS_PATH = "../../data/sensor_ids.json"
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "traffic-raw"


def main(interval_seconds: float):
    df = pd.read_parquet(DATA_PATH)

    with open(SENSOR_IDS_PATH) as f:
        sensor_ids = json.load(f)

    print(f"Loaded test set: {df.shape[0]} timestamps, {len(sensor_ids)} sensors")
    print(f"Replay interval: {interval_seconds}s per message")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for timestamp, row in df.iterrows():
        payload = {
            "timestamp": timestamp.isoformat(),
            "produced_at": time.time(),
            "speeds": {sid: float(row[sid]) for sid in sensor_ids},
        }

        producer.send(TOPIC, value=payload)
        print(f"Sent record for {timestamp}")

        time.sleep(interval_seconds)

    producer.flush()
    producer.close()
    print("Replay complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--interval", type=float, default=1.0,
        help="Seconds to wait between messages (default: 1.0)"
    )
    args = parser.parse_args()
    main(args.interval)
