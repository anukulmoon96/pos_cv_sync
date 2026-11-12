import pymysql
from kafka import KafkaProducer
import json
import logging
import os
import sys
import time
from datetime import datetime

from common import KAFKA_BROKER, TOPIC_NAME, setup_logging

log = logging.getLogger("producer")

# ---------------- MYSQL CONFIG (LOCAL DOCKER) ----------------
LOCAL_DB_CONFIG = {
    "host": "127.0.0.1",   # MySQL Docker is mapped on host
    "port": 3307,          # mapped port
    "user": "pos_user",
    "password": "strong_password",
    "database": "pos_db",
    "cursorclass": pymysql.cursors.DictCursor,
    # Raise instead of hanging forever on a dead connection.
    "connect_timeout": 10,
    "read_timeout": 300,
    "write_timeout": 300,
}

BATCH_SIZE = 5000
# Last pos_entries.id handed to Kafka, so a restart resumes where it stopped.
# Override once with `python producer.py <start_after_id>`.
CHECKPOINT_FILE = os.environ.get("POS_PRODUCER_CHECKPOINT", "producer_last_id.txt")
DEFAULT_START_ID = 28850042
MAX_BACKOFF = 60


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # converts datetime to ISO string
    raise TypeError(f"Type {type(obj)} not serializable")


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )


def fetch_new_records(last_id):
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET time_zone = 'Asia/Riyadh'")
            sql = "SELECT * FROM pos_entries WHERE id > %s ORDER BY id LIMIT %s"
            cursor.execute(sql, (last_id, BATCH_SIZE))
            rows = cursor.fetchall()
            return rows
    finally:
        conn.close()


def load_last_id():
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    try:
        with open(CHECKPOINT_FILE) as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return DEFAULT_START_ID


def save_last_id(last_id):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(last_id))
    os.replace(tmp, CHECKPOINT_FILE)


def send_batch(producer, rows):
    futures = []
    for row in rows:
        # Keyed by invoice so every copy of an invoice lands on the same
        # partition, in order.
        futures.append(producer.send(TOPIC_NAME, key=row.get("invoice_id"), value=row))
    producer.flush()
    for future in futures:
        future.get(timeout=60)  # raises if Kafka rejected a row


def stream_to_kafka():
    last_id = load_last_id()
    log.info("Starting after pos_entries.id %s", last_id)
    producer = None
    backoff = 1
    while True:
        try:
            if producer is None:
                producer = make_producer()
            rows = fetch_new_records(last_id)
            if rows:
                send_batch(producer, rows)
                last_id = rows[-1]["id"]
                save_last_id(last_id)
                log.info("Pushed %d rows to Kafka (last id %s)", len(rows), last_id)
        except Exception:
            # MySQL or Kafka down, or a row not acknowledged: the checkpoint
            # stays put and the same rows go again (the consumer drops repeats).
            log.exception("batch after id %s failed; retrying in %ds", last_id, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue
        backoff = 1
        if len(rows) < BATCH_SIZE:
            time.sleep(5)


if __name__ == "__main__":
    setup_logging()
    log.info("Starting producer (MySQL → Kafka)...")
    stream_to_kafka()
