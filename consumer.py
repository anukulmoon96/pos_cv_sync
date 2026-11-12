"""Kafka -> GCP MySQL sink for pos_db.pos_entries.

For each batch of messages, in one transaction:
  1. upsert pos_transactions, one row per (invoice_id, date). A re-sent copy of
     a payload changes nothing; a re-send with different items is merged in.
  2. for each NEW invoice from a store listed in pos_push_outlets, insert a
     'Turn Around Time' row into vehicle_alerts with pushed_through_pos = 1.
  3. when a merge changed an invoice that was pushed in step 2, refresh that
     row's items and amount, in vehicle_alerts and vehicle_alerts_opt (the
     other sync copies only new rows across).
Kafka offsets are committed only after the DB commit. A crash re-delivers the
batch, and step 1 turns the re-delivery into a no-op.
"""
import json
import logging
import os
import random
import time
from collections import Counter, OrderedDict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import pymysql
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from common import (
    KAFKA_BROKER, POS_TABLE, PUSH_OUTLETS_TABLE, TAT_TYPE, TOPIC_NAME, VA_TABLE, VA_TABLES,
    OutletMap, at, chunks, fmt_hms, gcp_connect, money, setup_logging, to_seconds,
)

log = logging.getLogger("consumer")

GROUP_ID = os.environ.get("POS_CONSUMER_GROUP", "pos-gcp-sink")
DEAD_LETTER_FILE = os.environ.get("POS_DEAD_LETTER_FILE", "dead_letter.jsonl")
TAT_MIN_SECONDS, TAT_MAX_SECONDS = 81, 181  # 0:01:21 .. 0:03:01
PUSH_REFRESH_SECONDS = 60

INSERT_POS = f"""
    INSERT INTO {POS_TABLE} (
        `date`, `time`, outlet_code, outlet, invoice_id, transaction_amount, items, item_count,
        pos_reg_id, order_source, aggregator_order_id, loyalty_id, is_loyalty_member,
        customer_name, customer_mobile, src_id, received_datetime, merge_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

UPDATE_POS = f"""
    UPDATE {POS_TABLE}
    SET items = %s, item_count = %s, transaction_amount = %s, merge_count = %s
    WHERE id = %s AND `date` = %s"""

INSERT_VA = f"""
    INSERT INTO {VA_TABLE} (
        created_at, outlet, type, tat, start_time, end_time, no_of_vehicles,
        invoice_id, items, amount, pos_id,
        pushed_through_pos, is_fake, deleted, is_manual, visibility
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

# Run once for each of VA_TABLES.
UPDATE_VA = """
    UPDATE {table}
    SET items = %s, amount = %s
    WHERE created_at = %s AND outlet = %s AND pos_id = %s AND pushed_through_pos = 1"""


def _text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _flag(value):
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return int(value.strip().lower() in ("1", "true", "yes"))
    return int(bool(value))


def normalize(raw):
    """Kafka message bytes -> one POS record. Raises on anything unusable."""
    row = json.loads(raw)
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    invoice_id = _text(payload.get("invoice_id"))
    if not invoice_id:
        raise ValueError("no invoice_id")
    seconds = to_seconds(payload.get("time"))
    if seconds is None:
        raise ValueError(f"bad time {payload.get('time')!r}")
    items = payload.get("items") or []
    if not isinstance(items, list):
        raise ValueError("items is not a list")

    received = payload.get("received_datetime") or row.get("received_datetime")
    received = (datetime.fromisoformat(str(received).replace("Z", "")).replace(tzinfo=None)
                if received else datetime.utcnow())

    return {
        "id": None,
        "invoice_id": invoice_id,
        "date": date.fromisoformat(str(payload["date"]).strip()),
        "seconds": seconds,
        "outlet_code": str(payload["outlet_id"]).strip().upper(),
        "outlet": None,
        "transaction_amount": money(payload.get("transaction_amount") or 0),
        "items": items,
        "pos_reg_id": _text(payload.get("pos_reg_id")),
        "order_source": _text(payload.get("order_source")),
        "aggregator_order_id": _text(payload.get("aggregator_order_id")),
        "loyalty_id": _text(payload.get("loyalty_id")),
        "is_loyalty_member": _flag(payload.get("is_loyalty_member")),
        "customer_name": _text(payload.get("customer_name")),
        "customer_mobile": _text(payload.get("customer_mobile")),
        "src_id": int(row["id"]),
        "received_datetime": received,
        "merge_count": 1,
    }


def _line_key(line):
    # 1 and 1.0 are the same quantity; sorted keys so field order doesn't matter.
    if isinstance(line, dict):
        line = {k: float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
                for k, v in line.items()}
    return json.dumps(line, sort_keys=True, default=str)


def merge_items(old, new):
    """Multiset union: a line both payloads carry is kept once (or as many times
    as the payload that repeats it most), lines only `new` has are appended."""
    merged = list(old)
    have = Counter(_line_key(line) for line in old)
    seen = Counter()
    for line in new:
        key = _line_key(line)
        seen[key] += 1
        if seen[key] > have[key]:
            merged.append(line)
    return merged


def _lines_total(items, fallback):
    try:
        return money(sum(Decimal(str(line["line_amount_gross"])) for line in items))
    except (KeyError, TypeError, InvalidOperation):
        return fallback


def merge_into(target, rec):
    """Fold rec's items into target. False when rec adds nothing (a duplicate)."""
    merged = merge_items(target["items"], rec["items"])
    if len(merged) == len(target["items"]):
        return False
    target["items"] = merged
    # The payloads' own totals each cover only their half, so the merged
    # invoice is priced from its lines.
    target["transaction_amount"] = _lines_total(
        merged, max(target["transaction_amount"], rec["transaction_amount"]))
    target["merge_count"] += 1
    return True


def dead_letter(message, exc):
    log.error("dropping %s[%s]@%s: %s", message.topic, message.partition, message.offset, exc)
    with open(DEAD_LETTER_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "topic": message.topic, "partition": message.partition, "offset": message.offset,
            "error": repr(exc), "value": message.value.decode("utf-8", "replace"),
        }) + "\n")


class PushList:
    """pos_push_outlets: stores whose invoices also become vehicle_alerts rows."""

    def __init__(self):
        self._enabled_from = {}
        self._loaded = 0.0

    def refresh(self, conn):
        if time.monotonic() - self._loaded < PUSH_REFRESH_SECONDS:
            return
        with conn.cursor() as cur:
            cur.execute(f"SELECT store_number, enabled_from FROM {PUSH_OUTLETS_TABLE} WHERE enabled = 1")
            self._enabled_from = {str(r["store_number"]).strip().upper(): r["enabled_from"]
                                  for r in cur.fetchall()}
        self._loaded = time.monotonic()

    def wants(self, rec):
        start = self._enabled_from.get(rec["outlet_code"])
        if start is None or at(rec["date"], rec["seconds"]) < start:
            return False
        if rec["outlet"] is None:
            log.warning("%s is in %s but not in outlets.store_number; invoice %s not pushed",
                        rec["outlet_code"], PUSH_OUTLETS_TABLE, rec["invoice_id"])
            return False
        return True


def va_row(rec):
    tat = random.randint(TAT_MIN_SECONDS, TAT_MAX_SECONDS)
    start = rec["seconds"]
    return (
        at(rec["date"], start),
        str(rec["outlet"]),
        TAT_TYPE,
        fmt_hms(tat, pad_hour=False),
        fmt_hms(start),
        fmt_hms((start + tat) % 86400),
        "None",
        rec["invoice_id"],
        json.dumps(rec["items"]),
        str(rec["transaction_amount"]),
        str(rec["id"]),
        1, 0, 0, 0, 0,  # pushed_through_pos, is_fake, deleted, is_manual, visibility
    )


class Sink:
    def __init__(self):
        self.conn = None
        self.outlets = OutletMap()
        self.push_list = PushList()

    def connect(self):
        if self.conn is None:
            self.conn = gcp_connect()
            self.outlets.refresh(self.conn, force=True)
        self.outlets.refresh(self.conn)
        self.push_list.refresh(self.conn)

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def write(self, messages):
        records = OrderedDict()
        for message in messages:
            try:
                rec = normalize(message.value)
            except Exception as exc:
                dead_letter(message, exc)
                continue
            key = (rec["invoice_id"], rec["date"])
            if key in records:
                merge_into(records[key], rec)
            else:
                records[key] = rec
        if not records:
            return 0, 0

        self.conn.begin()
        try:
            with self.conn.cursor() as cur:
                inserted, updated = self._upsert_pos(cur, records)
                self._push(cur, inserted, updated)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return len(inserted), len(updated)

    def write_each(self, messages):
        """Fallback when a batch hits a data error: write one message at a time
        and dead-letter the ones MySQL rejects."""
        for message in messages:
            try:
                self.write([message])
            except pymysql.err.DataError as exc:
                dead_letter(message, exc)

    def _select(self, cur, columns, keys, lock=False):
        found = {}
        for part in chunks(keys, 500):
            marks = ", ".join(["(%s, %s)"] * len(part))
            cur.execute(
                f"SELECT {columns} FROM {POS_TABLE} WHERE (invoice_id, `date`) IN ({marks})"
                + (" FOR UPDATE" if lock else ""),
                [v for key in part for v in key])
            for row in cur.fetchall():
                found[(row["invoice_id"], row["date"])] = row
        return found

    def _upsert_pos(self, cur, records):
        existing = self._select(
            cur, "id, invoice_id, `date`, `time`, outlet_code, outlet, items, transaction_amount, merge_count",
            list(records), lock=True)

        inserted, updated = [], []
        for key, rec in records.items():
            row = existing.get(key)
            if row is None:
                rec["outlet"] = self.outlets.resolve(self.conn, rec["outlet_code"])
                inserted.append(rec)
                continue
            current = {
                "id": row["id"], "invoice_id": row["invoice_id"], "date": row["date"],
                "seconds": to_seconds(row["time"]), "outlet_code": row["outlet_code"],
                "outlet": row["outlet"], "items": json.loads(row["items"]) if row["items"] else [],
                "transaction_amount": row["transaction_amount"], "merge_count": row["merge_count"],
            }
            if merge_into(current, rec):
                updated.append(current)

        if inserted:
            cur.executemany(INSERT_POS, [(
                r["date"], fmt_hms(r["seconds"]), r["outlet_code"], r["outlet"], r["invoice_id"],
                r["transaction_amount"], json.dumps(r["items"]), len(r["items"]),
                r["pos_reg_id"], r["order_source"], r["aggregator_order_id"], r["loyalty_id"],
                r["is_loyalty_member"], r["customer_name"], r["customer_mobile"], r["src_id"],
                r["received_datetime"], r["merge_count"],
            ) for r in inserted])
            ids = self._select(cur, "id, invoice_id, `date`", [(r["invoice_id"], r["date"]) for r in inserted])
            for r in inserted:
                r["id"] = ids[(r["invoice_id"], r["date"])]["id"]

        if updated:
            cur.executemany(UPDATE_POS, [(
                json.dumps(r["items"]), len(r["items"]), r["transaction_amount"], r["merge_count"],
                r["id"], r["date"],
            ) for r in updated])
        return inserted, updated

    def _push(self, cur, inserted, updated):
        new_alerts = [va_row(r) for r in inserted if self.push_list.wants(r)]
        if new_alerts:
            cur.executemany(INSERT_VA, new_alerts)
        refreshed = [(json.dumps(r["items"]), str(r["transaction_amount"]),
                      at(r["date"], r["seconds"]), str(r["outlet"]), str(r["id"]))
                     for r in updated if self.push_list.wants(r)]
        if refreshed:
            for table in VA_TABLES:
                cur.executemany(UPDATE_VA.format(table=table), refreshed)
        if new_alerts or refreshed:
            log.info("%s: %d alerts inserted, %d refreshed", VA_TABLE, len(new_alerts), len(refreshed))


def open_consumer():
    """KafkaConsumer on the topic, waiting for the broker if it isn't up yet (e.g. at boot)."""
    backoff = 1
    while True:
        try:
            return KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=500,
            )
        except KafkaError as exc:
            log.warning("Kafka at %s unreachable (%s); retrying in %ds", KAFKA_BROKER, exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def rewind(consumer, batch):
    """Seek back so the same messages come back on the next poll."""
    for tp, part in batch.items():
        try:
            consumer.seek(tp, part[0].offset)
        except Exception:
            # Revoked by a rebalance meanwhile; its new owner starts from the
            # last committed offset anyway.
            log.warning("could not rewind %s; no longer assigned here", tp)


def main():
    setup_logging()
    consumer = open_consumer()
    sink = Sink()
    log.info("consuming %s as %s -> %s / %s", TOPIC_NAME, GROUP_ID, POS_TABLE, VA_TABLE)

    backoff = 1
    while True:
        batch = consumer.poll(timeout_ms=1000)
        messages = [m for part in batch.values() for m in part]
        try:
            sink.connect()
            if messages:
                try:
                    inserted, updated = sink.write(messages)
                    log.info("%d messages: %d invoices new, %d merged", len(messages), inserted, updated)
                except pymysql.err.DataError:
                    log.exception("batch rejected; retrying message by message")
                    sink.write_each(messages)
        except Exception:
            log.exception("batch of %d failed; retrying in %ds", len(messages), backoff)
            sink.close()
            rewind(consumer, batch)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        backoff = 1
        if messages:
            try:
                consumer.commit()
            except KafkaError as exc:
                # Usually a rebalance mid-batch. The rows are written; whoever
                # owns the partitions now reads them again and the dedup makes
                # that a no-op.
                log.warning("offset commit failed (%s); these messages will be read again", exc)


if __name__ == "__main__":
    main()
