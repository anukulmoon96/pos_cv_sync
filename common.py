"""Settings and helpers shared by producer.py, consumer.py and matcher.py.

Every setting below can be overridden with an environment variable of the
same name. GCP_DB_PASSWORD has no default on purpose: set it in the
environment (e.g. a systemd EnvironmentFile) instead of in the repo.
"""
import logging
import os
import re
import time as _time
from datetime import datetime, time, timedelta
from decimal import Decimal

import pymysql

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9093")
TOPIC_NAME = os.environ.get("POS_TOPIC", "pos_entries_sync")

GCP_DB_HOST = os.environ.get("GCP_DB_HOST", "xx.xx.xx.xx")
GCP_DB_PORT = int(os.environ.get("GCP_DB_PORT", "3306"))
GCP_DB_USER = os.environ.get("GCP_DB_USER", "")
GCP_DB_NAME = os.environ.get("GCP_DB_NAME", "")

POS_TABLE = "pos_transactions"
PUSH_OUTLETS_TABLE = "pos_push_outlets"
# Where the CV pipeline and consumer.py insert alerts. The other Kafka sync
# copies new rows from it into VA_OPT_TABLE but not later changes, so the
# consumer's and matcher's updates are written to both (VA_OPT_TABLE= turns
# that off).
VA_TABLE = os.environ.get("VA_TABLE", "vehicle_alerts")
VA_OPT_TABLE = os.environ.get("VA_OPT_TABLE", "vehicle_alerts_opt")
if not re.fullmatch(r"\w+", VA_TABLE) or not re.fullmatch(r"\w*", VA_OPT_TABLE):
    raise SystemExit(f"VA_TABLE / VA_OPT_TABLE must be plain table names, got {VA_TABLE!r} / {VA_OPT_TABLE!r}")
VA_TABLES = (VA_TABLE, VA_OPT_TABLE) if VA_OPT_TABLE not in ("", VA_TABLE) else (VA_TABLE,)
TAT_TYPE = "Turn Around Time"

OUTLET_REFRESH_SECONDS = 300

log = logging.getLogger("pos_sync")


def setup_logging():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def gcp_connect():
    password = os.environ.get("GCP_DB_PASSWORD")
    if not password:
        raise SystemExit("GCP_DB_PASSWORD is not set")
    conn = pymysql.connect(
        host=GCP_DB_HOST,
        port=GCP_DB_PORT,
        user=GCP_DB_USER,
        password=password,
        database=GCP_DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
        read_timeout=600,
        write_timeout=600,
    )
    with conn.cursor() as cur:
        # Locks only rows that exist, so the consumer and matcher never block
        # each other on gap locks.
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    return conn


def to_seconds(value):
    """Seconds since midnight from 'HH:MM:SS', ' H:MM:SS', 'H:MM:SS.ffffff',
    'YYYY-MM-DD HH:MM:SS', a MySQL TIME (timedelta) or a time. None if unusable."""
    if value is None:
        return None
    if isinstance(value, timedelta):
        seconds = int(value.total_seconds())
        return seconds if 0 <= seconds < 86400 else None
    if isinstance(value, (datetime, time)):
        return value.hour * 3600 + value.minute * 60 + value.second
    text = str(value).strip()
    if " " in text:
        text = text.split()[-1]
    parts = text.split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = int(parts[0]), int(parts[1]), int(float(parts[2]))
    except ValueError:
        return None
    if not (0 <= h < 24 and 0 <= m < 60 and 0 <= s < 60):
        return None
    return h * 3600 + m * 60 + s


def fmt_hms(seconds, pad_hour=True):
    """'HH:MM:SS' (or 'H:MM:SS' with pad_hour=False)."""
    h, rest = divmod(int(seconds), 3600)
    m, s = divmod(rest, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if pad_hour else f"{h}:{m:02d}:{s:02d}"


def at(day, seconds):
    """datetime for a date plus seconds since midnight."""
    return datetime.combine(day, time.min) + timedelta(seconds=seconds)


def money(value):
    return Decimal(str(value)).quantize(Decimal("0.01"))


def chunks(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class OutletMap:
    """outlets.store_number -> outlets.id, reloaded every few minutes."""

    def __init__(self):
        self._ids = {}
        self._loaded = 0.0

    def refresh(self, conn, force=False):
        if not force and _time.monotonic() - self._loaded < OUTLET_REFRESH_SECONDS:
            return
        ids = {}
        with conn.cursor() as cur:
            cur.execute("SELECT id, store_number FROM outlets WHERE store_number IS NOT NULL ORDER BY id")
            for row in cur.fetchall():
                code = str(row["store_number"]).strip().upper()
                if not code:
                    continue
                if code in ids:
                    log.warning("store_number %s is on outlets %s and %s; using %s",
                                code, ids[code], row["id"], ids[code])
                    continue
                ids[code] = row["id"]
        self._ids = ids
        self._loaded = _time.monotonic()

    def lookup(self, code):
        return self._ids.get(code)

    def resolve(self, conn, code):
        """lookup(), reloading once a minute at most when the code is unknown."""
        outlet = self._ids.get(code)
        if outlet is None and _time.monotonic() - self._loaded > 60:
            self.refresh(conn, force=True)
            outlet = self._ids.get(code)
        return outlet
