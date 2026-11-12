"""Attributes POS invoices (pos_transactions) to computer-vision alerts (vehicle_alerts).

An invoice can only go to an alert at the same outlet whose start_time -> end_time
window contains the invoice's date + time. When several windows contain it (two
cars at the counter at once) it goes to the one whose start_time is closest to
the invoice time, so every invoice is counted on at most one alert.

Windows are compared as full timestamps: a visit that crosses midnight
(23:59:30 -> 00:01:10) picks up invoices dated the next day.

Each alert then carries all the invoices it won: invoice_id and pos_id
comma-separated in time order, amount summed, items as
{"<invoice_id>": "{\"items\": [...]}", ...}, the encoding the dashboards already
unwrap. An alert that wins nothing is cleared if this matcher wrote it before
(pos_id set); invoice data from anywhere else is left alone, unless a --date run
is given --replace: then that data is cleared too when pos_transactions has
every invoice in it, since the run has put each of them on another alert or on
none.

Alerts are read from vehicle_alerts and every change is written to
vehicle_alerts_opt too: the other Kafka sync copies only new rows across, and
sync_opt() repairs rows it copied after they had changed.

Only 'Turn Around Time' alerts with pushed_through_pos = 0 and is_fake = 0 are
touched. Results are recomputed from scratch and unchanged rows are skipped, so
re-running any range is safe.

Usage:
  python matcher.py --date 2026-10-21                         # one day, all outlets
  python matcher.py --date 2026-10-21 --end-date 2026-10-25 --outlets 12,40
  python matcher.py --date 2026-10-21 --replace --dry-run     # count what --replace would clear
  python matcher.py                                           # follow changes in real time
  python matcher.py --start-from-now                          # next start skips the first-run re-check
"""
import argparse
import json
import logging
import os
import time as _time
from bisect import bisect_right
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from common import (
    POS_TABLE, TAT_TYPE, VA_OPT_TABLE, VA_TABLE, VA_TABLES,
    OutletMap, at, chunks, gcp_connect, money, setup_logging, to_seconds,
)

log = logging.getLogger("matcher")

STATE_FILE = os.environ.get("MATCHER_STATE_FILE", "matcher_state.json")
POLL_SECONDS = int(os.environ.get("MATCHER_POLL_SECONDS", "15"))
MAX_BACKOFF = 120
# created_at and POS date/time are Saudi wall-clock time (UTC+3, no DST); the
# GCP server's own clock (NOW(), updated_at) is UTC.
STORE_UTC_OFFSET = "+03:00"
# Furthest an alert's created_at can be from any moment of its start_time ->
# end_time window (queue wait + time at the counter). Alerts are looked up by
# created_at, so a visit longer than this can be missed.
MAX_ALERT_SPAN = timedelta(minutes=int(os.environ.get("MATCHER_MAX_ALERT_SPAN_MIN", "60")))
# Safety net for anything the change feed missed: re-match this much recent
# history for all outlets every SWEEP_EVERY seconds.
SWEEP_EVERY = int(os.environ.get("MATCHER_SWEEP_EVERY_MIN", "30")) * 60
SWEEP_LOOKBACK = timedelta(hours=int(os.environ.get("MATCHER_SWEEP_LOOKBACK_HOURS", "2")))
FIRST_RUN_LOOKBACK = timedelta(hours=24)
# --start-from-now: covers the last cycles of a matcher stopped just before
# (cycles are at most MAX_BACKOFF apart).
RESTART_LOOKBACK = timedelta(minutes=15)
NEW_ALERT_LOOKBACK = timedelta(days=2)
RECENT = timedelta(minutes=30)
OUTLET_FIX_EVERY = 300
OUTLET_CHUNK = 25
WATERMARK_OVERLAP = timedelta(minutes=2)
ID_OVERLAP = 500
# Newest vehicle_alerts ids checked against VA_OPT_TABLE every cycle: many
# minutes of inserts, against a sync that lags a few seconds.
RECOPY_IDS = 2000
HALF_DAY = timedelta(hours=12)
ONE_SECOND = timedelta(seconds=1)

# NULL flags count as 0, as in alert-cleanup-route.js.
ELIGIBLE = f"""type = '{TAT_TYPE}'
          AND COALESCE(pushed_through_pos, 0) = 0
          AND COALESCE(is_fake, 0) = 0"""

TMP_TABLE = "tmp_alert_match"


def connect():
    conn = gcp_connect()
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {TMP_TABLE} (
            id BIGINT NOT NULL,
            created_at DATETIME(6) NOT NULL,
            invoice_id TEXT,
            amount VARCHAR(64),
            items LONGTEXT,
            pos_id TEXT,
            PRIMARY KEY (id, created_at))""")
    return conn


def _outlet_id(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _marks(values):
    return ", ".join(["%s"] * len(values))


def _as_datetime(value):
    """vehicle_alerts.created_at is a VARCHAR holding what the cameras send
    ('YYYY-MM-DD HH:MM:SS'), so it arrives as text; vehicle_alerts_opt's is a
    DATETIME. None if it can't be read."""
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).strip()).replace(tzinfo=None)
    except ValueError:
        return None


def alert_window(alert):
    """(outlet, start, end) as datetimes, or None if the alert can't be placed.

    start_time goes on whichever date puts it nearest created_at (a car first
    seen at 23:59:50 that reaches the counter at 00:00:20 belongs to the next
    day), and an end_time earlier than start_time means the visit crossed
    midnight."""
    outlet = _outlet_id(alert["outlet"])
    start = to_seconds(alert["start_time"])
    end = to_seconds(alert["end_time"])
    created = _as_datetime(alert["created_at"])
    if outlet is None or start is None or end is None or created is None:
        return None
    window_start = at(created.date(), start)
    if window_start - created > HALF_DAY:
        window_start -= timedelta(days=1)
    elif created - window_start > HALF_DAY:
        window_start += timedelta(days=1)
    return outlet, window_start, window_start + timedelta(seconds=(end - start) % 86400)


def _distance(invoice_time, alert):
    """How close an invoice is to an alert whose window contains it: seconds
    from the car's arrival at the counter (start_time)."""
    return abs((invoice_time - alert["start"]).total_seconds())


def alert_outlets(cur, ts_from, ts_to):
    cur.execute(f"""SELECT DISTINCT outlet FROM {VA_TABLE}
        WHERE created_at >= %s AND created_at < %s AND {ELIGIBLE}""", (ts_from, ts_to))
    return sorted({o for o in (_outlet_id(r["outlet"]) for r in cur.fetchall()) if o is not None})


def load_alerts(cur, outlets, ts_from, ts_to):
    """Eligible alerts at `outlets` with created_at in [ts_from, ts_to), placed on the timeline."""
    if ts_from >= ts_to:
        return []
    # outlet is a varchar here: compare as strings so its index stays usable.
    codes = [str(o) for o in outlets]
    cur.execute(f"""SELECT id, created_at, outlet, start_time, end_time
        FROM {VA_TABLE}
        WHERE created_at >= %s AND created_at < %s
          AND outlet IN ({_marks(codes)})
          AND {ELIGIBLE}""", (ts_from, ts_to, *codes))
    alerts = []
    for row in cur.fetchall():
        window = alert_window(row)
        if window:
            row["outlet_id"], row["start"], row["end"] = window
            row["key"] = (row["id"], row["created_at"])
            alerts.append(row)
    return alerts


def load_stored(cur, alerts):
    """Current invoice fields of `alerts` (sorted by created_at), by (id, created_at)."""
    stored = {}
    for part in chunks(alerts, 1000):
        ids = [a["id"] for a in part]
        cur.execute(f"""SELECT id, created_at, invoice_id, amount, items, pos_id
            FROM {VA_TABLE}
            WHERE created_at >= %s AND created_at <= %s
              AND id IN ({_marks(ids)})
              AND {ELIGIBLE}""", (part[0]["created_at"], part[-1]["created_at"], *ids))
        for row in cur.fetchall():
            stored[(row["id"], row["created_at"])] = row
    return stored


def load_pos(cur, outlets, ts_from, ts_to):
    """Invoices at `outlets` timed in [ts_from, ts_to], in time order."""
    cur.execute(f"""SELECT id, outlet, `date`, `time`, invoice_id, transaction_amount, items
        FROM {POS_TABLE}
        WHERE outlet IN ({_marks(outlets)})
          AND `date` BETWEEN %s AND %s
          AND TIMESTAMP(`date`, `time`) BETWEEN %s AND %s
        ORDER BY `date`, `time`, id""",
                (*outlets, ts_from.date(), ts_to.date(), ts_from, ts_to))
    rows = cur.fetchall()
    for row in rows:
        row["ts"] = at(row["date"], to_seconds(row["time"]))
    return rows


def assign(alerts, invoices):
    """{alert key: [invoices]}: each invoice goes to the closest alert whose
    window contains it (ties: lower alert id), or to none."""
    by_outlet = {}
    for alert in sorted(alerts, key=lambda a: a["start"]):
        by_outlet.setdefault(alert["outlet_id"], []).append(alert)
    index = {outlet: ([a["start"] for a in group], max(a["end"] - a["start"] for a in group))
             for outlet, group in by_outlet.items()}

    won = {}
    for invoice in invoices:
        group = by_outlet.get(invoice["outlet"])
        if not group:
            continue
        starts, longest = index[invoice["outlet"]]
        best = None
        # Walk back from the last alert that started by the invoice time; one
        # that started longer ago than the longest visit can't still be open.
        i = bisect_right(starts, invoice["ts"])
        while i > 0:
            i -= 1
            alert = group[i]
            if alert["start"] < invoice["ts"] - longest:
                break
            if alert["end"] >= invoice["ts"]:
                rank = (_distance(invoice["ts"], alert), alert["id"])
                if best is None or rank < best[0]:
                    best = (rank, alert)
        if best:
            won.setdefault(best[1]["key"], []).append(invoice)
    return won


def build_match(invoices):
    """invoice_id, amount, items, pos_id for an alert's invoices; all None for none."""
    if not invoices:
        return None, None, None, None
    return (
        ",".join(r["invoice_id"] for r in invoices),
        str(money(sum(r["transaction_amount"] for r in invoices))),
        {r["invoice_id"]: json.dumps({"items": json.loads(r["items"]) if r["items"] else []})
         for r in invoices},
        ",".join(str(r["id"]) for r in invoices),
    )


def is_current(stored, invoice_ids, amount, items, pos_ids):
    if (stored["invoice_id"] or "") != invoice_ids or (stored["pos_id"] or "") != pos_ids:
        return False
    try:
        # Parsed, so a JSON column's key reordering doesn't count as a change.
        return Decimal(str(stored["amount"])) == Decimal(amount) and json.loads(stored["items"]) == items
    except (TypeError, ValueError, InvalidOperation):
        return False


def write_updates(conn, updates):
    """Stage rows in a temp table and apply each chunk with one UPDATE ... JOIN
    per table in VA_TABLES instead of a round trip per alert."""
    with conn.cursor() as cur:
        for part in chunks(updates, 500):
            cur.execute(f"DELETE FROM {TMP_TABLE}")
            cur.executemany(f"INSERT INTO {TMP_TABLE} VALUES (%s, %s, %s, %s, %s, %s)", part)
            for table in VA_TABLES:
                cur.execute(f"""UPDATE {table} va
                    JOIN {TMP_TABLE} m ON va.id = m.id AND va.created_at = m.created_at
                    SET va.invoice_id = m.invoice_id, va.amount = m.amount,
                        va.items = m.items, va.pos_id = m.pos_id
                    WHERE COALESCE(va.pushed_through_pos, 0) = 0 AND COALESCE(va.is_fake, 0) = 0""")


def sync_opt(conn, ts_from=None, ts_to=None, *, after_id=None, dry_run=False):
    """Copy this pipeline's invoice data (rows with pos_id set on either side)
    from VA_TABLE to VA_OPT_TABLE wherever the two differ, for alerts created in
    [ts_from, ts_to), or with after_id for every row with a higher id. The other
    sync copies only new rows, a few seconds after they're inserted: a change
    written before then found no row in VA_OPT_TABLE to update, and the copy can
    carry the values from before it. Returns the number of rows copied (or that
    would be)."""
    if len(VA_TABLES) < 2:
        return 0
    if after_id is not None:
        scope, params = "va.id > %s", (after_id,)
    else:
        scope = """va.created_at >= %s AND va.created_at < %s
          AND vao.created_at >= %s AND vao.created_at < %s"""
        params = (ts_from, ts_to, ts_from, ts_to)
    tables = f"{VA_OPT_TABLE} vao JOIN {VA_TABLE} va ON va.id = vao.id AND va.created_at = vao.created_at"
    where = f"""{scope}
          AND (va.pos_id IS NOT NULL OR vao.pos_id IS NOT NULL)
          AND NOT (vao.invoice_id <=> va.invoice_id AND vao.amount <=> va.amount
                   AND vao.items <=> va.items AND vao.pos_id <=> va.pos_id)"""
    with conn.cursor() as cur:
        if dry_run:
            cur.execute(f"SELECT COUNT(*) AS n FROM {tables} WHERE {where}", params)
            return cur.fetchone()["n"]
        return cur.execute(f"""UPDATE {tables}
            SET vao.invoice_id = va.invoice_id, vao.amount = va.amount,
                vao.items = va.items, vao.pos_id = va.pos_id
            WHERE {where}""", params)


def _split_ids(text):
    return [i.strip() for i in (text or "").split(",") if i.strip()]


def _stale_foreign(conn, alerts, stored):
    """The alerts among `alerts` (each holding another process's invoices and
    winning none now) whose invoices pos_transactions has, every one, with an
    outlet. This run has put each of those on another alert or on none, so the
    old copy can only double count. An alert holding any invoice this matcher
    can't see keeps its old data."""
    old = {a["key"]: _split_ids(stored[a["key"]]["invoice_id"]) for a in alerts}
    first = min(a["start"] for a in alerts).date() - timedelta(days=1)
    last = max(a["start"] for a in alerts).date() + timedelta(days=1)
    known = set()
    with conn.cursor() as cur:
        for part in chunks(sorted(set().union(*old.values())), 1000):
            cur.execute(f"""SELECT invoice_id FROM {POS_TABLE}
                WHERE invoice_id IN ({_marks(part)}) AND `date` BETWEEN %s AND %s
                  AND outlet IS NOT NULL""", (*part, first, last))
            known.update(r["invoice_id"] for r in cur.fetchall())
    return [a for a in alerts if known.issuperset(old[a["key"]])]


def _rematch_piece(conn, outlets, ts_lo, ts_hi, replace=False, dry_run=False):
    stats = Counter()
    with conn.cursor() as cur:
        alerts = load_alerts(cur, outlets, ts_lo - MAX_ALERT_SPAN, ts_hi + MAX_ALERT_SPAN)
        touching = [a for a in alerts if a["start"] <= ts_hi and a["end"] >= ts_lo]
        if not touching:
            return stats
        # Their invoice lists are rebuilt whole, so read every invoice their
        # windows cover and every alert that could compete for one of them.
        lo = min(ts_lo, min(a["start"] for a in touching))
        hi = max(ts_hi, max(a["end"] for a in touching))
        alerts += load_alerts(cur, outlets, lo - MAX_ALERT_SPAN, ts_lo - MAX_ALERT_SPAN)
        alerts += load_alerts(cur, outlets, ts_hi + MAX_ALERT_SPAN, hi + MAX_ALERT_SPAN)
        invoices = load_pos(cur, outlets, lo, hi)
        touching.sort(key=lambda a: a["created_at"])
        stored = load_stored(cur, touching)

    won = assign(alerts, invoices)
    updates = []
    foreign = []  # alerts holding only another process's invoices that win none now
    for alert in touching:
        current = stored.get(alert["key"])
        if current is None:  # changed or gone since it was read
            continue
        invoice_ids, amount, items, pos_ids = build_match(won.get(alert["key"]))
        if invoice_ids is None:
            if not current["pos_id"]:  # nothing of ours to take back
                if replace and _split_ids(current["invoice_id"]):
                    foreign.append(alert)
                continue
        elif is_current(current, invoice_ids, amount, items, pos_ids):
            continue
        updates.append((alert["id"], alert["created_at"], invoice_ids, amount,
                        None if items is None else json.dumps(items), pos_ids))
    if foreign:
        stale = _stale_foreign(conn, foreign, stored)
        updates += [(a["id"], a["created_at"], None, None, None, None) for a in stale]
        stats["replaced"] += len(stale)
        stats["kept"] += len(foreign) - len(stale)
    if not dry_run:
        write_updates(conn, updates)
    stats["checked"] += len(touching)
    stats["written"] += len(updates)
    return stats


def rematch(conn, outlets, ts_lo, ts_hi, replace=False, dry_run=False):
    """Re-decide every invoice at `outlets` timed in [ts_lo, ts_hi] and rewrite
    each alert whose window overlaps that span, a day and OUTLET_CHUNK outlets
    at a time. Returns a Counter of alerts checked and written, and with
    replace=True of alerts whose other-process data was cleared or kept.
    dry_run counts without writing."""
    stats = Counter()
    piece_lo = ts_lo
    while piece_lo <= ts_hi:
        piece_hi = min(piece_lo + timedelta(days=1) - ONE_SECOND, ts_hi)
        for part in chunks(sorted(outlets), OUTLET_CHUNK):
            stats += _rematch_piece(conn, part, piece_lo, piece_hi, replace, dry_run)
        piece_lo = piece_hi + ONE_SECOND
    return stats


def match_days(conn, first, last, outlets=None, replace=False, dry_run=False):
    would = "would be " if dry_run else ""
    day = first
    while day <= last:
        ts_lo, ts_hi = at(day, 0), at(day, 86399)
        todo = outlets
        if todo is None:
            with conn.cursor() as cur:
                todo = alert_outlets(cur, ts_lo - MAX_ALERT_SPAN, ts_hi + MAX_ALERT_SPAN)
        stats = rematch(conn, todo, ts_lo, ts_hi, replace, dry_run)
        log.info("%s: %d outlets, %d alerts checked, %d %swritten",
                 day, len(todo), stats["checked"], stats["written"], would)
        if replace:
            log.info("%s: another process's invoices %scleared from %d alerts; kept on %d holding "
                     "invoices %s doesn't have", day, would, stats["replaced"], stats["kept"], POS_TABLE)
        copied = sync_opt(conn, ts_lo, ts_hi + ONE_SECOND, dry_run=dry_run)
        if copied:
            log.info("%s: %d rows %scopied to %s", day, copied, would, VA_OPT_TABLE)
        day += timedelta(days=1)


def fill_missing_outlets(conn, outlet_map):
    """Set pos_transactions.outlet on rows whose store_number has since been
    added to outlets. Bumps updated_at, so the change feed matches them next."""
    outlet_map.refresh(conn, force=True)
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT outlet_code FROM {POS_TABLE} WHERE outlet IS NULL")
        for row in cur.fetchall():
            outlet = outlet_map.lookup(row["outlet_code"])
            if outlet is None:
                continue
            n = cur.execute(f"UPDATE {POS_TABLE} SET outlet = %s WHERE outlet IS NULL AND outlet_code = %s",
                            (outlet, row["outlet_code"]))
            log.info("resolved %s -> outlet %s on %d POS rows", row["outlet_code"], outlet, n)


def _widen(spans, outlet, lo, hi):
    if outlet is None:
        return
    old = spans.get(outlet)
    spans[outlet] = (min(old[0], lo), max(old[1], hi)) if old else (lo, hi)


def start_state(cur, db_now, lookback):
    """Change-feed positions when none are saved: invoices updated in the last
    `lookback`, alerts inserted from now on (the first sweep re-matches recent
    ones anyway)."""
    cur.execute(f"SELECT COALESCE(MAX(id), 0) AS id FROM {VA_TABLE}")
    return {"pos_updated_at": str(db_now - lookback), "va_id": cur.fetchone()["id"]}


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


class Follower:
    """Real-time loop. Each cycle finds what changed since the last one:
      * invoices inserted or merged (pos_transactions.updated_at), whose owner
        may change, and
      * new CV alerts (vehicle_alerts.id), which can take invoices in their
        window from other alerts,
    then re-matches just those outlets and time spans."""

    def __init__(self):
        self.conn = None
        self.outlet_map = OutletMap()
        self.state = load_state()
        self.last_sweep = None
        self.last_outlet_fix = 0.0

    def run(self):
        backoff = POLL_SECONDS
        while True:
            try:
                if self.conn is None:
                    self.conn = connect()
                self.cycle()
                save_state(self.state)
            except Exception:
                # Whatever failed (usually the database being unreachable),
                # start over on a new connection instead of exiting, waiting
                # longer each time it fails again.
                log.exception("cycle failed; retrying in %ds", backoff)
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                _time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            backoff = POLL_SECONDS
            _time.sleep(POLL_SECONDS)

    def cycle(self):
        conn = self.conn
        with conn.cursor() as cur:
            # db_now is on updated_at's clock, now on created_at's.
            cur.execute("SELECT NOW() AS db_now, CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', %s) AS now",
                        (STORE_UTC_OFFSET,))
            clock = cur.fetchone()
            now = clock["now"]
            if not self.state:
                self.state = start_state(cur, clock["db_now"], FIRST_RUN_LOOKBACK)

        if _time.monotonic() - self.last_outlet_fix >= OUTLET_FIX_EVERY:
            fill_missing_outlets(conn, self.outlet_map)
            self.last_outlet_fix = _time.monotonic()

        pos_mark = datetime.fromisoformat(self.state["pos_updated_at"])
        va_mark = int(self.state["va_id"])
        spans = {}
        with conn.cursor() as cur:
            # The overlap re-reads rows from transactions that committed late
            # with an older timestamp; re-matching them is harmless.
            cur.execute(f"""SELECT outlet,
                    MIN(TIMESTAMP(`date`, `time`)) AS lo, MAX(TIMESTAMP(`date`, `time`)) AS hi,
                    MAX(updated_at) AS mark
                FROM {POS_TABLE}
                WHERE updated_at >= %s AND outlet IS NOT NULL
                GROUP BY outlet""", (pos_mark - WATERMARK_OVERLAP,))
            for row in cur.fetchall():
                _widen(spans, row["outlet"], row["lo"], row["hi"])
                pos_mark = max(pos_mark, row["mark"])

            cur.execute(f"""SELECT id, created_at, outlet, start_time, end_time
                FROM {VA_TABLE}
                WHERE created_at >= %s AND id > %s AND {ELIGIBLE}""",
                        (now - NEW_ALERT_LOOKBACK, max(va_mark - ID_OVERLAP, 0)))
            for row in cur.fetchall():
                window = alert_window(row)
                if window:
                    _widen(spans, *window)
                va_mark = max(va_mark, int(row["id"]))

        # Live traffic is all near `now`: one pass over the union of those
        # spans. Late data (a backlog, a store resolved hours later) is matched
        # outlet by outlet so it doesn't widen everyone else's span.
        stats = Counter()
        recent = {o: s for o, s in spans.items() if s[0] >= now - RECENT}
        if recent:
            stats += rematch(conn, list(recent), min(s[0] for s in recent.values()),
                             max(s[1] for s in recent.values()))
        for outlet, (lo, hi) in spans.items():
            if outlet not in recent:
                stats += rematch(conn, [outlet], lo, hi)
        if stats["written"]:
            log.info("changes: %d alerts checked, %d written", stats["checked"], stats["written"])
        # The other sync copies a row a few seconds after it's inserted, so the
        # rows it can overtake a change on are the newest ids, whatever their
        # created_at (a camera upload hours late, a consumer backlog).
        copied = sync_opt(conn, after_id=max(va_mark - RECOPY_IDS, 0))
        if copied:
            log.info("%d rows the other sync copied after they changed: re-copied to %s", copied, VA_OPT_TABLE)

        if self.last_sweep is None or _time.monotonic() - self.last_sweep >= SWEEP_EVERY:
            with conn.cursor() as cur:
                outlets = alert_outlets(cur, now - SWEEP_LOOKBACK, now)
            stats = rematch(conn, outlets, now - SWEEP_LOOKBACK, now)
            copied = sync_opt(conn, now - SWEEP_LOOKBACK, now + RECENT)
            log.info("sweep of last %s: %d alerts checked, %d written, %d re-copied to %s",
                     SWEEP_LOOKBACK, stats["checked"], stats["written"], copied, VA_OPT_TABLE)
            self.last_sweep = _time.monotonic()

        self.state = {"pos_updated_at": str(pos_mark), "va_id": va_mark}


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=date.fromisoformat, help="match this day once and exit")
    parser.add_argument("--end-date", type=date.fromisoformat, help="last day (inclusive) with --date")
    parser.add_argument("--outlets", help="comma-separated outlets.id (default: all)")
    parser.add_argument("--replace", action="store_true",
                        help="also clear another process's invoices from alerts that win none, "
                             f"when {POS_TABLE} has every one of them")
    parser.add_argument("--dry-run", action="store_true", help="log what would change, write nothing")
    parser.add_argument("--start-from-now", action="store_true",
                        help="save a place to continue from just before now and exit, so the next start "
                             "skips the first-run re-check of the last 24 hours; only right after stopping "
                             "a matcher that was running")
    args = parser.parse_args()

    if args.start_from_now:
        if args.date or args.end_date or args.outlets or args.replace or args.dry_run:
            parser.error("--start-from-now takes no other options")
        conn = gcp_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW() AS db_now")
                state = start_state(cur, cur.fetchone()["db_now"], RESTART_LOOKBACK)
        finally:
            conn.close()
        save_state(state)
        log.info("saved %s to %s; the next start continues from there", state, os.path.abspath(STATE_FILE))
        return

    if args.date is None:
        if args.end_date or args.outlets or args.replace or args.dry_run:
            parser.error("--end-date, --outlets, --replace and --dry-run need --date")
        log.info("following %s / %s in real time", POS_TABLE, VA_TABLE)
        Follower().run()
        return

    outlets = [int(o) for o in args.outlets.split(",")] if args.outlets else None
    conn = connect()
    try:
        match_days(conn, args.date, args.end_date or args.date, outlets, args.replace, args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
