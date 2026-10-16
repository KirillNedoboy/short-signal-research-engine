"""Durable research store, isolated from the live lane databases."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import json
import os
import sqlite3
import shutil
import time
from typing import Any, Iterator, Mapping
import uuid


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA auto_vacuum=INCREMENTAL;
CREATE TABLE IF NOT EXISTS collector_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS market_events (
    event_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    detected_at_ms INTEGER NOT NULL,
    first_delivery_ms INTEGER NOT NULL,
    last_delivery_ms INTEGER NOT NULL,
    entry_price REAL,
    entry_price_status TEXT NOT NULL,
    market_profile TEXT NOT NULL,
    status TEXT NOT NULL,
    observation_end_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_active ON market_events(status, observation_end_ms);
CREATE INDEX IF NOT EXISTS idx_events_symbol_delivery ON market_events(symbol, first_delivery_ms);
CREATE TABLE IF NOT EXISTS signal_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES market_events(event_id),
    lane TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    signal_time_ms INTEGER NOT NULL,
    delivery_time_ms INTEGER NOT NULL,
    delivery_status TEXT NOT NULL,
    strategy TEXT NOT NULL,
    model_version TEXT,
    config_version TEXT NOT NULL,
    source_event_id TEXT,
    signal_price REAL,
    decision_json TEXT NOT NULL,
    UNIQUE(lane, signal_id)
);
CREATE INDEX IF NOT EXISTS idx_signal_snapshots_event ON signal_snapshots(event_id);
CREATE TABLE IF NOT EXISTS feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES market_events(event_id),
    symbol TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    market_profile TEXT NOT NULL,
    features_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_features_event_time ON feature_snapshots(event_id, observed_at_ms);
CREATE TABLE IF NOT EXISTS raw_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id TEXT,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    exchange_ts_ms INTEGER NOT NULL,
    received_ts_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_trades_received ON raw_trades(received_ts_ms);
CREATE TABLE IF NOT EXISTS raw_orderbooks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    update_type TEXT NOT NULL,
    update_id INTEGER NOT NULL,
    cross_seq INTEGER,
    exchange_ts_ms INTEGER NOT NULL,
    received_ts_ms INTEGER NOT NULL,
    bids_json TEXT NOT NULL,
    asks_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_books_received ON raw_orderbooks(received_ts_ms);
CREATE TABLE IF NOT EXISTS liquidations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    position_side TEXT NOT NULL,
    bankruptcy_price REAL,
    quantity REAL,
    exchange_ts_ms INTEGER NOT NULL,
    received_ts_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_liquidations_received ON liquidations(received_ts_ms);
CREATE TABLE IF NOT EXISTS price_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES market_events(event_id),
    symbol TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    price REAL,
    status TEXT NOT NULL,
    provider TEXT,
    reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_price_samples_event_time ON price_samples(event_id, observed_at_ms);
CREATE TABLE IF NOT EXISTS outcomes (
    event_id TEXT PRIMARY KEY REFERENCES market_events(event_id),
    entry_price REAL,
    entry_status TEXT NOT NULL,
    price_15m REAL,
    price_15m_status TEXT NOT NULL,
    price_1h REAL,
    price_1h_status TEXT NOT NULL,
    price_4h REAL,
    price_4h_status TEXT NOT NULL,
    adverse_10_cross_at_ms INTEGER,
    adverse_20_cross_at_ms INTEGER,
    passage_order_json TEXT NOT NULL,
    quality TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type TEXT NOT NULL,
    cohort_end_ms INTEGER NOT NULL,
    generated_at_ms INTEGER NOT NULL,
    summary_json TEXT NOT NULL,
    UNIQUE(report_type, cohort_end_ms)
);
CREATE TABLE IF NOT EXISTS feed_health (
    provider TEXT NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    details_json TEXT NOT NULL,
    PRIMARY KEY(provider, market, symbol)
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class ResearchDB:
    """SQLite store with explicit raw-data pressure controls."""

    RAW_TABLES = ("raw_trades", "raw_orderbooks", "liquidations")

    def __init__(
        self,
        path: str | Path,
        *,
        max_bytes: int,
        reserve_free_bytes: int,
        raw_retention_sec: int,
        raw_max_rows: int,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self.max_bytes = max_bytes
        self.reserve_free_bytes = reserve_free_bytes
        self.raw_retention_sec = raw_retention_sec
        self.raw_max_rows = raw_max_rows
        self.raw_enabled = True
        self.last_prune_ms = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            # Feed handlers batch raw inserts until the next durable write.
            # Commit that batch before opening an explicit transaction so a
            # signal linkage can never fail with nested BEGIN.
            if self.conn.in_transaction:
                self.conn.commit()
            self.conn.execute("BEGIN")
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM collector_meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO collector_meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def prepare_run(self, started_ms: int) -> None:
        """Record a run watermark and refuse an unmarked historical store.

        A database produced by an older collector build has no startup
        watermark. Refusing it prevents silently mixing a pre-watermark
        historical cohort into a new production collection run; the operator
        must point the service at a new research path or explicitly migrate
        the old artifact.
        """

        prior_run = self.get_meta("run_started_at_ms")
        event_count = int(self.conn.execute("SELECT COUNT(*) FROM market_events").fetchone()[0])
        if event_count and prior_run is None:
            raise RuntimeError(
                "research database contains historical events without a startup watermark; "
                "use a new storage.db_path for this collector run"
            )
        self.set_meta("run_started_at_ms", str(started_ms))

    def link_signal(
        self,
        *,
        lane: str,
        signal_id: str,
        symbol: str,
        signal_time_ms: int,
        delivery_time_ms: int,
        delivery_status: str,
        strategy: str,
        model_version: str | None,
        config_version: str,
        source_event_id: str | None,
        signal_price: float | None,
        decision: Mapping[str, Any],
        merge_window_sec: int,
        now_ms: int,
        observation_end_ms: int,
    ) -> tuple[str, bool]:
        """Attach a signal to a nearby event or create a new event.

        Matching is symbol plus delivery-time proximity.  The source event id
        is preserved as metadata because different lanes can assign different
        ids to the same market move.
        """

        existing = self.conn.execute(
            "SELECT event_id FROM signal_snapshots WHERE lane=? AND signal_id=?",
            (lane, signal_id),
        ).fetchone()
        if existing:
            return str(existing[0]), False
        window_ms = merge_window_sec * 1000
        row = self.conn.execute(
            "SELECT event_id FROM market_events "
            "WHERE symbol=? AND first_delivery_ms BETWEEN ? AND ? "
            "ORDER BY ABS(first_delivery_ms-?) LIMIT 1",
            (symbol, delivery_time_ms - window_ms, delivery_time_ms + window_ms, delivery_time_ms),
        ).fetchone()
        created = row is None
        event_id = str(uuid.uuid4()) if created else str(row[0])
        with self.transaction() as conn:
            if created:
                conn.execute(
                    "INSERT INTO market_events(event_id,symbol,detected_at_ms,first_delivery_ms,last_delivery_ms,"
                    "entry_price,entry_price_status,market_profile,status,observation_end_ms,created_at_ms,updated_at_ms) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event_id,
                        symbol,
                        signal_time_ms,
                        delivery_time_ms,
                        delivery_time_ms,
                        signal_price,
                        "SIGNAL_PRICE" if signal_price is not None else "UNKNOWN",
                        "PENDING",
                        "PENDING",
                        observation_end_ms,
                        now_ms,
                        now_ms,
                    ),
                )
            else:
                conn.execute(
                    "UPDATE market_events SET last_delivery_ms=MAX(last_delivery_ms,?), "
                    "observation_end_ms=MAX(observation_end_ms,?),updated_at_ms=? WHERE event_id=?",
                    (delivery_time_ms, observation_end_ms, now_ms, event_id),
                )
            conn.execute(
                "INSERT INTO signal_snapshots(event_id,lane,signal_id,symbol,signal_time_ms,delivery_time_ms,"
                "delivery_status,strategy,model_version,config_version,source_event_id,signal_price,decision_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    lane,
                    signal_id,
                    symbol,
                    signal_time_ms,
                    delivery_time_ms,
                    delivery_status,
                    strategy,
                    model_version,
                    config_version,
                    source_event_id,
                    signal_price,
                    _json(decision),
                ),
            )
        return event_id, created

    def activate_event(
        self,
        event_id: str,
        *,
        entry_price: float | None,
        entry_status: str,
        market_profile: str,
        now_ms: int,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE market_events SET entry_price=COALESCE(?,entry_price),entry_price_status=?,"
                "market_profile=?,status='COLLECTING',updated_at_ms=? WHERE event_id=?",
                (entry_price, entry_status, market_profile, now_ms, event_id),
            )
            conn.execute(
                "INSERT INTO outcomes(event_id,entry_price,entry_status,price_15m_status,price_1h_status,"
                "price_4h_status,passage_order_json,quality,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(event_id) DO UPDATE SET entry_price=COALESCE(excluded.entry_price,outcomes.entry_price),"
                "entry_status=excluded.entry_status,updated_at_ms=excluded.updated_at_ms",
                (event_id, entry_price, entry_status, "PENDING", "PENDING", "PENDING", "[]", "LIVE", now_ms),
            )

    def mark_capacity_rejected(self, event_id: str, now_ms: int) -> None:
        self.conn.execute(
            "UPDATE market_events SET status='CAPACITY_REJECTED',updated_at_ms=? WHERE event_id=?",
            (now_ms, event_id),
        )
        self.conn.commit()

    def set_entry_price(self, event_id: str, price: float, status: str, now_ms: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE market_events SET entry_price=?,entry_price_status=?,updated_at_ms=? "
                "WHERE event_id=? AND entry_price IS NULL",
                (price, status, now_ms, event_id),
            )
            conn.execute(
                "UPDATE outcomes SET entry_price=COALESCE(entry_price,?),entry_status=?,updated_at_ms=? WHERE event_id=?",
                (price, status, now_ms, event_id),
            )

    def active_events(self, now_ms: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM market_events WHERE status IN ('ACTIVE','COLLECTING') AND observation_end_ms>=? ORDER BY first_delivery_ms",
                (now_ms,),
            )
        )

    def pending_events(self, now_ms: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM market_events WHERE status IN ('PENDING','CAPACITY_REJECTED') "
                "AND observation_end_ms>=? ORDER BY first_delivery_ms",
                (now_ms,),
            )
        )

    def add_feature(self, event_id: str, symbol: str, snapshot_ms: int, profile: str, features: Mapping[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO feature_snapshots(event_id,symbol,observed_at_ms,market_profile,features_json) VALUES(?,?,?,?,?)",
            (event_id, symbol, snapshot_ms, profile, _json(features)),
        )
        self.conn.commit()

    def add_price_sample(
        self,
        event_id: str,
        symbol: str,
        observed_ms: int,
        price: float | None,
        status: str,
        provider: str | None,
        reason: str | None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO price_samples(event_id,symbol,observed_at_ms,price,status,provider,reason) VALUES(?,?,?,?,?,?,?)",
            (event_id, symbol, observed_ms, price, status, provider, reason),
        )
        self.conn.commit()

    def record_outcome_samples(self, event_id: str, now_ms: int, horizons: tuple[int, ...], grace_sec: int) -> None:
        row = self.conn.execute("SELECT * FROM market_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            return
        entry = row["entry_price"]
        entry_status = str(row["entry_price_status"])
        fields: dict[str, Any] = {"updated_at_ms": now_ms}
        names = {horizons[0]: "price_15m", horizons[1]: "price_1h", horizons[2]: "price_4h"}
        for horizon_ms, field_name in names.items():
            if now_ms < row["first_delivery_ms"] + horizon_ms:
                continue
            existing = self.conn.execute(
                f"SELECT {field_name}_status FROM outcomes WHERE event_id=?", (event_id,)
            ).fetchone()
            if existing and str(existing[0]) != "PENDING":
                continue
            sample = self.conn.execute(
                "SELECT price,status FROM price_samples WHERE event_id=? AND observed_at_ms>=? "
                "AND observed_at_ms<=? AND status='KNOWN' AND price IS NOT NULL "
                "ORDER BY observed_at_ms LIMIT 1",
                (
                    event_id,
                    row["first_delivery_ms"] + horizon_ms,
                    row["first_delivery_ms"] + horizon_ms + grace_sec * 1000,
                ),
            ).fetchone()
            fields[f"{field_name}"] = float(sample[0]) if sample else None
            fields[f"{field_name}_status"] = "KNOWN" if sample else "UNKNOWN_WINDOW"
        if fields.keys() == {"updated_at_ms"}:
            return
        assignments = ",".join(f"{key}=?" for key in fields)
        values = list(fields.values()) + [event_id]
        self.conn.execute(f"UPDATE outcomes SET {assignments} WHERE event_id=?", values)
        if all(
            self.conn.execute(f"SELECT {field}_status FROM outcomes WHERE event_id=?", (event_id,)).fetchone()[0]
            != "PENDING"
            for field in ("price_15m", "price_1h", "price_4h")
        ):
            self.conn.execute("UPDATE market_events SET status='COMPLETED',updated_at_ms=? WHERE event_id=?", (now_ms, event_id))
        self.conn.commit()

    def record_crossings(self, event_id: str, observed_ms: int, price: float | None, status: str) -> None:
        row = self.conn.execute(
            "SELECT entry_price,entry_price_status FROM outcomes WHERE event_id=?", (event_id,)
        ).fetchone()
        if not row or row["entry_price"] is None or status != "KNOWN" or price is None:
            return
        entry = float(row["entry_price"])
        if entry <= 0:
            return
        outcome = self.conn.execute("SELECT * FROM outcomes WHERE event_id=?", (event_id,)).fetchone()
        order = json.loads(outcome["passage_order_json"] or "[]")
        passed = {item.get("level") for item in order if isinstance(item, dict)}
        for level, pct in ((10, 0.10), (20, 0.20)):
            if level not in passed and price >= entry * (1.0 + pct):
                order.append({"level": level, "at_ms": observed_ms, "price": price})
                self.conn.execute(
                    f"UPDATE outcomes SET adverse_{level}_cross_at_ms=?,passage_order_json=?,updated_at_ms=? WHERE event_id=?",
                    (observed_ms, _json(order), observed_ms, event_id),
                )
        self.conn.commit()

    def raw_trade(self, values: tuple[Any, ...]) -> None:
        if not self.raw_enabled:
            return
        self.conn.execute(
            "INSERT INTO raw_trades(provider,market,symbol,trade_id,side,price,quantity,exchange_ts_ms,received_ts_ms) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            values,
        )

    def raw_orderbook(self, values: tuple[Any, ...]) -> None:
        if not self.raw_enabled:
            return
        self.conn.execute(
            "INSERT INTO raw_orderbooks(provider,market,symbol,update_type,update_id,cross_seq,exchange_ts_ms,"
            "received_ts_ms,bids_json,asks_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
            values,
        )

    def liquidation(self, values: tuple[Any, ...]) -> None:
        if not self.raw_enabled:
            return
        self.conn.execute(
            "INSERT INTO liquidations(provider,symbol,position_side,bankruptcy_price,quantity,exchange_ts_ms,received_ts_ms) "
            "VALUES(?,?,?,?,?,?,?)",
            values,
        )

    def health(self, provider: str, market: str, symbol: str, now_ms: int, state: str, reason: str | None, details: Mapping[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO feed_health(provider,market,symbol,observed_at_ms,state,reason,details_json) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(provider,market,symbol) DO UPDATE SET observed_at_ms=excluded.observed_at_ms,state=excluded.state,"
            "reason=excluded.reason,details_json=excluded.details_json",
            (provider, market, symbol, now_ms, state, reason, _json(details)),
        )
        self.conn.commit()

    def db_size_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            try:
                total += candidate.stat().st_size
            except FileNotFoundError:
                pass
        return total

    def prune(self, now_ms: int) -> dict[str, Any]:
        """Prune raw rows first and disable raw writes under hard pressure."""

        cutoff = now_ms - self.raw_retention_sec * 1000
        for table, column in (("raw_trades", "received_ts_ms"), ("raw_orderbooks", "received_ts_ms"), ("liquidations", "received_ts_ms")):
            self.conn.execute(f"DELETE FROM {table} WHERE {column} < ?", (cutoff,))
        self.conn.commit()
        total_raw = sum(int(self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in self.RAW_TABLES)
        target = max(0, total_raw - self.raw_max_rows)
        while target:
            removed = 0
            for table in self.RAW_TABLES:
                if target <= 0:
                    break
                count = min(target, 10_000)
                self.conn.execute(
                    f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} ORDER BY id LIMIT ?)", (count,)
                )
                target -= count
                removed += count
            if removed == 0:
                break
            self.conn.commit()
        try:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.conn.execute("PRAGMA incremental_vacuum(2000)")
            self.conn.commit()
        except sqlite3.DatabaseError:
            pass
        size = self.db_size_bytes()
        try:
            free = int(shutil.disk_usage(self.path.parent).free)
        except OSError:
            free = None
        pressure = size >= self.max_bytes or (free is not None and free < self.reserve_free_bytes)
        # Signal snapshots, outcomes, and reports are retained. If raw data
        # was insufficient to clear hard pressure, compact the high-cadence
        # derived timelines before pausing raw writes.
        if pressure:
            for table in ("feature_snapshots", "price_samples"):
                for _ in range(100):
                    deleted = self.conn.execute(
                        f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} ORDER BY id LIMIT 10000)"
                    ).rowcount
                    if deleted == 0:
                        break
                    self.conn.commit()
                    try:
                        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        self.conn.execute("PRAGMA incremental_vacuum(1000)")
                        self.conn.commit()
                    except sqlite3.DatabaseError:
                        pass
                    size = self.db_size_bytes()
                    if size < int(self.max_bytes * 0.90):
                        break
                if size < int(self.max_bytes * 0.90):
                    break
            pressure = size >= self.max_bytes or (free is not None and free < self.reserve_free_bytes)
        self.raw_enabled = not pressure
        self.last_prune_ms = now_ms
        return {"db_bytes": size, "raw_rows": total_raw, "free_bytes": free, "raw_enabled": self.raw_enabled}

    def report(self, report_type: str, cohort_end_ms: int, generated_ms: int) -> dict[str, Any]:
        total = int(self.conn.execute("SELECT COUNT(*) FROM market_events WHERE first_delivery_ms<=?", (cohort_end_ms,)).fetchone()[0])
        completed = int(self.conn.execute("SELECT COUNT(*) FROM market_events WHERE status='COMPLETED' AND first_delivery_ms<=?", (cohort_end_ms,)).fetchone()[0])
        adverse10 = int(self.conn.execute("SELECT COUNT(*) FROM outcomes WHERE adverse_10_cross_at_ms IS NOT NULL AND event_id IN (SELECT event_id FROM market_events WHERE first_delivery_ms<=?)", (cohort_end_ms,)).fetchone()[0])
        adverse20 = int(self.conn.execute("SELECT COUNT(*) FROM outcomes WHERE adverse_20_cross_at_ms IS NOT NULL AND event_id IN (SELECT event_id FROM market_events WHERE first_delivery_ms<=?)", (cohort_end_ms,)).fetchone()[0])
        summary = {"events": total, "completed": completed, "adverse_10": adverse10, "adverse_20": adverse20, "data_quality": "descriptive"}
        self.conn.execute(
            "INSERT OR IGNORE INTO reports(report_type,cohort_end_ms,generated_at_ms,summary_json) VALUES(?,?,?,?)",
            (report_type, cohort_end_ms, generated_ms, _json(summary)),
        )
        self.conn.commit()
        return summary

