"""Gate cohort replay: "what would have happened if this gate had not blocked
these candidates?" (LANE-B analytics; source DB is opened read-only).

Cohort = evaluations of one branch where the chosen veto code was the SINGLE
blocker inside the window. For every cohort event we fetch post-decision 1m
klines from the public Bybit REST API (see DATA SOURCE below), compute forward
short returns at 15m/1h/4h, run the SAME virtual exit rules as the app
(app/outcomes/evaluator.py), and print the identical metrics for a control
cohort: evaluations of the same branch that passed the gate and produced a
persisted signal in the same period. Also prints the delta and its sign.

DATA SOURCE (honest caveat):
  The live DB retains NO candles/klines table; climax_evaluations.features_json
  stores decision-time features only (no price series). The runtime market
  frame store (market-frame-store.json.gz) is a 300-candle (~5 h) ring and
  cannot cover a multi-day window; scripts/baseline_replay.py replays recorded
  fixture frames, not arbitrary history. Therefore forward prices are fetched
  from the public Bybit v5 REST kline endpoint, which retains full 1m history
  for the window. [ФАКТ — проверено 2026-09-15: sqlite_master содержит только
  событийные/аналитические таблицы, ни одной ценовой.]

LOOKAHEAD ENFORCEMENT:
  * entry_price := features_json["price"] recorded in the evaluation itself
    (decision-time market price; never a later candle);
  * SL reference := recorded event_high / short_zone_high from the stored row;
  * TP reference := decision-time vwap from features_json, falling back to the
    app's entry*0.97 rule when the field is absent;
  * cohort dedupe is by (symbol, event_id) keeping the FIRST single-veto
    evaluation in the window, so a later re-evaluation of the same root can
    never shift the entry into the future;
  * forward candles are used ONLY for outcome measurement, t >= decision time;
  * no feature is reconstructed from future data anywhere in this script.

EXIT-RULE PARITY with app/outcomes/evaluator.py:
  tp_price = vwap else entry*0.97; sl_price = event_high/short_zone_high else
  entry*1.03; tp1_hit / stopped_virtual / mfe / mae scan the same 4 h
  evaluation window for BOTH cohorts (keeps blocked vs passed readings
  identical); the risk-adjusted classification (CLEAN_TP, SQUEEZE_BEFORE_TP,
  DIRTY_TP_HIGH_MAE, SL_OR_BAD, NOT_ENTERED) is imported from the app itself.
  Hit-rate ("clean%") = share of evaluated rows with CLEAN_TP.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.outcomes.evaluator import _classify_risk_adjusted  # parity import

UTC = timezone.utc
DB_PATH = "/opt/short-telegram-bot-lite/data/bot.sqlite"
KLINE_URL = "https://api.bybit.com/v5/market/kline"
HORIZONS = (("15m", 15), ("1h", 60), ("4h", 240))


class _Outcome:
    """Duck-typed stand-in consumed by _classify_risk_adjusted (field parity)."""

    __slots__ = ("mfe_pct", "mae_pct", "squeeze_extension_pct", "tp1_hit", "stopped_virtual")

    def __init__(self, mfe, mae, tp1_hit, stopped_virtual) -> None:
        self.mfe_pct = mfe
        self.mae_pct = mae
        self.squeeze_extension_pct = mae
        self.tp1_hit = tp1_hit
        self.stopped_virtual = stopped_virtual


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(str(value))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _connect_ro(db: str, timeout_sec: int) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=timeout_sec)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _fetch_klines(
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    timeout_sec: float,
    retries: int,
    cache: dict,
) -> list[list[float]]:
    """Closed-range 1m candles [open_ms, high, low, close] over [start,end]."""
    key = (symbol, start_ms // 60_000, end_ms // 60_000)
    if key in cache:
        return cache[key]
    query = urllib.parse.urlencode(
        {
            "category": "linear",
            "symbol": symbol,
            "interval": "1",
            "start": start_ms,
            "end": end_ms,
            "limit": 1000,
        }
    )
    last_error: Exception | None = None
    payload = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(
                f"{KLINE_URL}?{query}", headers={"User-Agent": "lane-b-cohort-replay"}
            )
            with urllib.request.urlopen(request, timeout=timeout_sec) as response:
                body = response.read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            payload = json.loads(body)
            last_error = None
            break
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
            last_error = error
            time.sleep(0.5 * (attempt + 1))
    if last_error is not None:
        raise RuntimeError(f"kline fetch failed for {symbol}: {last_error}")
    page = (payload.get("result") or {}).get("list") or []
    # endpoint returns newest-first; normalize to ascending [open_ms, high, low, close]
    rows = sorted([[int(float(r[0])), float(r[2]), float(r[3]), float(r[4])] for r in page])
    cache[key] = rows
    time.sleep(0.12)  # stay far below public rate limits
    return rows


def _metrics(cases: list[dict], now_ms: int, cost_pct: float = 0.0) -> dict:
    """cost_pct: round-trip fee+slippage estimate in % (EVALUATION, not fact):
    subtracted from every horizon return; virtual TP barrier shifted DOWN
    and SL barrier shifted DOWN (earlier stop) by cost_pct — net-aware."""
    per_h: dict[str, list[float]] = {label: [] for label, _ in HORIZONS}
    counts = {"CLEAN_TP": 0, "DIRTY_TP_HIGH_MAE": 0, "SQUEEZE_BEFORE_TP": 0,
              "SL_OR_BAD": 0, "NOT_ENTERED": 0, "INVALID_OR_MISSING": 0, "NO_FORWARD_DATA": 0}
    for case in cases:
        candles = case.get("candles") or []
        entry = case["entry_price"]
        base = _parse_dt(case["t"])
        base_ms = int(base.timestamp() * 1000)
        window_end = base_ms + 4 * 3_600_000
        case["forward_complete"] = bool(candles) and candles[-1][0] + 60_000 >= window_end
        for label, minutes in HORIZONS:
            target = base_ms + minutes * 60_000
            after = [row for row in candles if row[0] >= target]
            if after and after[0][0] + 60_000 <= now_ms:
                per_h[label].append((entry - after[0][3]) / entry * 100 - cost_pct)
        future = [row for row in candles if base_ms <= row[0] and row[0] + 60_000 <= min(window_end, now_ms)]
        if not future:
            case["status"] = "NO_FORWARD_DATA"
            counts["NO_FORWARD_DATA"] += 1
            continue
        vwap = case.get("vwap")
        tp_price = float(vwap) if vwap else entry * 0.97
        sl_ref = case.get("sl_ref")
        sl_price = float(sl_ref) if sl_ref else entry * 1.03
        tp1_hit = any(row[2] <= tp_price * (1 - cost_pct / 100) for row in future)
        stopped = any(row[1] >= sl_price * (1 - cost_pct / 100) for row in future)
        mfe = max((entry - row[2]) / entry * 100 for row in future)
        mae = max((row[1] - entry) / entry * 100 for row in future)
        status = _classify_risk_adjusted(_Outcome(mfe, mae, tp1_hit, stopped))
        case["status"] = status
        case["tp1_hit"] = tp1_hit
        case["stopped_virtual"] = stopped
        counts[status] = counts.get(status, 0) + 1
    evaluated = [c for c in cases if c.get("status") != "NO_FORWARD_DATA"]
    block: dict = {
        "n": len(cases),
        "n_evaluated": len(evaluated),
        "first_ts": min((c["t"] for c in cases), default=None),
        "last_ts": max((c["t"] for c in cases), default=None),
        "forward_short_return_pct": {},
        "clean_tp_rate": (sum(1 for c in evaluated if c["status"] == "CLEAN_TP") / len(evaluated)) if evaluated else None,
        "exit_counts": counts,
    }
    for label in per_h:
        values = per_h[label]
        block["forward_short_return_pct"][label] = {
            "n": len(values),
            "mean": round(statistics.mean(values), 4) if values else None,
            "median": round(statistics.median(values), 4) if values else None,
        }
    return block


def _fmt(x, digits=3, suffix="") -> str:
    return "n/a" if x is None else f"{x:+.{digits}f}{suffix}"


def _render(branch: str, veto: str, days: int, cohort: dict, control: dict) -> tuple[str, dict]:
    header = (
        f"{'cohort':<22}{'n':>4}{'eval':>6}{'15m mean':>10}{'15m med':>9}"
        f"{'1h mean':>9}{'1h med':>8}{'4h mean':>9}{'4h med':>8}{'clean%':>8}"
    )

    def row(name: str, block: dict) -> str:
        fr = block["forward_short_return_pct"]
        clean = block["clean_tp_rate"]
        clean_s = "n/a" if clean is None else f"{clean * 100:.1f}"
        return (
            f"{name:<22}{block['n']:>4}{block['n_evaluated']:>6}"
            f"{_fmt(fr['15m']['mean']).rjust(10)}{_fmt(fr['15m']['median']).rjust(9)}"
            f"{_fmt(fr['1h']['mean']).rjust(9)}{_fmt(fr['1h']['median']).rjust(8)}"
            f"{_fmt(fr['4h']['mean']).rjust(9)}{_fmt(fr['4h']['median']).rjust(8)}"
            f"{clean_s.rjust(8)}"
        )

    delta = {}
    dline = f"{'DELTA cohort-control':<22}"
    for label in ("15m", "1h", "4h"):
        c = cohort["forward_short_return_pct"][label]["mean"]
        k = control["forward_short_return_pct"][label]["mean"]
        d = None if (c is None or k is None) else round(c - k, 4)
        delta[f"mean_{label}_pct"] = d
        dline += f" d{label}={_fmt(d)}"
    cd = (
        None
        if cohort["clean_tp_rate"] is None or control["clean_tp_rate"] is None
        else round((cohort["clean_tp_rate"] - control["clean_tp_rate"]) * 100, 1)
    )
    delta["clean_tp_rate_pp"] = cd
    dline += f" dclean={_fmt(cd, 1, 'pp')}"
    lines = [
        f"gate cohort replay | branch={branch} veto={veto} window_days={days}",
        f"span: cohort {cohort['first_ts']} .. {cohort['last_ts']} | control {control['first_ts']} .. {control['last_ts']}",
        header,
        row("COHORT(blocked)", cohort),
        row("CONTROL(passed)", control),
        dline,
        "sign: delta>0 means the blocked cohort would have performed BETTER than",
        "signals that passed the gate (gate too strict); delta<0 supports the gate.",
        "clean% = CLEAN_TP hit-rate on the app's 4h virtual-exit window;",
        "forward short return = (entry - price_after)/entry*100, entry = recorded decision price.",
    ]
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "branch": branch,
        "veto_single_blocker": veto,
        "window_days": days,
        "data_source": "bybit v5 public kline REST (1m); DB provides decision-time rows only",
        "entry_price_source": "climax_evaluations.features_json.price (decision time)",
        "exit_rules": "app/outcomes/evaluator.py parity: tp=vwap else -3%, sl=event_high else +3%, 4h window",
        "lookahead_controls": [
            "entry/vwap/event_high taken from recorded evaluation fields only",
            "one case per (symbol,event): first single-veto evaluation in window",
            "forward candles only for outcome measurement, t>=decision time",
        ],
        "cohort_blocked": cohort,
        "control_passed": control,
        "delta": delta,
    }
    return "\n".join(lines), payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--branch", required=True, help="subtype_candidate, e.g. LOW_VOLUME_EXTENSION_FAILURE")
    parser.add_argument("--veto", required=True, help="single-blocker veto code, e.g. rejection_missing")
    parser.add_argument("--days", type=int, default=7, help="lookback window (default 7)")
    parser.add_argument("--db", default=DB_PATH, help="source DB path (opened mode=ro)")
    parser.add_argument("--timeout", type=int, default=30, help="SQLite busy timeout, seconds")
    parser.add_argument("--http-timeout", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--json-out", default=None, help="write full JSON report here")
    parser.add_argument("--fee-pct", type=float, default=0.055,
                        help="one-side taker fee %% (round-trip cost = 2*(fee+slip))")
    parser.add_argument("--slippage-pct", type=float, default=0.05,
                        help="one-side slippage %% estimate")
    parser.add_argument("--no-fetch", action="store_true", help="skip REST; report cohort/control membership only")
    args = parser.parse_args(argv)

    cutoff = datetime.now(UTC) - timedelta(days=args.days)
    cutoff_sql = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect_ro(args.db, args.timeout)
    try:
        # Bounded by ix_climax_evaluations_evaluation_time range scan + LIMIT.
        cohort_rows = conn.execute(
            "SELECT symbol, event_id, evaluation_time, event_high, live_veto_reasons_json,"
            " features_json FROM climax_evaluations"
            " WHERE evaluation_time >= ? AND evaluation_time < ? AND subtype_candidate = ?"
            " AND live_veto_reasons_json IS NOT NULL"
            " ORDER BY evaluation_time ASC LIMIT 400000",
            (cutoff_sql, datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"), args.branch),
        ).fetchall()
        control_rows = conn.execute(
            "SELECT symbol, signal_time, market_price, short_zone_high, event_high, context_json"
            " FROM signals WHERE signal_time >= ? AND strategy_subtype = ?"
            " ORDER BY signal_time ASC LIMIT 5000",
            (cutoff_sql, args.branch),
        ).fetchall()
    finally:
        conn.close()

    # One case per event: FIRST evaluation where the chosen veto is the ONLY blocker.
    seen: set = set()
    cases: list[dict] = []
    for row in cohort_rows:
        try:
            vetoes = json.loads(row["live_veto_reasons_json"] or "[]")
        except json.JSONDecodeError:
            continue
        if not (isinstance(vetoes, list) and len(vetoes) == 1 and vetoes[0] == args.veto):
            continue
        key = (row["symbol"], row["event_id"])
        if key in seen:
            continue
        try:
            features = json.loads(row["features_json"] or "{}")
        except json.JSONDecodeError:
            features = {}
        entry = features.get("price")
        if not entry or entry <= 0:
            continue
        seen.add(key)
        cases.append(
            {
                "symbol": row["symbol"],
                "event_id": row["event_id"],
                "t": row["evaluation_time"],
                "entry_price": float(entry),
                "vwap": features.get("vwap"),
                "sl_ref": row["event_high"],
            }
        )

    control: list[dict] = []
    for row in control_rows:
        try:
            context = json.loads(row["context_json"] or "{}")
        except json.JSONDecodeError:
            context = {}
        if not row["market_price"] or row["market_price"] <= 0:
            continue
        control.append(
            {
                "symbol": row["symbol"],
                "t": row["signal_time"],
                "entry_price": float(row["market_price"]),
                "vwap": context.get("signal_vwap") or context.get("vwap"),
                "sl_ref": row["short_zone_high"] or row["event_high"],
            }
        )

    now_ms = int(time.time() * 1000)
    cache: dict = {}
    fetch_errors: list[str] = []
    if not args.no_fetch:
        for case in cases + control:
            base_ms = int(_parse_dt(case["t"]).timestamp() * 1000)
            try:
                case["candles"] = _fetch_klines(
                    case["symbol"],
                    base_ms,
                    min(base_ms + 4 * 3_600_000 + 60_000, now_ms),
                    timeout_sec=args.http_timeout,
                    retries=args.retries,
                    cache=cache,
                )
            except RuntimeError as error:
                case["candles"] = []
                fetch_errors.append(str(error))

    cost_pct = 2.0 * (args.fee_pct + args.slippage_pct)
    cohort_block = _metrics(cases, now_ms, cost_pct)
    control_block = _metrics(control, now_ms, cost_pct)
    table, payload = _render(args.branch, args.veto, args.days, cohort_block, control_block)
    payload["cost_model_pct"] = {
        "fee_one_side": args.fee_pct,
        "slippage_one_side": args.slippage_pct,
        "round_trip_subtracted": cost_pct,
        "note": "estimate, not fact: applies to returns AND net TP/SL barriers",
    }
    if fetch_errors:
        payload["fetch_errors"] = fetch_errors[:20]
    print(table)
    print(json.dumps(payload, indent=2, default=str))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
