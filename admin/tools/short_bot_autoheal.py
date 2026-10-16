#!/usr/bin/env python3
"""Bounded scan-coverage watchdog for short-telegram-bot-lite (Lanes A/B).

Canonical location (defect-3 fix):
  /opt/short-telegram-bot-lite-admin/tools/short_bot_autoheal.py
  integrity baseline:  /opt/short-telegram-bot-lite-admin/tools/short_bot_autoheal.sha256
  verified by tools/release_drift_alarm.py (finding AUTOHEAL_SCRIPT_DRIFT).
  NEVER hot-edit the deployed copy: change the source, re-cut the sha256
  baseline, and record the change in work/autoheal-fix-20260923/.

DECISION CONTRACT -- this watchdog must never disable itself silently.
Every run evaluates every lane and emits exactly one journal line per lane
(`logger -t short-bot-autoheal`), plus one JSON report on stdout (which
systemd also journals).  Unknown outcomes raise on purpose so the unit fails
loudly.  Decisions (decision=... in the journal line, reasons always present):

  healthy            scan windows fresh in the lookback        -> no action
  no-data            <2 parseable scan windows in lookback     -> no action (logged)
  inactive           unit not active/running                   -> no action, ALERT
                     (only the mass-stale path can restart -- unchanged)
  provider-fault     mass STALE_DATA + upstream fault evidence -> ALERT, NO restart
  stale-unverified   mass STALE_DATA, no local corroboration   -> ALERT, NO restart
  crash-looping      restart warranted but >=CRASH_LOOP_RESTARTS unit starts
                     in the last CRASH_LOOP_WINDOW             -> ALERT, NO restart
  cooldown           restart warranted but 900 s per-unit cooldown active
                                                            -> ALERT(warning), no restart
  restart            all guards pass                           -> systemctl restart
  restart-failed     systemctl restart returned nonzero        -> ALERT, exit 1
  -- rule 4 (continuity latch; ALERT-ONLY, evaluated every pass) --
  continuity-ok          no symbol breaches the expired-gap thresholds
  continuity-unavailable no CONTINUITY_UNHEALTHY evidence exists yet
                         (bot-side persistence undeployed) -> rule inert, logged
  continuity-latch       >=3 expired UNREPAIRED gaps on one symbol within 60
                         scan windows, OR one expired gap missing >51 min
                         (3x the ~17-min tracker capacity) -> ALERT, NO restart

Exit code: 1 when any lane ended needs-action (ALERT / restart-failed), else
0.  A nonzero exit marks short-bot-autoheal.service as failed, which keeps
the alert visible to `systemctl --failed` and operators -- without changing
the timer or disabling anything.

RATIONALE for requiring corroboration before restart (defect 2, option (b)
chosen): mass STALE_DATA in this system is fed from upstream.  When the hub
or Bybit stops delivering fresh frames, restarting the bot re-fetches the
same stale data, destroys warm in-process state, and -- worst -- makes the
incident look "auto-recovered" while the real provider fault keeps starving
deliveries.  So restart now carries a burden of proof: the journal must show
a bot-local failure signature (scan-deadline pressure, frame-store
checkpoint failure, uncaught exception) AND must not show a provider/hub
transport fault in the same 5-minute lookback.  Every stale-but-not-
restartable case produces a loud alert instead.  Note on the other candidate
signals: provider error rate and frame-store fallback RATE are recorded in
the lane SQLite evidence DB, not the journal, and this watchdog deliberately
stays journal-only (it must not open production databases).  The trigger
thresholds themselves (STALE_THRESHOLD / window logic / COOLDOWN) are
unchanged by operator constraint.

RATIONALE for the precondition change (defect 1): the old guard required
NRestarts == 0, which is cumulative since unit load -- after ANY restart,
including a planned deployment, autoheal silently and permanently disabled
itself.  The new precondition means "not currently crash-looping":
ActiveState=active + SubState=running + fewer than CRASH_LOOP_RESTARTS
"Started ..." unit events in the last CRASH_LOOP_WINDOW (a bounded
restart-rate check measured from the journal, so a deploy costs one event
and clears itself once the window passes).  NRestarts is still reported for
observability but gates nothing.  Every decision NOT to act is logged with
its reasons -- there is no silent path (unknown decisions raise).

RULE 4 (continuity latch, added by operator directive 2026-09-23): per-
symbol alert-only watchdog on expired UNREPAIRED candle-continuity gaps
(stale reason CONTINUITY_UNHEALTHY): pages at >=3 such gaps for one symbol
within 60 scan windows, or at ANY single expired gap missing >51 min (3x
the ~17-min tracker capacity; the single-gap case catches the real SAGA
pattern in one shot).  It exists because the 2026-09 continuity incidents
(SAGAUSDT ~4h17m, SOLUSDT ~5h54m + ~7h18m, ALLOUSDT ~1h32m) stayed
invisible to the global 0.60 share rule -- fleet share was ~2%.  It NEVER
restarts anything and stays inert (logged every pass as
`continuity-unavailable`) until the bot-side stale_reason/gaps_expired
persistence lands -- see CONTINUITY_* constants and latch-fix workstream.

Modes:
  (no args)      normal: evaluate live system, act (restart + state write).
  --dry-run      evaluate the live system, print the report, do not restart,
                 do not write state, do not call logger (zero side effects).
  --selftest     offline decision-logic scenarios, no host access at all.
  --state-file   override the cooldown state path (tests).
  --now          pin wall clock as int seconds (tests).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# --- thresholds kept EXACTLY as before (operator constraint) --------------
STATE = Path("/run/short-bot-autoheal.json")
COOLDOWN = 900
STALE_THRESHOLD = 0.60
WINDOW_LIMIT = 3
LOOKBACK = "5 minutes ago"
UNITS = {
    "A": "short-telegram-bot-lite.service",
    "B": "short-telegram-bot-lite-b.service",
}

# --- defect 1: bounded restart-rate precondition ---------------------------
CRASH_LOOP_WINDOW = "30 minutes ago"
CRASH_LOOP_RESTARTS = 2  # >=2 starts in window = crash loop; 1 = deploy/self
STARTED_RE = re.compile(r"^Started ")  # journalctl -u UNIT -o cat, systemd msg

# --- defect 2: corroboration signals (same 5-min lookback as windows) ------
# Any hit vetoes a restart: upstream fault; a restart would mask it.
PROVIDER_FAULT_PATTERNS = {
    "hub-reconnect": re.compile(r"MarketDataHub reconnect attempt"),
    "hub-stale": re.compile(r"MarketDataHub stale \|"),
    "hub-subscriptions-rejected": re.compile(r"MarketDataHub subscriptions rejected"),
}
# Sequence/timestamp regressions are routine background noise (~110/day in
# normal ops); only a burst inside the lookback counts as an upstream fault.
PROVIDER_REGRESSION_RE = re.compile(r"Bybit ticker (?:sequence|timestamp) regression")
PROVIDER_REGRESSION_VETO = 10
# Any hit corroborates a bot-local fault that a restart can plausibly clear.
LOCAL_FAULT_PATTERNS = {
    "frame-store-checkpoint-failed": re.compile(r"frame store checkpoint failed"),
    "shadow-warmup-failed": re.compile(r"shadow frame-store warmup failed"),
    "uncaught-exception": re.compile(r"Traceback \(most recent call last\)"),
}
# Scans blowing their own deadline is the primary local stall signature; it
# rides inside the "Scan window complete" statuses dict already parsed.
DEADLINE_STATUS = "DEADLINE_EXCEEDED"
LOCAL_DEADLINE_SHARE = 0.10  # >=10% of one window's scheduled scans past
                             # deadline in the last two windows = corroborated

# --- RULE 4 (continuity latch): per-symbol CONTINUITY_UNHEALTHY expired ----
# gaps.  WHY IT EXISTS: a symbol's candle continuity can latch (GAP_DETECTED)
# and every scheduled scan for it then fails STALE_CONTINUITY indefinitely
# (measured: SAGAUSDT ~4h17m, SOLUSDT ~5h54m + ~7h18m, ALLOUSDT ~1h32m --
# ~19 hours of scanning lost) while the GLOBAL STALE_DATA share stays ~2%
# and degraded() -- correctly -- never fires.  The reason-keying is
# load-bearing: at every WS reconnect the whole fleet goes non-actionable
# for 1-2 windows with BLIP reasons (EPOCH_INVALIDATED / NO_TICKER / WARMING
# / INCOMPLETE_RING / CANDLE_STALE); per the latch-fix contract those rows
# NEVER emit `gap expired unrepaired`, so only true CONTINUITY_UNHEALTHY
# expirations feed this rule and reconnects cannot page.  Only expired
# UNREPAIRED gaps count -- a gap that healed itself never logs.  ALERT-ONLY:
# zero restart authority, ever.  Thresholds from the post-incident review,
# NOT arbitrary: >=3 gaps/symbol/60 windows (the originally proposed
# >=5/60 would have MISSED the real SOL run: ~1 gap-log/hour + ~20 parity
# conflicts/hour), or ANY single expired gap missing > 3x tracker capacity
# (~51 min; that single case covers the real SAGA incident in one shot).
CONTINUITY_LOOKBACK = "90 minutes ago"  # fetch frame holding >=60 1-min windows
CONTINUITY_STALE_REASON = "CONTINUITY_UNHEALTHY"
CONTINUITY_GAP_ALERT = 3        # expired unrepaired gaps for ONE symbol
CONTINUITY_GAP_WINDOWS = 60     # ... within the last 60 completed windows
CONTINUITY_CAPACITY_MIN = 17    # frame-store tracker capacity, ~17 minutes
CONTINUITY_RANGE_FACTOR = 3     # single gap missing >3x capacity pages at once
CONTINUITY_RANGE_ALERT_MIN = CONTINUITY_CAPACITY_MIN * CONTINUITY_RANGE_FACTOR
# Parsing seam -- the latch-fix companion journal lines are the ONLY
# accepted input (journal-only policy: the watchdog never opens the lane
# SQLite evidence DBs, whose details_json/evidence_json carry the same
# facts).  Shapes + semantics confirmed by latch-fix 2026-09-23: UNITS ARE
# MINUTES; exactly one raw WARNING per expired UNREPAIRED gap (emitted
# unconditionally in GapTracker._expire_gap); the two `continuity alert |`
# companions only on threshold crossings; `closed candle gap detected`
# lines are DETECTION events and must NEVER be counted here.
GAP_EVENT_RE = re.compile(r"gap expired unrepaired symbol=([A-Z0-9._\-]+)(.*)$")
CONTINUITY_ALERT_RE = re.compile(
    r"continuity alert \| symbol=([A-Z0-9._\-]+) reason=([A-Z_]+)(.*)$"
)
ALERT_EXPIRED_RE = re.compile(r"expired=(\d+)")
ALERT_MISSING_RE = re.compile(r"missing=(\d+)")
# Deployed marker (latch-fix): GapTracker's constructor logs this once per
# process start.  Present => rule 4 is armed (quiet-but-live); absent =>
# the data cannot exist => degrade without firing.  Lookup throttled via
# the state file so the wider lookback is paid at most once per TTL.
CONTINUITY_ARMED_RE = re.compile(r"continuity gap expiry armed")
CONTINUITY_ARMED_LOOKBACK = "24 hours ago"
CONTINUITY_ARMED_TTL_SEC = 21600

# decision -> (journal priority, needs_action_exit_1)
# Every possible decision MUST appear here; anything unknown raises.
DECISION_CLASS = {
    "healthy": ("info", False),
    "no-data": ("info", False),
    "inactive": ("err", True),
    "provider-fault": ("err", True),
    "stale-unverified": ("err", True),
    "crash-looping": ("err", True),
    "cooldown": ("warning", False),
    "restart": ("err", False),  # loud: an automated restart of a live bot
    #            deserves an err-priority journal line
    "restart-failed": ("err", True),
    # rule 4 (continuity latch) -- ALERT-ONLY decisions; "restart" is not a
    # reachable outcome from this rule regardless of severity.
    "continuity-ok": ("info", False),
    "continuity-unavailable": ("info", False),
    "continuity-latch": ("err", True),
}

WINDOW_RE = re.compile(
    r"Scan window complete .*?scheduled=(\d+).*?statuses=\{([^}]*)\}"
)
STATUS_RE = re.compile(r"ScanTerminalStatus\.([A-Z_]+).*?>:\s*(\d+)")


class RealSystem:
    """Host interaction seam; --selftest injects FakeSystem instead."""

    @staticmethod
    def _run(*args: str) -> str:
        return subprocess.run(
            args, text=True, capture_output=True, timeout=30, check=False
        ).stdout

    def journal(self, unit: str, since: str) -> str:
        return self._run(
            "journalctl", "-u", unit, "--since", since, "--no-pager", "-o", "cat"
        )

    def prop(self, unit: str, name: str) -> str:
        out = self._run("systemctl", "show", unit, f"-p{name}")
        return out.split("=", 1)[1].strip() if "=" in out else ""

    def restart(self, unit: str) -> int:
        return subprocess.run(
            ("systemctl", "restart", unit), check=False, timeout=60
        ).returncode

    def log(self, priority: str, text: str) -> None:
        subprocess.run(
            ("logger", "-t", "short-bot-autoheal", "-p", priority, text),
            check=False,
            timeout=15,
        )


def parse_windows(text: str) -> list[dict[str, int]]:
    """Same parsing as the original watchdog (behaviour kept)."""
    result: list[dict[str, int]] = []
    for line in text.splitlines():
        match = WINDOW_RE.search(line)
        if not match:
            continue
        scheduled = int(match.group(1))
        counts = {name: int(value) for name, value in STATUS_RE.findall(match.group(2))}
        if scheduled > 0:
            counts["scheduled"] = scheduled
            result.append(counts)
    return result[-WINDOW_LIMIT:]


def degraded(windows: list[dict[str, int]]) -> bool:
    if len(windows) < 2:
        return False
    tail = windows[-2:]
    return all(w.get("STALE_DATA", 0) / w["scheduled"] >= STALE_THRESHOLD for w in tail)


def shares(windows: list[dict[str, int]]) -> list[str]:
    return [
        f"{w.get('STALE_DATA', 0)}/{w['scheduled']} STALE_DATA" +
        (f"/{w.get(DEADLINE_STATUS, 0)} DEADLINE_EXCEEDED" if w.get(DEADLINE_STATUS) else "")
        for w in windows[-2:]
    ]


def collect(sysobj: RealSystem, unit: str) -> tuple[list[dict[str, int]], dict]:
    text = sysobj.journal(unit, LOOKBACK)
    windows = parse_windows(text)
    provider_faults = [
        name for name, rx in PROVIDER_FAULT_PATTERNS.items() if rx.search(text)
    ]
    regressions = len(PROVIDER_REGRESSION_RE.findall(text))
    if regressions >= PROVIDER_REGRESSION_VETO:
        provider_faults.append(f"ticker-regression-storm({regressions})")
    local_faults = [
        name for name, rx in LOCAL_FAULT_PATTERNS.items() if rx.search(text)
    ]
    for w in windows[-2:]:
        share = w.get(DEADLINE_STATUS, 0) / w["scheduled"]
        if share >= LOCAL_DEADLINE_SHARE:
            local_faults.append(f"scan-deadline-pressure({share:.0%})")
            break
    starts = sum(
        1
        for line in sysobj.journal(unit, CRASH_LOOP_WINDOW).splitlines()
        if STARTED_RE.search(line)
    )
    return windows, {
        "provider_faults": provider_faults,
        "regressions": regressions,
        "local_faults": local_faults,
        "restart_starts": starts,
    }


def parse_continuity_events(text: str) -> tuple[int, list[dict]]:
    """Return (completed_scan_windows, events).  THE single swap seam for
    the evidence shape (latch-fix format, confirmed 2026-09-23; missing/
    limit UNITS ARE MINUTES; journal-only by watchdog policy -- the DB
    columns details_json/evidence_json carry the same facts but the
    watchdog must never open production databases).

    Recognized lines:
      * `gap expired unrepaired symbol=<SYM> expected=<iso> observed=<iso>
        missing=<n>` -- exactly ONE raw WARNING per expired UNREPAIRED gap
        with stale_reason CONTINUITY_UNHEALTHY, <n> = missing minutes.
        Healed gaps, blip reasons (EPOCH_INVALIDATED / NO_TICKER / WARMING
        / INCOMPLETE_RING / CANDLE_STALE -- everything a WS reconnect
        produces) and mere DETECTION lines (`closed candle gap detected`)
        NEVER match this pattern: that is what keeps the rule
        reason-keyed, not stale-share-keyed, and reconnect-proof.
      * `continuity alert | symbol=<SYM> reason=EXPIRED_GAP_RATE expired=<n>
        windows=<w>` -- the bot's own rate verdict per crossing episode
        (ExpiredGapRateAlerter hysteresis); we re-verify n ourselves.
      * `continuity alert | symbol=<SYM> reason=EXPIRED_GAP_SPAN missing=<n>
        limit=<l>` -- the bot's span verdict per qualifying expiry.
    """
    windows = 0
    events: list[dict] = []
    for line in text.splitlines():
        if WINDOW_RE.search(line):
            windows += 1
            continue
        match = GAP_EVENT_RE.search(line)
        if match:
            value = ALERT_MISSING_RE.search(match.group(2))
            events.append({"kind": "gap", "symbol": match.group(1),
                           "at_window": windows, "n": 1,
                           "missing_min": int(value.group(1)) if value else 0})
            continue
        match = CONTINUITY_ALERT_RE.search(line)
        if not match:
            continue
        symbol, reason, rest = match.group(1), match.group(2), match.group(3)
        if reason == "EXPIRED_GAP_RATE":
            value = ALERT_EXPIRED_RE.search(rest)
            events.append({"kind": "rate", "symbol": symbol, "at_window": windows,
                           "n": int(value.group(1)) if value else 0})
        elif reason == "EXPIRED_GAP_SPAN":
            value = ALERT_MISSING_RE.search(rest)
            events.append({"kind": "span", "symbol": symbol, "at_window": windows,
                           "n": int(value.group(1)) if value else 0})
    return windows, events


def evaluate_continuity(
    sysobj: RealSystem, unit: str, now: int, state: dict
) -> tuple[str, list[str], dict]:
    """RULE 4: per-symbol continuity-latch detection.  ALERT-ONLY -- zero
    restart authority, ever: a restart cannot heal a latched continuity
    record; that is precisely the 19-hour failure mode, and only an
    operator / backfill closes it.

    Fires on either review-derived threshold (operator directive
    2026-09-23; NOT arbitrary):
      >= CONTINUITY_GAP_ALERT expired UNREPAIRED gaps for one symbol within
      the last CONTINUITY_GAP_WINDOWS completed scan windows (raw gap
      events AND the bot's EXPIRED_GAP_RATE verdict both count), or
      ANY single expired gap missing > CONTINUITY_RANGE_ALERT_MIN minutes
      (raw missing= or the EXPIRED_GAP_SPAN companion).

    No events in the frame -> the latch-fix startup marker decides the
    label: `continuity gap expiry armed` seen in the last
    CONTINUITY_ARMED_LOOKBACK => armed and quiet ("continuity-ok"); not
    seen => the data does not exist yet and the rule degrades WITHOUT
    firing ("continuity-unavailable").  The rule NEVER pages from absence
    either way, and logs decision + reasons every pass.
    """
    windows, events = parse_continuity_events(
        sysobj.journal(unit, CONTINUITY_LOOKBACK)
    )
    if not events:
        cache = state.get("continuity_armed")
        if not isinstance(cache, dict):
            cache = {}
            state["continuity_armed"] = cache
        entry = cache.get(unit) if isinstance(cache.get(unit), dict) else None
        if not entry or now - int(entry.get("ts", 0)) > CONTINUITY_ARMED_TTL_SEC:
            armed = bool(CONTINUITY_ARMED_RE.search(
                sysobj.journal(unit, CONTINUITY_ARMED_LOOKBACK)
            ))
            entry = {"deployed": armed, "ts": now}
            cache[unit] = entry
        if entry.get("deployed"):
            return "continuity-ok", [
                "rule 4 ARMED (latch-fix startup marker 'continuity gap "
                "expiry armed' observed) and zero expired-gap events in "
                f"the {CONTINUITY_LOOKBACK} frame: nothing latched",
                f"{windows} scan windows seen",
                "alert only -- this rule never restarts",
            ], {"scan_windows_seen": windows, "events": 0, "armed": True}
        return "continuity-unavailable", [
            f"no expired-gap / continuity-alert events in the "
            f"{CONTINUITY_LOOKBACK} frame ({windows} scan windows seen) "
            f"and no 'continuity gap expiry armed' startup marker within "
            f"{CONTINUITY_ARMED_LOOKBACK}: bot-side continuity data does "
            "not exist yet (latch-fix undeployed) -- the rule degrades "
            "WITHOUT firing, never pages from absence, never guesses from "
            "generic STALE shares",
        ], {"scan_windows_seen": windows, "events": 0, "armed": False}
    cutoff = max(0, windows - CONTINUITY_GAP_WINDOWS)
    recent = [e for e in events if e["at_window"] > cutoff]
    gap_counts: dict[str, int] = {}
    for e in recent:
        if e["kind"] == "gap":
            gap_counts[e["symbol"]] = gap_counts.get(e["symbol"], 0) + e["n"]
    breaches: list[str] = []
    for symbol in sorted(gap_counts):
        if gap_counts[symbol] >= CONTINUITY_GAP_ALERT:
            breaches.append(
                f"{symbol}: {gap_counts[symbol]} expired UNREPAIRED gaps "
                f"within the last {min(windows, CONTINUITY_GAP_WINDOWS)} "
                f"scan windows (>= {CONTINUITY_GAP_ALERT})"
            )
    for e in recent:
        if (e["kind"] == "gap"
                and e.get("missing_min", 0) > CONTINUITY_RANGE_ALERT_MIN):
            breaches.append(
                f"{e['symbol']}: raw expired gap missing "
                f"{e['missing_min']} min (> {CONTINUITY_RANGE_FACTOR}x "
                f"tracker capacity ~{CONTINUITY_CAPACITY_MIN} min)"
            )
        elif e["kind"] == "rate" and e["n"] >= CONTINUITY_GAP_ALERT:
            breaches.append(
                f"{e['symbol']}: bot verdict EXPIRED_GAP_RATE expired="
                f"{e['n']} (>= {CONTINUITY_GAP_ALERT})"
            )
        elif e["kind"] == "span" and e["n"] > CONTINUITY_RANGE_ALERT_MIN:
            breaches.append(
                f"{e['symbol']}: span companion reports missing {e['n']} "
                f"min (> {CONTINUITY_RANGE_FACTOR}x tracker capacity "
                f"~{CONTINUITY_CAPACITY_MIN} min)"
            )
    stats = {
        "scan_windows_seen": windows,
        "events_in_frame": len(recent),
        "expired_gaps_per_symbol": gap_counts,
    }
    detail = (
        f"{windows} scan windows, {len(events)} continuity events in the "
        f"lookback, {len(recent)} inside the last "
        f"{CONTINUITY_GAP_WINDOWS}-window frame; per-symbol expired "
        f"unrepaired gaps={gap_counts or '{}'}"
    )
    if breaches:
        return "continuity-latch", [
            "SYMBOL CONTINUITY LATCHED (stale_reason "
            + CONTINUITY_STALE_REASON + "): "
            + " || ".join(dict.fromkeys(breaches)),
            detail,
            "ALERT ONLY -- this rule never restarts; operator action / "
            "backfill required (the symbol stays blind to scheduled scans "
            "until continuity heals)",
        ], stats
    return "continuity-ok", [
        "no symbol reached "
        f"{CONTINUITY_GAP_ALERT} expired unrepaired gaps in the last "
        f"{min(windows, CONTINUITY_GAP_WINDOWS)} windows and no single gap "
        f"missed more than {CONTINUITY_RANGE_ALERT_MIN} min",
        detail,
    ], stats


def decide(
    props: dict[str, str],
    windows: list[dict[str, int]],
    signals: dict,
    now: int,
    last_restart_ts: int,
) -> tuple[str, list[str]]:
    """Pure decision logic.  ALWAYS returns (decision, non-empty reasons)."""
    active = props.get("ActiveState") == "active" and props.get("SubState") == "running"
    if not active:
        return "inactive", [
            f"ActiveState={props.get('ActiveState', '?') or '?'} "
            f"SubState={props.get('SubState', '?') or '?'}"
        ]
    if len(windows) < 2:
        return "no-data", [
            f"only {len(windows)} parseable scan windows in {LOOKBACK}"
        ]
    if not degraded(windows):
        return "healthy", [f"last two windows: {'; '.join(shares(windows))}"]
    stale_desc = f"mass STALE_DATA over last two windows: {'; '.join(shares(windows))}"
    if signals["provider_faults"]:
        return "provider-fault", [
            stale_desc,
            "upstream fault signatures: " + ", ".join(signals["provider_faults"]),
            "restart withheld: it cannot fix an upstream provider/hub fault "
            "and would mask it from the journal",
        ]
    if not signals["local_faults"]:
        return "stale-unverified", [
            stale_desc,
            "no bot-local failure signature (deadline pressure / frame-store "
            "checkpoint failure / uncaught exception) in the lookback",
            "restart withheld: burden of proof for a restart is unmet",
        ]
    if signals["restart_starts"] >= CRASH_LOOP_RESTARTS:
        return "crash-looping", [
            stale_desc,
            "local corroboration: " + ", ".join(signals["local_faults"]),
            f"{signals['restart_starts']} unit starts in last {CRASH_LOOP_WINDOW} "
            f"(>= {CRASH_LOOP_RESTARTS}): service is crash-looping; a restart "
            "would feed the loop",
        ]
    if now - last_restart_ts < COOLDOWN:
        return "cooldown", [
            stale_desc,
            "local corroboration: " + ", ".join(signals["local_faults"]),
            f"last autoheal restart {now - last_restart_ts}s ago "
            f"(cooldown {COOLDOWN}s)",
        ]
    return "restart", [
        stale_desc,
        "local corroboration: " + ", ".join(signals["local_faults"]),
        "no provider/hub fault in lookback",
        f"{signals['restart_starts']} unit starts in last {CRASH_LOOP_WINDOW} "
        "(below crash-loop bound)",
        "cooldown elapsed",
    ]


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def save_state(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, sort_keys=True))
    os.replace(tmp, path)


def run_pass(
    sysobj: RealSystem, now: int, state: dict, dry_run: bool
) -> tuple[dict, int]:
    """Evaluate every lane; act per DECISION CONTRACT.  Returns (report, exit)."""
    if not isinstance(state.get("last_restart_by_unit"), dict):
        state["last_restart_by_unit"] = {}
    last_by_unit = state["last_restart_by_unit"]
    report: dict = {
        "ts": now,
        "dry_run": dry_run,
        "units": {},
        "restarted": [],
        "cooldown_remaining": {},
    }
    exit_code = 0
    for lane, unit in UNITS.items():
        props = {p: sysobj.prop(unit, p) for p in ("ActiveState", "SubState", "NRestarts")}
        windows, signals = collect(sysobj, unit)
        last_ts = int(last_by_unit.get(unit, 0) or 0)
        decision, reasons = decide(props, windows, signals, now, last_ts)
        if decision not in DECISION_CLASS:  # pragma: no cover - fail loud
            raise RuntimeError(f"uncatalogued decision {decision!r} for {unit}")
        if decision == "restart":
            if dry_run:
                reasons = reasons + ["dry-run: restart suppressed, state not written"]
            else:
                rc = sysobj.restart(unit)
                if rc == 0:
                    last_by_unit[unit] = now
                    report["restarted"].append(unit)
                else:
                    decision = "restart-failed"
                    reasons = reasons + [f"systemctl restart rc={rc}"]
        elif last_ts and now - last_ts < COOLDOWN:
            report["cooldown_remaining"][unit] = COOLDOWN - (now - last_ts)
        priority, needs_action = DECISION_CLASS[decision]
        if needs_action:
            exit_code = 1
        line = (
            f"AUTOHEAL lane={lane} unit={unit} decision={decision} "
            f"nrestarts={props.get('NRestarts', '?')} "
            f"starts30m={signals['restart_starts']} "
            + " ".join(f"reason[{i}]={r}" for i, r in enumerate(reasons))
        )
        if dry_run:
            print(f"[dry-run][{priority}] {line}")
        else:
            sysobj.log(priority, line)
        report["units"][lane] = {
            "unit": unit,
            "decision": decision,
            "reasons": reasons,
            "journal_priority": priority,
            "active": props.get("ActiveState"),
            "sub": props.get("SubState"),
            "nrestarts": props.get("NRestarts"),
            "starts_30m": signals["restart_starts"],
            "regressions_5m": signals["regressions"],
            "windows": windows,
        }

        # --- RULE 4: continuity-latch detection (ALERT ONLY; zero restart
        # authority -- evaluated every pass on the 60+ window evidence frame).
        c_decision, c_reasons, c_stats = evaluate_continuity(sysobj, unit, now, state)
        if c_decision not in DECISION_CLASS:  # pragma: no cover - fail loud
            raise RuntimeError(f"uncatalogued continuity decision {c_decision!r}")
        c_priority, c_needs_action = DECISION_CLASS[c_decision]
        if c_needs_action:
            exit_code = 1
        c_line = (
            f"AUTOHEAL lane={lane} unit={unit} rule=continuity "
            f"decision={c_decision} action=alert-only "
            + " ".join(f"reason[{i}]={r}" for i, r in enumerate(c_reasons))
        )
        if dry_run:
            print(f"[dry-run][{c_priority}] {c_line}")
        else:
            sysobj.log(c_priority, c_line)
        report["units"][lane]["continuity"] = {
            "decision": c_decision,
            "reasons": c_reasons,
            "evidence": c_stats,
        }
    return report, exit_code


# --------------------------------------------------------------------------
# Offline self-test (no host access at all)
# --------------------------------------------------------------------------

class FakeSystem(RealSystem):
    """Canned per-unit journals: {unit: {"coverage"|"crash"|"continuity"|"armed": text}}."""

    def __init__(self, props=None, journals=None, restart_rc=0):
        self.props = props or {}
        self.journals = journals or {}
        self.restart_rc = restart_rc
        self.calls: list = []

    def journal(self, unit, since):
        self.calls.append(("journal", unit, since))
        texts = self.journals.get(unit, {})
        if since == CRASH_LOOP_WINDOW:
            return texts.get("crash", "")
        if since == CONTINUITY_LOOKBACK:
            return texts.get("continuity", "")
        if since == CONTINUITY_ARMED_LOOKBACK:
            return texts.get("armed", "")
        return texts.get("coverage", "")

    def prop(self, unit, name):
        return str(self.props.get(unit, {}).get(name, ""))

    def restart(self, unit):
        self.calls.append(("restart", unit))
        return self.restart_rc

    def log(self, priority, text):
        self.calls.append(("log", priority, text))


def window_line(scheduled: int, stale: int, deadline: int = 0) -> str:
    parts = []
    if stale:
        parts.append(f"<ScanTerminalStatus.STALE_DATA: 'STALE_DATA'>: {stale}")
    if deadline:
        parts.append(
            f"<ScanTerminalStatus.DEADLINE_EXCEEDED: 'DEADLINE_EXCEEDED'>: {deadline}"
        )
    rest = scheduled - stale - deadline
    if rest > 0:
        parts.append(f"<ScanTerminalStatus.SCANNED_OK: 'SCANNED_OK'>: {rest}")
    duration = 2800.0 + deadline * 900.0
    return (
        "Scan window complete | window=20260923T2200Z-1 "
        f"scheduled={scheduled} duration_ms={duration:.1f} "
        "statuses={" + ", ".join(parts) + "}"
    )


def crash_journal(n_starts: int) -> str:
    return "".join(
        "Started short-telegram-bot-lite-b.service - Telegram Short Signal Bot Lite LANE-B.\n"
        for _ in range(n_starts)
    )


def gap_event_line(symbol: str, missing: int = 4) -> str:
    """The latch-fix raw format exactly (missing= is MINUTES)."""
    return (
        "2026-09-23 22:00:00,000 | WARNING | app.market_data.hub | "
        f"gap expired unrepaired symbol={symbol} "
        f"expected=2026-09-23T21:00:00Z observed=2026-09-23T21:05:00Z "
        f"missing={missing}"
    )


def rate_line(symbol: str, expired: int, windows: int = 60) -> str:
    return (
        f"continuity alert | symbol={symbol} reason=EXPIRED_GAP_RATE "
        f"expired={expired} windows={windows}"
    )


def span_line(symbol: str, missing: int, limit: int = 51) -> str:
    return (
        f"continuity alert | symbol={symbol} reason=EXPIRED_GAP_SPAN "
        f"missing={missing} limit={limit}"
    )


def fresh_state() -> dict:
    return {"last_restart_by_unit": {}}


def recent_state(seconds_ago: int) -> dict:
    return {"last_restart_by_unit": {UNITS["B"]: NOW - seconds_ago}}


FRESH_PROPS = {
    u: {"ActiveState": "active", "SubState": "running", "NRestarts": "0"}
    for u in UNITS.values()
}
HEALTHY = (
    window_line(50, 0) + "\n" + window_line(50, 0) + "\n" + window_line(50, 0)
)
MASS_STALE = window_line(50, 45) + "\n" + window_line(50, 44)
MASS_STALE_DEADLINE = window_line(50, 45, 6) + "\n" + window_line(50, 44, 5)


def lanes(b_coverage: str = "", b_crash: str = "", b_continuity: str = "",
          b_armed: str = "") -> dict:
    """Lane A always fresh/quiet; lane B gets the scenario texts per rule."""
    return {
        UNITS["A"]: {"coverage": HEALTHY, "crash": "", "continuity": "", "armed": ""},
        UNITS["B"]: {"coverage": b_coverage, "crash": b_crash,
                     "continuity": b_continuity, "armed": b_armed},
    }


NOW = 1_000_000

STAMPED_NOW = {"last_restart_by_unit": {UNITS["B"]: NOW}}

SCENARIOS: list[dict] = [
    {
        "name": "1 healthy: fresh windows -> no action; rule 4 logs its data dependency",
        "fake": lambda: FakeSystem(props=FRESH_PROPS, journals=lanes(b_coverage=HEALTHY)),
        "expect": {"decision": "healthy", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 0},
    },
    {
        "name": "2 no-data: bot logging nothing -> logged decision, no action",
        "fake": lambda: FakeSystem(props=FRESH_PROPS, journals=lanes(b_coverage="")),
        "expect": {"decision": "no-data", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 0},
    },
    {
        "name": "3 inactive: mass-stale journal but unit not running -> ALERT, no restart",
        "fake": lambda: FakeSystem(
            props={
                **FRESH_PROPS,
                UNITS["B"]: {"ActiveState": "inactive", "SubState": "dead", "NRestarts": "3"},
            },
            journals=lanes(b_coverage=MASS_STALE_DEADLINE),
        ),
        "expect": {"decision": "inactive", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "4 WS RECONNECT, whole fleet briefly non-actionable: coverage=provider-fault "
                "NO restart; rule 4 must NOT page (blip reasons never emit gap events)",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=MASS_STALE_DEADLINE
                + "\nMarketDataHub reconnect attempt | backoff_index=0\n",
                # 62 completed windows, 100% per-symbol stale during the
                # reconnect blip, ZERO `gap expired unrepaired` lines ->
                # rule 4 has no events -> cannot page.
                b_continuity="\n".join(
                    [window_line(50, 50)] * 2 + [window_line(50, 0)] * 60
                ),
            ),
        ),
        "expect": {"decision": "provider-fault", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "5 stale + provider regression storm -> ALERT, NO restart",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=MASS_STALE_DEADLINE
                + "\n" + "Bybit ticker sequence regression for X\n" * 12
            ),
        ),
        "expect": {"decision": "provider-fault", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "6 stale without local corroboration -> ALERT 'stale-unverified', NO restart",
        "fake": lambda: FakeSystem(props=FRESH_PROPS, journals=lanes(b_coverage=MASS_STALE)),
        "expect": {"decision": "stale-unverified", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "7 crash-looping service (3 starts/30m, corroborated) -> ALERT, no restart, "
                "defect 1: decision + reason are never silent",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(b_coverage=MASS_STALE_DEADLINE, b_crash=crash_journal(3)),
        ),
        "expect": {"decision": "crash-looping", "continuity": "continuity-unavailable",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "8 cooldown active (restarted 100s ago, corroborated) -> logged, no restart, stamp kept",
        "fake": lambda: FakeSystem(props=FRESH_PROPS, journals=lanes(b_coverage=MASS_STALE_DEADLINE)),
        "state": lambda: recent_state(100),
        "expect": {"decision": "cooldown", "continuity": "continuity-unavailable",
                   "restarts": [], "state": recent_state(100), "exit": 0},
    },
    {
        "name": "9 genuinely stale + corroborated (deadline pressure), no provider fault, "
                "NRestarts=7 tolerated (defect 1) -> RESTART",
        "fake": lambda: FakeSystem(
            props={**FRESH_PROPS, UNITS["B"]: {"ActiveState": "active", "SubState": "running",
                                               "NRestarts": "7"}},
            journals=lanes(b_coverage=MASS_STALE_DEADLINE),
        ),
        "expect": {"decision": "restart", "continuity": "continuity-unavailable",
                   "restarts": [UNITS["B"]], "state": STAMPED_NOW, "exit": 0},
    },
    {
        "name": "10 restart corroboration via frame-store checkpoint failure (no deadline) -> RESTART",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(b_coverage=MASS_STALE
                           + "\nframe store checkpoint failed | error=OSError\n"),
        ),
        "expect": {"decision": "restart", "continuity": "continuity-unavailable",
                   "restarts": [UNITS["B"]], "state": STAMPED_NOW, "exit": 0},
    },
    {
        "name": "11 systemctl restart fails -> restart-failed ALERT, exit 1, stamp NOT written",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS, journals=lanes(b_coverage=MASS_STALE_DEADLINE), restart_rc=1
        ),
        "expect": {"decision": "restart-failed", "continuity": "continuity-unavailable",
                   "restarts": [UNITS["B"]], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "12 RULE4 SOL pattern: 3 expired unhealed gaps on one symbol within 60 windows, "
                "fleet share normal -> continuity-latch ALERT, NO restart",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_continuity="\n".join(
                    [window_line(50, 0)] * 62 + [gap_event_line("SOLUSDT")] * 3
                ),
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-latch",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "13 RULE4 SAGA pattern: single expired gap missing 257 min -> instant latch, NO restart",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_continuity="\n".join(
                    [window_line(50, 0)] * 61
                    + [gap_event_line("SAGAUSDT"), span_line("SAGAUSDT", 257)]
                ),
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-latch",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "14 RULE4 events present, both bounds respected (2 gaps; span exactly 51 min) -> continuity-ok",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_continuity="\n".join(
                    [window_line(50, 0)] * 62
                    + [gap_event_line("ALLOUSDT"),
                       span_line("ALLOUSDT", 51),
                       gap_event_line("ALLOUSDT")]
                ),
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-ok",
                   "restarts": [], "state": fresh_state(), "exit": 0},
    },
    {
        "name": "15 RULE4 windowing: 3 gap events but the oldest falls OUTSIDE the last 60 windows -> continuity-ok",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_continuity="\n".join(
                    [window_line(50, 0)] * 3 + [gap_event_line("THETASTD")]
                    + [window_line(50, 0)] * 60 + [gap_event_line("THETASTD")] * 2
                ),
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-ok",
                   "restarts": [], "state": fresh_state(), "exit": 0},
    },
    {
        "name": "16 RULE4 boundary: single gap missing 52 min (>51) -> continuity-latch, NO restart",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_continuity="\n".join(
                    [window_line(50, 0)] * 61
                    + [gap_event_line("XUSDT", 52), span_line("XUSDT", 52)]
                ),
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-latch",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "17 RULE4 bot verdict alone trips it: EXPIRED_GAP_RATE expired=3 while my raw count is 1 -> latch",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_continuity="\n".join(
                    [window_line(50, 0)] * 62
                    + [gap_event_line("1000PEPEUSDT"),
                       rate_line("1000PEPEUSDT", 3)]
                ),
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-latch",
                   "restarts": [], "state": fresh_state(), "exit": 1},
    },
    {
        "name": "18 RULE4 deployed & quiet: armed startup marker present, zero gap events -> continuity-ok (NOT 'unavailable')",
        "fake": lambda: FakeSystem(
            props=FRESH_PROPS,
            journals=lanes(
                b_coverage=HEALTHY,
                b_armed="2026-09-23 08:00:00,000 | INFO | app.market_data.hub | "
                        "continuity gap expiry armed | capacity=17 span_alert_factor=3",
            ),
        ),
        "expect": {"decision": "healthy", "continuity": "continuity-ok",
                   "restarts": [], "state": fresh_state(), "exit": 0},
    },
]


def selftest() -> int:
    failures = 0
    for sc in SCENARIOS:
        fake = sc["fake"]()
        state = sc["state"]() if "state" in sc else fresh_state()
        report, exit_code = run_pass(fake, NOW, state, dry_run=False)
        want = sc["expect"]
        errors: list[str] = []
        ub = report["units"]["B"]
        if ub["decision"] != want["decision"]:
            errors.append(f"coverage decision={ub['decision']} != {want['decision']}")
        cq = ub.get("continuity", {})
        if cq.get("decision") != want["continuity"]:
            errors.append(f"continuity decision={cq.get('decision')} != {want['continuity']}")
        if not ub["reasons"] or not cq.get("reasons"):
            errors.append("SILENT decision (missing logged reasons)")
        logs = [c for c in fake.calls if c[0] == "log"]
        b_cov = [c for c in logs if f"unit={UNITS['B']}" in c[2] and "rule=continuity" not in c[2]]
        b_con = [c for c in logs if f"unit={UNITS['B']}" in c[2] and "rule=continuity" in c[2]]
        if len(b_cov) != 1 or len(b_con) != 1:
            errors.append(
                f"B logged {len(b_cov)} coverage + {len(b_con)} continuity lines (need 1+1)"
            )
        restarts = [c[1] for c in fake.calls if c[0] == "restart"]
        if restarts != want["restarts"]:
            errors.append(f"restarts={restarts} != {want['restarts']}")
        if want["decision"] == "continuity-latch" and want["restarts"]:
            raise AssertionError("rule 4 scenario must stay alert-only")
        if state.get("last_restart_by_unit") != want["state"].get("last_restart_by_unit"):
            errors.append(f"restart-stamp state={state.get('last_restart_by_unit')} != {want['state'].get('last_restart_by_unit')}")
        if exit_code != want["exit"]:
            errors.append(f"exit={exit_code} != {want['exit']}")
        status = "PASS" if not errors else "FAIL"
        failures += bool(errors)
        print(f"{status} {sc['name']}")
        for c in b_cov + b_con:
            print(f"     journal[{c[1]}]: {c[2]}")
        for e in errors:
            print(f"     !! {e}")
    print(f"\nselftest: {len(SCENARIOS) - failures}/{len(SCENARIOS)} scenarios passed; every "
          "scenario logged exactly one coverage decision AND one continuity decision per lane "
          "(no silent paths)")
    return 0 if failures == 0 else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Bounded scan-coverage watchdog for short-telegram-bot-lite."
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="evaluate the live system; no restart, no state write, no logger calls")
    ap.add_argument("--selftest", action="store_true",
                    help="run offline decision-logic scenarios and exit")
    ap.add_argument("--state-file", default=str(STATE))
    ap.add_argument("--now", type=int, default=None)
    args = ap.parse_args(argv)
    if args.selftest:
        return selftest()
    state = load_state(Path(args.state_file))
    now = args.now if args.now is not None else int(time.time())
    sysobj = RealSystem()
    try:
        report, exit_code = run_pass(sysobj, now, state, args.dry_run)
    except Exception as exc:  # noqa: BLE001 - the watchdog must fail LOUD
        sys.stderr.write(f"AUTOHEAL watchdog crashed: {type(exc).__name__}: {exc}\n")
        raise
    if not args.dry_run:
        save_state(Path(args.state_file), state)
    print(json.dumps(report, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
