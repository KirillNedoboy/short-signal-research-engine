#!/usr/bin/env python3
"""ops-alert: turn release-drift measurements into operator Telegram alerts.

Read-only against production.  This script NEVER signals, never restarts,
never writes any bot state, and never imports bot delivery modules.  It
consumes `shared/runtime/release-drift-report.json` (regenerated every
15 min by short-bot-release-drift.timer), the watchdog journal lines
written by tools/short_bot_autoheal.py, and a private dedup state file.

Destination gate (hard project rule):
  Operational noise may only go to the dedicated ops channel: env
  ALERTS_CHAT_ID (the same variable the bot's config loader maps to
  `alerts_chat_id`).  There is deliberately NO fallback to SIGNAL_CHAT_ID:
  if ALERTS_CHAT_ID (or TELEGRAM_TOKEN) is unset, alerts are appended to
  --alert-file, echoed to the journal (stdout under systemd), and the exact
  one-line enable instruction is printed.

Delivery path: direct HTTPS call to {api-url}/bot<TOKEN>/sendMessage using
stdlib urllib only -- a separate minimal sender, never the bot's delivery
pipeline, so a notifier message can not be confused with a signal.
On transport failure the alert stays pending in state (last_sent is not
advanced, recovery entries are not dropped) and is retried on the next run.

Dedup: alert on the transition INTO a condition (first occurrence is never
suppressed); while it persists, bounded re-alert every --realert-hours;
one recovery message when it clears (retried until delivered).

Kill switch (independent of the timer):
  env OPS_ALERT_DISABLE_SEND=1  OR  existence of --kill-switch-file.
  Logging continues; only sending stops.

Thresholds default to runbook Appendix A ("Proposed alert thresholds",
work/lane-a-cutover-runbook-20260923/CUTOVER_RUNBOOK.md); every one is CLI
overridable.  Exit 0 on any fully-handled run (including alerting runs);
non-zero only when this script itself fails visibly.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

STATE_SCHEMA = 1
LOG = logging.getLogger("ops-alert")

# The five "fresh" vetoes named by the runbook; policy_live_delivery_disabled
# lives in the same map but has its own rule.
FRESH_VETO_REASONS = (
    "climax_fresh_data_unavailable",
    "climax_fresh_admission_failed",
    "climax_inconsistent_high_meta",
    "trapped_fresh_data_unavailable",
    "trapped_fresh_admission_failed",
)
POLICY_REASON = "policy_live_delivery_disabled"

# Mirrors the err-priority entries of DECISION_CLASS in
# tools/short_bot_autoheal.py (the watchdog's own severity map, read
# read-only).  `restart` is included: an automatic restart of a live bot
# deserves an operator message even though it self-heals.  The named
# alertables in the task (continuity-latch, provider-fault) are in here.
ALERT_DECISIONS = (
    "continuity-latch",
    "provider-fault",
    "inactive",
    "crash-looping",
    "stale-unverified",
    "restart-failed",
    "restart",
)

HEAL_LINE_RE = re.compile(
    r"AUTOHEAL lane=(?P<lane>\S+) unit=(?P<unit>\S+)"
    r"(?: rule=continuity)? decision=(?P<decision>[\w-]+)"
)
HEAL_REASON_RE = re.compile(r"reason\[\d+\]=(.*?)(?= reason\[\d+\]=|$)")

ENABLE_INSTRUCTION = (
    "To enable sending, ensure the ops-alert unit's EnvironmentFile (by "
    "proposal /opt/short-telegram-bot-lite-admin/lane-b/.env, which already "
    "contains TELEGRAM_TOKEN and ALERTS_CHAT_ID) is in place and readable, "
    "then `systemctl restart short-bot-ops-alert.service` (no bot restart "
    "needed)."
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(value) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


class Condition:
    """One concrete alert-worthy finding, identified by a stable key."""

    def __init__(self, key: str, text: str, sev: str = "ALERT") -> None:
        self.key = key
        self.text = text
        self.sev = sev


# --------------------------------------------------------------------------
# input: drift report
# --------------------------------------------------------------------------

def load_report(path: str) -> tuple[dict | None, datetime | None, str | None]:
    """Return (report, generated_at, problem).  Missing or broken input is a
    logged, alertable problem -- never healthy, never a crash."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None, None, f"report file not found: {path}"
    except (OSError, ValueError) as exc:
        return None, None, f"report unreadable ({type(exc).__name__}): {path}"
    if not isinstance(data, dict):
        return None, None, f"report malformed (not an object): {path}"
    return data, parse_iso(data.get("generated_at_utc")), None


# --------------------------------------------------------------------------
# input: autoheal watchdog journal
# --------------------------------------------------------------------------

def read_autoheal_events(args, since: datetime) -> list[dict]:
    """Return [{ts, lane, decision, reasons, line}] for alertable decisions.

    Primary source: journalctl -u <autoheal unit> (same read pattern the drift
    tool uses).  --journal-fixture replaces the source with a plain file of
    identical line format (tests, or hosts without journald access)."""
    raw_lines: list[tuple[datetime, str]] = []
    if args.journal_fixture:
        try:
            with open(args.journal_fixture, "r", encoding="utf-8") as fh:
                for line in fh:
                    raw_lines.append((utcnow(), line.rstrip("\n")))
        except OSError as exc:
            LOG.warning("journal fixture unreadable (%s): %s",
                        type(exc).__name__, str(exc)[:160])
    else:
        cmd = ["journalctl", "-u", args.autoheal_unit, "--since",
               since.strftime("%Y-%m-%d %H:%M:%S UTC"), "--no-pager", "-o", "json"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=args.journal_timeout)
            for chunk in proc.stdout.splitlines():
                if not chunk.strip():
                    continue
                try:
                    rec = json.loads(chunk)
                    ts_us = int(rec.get("__REALTIME_TIMESTAMP", "0"))
                    raw_lines.append(
                        (datetime.fromtimestamp(ts_us / 1e6, timezone.utc),
                         rec.get("MESSAGE", "")))
                except (ValueError, TypeError):
                    continue
        except Exception as exc:  # noqa: BLE001 - journald must not break us
            LOG.warning("journalctl read failed (%s); autoheal rules skipped "
                        "this run (NOT treated healthy)", type(exc).__name__)
    events: list[dict] = []
    for ts, line in raw_lines:
        match = HEAL_LINE_RE.search(line)
        if not match or match.group("decision") not in ALERT_DECISIONS:
            continue
        events.append({"ts": ts, "lane": match.group("lane"),
                       "decision": match.group("decision"),
                       "reasons": HEAL_REASON_RE.findall(line), "line": line})
    return events


# --------------------------------------------------------------------------
# evaluators: runbook rule -> exact report field paths
# --------------------------------------------------------------------------

def eval_staleness(report, generated_at, problem, now, args) -> list[Condition]:
    conds: list[Condition] = []
    if report is None:
        conds.append(Condition(
            "INPUT_MISSING",
            "drift report missing/unreadable -> measurement is blind. "
            f"detail: {problem}"))
        return conds
    if generated_at is None:
        conds.append(Condition(
            "INPUT_MISSING",
            "drift report has no parsable generated_at_utc -> freshness unknown"))
        return conds
    age_min = (now - generated_at).total_seconds() / 60.0
    if age_min > args.report_max_age_min:
        conds.append(Condition(
            "INPUT_STALE",
            f"drift report is {age_min:.0f} min old (>{args.report_max_age_min:.0f}); "
            f"short-bot-release-drift.timer may be dead. generated_at={iso(generated_at)}"))
    return conds


def eval_vetoes(report: dict, state: dict, now: datetime, args) -> list[Condition]:
    """`.delivery_veto_counts.units.<LANE>.counts_by_reason.<fresh_*>`:
    the report holds ROLLING-24 h journal counts, so a 15-min interval is
    measured as the DELTA between two consecutive evaluations (>20 fires).
    Counter decrease (window drop/journald rotation) re-baselines silently;
    a too-old baseline suppresses the delta rule for ONE interval only."""
    conds: list[Condition] = []
    units = (report.get("delivery_veto_counts") or {}).get("units") or {}
    prev = state.get("veto_baseline") or {}
    prev_at = parse_iso(state.get("veto_baseline_at"))
    interval_ok = (prev_at is not None
                   and 0 < (now - prev_at).total_seconds()
                   <= args.baseline_max_age_min * 60)
    fresh: dict[str, dict] = {}
    for lane, body in units.items():
        counts = (body or {}).get("counts_by_reason") or {}
        fresh[lane] = {}
        for reason in FRESH_VETO_REASONS:
            cur = counts.get(reason)
            if not isinstance(cur, (int, float)):
                continue
            fresh[lane][reason] = cur
            if not interval_ok:
                continue
            old = (prev.get(lane) or {}).get(reason)
            if not isinstance(old, (int, float)) or cur < old:
                continue
            delta = cur - old
            if delta > args.veto_max_per_interval:
                conds.append(Condition(
                    f"VETO_{lane}_{reason}",
                    f"+{delta:g} `{reason}` vetoes on {lane} since last check "
                    f"(threshold >{args.veto_max_per_interval:g} per 15 min; "
                    f"rolling-24h window total now {cur:g})"))
    if prev and not interval_ok:
        LOG.info("previous veto baseline too old; re-baselined (delta rule "
                 "arms from next interval)")
    state["veto_baseline"] = fresh
    state["veto_baseline_at"] = iso(now)
    return conds


def eval_policy(report: dict, state: dict, now: datetime, args) -> list[Condition]:
    """`.delivery_veto_counts.units.<LANE>.counts_by_reason.policy_live_delivery_disabled`
    >100/h: delta of the rolling-24h count vs the snapshot taken ~1 h ago."""
    conds: list[Condition] = []
    units = (report.get("delivery_veto_counts") or {}).get("units") or {}
    cur_counts = {}
    for lane, body in units.items():
        val = ((body or {}).get("counts_by_reason") or {}).get(POLICY_REASON)
        if isinstance(val, (int, float)):
            cur_counts[lane] = val
    hist = [h for h in (state.get("policy_hist") or [])
            if parse_iso(h.get("at")) is not None
            and (now - parse_iso(h["at"])).total_seconds() <= 25 * 3600]
    for lane, cur in cur_counts.items():
        ref = None
        for h in hist:  # oldest snapshot in the ~1 h window
            ago_h = (now - parse_iso(h["at"])).total_seconds() / 3600.0
            if 0.75 <= ago_h <= 1.5:
                ref = h
                break
        if ref is None:
            continue
        old = (ref.get("counts") or {}).get(lane)
        if not isinstance(old, (int, float)) or cur < old:
            continue
        rate = cur - old
        if rate > args.policy_max_per_hour:
            conds.append(Condition(
                f"POLICY_DISABLED_{lane}",
                f"`{POLICY_REASON}` on {lane}: +{rate:g} in last ~1 h "
                f"(threshold >{args.policy_max_per_hour:g}/h; rolling-24h total "
                f"{cur:g}) - policy is cutting candidate delivery"))
    hist.append({"at": iso(now), "counts": cur_counts})
    state["policy_hist"] = hist[-200:]
    return conds


def eval_deadline(report: dict, args, now) -> list[Condition]:
    """`.deadline_rates.units.<LANE>.journal_windows_hourly.<hour>.deadline_exceeded`
    >5000/h: checks the latest 3 hour buckets (current partial + 2 full)."""
    conds: list[Condition] = []
    units = (report.get("deadline_rates") or {}).get("units") or {}
    for lane, body in units.items():
        buckets = (body or {}).get("journal_windows_hourly") or {}
        for hour in sorted(buckets.keys())[-3:]:
            val = (buckets.get(hour) or {}).get("deadline_exceeded")
            if isinstance(val, (int, float)) and val > args.deadline_max_per_hour:
                conds.append(Condition(
                    f"DEADLINE_{lane}",
                    f"DEADLINE_EXCEEDED on {lane}: {val:g} in hour {hour} UTC "
                    f"(threshold >{args.deadline_max_per_hour:g}/h)"))
                break
    return conds


def eval_share(report: dict, args, now) -> list[Condition]:
    """`.deadline_rates.units.<LANE>.symbol_results_hourly[].share_attempts_not_ok`
    >0.15 sustained over the last --not-ok-hours hourly buckets (each with
    attempted>0, so a dead denominator cannot fake a pass)."""
    conds: list[Condition] = []
    units = (report.get("deadline_rates") or {}).get("units") or {}
    for lane, body in units.items():
        series = sorted((body or {}).get("symbol_results_hourly") or [],
                        key=lambda row: row.get("h", ""))
        window = series[-args.not_ok_hours:]
        if len(window) < args.not_ok_hours:
            continue
        rows = [(r.get("h"), r.get("attempted"), r.get("share_attempts_not_ok"))
                for r in window]
        if all(isinstance(a, (int, float)) and a > 0
               and isinstance(s, (int, float)) and s > args.not_ok_share
               for _, a, s in rows):
            txt = ", ".join(f"{h}={s:g}" for h, _, s in rows)
            conds.append(Condition(
                f"SCAN_QUALITY_{lane}",
                f"non-SCANNED_OK share of symbol results on {lane} "
                f">{args.not_ok_share:g} sustained {args.not_ok_hours} h ({txt}); "
                f"observed healthy ~0.003"))
    return conds


def eval_coverage(report: dict, args, now) -> list[Condition]:
    """`.coverage_gaps.lanes.<LANE>.per_symbol[].{eligible_hours,
    hours_since_last_ok}`: eligible_hours>0 AND hours_since_last_ok>6.
    eligible>0 with hours_since_last_ok == null (never scanned OK in the
    window) is also NOT healthy -> fires."""
    conds: list[Condition] = []
    lanes = (report.get("coverage_gaps") or {}).get("lanes") or {}
    for lane, body in lanes.items():
        for row in (body or {}).get("per_symbol") or []:
            sym = row.get("symbol")
            elig = row.get("eligible_hours")
            since = row.get("hours_since_last_ok")
            if not sym or not isinstance(elig, (int, float)) or elig <= 0:
                continue
            if since is None:
                conds.append(Condition(
                    f"COVERAGE_{lane}_{sym}",
                    f"{sym} on {lane}: eligible in {elig:g} h of the 24 h window "
                    f"but NEVER scanned OK (no last_ok evidence)"))
            elif isinstance(since, (int, float)) and since > args.coverage_max_hours:
                conds.append(Condition(
                    f"COVERAGE_{lane}_{sym}",
                    f"{sym} on {lane}: eligible in {elig:g} h but last OK scan "
                    f"{since:.1f} h ago (threshold >{args.coverage_max_hours:g} h)"))
    return conds


def eval_autoheal(events: list[dict], state: dict, now) -> list[Condition]:
    """Any alertable watchdog decision with a NEWER journal line than the
    watermark stored for that (lane, decision).  The watchdog repeats its
    line every 60 s pass while a condition persists, so 'no newer line'
    means the condition cleared -> recovery."""
    conds: list[Condition] = []
    last_seen = state.get("heal_last_event_us") or {}
    newest: dict[str, int] = {}
    latest: dict[str, dict] = {}
    for ev in events:
        key = f"AUTOHEAL_{ev['lane']}_{ev['decision']}"
        us = int(ev["ts"].timestamp() * 1e6)
        if us > newest.get(key, -1):
            newest[key] = us
            latest[key] = ev
    for key, us in sorted(newest.items()):
        if last_seen.get(key, -1) >= us:
            continue
        ev = latest[key]
        reasons = "; ".join(ev["reasons"]) if ev["reasons"] else "(no reason fields)"
        conds.append(Condition(
            key,
            f"watchdog reports {ev['decision'].upper()} on lane {ev['lane']} "
            f"(line at {iso(ev['ts'])}): {reasons[:400]}"))
    state["_heal_new"] = newest  # consumed by finalize_state
    return conds


# --------------------------------------------------------------------------
# dedup / persistence / recovery state machine
# --------------------------------------------------------------------------

def dedup(state: dict, conditions: list[Condition], now: datetime,
          args) -> tuple[list[tuple[str, str]], list[str], list[str]]:
    """Return (messages, active_keys_for_audit, suppressed).
    messages = [(state_key, text)] in send order.  Rules:
      * first occurrence (or a not-yet-delivered occurrence) -> message NOW,
        never suppressed;
      * persisting condition -> one message per --realert-hours, else silence;
      * condition cleared -> one recovery message, RETRIED until delivered."""
    active = state.get("active") or {}
    messages: list[tuple[str, str]] = []
    suppressed: list[str] = []
    present = {cond.key for cond in conditions}
    for cond in conditions:
        entry = active.get(cond.key)
        if entry is None:
            active[cond.key] = entry = {"first_seen": iso(now), "last_sent": None,
                                        "count": 0, "phase": "alert"}
        if entry.get("phase") == "recovering" or entry.get("last_sent") is None:
            # first occurrence or an earlier send that never left the process
            messages.append((cond.key, f"[ops-alert][{cond.sev}][{cond.key}] {cond.text}"))
            entry["phase"] = "alert"
            entry["recovery_text"] = None
        else:
            age_h = (now - parse_iso(entry["last_sent"])).total_seconds() / 3600.0
            if age_h >= args.realert_hours:
                text = (f"[ops-alert][persist][{cond.key}] {cond.text} "
                        f"[still firing since {entry['first_seen']}, "
                        f"occurrence #{entry['count'] + 1}]")
                messages.append((cond.key, text))
            else:
                suppressed.append(cond.key)
        entry["count"] = entry.get("count", 0) + 1
        entry["text"] = cond.text
    for key in sorted(active.keys()):
        if key in present:
            continue
        entry = active[key]
        if entry.get("phase") == "recovering":
            messages.append((key, entry["recovery_text"]))  # retry undelivered recovery
            continue
        if entry.get("last_sent") is None:
            del active[key]  # never announced (API died pre-delivery, then the
            continue         # condition cleared): retire silently, no ghost recovery
        entry["phase"] = "recovering"
        entry["recovery_text"] = (
            f"[ops-alert][recovered] {key} cleared at {iso(now)} "
            f"(first seen {entry.get('first_seen')}, "
            f"{entry.get('count', 0)} occurrence(s) observed)")
        messages.append((key, entry["recovery_text"]))
    state["active"] = active
    return messages, sorted(present), suppressed


# --------------------------------------------------------------------------
# delivery
# --------------------------------------------------------------------------

class Sender:
    """Minimal Telegram sender (stdlib only).  Talks to sendMessage on
    ALERTS_CHAT_ID exclusively; raises on failure so the caller can keep the
    alert pending.  Token material never reaches logs."""

    def __init__(self, token: str | None, chat: str | None, api_url: str,
                 timeout: int) -> None:
        self.token = token
        self.chat = chat
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
    @property
    def configured(self) -> bool:
        return bool(self.token and self.chat)

    def send(self, text: str) -> None:
        url = f"{self.api_url}/bot{self.token}/sendMessage"
        payload = urllib.parse.urlencode({
            "chat_id": self.chat, "text": text[:4096],
            "disable_web_page_preview": "true"}).encode("utf-8")
        last_err = "unknown"
        for attempt in (1, 2):
            try:
                req = urllib.request.Request(
                    url, data=payload,
                    headers={"User-Agent": "short-bot-ops-alert/1.0"})
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = json.loads(resp.read().decode("utf-8", "replace"))
                if body.get("ok"):
                    return
                last_err = f"api ok=false code={body.get('error_code')}"
                retry_after = float(((body.get("parameters") or {})
                                     .get("retry_after")) or 0)
                if retry_after:
                    time.sleep(min(retry_after, 8))  # bounded flood control
            except urllib.error.HTTPError as exc:
                last_err = f"HTTP {exc.code}"  # never log URL: it holds the token
                snippet = ""
                try:
                    snippet = exc.read().decode("utf-8", "replace")[:300]
                except OSError:
                    pass
                retry = re.search(r'"retry_after":\s*(\d+)', snippet)
                if exc.code == 429 and retry:
                    time.sleep(min(int(retry.group(1)), 8))
            except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
                last_err = type(exc).__name__
            if attempt == 1:
                time.sleep(2)
        raise RuntimeError(f"telegram send failed after 2 attempts: {last_err}")

def deliver(messages: list[tuple[str, str]], sender: Sender, args,
            log_only_reason: str | None) -> tuple[set[str], bool]:
    """Send every message; return (delivered_keys, transport_healthy).
    A dead transport stops the run early (no hammering); undelivered keys
    simply stay pending in state and are retried next run -- never dropped."""
    delivered: set[str] = set()
    if not messages:
        return delivered, True
    log_only = bool(log_only_reason) or not sender.configured
    if log_only:
        if not sender.configured:
            LOG.warning("destination unset -> log-only; signal-chat fallback "
                        "forbidden by project rule. ENABLE: %s", ENABLE_INSTRUCTION)
        if log_only_reason:
            LOG.warning("sending disabled: %s", log_only_reason)
    # anti-flood cap: overflow folds into one summary message; full texts are
    # still written to the alert file, and the folded keys count as delivered
    # (the condition itself was reported; cadence keeps later transitions).
    cap_children: dict[str, list[str]] = {}
    if len(messages) > args.max_msgs_per_run:
        head = messages[: args.max_msgs_per_run - 1]
        tail = messages[args.max_msgs_per_run - 1:]
        for key, text in tail:
            append_alert_line(args.alert_file, f"{iso(utcnow())} OVERFLOW-IN-CAP {text}")
        cap_key = "__cap_fold__"
        head.append((cap_key, f"[ops-alert][CAP] +{len(tail)} alert condition(s) "
                              f"folded into the alert log this run to respect "
                              f"chat rate limits: " + ", ".join(k for k, _ in tail)))
        cap_children[cap_key] = [k for k, _ in tail]
        messages = head
    healthy = True
    for key, text in messages:
        if log_only:
            LOG.warning("LOG-ONLY [%s]: %s", log_only_reason or "destination unset", text)
            append_alert_line(args.alert_file, f"{iso(utcnow())} LOG-ONLY {text}")
            delivered.update(cap_children.get(key, [key]))
            continue
        try:
            sender.send(text)
        except Exception as exc:  # noqa: BLE001 - keep pending, never silent
            healthy = False
            LOG.error("delivery failed (%s): %s -> %s stays pending for next run",
                      type(exc).__name__, str(exc)[:200], key)
            break
        LOG.info("sent: %s", text)
        append_alert_line(args.alert_file, f"{iso(utcnow())} SENT {text}")
        delivered.update(cap_children.get(key, [key]))
        time.sleep(args.msg_gap_sec)  # per-chat pacing (<1 msg/s)
    return delivered, healthy


def append_alert_line(alert_file: str, line: str) -> None:
    try:
        os.makedirs(os.path.dirname(os.path.abspath(alert_file)), exist_ok=True)
        with open(alert_file, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError as exc:
        LOG.error("alert file write failed (%s): %s", type(exc).__name__,
                  str(exc)[:160])


def finalize_state(state: dict, messages: list[tuple[str, str]],
                   delivered: set[str], now: datetime) -> None:
    """Advance bookkeeping ONLY for what actually left the process (log-only
    counts as delivered because the alert file is the record).  Pending
    first-fires keep last_sent=None (dedup retries them immediately);
    undelivered recoveries keep phase=recovering (retried)."""
    active = state["active"]
    for key, _ in messages:
        if key not in delivered:
            continue
        entry = active.get(key)
        if entry is None:
            continue
        if entry.get("phase") == "recovering":
            del active[key]  # recovery acknowledged; key fully retired
            continue
        entry["last_sent"] = iso(now)
    watermark = dict(state.get("heal_last_event_us") or {})
    for key, us in (state.get("_heal_new") or {}).items():
        entry = active.get(key)
        pending = entry is not None and entry.get("last_sent") is None
        if not pending:
            watermark[key] = max(watermark.get(key, -1), us)
    state["heal_last_event_us"] = watermark
    state.pop("_heal_new", None)
    state["schema"] = STATE_SCHEMA
    state["updated_at"] = iso(now)


def atomic_write(path: str, payload: dict) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".ops-alert-state.", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=1, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_state(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and data.get("schema") == STATE_SCHEMA:
            return data
        LOG.warning("state file has unexpected schema; starting fresh")
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as exc:
        LOG.warning("state unreadable (%s); starting fresh", type(exc).__name__)
    return {}


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    base = "/opt/short-telegram-bot-lite-admin/shared/runtime"
    p.add_argument("--report", default=f"{base}/release-drift-report.json")
    p.add_argument("--state", default=f"{base}/ops-alert-state.json")
    p.add_argument("--alert-file", default=f"{base}/ops-alert.log")
    p.add_argument("--kill-switch-file", default=f"{base}/ops-alert.disabled")
    p.add_argument("--autoheal-unit", default="short-bot-autoheal.service")
    p.add_argument("--journal-fixture", default=None,
                   help="read AUTOHEAL lines from this file instead of journalctl")
    p.add_argument("--journal-timeout", type=int, default=20)
    p.add_argument("--api-url", default="https://api.telegram.org")
    p.add_argument("--send-timeout", type=int, default=10)
    p.add_argument("--msg-gap-sec", type=float, default=1.2)
    p.add_argument("--max-msgs-per-run", type=int, default=8)
    p.add_argument("--realert-hours", type=float, default=6.0,
                   help="bounded re-alert cadence while a condition persists")
    # runbook Appendix A defaults, all overridable:
    p.add_argument("--veto-max-per-interval", type=float, default=20)
    p.add_argument("--policy-max-per-hour", type=float, default=100)
    p.add_argument("--deadline-max-per-hour", type=float, default=5000)
    p.add_argument("--not-ok-share", type=float, default=0.15)
    p.add_argument("--not-ok-hours", type=int, default=2)
    p.add_argument("--coverage-max-hours", type=float, default=6)
    p.add_argument("--report-max-age-min", type=float, default=45)
    p.add_argument("--baseline-max-age-min", type=float, default=45)
    p.add_argument("--lookback-min", type=float, default=90)
    p.add_argument("--now", default=None, help="override wall clock (ISO; tests)")
    p.add_argument("--dry-run", action="store_true",
                   help="evaluate and print; send nothing, mutate nothing")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def evaluate_all(report, generated_at, problem, events, state, now,
                 args) -> list[Condition]:
    conditions: list[Condition] = []
    trace: list[str] = []

    def run(name, found, n_items):
        conditions.extend(found)
        trace.append(f"{name}: evaluated={n_items} fired={len(found)}")

    run("staleness", eval_staleness(report, generated_at, problem, now, args),
        1 if report is not None else 0)
    if report is not None:
        units = (report.get("delivery_veto_counts") or {}).get("units") or {}
        run("fresh_vetoes>20/interval",
            eval_vetoes(report, state, now, args),
            sum(len(FRESH_VETO_REASONS) for _ in units))
        run("policy_disabled>100/h", eval_policy(report, state, now, args), len(units))
        run("deadline>5000/h", eval_deadline(report, args, now), len(units))
        run("share_not_ok>0.15/2h", eval_share(report, args, now), len(units))
        lanes = (report.get("coverage_gaps") or {}).get("lanes") or {}
        run("coverage_gap(elig>0,>6h)", eval_coverage(report, args, now),
            sum(len((b or {}).get("per_symbol") or []) for b in lanes.values()))
    else:
        LOG.warning("report-dependent rules skipped (no input); NOT healthy")
    run("watchdog_decisions", eval_autoheal(events, state, now), len(events))
    LOG.info("rules: %s", " | ".join(trace))
    return conditions


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="ops-alert %(levelname)s %(message)s", stream=sys.stdout)
    now = parse_iso(args.now) or utcnow()

    sender = Sender(os.environ.get("TELEGRAM_TOKEN"),
                    os.environ.get("ALERTS_CHAT_ID"),
                    args.api_url, args.send_timeout)
    log_only_reason = None
    if os.environ.get("OPS_ALERT_DISABLE_SEND", "").strip().lower() in {
            "1", "true", "yes", "on"}:
        log_only_reason = "kill switch env OPS_ALERT_DISABLE_SEND is set"
    elif args.kill_switch_file and os.path.exists(args.kill_switch_file):
        log_only_reason = f"kill switch flag file present: {args.kill_switch_file}"

    report, generated_at, problem = load_report(args.report)
    if problem:
        LOG.warning("input problem: %s", problem)
    state = load_state(args.state)
    events = read_autoheal_events(args, now - timedelta(minutes=args.lookback_min))

    conditions = evaluate_all(report, generated_at, problem, events, state,
                              now, args)
    messages, present, suppressed = dedup(state, conditions, now, args)
    for key in suppressed:
        LOG.info("quiet (condition persists; re-alert at +%sh): %s",
                 args.realert_hours, key)

    if args.dry_run:
        if messages:
            for _, text in messages:
                print(f"[DRY-RUN] would send: {text}")
        else:
            print(f"[DRY-RUN] nothing to send: 0 alert conditions met "
                  f"(report generated_at="
                  f"{iso(generated_at) if generated_at else f'problem: {problem}'})")
        print("[DRY-RUN] no message sent; state file NOT written")
        return 0

    delivered, healthy = deliver(messages, sender, args, log_only_reason)
    finalize_state(state, messages, delivered, now)
    try:
        atomic_write(args.state, state)
    except OSError as exc:
        LOG.error("state write failed (%s): %s", type(exc).__name__, str(exc)[:160])
        return 4
    LOG.info("run complete: %d/%d delivered, %d persisting-quiet, %d active keys, "
             "transport=%s", len(delivered), len(messages), len(suppressed),
             len(state.get("active") or {}), "ok" if healthy else "FAILED-PENDING")
    return 0


if __name__ == "__main__":
    sys.exit(main())
