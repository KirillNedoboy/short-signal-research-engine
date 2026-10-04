"""Write the checked-in AKEUSDT offline replay reports."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT.parent))

from research.climax_replay import load_fixture, run_replay


FIXTURE_PATH = ROOT / "fixtures" / "akeusdt_2026-07-15.json"
DEFAULT_OUTPUT_DIR = ROOT / "reports"
REPORT_STEM = "AKEUSDT_2026-07-15_climax_replay"


def write_reports(report: dict[str, Any], output_dir: Path) -> dict[str, Path]:
    """Serialize one replay result as deterministic JSON and Markdown."""

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{REPORT_STEM}.json"
    markdown_path = output_dir / f"{REPORT_STEM}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def _render_markdown(report: dict[str, Any]) -> str:
    blogger = report["evidence"]["blogger"]
    baseline = report["baseline"]
    lines = [
        "# AKEUSDT — CLIMAX_SHORT_RESEARCH_V1 forensic replay",
        "",
        "## Scope and evidence",
        "",
        f"- Replay window: `{report['replay_window']['start_utc']}`–`{report['replay_window']['end_utc']}`.",
        "- Market data: Bybit public REST historical snapshot, captured into the deterministic fixture.",
        f"- Blogger data: `{blogger['source']}`. It is kept separate from server and exchange facts.",
        f"- Blogger timestamp: `{blogger['signal_time_utc']}` / `{blogger['signal_time_msk']}`; published short zone: `{blogger['entry_short_zone']}`.",
        "- This is offline research only. It does not read or write the baseline SQLite database and is not imported by the runtime.",
        "",
        "## Blogger versus baseline versus research models",
        "",
        "| Side | Time UTC | State / decision | Paper entry | Delay vs 11:08 MSK |",
        "| --- | --- | --- | --- | --- |",
        f"| Blogger | {blogger['signal_time_utc']} | screenshot-derived short idea | {blogger['entry_short_zone']} | 0m |",
        "| Baseline | 2026-07-15T08:03:00Z | EARLY_PUMP_WATCH, score 70; not actionable | — | -5m |",
        "| Baseline | 2026-07-15T08:07:00Z | REJECT, score 0 | — | -1m |",
        "| Baseline | 2026-07-15T08:10:00Z | REJECT, score 0 | — | +2m |",
    ]
    for name, model in report["models"].items():
        if model["status"] == "CONFIRMED":
            lines.append(
                f"| {name} | {model['confirmation_time_utc']} | CONFIRMED | {model['paper_entry']['price']:.7f} | {model['delay_vs_blogger_minutes']:+d}m |"
            )
        else:
            lines.append(f"| {name} | — | {model['status']}: {model['missing_reason']} | — | — |")

    lifecycle = baseline["lifecycle_at_0808_utc"]
    lines.extend(
        [
            "",
            "## Baseline finding",
            "",
            f"AKEUSDT was eligible and deep-scanned. `pump_detected` is established by the 08:03 EARLY_PUMP_WATCH. The lifecycle at 08:08 UTC is `{lifecycle['value']}` with `{lifecycle['certainty']}` certainty: {lifecycle['basis']}",
            "",
            "The baseline only admits a signal after a pullback in the configured 2.4–8.0% band and activation of the short zone. The first large reversal fell outside that narrow maturity path, so the engine retained no actionable state and emitted no SIGNAL. Journal data also shows roughly three-minute cycle stretches when Bybit rate limits hit, which made this fast reversal harder to observe at a useful point.",
            "",
            "## Climax evidence from candles",
            "",
            "| Feature | Value |",
            "| --- | --- |",
        ]
    )
    for key, value in report["climax_features"].items():
        lines.append(f"| {key} | {value} |")

    lines.extend(["", "## OI, funding, and premium", "", "OI observations use matched public snapshot timestamps. They describe association, not intrabar causality.", ""])
    for interval, observations in report["oi_price_observations"].items():
        near_event = [row for row in observations if "2026-07-15T08:" in row["to_utc"]]
        lines.append(f"### OI {interval}")
        lines.append("")
        lines.append("| From | To | Price change | OI change |")
        lines.append("| --- | --- | ---: | ---: |")
        for row in near_event:
            lines.append(f"| {row['from_utc']} | {row['to_utc']} | {row['price_change_pct']:+.4f}% | {row['oi_change_pct']:+.4f}% |")
        lines.append("")
    funding = report["funding_premium"]
    lines.extend(
        [
            f"Funding nearest the blogger time: `{funding['funding_rate']}` at `{funding['funding_time_utc']}`.",
            f"Premium-index close nearest the blogger time: `{funding['premium_index_close']}` at `{funding['premium_time_utc']}`.",
            "",
            "## Research-model outcomes",
            "",
        ]
    )
    for name, model in report["models"].items():
        lines.append(f"### {name}")
        lines.append("")
        if model["status"] != "CONFIRMED":
            lines.append(f"No confirmation: `{model['missing_reason']}`.")
            lines.append("")
            continue
        lines.append(f"Criteria: {', '.join(model['criteria'])}.")
        lines.append("")
        lines.append("| Horizon | Close | Short return | MFE | MAE |")
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for horizon, outcome in model["outcomes"].items():
            lines.append(
                f"| {horizon} | {outcome['close_price']:.7f} | {outcome['short_return_pct']:+.4f}% | {outcome['mfe_pct']:.4f}% | {outcome['mae_pct']:.4f}% |"
            )
        lines.append("")
        lines.append(f"First hit, favorable 5% vs adverse 3%: `{model['first_hit']['favorable_5_vs_adverse_3']}`.")
        lines.append(f"First hit, favorable 10% vs adverse 5%: `{model['first_hit']['favorable_10_vs_adverse_5']}`.")
        lines.append("")

    lines.extend(["## Explicitly unavailable historical data", ""])
    for field, detail in report["missing_data"].items():
        lines.append(f"- `{field}`: `NULL`; `{detail['missing_reason']}`.")

    lines.extend(["", "## What to capture live", ""])
    for item in report["live_capture_recommendations"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    """Generate the checked-in report pair from the checked-in fixture."""

    report = run_replay(load_fixture(FIXTURE_PATH))
    paths = write_reports(report, DEFAULT_OUTPUT_DIR)
    print(f"JSON: {paths['json']}")
    print(f"Markdown: {paths['markdown']}")


if __name__ == "__main__":
    main()
