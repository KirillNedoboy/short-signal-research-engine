"""Command-line entry point for sealed offline ReplayBundle operations."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from app.config import load_config
from app.research.replay import build_bundle, generate_report, replay_bundle, verify_bundle


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m scripts.replay_bundle")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--spec", required=True, type=Path)
    build.add_argument("--config", required=True, type=Path)
    build.add_argument("--output-root", required=True, type=Path)
    build.add_argument("--env", type=Path)
    build.add_argument("--run-id")
    build.add_argument("--allow-dirty", action="store_true")
    for name in ("verify", "replay", "report"):
        subparser = commands.add_parser(name)
        subparser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    if args.command == "build":
        env = args.env or args.config.with_name(".replay-no-env")
        path = build_bundle(args.spec, load_config(args.config, env), args.output_root, args.run_id, args.allow_dirty)
        print(path)
        return 0
    if args.command == "verify":
        result = verify_bundle(args.bundle)
        print(json.dumps(asdict(result), sort_keys=True))
        return 0 if result.valid else 1
    if args.command == "replay":
        result = replay_bundle(args.bundle)
        print(json.dumps(asdict(result), sort_keys=True))
        return 0 if result.verified.valid and result.matches_recorded_decisions else 1
    report = generate_report(args.bundle)
    print(report.markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
