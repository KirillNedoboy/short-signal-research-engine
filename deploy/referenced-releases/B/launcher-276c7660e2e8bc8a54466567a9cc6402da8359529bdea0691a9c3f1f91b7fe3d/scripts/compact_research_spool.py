"""Admin-only compaction of sealed research spool segments."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.research.spool_compactor import SpoolCompactionError, compact_sealed_segment


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spool-root", required=True)
    parser.add_argument("--archive-root", required=True)
    parser.add_argument("--code-sha", required=True)
    parser.add_argument("--segment")
    args = parser.parse_args(argv)
    root = Path(args.spool_root) / "sealed"
    segments = [Path(args.segment)] if args.segment else sorted(root.glob("*.jsonl"))
    if not segments:
        manifests = sorted(
            (Path(args.archive_root) / "manifests").glob("compaction-*.json")
        )
        results = []
        for manifest_path in manifests:
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if manifest.get("status") == "COMPLETE":
                results.append(
                    {"status": "NOOP_ALREADY_COMPACTED", "manifest": manifest}
                )
        print(json.dumps(results, sort_keys=True, indent=2))
        return 0
    try:
        results = [
            compact_sealed_segment(segment, args.archive_root, code_sha=args.code_sha)
            for segment in segments
        ]
    except (SpoolCompactionError, FileNotFoundError, ValueError) as exc:
        print(f"compaction error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(results, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
