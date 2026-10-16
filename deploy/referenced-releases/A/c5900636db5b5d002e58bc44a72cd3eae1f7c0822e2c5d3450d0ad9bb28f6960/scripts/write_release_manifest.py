"""Write the immutable release manifest carrying the deployed code SHA."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.infra.atomic_json import atomic_write_json  # noqa: E402

SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")
SCHEMA_VERSION = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-sha", required=True, help="40-64 lowercase hex SHA")
    parser.add_argument("--output", required=True, help="manifest path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    code_sha = args.code_sha.strip()
    if not SHA_RE.fullmatch(code_sha):
        print(
            "error: --code-sha must be 40-64 lowercase hex characters",
            file=sys.stderr,
        )
        return 2
    output = Path(args.output)
    if output.exists():
        try:
            existing = json.loads(output.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            existing = None
        if isinstance(existing, dict) and existing.get("code_sha") != code_sha:
            print(
                f"error: refusing to overwrite manifest SHA "
                f"{existing.get('code_sha')!r} with {code_sha!r}",
                file=sys.stderr,
            )
            return 1
    atomic_write_json(output, {"schema_version": SCHEMA_VERSION, "code_sha": code_sha})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
