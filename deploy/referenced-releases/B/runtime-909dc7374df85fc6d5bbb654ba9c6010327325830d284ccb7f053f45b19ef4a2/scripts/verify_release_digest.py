#!/usr/bin/env python3
"""D8: verify a Lane A release identity under digest rule v2 (derived-state exclusion).

Rule v2 aligns with the Lane C 2026-09-23 release-digest rule: instead of
purging bytecode before hashing (rule v1, which relied on every operator
remembering), the rule EXCLUDES derived state at find level, so the digest is
invariant under "run anything with any Python inside the release".

The exact inclusion/exclusion set:

    include : every REGULAR file under the release root, except the paths
              and names listed below;
    exclude : ./.venv/*, ./data/*            (environment/layout links;
              they are symlinks in Lane A and never -type f, the path
              exclusions keep the rule correct if one becomes a real dir)
              ./.env, release-manifest.json   (secret + identity carrier)
              interpreter.json                (reserved for a Lane-C-style
              interpreter pin; NOT shipped by Lane A — see divergence note)
              */__pycache__/*, *.pyc,         (derived bytecode caches — the
              ./.pytest_cache/*, ./.ruff_cache/*  class v1 got wrong)

Divergence from Lane C (explicit): Lane C ships a REAL .venv and therefore
pins it in interpreter.json from the release's own interpreter.  Lane A's
.venv and data are symlinks to shared external targets; pinning an
interpreter that lives OUTSIDE the release would create a file claiming to
verify something the release bytes cannot, so Lane A ships no
interpreter.json today.  The rule still excludes the name so a later Lane A
release may add a Lane-C-shaped pin without changing the identity rule.

Equivalence: for a release tree with no derived state present, rule v2
produces exactly the same digest as rule v1-with-purge (the v1 cutover
procedure), so the directory names of existing releases remain truthful.

Usage:  python scripts/verify_release_digest.py [RELEASE_DIR]
Exit 0 iff recomputed digest == directory name == release-manifest code_sha
(any one that exists; if the directory name is not a 64-hex digest the name
check is reported SKIPPED and the manifest must match).
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

SHA_RE = re.compile(r"^[0-9a-f]{64}$")

FIND_ARGS = [
    "find", ".", "-type", "f",
    "!", "-path", "./.venv/*",
    "!", "-path", "./data/*",
    "!", "-name", ".env",
    "!", "-name", "release-manifest.json",
    "!", "-name", "interpreter.json",
    "!", "-path", "*/__pycache__/*",
    "!", "-name", "*.pyc",
    "!", "-path", "./.pytest_cache/*",
    "!", "-path", "./.ruff_cache/*",
    "-exec", "sha256sum", "{}", "+",
]


def compute_digest(release_dir: Path) -> str:
    listing = subprocess.run(
        FIND_ARGS, cwd=release_dir, capture_output=True, text=True, check=True
    ).stdout
    entries: list[tuple[str, str]] = []
    for line in listing.splitlines():
        if not line.strip():
            continue
        digest_value, _, path = line.partition("  ")
        entries.append((digest_value, path))
    entries.sort(key=lambda item: item[1])
    material = "".join(f"{digest_value}  {path}\n" for digest_value, path in entries)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def main(argv: list[str] | None = None) -> int:
    args = [a for a in (argv if argv is not None else sys.argv[1:])]
    release_dir = Path(args[0]).resolve() if args else Path(__file__).resolve().parents[1]
    if not release_dir.is_dir():
        print(f"error: not a directory: {release_dir}", file=sys.stderr)
        return 2
    recomputed = compute_digest(release_dir)
    dir_name = release_dir.name
    print(f"release dir : {release_dir}")
    print(f"digest (v2) : {recomputed}")
    failures = 0
    if SHA_RE.fullmatch(dir_name):
        if dir_name == recomputed:
            print("dir name    : MATCH")
        else:
            print(f"dir name    : MISMATCH (name {dir_name})")
            failures += 1
    else:
        print("dir name    : SKIPPED (directory name is not a 64-hex digest)")
    manifest = release_dir / "release-manifest.json"
    if manifest.is_file():
        try:
            code_sha = json.loads(manifest.read_text(encoding="utf-8")).get("code_sha")
        except (OSError, ValueError) as exc:
            print(f"manifest    : UNREADABLE ({exc})", file=sys.stderr)
            return 1
        if code_sha == recomputed:
            print("manifest    : MATCH")
        else:
            print(f"manifest    : MISMATCH (code_sha {code_sha!r})")
            failures += 1
    elif SHA_RE.fullmatch(dir_name):
        print("manifest    : MISMATCH (release-manifest.json missing)")
        failures += 1
    else:
        print("manifest    : SKIPPED (no manifest and dir name is not a digest)")
    if not SHA_RE.fullmatch(dir_name) and not manifest.is_file():
        print("error: nothing to verify against (no sha-named dir, no manifest)", file=sys.stderr)
        return 1
    if failures:
        print(f"DIGEST_VERIFY: FAIL ({failures} mismatch(es))")
        return 1
    print("DIGEST_VERIFY: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
