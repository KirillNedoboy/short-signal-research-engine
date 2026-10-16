#!/usr/bin/env python3
"""Verify the allowlist and hashes of a sanitized export."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "EXPORT_MANIFEST.json"
SUMS = ROOT / "EXPORT_SHA256SUMS"

def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main() -> int:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    listed = set(payload.get("files", []))
    actual = {p.relative_to(ROOT).as_posix() for p in ROOT.rglob("*") if p.is_file() and not any(part in {".git", "__pycache__", ".pytest_cache", ".venv", "vendor"} for part in p.relative_to(ROOT).parts) and p.name not in {MANIFEST.name, SUMS.name}}
    errors = []
    if listed != actual:
        errors.append(f"manifest_set_mismatch missing={len(listed-actual)} extra={len(actual-listed)}")
    checks = {}
    for line in SUMS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        if "  " in line:
            digest_value, rel = line.split("  ", 1)
        else:
            digest_value, rel = "", ""
        if not rel:
            errors.append("malformed_checksum_line")
            continue
        checks[rel] = digest_value
    required = listed | {MANIFEST.name}
    if set(checks) != required:
        errors.append(f"checksum_set_mismatch missing={len(required-set(checks))} extra={len(set(checks)-required)}")
    for rel in sorted(required):
        path = ROOT / rel
        if not path.is_file() or digest(path) != checks.get(rel):
            errors.append("hash_mismatch:" + rel)
    if errors:
        for error in errors[:10]:
            print(error)
        return 1
    print(f"export_ok files={len(listed)} checksums={len(checks)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
