#!/usr/bin/env python3
"""Verify the allowlist and hashes of a sanitized export."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "EXPORT_MANIFEST.json"
SUMS = ROOT / "EXPORT_SHA256SUMS"

def staged_digests(relative_paths: set[str]) -> dict[str, str]:
    entries = subprocess.check_output(["git", "ls-files", "-s", "-z"], cwd=ROOT).split(b"\0")
    staged = {}
    for entry in entries:
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        _, object_id, _ = metadata.split()
        staged[raw_path.decode("utf-8", errors="surrogateescape")] = object_id.decode("ascii")
    missing = relative_paths - staged.keys()
    if missing:
        raise RuntimeError("paths are missing from the Git index: " + ", ".join(sorted(missing)[:5]))

    object_ids = [staged[path] for path in sorted(relative_paths)]
    result = subprocess.run(
        ["git", "cat-file", "--batch"],
        cwd=ROOT,
        input=("\n".join(object_ids) + "\n").encode("ascii"),
        stdout=subprocess.PIPE,
        check=True,
    ).stdout
    digests = {}
    offset = 0
    for path, object_id in zip(sorted(relative_paths), object_ids):
        header_end = result.index(b"\n", offset)
        actual_id, object_type, size_text = result[offset:header_end].decode("ascii").split()
        size = int(size_text)
        offset = header_end + 1
        blob = result[offset : offset + size]
        offset += size + 1  # blob bytes and cat-file record delimiter
        if actual_id != object_id or object_type != "blob" or len(blob) != size:
            raise RuntimeError("invalid Git object data for " + path)
        digests[path] = hashlib.sha256(blob).hexdigest()
    return digests

def main() -> int:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    listed = set(payload.get("files", []))
    actual = {
        rel
        for path in ROOT.rglob("*")
        if path.is_file()
        for rel in (path.relative_to(ROOT).as_posix(),)
        if not any(part in {".git", "__pycache__", ".pytest_cache", ".venv", "vendor"} for part in path.relative_to(ROOT).parts)
        and path.name not in {MANIFEST.name, SUMS.name}
        and not (rel.startswith("data/lane-c-market-store.json.gz"))
    }
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
    try:
        staged_hashes = staged_digests(required)
    except (OSError, subprocess.CalledProcessError, RuntimeError) as exc:
        errors.append("git_index_unavailable:" + type(exc).__name__)
        staged_hashes = {}
    for rel in sorted(required):
        path = ROOT / rel
        if not path.is_file() or staged_hashes.get(rel) != checks.get(rel):
            errors.append("hash_mismatch:" + rel)
    if errors:
        for error in errors[:10]:
            print(error)
        return 1
    print(f"export_ok files={len(listed)} checksums={len(checks)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
