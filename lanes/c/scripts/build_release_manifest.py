"""Build and verify a reproducible, self-contained Lane C release.

The launcher already verifies ``SOURCE_PIN`` by hashing the bytes under
``src/``. This tool keeps that exact contract and adds a deterministic tar.gz
release containing only the reviewed runtime inputs. Release archives never
include credentials, databases, caches, logs, user data, or a virtualenv.

Examples::

    python lanes/c/scripts/build_release_manifest.py
    python lanes/c/scripts/build_release_manifest.py --verify
    python lanes/c/scripts/build_release_manifest.py --build --output /tmp/lane-c.tar.gz
    python lanes/c/scripts/build_release_manifest.py --verify-archive /tmp/lane-c.tar.gz
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src"
SUMS = ROOT / "SHA256SUMS"
PIN = ROOT / "SOURCE_PIN"
RELEASE_MANIFEST = "RELEASE_MANIFEST.json"
RELEASE_SCHEMA_VERSION = 1

# Keep this list deliberately explicit. Adding a new runtime input requires a
# reviewed change here, which prevents an accidental ``data/`` or ``.env``
# copy from becoming part of an immutable release.
RELEASE_FILES = (
    ".gitattributes",
    "README.md",
    "ROLLBACK.md",
    "SHADOW_GATE.md",
    "SOURCE_PROVENANCE.md",
    "SOURCE_PIN",
    "SHA256SUMS",
    "config.example.yaml",
    "requirements.lock",
    "run_live.py",
    "deploy/lane-c-runtime.service.example",
    "deploy/lane-c-shadow.service.example",
    "deploy/lane-c-shadow-root-guard.service.example",
    "scripts/build_release_manifest.py",
    "scripts/compat_validate.py",
    "tools/replay_shadow.py",
    "tools/shadow_guard.py",
    "tools/shadow_replay_worker.py",
)

_SKIP_NAMES = frozenset(
    {
        ".env",
        ".git",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "artifacts",
        "data",
        "logs",
        "runtime",
        "userdata",
        "vendor",
    }
)


class ReleaseError(RuntimeError):
    """Raised when a release cannot be made safe and reproducible."""


def _is_cache(path: Path) -> bool:
    return any(part in {"__pycache__", ".pytest_cache", ".ruff_cache"} for part in path.parts) or path.suffix in {
        ".pyc",
        ".pyo",
    }


def entries(*, root: Path = ROOT, source: Path | None = None) -> list[tuple[str, str]]:
    """Return ``(sha256, relative_path)`` entries for the launcher source pin."""

    source_root = source or root / "src"
    if not source_root.is_dir():
        raise ReleaseError(f"source directory is missing: {source_root}")
    result: list[tuple[str, str]] = []
    for path in sorted(source_root.rglob("*")):
        if not path.is_file() or _is_cache(path):
            continue
        if path.is_symlink():
            raise ReleaseError(f"source symlink is not allowed: {path}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        result.append((digest, path.relative_to(root).as_posix()))
    return result


def aggregate(items: Iterable[tuple[str, str]]) -> str:
    payload = "".join(f"{digest}  {name}\n" for digest, name in items).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def source_manifest(*, root: Path = ROOT) -> tuple[str, str, int]:
    """Return rendered source sums, source pin, and source file count."""

    items = entries(root=root)
    rendered = "".join(f"{digest}  {name}\n" for digest, name in items)
    return rendered, aggregate(items), len(items)


def write_source_manifest(*, root: Path = ROOT) -> tuple[str, int]:
    """Write the launcher-compatible source manifest with LF line endings."""

    rendered, digest, count = source_manifest(root=root)
    sums = root / "SHA256SUMS"
    pin = root / "SOURCE_PIN"
    current_sums = sums.read_bytes() if sums.exists() else None
    current_pin = pin.read_bytes() if pin.exists() else None
    if current_sums != rendered.encode("utf-8"):
        sums.write_bytes(rendered.encode("utf-8"))
    if current_pin != f"{digest}\n".encode("utf-8"):
        pin.write_bytes(f"{digest}\n".encode("utf-8"))
    return digest, count


def verify_source_manifest(*, root: Path = ROOT) -> tuple[str, int]:
    rendered, digest, count = source_manifest(root=root)
    sums = root / "SHA256SUMS"
    pin = root / "SOURCE_PIN"
    if not sums.exists() or sums.read_bytes() != rendered.encode("utf-8"):
        raise ReleaseError("Lane C source manifest mismatch")
    if not pin.exists() or pin.read_text(encoding="utf-8").strip() != digest:
        raise ReleaseError("Lane C source pin mismatch")
    return digest, count


def _validate_release_path(relative: str) -> PurePosixPath:
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ReleaseError(f"unsafe release path: {relative!r}")
    if path.parts[0] in _SKIP_NAMES:
        raise ReleaseError(f"release path contains excluded data: {relative!r}")
    return path


def _required_files(*, root: Path = ROOT) -> list[Path]:
    paths: list[Path] = []
    for relative in RELEASE_FILES:
        _validate_release_path(relative)
        path = root / Path(relative)
        if not path.is_file():
            raise ReleaseError(f"required release input is missing: {relative}")
        if path.is_symlink():
            raise ReleaseError(f"release symlink is not allowed: {relative}")
        paths.append(path)
    return paths


def _copy_release_inputs(*, root: Path, stage: Path) -> None:
    _required_files(root=root)
    for relative in RELEASE_FILES:
        source = root / Path(relative)
        destination = stage / Path(relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        # Archive modes are normalized so a checkout made with a different
        # umask produces the same bytes and metadata.
        destination.chmod(0o644)

    source_root = root / "src"
    destination_root = stage / "src"
    for source in sorted(source_root.rglob("*")):
        if _is_cache(source):
            continue
        if source.is_symlink():
            raise ReleaseError(f"source symlink is not allowed: {source}")
        if source.is_dir():
            continue
        relative = source.relative_to(source_root)
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(0o644)


def _payload_entries(stage: Path) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for path in sorted(stage.rglob("*")):
        if not path.is_file() or path.name == RELEASE_MANIFEST or _is_cache(path):
            continue
        relative = path.relative_to(stage).as_posix()
        _validate_release_path(relative)
        data = path.read_bytes()
        result.append({"path": relative, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    return result


def _write_release_manifest(stage: Path, source_pin: str) -> dict[str, object]:
    payload = {
        "schema_version": RELEASE_SCHEMA_VERSION,
        "layout": "lane-c-runtime",
        "source_pin": source_pin,
        "files": _payload_entries(stage),
        # The manifest intentionally excludes its own hash to avoid a circular
        # dependency. Its canonical bytes are covered by the archive hash.
    }
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    (stage / RELEASE_MANIFEST).write_bytes(encoded)
    return payload


def _tar_info(path: Path, *, stage: Path) -> tarfile.TarInfo:
    relative = path.relative_to(stage).as_posix()
    info = tarfile.TarInfo(relative)
    info.size = path.stat().st_size
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mode = 0o644
    return info


def _write_archive(stage: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    if temporary.exists():
        temporary.unlink()
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=9) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                    for path in sorted(stage.rglob("*")):
                        if path.is_dir():
                            continue
                        with path.open("rb") as handle:
                            archive.addfile(_tar_info(path, stage=stage), handle)
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def build_release(output: Path, *, root: Path = ROOT) -> dict[str, object]:
    """Build one reproducible archive and return its release manifest."""

    output = output.expanduser().resolve()
    root = root.expanduser().resolve()
    try:
        output.relative_to(root)
    except ValueError:
        pass
    else:
        raise ReleaseError("archive output must be outside the Lane C checkout")
    source_pin, source_count = write_source_manifest(root=root)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lane-c-release-", dir=output.parent) as temporary:
        stage = Path(temporary)
        _copy_release_inputs(root=root, stage=stage)
        payload = _write_release_manifest(stage, source_pin)
        _write_archive(stage, output)
    archive_hash = hashlib.sha256(output.read_bytes()).hexdigest()
    return {
        **payload,
        "source_files": source_count,
        "archive": str(output),
        "archive_sha256": archive_hash,
    }


def _safe_member_name(name: str) -> PurePosixPath:
    return _validate_release_path(name)


def verify_archive(archive_path: Path) -> dict[str, object]:
    """Verify archive paths, payload hashes, and the launcher source pin."""

    archive_path = archive_path.expanduser().resolve()
    if not archive_path.is_file() or archive_path.is_symlink():
        raise ReleaseError(f"archive is not a regular file: {archive_path}")
    archive_hash = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="lane-c-verify-") as temporary:
        extract_root = Path(temporary)
        names: set[str] = set()
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                relative = _safe_member_name(member.name).as_posix()
                if relative in names or not member.isfile():
                    raise ReleaseError(f"archive contains an invalid or duplicate entry: {member.name!r}")
                names.add(relative)
                destination = extract_root / Path(relative)
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise ReleaseError(f"archive entry has no data: {member.name!r}")
                destination.write_bytes(source.read())
        manifest_path = extract_root / RELEASE_MANIFEST
        if not manifest_path.exists():
            raise ReleaseError("archive release manifest is missing")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ReleaseError("archive release manifest is not valid JSON") from exc
        if manifest.get("schema_version") != RELEASE_SCHEMA_VERSION:
            raise ReleaseError("unsupported release manifest schema")
        expected_files = manifest.get("files")
        if not isinstance(expected_files, list):
            raise ReleaseError("release manifest files are missing")
        expected_paths: set[str] = set()
        for entry in expected_files:
            if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                raise ReleaseError("invalid release manifest file entry")
            relative = _safe_member_name(entry["path"]).as_posix()
            expected_paths.add(relative)
            actual_path = extract_root / Path(relative)
            if not actual_path.is_file():
                raise ReleaseError(f"release manifest file is missing: {relative}")
            actual_hash = hashlib.sha256(actual_path.read_bytes()).hexdigest()
            if actual_hash != entry.get("sha256") or actual_path.stat().st_size != entry.get("size"):
                raise ReleaseError(f"release file hash mismatch: {relative}")
        actual_payload = {name for name in names if name != RELEASE_MANIFEST}
        if expected_paths != actual_payload:
            raise ReleaseError("release manifest does not cover the archive payload")
        source_pin, source_count = verify_source_manifest(root=extract_root)
        if source_pin != manifest.get("source_pin"):
            raise ReleaseError("release source pin does not match the source manifest")
    return {
        "schema_version": RELEASE_SCHEMA_VERSION,
        "archive": str(archive_path),
        "archive_sha256": archive_hash,
        "source_pin": source_pin,
        "source_files": source_count,
        "files": len(expected_paths),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="verify SOURCE_PIN and SHA256SUMS")
    parser.add_argument("--build", action="store_true", help="build a deterministic release archive")
    parser.add_argument("--output", type=Path, help="archive output path used with --build")
    parser.add_argument(
        "--verify-archive",
        "--verify-release",
        dest="verify_archive",
        type=Path,
        help="verify an existing release archive",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.verify_archive is not None:
            if args.build or args.verify or args.output:
                raise ReleaseError("--verify-archive cannot be combined with build or source options")
            result = verify_archive(args.verify_archive)
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.build:
            if args.verify:
                raise ReleaseError("--build cannot be combined with --verify")
            if args.output is None:
                raise ReleaseError("--build requires --output outside the repository")
            result = build_release(args.output)
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.output:
            raise ReleaseError("--output requires --build")
        if args.verify:
            digest, count = verify_source_manifest()
            print(f"verified files={count} source_sha256={digest}")
        else:
            digest, count = write_source_manifest()
            print(f"wrote files={count} source_sha256={digest}")
        return 0
    except (OSError, ReleaseError, tarfile.TarError, ValueError) as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
