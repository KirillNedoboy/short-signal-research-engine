"""Stable non-secret metadata for one bot runtime.

Provenance is manifest-first: every built release carries an immutable
``release-manifest.json`` next to the code.  Git is consulted only in a
developer checkout, and a deployed release without a valid manifest fails
closed instead of reporting ``unknown``.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

MANIFEST_NAME = "release-manifest.json"
_SHA_RE = re.compile(r"^[0-9a-f]{40,64}$")


class ReleaseProvenanceError(RuntimeError):
    """The deployed release identity could not be established."""


def release_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_code_version(root: Path | None = None) -> str:
    """Return the immutable release SHA from its manifest (git in dev only)."""

    root = release_root() if root is None else Path(root)
    manifest_path = root / MANIFEST_NAME
    if manifest_path.exists():
        code_sha = _validated_manifest_sha(manifest_path)
        directory_name = root.resolve().name
        if (
            _SHA_RE.fullmatch(directory_name)
            and directory_name != code_sha
        ):
            raise ReleaseProvenanceError(
                f"manifest code_sha {code_sha} does not match release directory "
                f"{directory_name}"
            )
        return code_sha
    if (root / ".git").exists():
        version = _git_head(root)
        if version:
            return version
    raise ReleaseProvenanceError(
        f"no valid {MANIFEST_NAME} under {root} and no developer .git checkout"
    )


def _validated_manifest_sha(manifest_path: Path) -> str:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReleaseProvenanceError(f"malformed {manifest_path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") not in (1, 2):
        raise ReleaseProvenanceError(f"unsupported manifest schema in {manifest_path}")
    code_sha = payload.get("code_sha")
    if not isinstance(code_sha, str) or not _SHA_RE.fullmatch(code_sha):
        raise ReleaseProvenanceError(f"invalid manifest code_sha in {manifest_path}")
    return code_sha


def _git_head(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    version = result.stdout.strip()
    return version if _SHA_RE.fullmatch(version) else None
