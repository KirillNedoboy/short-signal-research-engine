from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from app.infra.runtime_metadata import (
    ReleaseProvenanceError,
    resolve_code_version,
)

SHA_A = "a" * 40
SHA_B = "b" * 64


def _manifest(root: Path, schema_version: int = 1, code_sha: str = SHA_A) -> None:
    (root / "release-manifest.json").write_text(
        json.dumps({"schema_version": schema_version, "code_sha": code_sha}),
        encoding="utf-8",
    )


def test_valid_manifest_resolution(tmp_path) -> None:
    _manifest(tmp_path, code_sha=SHA_B)
    assert resolve_code_version(tmp_path) == SHA_B
def test_full_manifest_schema_two_resolution(tmp_path) -> None:
    payload = {
        "schema_version": 2,
        "code_sha": SHA_B,
        "source_release": SHA_B,
        "payload_sha256": "c" * 64,
    }
    (tmp_path / "release-manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    assert resolve_code_version(tmp_path) == SHA_B



def test_manifest_directory_sha_mismatch_fails(tmp_path) -> None:
    release = tmp_path / SHA_A
    release.mkdir()
    _manifest(release, code_sha=SHA_B)
    with pytest.raises(ReleaseProvenanceError, match="does not match release directory"):
        resolve_code_version(release)


def test_manifest_directory_sha_match_succeeds(tmp_path) -> None:
    release = tmp_path / SHA_A
    release.mkdir()
    _manifest(release, code_sha=SHA_A)
    assert resolve_code_version(release) == SHA_A


@pytest.mark.parametrize(
    "payload",
    [
        "{}",
        "not json",
        '{"schema_version": 3, "code_sha": "' + SHA_A + '"}',
        '{"schema_version": 1, "code_sha": "ZZZ"}',
        '{"schema_version": 1}',
    ],
)
def test_malformed_deployed_manifest_fails(tmp_path, payload: str) -> None:
    (tmp_path / "release-manifest.json").write_text(payload, encoding="utf-8")
    with pytest.raises(ReleaseProvenanceError):
        resolve_code_version(tmp_path)


def test_missing_manifest_and_no_git_fails(tmp_path) -> None:
    with pytest.raises(ReleaseProvenanceError, match="no valid release-manifest.json"):
        resolve_code_version(tmp_path)


def test_git_fallback_only_when_dot_git_exists(tmp_path) -> None:
    root = tmp_path / "checkout"
    root.mkdir()
    # No manifest, no .git -> hard failure (deployed-code contract).
    with pytest.raises(ReleaseProvenanceError):
        resolve_code_version(root)
    (root / ".git").mkdir()
    try:
        subprocess.run(
            ["git", "init", "-q"], cwd=root, check=True, capture_output=True, timeout=15
        )
        (root / "f.txt").write_text("x", encoding="utf-8")
        subprocess.run(["git", "add", "f.txt"], cwd=root, check=True, capture_output=True, timeout=15)
        subprocess.run(
            ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "c"],
            cwd=root, check=True, capture_output=True, timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        pytest.skip("git unavailable")
    version = resolve_code_version(root)
    assert len(version) == 40
    assert all(ch in "0123456789abcdef" for ch in version)


def test_stale_env_var_has_no_effect(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SHORT_BOT_CODE_VERSION", "0" * 40)
    _manifest(tmp_path, code_sha=SHA_A)
    assert resolve_code_version(tmp_path) == SHA_A


# --- scripts/write_release_manifest.py ---------------------------------------------

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "write_release_manifest.py"


def _run_writer(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], capture_output=True, text=True, timeout=30
    )


def test_writer_deterministic_content(tmp_path) -> None:
    out = tmp_path / "release-manifest.json"
    assert _run_writer("--code-sha", SHA_A, "--output", str(out)).returncode == 0
    assert json.loads(out.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "code_sha": SHA_A,
    }
    first = out.read_bytes()
    assert _run_writer("--code-sha", SHA_A, "--output", str(out)).returncode == 0
    assert out.read_bytes() == first  # idempotent same-SHA rewrite


@pytest.mark.parametrize("sha", ["A" * 40, "z" * 40, SHA_A[:-1], SHA_A + "0" * 25])
def test_writer_rejects_invalid_sha(tmp_path, sha: str) -> None:
    out = tmp_path / "release-manifest.json"
    result = _run_writer("--code-sha", sha, "--output", str(out))
    assert result.returncode == 2
    assert not out.exists()


def test_writer_refuses_conflicting_overwrite(tmp_path) -> None:
    out = tmp_path / "release-manifest.json"
    assert _run_writer("--code-sha", SHA_A, "--output", str(out)).returncode == 0
    result = _run_writer("--code-sha", SHA_B, "--output", str(out))
    assert result.returncode == 1
    assert json.loads(out.read_text(encoding="utf-8"))["code_sha"] == SHA_A
