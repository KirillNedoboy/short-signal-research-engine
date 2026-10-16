"""Process-level runtime fence contracts."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from app.runtime.instance_fence import (
    InstanceFence,
    InstanceFenceHeldError,
    InstanceFenceUnsupportedError,
    run_fenced,
)


class _RecordingFence:
    identity = "test-runtime"
    path = Path("test-runtime.lock")

    def __init__(self) -> None:
        self.acquired = False
        self.closed = False

    def acquire(self) -> "_RecordingFence":
        self.acquired = True
        return self

    def close(self) -> None:
        self.closed = True


def test_run_fenced_acquires_before_running_entrypoint_and_closes_afterward() -> None:
    fence = _RecordingFence()
    observed: list[bool] = []

    async def entrypoint() -> None:
        observed.append(fence.acquired)

    result = asyncio.run(run_fenced(entrypoint, fence_factory=lambda: fence))

    assert result == 0
    assert observed == [True]
    assert fence.closed is True


def test_run_fenced_logs_identity_path_and_reason_when_fence_is_held(caplog: pytest.LogCaptureFixture) -> None:
    class _HeldFence(_RecordingFence):
        identity = "held-runtime"
        path = Path("held-runtime.lock")

        def acquire(self) -> "_HeldFence":
            raise InstanceFenceHeldError(self.identity, self.path)

    called = False

    async def entrypoint() -> None:
        nonlocal called
        called = True

    with caplog.at_level(logging.ERROR):
        result = asyncio.run(run_fenced(entrypoint, fence_factory=_HeldFence))

    assert result != 0
    assert called is False
    assert "held-runtime" in caplog.text
    assert "held-runtime.lock" in caplog.text
    assert "already held" in caplog.text


def test_instance_fence_identity_and_path_are_explicit(tmp_path: Path) -> None:
    first = InstanceFence(identity="runtime-a", path=tmp_path / "a.lock")
    second = InstanceFence(identity="runtime-b", path=tmp_path / "b.lock")
    assert first.identity == "runtime-a"
    assert second.identity == "runtime-b"
    assert first.path != second.path


def test_instance_fence_fails_closed_when_linux_flock_is_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import app.runtime.instance_fence as instance_fence_module

    monkeypatch.setattr(instance_fence_module.sys, "platform", "win32")

    with pytest.raises(InstanceFenceUnsupportedError):
        InstanceFence(path=tmp_path / "runtime.lock").acquire()


def test_instance_fence_reacquires_after_normal_close(tmp_path: Path) -> None:
    if sys.platform != "linux":
        pytest.skip("fcntl.flock is Linux-only")
    lock_path = tmp_path / "runtime.lock"
    first = InstanceFence(path=lock_path).acquire()
    first.close()
    second = InstanceFence(path=lock_path).acquire()
    second.close()
    assert lock_path.exists()


@pytest.mark.skipif(sys.platform != "linux", reason="fcntl.flock subprocess contract is Linux-only")
def test_linux_fence_blocks_second_process_and_reacquires_after_owner_exit(tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime.lock"
    owner_ready = "owner-acquired"
    blocked = "second-blocked"
    reacquired = "third-acquired"
    owner_script = "\n".join(
        [
            "import sys",
            "from pathlib import Path",
            "from app.runtime.instance_fence import InstanceFence",
            f"fence = InstanceFence(path=Path({str(lock_path)!r}))",
            "fence.acquire()",
            f"print({owner_ready!r}, flush=True)",
            "sys.stdin.read(1)",
        ]
    )
    contender_script = "\n".join(
        [
            "from pathlib import Path",
            "from app.runtime.instance_fence import InstanceFence, InstanceFenceHeldError",
            f"fence = InstanceFence(path=Path({str(lock_path)!r}))",
            "try:",
            "    fence.acquire()",
            f"except InstanceFenceHeldError: print({blocked!r}, flush=True)",
            "else: print('unexpected-acquire', flush=True); raise SystemExit(2)",
        ]
    )
    reacquire_script = "\n".join(
        [
            "from pathlib import Path",
            "from app.runtime.instance_fence import InstanceFence",
            f"fence = InstanceFence(path=Path({str(lock_path)!r})).acquire()",
            f"print({reacquired!r}, flush=True)",
            "fence.close()",
        ]
    )
    environment = {**os.environ, "PYTHONPATH": str(Path.cwd())}
    owner = subprocess.Popen(
        [sys.executable, "-c", owner_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        text=True,
        env=environment,
    )
    try:
        assert owner.stdout is not None
        assert owner.stdout.readline().strip() == owner_ready
        assert lock_path.exists()
        second = subprocess.run(
            [sys.executable, "-c", contender_script],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        assert second.returncode == 0
        assert second.stdout.strip() == blocked
        assert owner.poll() is None
    finally:
        assert owner.stdin is not None
        owner.stdin.write("\n")
        owner.stdin.flush()
        owner.wait(timeout=5)
    third = subprocess.run(
        [sys.executable, "-c", reacquire_script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    assert third.returncode == 0
    assert third.stdout.strip() == reacquired
    assert lock_path.exists()


@pytest.mark.skipif(sys.platform != "linux", reason="fcntl.flock crash contract is Linux-only")
def test_linux_fence_reacquires_after_abnormal_owner_termination(tmp_path: Path) -> None:
    lock_path = tmp_path / "crash-runtime.lock"
    owner_script = "\n".join(
        [
            "import os, sys",
            "from pathlib import Path",
            "from app.runtime.instance_fence import InstanceFence",
            f"fence = InstanceFence(path=Path({str(lock_path)!r})).acquire()",
            "print('owner-acquired', flush=True)",
            "os.read(sys.stdin.fileno(), 1)",
        ]
    )
    environment = {**os.environ, "PYTHONPATH": str(Path.cwd())}
    owner = subprocess.Popen(
        [sys.executable, "-c", owner_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        text=True,
        env=environment,
    )
    try:
        assert owner.stdout is not None
        assert owner.stdout.readline().strip() == "owner-acquired"
        assert lock_path.exists()
        owner.kill()
        owner.wait(timeout=5)
        assert owner.returncode != 0
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=5)
    recovered = InstanceFence(path=lock_path).acquire()
    recovered.close()
