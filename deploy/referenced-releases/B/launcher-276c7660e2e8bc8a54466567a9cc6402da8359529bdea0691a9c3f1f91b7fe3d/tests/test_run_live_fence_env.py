"""Live-runner fence environment parameterization contracts.

Both fenced entrypoints must keep byte-identical lane-A defaults when the
environment is empty and must honor the lane-B overrides exactly, so a second
lane can never contend for the production flock.
"""

from __future__ import annotations

from pathlib import Path


def test_live_runner_preserves_v1_fence_defaults(monkeypatch) -> None:
    import scripts.run_live as run_live

    monkeypatch.delenv("SHORT_BOT_INSTANCE_IDENTITY", raising=False)
    monkeypatch.delenv("SHORT_BOT_INSTANCE_LOCK_PATH", raising=False)
    fence = run_live._instance_fence_from_environment()
    assert fence.identity == "short-telegram-bot-lite"
    assert fence.path == Path("/run/short-telegram-bot-lite.lock")


def test_live_runner_accepts_isolated_fence_environment(
    monkeypatch, tmp_path: Path
) -> None:
    import scripts.run_live as run_live

    lock_path = tmp_path / "lane-b" / "short-telegram-bot-lite-lane-b.lock"
    monkeypatch.setenv("SHORT_BOT_INSTANCE_IDENTITY", "short-telegram-bot-lite-lane-b")
    monkeypatch.setenv("SHORT_BOT_INSTANCE_LOCK_PATH", str(lock_path))
    fence = run_live._instance_fence_from_environment()
    assert fence.identity == "short-telegram-bot-lite-lane-b"
    assert fence.path == lock_path


def test_once_runner_preserves_v1_fence_defaults(monkeypatch) -> None:
    import scripts.run_once as run_once

    monkeypatch.delenv("SHORT_BOT_INSTANCE_IDENTITY", raising=False)
    monkeypatch.delenv("SHORT_BOT_INSTANCE_LOCK_PATH", raising=False)
    fence = run_once._instance_fence_from_environment()
    assert fence.identity == "short-telegram-bot-lite"
    assert fence.path == Path("/run/short-telegram-bot-lite.lock")


def test_once_runner_accepts_isolated_fence_environment(
    monkeypatch, tmp_path: Path
) -> None:
    import scripts.run_once as run_once

    lock_path = tmp_path / "lane-b" / "short-telegram-bot-lite-lane-b.lock"
    monkeypatch.setenv("SHORT_BOT_INSTANCE_IDENTITY", "short-telegram-bot-lite-lane-b")
    monkeypatch.setenv("SHORT_BOT_INSTANCE_LOCK_PATH", str(lock_path))
    fence = run_once._instance_fence_from_environment()
    assert fence.identity == "short-telegram-bot-lite-lane-b"
    assert fence.path == lock_path
