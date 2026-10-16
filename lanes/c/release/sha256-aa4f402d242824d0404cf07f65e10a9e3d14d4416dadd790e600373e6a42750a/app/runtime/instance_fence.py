"""Linux process fence for runtime entrypoints."""

from __future__ import annotations

import logging
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Protocol, TextIO


class InstanceFenceError(RuntimeError):
    """Base error for process-level runtime fence acquisition failures."""


class InstanceFenceHeldError(InstanceFenceError):
    """Raised when another process already owns the runtime fence."""

    def __init__(self, identity: str, path: Path) -> None:
        self.identity = identity
        self.path = path
        super().__init__(f"runtime fence already held for {identity} at {path}")


class InstanceFenceUnsupportedError(InstanceFenceError):
    """Raised when the authoritative Linux flock fence is unavailable."""

    def __init__(self, identity: str, path: Path) -> None:
        self.identity = identity
        self.path = path
        super().__init__(f"runtime fence requires Linux fcntl.flock for {identity} at {path}")


class _Fence(Protocol):
    identity: str
    path: Path

    def acquire(self) -> "_Fence": ...

    def close(self) -> None: ...


class InstanceFence:
    """Hold one non-blocking advisory flock for a process lifetime."""

    def __init__(
        self,
        identity: str = "short-telegram-bot-lite",
        path: Path = Path("/run/short-telegram-bot-lite.lock"),
    ) -> None:
        self.identity = identity
        self.path = path
        self._handle: TextIO | None = None

    def acquire(self) -> "InstanceFence":
        """Acquire the Linux-only fence without waiting for a competing process."""
        if sys.platform != "linux":
            raise InstanceFenceUnsupportedError(self.identity, self.path)
        if self._handle is not None:
            return self

        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise InstanceFenceHeldError(self.identity, self.path) from exc
        except OSError:
            handle.close()
            raise
        self._handle = handle
        return self

    def close(self) -> None:
        """Release the descriptor-held lock while deliberately retaining the lockfile."""
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            if sys.platform == "linux":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> "InstanceFence":
        return self.acquire()

    def __exit__(self, *_: object) -> None:
        self.close()


async def run_fenced(
    async_entrypoint: Callable[[], Awaitable[object]],
    *,
    fence_factory: Callable[[], _Fence] = InstanceFence,
) -> int:
    """Run an async entrypoint only while its process-level fence is held."""
    logger = logging.getLogger(__name__)
    fence = fence_factory()
    try:
        fence.acquire()
    except (InstanceFenceHeldError, InstanceFenceUnsupportedError) as exc:
        logger.error(
            "Runtime fence unavailable | identity=%s path=%s reason=%s",
            fence.identity,
            fence.path,
            exc,
        )
        return 1
    except OSError as exc:
        logger.error(
            "Runtime fence unavailable | identity=%s path=%s reason=%s",
            fence.identity,
            fence.path,
            f"{type(exc).__name__}: {exc}",
        )
        return 1

    try:
        await async_entrypoint()
    finally:
        fence.close()
    return 0
