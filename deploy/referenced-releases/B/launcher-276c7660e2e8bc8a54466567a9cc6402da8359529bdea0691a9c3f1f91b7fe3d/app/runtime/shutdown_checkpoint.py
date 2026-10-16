"""SIGUSR1 checkpoint request with a bounded shutdown barrier."""

from __future__ import annotations

import asyncio
import inspect
import os
import signal
from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass(frozen=True, slots=True)
class CheckpointAck:
    generation: int
    pid: int
    symbols: int
    acknowledged: bool = True


Checkpoint = Callable[..., Awaitable[int | None] | int | None]
_SIGUSR1 = getattr(signal, "SIGUSR1", signal.SIGTERM)


class ShutdownCheckpointBarrier:
    """Send SIGUSR1 and wait for a matching checkpoint acknowledgement."""

    def __init__(
        self,
        *,
        checkpoint: Checkpoint,
        pid_provider: Callable[[], int] = os.getpid,
        signal_sender: Callable[[int, signal.Signals], None] = os.kill,
        signal_number: signal.Signals = _SIGUSR1,
    ) -> None:
        self._checkpoint = checkpoint
        self._pid_provider = pid_provider
        self._signal_sender = signal_sender
        self._signal_number = signal_number
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: tuple[int, int, asyncio.Future[CheckpointAck]] | None = None
        self._last_generation = 0
        self._tasks: set[asyncio.Task[None]] = set()
        self._installed = False

    @property
    def installed(self) -> bool:
        return self._installed

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(self._signal_number, self.notify_signal)
        self._loop = loop
        self._installed = True

    def uninstall(self) -> None:
        if self._installed and self._loop is not None:
            self._loop.remove_signal_handler(self._signal_number)
        self._installed = False

    async def request(self, *, timeout_sec: float) -> CheckpointAck:
        if timeout_sec <= 0:
            raise ValueError("checkpoint timeout must be positive")
        if self._pending is not None:
            raise RuntimeError("checkpoint request already pending")
        loop = asyncio.get_running_loop()
        self._loop = loop
        generation = self._last_generation + 1
        pid = int(self._pid_provider())
        future: asyncio.Future[CheckpointAck] = loop.create_future()
        self._pending = (generation, pid, future)
        try:
            self._signal_sender(pid, self._signal_number)
        except BaseException:
            self._pending = None
            raise
        try:
            ack = await asyncio.wait_for(future, timeout=timeout_sec)
        finally:
            if self._pending is not None and self._pending[2] is future:
                self._pending = None
        if (
            not ack.acknowledged
            or ack.generation != generation
            or ack.pid != pid
        ):
            raise RuntimeError("checkpoint acknowledgement identity mismatch")
        self._last_generation = generation
        return ack

    def notify_signal(self) -> None:
        """Handle SIGUSR1 without doing blocking work in the signal callback."""
        if self._pending is None or self._loop is None:
            return
        generation, pid, future = self._pending
        if future.done():
            return
        task = self._loop.create_task(self._run_checkpoint(generation, pid, future))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_checkpoint(
        self,
        generation: int,
        pid: int,
        future: asyncio.Future[CheckpointAck],
    ) -> None:
        try:
            result = self._checkpoint(generation=generation, pid=pid)
            if inspect.isawaitable(result):
                result = await result
            if result is None:
                raise RuntimeError("checkpoint produced no acknowledgement")
            ack = CheckpointAck(
                generation=generation,
                pid=pid,
                symbols=int(result),
            )
            if not future.done():
                future.set_result(ack)
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
