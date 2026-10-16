"""Run unavoidable synchronous calls without pinning the asyncio loop."""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable, TypeVar


_Result = TypeVar("_Result")


def _set_result(future: asyncio.Future[_Result], value: _Result) -> None:
    if not future.done():
        future.set_result(value)


def _set_exception(future: asyncio.Future[_Result], error: BaseException) -> None:
    if not future.done():
        future.set_exception(error)


async def run_blocking(
    function: Callable[..., _Result], /, *args: Any, **kwargs: Any
) -> _Result:
    """Call a synchronous function on a daemon thread.

    Cancellation releases the event loop immediately.  The underlying call may
    finish later, but its daemon thread cannot hold process shutdown hostage.
    Repository methods create their own SQLAlchemy sessions per call, so they
    remain isolated from the event-loop thread.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[_Result] = loop.create_future()

    def invoke() -> None:
        try:
            value = function(*args, **kwargs)
        except BaseException as error:  # propagate the original failure
            try:
                loop.call_soon_threadsafe(_set_exception, future, error)
            except RuntimeError:
                # The loop may close after cancellation; the daemon call is
                # intentionally allowed to finish without a callback.
                return
        else:
            try:
                loop.call_soon_threadsafe(_set_result, future, value)
            except RuntimeError:
                return

    threading.Thread(target=invoke, name="outcome-db-call", daemon=True).start()
    return await future
