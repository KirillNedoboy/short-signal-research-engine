"""C facade over A's per-symbol mutation coordinator."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from .symbol_mutation import SymbolMutationCoordinator


class PerSymbolCoordinator:
    """Use the A lock implementation while exposing the C ``hold`` seam."""

    def __init__(self) -> None:
        self._delegate = SymbolMutationCoordinator()

    @asynccontextmanager
    async def hold(self, symbol: str) -> AsyncIterator[None]:
        async with self._delegate.mutation(symbol):
            yield


__all__ = ["PerSymbolCoordinator"]

