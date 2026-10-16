from __future__ import annotations

import asyncio
from pathlib import Path

from app.config import AppConfig
from app.market_data.provider import CanonicalMarketDataProvider


def test_provider_forwards_shutdown_checkpoint_identity(tmp_path: Path) -> None:
    calls: dict[str, object] = {}

    class Store:
        def checkpoint(self, path, *, generation=None, metadata=None):  # noqa: ANN001
            calls.update(path=path, generation=generation, metadata=metadata)
            return 3

    provider = CanonicalMarketDataProvider(
        scanner=object(),
        config=AppConfig(
            canonical_market_data_provider_mode="HUB_PREFERRED",
            market_frame_store_mode="PREFERRED",
            market_frame_snapshot_path=str(tmp_path / "frames.json.gz"),
        ),
    )
    provider.attach_frame_store(Store())

    result = asyncio.run(
        provider.checkpoint_frame_store(
            generation=7,
            metadata={"pid": 4321, "reason": "SIGUSR1_SHUTDOWN"},
        )
    )

    assert result == 3
    assert calls == {
        "path": str(tmp_path / "frames.json.gz"),
        "generation": 7,
        "metadata": {"pid": 4321, "reason": "SIGUSR1_SHUTDOWN"},
    }
