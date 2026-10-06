from app.features.builder import _extract_liquidity


def test_partial_liquidity_payload_is_not_available():
    result = _extract_liquidity({"spread_pct": 0.1})
    assert result["liquidity_available"] is False


def test_complete_liquidity_payload_is_available():
    result = _extract_liquidity(
        {
            "spread_pct": 0.1,
            "slippage_pct": 0.05,
            "orderbook_depth_usdt_1pct": 100_000.0,
            "orderbook_depth_usdt_2pct": 200_000.0,
        }
    )
    assert result["liquidity_available"] is True
