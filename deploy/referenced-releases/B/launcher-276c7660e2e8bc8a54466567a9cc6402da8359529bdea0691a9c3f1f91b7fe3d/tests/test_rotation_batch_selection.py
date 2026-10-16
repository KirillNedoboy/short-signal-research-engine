from app.market.coverage import select_rotation_batch


def test_select_rotation_batch_prefers_uncovered_symbols_until_rotation_complete():
    eligible = ["A", "B", "C", "D", "E"]
    preferred = ["A", "B", "C", "D", "D"]

    batch = select_rotation_batch(
        eligible_symbols=eligible,
        already_scheduled={"A", "B", "C"},
        preferred_symbols=preferred,
        batch_size=3,
    )

    assert batch == ["D", "E"]


def test_select_rotation_batch_uses_preferred_order_for_new_rotation():
    eligible = ["A", "B", "C", "D"]
    preferred = ["C", "A", "D", "B"]

    batch = select_rotation_batch(
        eligible_symbols=eligible,
        already_scheduled=set(),
        preferred_symbols=preferred,
        batch_size=2,
    )

    assert batch == ["C", "A"]
