from app.outcomes.evaluator import OutcomeEvaluator


def test_outcome_evaluator_calculates_short_side_metrics(make_frame, make_signal_record) -> None:
    prices = [105.0, 104.5, 103.0, 100.0, 99.0, 98.5, 99.5, 100.5, 101.0]
    frame = make_frame(prices)
    signal = make_signal_record()
    evaluator = OutcomeEvaluator()

    outcome = evaluator.evaluate(signal, frame)

    assert outcome is not None
    assert outcome.mfe_pct > 0
    assert outcome.mae_pct >= 0
    assert outcome.reached_vwap is True
    assert outcome.time_to_vwap_minutes is not None


def test_outcome_evaluator_classifies_clean_tp(make_frame, make_signal_record) -> None:
    prices = [105.0, 103.5, 101.5, 99.5, 98.5, 99.0, 99.5]
    frame = make_frame(prices)
    signal = make_signal_record()
    evaluator = OutcomeEvaluator()

    outcome = evaluator.evaluate(signal, frame)

    assert outcome is not None
    assert outcome.risk_adjusted_status == "CLEAN_TP"
    assert outcome.is_clean_short is True


def test_outcome_evaluator_classifies_dirty_tp_after_high_mae(make_frame, make_signal_record) -> None:
    prices = [105.0, 110.5, 108.0, 103.0, 100.5, 98.5, 99.0]
    frame = make_frame(prices)
    signal = make_signal_record()
    evaluator = OutcomeEvaluator()

    outcome = evaluator.evaluate(signal, frame)

    assert outcome is not None
    assert outcome.risk_adjusted_status == "DIRTY_TP_HIGH_MAE"
    assert outcome.is_clean_short is False


def test_outcome_evaluator_classifies_squeeze_before_tp(make_frame, make_signal_record) -> None:
    prices = [105.0, 116.5, 112.0, 108.0, 101.0, 98.0, 99.0]
    frame = make_frame(prices)
    signal = make_signal_record()
    evaluator = OutcomeEvaluator()

    outcome = evaluator.evaluate(signal, frame)

    assert outcome is not None
    assert outcome.risk_adjusted_status == "SQUEEZE_BEFORE_TP"
    assert outcome.is_squeeze_before_tp is True


def test_outcome_evaluator_classifies_sl_or_bad(make_frame, make_signal_record) -> None:
    prices = [105.0, 110.0, 111.5, 112.0, 111.0, 110.5, 110.0]
    frame = make_frame(prices)
    signal = make_signal_record()
    evaluator = OutcomeEvaluator()

    outcome = evaluator.evaluate(signal, frame)

    assert outcome is not None
    assert outcome.risk_adjusted_status == "SL_OR_BAD"
