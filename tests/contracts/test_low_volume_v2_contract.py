from app.signals.climax import evaluate_climax_bundle
from tests.contracts.helpers import climax_config, ready_low_volume_inputs


def test_v2_dispatch_is_additive_and_v1_remains_selected_when_disabled(make_features, make_event_state, make_frame):
    state, features, frame = ready_low_volume_inputs(make_features, make_event_state, make_frame)
    legacy = evaluate_climax_bundle(state, features, frame, climax_config(volume_climax_unwind_enabled=False))
    assert "LOW_VOLUME_EXTENSION_FAILURE" in legacy.branch_evaluations
    assert "LOW_VOLUME_EXTENSION_FAILURE_V2" not in legacy.branch_evaluations
    assert legacy.selected.subtype == "LOW_VOLUME_EXTENSION_FAILURE"


def test_v2_dispatch_has_distinct_branch(make_features, make_event_state, make_frame):
    state, features, frame = ready_low_volume_inputs(make_features, make_event_state, make_frame)
    result = evaluate_climax_bundle(
        state, features, frame,
        climax_config(volume_climax_unwind_enabled=False, low_volume_extension_v2_enabled=True),
    )
    assert "LOW_VOLUME_EXTENSION_FAILURE" in result.branch_evaluations
    assert "LOW_VOLUME_EXTENSION_FAILURE_V2" in result.branch_evaluations
    assert result.branch_evaluations["LOW_VOLUME_EXTENSION_FAILURE_V2"].metadata["strategy_subtype"] == "LOW_VOLUME_EXTENSION_FAILURE_V2"
