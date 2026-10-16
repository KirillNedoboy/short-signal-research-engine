from datetime import datetime, timezone

from app.observability.delivery_funnel import audit_delivery_funnel


def audit(*, links=(), provenance=True, event_id='trapped_longs:KAIAUSDT:123'):
    return audit_delivery_funnel(
        signals=[{'id': 3, 'symbol': 'KAIAUSDT', 'event_id': event_id}],
        provenances=[{'signal_id': 3}] if provenance else [],
        outbox=[{'id': 3, 'entity_type': 'SIGNAL', 'entity_id': 3, 'status': 'SENT'}],
        event_states=[{'symbol': 'KAIAUSDT', 'event_id': 'legacy:other', 'signal_id': None}],
        final_audits=[],
        trapped_links=links,
        now=datetime.now(timezone.utc),
    )


def test_verified_actionable_trapped_link_satisfies_event_invariant():
    assert audit(links={(3, 'KAIAUSDT', 'trapped_longs:KAIAUSDT:123')}) == []


def test_wrong_event_or_symbol_remains_a_violation():
    for link in [(3, 'KAIAUSDT', 'other'), (3, 'ETHUSDT', 'trapped_longs:KAIAUSDT:123')]:
        assert {'kind': 'signal_without_event_link', 'signal_id': 3} in audit(links={link})


def test_missing_provenance_is_not_silenced_by_candidate_link():
    findings = audit(links={(3, 'KAIAUSDT', 'trapped_longs:KAIAUSDT:123')}, provenance=False)
    assert {'kind': 'signal_without_provenance', 'signal_id': 3} in findings
    assert {'kind': 'signal_without_event_link', 'signal_id': 3} in findings


def test_unverified_trapped_event_remains_a_violation():
    assert {'kind': 'signal_without_event_link', 'signal_id': 3} in audit()
