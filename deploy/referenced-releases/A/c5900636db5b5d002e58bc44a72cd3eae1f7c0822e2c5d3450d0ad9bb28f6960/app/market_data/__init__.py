"""In-memory public-market data shadowing primitives."""

from .backfill import BackfillResult, BackfillStatus, ClosedCandleBackfiller
from .continuity import GapEvent, GapHealth, GapState, GapTracker
from .evidence import (
    DEFAULT_PROVIDER_EVIDENCE_PATH,
    MAX_EVIDENCE_BYTES,
    MAX_RECENT_SAMPLES,
    MAX_SAMPLE_BYTES,
    PROVIDER_EVIDENCE_SCHEMA_VERSION,
    ProviderEvidencePublisher,
    ProviderEvidenceReport,
    ProviderEvidenceSample,
    normalize_provider_evidence,
)
from .hub import MarketDataHub
from .models import (
    BYBIT_PUBLIC_WS_SOURCE,
    REST_BACKFILL_SOURCE,
    ConnectionEpoch,
    HealthStatus,
    MarketCandle,
    MarketDataHealth,
    MarketTickerSnapshot,
    SubscriptionState,
)
from .parity import KlineParityResult, ParityMonitor, TickerProbeResult
from .provider import (
    CanonicalMarketDataProvider,
    CanonicalScanSnapshot,
    ComparisonClassification,
    DecisionMarketSnapshot,
    FallbackReason,
    FrozenCandleFrame,
    MarketDataSource,
    ResolvedTickerGroup,
    SourceProvenance,
)

__all__ = [
    "BYBIT_PUBLIC_WS_SOURCE",
    "REST_BACKFILL_SOURCE",
    "BackfillResult",
    "BackfillStatus",
    "ClosedCandleBackfiller",
    "DEFAULT_PROVIDER_EVIDENCE_PATH",
    "MAX_EVIDENCE_BYTES",
    "MAX_RECENT_SAMPLES",
    "MAX_SAMPLE_BYTES",
    "CanonicalMarketDataProvider",
    "CanonicalScanSnapshot",
    "ComparisonClassification",
    "ConnectionEpoch",
    "DecisionMarketSnapshot",
    "FallbackReason",
    "FrozenCandleFrame",
    "GapEvent",
    "GapHealth",
    "GapState",
    "GapTracker",
    "HealthStatus",
    "KlineParityResult",
    "MarketCandle",
    "MarketDataSource",
    "MarketDataHealth",
    "MarketDataHub",
    "MarketTickerSnapshot",
    "ParityMonitor",
    "PROVIDER_EVIDENCE_SCHEMA_VERSION",
    "ProviderEvidencePublisher",
    "ProviderEvidenceReport",
    "ProviderEvidenceSample",
    "ResolvedTickerGroup",
    "SourceProvenance",
    "SubscriptionState",
    "TickerProbeResult",
    "normalize_provider_evidence",
]
