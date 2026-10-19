"""Signal engine entrypoint."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta

from app.config import AppConfig
from app.domain import CandidateEvaluation, EventState, EventStatus, SignalDecision, SignalType, ShortZone, SymbolFeatures
from app.signals.filters import evaluate_core_filters
from app.signals.scoring import score_setup
from app.signals.squeeze_guard import evaluate_squeeze_guard


class SignalEngine:
    """Create Telegram signal decisions from a mature event."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def evaluate(
        self,
        state: EventState,
        features: SymbolFeatures,
        zone: ShortZone,
        signal_time: datetime,
    ) -> SignalDecision | None:
        return self.analyze(state, features, zone, signal_time).decision

    def analyze(
        self,
        state: EventState,
        features: SymbolFeatures,
        zone: ShortZone,
        signal_time: datetime,
    ) -> CandidateEvaluation:
        evaluation = CandidateEvaluation(checked=False)
        if state.state not in {EventStatus.PULLBACK_OBSERVED, EventStatus.SHORT_ZONE_ACTIVE}:
            return evaluation
        evaluation.checked = True

        if state.expires_at and signal_time >= state.expires_at:
            evaluation.reject_reasons.append("stale")
            return evaluation
        if state.event_high_time and signal_time >= state.event_high_time + timedelta(minutes=self._config.max_signal_age_minutes):
            evaluation.reject_reasons.append("stale")
            return evaluation
        if not (zone.low <= features.price <= zone.high):
            evaluation.reject_reasons.append("short_zone_not_active")
            return evaluation

        core_filters = evaluate_core_filters(features, self._config)
        core_watch_flags = _core_watch_flags(core_filters, features, self._config)
        score, breakdown, risk_flags, breakout_risk = score_setup(features, zone, self._config)
        liquidity_level = _liquidity_block_level(features, self._config)
        squeeze = evaluate_squeeze_guard(features, self._config)
        score = max(0, score - _squeeze_penalty(squeeze.action, squeeze.level))
        grade = _grade_from_score(score)
        blockers = _derive_blockers(features, core_filters, liquidity_level, squeeze)
        reject_reasons = _derive_reject_reasons(core_filters, score, features, liquidity_level, squeeze, breakout_risk)

        risk_flags = [*risk_flags, *core_watch_flags, *_squeeze_warning_lines(squeeze)]
        evaluation.score = score
        evaluation.grade = grade
        evaluation.blockers = blockers
        evaluation.risk_flags = risk_flags
        evaluation.reject_reasons = reject_reasons
        evaluation.close_to_watch = bool(score >= self._config.watch_min_score and blockers)
        evaluation.squeeze_risk_score = squeeze.score
        evaluation.squeeze_risk_level = squeeze.level
        evaluation.squeeze_risk_reasons = squeeze.reasons
        evaluation.squeeze_guard_action = squeeze.action
        evaluation.data_quality_warnings = squeeze.data_quality

        all_core_pass = all(core_filters.values())
        action_blocked = liquidity_level == "block" or squeeze.block_signal or breakout_risk
        public_grade_allowed = _public_grade_allowed(grade, self._config)
        if all_core_pass and score >= 50 and public_grade_allowed and not action_blocked and not squeeze.force_watch:
            decision = self._build_decision(
                state=state,
                features=features,
                zone=zone,
                signal_time=signal_time,
                score=score,
                grade=grade,
                risk_flags=risk_flags,
                breakdown=breakdown,
                blockers=blockers,
                squeeze=squeeze,
                signal_type=_actionable_signal_type(score, features, breakout_risk),
                actionable=True,
                decision_type="SIGNAL",
            )
            evaluation.decision = decision
            return evaluation

        watch_candidate = self._is_watch_candidate(
            score=score,
            blockers=blockers,
            core_watch_flags=core_watch_flags,
            liquidity_level=liquidity_level,
            squeeze=squeeze,
            all_core_pass=all_core_pass,
        )
        non_public_grade = not public_grade_allowed and grade == "C"
        if non_public_grade and self._config.grade_c_mode == "suppress":
            return evaluation
        if (
            non_public_grade
            and self._config.enable_watch_candidates
            and self._config.grade_c_mode == "watch_only"
            and score >= self._config.watch_min_score
        ):
            watch_candidate = True
        if watch_candidate:
            decision = self._build_decision(
                state=state,
                features=features,
                zone=zone,
                signal_time=signal_time,
                score=score,
                grade=grade,
                risk_flags=risk_flags,
                breakdown=breakdown,
                blockers=blockers,
                squeeze=squeeze,
                signal_type=SignalType.WATCH,
                actionable=False,
                decision_type="WATCH",
            )
            evaluation.decision = decision
            evaluation.close_to_watch = True
            return evaluation

        return evaluation

    def _is_watch_candidate(
        self,
        *,
        score: int,
        blockers: list[str],
        core_watch_flags: list[str],
        liquidity_level: str,
        squeeze,
        all_core_pass: bool,
    ) -> bool:
        if not self._config.enable_watch_candidates:
            return False
        if score < self._config.watch_min_score:
            return False
        if squeeze.block_signal:
            return False
        return bool(
            blockers
            or core_watch_flags
            or liquidity_level == "watch"
            or squeeze.force_watch
            or (not all_core_pass and score >= self._config.watch_min_score)
        )

    def _build_decision(
        self,
        *,
        state: EventState,
        features: SymbolFeatures,
        zone: ShortZone,
        signal_time: datetime,
        score: int,
        grade: str,
        risk_flags: list[str],
        breakdown: dict[str, float],
        blockers: list[str],
        squeeze,
        signal_type: SignalType,
        actionable: bool,
        decision_type: str,
    ) -> SignalDecision:
        snapshot = asdict(features)
        snapshot["signal_vwap"] = features.vwap
        return SignalDecision(
            symbol=features.symbol,
            event_id=state.event_id,
            signal_type=signal_type,
            grade=grade,
            score=score,
            market_price=features.price,
            short_zone_low=zone.low,
            short_zone_high=zone.high,
            signal_time=signal_time,
            reasons=[
                f"Dist to VWAP: +{features.dist_to_vwap_pct:.1f}%",
                f"Upper wick: {features.upper_wick_ratio:.2f}",
                f"Rejection from high: {features.rejection_from_high_pct:.2f}%",
                f"Volume z-score 30m: {features.vol_zscore_30m:.2f}",
                f"Pullback from event high: {(features.pullback_from_high_pct or 0.0):.2f}%",
                f"Dist to EMA20 ATR: {features.dist_to_ema20_atr:.2f}",
                f"RSI 15m: {features.rsi_15m:.1f}",
            ],
            risk_flags=risk_flags,
            features_snapshot=snapshot,
            score_breakdown=breakdown,
            decision_type=decision_type,
            actionable=actionable,
            lifecycle_state=state.state.value,
            blockers=blockers,
            squeeze_risk_score=squeeze.score,
            squeeze_risk_level=squeeze.level,
            squeeze_risk_reasons=squeeze.reasons,
            squeeze_guard_action=squeeze.action,
            data_quality_warnings=squeeze.data_quality,
        )


def _actionable_signal_type(score: int, features: SymbolFeatures, breakout_risk: bool) -> SignalType:
    strong_rejection = features.upper_wick_ratio >= 0.18 or features.rejection_from_high_pct >= 1.2
    if score >= 75 and strong_rejection and not breakout_risk:
        return SignalType.AGGRESSIVE
    return SignalType.CONFIRM


def _public_grade_allowed(grade: str, config: AppConfig) -> bool:
    if grade == "C" and config.send_grade_c_to_telegram:
        return True
    return _grade_rank(grade) <= _grade_rank(config.min_public_signal_grade)


def _grade_rank(grade: str) -> int:
    return {"A": 0, "B": 1, "C": 2}[grade]


def _grade_from_score(score: int) -> str:
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    return "C"


def _core_watch_flags(core_filters: dict[str, bool], features: SymbolFeatures, config: AppConfig) -> list[str]:
    failed = {name for name, passed in core_filters.items() if not passed}
    if not failed or not failed <= {"rejection", "volume"}:
        return []
    flags: list[str] = []
    if "rejection" in failed:
        wick_near = features.upper_wick_ratio >= config.upper_wick_min * 0.85
        rejection_near = features.rejection_from_high_pct >= config.rejection_min * 0.85
        if wick_near or rejection_near:
            flags.append("Rejection is moderately below actionable threshold.")
    if "volume" in failed and features.vol_zscore_30m >= _watch_volume_threshold(config):
        flags.append("Volume z-score is moderately below actionable threshold.")
    return flags


def _watch_volume_threshold(config: AppConfig) -> float:
    return min(config.vol_zscore_min * 0.85, 0.69)


def _liquidity_block_level(features: SymbolFeatures, config: AppConfig) -> str:
    if not features.liquidity_available:
        return "block"
    moderate_failures = 0
    severe_failures = 0
    if features.spread_pct is not None and features.spread_pct > config.max_spread_pct:
        moderate_failures += 1
        severe_failures += int(features.spread_pct > config.max_spread_pct * 1.5)
    if features.slippage_pct is not None and features.slippage_pct > config.max_slippage_pct:
        moderate_failures += 1
        severe_failures += int(features.slippage_pct > config.max_slippage_pct * 1.5)
    if features.orderbook_depth_usdt_1pct is not None and features.orderbook_depth_usdt_1pct < config.min_orderbook_depth_usdt_1pct:
        moderate_failures += 1
        severe_failures += int(features.orderbook_depth_usdt_1pct < config.min_orderbook_depth_usdt_1pct * 0.5)
    if features.orderbook_depth_usdt_2pct is not None and features.orderbook_depth_usdt_2pct < config.min_orderbook_depth_usdt_2pct:
        moderate_failures += 1
        severe_failures += int(features.orderbook_depth_usdt_2pct < config.min_orderbook_depth_usdt_2pct * 0.5)
    if severe_failures or moderate_failures >= 2:
        return "block"
    if moderate_failures == 1:
        return "watch"
    return "ok"


def _derive_blockers(
    features: SymbolFeatures,
    core_filters: dict[str, bool],
    liquidity_level: str,
    squeeze,
) -> list[str]:
    blockers: list[str] = []
    if not core_filters.get("pullback", False):
        blockers.append("no_pullback")
    pullback = features.pullback_from_high_pct or 0.0
    if 0 < pullback < 2.0:
        blockers.append("shallow_pullback")
    if not core_filters.get("rejection", False):
        blockers.append("weak_rejection")
    if not features.latest_failed_retest:
        blockers.append("retest_not_failed")
    if not features.liquidity_available:
        blockers.append("liquidity_missing")
    elif liquidity_level in {"watch", "block"}:
        if features.spread_pct is not None and features.spread_pct > 0:
            blockers.append("spread_too_wide")
    blockers.extend(reason for reason in squeeze.reasons if reason in {
        "funding_negative_trap",
        "shallow_pullback",
        "retest_not_failed",
        "spread_too_wide",
    })
    blockers.extend(warning for warning in squeeze.data_quality if warning in {"derivatives_missing", "liquidity_missing", "oi_missing"})
    return _dedupe(blockers)


def _derive_reject_reasons(
    core_filters: dict[str, bool],
    score: int,
    features: SymbolFeatures,
    liquidity_level: str,
    squeeze,
    breakout_risk: bool,
) -> list[str]:
    reasons: list[str] = []
    if not core_filters.get("dist_to_vwap", False):
        reasons.append("score_too_low")
    if not core_filters.get("pullback", False):
        reasons.append("no_pullback")
    if not core_filters.get("rejection", False):
        reasons.append("weak_rejection")
    if not core_filters.get("volume", False):
        reasons.append("score_too_low")
    if score < 50:
        reasons.append("score_too_low")
    if not features.latest_failed_retest:
        reasons.append("retest_not_failed")
    pullback = features.pullback_from_high_pct or 0.0
    if 0 < pullback < 2.0:
        reasons.append("shallow_pullback")
    if not features.liquidity_available:
        reasons.append("liquidity_missing")
    elif liquidity_level in {"watch", "block"}:
        reasons.append("spread_too_wide")
    reasons.extend(squeeze.data_quality)
    reasons.extend(squeeze.reasons)
    if breakout_risk:
        reasons.append("squeeze_risk")
    return _dedupe(reasons) or ["other"]


def _squeeze_penalty(action: str, level: str) -> int:
    if action != "SCORE_PENALTY":
        return 0
    return {
        "LOW": 0,
        "MEDIUM": 6,
        "HIGH": 10,
        "EXTREME": 16,
    }[level]


def _squeeze_warning_lines(squeeze) -> list[str]:
    lines: list[str] = []
    if squeeze.level in {"MEDIUM", "HIGH", "EXTREME"}:
        lines.append(f"Squeeze risk: {squeeze.level} ({', '.join(squeeze.reasons) or 'n/a'}).")
    if squeeze.data_quality:
        lines.append(f"Data quality: {', '.join(squeeze.data_quality)}.")
    return lines


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
