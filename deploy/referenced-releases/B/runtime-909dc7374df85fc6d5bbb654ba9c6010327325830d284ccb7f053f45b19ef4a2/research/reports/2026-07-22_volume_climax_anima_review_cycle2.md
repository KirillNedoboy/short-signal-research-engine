# Anima Review Cycle 2: `VOLUME_CLIMAX_UNWIND`

- **Run time:** 2026-07-22T16:05Z
- **Repository:** `/opt/short-telegram-bot-lite`
- **Baseline:** `a4160e4c099a02118e1c27bc7a2619bc255e235a`
- **Review mode:** read-only multi-interpretation review after local revise patch
- **Live admission:** unchanged
- **Service:** not restarted
- **Verdict:** `REVISE`

## Verification evidence

```text
Targeted lifecycle/climax tests: 26 passed in 0.89s
Full suite: 120 passed in 4.00s
compileall: pass
git diff --check: pass
```

Дополнительный deterministic replay подтвердил последовательность ERAUSDT-style highs:

```text
0.12700 → revision 1
0.12985 → revision 2
0.13326 → revision 3
0.13747 → revision 4
```

`root_created_at` не изменяется при новых highs. При одновременных `price_acceleration_resumed`, `active_short_squeeze` и `oi_continuation` fallback остаётся заблокирован.

## Independent interpretations

### Trader

**Что улучшилось:**

- threshold закрытых свечей стал конфигурационным;
- confirmation window стал config-driven;
- boundary behavior проверяется тестами;
- volume-climax attempt получил отдельный namespace;
- deterministic lifecycle sequence ведёт себя ожидаемо.

**Оставшиеся блокеры:**

1. Lifecycle по-прежнему запускается только при `evaluation.subtype == "VOLUME_CLIMAX_UNWIND"`.
2. При veto или недостаточном score subtype становится `None`, поэтому rejected-but-eligible candidates не попадают в lifecycle shadow.
3. `latest_failed_retest` одновременно отключает acceleration veto и squeeze veto.
4. Нет доказательства, что последняя свеча полностью закрыта.
5. Не enforced `climax_max_attempts_per_root_event`.

**Trader verdict:** `REVISE`.

### Systems

**Что улучшилось:**

- namespace `volume_climax:{root}:r{revision}:a1` устранит прямой collision с legacy ID;
- `shadow_attempt_id` инициализируется до ветвления;
- lifecycle evaluation получает namespaced attempt correlation;
- lifecycle-specific `decision_delta` добавлен;
- config parameters передаются в lifecycle.

**Оставшиеся блокеры:**

1. Lifecycle не охватывает live-rejected candidates.
2. `upsert_shadow_root_event()` маскирует DB error fallback-ом к revision `1`, что может нарушить monotonic revision.
3. Root, EventState, attempt и evaluation сохраняются отдельными операциями без атомарности/reconciliation.
4. Expired attempt может получить `attempt_state="EXPIRED"`, но остаться с `attempt_closed_at=NULL`.
5. `climax_max_attempts_per_root_event` объявлен и валидируется, но lifecycle path его не применяет.
6. `climax_root_events` хранит только текущий peak, а не append-only revision history.

**Systems verdict:** `REVISE`.

### Research

**Что улучшилось:**

- lifecycle telemetry и revision fields присутствуют в коде;
- локальный test suite остается зеленым;
- offline replay воспроизводим;
- в SQLite существуют root events, evaluations и attempt states.

**Что не подтверждено:**

1. Работающий systemd-процесс был запущен до revise-патча и не перезагружал новый код.
2. В доступной runtime SQLite нет новых `volume_climax:%` attempts, `FALLBACK_READY` или `LIVE_REJECTED_SHADOW_FALLBACK_READY`.
3. Partial-candle regression отсутствует.
4. Replay не содержит полноценную fees/funding/spread/slippage/depth/latency модель.
5. OOS и time-split cohort отсутствуют.
6. Нет доказательства улучшения hit-rate, MAE/MFE или PnL.

**Research verdict:** `REVISE`.

## Cycle comparison

| Area | Cycle 1 | Cycle 2 |
|---|---|---|
| Config wiring | dead config | исправлена |
| Attempt namespace | collision risk | исправлен в коде |
| `shadow_attempt_id` | runtime risk | исправлен |
| Lifecycle decision delta | неполный | lifecycle-specific |
| Rejected-candidate admission | blocker | blocker сохраняется |
| Closed-candle proof | blocker | blocker сохраняется |
| Persistence fail-open | blocker | blocker сохраняется |
| Atomicity/reconciliation | отсутствует | отсутствует |
| Cost-aware OOS replay | отсутствует | отсутствует |
| Overall verdict | `REVISE` | `REVISE` |

## Next controlled patch

Следующий цикл должен быть ограничен следующими изменениями:

1. Выделить pre-admission `volume_climax_candidate` из экстремальных признаков.
2. Передавать этот candidate в lifecycle независимо от `evaluation.actionable` и `evaluation.subtype`.
3. Сохранить live evaluator и Telegram admission без изменений.
4. Развести независимые признаки:
   - `rejection_confirmed`;
   - `price_acceleration_resumed`;
   - `active_short_squeeze`.
5. Сделать persistence fail-closed: DB exception не должна возвращать revision `1`.
6. Закрывать expired attempt атомарно или явно выставлять `attempt_closed_at`.
7. Добавить integration tests для rejected candidate, partial candle, expiry и persistence failure.
8. Не включать live promote до runtime validation и cost-aware OOS replay.

## Decision

Второй Anima cycle подтверждает, что revise-патч улучшил локальную структуру и тестируемость, но не доказал production runtime effect и не снял admission/persistence/closed-candle blockers.

```text
PROMOTE: NO
LIVE RESTART: NO
AUTO-EXECUTION: OFF
NEXT ACTION: REVISE admission + fail-closed persistence, then rerun Anima
```
