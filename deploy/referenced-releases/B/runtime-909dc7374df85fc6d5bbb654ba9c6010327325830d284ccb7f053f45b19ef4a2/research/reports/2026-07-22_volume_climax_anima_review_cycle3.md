# Anima Review Cycle 3: `VOLUME_CLIMAX_UNWIND`

- **Repository:** `/opt/short-telegram-bot-lite`
- **Baseline:** `a4160e4c099a02118e1c27bc7a2619bc255e235a`
- **Review mode:** read-only multi-interpretation review after admission/fail-closed patch
- **Service restart:** no
- **Live admission:** unchanged
- **Auto-execution:** off
- **Verdict:** `REVISE`

## Delegation result

| Interpretation | Result |
|---|---|
| Trader | timeout after 600s; no verdict received |
| Systems | completed; `REVISE` |
| Research | completed; `REVISE` |

Поэтому это не объявляется полным консенсусом 3/3. Два завершённых независимых review дали одинаковый `REVISE`; trader path остается неполученным evidence.

## Verification evidence

```text
Targeted tests: 22–26 passed depending on selected subset
Full suite: 120 passed
compileall: pass
git diff --check: pass
```

Патч также проверен по коду и read-only SQLite inspection.

## What improved since Cycle 2

1. Rejected volume candidate может пройти lifecycle admission через:

   ```python
   evaluation.subtype == "VOLUME_CLIMAX_UNWIND"
   or evaluation.metadata["strategy_subtype"] == "VOLUME_CLIMAX_UNWIND"
   ```

2. `price_acceleration_resumed` и `active_short_squeeze` больше не подавляются `latest_failed_retest`.
3. Root-event persistence теперь fail-closed и больше не возвращает фиктивную revision `1` после DB exception.
4. Terminal/expired attempts получают `attempt_closed_at` и `attempt_close_reason` при создании.
5. Lifecycle namespace остается отделенным:

   ```text
   volume_climax:{root}:r{revision}:a1
   ```

6. Full suite остается зеленым на уровне `120 passed`.

## Remaining blockers

### 1. Metadata admission всё еще не полноценный pre-admission

`evaluate_climax()` сначала выбирает одну evaluation из нескольких кандидатов. Если volume-climax candidate существует, но его вытесняет low-volume evaluation с большим score, metadata выбранной evaluation уже не содержит volume subtype.

Следовательно, нужен независимый `volume_climax_candidate` до финального выбора стратегии, а не только проверка metadata уже выбранной evaluation.

### 2. Fail-closed исправлен только для root-event persistence

Остальные lifecycle storage operations всё еще fail-open:

- `upsert_shadow_entry_attempt()` проглатывает DB exception;
- `transition_shadow_entry_attempt()` возвращает `False` после ошибки;
- `record_climax_evaluation()` проглатывает exception.

Это оставляет возможность рассинхронизации root, EventState, attempt и evaluation.

### 3. Нет atomicity/reconciliation

Root event, EventState, attempt и evaluation сохраняются отдельными операциями. Нет общего transaction boundary и startup reconciliation для восстановления lifecycle invariants.

Локальная БД содержит `attempt_correlation_missing` events, поэтому correlation gaps не только теоретические.

### 4. Attempt limit не enforced

`climax_max_attempts_per_root_event` присутствует и валидируется, но lifecycle path не ограничивает число revisions/attempts на root.

### 5. Expired candidate остается в active pool

Attempt закрывается, но active candidate может продолжать переоцениваться до истечения общего candidate TTL. Нужна явная очистка после lifecycle expiry.

### 6. Нет append-only root revision history

`climax_root_events` хранит текущий peak/revision и обновляет его in-place. Полной immutable history всех peaks для audit/replay нет.

### 7. Closed-candle boundary не доказан

Фильтр `timestamps <= features.asof` не доказывает, что последняя 1m candle полностью закрыта. Нет exchange close-time check или исключения текущей свечи через wall-clock boundary.

### 8. Runtime effect не подтвержден

Systemd-процесс старше текущего patch-set. В доступной SQLite:

```text
volume_climax:% attempts: 0
LIVE_REJECTED_SHADOW_FALLBACK_READY: 0
```

Это означает отсутствие доказательства runtime activation, а не доказательство, что новый код сам по себе не работает.

### 9. Research validation incomplete

Не выполнены:

- fees/funding/spread/slippage/depth/partial-fill model;
- latency-aware execution replay;
- time-split OOS cohort;
- multi-symbol validation;
- executable PnL/MAE/MFE comparison.

## Cycle comparison

| Area | Cycle 1 | Cycle 2 | Cycle 3 |
|---|---|---|---|
| Config wiring | blocker | fixed | fixed |
| Namespace collision | blocker | fixed | fixed |
| Rejected candidate admission | blocker | blocker | partially fixed; independent candidate still needed |
| Independent veto | blocker | blocker | fixed in wiring; runtime unverified |
| Root persistence fallback | blocker | blocker | fixed |
| All persistence fail-closed | blocker | blocker | still open |
| Expired attempt closure | blocker | blocker | fixed on create path; pool cleanup open |
| Closed-candle proof | blocker | blocker | blocker |
| Attempt limit | blocker | blocker | blocker |
| Atomicity/reconciliation | absent | absent | absent |
| Cost-aware OOS replay | absent | absent | absent |
| Verdict | `REVISE` | `REVISE` | `REVISE` |

## Next controlled patch

1. Produce an independent pre-admission `volume_climax_candidate` before candidate selection.
2. Enforce `climax_max_attempts_per_root_event`.
3. Make all lifecycle persistence operations fail-closed or add explicit reconciliation.
4. Remove expired candidates from active monitoring.
5. Add append-only revision history.
6. Add strict closed-candle boundary and partial-candle regression tests.
7. Only after tests, restart in a controlled shadow-only window and verify runtime IDs/attempts read-only.
8. Run cost-aware, time-split, multi-symbol replay before any promote decision.

```text
PROMOTE: NO
LIVE RESTART: NO
AUTO-EXECUTION: OFF
NEXT: REVISE independent pre-admission + persistence invariants, then rerun
```
