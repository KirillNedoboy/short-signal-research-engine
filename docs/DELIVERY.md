# Signal and delivery contract

## Output classes

| Class | Meaning | Actionable? | Ordinary short admission? |
|---|---|---:|---:|
| `ACTIONABLE` | All required strategy, data, liquidity, score, grade, and policy gates passed | Yes, manual entry only | Yes |
| `WATCH` | Interesting setup or research observation that did not pass public admission | No | No |
| `EARLY_DROP_WARNING` | Separate multi-factor weakening/degradation observation | No | No |
| `NO_SETUP` | Valid scan completed but no setup passed | No | No |
| data-quality outcome | Scan could not be trusted | No | No |

```text
EARLY_DROP_WARNING != SHORT_SIGNAL
WATCH != ACTIONABLE_SHORT_SIGNAL
AUTOEXECUTION=OFF
```

## Ordinary signal lifecycle

```text
candidate
→ data-quality validation
→ event/lifecycle evaluation
→ strategy hard gates
→ liquidity/squeeze vetoes
→ score and grade
→ admission policy
→ signal persistence
→ outbox enqueue
→ Telegram delivery
→ delivery verification
```

A persisted signal is not proof of Telegram delivery. The outbox state is the delivery evidence.

## Outbox semantics

The storage layer uses a transactional outbox so signal persistence and enqueue are committed together. Workers lease pending records, attempt delivery, record provider results, retry bounded failures, and mark terminal failures as `DEAD` according to the release policy. Semantics are at-least-once; message idempotency and provenance prevent a retry from being mistaken for a new strategy event.

No real chat identifiers, tokens, or provider URLs belong in examples or documentation.

## Early Drop Warning

The warning evaluator is separate from ordinary short scoring and admission. It has its own persistence entity, cooldown and delivery type. It may report data degradation or weakening, but it does not create a short signal, change a short score, or authorize an order.

## Manual execution boundary

The Telegram message is an information output. The operator decides whether to enter manually. The repository contains no live-order, copy-trading, or autoexecution contract.
