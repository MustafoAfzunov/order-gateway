# Plan (draft, awaiting approval)

## 1. Module layout
```
app/
  config.py      Settings (env): GATEWAY_DB, BROKER_DB, MAX_ORDER_VALUE (Decimal), MAX_SUBMIT_ATTEMPTS
  db.py          connect() (WAL, busy_timeout, row_factory), init_schema()
  models.py      Pydantic Signal (+ later API models); OrderStatus, ALLOWED_TRANSITIONS, TERMINAL, can_transition()
  repo.py        all SQL: insert_instruction, claim, transition, apply_broker_update, log_event,
                 kill-switch get/set
  risk.py        order_value(), max_value_violation(), is_kill_switch_on(), set_kill_switch()
  broker.py      Broker protocol (place_order, get_order) + MockBroker (own SQLite file)
  service.py     OrderService.submit() (phase 3); resolve_unknown(), reconcile() later
  api.py         create_app() + FastAPI routes (thin); lifespan runs reconcile() on startup
  logging_setup.py  key=value formatter, log_event(), configure_logging()
tests/
  conftest.py    tmp-file DBs, app/gateway/broker fixtures, "restart" helper
  test_submit.py, test_concurrency.py, test_timeout.py, test_updates.py,
  test_restart.py, test_risk.py, test_kill_switch.py, test_broker.py, test_logs.py
```

## 2. DB schema

**gateway.db**
```sql
CREATE TABLE orders (
  id               INTEGER PRIMARY KEY,
  signal_id        TEXT NOT NULL UNIQUE,
  client_order_id  TEXT NOT NULL UNIQUE,            -- 'CO-' || signal_id
  symbol           TEXT NOT NULL,
  side             TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  qty              TEXT NOT NULL,                   -- Decimal as text
  reference_price  TEXT NOT NULL,                   -- Decimal as text
  status           TEXT NOT NULL CHECK (status IN ('NEW','SUBMITTING','UNKNOWN','ACCEPTED',
                                          'PARTIALLY_FILLED','FILLED','REJECTED',
                                          'BLOCKED','REJECTED_RISK')),
  filled_qty       TEXT NOT NULL DEFAULT '0',       -- cumulative, from broker
  last_seq         INTEGER NOT NULL DEFAULT 0,      -- highest broker seq applied
  submit_attempts  INTEGER NOT NULL DEFAULT 0,
  broker_order_id  TEXT,
  reject_reason    TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL
);
CREATE TABLE order_events (                         -- per-instruction audit log (req 9)
  id             INTEGER PRIMARY KEY,
  order_id       INTEGER NOT NULL REFERENCES orders(id),
  event          TEXT NOT NULL,                     -- e.g. CLAIMED, BROKER_TIMEOUT, UPDATE_IGNORED_STALE_SEQ
  from_status    TEXT, to_status TEXT,
  detail         TEXT,                              -- JSON
  created_at     TEXT NOT NULL
);
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);  -- kill_switch = 'on'|'off'
```

**broker.db** (mock broker only)
```sql
CREATE TABLE broker_orders (
  client_order_id TEXT PRIMARY KEY,                 -- idempotency key
  broker_order_id TEXT NOT NULL UNIQUE,
  instrument TEXT, side TEXT, qty TEXT, price TEXT,
  status TEXT NOT NULL, filled_qty TEXT NOT NULL DEFAULT '0',
  seq INTEGER NOT NULL                              -- 1 on create, +1 per make_update
);
```

## 3. State machine

```
NEW ──claim──▶ SUBMITTING ──broker reply──▶ ACCEPTED ─▶ PARTIALLY_FILLED ─▶ FILLED
                  │  ▲                         │               │  (loop)
          timeout │  │ get_order: not found    ├──────────────────────────▶ FILLED
                  ▼  │ (retry, same coid)      ▼
                UNKNOWN ──get_order: found──▶ (whatever broker says)
SUBMITTING / UNKNOWN / ACCEPTED ──broker reject──▶ REJECTED
NEW ──kill switch on──▶ BLOCKED        NEW ──over max value──▶ REJECTED_RISK
```

| From | Allowed to | Trigger |
|---|---|---|
| (insert) | NEW | valid signal (every signal is inserted as NEW first) |
| NEW | BLOCKED | kill switch on (recorded, never sent) |
| NEW | REJECTED_RISK | qty × reference_price > MAX_ORDER_VALUE (recorded, never sent) |
| NEW | SUBMITTING | atomic claim (`rowcount == 1`) |
| SUBMITTING | ACCEPTED, PARTIALLY_FILLED, FILLED, REJECTED | broker response |
| SUBMITTING | UNKNOWN | timeout / transport error / found after restart |
| UNKNOWN | ACCEPTED, PARTIALLY_FILLED, FILLED, REJECTED | `get_order` found it |
| UNKNOWN | SUBMITTING | `get_order` = not found; atomic re-claim `WHERE status='UNKNOWN'` |
| ACCEPTED | PARTIALLY_FILLED, FILLED, REJECTED | broker update, seq > last_seq |
| PARTIALLY_FILLED | PARTIALLY_FILLED, FILLED | broker update, seq > last_seq, filled_qty grows |
| FILLED, REJECTED, BLOCKED, REJECTED_RISK | — | terminal (a BLOCKED signal never fires later; the sender sends a new signal_id) |

Update rule (`OrderService.handle_update`, phase 5): one `BEGIN IMMEDIATE` transaction does
the read, the checks, the write and the `order_events` row. Checks, in order (each one failing
means "ignore + log the reason"): `seq <= last_seq` → STALE_OR_DUPLICATE; terminal →
ORDER_TERMINAL; `!can_transition()` → INVALID_TRANSITION; cumulative fill below the stored one →
FILL_DECREASED; above qty → FILL_EXCEEDS_QTY; fill doesn't match the status (FILLED ≠ qty,
PARTIALLY_FILLED not strictly between 0 and qty, ACCEPTED/REJECTED ≠ 0) →
FILL_INCONSISTENT_WITH_STATUS. The UPDATE also carries `AND last_seq < ? AND status NOT IN
(terminal)` as a second guard. An update for an unknown client_order_id → UNKNOWN_ORDER:
logged (Python log only, because order_events needs an order row), nothing changed.

Reconciliation (`OrderService.reconcile`, phase 6), run at startup and periodically. For every
NEW / SUBMITTING / UNKNOWN / ACCEPTED / PARTIALLY_FILLED order (re-read fresh first):
- SUBMITTING that this process is sending (in-memory in-flight counter) → skipped.
- SUBMITTING that nobody is sending → orphaned by a crash → `UNKNOWN` (event
  ORPHANED_SUBMITTING). A fresh process has nothing in flight, so after a restart every
  SUBMITTING row counts as orphaned. This replaces the planned "startup moves all SUBMITTING".
- UNKNOWN → `resolve_unknown()`: get_order; adopt if found; if not found, resend once with the
  same client_order_id, unless the kill switch is on.
- NEW / ACCEPTED / PARTIALLY_FILLED → get_order. Same seq → "in sync" (no event); otherwise apply
  through the update path (event RECONCILED). Not found: expected for NEW (never sent; left NEW,
  not sent by reconcile), an anomaly for ACCEPTED/PARTIALLY_FILLED (event
  RECONCILE_MISSING_AT_BROKER, order unchanged, never resent).
- An exception on one order is logged and reported; the others are still reconciled.
- Compare-and-set includes `submit_attempts` (a version number) to prevent ABA on SUBMITTING.

## 4. API (`app/api.py`, phase 7; thin: parse, call OrderService, shape response)
- `POST /signals`: 200 with the stored order, for a new signal and a duplicate alike (safe for
  the sender to retry). 409 for the same `signal_id` with different contents, 422 for invalid
  input. Kill switch / risk limit show up as status `BLOCKED` / `REJECTED_RISK`, not as errors.
- `GET /orders/{signal_id}`: `{order, events}`, where events is the per-instruction audit log.
  404 if unknown.
- `POST /broker/updates`: 200 with `{applied, reason, order}`, even when ignored (stale,
  terminal, unknown order), so the broker doesn't resend something we've handled. 422 if
  malformed.
- `POST /reconcile`: list of `{signal_id, status_before, status_after, note}`.
- `POST /kill-switch` `{"on": bool}`.
- Startup (FastAPI lifespan): `init_schema` then `reconcile()`.
- Not built (from the earlier draft): order listing by status, `GET` kill-switch state,
  `/health`, 201 vs 200 on submit.

Logs (`app/logging_setup.py`): one key=value line per event. `signal_id=` and
`client_order_id=` are on every line ('-' when not applicable), so `grep signal_id=X` tells
one instruction's story. Lines written inside a transaction are emitted only after COMMIT.

## 5. Decisions needing your approval
1. ~~Over-limit signals are stored as `REJECTED`.~~ **Superseded in phase 3 (user's spec):**
   stored as `REJECTED_RISK` with a reason, separate from broker `REJECTED`.
2. ~~Kill switch refuses without storing.~~ **Superseded in phase 3 (user's spec):** the signal is
   stored as `BLOCKED` (terminal). The switch is persisted in `settings`.
3. **Kill switch also blocks re-sends** (implemented in phase 4: event `RETRY_SKIPPED_KILL_SWITCH`, order stays UNKNOWN) (UNKNOWN that came back not found becomes a new
   `place_order`). `get_order`, updates and reconcile still run.
4. ~~Retry cap via `MAX_SUBMIT_ATTEMPTS`.~~ **Phase 4:** each `resolve_unknown()` call makes at
   most one resend. If that resend times out too, the order stays `UNKNOWN` (no loop). A
   cap across repeated calls (e.g. reconciliation) is still open for phase 6; `submit_attempts`
   is already counted. If the lookup itself fails, the order stays `UNKNOWN` and is never resent.
5. **MockBroker runs in-process** (a Python class with its own SQLite file), not over HTTP.
   `Behavior`: `ACCEPT`, `REJECT`, `PARTIAL_FILL` (fills qty/2), `FILL`, `ACCEPT_THEN_TIMEOUT`
   (records the order, then raises `BrokerTimeout`), `TIMEOUT_BEFORE_ACCEPT` (raises, records
   nothing). Precedence: the per-call `behavior=` argument, then the `script(coid, ...)` queue,
   then `default_behavior`. A behavior applies only when the order is created; resending an
   existing `client_order_id` returns the stored order unchanged.
6. **Out of scope:** cancels, amendments, order expiry, multiple processes or hosts (SQLite plus
   one process is assumed; the claim is still safe across processes sharing the file).

## 6. Test list
**Submission / idempotency**
- test_submit_new_signal_places_one_broker_order
- test_duplicate_submission_returns_existing_and_no_second_broker_order
- test_same_signal_id_different_payload_returns_409
- test_concurrent_submissions_same_signal_produce_one_broker_order (20 threads, barrier, separate connections)
- test_claim_only_one_caller_gets_rowcount_one
- test_submitting_is_committed_before_broker_call (broker spy reads gateway.db from its own connection)

**Broker responses**
- test_broker_accept_sets_accepted
- test_broker_reject_sets_rejected_with_reason
- test_broker_partial_fill_sets_partially_filled
- test_broker_fill_sets_filled
- test_mock_broker_idempotent_on_client_order_id

**Timeouts**
- test_timeout_after_accept_resolved_by_get_order_without_resend
- test_timeout_before_accept_resends_with_same_client_order_id
- test_get_order_called_before_any_retry (call-order spy)
- test_retry_cap_leaves_order_unknown

**Broker updates**
- test_duplicate_update_does_not_double_count_fill
- test_out_of_order_update_is_ignored
- test_terminal_order_ignores_later_updates (FILLED then late partial; REJECTED then accept)
- test_decreasing_cumulative_fill_is_rejected
- test_fill_exceeding_qty_is_rejected
- test_invalid_transition_raises

**Restart / reconciliation**
- test_state_survives_restart
- test_restart_moves_submitting_to_unknown_and_reconciles
- test_crash_after_broker_accept_recovers_without_duplicate (broker has 1 order after restart)
- test_reconcile_applies_missed_fills

**Risk / kill switch**
- test_order_value_over_limit_rejected_without_broker_call
- test_order_value_exactly_at_limit_is_allowed
- test_max_order_value_is_configurable
- test_order_value_uses_decimal_exactly (e.g. 0.1 × 3)
- test_kill_switch_blocks_new_submissions
- test_kill_switch_allows_reads_updates_and_reconcile
- test_kill_switch_persists_across_restart

**Logs**
- test_every_transition_writes_order_event
- test_ignored_update_is_logged_with_reason
