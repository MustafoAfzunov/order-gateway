# CLAUDE.md: Order Gateway (take-home)

A small service that receives trading signals and sends them to a mock broker. It must never
create duplicate orders and must recover correctly from failures.

**Correctness and clear reasoning matter more than features.** The author has to explain every
line in an interview. Keep the code simple and comment the WHY at the critical points (claim,
write-ahead, timeout handling, update ordering). Don't comment the obvious WHAT.

Read `docs/PLAN.md` before changing code.

## Stack
- Python 3.11+, FastAPI, Pydantic v2, stdlib `sqlite3` (no ORM), pytest.
- `decimal.Decimal` for every quantity and price. Never `float`. Decimals are stored in SQLite as
  TEXT and parsed back with `Decimal(str)`.

## Requirements
1. Store instructions and order status in a DB.
2. Duplicate or simultaneous submissions of the same signal produce exactly one broker order.
3. A mock broker with accepted, rejected, partially filled, filled and timeout responses.
4. If a timeout happens after the broker accepted but the response was lost, use a stable
   `client_order_id` to check the broker before any retry.
5. Duplicate or out-of-order broker updates never double-count fills or move a terminal order
   backwards.
6. State survives restart. Unresolved orders are reconciled with the broker.
7. Configurable max order value (`qty * reference_price`).
8. Kill switch: it blocks new submissions, but monitoring and reconciliation keep working.
9. Clear per-instruction logs.

## Mandatory design rules (non-negotiable)
1. `signal_id` is `UNIQUE` in the DB. The constraint is the source of truth, not a pre-check in
   Python.
2. `client_order_id = "CO-{signal_id}"`. It's deterministic, so every retry, restart and
   reconciliation uses the same ID.
3. **Atomic claim:** `UPDATE instructions SET status='SUBMITTING' WHERE id=? AND status='NEW'`.
   Only the caller that gets `rowcount == 1` may call the broker. Everyone else returns the
   stored state.
4. **Write-ahead:** `SUBMITTING` is committed **before** the broker call. Never call the broker
   inside an open transaction or before the commit.
5. **Timeout:** on timeout or a transport error, set `UNKNOWN`, then call
   `get_order(client_order_id)` **before any retry**. Retry `place_order` only if the broker
   says the order doesn't exist.
6. **Broker updates** carry a per-order `seq` and a **cumulative** `filled_qty`. Ignore any
   update with `seq <= last_seq`. Never add fill deltas; always store the cumulative value.
7. **Terminal states (`FILLED`, `REJECTED`, `BLOCKED`, `REJECTED_RISK`) never change.** Every
   status UPDATE is a compare-and-set, `WHERE id = ? AND status = ? AND submit_attempts = ?`
   (both as we read them), done after `can_transition()` has approved the move. A terminal
   status has no allowed moves, so it can't pass that check. `submit_attempts` works as a
   version number: SUBMITTING can come back (UNKNOWN -> SUBMITTING on retry), and checking status
   alone would let a stale reader take over a newer attempt (ABA). Broker updates go through
   `handle_update`'s path instead (BEGIN IMMEDIATE plus `last_seq < ?` and not terminal).
8. The mock broker has **its own separate SQLite file** and is **idempotent on
   `client_order_id`**. Placing the same ID twice returns the existing order and doesn't create
   a second one.

## Conventions
- Every state change is a single conditional `UPDATE ... WHERE <expected state>`, with
  `rowcount` checked. There's no read-then-write in Python without the condition in SQL.
- All broker information (pushes, the place_order response, get_order lookups, reconcile)
  goes through one path, `OrderService._handle`, so the seq, terminal and fill rules can't be
  bypassed.
- Single gateway process per DB file: the in-flight set that tells "SUBMITTING, being sent
  now" from "SUBMITTING, orphaned by a crash" lives in memory.
- Allowed transitions live in one place (`ALLOWED_TRANSITIONS` in `app/models.py`). Code that changes status asserts the
  transition is allowed.
- Every transition and every ignored or rejected update writes a row to `order_events` and a
  log line tagged with `signal_id` and `client_order_id`.
- One `sqlite3` connection per request or thread, WAL mode, and `busy_timeout` set. Tests for
  concurrency use real threads with separate connections.
- Tests use temp-file SQLite DBs (never `:memory:`, which can't be shared or survive a
  "restart").
- Tests are named `test_<what>_<expected>`. Never skip, weaken or delete a failing test to get
  green. Fix the code or report it.
- Never claim something works without running `pytest` and showing the output.

## Commands
```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/pytest -v
DB_PATH=gateway.db BROKER_DB_PATH=broker.db MAX_ORDER_VALUE=100000 .venv/bin/uvicorn app.api:app
```
