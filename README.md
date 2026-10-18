# Order Gateway

A small service that receives trading signals and sends them to a (mock) broker, built so
that **a signal never becomes two broker orders**, and so that a crash, a timeout or a
misbehaving broker feed leaves the system in a state it can recover from.

Python 3.11+ · FastAPI · Pydantic v2 · stdlib `sqlite3` (no ORM) · pytest · `Decimal` for all
money and quantities.

---

## Setup, run, test

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# run the tests (231 tests, ~5 s)
.venv/bin/pytest -q

# run the API
DB_PATH=gateway.db BROKER_DB_PATH=broker.db MAX_ORDER_VALUE=100000 \
  .venv/bin/uvicorn app.api:app --port 8000
```

| Env var | Default | Meaning |
|---|---|---|
| `DB_PATH` | `gateway.db` | the gateway's SQLite file |
| `BROKER_DB_PATH` | `broker.db` | the mock broker's own SQLite file |
| `MAX_ORDER_VALUE` | `100000` | max `qty * reference_price` (inclusive) |

On startup the app creates its tables and runs `reconcile()` before it accepts requests.

### curl examples

```bash
# submit a signal (sending it again returns the same order and doesn't create a second one)
curl -s -X POST localhost:8000/signals -H 'content-type: application/json' \
  -d '{"signal_id":"s1","symbol":"AAPL","side":"BUY","qty":"10","reference_price":"150.25"}'

# the order plus its full event log
curl -s localhost:8000/orders/s1

# a broker push update: cumulative fill, per-order seq
curl -s -X POST localhost:8000/broker/updates -H 'content-type: application/json' \
  -d '{"client_order_id":"CO-s1","seq":2,"status":"PARTIALLY_FILLED","cumulative_filled_qty":"4"}'

# compare every unresolved order with the broker
curl -s -X POST localhost:8000/reconcile

# kill switch on / off
curl -s -X POST localhost:8000/kill-switch -H 'content-type: application/json' -d '{"on":true}'
curl -s -X POST localhost:8000/kill-switch -H 'content-type: application/json' -d '{"on":false}'
```

| Endpoint | Response |
|---|---|
| `POST /signals` | 200 with the stored order (new or duplicate). 409 if the `signal_id` exists with different contents. 422 if invalid. The kill switch and risk limit show up as status `BLOCKED` / `REJECTED_RISK`. |
| `GET /orders/{signal_id}` | `{order, events}`. 404 if unknown. |
| `POST /broker/updates` | 200 with `{applied, reason, order}`, also when the update is ignored, so the broker doesn't resend something we've handled. |
| `POST /reconcile` | `[{signal_id, status_before, status_after, note}]` |
| `POST /kill-switch` | `{"on": bool}` |

Numbers are sent and returned as JSON **strings** (`"qty": "10"`) so they are never turned
into floats.

---

## Layout

```
app/
  config.py         settings from env
  db.py             connection helper (WAL, busy_timeout, autocommit) + schema
  models.py         Signal, Order, BrokerUpdate, OrderStatus, ALLOWED_TRANSITIONS, can_transition()
  risk.py           exact order value, max value check, kill switch flag
  broker.py         MockBroker (own SQLite file, idempotent on client_order_id)
  service.py        OrderService: submit, resolve_unknown, handle_update, reconcile
  logging_setup.py  key=value logs
  api.py            FastAPI routes (thin) + startup reconcile
tests/              one file per concern; real SQLite files, real threads
docs/PLAN.md        the design as built, including decisions changed along the way
```

---

## Design

### State machine

```
                         ┌─────────────── kill switch on ──────────────▶ BLOCKED        (terminal)
                         ├─────────────── qty×price > max ─────────────▶ REJECTED_RISK  (terminal)
                         │
  signal ──▶ NEW ──atomic claim──▶ SUBMITTING ──broker response──┬──▶ ACCEPTED ──▶ PARTIALLY_FILLED ⟲
                                    │      ▲                     │        │              │
                       timeout /    │      │ get_order:          │        ▼              ▼
                       crash        ▼      │ not found (resend,  ├──────────────────▶ FILLED    (terminal)
                                  UNKNOWN ─┘ same id, kill       │
                                    │        switch off only)    └──────────────────▶ REJECTED  (terminal)
                                    └── get_order: found ──▶ adopt the broker's state
```

Full table in `app/models.py::ALLOWED_TRANSITIONS`. Terminal states have no outgoing moves,
so nothing (late update, reconcile, retry) can ever change them.

### What happens to one signal

1. **Insert** `NEW` with `INSERT … ON CONFLICT(signal_id) DO NOTHING`. Duplicates, even
   simultaneous ones, all land on the same single row.
2. **Kill switch**, then **max order value**. Either one stores a terminal status with a reason.
3. **Atomic claim**: `UPDATE … SET status='SUBMITTING' WHERE id=? AND status='NEW' AND
   submit_attempts=?`. Only the caller whose update changed a row (`rowcount == 1`) may call
   the broker.
4. **Broker call**, made with no transaction open. The response goes through the same update
   path as a pushed update.
5. On **timeout**: `UNKNOWN`, then `get_order(client_order_id)` before anything else.

### Why each mechanism

**Write-ahead (commit `SUBMITTING` before calling the broker).** If the process dies during
the broker call, the database says "this order may have been sent", never "nothing
happened". Recovery then knows it must *ask the broker* instead of sending again. Writing the
row after the call would leave a crash window where we had placed an order and kept no record
of it.

**Deterministic `client_order_id = "CO-{signal_id}"`.** After a timeout or a crash the only
question that matters is "does the broker have this order?". That question can only be asked
if we know the ID *without* the lost response. A random ID would make a lost response
impossible to look up. It also lets the broker deduplicate on its side: a resend with the same
ID can't create a second order at a broker that is idempotent on it.

**Never resend blindly.** A timeout means "no answer", not "no order". The broker may have
accepted, or even filled, the order. So: `UNKNOWN`, then `get_order`. If it's found, take the
broker's state. Resend (same ID) only if the broker says it has never seen it, and only while
the kill switch is off.

**Cumulative fills + per-order `seq`.** Broker feeds replay and reorder messages. Each update
carries the *total* filled quantity and a sequence number. We ignore any update with
`seq <= last_seq`, and we *set* `filled_qty` to the broker's total, never add to it. So:
- a replayed update changes nothing (same seq is ignored; even if applied, same total),
- an old update arriving late is ignored,
- a skipped `seq` is harmless (the next total already includes the missed fill).

We also refuse updates whose fill goes down, exceeds the quantity, or contradicts the status
(e.g. `FILLED` with less than the full quantity). Every update, applied or ignored, is written
to `order_events` with its reason. The read, checks and write happen in one `BEGIN IMMEDIATE`
transaction, which takes the write lock *before* reading.

**Restart reconciliation.** On startup (and on `POST /reconcile`) every non-terminal order is
compared with the broker through the same update path:
- `SUBMITTING` that this process isn't currently sending was orphaned by a crash, so it goes
  to `UNKNOWN`, then lookup. A freshly started process sends nothing, so after a restart every
  `SUBMITTING` row counts as orphaned.
- `UNKNOWN`: lookup, then adopt, or resend once (kill switch off only).
- `ACCEPTED` / `PARTIALLY_FILLED`: pick up fills that happened while we were down or whose push
  was lost.

Only the resend step is blocked by the kill switch. Lookups, updates and reconciliation keep
working.

**Compare-and-set with a version.** Every status change is
`UPDATE … WHERE id=? AND status=? AND submit_attempts=?`. Status alone is not enough, because
`SUBMITTING` can come back (`SUBMITTING → UNKNOWN → SUBMITTING` on a retry). A reconciler
holding the old `SUBMITTING` would then match the new attempt and send it a second time (the
ABA problem). `submit_attempts` increases on every claim, so it identifies the attempt.
`tests/test_reconcile.py::test_stale_orphan_snapshot_cannot_hijack_a_newer_attempt` forces
this interleaving and fails without the version check.

**Exact risk arithmetic.** `qty * reference_price` is computed with enough `Decimal` precision
to be exact, with rounding trapped as an error, and inputs are capped at 24 digits / 10
decimals. The default 28-digit context would round
`97803337049860.1412280187 × 0.0000102246` (exactly `1000000000.00000000000000000002`) down to
the 1,000,000,000 limit and let it through. There's a test for that case.

### Logs

One `key=value` line per event. `signal_id=` and `client_order_id=` are on every line (`-`
when not applicable), so `grep signal_id=s1` gives one instruction's whole story (`ts=` and
`level=` trimmed here):

```
event=RECEIVED      signal_id=s1 client_order_id=CO-s1 to_status=NEW qty=10 reference_price=150.25
event=CLAIMED       signal_id=s1 client_order_id=CO-s1 from_status=NEW to_status=SUBMITTING
event=BROKER_SEND   signal_id=s1 client_order_id=CO-s1 attempt=1
event=BROKER_TIMEOUT signal_id=s1 client_order_id=CO-s1 from_status=SUBMITTING to_status=UNKNOWN
event=BROKER_LOOKUP signal_id=s1 client_order_id=CO-s1 result=found broker_status=ACCEPTED seq=1
event=RESOLVED_FROM_BROKER signal_id=s1 client_order_id=CO-s1 from_status=UNKNOWN to_status=ACCEPTED
```

The same events are stored per order in `order_events` (returned by `GET /orders/{id}`). Log
lines from inside a transaction are emitted only after `COMMIT`, so the log never reports a
change that was rolled back.

### How it's tested

Tests use real SQLite files (never `:memory:`) and real threads with separate connections.
The concurrency tests don't rely on luck. Where a natural race would usually serialise and
hide a bug, a `threading.Barrier` or `Event` forces the worst interleaving: N threads all see
`NEW` before any claims, N resolvers all hear "not found" before any re-claims, or a stale
reconciler resumes mid-send. "Restart" means building a new `OrderService` and `MockBroker` on
the same files. Each core guard was checked by removing it and confirming that a test fails.

---

## Limitations (honest list)

- **SQLite allows one writer at a time.** Writes are serialised. `busy_timeout` is 5 s, and
  under enough contention a request fails with `database is locked` instead of waiting
  forever. Fine for one gateway process; not a throughput design.
- **Single instance only.** Two things assume one process per DB file. Claims are safe across
  processes (they're conditional `UPDATE`s), but the *in-flight* set that tells "being sent
  now" from "orphaned by a crash" lives in memory. Two instances would see each other's live
  sends as orphans. Running several instances needs Postgres with `SELECT … FOR UPDATE` row
  locks (or `SKIP LOCKED` workers) and a lease/heartbeat column on `SUBMITTING` instead of the
  in-memory set.
- **Real brokers may not be idempotent on `client_order_id`.** The mock is. The design asks
  before it resends, so it doesn't *depend* on broker idempotency, but there is one window:
  a request still in flight that lands *after* our `get_order` said "not found" would, at a
  non-idempotent broker, become a second order. Mitigations: a broker that enforces unique
  client IDs (most FIX venues reject duplicate `ClOrdID`), or waiting longer than the broker's
  max processing time before treating "not found" as final.
- **No cancel / amend / expiry.** Not in the state machine. An order can't be cancelled, and a
  partially filled order can't end as "rest cancelled".
- **No auth.** Any client that can reach the API can submit signals, inject broker updates and
  flip the kill switch.
- **Seq assumptions.** `seq` is assumed to be per order, strictly increasing at the broker,
  and the same counter for push updates and for `get_order` snapshots. The mock's `place_order`
  response is seq 1. A broker that resets seq (e.g. after a failover) or numbers per session
  would have its new updates ignored as stale. Fills are assumed cumulative; a delta-based
  feed would need conversion before `handle_update`.
- **No background reconciler.** Reconciliation runs at startup and on `POST /reconcile`. An
  order stuck in `UNKNOWN` (e.g. both send and retry timed out) waits for one of those. Each
  run makes at most one resend per order, and there is no overall retry cap yet
  (`submit_attempts` is recorded).
- **A signal can still go out late after a kill-switch period.** An `UNKNOWN` order whose
  resend was skipped is resent by the next reconcile after the switch is turned off. A
  `BLOCKED` signal is never sent (that status is terminal).
- **Kill-switch timing gap.** A signal that passed the kill-switch check just before the
  switch was turned on still goes out, like any order already being sent.
- **Risk uses `reference_price`,** not the execution price, and there's a single global limit
  (no per-instrument or per-day limits).
- **No schema migrations.** `CREATE TABLE IF NOT EXISTS` only. A DB file from an older version
  keeps its old `CHECK` constraints.
- **Updates for unknown orders are logged but not stored** (`order_events` needs an order row).
- The test client prints a `StarletteDeprecationWarning` about `httpx`, which comes from the
  library versions.

---

## How AI tools were used

_TODO (author): describe how AI tools were used in building this project._
