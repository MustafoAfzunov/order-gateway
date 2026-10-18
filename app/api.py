"""HTTP API. Thin on purpose: parse the request, call OrderService, shape the response.
All decisions live in app/service.py.

Run: uvicorn app.api:app
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from app.broker import MockBroker
from app.config import Settings, load_settings
from app.db import init_schema
from app.logging_setup import LOGGER_NAME, configure_logging, log_event
from app.models import BrokerUpdate, Order, Signal
from app.service import IgnoreReason, OrderService, SignalConflict

log = logging.getLogger(LOGGER_NAME)


class OrderWithEvents(BaseModel):
    order: Order
    events: list[dict[str, Any]]  # the per-instruction audit log, oldest first


class UpdateResponse(BaseModel):
    applied: bool
    reason: IgnoreReason | None
    order: Order | None


class ReconcileItem(BaseModel):
    signal_id: str
    status_before: str
    status_after: str
    note: str


class KillSwitchRequest(BaseModel):
    on: bool


class KillSwitchResponse(BaseModel):
    on: bool


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        cfg = settings or load_settings()
        init_schema(cfg.db_path)
        broker = MockBroker(cfg.broker_db_path)
        service = OrderService(cfg.db_path, broker, cfg)
        app.state.broker = broker
        app.state.service = service
        # Recover before serving: whatever a crash left in SUBMITTING/UNKNOWN is checked
        # with the broker now. This process has nothing in flight yet, so every SUBMITTING
        # row is an orphan.
        log_event(log, "STARTUP_RECONCILE", signal_id=None, client_order_id=None)
        service.reconcile()
        yield

    app = FastAPI(title="Order gateway", lifespan=lifespan)

    def service_of(request: Request) -> OrderService:
        svc: OrderService = request.app.state.service
        return svc

    # Plain `def` endpoints: FastAPI runs them in a thread pool, which suits the blocking
    # sqlite3 and broker calls. Each request opens its own DB connections.

    @app.post("/signals", response_model=Order)
    def submit_signal(signal: Signal, request: Request) -> Order:
        # Same response for a first submission and a duplicate (the stored order), so a sender
        # can safely retry. The kill switch and risk limit show up as status BLOCKED /
        # REJECTED_RISK, not as errors: the signal was received and recorded.
        try:
            return service_of(request).submit(signal)
        except SignalConflict as e:
            raise HTTPException(status_code=409, detail=str(e)) from e

    @app.get("/orders/{signal_id}", response_model=OrderWithEvents)
    def get_order(signal_id: str, request: Request) -> OrderWithEvents:
        svc = service_of(request)
        order = svc.get(signal_id)
        if order is None:
            raise HTTPException(status_code=404, detail=f"no order for signal_id {signal_id}")
        return OrderWithEvents(order=order, events=svc.events(signal_id))

    @app.post("/broker/updates", response_model=UpdateResponse)
    def broker_update(update: BrokerUpdate, request: Request) -> UpdateResponse:
        # 200 even when ignored (stale, terminal, unknown order...): we received and recorded
        # it. An error status would only make the broker resend something we've handled.
        result = service_of(request).handle_update(update)
        return UpdateResponse(applied=result.applied, reason=result.reason, order=result.order)

    @app.post("/reconcile", response_model=list[ReconcileItem])
    def reconcile(request: Request) -> list[ReconcileItem]:
        return [ReconcileItem(signal_id=r.signal_id, status_before=r.status_before.value,
                              status_after=r.status_after.value, note=r.note)
                for r in service_of(request).reconcile()]

    @app.post("/kill-switch", response_model=KillSwitchResponse)
    def kill_switch(body: KillSwitchRequest, request: Request) -> KillSwitchResponse:
        return KillSwitchResponse(on=service_of(request).set_kill_switch(body.on))

    return app


app = create_app()  # settings are read from env at startup (lifespan), not at import
