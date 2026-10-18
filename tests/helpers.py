from app.db import connection
from app.models import Signal
from app.risk import set_kill_switch


def signal(signal_id="sig-1", qty="2", price="100", side="BUY", instrument="AAPL") -> Signal:
    return Signal(signal_id=signal_id, instrument=instrument, side=side, qty=qty,
                  reference_price=price)


def kill_switch(db_path, on: bool) -> None:
    with connection(db_path) as c:
        set_kill_switch(c, on)


def event_names(service, signal_id="sig-1"):
    return [e["event"] for e in service.events(signal_id)]
