import pytest

from app.models import ALLOWED_TRANSITIONS, TERMINAL, OrderStatus, can_transition

S = OrderStatus
ALL = list(OrderStatus)


def test_every_status_has_a_transition_entry():
    assert set(ALLOWED_TRANSITIONS) == set(OrderStatus)


def test_terminal_states():
    assert TERMINAL == {S.FILLED, S.REJECTED, S.BLOCKED, S.REJECTED_RISK}


@pytest.mark.parametrize("terminal", sorted(TERMINAL))
@pytest.mark.parametrize("target", ALL)
def test_terminal_state_never_changes(terminal, target):
    assert not can_transition(terminal, target)


@pytest.mark.parametrize("current", ALL)
def test_nothing_moves_back_to_new(current):
    assert not can_transition(current, S.NEW)


def test_new_can_only_be_claimed_blocked_or_risk_rejected():
    assert ALLOWED_TRANSITIONS[S.NEW] == {S.SUBMITTING, S.BLOCKED, S.REJECTED_RISK}


def test_new_cannot_skip_submitting():
    # Write-ahead rule: the broker is only called after SUBMITTING is persisted.
    for target in (S.ACCEPTED, S.FILLED, S.UNKNOWN):
        assert not can_transition(S.NEW, target)


def test_timeout_moves_submitting_to_unknown():
    assert can_transition(S.SUBMITTING, S.UNKNOWN)


def test_unknown_can_be_resolved_or_retried():
    for target in (S.ACCEPTED, S.PARTIALLY_FILLED, S.FILLED, S.REJECTED, S.SUBMITTING):
        assert can_transition(S.UNKNOWN, target)


@pytest.mark.parametrize("current", [S.ACCEPTED, S.PARTIALLY_FILLED])
def test_live_order_cannot_return_to_submitting_or_unknown(current):
    # Once the broker has confirmed the order, resending would risk a duplicate.
    assert not can_transition(current, S.SUBMITTING)
    assert not can_transition(current, S.UNKNOWN)


def test_partial_fill_cannot_move_back_to_accepted():
    assert not can_transition(S.PARTIALLY_FILLED, S.ACCEPTED)


def test_partial_fill_can_repeat_and_complete():
    assert can_transition(S.PARTIALLY_FILLED, S.PARTIALLY_FILLED)
    assert can_transition(S.PARTIALLY_FILLED, S.FILLED)


def test_every_non_terminal_state_can_reach_a_terminal_state():
    for start in set(OrderStatus) - TERMINAL:
        seen, frontier = {start}, [start]
        while frontier:
            for nxt in ALLOWED_TRANSITIONS[frontier.pop()]:
                if nxt not in seen:
                    seen.add(nxt)
                    frontier.append(nxt)
        assert seen & TERMINAL, f"{start} cannot reach a terminal state"
