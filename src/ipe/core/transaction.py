"""서비스/액션 트랜잭션 매니저 (DESIGN §11, §12, §18.4).

상태 어휘는 닫힌 집합 — 자유 문자열을 거부해 sqlite 행이 항상 invocationStatus /
actionStatusEvent 페이로드로 되돌아갈 수 있게 한다. timeout_ms=0은 "IPE 측
타임아웃 없음"이며 다른 종료 수단이 있을 때만 허용된다(설정 검증이 강제).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ipe.core.vocab import (
    ACTION_STATES,
    ACTION_TERMINAL,
    SERVICE_STATES,
    SERVICE_TERMINAL,
)

if TYPE_CHECKING:
    from ipe.runtime.state import StatePersistence



class VocabularyError(ValueError):
    pass


def _check(state: str, allowed: set[str], kind: str) -> None:
    if state not in allowed:
        raise VocabularyError(f"invalid {kind} transaction state: {state!r}")


class ServiceTransactionManager:
    def __init__(self, state: StatePersistence, default_timeout_ms: int = 5000) -> None:
        self.state = state
        self.default_timeout_ms = default_timeout_ms

    def begin(self, corr_id: str, now: float, timeout_ms: int | None = None) -> str:
        t = self.default_timeout_ms if timeout_ms is None else timeout_ms
        if not self.state.begin_transaction(corr_id, "service", now, timeout_ms=t):
            return "duplicate"
        return "pending"

    def set_state(self, corr_id: str, st: str, now: float) -> None:
        _check(st, SERVICE_STATES, "service")
        self.state.update_transaction(corr_id, st, now)

    def state_of(self, corr_id: str) -> str | None:
        t = self.state.get_transaction(corr_id)
        return t["state"] if t else None

    def is_terminal(self, st: str) -> bool:
        return st in SERVICE_TERMINAL

    def sweep_timeouts(self, now: float) -> list[str]:
        return _sweep(self.state, "service", SERVICE_TERMINAL,
                      self.default_timeout_ms, now)


class ActionTransactionManager:
    def __init__(self, state: StatePersistence, default_timeout_ms: int = 120000) -> None:
        self.state = state
        self.default_timeout_ms = default_timeout_ms

    def begin(self, goal_id: str, now: float, timeout_ms: int | None = None) -> str:
        t = self.default_timeout_ms if timeout_ms is None else timeout_ms
        if not self.state.begin_transaction(goal_id, "action", now, timeout_ms=t,
                                            initial_state="goalPending"):
            return "duplicate"
        return "goalPending"

    def set_state(self, goal_id: str, st: str, now: float) -> None:
        _check(st, ACTION_STATES, "action")
        self.state.update_transaction(goal_id, st, now)

    def next_feedback_seq(self, goal_id: str, now: float) -> int:
        return self.state.next_seq(goal_id, now)

    def state_of(self, goal_id: str) -> str | None:
        t = self.state.get_transaction(goal_id)
        return t["state"] if t else None

    def is_terminal(self, st: str) -> bool:
        return st in ACTION_TERMINAL

    def sweep_timeouts(self, now: float) -> list[str]:
        return _sweep(self.state, "action", ACTION_TERMINAL,
                      self.default_timeout_ms, now)


def _sweep(state: StatePersistence, kind: str, terminal: set[str] | frozenset[str],
           default_timeout_ms: int, now: float) -> list[str]:
    timed_out = []
    for t in state.active_transactions(kind):
        if t["state"] in terminal:
            continue
        timeout_ms = t.get("timeout_ms")
        if timeout_ms is None:
            timeout_ms = default_timeout_ms
        if timeout_ms == 0:   # 0 = IPE 측 타임아웃 없음 (설정 검증이 보장)
            continue
        if (now - t["started"]) * 1000.0 >= timeout_ms:
            state.update_transaction(t["corr_id"], "timeout", now)
            timed_out.append(t["corr_id"])
    return timed_out
