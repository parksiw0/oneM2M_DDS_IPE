"""상태 어휘·큐 클래스의 단일 진실원.

transaction.py(검증)와 runtime/state.py(DB 제약)가 같은 집합을 import한다 —
한쪽만 고쳐 어휘가 갈라지는 사고를 구조적으로 막는다. 이 모듈은 어디에도
의존하지 않는다(core↔runtime 계층 위반 해소).
"""

from __future__ import annotations

# ── 큐 클래스 (§13.8) — policy(생산)·queues(소비)·state(spool)가 공유
CLASS_TERMINAL = "TERMINAL"
CLASS_OBSERVE_LATEST = "OBSERVE_LATEST"
CLASS_OBSERVE_BULK = "OBSERVE_BULK"
OUTBOUND_CLASSES = frozenset({CLASS_TERMINAL, CLASS_OBSERVE_LATEST, CLASS_OBSERVE_BULK})

# ── 서비스 트랜잭션 어휘 (§11, invocationStatus 8종)
SERVICE_STATES = frozenset({
    "pending", "accepted", "invoked", "responded",
    "timeout", "rejected", "failed", "duplicate",
})
SERVICE_TERMINAL = frozenset({"responded", "timeout", "rejected", "failed", "duplicate"})

# ── 액션 종결 사유 (§12, actionStatusEvent의 terminationReason)
TERMINATION_REASONS = frozenset({
    "succeeded", "aborted", "canceled", "goalRejected", "timeout", "failed",
    "serverUnavailable", "expired", "duplicateGoal", "cancelAccepted",
    "cancelRejected", "orphanedAtRestart", "shutdownAbandoned",
})

# ── 액션 트랜잭션 수명주기 (§12) — 종결 사유와 in-flight 상태의 합집합이
#    DB가 허용하는 전체 상태다(state.py가 파생)
ACTION_INFLIGHT = frozenset({
    "pending", "goalPending", "goalSent", "goalAccepted",
    "executing", "canceling", "duplicate",
})
ACTION_TERMINAL = TERMINATION_REASONS | frozenset({"resultReceived"})
ACTION_STATES = ACTION_TERMINAL | ACTION_INFLIGHT

# ── admission 상태 기계 (§13.7)
PROCESSED_ACTIVE_STATES = frozenset({"queued", "dispatched", "overflow"})
PROCESSED_TERMINAL_STATES = frozenset({
    "succeeded", "failed", "expired", "rejected", "duplicate", "invalid",
    "accessDenied", "canceled", "unknown", "shuttingDown", "shutdownAbandoned",
})
