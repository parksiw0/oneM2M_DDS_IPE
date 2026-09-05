"""유한 런타임 큐 (DESIGN v3 §13.8).

InboundQueue는 control(먼저 드레인)/normal 2레인이고 put은 전부 논블로킹 —
False면 포화이며 호출자가 admission 상태 머신을 따른다.
OutboundQueue는 클래스별 정책: TERMINAL은 드롭 금지(포화 시 False, 호출자가
스풀로 강등), OBSERVE_LATEST는 키별 병합(최신 우선), OBSERVE_BULK는 oldest-drop.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict, deque
from collections.abc import Hashable
from queue import Empty
from typing import Any

from ipe.core.vocab import (  # 정본은 vocab — 재export(기존 import 경로 호환)
    CLASS_OBSERVE_BULK,
    CLASS_OBSERVE_LATEST,
    CLASS_TERMINAL,
    OUTBOUND_CLASSES,
)


class InboundQueue:
    """유한 2레인 인바운드 큐 (control 레인을 normal보다 먼저 드레인)."""

    def __init__(self, maxsize: int = 1000, control_maxsize: int = 64) -> None:
        if maxsize < 1 or control_maxsize < 1:
            raise ValueError("queue sizes must be >= 1")
        self._lock = threading.Lock()
        self._maxsize = maxsize
        self._control_maxsize = control_maxsize
        self._normal: deque[Any] = deque()
        self._control: deque[Any] = deque()

    def put_normal(self, item: Any) -> bool:
        """논블로킹. False면 normal 레인 포화."""
        with self._lock:
            if len(self._normal) >= self._maxsize:
                return False
            self._normal.append(item)
            return True

    def put_control(self, item: Any) -> bool:
        """논블로킹. control 레인은 normal 포화와 무관."""
        with self._lock:
            if len(self._control) >= self._control_maxsize:
                return False
            self._control.append(item)
            return True

    def get_batch(self, budget: int) -> list[Any]:
        """최대 `budget`개 드레인: control 레인 전부 먼저, 그다음 normal."""
        if budget < 1:
            return []
        out: list[Any] = []
        with self._lock:
            while self._control and len(out) < budget:
                out.append(self._control.popleft())
            while self._normal and len(out) < budget:
                out.append(self._normal.popleft())
        return out

    def depths(self) -> dict[str, int]:
        with self._lock:
            return {"control": len(self._control), "normal": len(self._normal)}

    def empty(self) -> bool:
        with self._lock:
            return not self._control and not self._normal


def _derive_latest_key(op: Any) -> Hashable | None:
    """op에서 (robot, interface, view) 키를 최선껏 추출."""
    if isinstance(op, dict):
        robot = op.get("robot_id", op.get("robot"))
        interface = op.get("interface")
        view = op.get("view")
    else:
        robot = getattr(op, "robot_id", getattr(op, "robot", None))
        interface = getattr(op, "interface", None)
        view = getattr(op, "view", None)
    if robot is None or interface is None:
        return None
    return (robot, interface, view)


def _derive_order_key(op: Any) -> Hashable:
    if isinstance(op, dict):
        robot = op.get("robot_id", op.get("robot"))
        interface = op.get("interface")
    else:
        robot = getattr(op, "robot_id", getattr(op, "robot", None))
        interface = getattr(op, "interface", None)
    if robot is None or interface is None:
        return ("__global__",)
    return (robot, interface)


class OutboundQueue:
    """클래스별 정책 + get 시 클래스 우선순위를 가진 유한 아웃바운드 큐."""

    def __init__(
        self,
        maxsize: int = 5000,
        *,
        terminal_maxsize: int | None = None,
        bulk_maxsize: int | None = None,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self._terminal_maxsize = terminal_maxsize if terminal_maxsize is not None else maxsize
        self._bulk_maxsize = bulk_maxsize if bulk_maxsize is not None else maxsize
        self._latest_maxsize = maxsize
        if self._terminal_maxsize < 1 or self._bulk_maxsize < 1:
            raise ValueError("queue sizes must be >= 1")
        self._cond = threading.Condition()
        self._terminal: deque[Any] = deque()
        self._latest: OrderedDict[Hashable, Any] = OrderedDict()
        self._bulk: deque[Any] = deque()
        self._dropped = {CLASS_OBSERVE_BULK: 0, CLASS_OBSERVE_LATEST: 0}
        self._active_keys: set[Hashable] = set()

    def put(self, op: Any, class_: str, key: Hashable | None = None) -> bool:
        """클래스 정책에 따른 논블로킹 put.

        False는 TERMINAL 포화 시에만 반환된다(호출자가 스풀링).
        OBSERVE_LATEST는 병합 키 필수: 명시적 `key` 또는 op의
        (robot_id/robot, interface, view) 필드에서 유도.
        """
        if class_ not in OUTBOUND_CLASSES:
            raise ValueError(f"unknown outbound class: {class_!r}")
        with self._cond:
            if class_ == CLASS_TERMINAL:
                if len(self._terminal) >= self._terminal_maxsize:
                    return False
                self._terminal.append(op)
            elif class_ == CLASS_OBSERVE_LATEST:
                if key is None:
                    key = _derive_latest_key(op)
                if key is None:
                    raise ValueError(
                        "OBSERVE_LATEST op needs a coalesce key "
                        "(explicit or robot/interface/view fields)"
                    )
                # 기존 키를 교체해도 큐 내 위치는 유지된다(dict 삽입 순서
                # 의미론) — 병합이 특정 키를 굶기지 못한다.
                if key not in self._latest and len(self._latest) >= self._latest_maxsize:
                    self._latest.popitem(last=False)
                    self._dropped[CLASS_OBSERVE_LATEST] += 1
                self._latest[key] = op
            else:  # CLASS_OBSERVE_BULK
                if len(self._bulk) >= self._bulk_maxsize:
                    self._bulk.popleft()
                    self._dropped[CLASS_OBSERVE_BULK] += 1
                self._bulk.append(op)
            self._cond.notify()
            return True

    def get(self, timeout: float | None = None) -> Any:
        """TERMINAL > LATEST > BULK 우선순위로 pop, 클래스 내 FIFO.

        타임아웃 시 queue.Empty를 던진다.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                if self._terminal:
                    return self._terminal.popleft()
                if self._latest:
                    _, op = self._latest.popitem(last=False)
                    return op
                if self._bulk:
                    return self._bulk.popleft()
                if deadline is None:
                    self._cond.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not self._cond.wait(remaining):
                        if self._terminal or self._latest or self._bulk:
                            continue
                        raise Empty

    def get_nowait(self) -> Any:
        return self.get(timeout=0)

    def claim(self, timeout: float | None = None) -> Any:
        """Claim the next operation while serializing each robot interface."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                op = self._pop_claimable()
                if op is not None:
                    self._active_keys.add(_derive_order_key(op))
                    return op
                if deadline is None:
                    self._cond.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not self._cond.wait(remaining):
                        op = self._pop_claimable()
                        if op is not None:
                            self._active_keys.add(_derive_order_key(op))
                            return op
                        raise Empty

    def _pop_claimable(self) -> Any | None:
        for index, op in enumerate(self._terminal):
            if _derive_order_key(op) not in self._active_keys:
                del self._terminal[index]
                return op
        for key, op in self._latest.items():
            if _derive_order_key(op) not in self._active_keys:
                # Return immediately after deletion; do not advance the iterator.
                del self._latest[key]
                return op
        for index, op in enumerate(self._bulk):
            if _derive_order_key(op) not in self._active_keys:
                del self._bulk[index]
                return op
        return None

    def try_claim(self, op: Any) -> bool:
        """Reserve an interface for spool replay without enqueuing another copy."""
        with self._cond:
            key = _derive_order_key(op)
            if key in self._active_keys:
                return False
            self._active_keys.add(key)
            return True

    def release(self, op: Any) -> None:
        with self._cond:
            self._active_keys.discard(_derive_order_key(op))
            self._cond.notify_all()

    def depths(self) -> dict[str, int]:
        with self._cond:
            return {
                CLASS_TERMINAL: len(self._terminal),
                CLASS_OBSERVE_LATEST: len(self._latest),
                CLASS_OBSERVE_BULK: len(self._bulk),
            }

    def dropped_counters(self) -> dict[str, int]:
        with self._cond:
            return dict(self._dropped)

    def empty(self) -> bool:
        with self._cond:
            return not (self._terminal or self._latest or self._bulk)

    def idle(self) -> bool:
        with self._cond:
            return not (self._terminal or self._latest or self._bulk or self._active_keys)
