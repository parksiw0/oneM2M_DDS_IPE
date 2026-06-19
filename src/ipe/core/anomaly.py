"""이상값 검출 게이트 (DESIGN §7.4).

폐기 필터가 아니라 라우팅 분류기다 — 호출자는 (is_anomaly, score)를 받아
tag/escalate/suppress 모드로 라우팅한다. 상태는 (robot, interface) 키 단위
인메모리이고 재시작 시 재학습한다(warm-up 동안은 전부 정상 취급).
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any

from ipe.core.common import as_numbers

log = logging.getLogger(__name__)

DEFAULT_WINDOW = 256
DEFAULT_RETRAIN_EVERY = 128
DEFAULT_MIN_SAMPLES = 64
DEFAULT_CONTAMINATION = 0.05
DEFAULT_MAD_THRESHOLD = 3.5


def flatten_numbers(fields: dict[str, Any], names: list[str]) -> list[float] | None:
    """지정 필드를 고정 길이 수치 벡터로 평탄화. 수치가 없으면 None(판정 불가=정상)."""
    out: list[float] = []
    for name in names:
        nums = as_numbers(fields.get(name))
        if nums is None:
            return None
        out.extend(nums)
    return out or None


class MadDetector:
    """수정 z-score(중앙값 절대편차) — 의존성 0 폴백 검출기."""

    def __init__(self, window: int, min_samples: int, threshold: float, **_: Any) -> None:
        self.buf: deque[list[float]] = deque(maxlen=window)
        self.min_samples = min_samples
        self.threshold = threshold

    def score(self, vec: list[float]) -> tuple[bool, float]:
        history = list(self.buf)
        self.buf.append(vec)
        if len(history) < self.min_samples:
            return False, 0.0
        worst = 0.0
        for i, x in enumerate(vec):
            col = sorted(row[i] for row in history if i < len(row))
            if not col:
                continue
            med = col[len(col) // 2]
            mad = sorted(abs(c - med) for c in col)[len(col) // 2]
            if mad == 0.0:
                # 상수 신호: 값이 조금이라도 벗어나면 이상
                z = 0.0 if x == med else self.threshold + 1.0
            else:
                z = 0.6745 * abs(x - med) / mad
            worst = max(worst, z)
        return worst > self.threshold, worst


class IsolationForestDetector:
    """sklearn IsolationForest — 슬라이딩 윈도 버퍼 + 표본 주기 재학습."""

    def __init__(self, window: int, min_samples: int, retrain_every: int,
                 contamination: float, **_: Any) -> None:
        from sklearn.ensemble import IsolationForest  # 지연 import — 부재 시 게이트가 무력화
        self._cls = IsolationForest
        self.buf: deque[list[float]] = deque(maxlen=window)
        self.min_samples = min_samples
        self.retrain_every = retrain_every
        self.contamination = contamination
        self.model: Any = None
        self._since_train = 0
        self._dim: int | None = None

    def score(self, vec: list[float]) -> tuple[bool, float]:
        if self._dim is None:
            self._dim = len(vec)
        if len(vec) != self._dim:
            return False, 0.0   # 가변 길이 벡터는 판정 불가 — 정상 취급
        history_n = len(self.buf)
        self.buf.append(vec)
        self._since_train += 1
        if history_n < self.min_samples:
            return False, 0.0
        if self.model is None or self._since_train >= self.retrain_every:
            self.model = self._cls(contamination=self.contamination, random_state=0)
            self.model.fit(list(self.buf))
            self._since_train = 0
        # decision_function < 0 = 이상 영역. score는 양수가 클수록 더 이상.
        raw = float(self.model.decision_function([vec])[0])
        return raw < 0.0, -raw


DETECTORS = {"mad": MadDetector, "isolation_forest": IsolationForestDetector}


class AnomalyGate:
    """(robot, interface)별 검출기 인스턴스를 관리하는 게이트."""

    def __init__(self) -> None:
        self._state: dict[str, Any] = {}
        self._disabled: set[str] = set()
        self.suppressed: dict[str, int] = {}   # suppress 모드 드롭 카운터(diag 노출)

    def evaluate(self, key: str, flt: dict[str, Any],
                 fields: dict[str, Any]) -> tuple[bool, float]:
        if key in self._disabled:
            return False, 0.0
        vec = flatten_numbers(fields, flt.get("fields") or [])
        if vec is None:
            return False, 0.0
        det = self._state.get(key)
        if det is None:
            name = flt.get("detector", "isolation_forest")
            kwargs = {
                "window": flt.get("window", DEFAULT_WINDOW),
                "min_samples": flt.get("min_samples", DEFAULT_MIN_SAMPLES),
                "retrain_every": flt.get("retrain_every", DEFAULT_RETRAIN_EVERY),
                "contamination": flt.get("contamination", DEFAULT_CONTAMINATION),
                "threshold": flt.get("threshold", DEFAULT_MAD_THRESHOLD),
            }
            try:
                det = self._state[key] = DETECTORS[name](**kwargs)
                for vec in getattr(self, "_pending_bufs", {}).pop(key, []):
                    det.buf.append([float(x) for x in vec])
            except Exception as e:
                # sklearn 부재 등 — 필터를 무력화하고 통과(기동 이벤트는 호출자 소관)
                log.warning("anomaly detector %s unavailable for %s: %s", name, key, e)
                self._disabled.add(key)
                return False, 0.0
        try:
            return det.score(vec)
        except Exception:
            # 반복 실패는 메시지당 로그 폭주가 되므로 1회 보고 후 키 비활성
            log.exception("anomaly scoring failed for %s — disabling gate", key)
            self._disabled.add(key)
            return False, 0.0

    def note_suppressed(self, key: str) -> None:
        self.suppressed[key] = self.suppressed.get(key, 0) + 1

    def snapshot(self) -> dict[str, list[list[float]]]:
        """학습 버퍼 직렬화(영속용) — 모델이 아니라 버퍼만(§7.4)."""
        return {k: [list(v) for v in d.buf]
                for k, d in self._state.items() if hasattr(d, "buf")}

    def restore(self, bufs: dict[str, list[list[float]]]) -> None:
        """부팅 시 버퍼 복원 — 검출기는 lazy 생성이므로 보류했다가 생성 직후 주입."""
        self._pending_bufs = dict(bufs or {})
