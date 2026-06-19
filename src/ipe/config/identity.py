"""robot 식별, 인터페이스->oneM2M 네이밍, 패턴 매칭.

인터페이스의 소속 robot 판정(최장 namespace prefix), 충돌 없는 oneM2M 경로
생성, `{capture}`/`*`/`**` 패턴의 인터페이스 선택과 캡처 세그먼트의 경로 환류 —
브리지를 멀티 robot 안전·설정 주도로 만드는 토대.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from functools import lru_cache

from ipe.config.spec import RobotSpec

# oneM2M resourceName: 보수적인 CSE 안전 문자 집합만 허용하고, 단일 경로
# 세그먼트 안의 그 외 문자는 sanitize 문자로 치환한다. '/'는 경로 구분자
# (path_style이 처리)이지 조용히 sanitize해 버릴 대상이 아니다.
_ILLEGAL = re.compile(r"[^A-Za-z0-9_\-]")
_MAX_SEGMENT = 64


def sanitize_segment(seg: str, repl: str = "_") -> str:
    out = _ILLEGAL.sub(repl, seg)
    if not out:
        out = repl
    if out[0].isdigit():           # oneM2M rn은 숫자로 시작하면 안 된다
        out = repl + out
    return out[:_MAX_SEGMENT]


# ---------------------------------------------------------------------------
# 패턴 매칭: 이름 있는 캡처를 지원하는 glob
#   '**' -> '/' 포함 임의 문자      '*' -> '/' 제외 임의 문자
#   '{robot}' -> 세그먼트 하나의 이름 있는 캡처 ('/' 불포함)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1024)
def compile_pattern(pattern: str) -> re.Pattern[str]:
    """glob+캡처 패턴 컴파일. 캡처 이름을 선제 검증(식별자 문법, 중복 금지)해서
    잘못된 이름이 re.compile의 불투명한 re.error 대신 여기서 ValueError를
    내게 한다(B5)."""
    out: list[str] = ["^"]
    names: set[str] = set()
    i = 0
    n = len(pattern)
    while i < n:
        c = pattern[i]
        if c == "{":
            j = pattern.find("}", i)
            if j == -1:
                raise ValueError(f"pattern '{pattern}': unterminated '{{' capture")
            name = pattern[i + 1 : j]
            if not name.isidentifier():
                raise ValueError(
                    f"pattern '{pattern}': capture name '{name}' is not a valid identifier"
                )
            if name in names:
                raise ValueError(f"pattern '{pattern}': duplicate capture name '{name}'")
            names.add(name)
            out.append(f"(?P<{name}>[^/]+)")
            i = j + 1
        elif c == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                out.append(".*")
                i += 2
            else:
                out.append("[^/]*")
                i += 1
        else:
            out.append(re.escape(c))
            i += 1
    out.append("$")
    return re.compile("".join(out))


def match_pattern(pattern: str, interface: str) -> dict[str, str] | None:
    m = compile_pattern(pattern).match(interface)
    if m is None:
        return None
    return m.groupdict()


def pattern_specificity(pattern: str) -> int:
    """클수록 구체적. 결정론적 우선순위 동률 깨기에 쓴다.

    정규식이 아니라 토큰 순회라서 '*' 뒤의 리터럴도 센다 — '/a/*/b'가
    '/a/*'를 이겨야 한다(B4).
    """
    literal = 0
    penalty = 0
    i = 0
    n = len(pattern)
    while i < n:
        c = pattern[i]
        if c == "{":
            j = pattern.find("}", i)
            if j == -1:        # 형식 오류; compile_pattern이 크게 알린다
                literal += 1
                i += 1
                continue
            penalty += 1
            i = j + 1
        elif c == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                penalty += 2
                i += 2
            else:
                penalty += 1
                i += 1
        else:
            literal += 1
            i += 1
    return literal - penalty


def apply_captures(template: str, captures: dict[str, str]) -> str:
    out = template
    for k, v in captures.items():
        out = out.replace("{" + k + "}", v)
    return out


_CAPTURE_REF = re.compile(r"\{[^{}/]*\}")


def unresolved_captures(text: str) -> list[str]:
    """apply_captures 후 남은 플레이스홀더 — 조용히 sanitize되어 경로에
    들어가면 안 되고 반드시 보고해야 한다(B6)."""
    return _CAPTURE_REF.findall(text)


# ---------------------------------------------------------------------------
# robot 판정
# ---------------------------------------------------------------------------

def resolve_robot(interface: str, robots: Iterable[RobotSpec]) -> RobotSpec:
    """가장 긴 비어 있지 않은 namespace prefix가 이긴다; 없으면 첫/기본 robot."""
    best: RobotSpec | None = None
    best_len = -1
    fallback: RobotSpec | None = None
    for r in robots:
        if fallback is None:
            fallback = r
        ns = r.namespace.rstrip("/")
        if ns == "":
            if best is None:
                fallback = r
            continue
        if (interface == ns or interface.startswith(ns + "/")) and len(ns) > best_len:
            best, best_len = r, len(ns)
    if best is not None:
        return best
    # 명시적으로 빈 namespace인 robot을 catch-all로 우선
    for r in robots:
        if r.namespace.rstrip("/") == "":
            return r
    return fallback if fallback is not None else RobotSpec(id="default")


# ---------------------------------------------------------------------------
# 인터페이스 -> 경로 세그먼트
# ---------------------------------------------------------------------------

def interface_segments(
    robot: RobotSpec,
    interface: str,
    path_style: str,
    sanitize: str,
    alias: str | None,
) -> list[str]:
    """인터페이스의 브랜치 상대 oneM2M 세그먼트 (robot 루트 제외)."""
    if alias:
        return [sanitize_segment(alias, sanitize)]

    ns = robot.namespace.rstrip("/")
    rel = interface
    if ns and (interface == ns or interface.startswith(ns + "/")):
        rel = interface[len(ns):]
    rel = rel.strip("/")
    parts = [p for p in rel.split("/") if p] or [interface.strip("/").replace("/", sanitize)]

    if path_style == "flat":
        joined = "__".join(parts)
        return [sanitize_segment(joined, sanitize)]
    # nested(기본)와 alias 없는 aliased는 nested로 동작
    return [sanitize_segment(p, sanitize) for p in parts]


def robot_root(robot: RobotSpec) -> str:
    """공유 AE 아래 robot의 경로 세그먼트 (ae_per_robot이면 빈 문자열)."""
    if robot.ae_per_robot:
        return ""
    return sanitize_segment(robot.id)


def robot_ae(robot: RobotSpec, shared_ae: str) -> str:
    """이 robot의 인터페이스 서브트리를 소유하는 AE.

    절대 경로 유일성 검사가 이 값을 키로 쓰므로, ae_per_robot robot은
    자연히 분리되고 공유 AE robot끼리는 충돌하면 안 된다.
    """
    if robot.ae_per_robot:
        return robot.ae_name or f"C{robot.id}"
    return shared_ae
