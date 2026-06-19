"""CIN / reference / FCNT 콘텐츠 빌더 (DESIGN §7.2, §16.3).

``build_cin_content``는 **con 본문 dict**만 반환한다(``m2m:cin`` 봉투는 oneM2M
계층 소관). ``seq``가 상관 키 — 소비자는 seq로 순서를 잡고, 타임스탬프 둘은
메타데이터일 뿐이다(벽시계로 정렬하지 않는다).
"""

from __future__ import annotations

from typing import Any

from ipe.core.normalize import epoch_to_onem2m_ts, get_path, sanitize_value


def build_cin_content(normalized: dict[str, Any], ir: dict[str, Any]) -> dict[str, Any]:
    """CIN con 본문 {seq, source_ts, ingest_ts, topic, robot, data} 생성."""
    source_ts = ir.get("source_ts")
    return {
        "seq": ir["seq"],
        "source_ts": epoch_to_onem2m_ts(source_ts) if source_ts is not None else None,
        "ingest_ts": epoch_to_onem2m_ts(ir["ingest_ts"]),
        "topic": normalized["topic"],
        "robot": normalized["robot"],
        "data": normalized["fields"],
    }


def build_reference_content(meta: dict[str, Any]) -> dict[str, Any]:
    """레퍼런스 con 본문(메타데이터만, payload 본문 없음) 생성.

    스키마: {kind: "reference", mime, size_bytes, hash?, seq, source_ts, note}.
    ``hash``는 선택이며 주어졌을 때만 넣는다.
    """
    out: dict[str, Any] = {
        "kind": "reference",
        "mime": meta.get("mime"),
        "size_bytes": meta.get("size_bytes"),
        "seq": meta.get("seq"),
        "source_ts": meta.get("source_ts"),
        "note": meta.get("note"),
    }
    if meta.get("hash") is not None:
        out["hash"] = meta["hash"]
    return out


def build_fcnt_attrs(
    normalized: dict[str, Any],
    field_map: dict[str, str],
) -> dict[str, Any]:
    """정규화된 필드를 SDT 짧은 이름으로 매핑 — field_map 항목만.

    dgt/src/frm을 주입하지 않는다: SDT에 없는 속성을 만들어내면 안 된다.
    None 값은 건너뛴다 — oneM2M UPDATE에서 null은 속성 삭제를 뜻한다.
    """
    fields = normalized["fields"]
    attrs: dict[str, Any] = {}
    for short, src in field_map.items():
        found, value = get_path(fields, src)
        if not found:
            continue
        v = sanitize_value(value)
        if v is not None:
            attrs[short] = v
    return attrs


def build_fcnt_create(
    cnd: str,
    fcnt_type: str,
    resource_name: str,
    initial_attrs: dict[str, Any],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "cnd": cnd,
        "rn": resource_name,
    }
    body.update(initial_attrs)
    return {fcnt_type: body}


def build_fcnt_update(fcnt_type: str, attrs: dict[str, Any]) -> dict[str, Any]:
    return {fcnt_type: attrs}
