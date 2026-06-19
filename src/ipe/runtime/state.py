"""SQLite 기반 런타임 상태 저장소 (DESIGN v3 §13.7 / §13.8 / §16.1).

인바운드 알림 admission 상태 머신, 서비스/액션 트랜잭션, KV 저장소,
TERMINAL 스풀, 보존 기간 정리를 담당한다. 연결 모델은 스레드당 1연결
(WAL + NORMAL + busy_timeout)이고, ":memory:"는 sqlite 특성상 연결마다
별개 DB라 단일 공유 연결 + 락으로 대체한다(WAL 불가).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

from ipe.runtime.queues import (
    CLASS_OBSERVE_LATEST,
    OUTBOUND_CLASSES,
)

# ---------------------------------------------------------------------------
# 상태 어휘 (임의 문자열은 거부)
# ---------------------------------------------------------------------------

from ipe.core.vocab import (
    ACTION_STATES as ACTION_TX_STATES,
    ACTION_TERMINAL as ACTION_TX_TERMINAL,
    PROCESSED_ACTIVE_STATES,
    PROCESSED_TERMINAL_STATES,
    SERVICE_STATES as SERVICE_TX_STATES,
    SERVICE_TERMINAL as SERVICE_TX_TERMINAL,
)

TRANSACTION_KINDS: dict[str, frozenset[str]] = {
    "service": SERVICE_TX_STATES,
    "action": ACTION_TX_STATES,
}
_TX_TERMINAL_ALL = SERVICE_TX_TERMINAL | ACTION_TX_TERMINAL

_TX_COLUMNS = "corr_id, kind, state, seq, timeout_ms, started, updated"
_PROCESSED_COLUMNS = "robot_id, interface, corr_id, event_id, state, ts"


def _tx_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "corr_id": row[0],
        "kind": row[1],
        "state": row[2],
        "seq": row[3],
        "timeout_ms": row[4],
        "started": row[5],
        "updated": row[6],
    }


def _processed_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "robot_id": row[0],
        "interface": row[1],
        "corr_id": row[2],
        "event_id": row[3],
        "state": row[4],
        "ts": row[5],
    }


class StatePersistence:
    """IPE 런타임용 스레드 안전 sqlite 영속 계층."""

    def __init__(
        self,
        path: str = ":memory:",
        *,
        max_spool_entries: int = 10000,
        max_spool_mb: int = 64,
    ) -> None:
        if max_spool_entries < 1:
            raise ValueError("max_spool_entries must be >= 1")
        if max_spool_mb < 1:
            raise ValueError("max_spool_mb must be >= 1")
        self._path = path
        self._memory = path == ":memory:"
        self.max_spool_entries = max_spool_entries
        self.max_spool_bytes = max_spool_mb * 1024 * 1024
        self._closed = False
        # 메모리 sqlite DB는 연결마다 별개 — 공유 연결 + 락이 스레드 간
        # 상태 공유의 유일한 방법이다.
        self._mem_lock = threading.Lock()
        self._local = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        if self._memory:
            self._shared = self._new_conn()
        else:
            self._shared = None
        with self._conn() as conn:
            self._create_schema(conn)

    # -- 연결 관리 ------------------------------------------------------------

    def _new_conn(self) -> sqlite3.Connection:
        # isolation_level=None -> autocommit. 다중 문장 원자성이 필요한 곳은
        # 명시적 BEGIN IMMEDIATE를 쓴다.
        conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        if not self._memory:
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=2000")
        with self._conns_lock:
            self._all_conns.append(conn)
        return conn

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        """:memory:면 공유 연결+락, 아니면 호출 스레드 전용 연결을 내준다."""
        if self._closed:
            raise sqlite3.ProgrammingError("StatePersistence is closed")
        if self._memory:
            with self._mem_lock:
                yield self._shared  # type: ignore[misc]
        else:
            conn = getattr(self._local, "conn", None)
            if conn is None:
                conn = self._new_conn()
                self._local.conn = conn
            yield conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """읽기-수정-쓰기 시퀀스용 짧은 IMMEDIATE 트랜잭션."""
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS processed ("
            " robot_id TEXT NOT NULL,"
            " interface TEXT NOT NULL,"
            " corr_id TEXT NOT NULL,"
            " event_id TEXT,"
            " state TEXT NOT NULL,"
            " ts REAL NOT NULL,"
            " PRIMARY KEY (robot_id, interface, corr_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transactions ("
            " corr_id TEXT PRIMARY KEY,"
            " kind TEXT NOT NULL,"
            " state TEXT NOT NULL,"
            " seq INTEGER NOT NULL DEFAULT 0,"
            " timeout_ms INTEGER NOT NULL DEFAULT 0,"
            " started REAL NOT NULL,"
            " updated REAL NOT NULL)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS spool ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " class TEXT NOT NULL,"
            " key TEXT,"
            " payload TEXT NOT NULL,"
            " nbytes INTEGER NOT NULL,"
            " ts REAL NOT NULL)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spool_class ON spool(class)")

    # -- admission 상태 머신 ---------------------------------------------------

    def admit(
        self, robot_id: str, interface: str, corr_id: str, event_id: str, ts: float
    ) -> str:
        """인바운드 알림 이벤트 1건을 admission 처리한다.

        반환: 'queued'(신규), 'reaccepted'(이전 overflow 재수용),
        'duplicate'(그 외 어떤 상태로든 이미 알려짐).
        """
        try:
            with self._conn() as conn:
                conn.execute(
                    "INSERT INTO processed(robot_id, interface, corr_id, event_id, state, ts)"
                    " VALUES(?,?,?,?,'queued',?)",
                    (robot_id, interface, corr_id, event_id, ts),
                )
            return "queued"
        except sqlite3.IntegrityError:
            pass
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE processed SET state='queued', event_id=?, ts=?"
                " WHERE robot_id=? AND interface=? AND corr_id=? AND state='overflow'",
                (event_id, ts, robot_id, interface, corr_id),
            )
            return "reaccepted" if cur.rowcount == 1 else "duplicate"

    def mark_overflow(self, robot_id: str, interface: str, corr_id: str, ts: float) -> bool:
        """'queued' -> 'overflow' 전이 (enqueue 실패). CAS 의미론."""
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE processed SET state='overflow', ts=?"
                " WHERE robot_id=? AND interface=? AND corr_id=? AND state='queued'",
                (ts, robot_id, interface, corr_id),
            )
            return cur.rowcount == 1

    def cas_dispatch(self, robot_id: str, interface: str, corr_id: str, ts: float) -> bool:
        """'queued' -> 'dispatched' 전이. 한 호출자만 이긴다."""
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE processed SET state='dispatched', ts=?"
                " WHERE robot_id=? AND interface=? AND corr_id=? AND state='queued'",
                (ts, robot_id, interface, corr_id),
            )
            return cur.rowcount == 1

    def finish(
        self, robot_id: str, interface: str, corr_id: str, terminal_state: str, ts: float
    ) -> bool:
        """활성 상태를 종단 상태로 전이한다.

        종단 행은 불변(False 반환)이므로 finish를 두 번 불러도 안전하다.
        """
        if terminal_state not in PROCESSED_TERMINAL_STATES:
            raise ValueError(f"not a terminal processed state: {terminal_state!r}")
        placeholders = ",".join("?" * len(PROCESSED_ACTIVE_STATES))
        with self._conn() as conn:
            cur = conn.execute(
                f"UPDATE processed SET state=?, ts=?"
                f" WHERE robot_id=? AND interface=? AND corr_id=?"
                f" AND state IN ({placeholders})",
                (terminal_state, ts, robot_id, interface, corr_id, *PROCESSED_ACTIVE_STATES),
            )
            return cur.rowcount == 1

    def get_processed(self, robot_id: str, interface: str, corr_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            cur = conn.execute(
                f"SELECT {_PROCESSED_COLUMNS} FROM processed"
                " WHERE robot_id=? AND interface=? AND corr_id=?",
                (robot_id, interface, corr_id),
            )
            row = cur.fetchone()
        return _processed_row(row) if row else None

    def sweep_boot(self, ts: float) -> dict[str, list[dict[str, Any]]]:
        """부팅 시 남은 활성 행을 스윕한다.

        스윕 전 행들을 반환하고 원자적으로 전이한다:
        - 'queued'(크래시로 큐 유실) -> 'overflow' — catch-up 스윕이
          재수용('reaccepted')할 수 있게.
        - 'dispatched'(실행 결과 미상) -> 종단 'unknown' — 호출자가
          unknown/orphanedAtRestart 이벤트로 보고한다.
        """
        with self._tx() as conn:
            queued = [
                _processed_row(r)
                for r in conn.execute(
                    f"SELECT {_PROCESSED_COLUMNS} FROM processed WHERE state='queued'"
                    " ORDER BY ts"
                ).fetchall()
            ]
            dispatched = [
                _processed_row(r)
                for r in conn.execute(
                    f"SELECT {_PROCESSED_COLUMNS} FROM processed WHERE state='dispatched'"
                    " ORDER BY ts"
                ).fetchall()
            ]
            conn.execute(
                "UPDATE processed SET state='overflow', ts=? WHERE state='queued'", (ts,)
            )
            conn.execute(
                "UPDATE processed SET state='unknown', ts=? WHERE state='dispatched'", (ts,)
            )
        return {"queued": queued, "dispatched": dispatched}

    # -- 트랜잭션 ---------------------------------------------------------------

    def begin_transaction(
        self,
        corr_id: str,
        kind: str,
        ts: float,
        timeout_ms: int = 0,
        initial_state: str = "pending",
    ) -> bool:
        """새 트랜잭션 삽입. corr_id 중복이면 False."""
        if kind not in TRANSACTION_KINDS:
            raise ValueError(f"unknown transaction kind: {kind!r}")
        if timeout_ms < 0:
            raise ValueError("timeout_ms must be >= 0")
        if initial_state not in TRANSACTION_KINDS[kind]:
            raise ValueError(f"state {initial_state!r} not allowed for kind {kind!r}")
        try:
            with self._conn() as conn:
                conn.execute(
                    "INSERT INTO transactions(corr_id, kind, state, seq, timeout_ms,"
                    " started, updated) VALUES(?,?,?,0,?,?,?)",
                    (corr_id, kind, initial_state, timeout_ms, ts, ts),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def update_transaction(self, corr_id: str, state: str, ts: float) -> None:
        """트랜잭션 상태 설정. 해당 kind 어휘에 속한 상태만 허용."""
        with self._tx() as conn:
            row = conn.execute(
                "SELECT kind FROM transactions WHERE corr_id=?", (corr_id,)
            ).fetchone()
            if row is None:
                return
            allowed = TRANSACTION_KINDS[row[0]]
            if state not in allowed:
                raise ValueError(f"state {state!r} not allowed for kind {row[0]!r}")
            conn.execute(
                "UPDATE transactions SET state=?, updated=? WHERE corr_id=?",
                (state, ts, corr_id),
            )

    def next_seq(self, corr_id: str, ts: float) -> int:
        with self._tx() as conn:
            conn.execute(
                "UPDATE transactions SET seq=seq+1, updated=? WHERE corr_id=?",
                (ts, corr_id),
            )
            row = conn.execute(
                "SELECT seq FROM transactions WHERE corr_id=?", (corr_id,)
            ).fetchone()
            return row[0] if row else 0

    def get_transaction(self, corr_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            cur = conn.execute(
                f"SELECT {_TX_COLUMNS} FROM transactions WHERE corr_id=?", (corr_id,)
            )
            row = cur.fetchone()
        return _tx_row(row) if row else None

    def active_transactions(self, kind: str | None = None) -> list[dict[str, Any]]:
        with self._conn() as conn:
            if kind is None:
                cur = conn.execute(f"SELECT {_TX_COLUMNS} FROM transactions")
            else:
                cur = conn.execute(
                    f"SELECT {_TX_COLUMNS} FROM transactions WHERE kind=?", (kind,)
                )
            rows = cur.fetchall()
        return [_tx_row(r) for r in rows]

    def sweep_timeouts(self, now: float) -> list[dict[str, Any]]:
        """corr별 timeout_ms가 만료된 비종단 트랜잭션을 'timeout'으로 전이.

        timeout_ms == 0은 IPE 측 타임아웃 없음(여기서 스윕 안 함).
        전이된 행들을 반환한다(state는 이미 'timeout').
        """
        placeholders = ",".join("?" * len(_TX_TERMINAL_ALL))
        with self._tx() as conn:
            rows = conn.execute(
                f"SELECT {_TX_COLUMNS} FROM transactions"
                f" WHERE timeout_ms > 0 AND state NOT IN ({placeholders})"
                f" AND (? - started) * 1000.0 >= timeout_ms",
                (*_TX_TERMINAL_ALL, now),
            ).fetchall()
            swept = []
            for r in rows:
                conn.execute(
                    "UPDATE transactions SET state='timeout', updated=? WHERE corr_id=?",
                    (now, r[0]),
                )
                d = _tx_row(r)
                d["state"] = "timeout"
                d["updated"] = now
                swept.append(d)
        return swept

    # -- KV 저장소 --------------------------------------------------------------

    def set_kv(self, key: str, value: Any) -> None:
        """JSON 직렬화 가능한 값을 key 아래에 upsert."""
        encoded = json.dumps(value)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES(?,?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, encoded),
            )

    def get_kv(self, key: str, default: Any = None) -> Any:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def delete_kv(self, key: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM kv WHERE key=?", (key,))
            return cur.rowcount == 1

    # -- TERMINAL 스풀 -----------------------------------------------------------

    def spool_put(self, class_: str, key: str | None, payload_json: str, ts: float) -> int:
        """아웃바운드 op 1건 스풀링. 개수/용량 상한은 oldest-drop으로 강제.

        OBSERVE_LATEST는 key당 최신 1건만 유지(병합, drop으로 안 셈).
        드롭된(가장 오래된) 행 수를 반환한다.
        """
        if class_ not in OUTBOUND_CLASSES:
            raise ValueError(f"unknown outbound class: {class_!r}")
        nbytes = len(payload_json.encode("utf-8"))
        with self._tx() as conn:
            if class_ == CLASS_OBSERVE_LATEST and key is not None:
                conn.execute("DELETE FROM spool WHERE class=? AND key=?", (class_, key))
            conn.execute(
                "INSERT INTO spool(class, key, payload, nbytes, ts) VALUES(?,?,?,?,?)",
                (class_, key, payload_json, nbytes, ts),
            )
            dropped = 0
            while True:
                count, total = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(nbytes), 0) FROM spool"
                ).fetchone()
                if count <= self.max_spool_entries and total <= self.max_spool_bytes:
                    break
                if count <= 1:
                    break  # 마지막 남은(최신) 행은 절대 드롭하지 않는다
                conn.execute(
                    "DELETE FROM spool WHERE id=(SELECT MIN(id) FROM spool)"
                )
                dropped += 1
        return dropped

    def spool_list(self, limit: int = 100, class_: str | None = None) -> list[dict[str, Any]]:
        """오래된 순서의 스풀 행 (재전송이 삽입 순서를 보존하도록)."""
        with self._conn() as conn:
            if class_ is None:
                cur = conn.execute(
                    "SELECT id, class, key, payload, nbytes, ts FROM spool"
                    " ORDER BY id LIMIT ?",
                    (limit,),
                )
            else:
                cur = conn.execute(
                    "SELECT id, class, key, payload, nbytes, ts FROM spool"
                    " WHERE class=? ORDER BY id LIMIT ?",
                    (class_, limit),
                )
            rows = cur.fetchall()
        return [
            {"id": r[0], "class": r[1], "key": r[2], "payload": r[3], "nbytes": r[4], "ts": r[5]}
            for r in rows
        ]

    def spool_delete(self, ids: Iterable[int]) -> int:
        ids = list(ids)
        if not ids:
            return 0
        placeholders = ",".join("?" * len(ids))
        with self._conn() as conn:
            cur = conn.execute(f"DELETE FROM spool WHERE id IN ({placeholders})", ids)
            return cur.rowcount

    def spool_counts(self) -> dict[str, int]:
        with self._conn() as conn:
            rows = conn.execute("SELECT class, COUNT(*) FROM spool GROUP BY class").fetchall()
        return {r[0]: r[1] for r in rows}

    # -- 보존 기간 정리 -----------------------------------------------------------

    def cleanup(self, now: float, retention_days: int = 7) -> dict[str, int]:
        """보존 기간이 지난 종단 processed/transaction 행 삭제."""
        cutoff = now - retention_days * 86400.0
        p_terms = ",".join("?" * len(PROCESSED_TERMINAL_STATES))
        t_terms = ",".join("?" * len(_TX_TERMINAL_ALL))
        with self._conn() as conn:
            cur = conn.execute(
                f"DELETE FROM processed WHERE state IN ({p_terms}) AND ts <= ?",
                (*PROCESSED_TERMINAL_STATES, cutoff),
            )
            n_processed = cur.rowcount
            cur = conn.execute(
                f"DELETE FROM transactions WHERE state IN ({t_terms}) AND updated <= ?",
                (*_TX_TERMINAL_ALL, cutoff),
            )
            n_tx = cur.rowcount
        return {"processed": n_processed, "transactions": n_tx}

    # -- 수명 주기 --------------------------------------------------------------

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self._conns_lock:
            conns, self._all_conns = self._all_conns, []
        for conn in conns:
            try:
                if not self._memory:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error:
                pass
            try:
                conn.close()
            except sqlite3.Error:
                pass
