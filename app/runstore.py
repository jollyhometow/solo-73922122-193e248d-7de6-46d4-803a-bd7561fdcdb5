"""运行批次（弱网分段补传）的 SQLite 持久化与事务逻辑。

一致性设计：
  * 进程内用一把可重入锁串行化所有批次操作；SQLite 侧用
    ``BEGIN IMMEDIATE`` 事务把"读-判-写"包成原子单元，跨进程同样安全；
  * 分段内容在写入前已规范化为 canonical JSON，同内容重传按字符串比对
    判等 => 幂等；异内容重传、跨段重复记录 ID 在写库前抛错，事务回滚，
    已收数据不变；
  * 发布在单个事务内完成"齐段校验 + 快照组装 + 状态翻转"，快照落库后
    才提交 => 并发补传与发布都不会暴露半成品；
  * 发布后快照原样存于 runs.snapshot_json，发布重试直接返回库存快照，
    保证逐字节一致。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    total_segments  INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    snapshot_json   TEXT,
    published_at    TEXT,
    created_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS segments (
    run_id          TEXT NOT NULL,
    segment_index   INTEGER NOT NULL,
    payload_json    TEXT NOT NULL,
    received_at     TEXT NOT NULL,
    PRIMARY KEY (run_id, segment_index),
    FOREIGN KEY (run_id) REFERENCES runs (run_id)
);
"""


class RunNotFound(Exception):
    """运行批次不存在。"""


class RunAlreadyPublished(Exception):
    """运行已发布，拒绝再写入分段。"""


class TotalSegmentsMismatch(Exception):
    """提交的总段数与运行已确立的总段数不一致。"""

    def __init__(self, established: int) -> None:
        super().__init__(f"总段数应为 {established}")
        self.established = established


class SegmentConflict(Exception):
    """同段号异内容重传。"""


class DuplicateRecordId(Exception):
    """记录 ID 与已收的其他段重复。"""

    def __init__(self, duplicates: list[str]) -> None:
        super().__init__(f"跨段重复记录 ID: {', '.join(duplicates)}")
        self.duplicates = duplicates


class RunIncomplete(Exception):
    """发布时仍有缺段。"""

    def __init__(self, missing: list[int]) -> None:
        super().__init__(f"缺少分段: {missing}")
        self.missing = missing


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunStore:
    """运行批次的 SQLite 存储。所有公共方法都可安全并发调用。"""

    def __init__(self, db_path: str) -> None:
        self._lock = threading.RLock()
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ 写操作

    def submit_segment(
        self,
        run_id: str,
        segment_index: int,
        total_segments: int,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """提交一个分段，返回该运行的状态视图（含 replay 标记）。

        records 必须是已校验、已规范化的 canonical 记录列表。
        同段同内容重传幂等（replay=True，不写库）；异内容重传、总段数不一致、
        跨段重复记录 ID、已发布运行均抛异常，且事务回滚不改已收数据。
        """
        payload = json.dumps(
            records, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        record_ids = [r["id"] for r in records]
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT total_segments, status FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    self._conn.execute(
                        "INSERT INTO runs (run_id, total_segments, status, created_at)"
                        " VALUES (?, ?, 'open', ?)",
                        (run_id, total_segments, _utc_now_iso()),
                    )
                else:
                    if run["status"] == "published":
                        raise RunAlreadyPublished()
                    if run["total_segments"] != total_segments:
                        raise TotalSegmentsMismatch(run["total_segments"])

                existing = self._conn.execute(
                    "SELECT payload_json FROM segments"
                    " WHERE run_id = ? AND segment_index = ?",
                    (run_id, segment_index),
                ).fetchone()
                if existing is not None:
                    if existing["payload_json"] == payload:
                        self._conn.execute("COMMIT")
                        return self._status_view(run_id, replay=True)
                    raise SegmentConflict()

                seen: set[str] = set()
                for row in self._conn.execute(
                    "SELECT payload_json FROM segments"
                    " WHERE run_id = ? AND segment_index != ?",
                    (run_id, segment_index),
                ).fetchall():
                    seen.update(r["id"] for r in json.loads(row["payload_json"]))
                duplicates = sorted(set(record_ids) & seen)
                if duplicates:
                    raise DuplicateRecordId(duplicates)

                self._conn.execute(
                    "INSERT INTO segments"
                    " (run_id, segment_index, payload_json, received_at)"
                    " VALUES (?, ?, ?, ?)",
                    (run_id, segment_index, payload, _utc_now_iso()),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self._status_view(run_id, replay=False)

    def publish(self, run_id: str) -> dict[str, Any]:
        """发布运行：齐段后在单事务内固定完整快照并返回。

        缺段抛 RunIncomplete（运行保持开放，可续传）；已发布则直接返回
        库存原快照（重试幂等）；运行不存在抛 RunNotFound。
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT total_segments, status, snapshot_json FROM runs"
                    " WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if run is None:
                    raise RunNotFound(run_id)
                if run["status"] == "published":
                    snapshot = json.loads(run["snapshot_json"])
                    self._conn.execute("COMMIT")
                    return snapshot

                rows = self._conn.execute(
                    "SELECT segment_index, payload_json FROM segments"
                    " WHERE run_id = ? ORDER BY segment_index",
                    (run_id,),
                ).fetchall()
                total = run["total_segments"]
                received = {r["segment_index"] for r in rows}
                missing = [i for i in range(total) if i not in received]
                if missing:
                    raise RunIncomplete(missing)

                segments = [
                    {
                        "segment_index": r["segment_index"],
                        "records": json.loads(r["payload_json"]),
                    }
                    for r in rows
                ]
                published_at = _utc_now_iso()
                snapshot = {
                    "run_id": run_id,
                    "state": "published",
                    "total_segments": total,
                    "record_count": sum(len(s["records"]) for s in segments),
                    "published_at": published_at,
                    "segments": segments,
                }
                self._conn.execute(
                    "UPDATE runs SET status = 'published', snapshot_json = ?,"
                    " published_at = ? WHERE run_id = ?",
                    (json.dumps(snapshot, ensure_ascii=False), published_at, run_id),
                )
                self._conn.execute("COMMIT")
                return snapshot
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ 读操作

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """返回运行状态视图；不存在返回 None。已发布时视图含原快照。"""
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if exists is None:
                return None
            return self._status_view(run_id)

    # ------------------------------------------------------------------ 内部

    def _status_view(
        self, run_id: str, replay: bool | None = None
    ) -> dict[str, Any]:
        """组装状态视图。调用方须已持有 self._lock。"""
        run = self._conn.execute(
            "SELECT total_segments, status, snapshot_json, published_at FROM runs"
            " WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        rows = self._conn.execute(
            "SELECT segment_index, payload_json FROM segments"
            " WHERE run_id = ? ORDER BY segment_index",
            (run_id,),
        ).fetchall()
        received = [r["segment_index"] for r in rows]
        total = run["total_segments"]
        view: dict[str, Any] = {
            "run_id": run_id,
            "state": run["status"],
            "total_segments": total,
            "received_segments": received,
            "missing_segments": [i for i in range(total) if i not in set(received)],
            "received_record_count": sum(
                len(json.loads(r["payload_json"])) for r in rows
            ),
            "published_at": run["published_at"],
        }
        if run["status"] == "published":
            view["snapshot"] = json.loads(run["snapshot_json"])
        if replay is not None:
            view["replay"] = replay
        return view
