"""运行批次 API 路由：分段补传、发布、状态查询。

语义约定：
  * 分段可乱序提交；同段同内容重传幂等（replay=true）；
  * 同段异内容重传、跨段重复记录 ID、总段数不一致 => 409，已收数据不变；
  * 发布缺段 => 409 并返回缺号，运行保持开放可续传；齐段后发布在单事务内
    固定完整快照；发布重试原样返回库存快照；
  * 已发布的运行拒绝任何分段写入（409）。
"""

from __future__ import annotations

import os
import threading
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Path
from fastapi.responses import JSONResponse

from .examples import RUN_SEGMENT_EXAMPLES
from .runschemas import (
    MAX_SEGMENTS,
    ErrorOut,
    RunRecordIn,
    RunStatusOut,
    SegmentAckOut,
    SegmentSubmission,
    SnapshotOut,
)
from .runstore import (
    DuplicateRecordId,
    RunAlreadyPublished,
    RunIncomplete,
    RunNotFound,
    RunStore,
    SegmentConflict,
    TotalSegmentsMismatch,
)

DEFAULT_DB_PATH = "data/runs.db"

_store: RunStore | None = None
_store_lock = threading.Lock()


def get_store() -> RunStore:
    """进程级 RunStore 单例（测试可用 dependency_overrides 替换）。"""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = RunStore(os.environ.get("RUNS_DB_PATH", DEFAULT_DB_PATH))
    return _store


def _canonical_records(records: list[RunRecordIn]) -> list[dict[str, Any]]:
    """把校验过的记录规范化，供幂等比对与快照存储。

    时间已在模型层归一化为 UTC；数值 -0.0 归一化为 0.0；
    检测项目按键名排序，保证同一逻辑内容得到唯一字节表示。
    """
    return [
        {
            "id": r.id,
            "barcode": r.barcode,
            "timestamp": r.timestamp.isoformat(),
            "results": {
                item: (0.0 if value == 0 else value)
                for item, value in sorted(r.results.items())
            },
        }
        for r in records
    ]


def _error(status_code: int, error: str, **extra: Any) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ErrorOut(error=error, **extra).model_dump(exclude_none=True),
    )


router = APIRouter(prefix="/runs", tags=["运行批次"])

RunIdPath = Annotated[
    str,
    Path(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
        description="运行 ID（字母数字开头，可含 . _ -）",
        examples=["RUN-2026-0456"],
    ),
]

SegmentIndexPath = Annotated[
    int,
    Path(ge=0, le=MAX_SEGMENTS - 1, description="段号，从 0 起，至多 7"),
]


@router.put(
    "/{run_id}/segments/{segment_index}",
    response_model=SegmentAckOut,
    summary="提交一个分段（允许乱序，同内容重传幂等）",
    description=(
        "提交本次运行的一个分段。段号从 0 起、至多 "
        f"{MAX_SEGMENTS} 段，每段至多 12 条记录。\n\n"
        "* 各段提交的 `total_segments` 必须一致，否则 409；\n"
        "* 同段同内容重传幂等（`replay=true`，不改数据）；\n"
        "* 同段异内容重传、跨段重复记录 ID => 409，已收数据不变；\n"
        "* 运行已发布 => 409。"
    ),
    responses={
        409: {"model": ErrorOut, "description": "与已收数据冲突或运行已发布"},
        422: {"description": "请求体校验失败（含段号超出总段数）"},
    },
)
def submit_segment(
    run_id: RunIdPath,
    segment_index: SegmentIndexPath,
    body: SegmentSubmission = Body(openapi_examples=RUN_SEGMENT_EXAMPLES),
    store: RunStore = Depends(get_store),
) -> SegmentAckOut | JSONResponse:
    if segment_index >= body.total_segments:
        return _error(
            422,
            "segment_index_out_of_range",
            run_id=run_id,
            established_total_segments=body.total_segments,
        )
    try:
        view = store.submit_segment(
            run_id,
            segment_index,
            body.total_segments,
            _canonical_records(body.records),
        )
    except RunAlreadyPublished:
        return _error(409, "run_already_published", run_id=run_id)
    except TotalSegmentsMismatch as exc:
        return _error(
            409,
            "total_segments_mismatch",
            run_id=run_id,
            established_total_segments=exc.established,
        )
    except SegmentConflict:
        return _error(409, "segment_conflict", run_id=run_id)
    except DuplicateRecordId as exc:
        return _error(
            409, "duplicate_record_id", run_id=run_id, duplicate_ids=exc.duplicates
        )
    return SegmentAckOut(
        run_id=run_id,
        segment_index=segment_index,
        total_segments=view["total_segments"],
        state="open",
        replay=view["replay"],
        received_segments=view["received_segments"],
        missing_segments=view["missing_segments"],
    )


@router.post(
    "/{run_id}/publish",
    response_model=SnapshotOut,
    summary="发布运行批次（齐段后一次性固定完整快照）",
    description=(
        "所有分段齐了之后，在单个事务内按段号固定完整快照并返回。\n\n"
        "* 缺段 => 409 并返回 `missing_segments`，运行保持开放可续传；\n"
        "* 发布成功后再重试 => 原样返回首次固定的快照（逐字节一致）；\n"
        "* 并发补传与发布互斥，不会暴露半成品。"
    ),
    responses={
        404: {"model": ErrorOut, "description": "运行批次不存在"},
        409: {"model": ErrorOut, "description": "仍有缺段，返回缺号"},
    },
)
def publish_run(
    run_id: RunIdPath,
    store: RunStore = Depends(get_store),
) -> dict[str, Any] | JSONResponse:
    try:
        return store.publish(run_id)
    except RunNotFound:
        return _error(404, "run_not_found", run_id=run_id)
    except RunIncomplete as exc:
        return _error(
            409, "run_incomplete", run_id=run_id, missing_segments=exc.missing
        )


@router.get(
    "/{run_id}",
    response_model=RunStatusOut,
    summary="查询运行批次状态（已发布时含原快照）",
    responses={404: {"model": ErrorOut, "description": "运行批次不存在"}},
)
def get_run(
    run_id: RunIdPath,
    store: RunStore = Depends(get_store),
) -> dict[str, Any] | JSONResponse:
    view = store.get_run(run_id)
    if view is None:
        return _error(404, "run_not_found", run_id=run_id)
    return view
