"""运行批次 API 的请求/响应模型。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_SEGMENTS = 8          # 每次运行至多 8 段
MAX_RECORDS_PER_SEGMENT = 12  # 每段至多 12 条记录
MAX_RESULTS_PER_RECORD = 32   # 每条记录至多 32 个检测项目


class RunRecordIn(BaseModel):
    """单条仪器检测记录。"""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        min_length=1,
        max_length=64,
        description="记录唯一 ID（同一运行内跨段唯一）",
        examples=["R-001"],
    )
    barcode: str = Field(
        min_length=1,
        max_length=128,
        description="容器条码",
        examples=["BC-100"],
    )
    timestamp: datetime = Field(
        description="UTC 时间，ISO 8601 格式，必须携带时区（推荐 Z 后缀）",
        examples=["2026-11-14T08:00:00Z"],
    )
    results: dict[str, float] = Field(
        min_length=1,
        max_length=MAX_RESULTS_PER_RECORD,
        description="检测项目 -> 数值（必须为有限数，拒绝 NaN/Infinity）",
        examples=[{"GLU": 5.2, "NA": 140.0}],
    )

    @field_validator("timestamp")
    @classmethod
    def _require_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("timestamp 必须携带时区信息（UTC 时间，建议以 Z 结尾）")
        return v.astimezone(timezone.utc)

    @field_validator("results")
    @classmethod
    def _require_finite_and_named(cls, v: dict[str, float]) -> dict[str, float]:
        import math

        for item, value in v.items():
            if not item.strip():
                raise ValueError("检测项目名不能为空")
            if not math.isfinite(value):
                raise ValueError(f"检测项目 {item!r} 的数值必须为有限数")
        return v


class SegmentSubmission(BaseModel):
    """分段提交体：总段数 + 本段记录。"""

    model_config = ConfigDict(extra="forbid")

    total_segments: int = Field(
        ge=1,
        le=MAX_SEGMENTS,
        description="本次运行的总段数（1-8），同一运行的各段提交必须一致",
        examples=[2],
    )
    records: list[RunRecordIn] = Field(
        default_factory=list,
        max_length=MAX_RECORDS_PER_SEGMENT,
        description="本段携带的检测记录，0-12 条",
    )

    @model_validator(mode="after")
    def _ids_unique_within_segment(self) -> "SegmentSubmission":
        ids = [r.id for r in self.records]
        if len(ids) != len(set(ids)):
            raise ValueError("records 内存在重复 ID")
        return self


class RunRecordOut(BaseModel):
    """快照中的单条检测记录（时间已归一化为 UTC）。"""

    id: str
    barcode: str
    timestamp: datetime
    results: dict[str, float]


class SegmentOut(BaseModel):
    segment_index: int
    records: list[RunRecordOut]


class SnapshotOut(BaseModel):
    """发布后固定的完整快照；发布重试原样返回。"""

    run_id: str
    state: Literal["published"]
    total_segments: int
    record_count: int
    published_at: datetime
    segments: list[SegmentOut] = Field(description="按段号升序排列的完整分段")


class SegmentAckOut(BaseModel):
    """分段提交应答。"""

    run_id: str
    segment_index: int
    total_segments: int
    state: Literal["open"]
    replay: bool = Field(
        description="true 表示同内容重传（幂等命中，未改动已收数据）"
    )
    received_segments: list[int] = Field(description="已收齐的段号（升序）")
    missing_segments: list[int] = Field(description="仍缺失的段号（升序）")


class RunStatusOut(BaseModel):
    """运行批次状态。"""

    run_id: str
    state: Literal["open", "published"]
    total_segments: int
    received_segments: list[int]
    missing_segments: list[int]
    received_record_count: int = Field(description="已收记录总数")
    published_at: datetime | None = None
    snapshot: SnapshotOut | None = Field(
        default=None, description="仅 state=published 时存在，内容与发布响应一致"
    )


class ErrorOut(BaseModel):
    """统一错误体。"""

    error: str
    run_id: str | None = None
    missing_segments: list[int] | None = None
    established_total_segments: int | None = None
    duplicate_ids: list[str] | None = None
