"""采集归档服务 API。

- GET  /health                       健康检查
- GET  /status                       已接收数、连续进度、缺口、留存边界与运行指标
- GET  /errors                       错误查询（默认只看未恢复，可按 kind 过滤）
- GET  /vessels                      已接收船舶清单（已清理序号不计入）
- GET  /trajectories/{mmsi}         按 MMSI 读取连续轨迹（注明已清理范围）
- POST /admin/cleanup                按全局截止序号清理（不得超过连续进度）
- GET  /cleanup/status               留存边界、待删数量、清理申请与错误
- POST /admin/reset                  清空归档并重置进度（进入新代际，演示用）
- POST /admin/inject-gap             故障注入：删除指定序号，水位回退
- POST /admin/rewind-backfill        历史游标回退到水位，重新补采缺口区间
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .db import ERROR_KIND_CLEANUP, Database
from .ingest import STATION_URL, IngestEngine

DB_PATH = os.getenv("DB_PATH", "/data/collector.db")


# ----------------------------- 响应模型 -----------------------------

class PositionOut(BaseModel):
    seq: int
    mmsi: str
    utc_ts: float
    latitude: float
    longitude: float
    source: str
    received_at: float


class StatusOut(BaseModel):
    received_total: int
    contiguous_seq: int
    complete: bool
    vessel_count: int
    open_error_count: int
    next_missing_seqs: list[int]
    cleanup_boundary: int
    cleanup_pending: int
    sse_connected: bool
    sse_reconnects: int
    sse_disconnects: int
    sse_received: int
    history_received: int
    history_requests: int
    history_failures: int
    last_event_at: float | None
    last_history_at: float | None
    last_backfill_seq: int | None
    max_seen_seq: int
    history_cursor: int
    station_url: str


class ErrorOut(BaseModel):
    id: int
    kind: str
    message: str
    occurred_at: float
    resolved_at: float | None


class VesselOut(BaseModel):
    mmsi: str
    received: int
    min_seq: int
    max_seq: int


class TrajectoryOut(BaseModel):
    mmsi: str
    contiguous_seq: int
    cleanup_boundary: int
    cleaned_up_to: int
    cleaned_note: str
    returned: int
    received_total_for_mmsi: int
    truncated_by_gap: bool
    limit: int
    positions: list[PositionOut]


class GapIn(BaseModel):
    # 删除这些序号的船位，模拟岸站缺报；删除后连续水位回退到缺口之前
    seqs: list[int]


class CleanupIn(BaseModel):
    # 清理截止序号：序号不大于该值的明细将被回收；不得超过当前连续进度
    cutoff_seq: int = Field(..., ge=1, description="清理截止全局序号（含），须 ≤ contiguous_seq")


class CleanupAcceptedOut(BaseModel):
    request_id: int
    accepted: bool
    cutoff_seq: int
    cleanup_boundary: int
    contiguous_seq: int
    reason: str | None = None
    pending_delete: int


class CleanupRequestOut(BaseModel):
    id: int
    cutoff_seq: int
    accepted: bool
    boundary_after: int | None
    reason: str | None
    occurred_at: float
    completed_at: float | None


class CleanupStatusOut(BaseModel):
    cleanup_boundary: int
    contiguous_seq: int
    pending_delete: int
    archive_epoch: int
    open_cleanup_error_count: int
    recent_requests: list[CleanupRequestOut]
    recent_errors: list[ErrorOut]


# ----------------------------- 应用装配 -----------------------------

db = Database(DB_PATH)
engine = IngestEngine(db)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await engine.start()
    yield
    await engine.stop()
    await db.close()


app = FastAPI(
    title="AIS 船位采集归档服务",
    description=(
        "连接岸站 SSE 实时流并从历史接口分页补采。"
        "序号全局连续，重叠去重；连续进度只在无缺口时推进，"
        "轨迹查询不返回缺口之后的记录；重启自动接续补齐。"
        "支持按全局序号回收明细：留存边界只增不减，受理即逻辑排除，"
        "后台分批物理删除且自动续清，已清理记录不会重新入库。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


def _row_to_position(row: Any) -> PositionOut:
    return PositionOut(
        seq=row["seq"],
        mmsi=row["mmsi"],
        utc_ts=row["utc_ts"],
        latitude=row["latitude"],
        longitude=row["longitude"],
        source=row["source"],
        received_at=row["received_at"],
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/status", response_model=StatusOut, summary="采集状态与连续进度")
async def status() -> StatusOut:
    # 边界、计数、水位必须来自同一快照，避免查询中途后台删除造成口径不一致
    async with db.snapshot() as snap:
        boundary = await snap.get_cleanup_boundary()
        watermark = await snap.get_watermark()
        total = await snap.count_positions(boundary)
        vessel_count = await snap.count_mmsi(boundary)
        pending = await snap.count_pending_cleanup(boundary)
        missing = await snap.first_missing_after(
            watermark, engine.max_seen_seq
        )
        open_errors = await snap.count_open_errors()
    return StatusOut(
        received_total=total,
        contiguous_seq=watermark,
        # 留存窗口内无缺口：留存下界为边界、上界为连续水位
        complete=watermark <= engine.max_seen_seq and not missing
        and total == max(0, watermark - boundary),
        vessel_count=vessel_count,
        open_error_count=open_errors,
        next_missing_seqs=missing,
        cleanup_boundary=boundary,
        cleanup_pending=pending,
        sse_connected=engine.sse_connected,
        sse_reconnects=engine.sse_reconnects,
        sse_disconnects=engine.sse_disconnects,
        sse_received=engine.sse_received,
        history_received=engine.history_received,
        history_requests=engine.history_requests,
        history_failures=engine.history_failures,
        last_event_at=engine.last_event_at,
        last_history_at=engine.last_history_at,
        last_backfill_seq=engine.last_backfill_seq,
        max_seen_seq=engine.max_seen_seq,
        history_cursor=int(await db.get_meta("history_cursor", "0")),
        station_url=STATION_URL,
    )


@app.get("/errors", response_model=list[ErrorOut], summary="错误查询")
async def errors(
    limit: int = Query(50, ge=1, le=500),
    include_resolved: bool = Query(False, description="是否包含已恢复错误"),
    kind: str | None = Query(None, description="按错误类型过滤，如 sse/history_page/cleanup"),
) -> list[ErrorOut]:
    async with db.snapshot() as snap:
        rows = await snap.list_errors(limit, include_resolved, kind)
        return [
            ErrorOut(
                id=r["id"],
                kind=r["kind"],
                message=r["message"],
                occurred_at=r["occurred_at"],
                resolved_at=r["resolved_at"],
            )
            for r in rows
        ]


@app.get("/vessels", response_model=list[VesselOut], summary="已接收船舶（排除已清理序号）")
async def vessels() -> list[VesselOut]:
    async with db.snapshot() as snap:
        boundary = await snap.get_cleanup_boundary()
        rows = await snap.list_vessels(boundary)
        return [
            VesselOut(
                mmsi=r["mmsi"],
                received=r["cnt"],
                min_seq=r["min_seq"],
                max_seq=r["max_seq"],
            )
            for r in rows
        ]


@app.get(
    "/trajectories/{mmsi}",
    response_model=TrajectoryOut,
    summary="按 MMSI 读取连续轨迹（注明已清理范围）",
)
async def trajectory(
    mmsi: str,
    limit: int = Query(500, ge=1, le=5000, description="返回最近 N 条（按 seq 倒序截断）"),
) -> TrajectoryOut:
    # 同一快照内读取边界、水位与轨迹明细/计数
    async with db.snapshot() as snap:
        boundary = await snap.get_cleanup_boundary()
        watermark = await snap.get_watermark()
        rows, total, visible = await snap.trajectory(mmsi, watermark, boundary, limit)
        if total == 0:
            raise HTTPException(
                status_code=404,
                detail=f"留存轨迹中未接收到 MMSI={mmsi} 的船位"
                + (f"（序号 ≤ {boundary} 的明细已清理）" if boundary else ""),
            )
        note = (
            f"序号 1..{boundary} 的明细已清理，轨迹仅包含 "
            f"{boundary + 1}..{watermark} 区间内记录"
            if boundary
            else "尚未发生清理，轨迹包含全部留存记录"
        )
        return TrajectoryOut(
            mmsi=mmsi,
            contiguous_seq=watermark,
            cleanup_boundary=boundary,
            cleaned_up_to=boundary,
            cleaned_note=note,
            returned=len(rows),
            received_total_for_mmsi=total,
            truncated_by_gap=total > visible,
            limit=limit,
            positions=[_row_to_position(r) for r in reversed(rows)],
        )


@app.post(
    "/admin/cleanup",
    response_model=CleanupAcceptedOut,
    summary="按全局截止序号清理明细（截止序号不得超过连续进度）",
)
async def admin_cleanup(body: CleanupIn) -> CleanupAcceptedOut:
    result = await db.accept_cleanup(body.cutoff_seq)
    # 刷新引擎缓存：受理后在途历史页/实时帧/回退补采立即被拦截
    await engine.refresh_cleanup_boundary()
    if not result["accepted"]:
        # 驳回不改变任何状态，返回 409 便于调用方区分
        raise HTTPException(
            status_code=409,
            detail={
                "reason": result["reason"],
                "cutoff_seq": result["cutoff_seq"],
                "contiguous_seq": result["contiguous_seq"],
                "cleanup_boundary": result["cleanup_boundary"],
            },
        )
    async with db.snapshot() as snap:
        pending = await snap.count_pending_cleanup(result["cleanup_boundary"])
    engine.cleanup_pending = pending
    return CleanupAcceptedOut(pending_delete=pending, **result)


@app.get(
    "/cleanup/status",
    response_model=CleanupStatusOut,
    summary="清理状态：留存边界、待删数量、申请记录与清理错误",
)
async def cleanup_status(
    limit: int = Query(20, ge=1, le=200, description="最近申请/错误条数"),
) -> CleanupStatusOut:
    async with db.snapshot() as snap:
        boundary = await snap.get_cleanup_boundary()
        watermark = await snap.get_watermark()
        epoch = await snap.get_archive_epoch()
        pending = await snap.count_pending_cleanup(boundary)
        req_rows = await snap.list_cleanup_requests(limit)
        err_rows = await snap.list_errors(limit, True, ERROR_KIND_CLEANUP)
        open_errs = [r for r in err_rows if r["resolved_at"] is None]
        return CleanupStatusOut(
            cleanup_boundary=boundary,
            contiguous_seq=watermark,
            pending_delete=pending,
            archive_epoch=epoch,
            open_cleanup_error_count=len(open_errs),
            recent_requests=[
                CleanupRequestOut(
                    id=r["id"],
                    cutoff_seq=r["cutoff_seq"],
                    accepted=bool(r["accepted"]),
                    boundary_after=r["boundary_after"],
                    reason=r["reason"],
                    occurred_at=r["occurred_at"],
                    completed_at=r["completed_at"],
                )
                for r in req_rows
            ],
            recent_errors=[
                ErrorOut(
                    id=r["id"],
                    kind=r["kind"],
                    message=r["message"],
                    occurred_at=r["occurred_at"],
                    resolved_at=r["resolved_at"],
                )
                for r in err_rows
            ],
        )


@app.post("/admin/reset", summary="清空归档并重置（进入新代际，演示用）")
async def admin_reset() -> dict[str, str]:
    await db.reset_all()
    # 重置内存计数与清理状态，引擎循环会以 cursor=0 重新补采
    engine.sse_reconnects = 0
    engine.sse_disconnects = 0
    engine.sse_received = 0
    engine.history_received = 0
    engine.history_requests = 0
    engine.history_failures = 0
    engine.last_event_at = None
    engine.last_history_at = None
    engine.last_backfill_seq = None
    engine.max_seen_seq = 0
    engine._backfill_open_error = None
    engine._sse_open_error = None
    engine._cleanup_open_error = None
    engine.cleanup_pending = 0
    # 边界归零、代际递增：旧清理任务凭旧代际自动停手
    await engine.refresh_cleanup_boundary()
    return {"status": "reset done", "archive_epoch": str(engine.archive_epoch)}


@app.post(
    "/admin/inject-gap",
    summary="故障注入：删除指定序号并回退连续水位（演示缺口语义）",
)
async def admin_inject_gap(body: GapIn) -> dict[str, Any]:
    seqs = sorted({int(s) for s in body.seqs if int(s) >= 1})
    if not seqs:
        raise HTTPException(status_code=422, detail="seqs 不能为空")
    deleted = await db.delete_positions(seqs)
    new_watermark = await db.recompute_watermark()
    return {
        "deleted": deleted,
        "requested": seqs,
        "contiguous_seq": new_watermark,
    }


@app.post(
    "/admin/rewind-backfill",
    summary="把历史补采游标回退到连续水位，重新补采缺口区间（演示补齐）",
)
async def admin_rewind_backfill() -> dict[str, Any]:
    watermark = await db.get_watermark()
    # 已清理序号不会因回退补采重新入库（边界拦截 + SQL 条件插入双重防护）
    await db.set_meta("history_cursor", str(watermark))
    engine.last_backfill_seq = None
    return {"history_cursor": watermark, "contiguous_seq": watermark}
