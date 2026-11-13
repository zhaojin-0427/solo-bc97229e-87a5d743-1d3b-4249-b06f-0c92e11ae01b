"""采集引擎：并发运行 SSE 实时接收与历史分页补采两个循环。

容错原则（对应需求）：
1. 启动即同时开 SSE 与历史补采；两者重叠记录靠 seq 主键去重，只入库一次。
2. SSE 不补发旧记录，断线重连期间错过的序号由历史补采循环补齐。
3. 历史翻页失败只记错误并重试，绝不影响实时接收。
4. 每次写入后尝试推进连续水位；只有 1..w 全部入库，水位才等于 w。
5. 所有进度（船位、水位、历史翻页游标、错误）都在 SQLite 中，重启自动接续。
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from typing import Any

import httpx

from .db import (
    ERROR_KIND_CLEANUP,
    ERROR_KIND_HISTORY,
    ERROR_KIND_PARSE,
    ERROR_KIND_SSE,
    Database,
)

STATION_URL = os.getenv("STATION_URL", "http://station:8000")
HISTORY_PAGE_SIZE = int(os.getenv("HISTORY_PAGE_SIZE", "50"))
HISTORY_RETRY_BASE = float(os.getenv("HISTORY_RETRY_BASE", "1.0"))
HISTORY_RETRY_MAX = float(os.getenv("HISTORY_RETRY_MAX", "30"))
SSE_RECONNECT_BASE = float(os.getenv("SSE_RECONNECT_BASE", "0.5"))
SSE_RECONNECT_MAX = float(os.getenv("SSE_RECONNECT_MAX", "30"))
SSE_READ_TIMEOUT = float(os.getenv("SSE_READ_TIMEOUT", "60"))
STATION_CATCHUP_IDLE = float(os.getenv("STATION_CATCHUP_IDLE", "0.5"))
# 后台清理每批物理删除的行数与空闲轮询间隔
CLEANUP_BATCH_SIZE = int(os.getenv("CLEANUP_BATCH_SIZE", "100"))
CLEANUP_IDLE_INTERVAL = float(os.getenv("CLEANUP_IDLE_INTERVAL", "1.0"))
# 故障演练开关：>0 时后台清理每批按该概率主动失败（默认关闭）
CLEANUP_FAIL_RATE = float(os.getenv("CLEANUP_FAIL_RATE", "0.0"))

META_HISTORY_CURSOR = "history_cursor"


def _validate_position(raw: dict[str, Any]) -> dict[str, Any]:
    required = {"seq", "mmsi", "utc_ts", "latitude", "longitude"}
    missing = required - raw.keys()
    if missing:
        raise ValueError(f"船位缺少字段: {sorted(missing)}")
    seq = raw["seq"]
    if not isinstance(seq, int) or seq < 1:
        raise ValueError(f"非法 seq: {seq!r}")
    lat = float(raw["latitude"])
    lon = float(raw["longitude"])
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError(f"经纬度越界: lat={lat}, lon={lon}")
    return {
        "seq": seq,
        "mmsi": str(raw["mmsi"]),
        "utc_ts": float(raw["utc_ts"]),
        "latitude": lat,
        "longitude": lon,
    }


class IngestEngine:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        # 仅用于状态展示的内存计数（权威计数以数据库为准）
        self.sse_connected = False
        self.sse_reconnects = 0
        self.sse_disconnects = 0
        self.sse_received = 0
        self.history_received = 0
        self.history_requests = 0
        self.history_failures = 0
        self.last_event_at: float | None = None
        self.last_history_at: float | None = None
        self.last_backfill_seq: int | None = None
        # 任一来源见过的最大序号，用于界定真实缺口的上界
        self.max_seen_seq = 0
        self._backfill_open_error: int | None = None
        self._sse_open_error: int | None = None
        self._cleanup_open_error: int | None = None
        # 留存边界缓存：已受理的最大清理截止序号（权威值在 SQLite meta 中）。
        # 已清理记录在内存层即被拦截，SQL 条件插入是兜底硬防护。
        self.cleanup_boundary = 0
        # 当前归档代际：与库内 epoch 一致时后台清理才允许动手
        self.archive_epoch = 0
        # 后台正在等待物理删除的行数（仅状态展示，权威计数以快照查询为准）
        self.cleanup_pending = 0

    async def refresh_cleanup_boundary(self) -> int:
        """从数据库重新加载留存边界与代际（启动/受理/重置后调用）。"""
        self.cleanup_boundary = await self.db.get_cleanup_boundary()
        self.archive_epoch = await self.db.get_archive_epoch()
        return self.cleanup_boundary

    async def reconcile_cleanup_errors(self) -> None:
        """启动/恢复时对账清理错误：若边界下已无待删明细，
        历史未恢复的清理错误视为随恢复完成而关闭（重启后续清成功的场景）。"""
        async with self.db.snapshot() as snap:
            boundary = await snap.get_cleanup_boundary()
            pending = await snap.count_pending_cleanup(boundary)
            rows = await snap.list_errors(50, False, ERROR_KIND_CLEANUP)
        if pending == 0:
            for row in rows:
                await self.db.resolve_error(row["id"])

    # ----------------------------- 生命周期 -----------------------------

    async def start(self) -> None:
        # 重启恢复：以库内最大序号作为缺口上界的初值
        cur = await self.db.conn.execute("SELECT COALESCE(MAX(seq), 0) m FROM positions")
        row = await cur.fetchone()
        self.max_seen_seq = row["m"]
        # 重启恢复：边界/代际/待清状态都在 SQLite，后台清理自动续清
        await self.refresh_cleanup_boundary()
        await self.reconcile_cleanup_errors()
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._sse_loop(), name="sse-loop"),
            asyncio.create_task(self._history_loop(), name="history-loop"),
            asyncio.create_task(self._cleanup_loop(), name="cleanup-loop"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    # ----------------------------- 公共写入路径 -----------------------------

    async def _ingest(self, raw: dict[str, Any], source: str) -> bool:
        try:
            pos = _validate_position(raw)
        except (ValueError, TypeError, KeyError) as exc:
            await self.db.add_error(ERROR_KIND_PARSE, f"{source} 记录解析失败: {exc}")
            return False
        # 留存边界内存拦截：在途历史页、实时帧、回退补采送达的已清理序号
        # 一律不重新入库（SQL 条件插入还会再兜底一次）
        if pos["seq"] <= self.cleanup_boundary:
            return False
        inserted = await self.db.upsert_position(
            pos, source, self.cleanup_boundary
        )
        # SSE 侧代际检测：已入库的最大序号远大于当前序号（回退超过一个补采页），
        # 且当前序号很小，判定岸站重置；回退历史游标重拉新一代际（去重仍安全）。
        # 已清理序号造成的“回退”不算代际切换，避免误判。
        if (
            source == "sse"
            and not inserted
            and pos["seq"] > self.cleanup_boundary
            and self.max_seen_seq - pos["seq"] > HISTORY_PAGE_SIZE
        ):
            cursor_now = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
            if cursor_now > pos["seq"]:
                await self.db.set_meta(META_HISTORY_CURSOR, "0")
                eid = await self.db.add_error(
                    ERROR_KIND_SSE,
                    f"SSE 收到回退序号 seq={pos['seq']}（已见最大 "
                    f"{self.max_seen_seq}，历史游标 {cursor_now}），"
                    "判定岸站重置，已回退补采游标",
                )
                await self.db.resolve_error(eid)
        if pos["seq"] > self.max_seen_seq:
            self.max_seen_seq = pos["seq"]
        if inserted:
            if source == "sse":
                self.sse_received += 1
            else:
                self.history_received += 1
            self.last_event_at = time.time()
        # 无论新老记录都尝试推进水位（新记录可能恰好补上缺口）
        await self.db.advance_watermark()
        return inserted

    # ----------------------------- SSE 实时循环 -----------------------------

    async def _sse_loop(self) -> None:
        backoff = SSE_RECONNECT_BASE
        while not self._stop.is_set():
            self.sse_connected = False
            # 每次循环代表一次连接尝试（含首次连接），先计数再握手
            self.sse_reconnects += 1
            try:
                timeout = httpx.Timeout(connect=10, read=SSE_READ_TIMEOUT,
                                        write=10, pool=10)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "GET", f"{STATION_URL}/stream"
                    ) as resp:
                        if resp.status_code != 200:
                            body = await resp.aread()
                            raise httpx.HTTPStatusError(
                                f"SSE 握手失败 HTTP {resp.status_code}: "
                                f"{body[:200]!r}",
                                request=resp.request,
                                response=resp,
                            )
                        # 连接成功：退避与未恢复错误归零
                        backoff = SSE_RECONNECT_BASE
                        self.sse_connected = True
                        # 首次成功连接也算一次 reconnect（=1 表示已连上）
                        if self._sse_open_error is not None:
                            await self.db.resolve_error(self._sse_open_error)
                            self._sse_open_error = None
                        async for raw_line in resp.aiter_lines():
                            if self._stop.is_set():
                                break
                            if not raw_line or not raw_line.startswith("data:"):
                                # 注释心跳、event: 行、空行均忽略
                                continue
                            payload = raw_line[len("data:"):].strip()
                            if payload == "stream interrupted":
                                raise ConnectionError("岸站主动中断实时流")
                            try:
                                obj = json.loads(payload)
                            except json.JSONDecodeError as exc:
                                await self.db.add_error(
                                    ERROR_KIND_PARSE,
                                    f"SSE 数据非合法 JSON: {exc}",
                                )
                                continue
                            await self._ingest(obj, "sse")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # 断流/超时/连接拒绝都走这里：记录一次未恢复错误，退避重连
                self.sse_connected = False
                self.sse_disconnects += 1
                if self._sse_open_error is None:
                    self._sse_open_error = await self.db.add_error(
                        ERROR_KIND_SSE, f"实时流中断: {type(exc).__name__}: {exc}"
                    )
            if self._stop.is_set():
                break
            # 指数退避 + 抖动，避免重连风暴
            delay = min(backoff, SSE_RECONNECT_MAX) * (
                0.5 + random.random() * 0.5
            )
            backoff = min(backoff * 2, SSE_RECONNECT_MAX)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # ----------------------------- 历史补采循环 -----------------------------

    async def _history_loop(self) -> None:
        backoff = HISTORY_RETRY_BASE
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15)
        ) as client:
            while not self._stop.is_set():
                cursor = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
                try:
                    self.history_requests += 1
                    resp = await client.get(
                        f"{STATION_URL}/history",
                        params={"cursor": cursor, "limit": HISTORY_PAGE_SIZE},
                    )
                    resp.raise_for_status()
                    page = resp.json()
                except (httpx.HTTPError, json.JSONDecodeError, KeyError) as exc:
                    # 翻页失败：持久化错误并退避重试，实时循环不受任何影响
                    self.history_failures += 1
                    if self._backfill_open_error is None:
                        self._backfill_open_error = await self.db.add_error(
                            ERROR_KIND_HISTORY,
                            f"历史翻页失败 cursor={cursor}: "
                            f"{type(exc).__name__}: {exc}",
                        )
                    delay = min(backoff, HISTORY_RETRY_MAX) * (
                        0.5 + random.random() * 0.5
                    )
                    backoff = min(backoff * 2, HISTORY_RETRY_MAX)
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
                    continue

                # 成功拿到一页：恢复退避与错误状态
                backoff = HISTORY_RETRY_BASE
                if self._backfill_open_error is not None:
                    await self.db.resolve_error(self._backfill_open_error)
                    self._backfill_open_error = None

                records = page.get("records", [])

                # 岸站代际检测（空页路径）：游标很高却拿不到任何记录，
                # 探测第 1 页判断岸站是否已重置（新代际 seq 从 1 重新开始）
                if not records and cursor > 0:
                    try:
                        probe = await client.get(
                            f"{STATION_URL}/history",
                            params={"cursor": 0, "limit": 1},
                        )
                        probe.raise_for_status()
                        probe_page = probe.json()
                        probe_records = probe_page.get("records", [])
                        if probe_records and probe_records[0]["seq"] <= cursor:
                            await self.db.set_meta(META_HISTORY_CURSOR, "0")
                            # 这是可自愈的代际切换告警，直接标记已解决
                            eid = await self.db.add_error(
                                ERROR_KIND_HISTORY,
                                f"检测到岸站序号重置(探测 seq="
                                f"{probe_records[0]['seq']} <= cursor={cursor})，"
                                "已自动回退补采游标重新补采",
                            )
                            await self.db.resolve_error(eid)
                            continue
                    except (httpx.HTTPError, json.JSONDecodeError, KeyError):
                        # 探测失败不影响主流程，下轮常规重试
                        pass

                # 岸站代际检测（非空页路径）：页首 seq 回退到游标之前
                if records and cursor > 0 and records[0]["seq"] <= cursor:
                    await self.db.set_meta(META_HISTORY_CURSOR, "0")
                    eid = await self.db.add_error(
                        ERROR_KIND_HISTORY,
                        f"检测到岸站序号重置(页首 seq={records[0]['seq']} "
                        f"<= cursor={cursor})，已自动回退补采游标",
                    )
                    await self.db.resolve_error(eid)
                    continue

                for raw in records:
                    await self._ingest(raw, "history")
                self.last_history_at = time.time()

                if records:
                    self.last_backfill_seq = records[-1]["seq"]
                    # 以本页最后一条 seq 作为下次请求游标，持久化以便重启接续
                    await self.db.set_meta(
                        META_HISTORY_CURSOR, str(records[-1]["seq"])
                    )

                if not page.get("has_more"):
                    # 已补到岸站当前最新；短暂空闲后再轮询新产生的船位
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=STATION_CATCHUP_IDLE
                        )
                    except asyncio.TimeoutError:
                        pass
                else:
                    # 还有历史页，尽量连续拉取但让出事件循环
                    await asyncio.sleep(0)

    # ----------------------------- 后台分批清理循环 -----------------------------

    async def _cleanup_loop(self) -> None:
        """按留存边界分批物理删除明细，期间采集与查询持续可用。

        - 边界与待清理状态均持久化在 SQLite；失败/重启后本循环自动续清。
        - 每批删除同时校验归档代际与当前边界：发生过清空重置（epoch 变化）
          或边界回退时一条都不会删，旧清理不可能删除新归档。
        - 逻辑排除在受理时即刻生效，物理删除只是回收空间，可慢可中断。
        """
        backoff = 1.0
        while not self._stop.is_set():
            try:
                batch = await self.db.next_cleanup_batch(CLEANUP_BATCH_SIZE)
                if batch is None:
                    # 边界下已无物理行：清理处于健康状态，关闭历史未恢复错误，
                    # 把受理申请标记完成，随后空闲轮询
                    if self._cleanup_open_error is not None:
                        await self.db.resolve_error(self._cleanup_open_error)
                        self._cleanup_open_error = None
                    await self.db.mark_cleanup_requests_done()
                    self.cleanup_pending = 0
                    backoff = 1.0
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=CLEANUP_IDLE_INTERVAL
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue
                epoch, boundary, batch_max_seq = batch
                if epoch != self.archive_epoch:
                    # 库已进入新代际（重置后新归档）：同步代际并停手本批
                    self.archive_epoch = epoch
                    continue
                if CLEANUP_FAIL_RATE > 0 and random.random() < CLEANUP_FAIL_RATE:
                    raise RuntimeError(
                        f"注入的清理批次故障 (fail_rate={CLEANUP_FAIL_RATE})"
                    )
                deleted = await self.db.delete_cleanup_batch(
                    epoch, batch_max_seq
                )
                if self._cleanup_open_error is not None:
                    await self.db.resolve_error(self._cleanup_open_error)
                    self._cleanup_open_error = None
                backoff = 1.0
                # 待删量按本批实际删除量递减（仅展示用途）
                self.cleanup_pending = max(0, self.cleanup_pending - deleted)
                # 每批之间让出事件循环，避免长时间占用写连接
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._cleanup_open_error is None:
                    self._cleanup_open_error = await self.db.add_error(
                        ERROR_KIND_CLEANUP,
                        f"后台清理批次失败: {type(exc).__name__}: {exc}",
                    )
                delay = min(backoff, 30.0) * (0.5 + random.random() * 0.5)
                backoff = min(backoff * 2, 30.0)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
