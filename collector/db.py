"""SQLite 持久化层。

关键设计：
- positions 以岸站全局序号 seq 为主键，INSERT OR IGNORE 天然去重，
  SSE 与历史补采重叠的记录只入库一次。
- 连续进度 contiguous_seq 持久化在 meta 表：只有 seq=1..w 全部存在时
  水位才允许推进到 w；记录只增不改，水位单调不减，重启后自动恢复。
- 留存边界 cleanup_boundary（已受理的最大清理截止序号）持久化且单调不减：
  序号不大于边界的记录对所有查询立即不可见（逻辑排除），物理行由后台分批删除。
- 入库语句带边界条件，已清理序号即使被实时帧/历史补采再次送到也不会重新入库。
- archive_epoch 随每次清空重置递增：重置前已在跑的旧清理任务因代际不匹配
  自动停手，旧清理永远不会删除新归档的记录。
- 读连接池中的每个连接以 BEGIN/COMMIT 构成快照事务，单次查询内的边界、
  明细与计数必然来自同一个状态；WAL 保证读快照与采集写入互不阻塞。
- ingest_errors 记录断流、翻页失败、清理失败等错误，恢复后写 resolved_at。
- cleanup_requests 留痕每次清理申请（受理/驳回、边界、完成时间）。
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    seq        INTEGER PRIMARY KEY,
    mmsi       TEXT    NOT NULL,
    utc_ts     REAL    NOT NULL,
    latitude   REAL    NOT NULL,
    longitude  REAL    NOT NULL,
    source     TEXT    NOT NULL CHECK (source IN ('sse', 'history')),
    received_at REAL   NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_mmsi_seq ON positions(mmsi, seq);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    message     TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    resolved_at REAL
);
CREATE INDEX IF NOT EXISTS idx_errors_occurred ON ingest_errors(occurred_at);

CREATE TABLE IF NOT EXISTS cleanup_requests (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    cutoff_seq     INTEGER NOT NULL,
    accepted       INTEGER NOT NULL,
    boundary_after INTEGER,
    reason         TEXT,
    occurred_at    REAL NOT NULL,
    completed_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_cleanup_occurred ON cleanup_requests(occurred_at);
"""

ERROR_KIND_SSE = "sse"
ERROR_KIND_HISTORY = "history_page"
ERROR_KIND_PARSE = "parse"
ERROR_KIND_CLEANUP = "cleanup"

META_WATERMARK = "contiguous_seq"
META_CURSOR = "history_cursor"
META_BOUNDARY = "cleanup_boundary"
META_EPOCH = "archive_epoch"

_READ_POOL_SIZE = int(os.getenv("READ_POOL_SIZE", "4"))


def _default_meta() -> dict[str, str]:
    return {
        META_WATERMARK: "0",
        META_CURSOR: "0",
        META_BOUNDARY: "0",
        META_EPOCH: "0",
    }


class ReadSession:
    """单个只读连接上的快照会话（外层已 BEGIN）。所有读方法共用同一快照。"""

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self.conn = conn

    async def _meta_int(self, key: str) -> int:
        cur = await self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        )
        row = await cur.fetchone()
        return int(row["value"]) if row else 0

    async def get_watermark(self) -> int:
        return await self._meta_int(META_WATERMARK)

    async def get_cleanup_boundary(self) -> int:
        return await self._meta_int(META_BOUNDARY)

    async def get_archive_epoch(self) -> int:
        return await self._meta_int(META_EPOCH)

    async def count_positions(self, boundary: int) -> int:
        cur = await self.conn.execute(
            "SELECT COUNT(*) c FROM positions WHERE seq > ?", (boundary,)
        )
        return (await cur.fetchone())["c"]

    async def count_pending_cleanup(self, boundary: int) -> int:
        """物理上仍在库、但序号已不大于留存边界的待删行数。"""
        cur = await self.conn.execute(
            "SELECT COUNT(*) c FROM positions WHERE seq <= ?", (boundary,)
        )
        return (await cur.fetchone())["c"]

    async def count_mmsi(self, boundary: int) -> int:
        cur = await self.conn.execute(
            "SELECT COUNT(DISTINCT mmsi) c FROM positions WHERE seq > ?",
            (boundary,),
        )
        return (await cur.fetchone())["c"]

    async def first_missing_after(
        self, watermark: int, upper_bound: int, probe: int = 100
    ) -> list[int]:
        """返回水位之后、upper_bound 以内真实缺失的 seq（最多 20 个）。"""
        upper = min(watermark + probe, upper_bound)
        if upper <= watermark:
            return []
        cur = await self.conn.execute(
            "SELECT seq FROM positions WHERE seq > ? AND seq <= ? ORDER BY seq",
            (watermark, upper),
        )
        present = {row["seq"] for row in await cur.fetchall()}
        return [
            s for s in range(watermark + 1, upper + 1) if s not in present
        ][:20]

    async def list_vessels(self, boundary: int) -> list[aiosqlite.Row]:
        cur = await self.conn.execute(
            "SELECT mmsi, COUNT(*) AS cnt, MIN(seq) AS min_seq, MAX(seq) AS max_seq "
            "FROM positions WHERE seq > ? GROUP BY mmsi ORDER BY mmsi",
            (boundary,),
        )
        return await cur.fetchall()

    async def trajectory(
        self, mmsi: str, watermark: int, boundary: int, limit: int
    ) -> tuple[list[aiosqlite.Row], int, int]:
        """只返回 boundary < seq <= 连续水位 的船位。

        - seq <= boundary：已清理（或待清理），一律不可见；
        - seq > 连续水位：缺口之后的记录，一律不可见。
        返回 (可见行[按 seq 倒序限量], 该 MMSI 留存总数, 水位内可见总数)。
        """
        cur = await self.conn.execute(
            "SELECT seq, mmsi, utc_ts, latitude, longitude, source, received_at "
            "FROM positions WHERE mmsi = ? AND seq > ? AND seq <= ? "
            "ORDER BY seq DESC LIMIT ?",
            (mmsi, boundary, watermark, limit),
        )
        rows = await cur.fetchall()
        cur2 = await self.conn.execute(
            "SELECT COUNT(*) c FROM positions WHERE mmsi = ? AND seq > ?",
            (mmsi, boundary),
        )
        total = (await cur2.fetchone())["c"]
        cur3 = await self.conn.execute(
            "SELECT COUNT(*) c FROM positions "
            "WHERE mmsi = ? AND seq > ? AND seq <= ?",
            (mmsi, boundary, watermark),
        )
        visible = (await cur3.fetchone())["c"]
        return rows, total, visible

    async def count_open_errors(self) -> int:
        cur = await self.conn.execute(
            "SELECT COUNT(*) c FROM ingest_errors WHERE resolved_at IS NULL"
        )
        return (await cur.fetchone())["c"]

    async def list_errors(
        self, limit: int, include_resolved: bool, kind: str | None = None
    ) -> list[aiosqlite.Row]:
        sql = "SELECT * FROM ingest_errors WHERE 1=1"
        params: list[Any] = []
        if not include_resolved:
            sql += " AND resolved_at IS NULL"
        if kind is not None:
            sql += " AND kind = ?"
            params.append(kind)
        sql += " ORDER BY occurred_at DESC LIMIT ?"
        params.append(limit)
        cur = await self.conn.execute(sql, params)
        return await cur.fetchall()

    async def list_cleanup_requests(self, limit: int) -> list[aiosqlite.Row]:
        cur = await self.conn.execute(
            "SELECT * FROM cleanup_requests ORDER BY occurred_at DESC LIMIT ?",
            (limit,),
        )
        return await cur.fetchall()


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        self._read_pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue()
        # 多语句写事务串行化，保证并发清理/重置/后台删除之间状态严格一致
        self._wlock = asyncio.Lock()

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        # WAL 允许采集写入与轨迹查询并发；busy_timeout 防偶发锁冲突
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(SCHEMA)
        for key, value in _default_meta().items():
            await self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)",
                (key, value),
            )
        await self._conn.commit()
        # 只读连接池：各自独立快照，互不阻塞写连接
        for _ in range(_READ_POOL_SIZE):
            rc = await aiosqlite.connect(f"file:{self.path}?mode=ro", uri=True)
            rc.row_factory = aiosqlite.Row
            await rc.execute("PRAGMA busy_timeout=5000")
            await self._read_pool.put(rc)

    async def close(self) -> None:
        while not self._read_pool.empty():
            rc = self._read_pool.get_nowait()
            await rc.close()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "Database 尚未 connect"
        return self._conn

    @asynccontextmanager
    async def snapshot(self):
        """借出一个只读连接并开启延迟事务，得到同一状态下的一致性快照。"""
        rc = await self._read_pool.get()
        try:
            await rc.execute("BEGIN")
            try:
                yield ReadSession(rc)
                await rc.execute("COMMIT")
            except Exception:
                await rc.execute("ROLLBACK")
                raise
        finally:
            await self._read_pool.put(rc)

    # ----------------------------- meta（写连接） -----------------------------

    async def get_meta(self, key: str, default: str | None = None) -> str | None:
        cur = await self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        )
        row = await cur.fetchone()
        return row["value"] if row else default

    async def set_meta(self, key: str, value: str) -> None:
        await self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await self.conn.commit()

    # ----------------------------- 船位写入 -----------------------------

    async def upsert_position(
        self, pos: dict[str, Any], source: str, boundary: int
    ) -> bool:
        """插入一条船位；seq 已存在或 seq 不大于留存边界时忽略。

        边界条件直接写在 SQL 里（与留存边界同一语句判定），
        已清理记录即使被在途历史页、实时帧或回退补采再次送达也不会回流。
        返回是否为新记录。
        """
        cur = await self.conn.execute(
            "INSERT OR IGNORE INTO positions"
            "(seq, mmsi, utc_ts, latitude, longitude, source, received_at) "
            "SELECT ?, ?, ?, ?, ?, ?, ? "
            "WHERE ? > (SELECT CAST(value AS INTEGER) FROM meta "
            "           WHERE key = ?)",
            (
                pos["seq"],
                pos["mmsi"],
                pos["utc_ts"],
                pos["latitude"],
                pos["longitude"],
                source,
                time.time(),
                pos["seq"],
                META_BOUNDARY,
            ),
        )
        await self.conn.commit()
        return cur.rowcount > 0

    async def seq_exists(self, seq: int) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM positions WHERE seq = ?", (seq,)
        )
        return await cur.fetchone() is not None

    async def advance_watermark(self) -> int:
        """从当前连续水位向后推进，遇到第一个缺口即停。返回最新水位。"""
        w = int(await self.get_meta(META_WATERMARK, "0"))
        # 每个序号最多触发一次主键查询；有缺口时每次写入仅一次探测，代价可忽略
        while await self.seq_exists(w + 1):
            w += 1
        if w != int(await self.get_meta(META_WATERMARK, "0")):
            await self.set_meta(META_WATERMARK, str(w))
        return w

    async def get_watermark(self) -> int:
        return int(await self.get_meta(META_WATERMARK, "0"))

    async def get_cleanup_boundary(self) -> int:
        return int(await self.get_meta(META_BOUNDARY, "0"))

    async def get_archive_epoch(self) -> int:
        return int(await self.get_meta(META_EPOCH, "0"))

    # ----------------------------- 清理受理与后台删除 -----------------------------

    async def accept_cleanup(self, cutoff_seq: int) -> dict[str, Any]:
        """受理一次按截止序号的清理申请。

        - cutoff_seq 不得超过当前连续进度，否则驳回（边界不变）；
        - 受理后留存边界取 max(旧边界, cutoff)，只增不减；
        - 边界与申请记录在同一串行写事务中持久化，并发/重复申请不会回退边界。
        """
        async with self._wlock:
            watermark = int(await self.get_meta(META_WATERMARK, "0"))
            boundary = int(await self.get_meta(META_BOUNDARY, "0"))
            now = time.time()
            if cutoff_seq > watermark:
                cur = await self.conn.execute(
                    "INSERT INTO cleanup_requests"
                    "(cutoff_seq, accepted, boundary_after, reason, occurred_at) "
                    "VALUES(?, 0, ?, ?, ?)",
                    (
                        cutoff_seq,
                        boundary,
                        f"cutoff_seq={cutoff_seq} 超过连续进度 {watermark}",
                        now,
                    ),
                )
                await self.conn.commit()
                return {
                    "request_id": cur.lastrowid,
                    "accepted": False,
                    "cutoff_seq": cutoff_seq,
                    "cleanup_boundary": boundary,
                    "contiguous_seq": watermark,
                    "reason": "cutoff_exceeds_contiguous_seq",
                }
            new_boundary = max(boundary, cutoff_seq)
            if new_boundary != boundary:
                await self.set_meta(META_BOUNDARY, str(new_boundary))
            cur = await self.conn.execute(
                "INSERT INTO cleanup_requests"
                "(cutoff_seq, accepted, boundary_after, reason, occurred_at) "
                "VALUES(?, 1, ?, NULL, ?)",
                (cutoff_seq, new_boundary, now),
            )
            await self.conn.commit()
            return {
                "request_id": cur.lastrowid,
                "accepted": True,
                "cutoff_seq": cutoff_seq,
                "cleanup_boundary": new_boundary,
                "contiguous_seq": watermark,
                "reason": None,
            }

    async def next_cleanup_batch(
        self, batch_size: int
    ) -> tuple[int, int, int] | None:
        """取下一批待删明细：返回 (代际, 当前边界, 本批最大 seq)；无待删返回 None。"""
        async with self._wlock:
            epoch = int(await self.get_meta(META_EPOCH, "0"))
            boundary = int(await self.get_meta(META_BOUNDARY, "0"))
            cur = await self.conn.execute(
                "SELECT MAX(seq) AS m FROM "
                "(SELECT seq FROM positions WHERE seq <= ? LIMIT ?)",
                (boundary, batch_size),
            )
            row = await cur.fetchone()
            if row["m"] is None:
                return None
            return epoch, boundary, row["m"]

    async def delete_cleanup_batch(
        self, epoch: int, batch_max_seq: int
    ) -> int:
        """删除一批已清理明细。

        单条原子语句同时校验代际与当前边界：
        代际变化（发生过清空重置）或边界回退时一行都不会删。
        """
        async with self._wlock:
            cur = await self.conn.execute(
                "DELETE FROM positions WHERE seq <= ? "
                "AND seq <= (SELECT CAST(value AS INTEGER) FROM meta "
                "            WHERE key = ?) "
                "AND ? = (SELECT CAST(value AS INTEGER) FROM meta "
                "         WHERE key = ?)",
                (batch_max_seq, META_BOUNDARY, epoch, META_EPOCH),
            )
            await self.conn.commit()
            return cur.rowcount

    async def mark_cleanup_requests_done(self) -> None:
        """边界以下已无物理行时，把所有未完成的受理申请标记完成。"""
        async with self._wlock:
            await self.conn.execute(
                "UPDATE cleanup_requests SET completed_at = ? "
                "WHERE accepted = 1 AND completed_at IS NULL",
                (time.time(),),
            )
            await self.conn.commit()

    # ----------------------------- 错误记录 -----------------------------

    async def add_error(self, kind: str, message: str) -> int:
        cur = await self.conn.execute(
            "INSERT INTO ingest_errors(kind, message, occurred_at) VALUES(?, ?, ?)",
            (kind, message[:2000], time.time()),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def resolve_error(self, error_id: int) -> None:
        await self.conn.execute(
            "UPDATE ingest_errors SET resolved_at = ? WHERE id = ? AND resolved_at IS NULL",
            (time.time(), error_id),
        )
        await self.conn.commit()

    async def reset_all(self) -> None:
        """清空归档并进入下一个代际。

        边界、水位、游标全部归零，archive_epoch 递增；
        正在运行的旧清理任务凭旧代际自动停手，不会删除新归档。
        """
        async with self._wlock:
            old_epoch = int(await self.get_meta(META_EPOCH, "0"))
            await self.conn.execute("DELETE FROM positions")
            await self.conn.execute("DELETE FROM ingest_errors")
            await self.conn.execute("DELETE FROM cleanup_requests")
            await self.conn.execute("DELETE FROM meta")
            defaults = _default_meta()
            defaults[META_EPOCH] = str(old_epoch + 1)
            for key, value in defaults.items():
                await self.conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?)", (key, value)
                )
            await self.conn.commit()

    async def recompute_watermark(self) -> int:
        """从留存边界之后重算连续水位。

        故障注入演练删除记录后水位必须回退；已清理区间（seq <= 边界）
        不再属于留存轨迹，不能把水位拖回边界以下。
        """
        async with self._wlock:
            boundary = int(await self.get_meta(META_BOUNDARY, "0"))
            w = boundary
            while await self.seq_exists(w + 1):
                w += 1
            await self.set_meta(META_WATERMARK, str(w))
            return w

    async def delete_positions(self, seqs: list[int]) -> int:
        if not seqs:
            return 0
        placeholders = ",".join("?" for _ in seqs)
        async with self._wlock:
            cur = await self.conn.execute(
                f"DELETE FROM positions WHERE seq IN ({placeholders})", seqs
            )
            await self.conn.commit()
            return cur.rowcount
