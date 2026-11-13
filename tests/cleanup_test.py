#!/usr/bin/env python3
"""清理/回收明细端到端演练（只通过 HTTP 驱动，另含一次真实杀进程重启）。

覆盖需求：
1. 截止序号超过连续进度 -> 驳回(409)，边界不变，驳回留痕
2. 受理后边界=已受理最大截止序号，轨迹/船舶/计数立即排除 seq<=边界，
   轨迹注明已清理范围；后台分批删除，待删数量最终归零、申请标记完成
3. 并发/重复清理（含更小截止序号）边界只增不减、绝不回退
4. 回退补采、实时帧、岸站重置后重新送达的已清理序号不会重新入库
5. 杀进程重启：边界与待清状态持久化，自动续清
6. 采集器清空重置进入新代际：旧清理不删除新归档，新归档可再次清理
7. 清理错误查询入口可用
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time

import httpx

FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL
    if not cond:
        FAIL += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def wait_until(desc: str, fn, timeout: float = 60.0, interval: float = 0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc} (last={last})")


def restart_collector(args) -> None:
    subprocess.run(["pkill", "-f", "[c]ollector.app:app"], capture_output=True)
    time.sleep(2)
    try:
        httpx.get(f"{args.collector}/health", timeout=2)
        raise AssertionError("采集器未被杀死")
    except httpx.HTTPError:
        pass
    env = {
        "STATION_URL": args.station,
        "DB_PATH": args.db,
        "CLEANUP_BATCH_SIZE": "20",
        "PATH": "/usr/bin:/bin",
    }
    if args.pylibs:
        env["PYTHONPATH"] = args.pylibs
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "collector.app:app",
            "--host", "127.0.0.1", "--port", "8001",
        ],
        env=env,
        stdout=open("/tmp/collector-cleanup-restart.log", "w"),
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    assert proc.pid > 0
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if httpx.get(f"{args.collector}/health", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(0.5)
    raise AssertionError("重启后采集器未恢复")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="http://127.0.0.1:8000")
    ap.add_argument("--collector", default="http://127.0.0.1:8001")
    ap.add_argument("--db", default="/tmp/colldata/collector.db")
    ap.add_argument("--pylibs", default="")
    args = ap.parse_args()
    st = httpx.Client(base_url=args.station, timeout=15)
    col = httpx.Client(base_url=args.collector, timeout=15)

    # ---------- 0. 两侧重置，干净开局（必须同时重置，否则岸站高序号会拖慢补采） ----------
    col.post("/admin/reset")
    st.post("/station/reset")
    target = st.get("/station/status").json()["total_positions"]
    wait_until(
        "初始补齐",
        lambda: (lambda s: s["complete"] and s["received_total"] >= target)
        (col.get("/status").json()),
        timeout=90,
    )
    # 再等水位积累到足够高度，保证后续清理截止序号之间有合理间隔
    wait_until(
        "水位积累",
        lambda: col.get("/status").json()["contiguous_seq"] >= 150,
        timeout=90,
    )
    s0 = col.get("/status").json()
    w0 = s0["contiguous_seq"]
    check("初始无清理边界", s0["cleanup_boundary"] == 0 and s0["cleanup_pending"] == 0)
    # 后续所有截止序号都基于实际水位动态计算，避免与采集速度耦合
    cutoff1 = max(30, w0 // 3)          # 第一批清理截止序号
    cutoff2 = max(cutoff1 + 20, (2 * w0) // 3)  # 并发清理推进到的更大边界

    # ---------- 1. 截止序号超过连续进度：驳回 ----------
    r = col.post("/admin/cleanup", json={"cutoff_seq": w0 + 5})
    check("超界清理被 409 驳回", r.status_code == 409, r.text)
    s = col.get("/status").json()
    check("驳回后边界保持 0", s["cleanup_boundary"] == 0)
    check("驳回后口径仍自洽（未排除任何序号）",
          s["received_total"] == s["contiguous_seq"] - s["cleanup_boundary"])
    cs = col.get("/cleanup/status").json()
    check("驳回申请留痕", any(
        q["cutoff_seq"] == w0 + 5 and q["accepted"] is False
        for q in cs["recent_requests"]
    ))

    # ---------- 2. 受理清理：立即排除 + 后台分批删除 ----------
    cutoff1 = 60
    r = col.post("/admin/cleanup", json={"cutoff_seq": cutoff1})
    check("合法清理受理", r.status_code == 200, r.text)
    body = r.json()
    check("受理返回边界等于截止序号", body["cleanup_boundary"] == cutoff1)
    # 受理即生效：计数立刻排除（不等待物理删除）。
    # 注意系统持续采集，受理与下一次状态查询之间可能有新船位，
    # 因此以“留存数 = 水位 - 边界”的同口径恒等式校验，而非比较两次快照。
    s = col.get("/status").json()
    check("受理后边界立即生效", s["cleanup_boundary"] == cutoff1)
    check("接收计数立即排除已清理序号",
          s["received_total"] == s["contiguous_seq"] - cutoff1,
          f"total={s['received_total']} w={s['contiguous_seq']} b={cutoff1}")
    vessels = col.get("/vessels").json()
    check("船舶列表序号范围全部在边界之外",
          all(v["min_seq"] > cutoff1 and v["max_seq"] > cutoff1 for v in vessels))
    mmsi = vessels[0]["mmsi"]
    traj = col.get(f"/trajectories/{mmsi}").json()
    check("轨迹不含已清理序号", all(p["seq"] > cutoff1 for p in traj["positions"]))
    check("轨迹不越过连续水位", all(p["seq"] <= traj["contiguous_seq"] for p in traj["positions"]))
    check("轨迹注明已清理范围",
          traj["cleaned_up_to"] == cutoff1 and f"1..{cutoff1}" in traj["cleaned_note"],
          traj["cleaned_note"])
    # 完整窗口口径自洽：留存数 = 水位 - 边界
    check("留存计数与边界/水位同口径",
          s["received_total"] == s["contiguous_seq"] - s["cleanup_boundary"])

    # 后台分批删除：待删归零，申请标记完成
    wait_until(
        "后台清理完成",
        lambda: col.get("/cleanup/status").json()["pending_delete"] == 0,
        timeout=30,
    )
    cs = col.get("/cleanup/status").json()
    check("待删数量归零", cs["pending_delete"] == 0)
    req60 = next(q for q in cs["recent_requests"] if q["cutoff_seq"] == cutoff1 and q["accepted"])
    check("受理申请最终标记完成", req60["completed_at"] is not None)
    check("物理删除后仍无已清理序号",
          all(p["seq"] > cutoff1 for p in col.get(f"/trajectories/{mmsi}").json()["positions"]))

    # ---------- 3. 重复/并发清理，边界只增不减 ----------
    r = col.post("/admin/cleanup", json={"cutoff_seq": cutoff1})
    check("重复清理可受理（幂等）", r.status_code == 200 and r.json()["cleanup_boundary"] == cutoff1)
    smaller = max(1, cutoff1 // 2)
    r = col.post("/admin/cleanup", json={"cutoff_seq": smaller})
    check("更小截止序号不回退边界", r.status_code == 200 and r.json()["cleanup_boundary"] == cutoff1)

    results: list[httpx.Response] = []
    barrier = threading.Barrier(5)

    def fire(cutoff: int) -> None:
        barrier.wait()
        results.append(col.post("/admin/cleanup", json={"cutoff_seq": cutoff}))

    threads = [threading.Thread(target=fire, args=(cutoff2,)) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("并发清理全部受理", all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))
    s = col.get("/status").json()
    check("并发后边界推进到最大截止序号", s["cleanup_boundary"] == cutoff2, str(s["cleanup_boundary"]))
    wait_until("第二批后台清理完成",
               lambda: col.get("/cleanup/status").json()["pending_delete"] == 0)
    boundary_after = col.get("/status").json()["cleanup_boundary"]
    check("物理删除期间边界未回退", boundary_after == cutoff2)

    # ---------- 4. 已清理记录不得重新入库 ----------
    # 4a. 回退补采游标后历史循环重拉，已清理序号仍被拦截
    col.post("/admin/rewind-backfill")
    time.sleep(4)
    vessels = col.get("/vessels").json()
    check("回退补采后无已清理序号回流", all(v["min_seq"] > cutoff2 for v in vessels))

    # 4b. 岸站重置：序号从 1 重新产生，SSE/历史两路都会再次送达小序号
    old_max = col.get("/status").json()["max_seen_seq"]
    st.post("/station/reset")
    wait_until(
        "岸站重置后新代际数据到达",
        lambda: col.get("/status").json()["max_seen_seq"] > old_max + 5,
        timeout=60,
    )
    time.sleep(2)
    vessels = col.get("/vessels").json()
    check("岸站重置后已清理序号仍不回流", all(v["min_seq"] > cutoff2 for v in vessels),
          str([(v["mmsi"], v["min_seq"]) for v in vessels]))
    s = col.get("/status").json()
    check("岸站重置不改变采集器清理边界", s["cleanup_boundary"] == cutoff2)
    check("采集仍在继续", s["sse_connected"] and s["received_total"] > 0)

    # ---------- 5. 杀进程重启：边界持久化、自动续清 ----------
    w = col.get("/status").json()["contiguous_seq"]
    col.post("/admin/cleanup", json={"cutoff_seq": w})  # 立即杀进程，制造未删完状态
    restart_collector(args)
    s = col.get("/status").json()
    check("重启后清理边界保留", s["cleanup_boundary"] == w, str(s["cleanup_boundary"]))
    wait_until(
        "重启后自动续清完成",
        lambda: col.get("/cleanup/status").json()["pending_delete"] == 0,
        timeout=40,
    )
    check("重启后续清归零", col.get("/cleanup/status").json()["pending_delete"] == 0)
    wait_until("重启后采集接续",
               lambda: col.get("/status").json()["sse_connected"], timeout=40)

    # ---------- 6. 采集器清空重置：新代际，旧清理不删新归档 ----------
    epoch_before = col.get("/cleanup/status").json()["archive_epoch"]
    # 两侧一起重置，保证新代际从序号 1 快速补齐
    col.post("/admin/reset")
    st.post("/station/reset")
    cs = col.get("/cleanup/status").json()
    check("重置后边界归零", cs["cleanup_boundary"] == 0)
    check("重置后代际递增", cs["archive_epoch"] == epoch_before + 1,
          f"{epoch_before} -> {cs['archive_epoch']}")
    check("重置后清理申请留痕清空", cs["recent_requests"] == [])
    target6 = st.get("/station/status").json()["total_positions"]
    wait_until(
        "新归档重新采集",
        lambda: (lambda s: s["complete"] and s["received_total"] >= target6)
        (col.get("/status").json()),
        timeout=90,
    )
    s = col.get("/status").json()
    check("新归档包含小序号（未被旧清理删除）",
          min(v["min_seq"] for v in col.get("/vessels").json()) == 1)
    # 等新代际积累足够序号后再清理
    wait_until("新代际水位积累",
               lambda: col.get("/status").json()["contiguous_seq"] >= 40,
               timeout=90)
    new_cutoff = 20
    # 新代际上重新清理，应正常受理并由后台删除
    r = col.post("/admin/cleanup", json={"cutoff_seq": new_cutoff})
    check("新代际可再次清理", r.status_code == 200, r.text)
    wait_until("新代际清理完成",
               lambda: col.get("/cleanup/status").json()["pending_delete"] == 0)
    s = col.get("/status").json()
    check("新代际清理生效且采集继续",
          s["cleanup_boundary"] == new_cutoff and s["max_seen_seq"] > 40)

    # ---------- 7. 清理错误查询入口 ----------
    r = col.get("/errors?kind=cleanup&include_resolved=true")
    check("清理错误查询可用", r.status_code == 200 and isinstance(r.json(), list))
    r2 = col.get("/cleanup/status")
    check("清理状态查询可用", r2.status_code == 200 and "pending_delete" in r2.json())

    print(f"\n失败用例: {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
