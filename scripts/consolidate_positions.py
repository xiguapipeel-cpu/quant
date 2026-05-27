"""
合并 position_monitor 中同一股票的"重复持仓周期"
====================================================

问题：同一股票连续几天触发 BUY 信号 → 每天独立成一条 position_monitor
     但它们实际是同一笔持仓（建仓确认期持续命中），不应分开统计。

合并规则（按 code 分组，signal_date 升序遍历）：
  - 维护"当前活跃周期" active_session（保留的 id）
  - 维护"周期屏障" active_exit（active_session 的 exit_date，若 still open 则 ∞）
  - 新记录 signal_date <= active_exit ⇒ 属于当前周期 → 删除
  - 新记录 signal_date > active_exit  ⇒ 开启新周期 → 保留

用法：
  python -m scripts.consolidate_positions             # 仅打印将要删的数量（dry-run）
  python -m scripts.consolidate_positions --execute   # 真实删除
"""
import argparse
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from utils.logger import setup_logger

logger = setup_logger("consolidate_positions")

STRATEGY_ID = "major_capital_accumulation"


async def main(execute: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT id, code, signal_date, entry_date, status, exit_date
                FROM position_monitor
                WHERE strategy=%s
                ORDER BY code, signal_date ASC, id ASC
            """, (STRATEGY_ID,))
            rows = await cur.fetchall()

    by_code = defaultdict(list)
    for r in rows:
        by_code[r[1]].append({
            "id": r[0], "code": r[1],
            "signal_date": str(r[2]),
            "entry_date":  str(r[3]),
            "status":      r[4],
            "exit_date":   str(r[5]) if r[5] else None,
        })

    delete_ids: list[int] = []
    sessions_kept = 0

    for code, items in by_code.items():
        active_id = None
        active_exit = None   # None = 周期还 open（无屏障）

        for r in items:
            sd = r['signal_date']
            if active_id is None:
                active_id = r['id']
                active_exit = r['exit_date'] if r['status'] == 'exited' else None
                sessions_kept += 1
                continue

            # 当前周期还 open（active_exit=None）→ 所有新 signal 都视为重复
            if active_exit is None:
                delete_ids.append(r['id'])
                continue

            # 周期已 exited，比较 sd 与屏障
            if sd <= active_exit:
                # 周期内重复 → 删
                delete_ids.append(r['id'])
            else:
                # 新周期
                active_id = r['id']
                active_exit = r['exit_date'] if r['status'] == 'exited' else None
                sessions_kept += 1

    print(f"扫描: {len(by_code)} 只股票 / {len(rows)} 条记录")
    print(f"将保留: {sessions_kept} 个独立持仓周期")
    print(f"将删除: {len(delete_ids)} 条重复记录（同周期内 BUY 信号叠加）")
    print(f"压缩率: {sessions_kept}/{len(rows)} = {sessions_kept/len(rows)*100:.1f}%")

    if not execute:
        print("\n[dry-run] 未执行删除，加 --execute 真删")
        await close_pool()
        return

    # 批量删除
    if delete_ids:
        pool2 = await get_pool()
        async with pool2.acquire() as conn:
            async with conn.cursor() as cur:
                # 分批避免单 SQL 过长
                batch = 500
                for i in range(0, len(delete_ids), batch):
                    chunk = delete_ids[i:i + batch]
                    placeholders = ",".join(["%s"] * len(chunk))
                    await cur.execute(
                        f"DELETE FROM position_monitor WHERE id IN ({placeholders})",
                        tuple(chunk),
                    )
                logger.info(f"[execute] 已删除 {len(delete_ids)} 条")
    await close_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="真实执行删除（不加只 dry-run）")
    args = ap.parse_args()
    asyncio.run(main(args.execute))
