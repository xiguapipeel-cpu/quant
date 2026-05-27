"""
position_monitor 表 DAO — 持仓监控（双轨：is_real=0 模拟 / 1 真实）
"""
from typing import Optional, Iterable
from db.mysql_pool import get_pool


# ── 创建/更新 ─────────────────────────────────────────
async def upsert_position(
    strategy: str,
    code: str,
    name: str,
    signal_date: str,
    entry_date: str,
    entry_price: float,
    is_real: int = 0,
    shares: Optional[int] = None,
) -> None:
    """同 (strategy, code, signal_date) 视为同一笔。is_real / shares 可后续标记真实持仓时更新。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO position_monitor
                  (strategy, code, name, signal_date, entry_date, entry_price,
                   is_real, shares, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open')
                ON DUPLICATE KEY UPDATE
                  name = VALUES(name),
                  is_real = GREATEST(is_real, VALUES(is_real)),
                  shares = COALESCE(VALUES(shares), shares)
            """, (strategy, code, name, signal_date, entry_date, entry_price, is_real, shares))


async def mark_as_real(position_id: int, shares: int) -> bool:
    """把模拟持仓标记为真实持仓（接收离场推送）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE position_monitor SET is_real=1, shares=%s WHERE id=%s",
                (shares, position_id),
            )
            return cur.rowcount > 0


async def update_trail_state(
    position_id: int,
    *,
    highest_price: Optional[float],
    highest_date: Optional[str],
    lowest_price: Optional[float],
    lowest_date: Optional[str],
    days_held: int,
    last_check_date: str,
) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE position_monitor SET
                  highest_price=%s, highest_date=%s,
                  lowest_price=%s, lowest_date=%s,
                  days_held=%s, last_check_date=%s
                WHERE id=%s
            """, (highest_price, highest_date, lowest_price, lowest_date,
                  days_held, last_check_date, position_id))


async def mark_exited(
    position_id: int,
    exit_date: str,
    exit_price: float,
    exit_reason: str,
    exit_pnl_pct: float,
) -> None:
    """记录信号触发：exit_date=信号日, exit_price=信号日收盘价。
    actual_exit_* 字段由后续 fill_actual_exit 在次日补充。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE position_monitor SET
                  status='exited',
                  exit_date=%s, exit_price=%s,
                  exit_reason=%s, exit_pnl_pct=%s,
                  actual_filled=0
                WHERE id=%s
            """, (exit_date, exit_price, exit_reason[:255], exit_pnl_pct, position_id))


async def fill_actual_exit(
    position_id: int,
    actual_exit_date: str,
    actual_exit_price: float,
    actual_exit_pnl_pct: float,
) -> None:
    """用实际成交价（次日开盘）填充 + 重算 PnL"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE position_monitor SET
                  actual_exit_date=%s,
                  actual_exit_price=%s,
                  actual_filled=1,
                  exit_pnl_pct=%s
                WHERE id=%s
            """, (actual_exit_date, actual_exit_price, actual_exit_pnl_pct, position_id))


async def list_pending_actual_fill(strategy: str) -> list[dict]:
    """已离场但还没填实际成交价的真实/模拟持仓"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT * FROM position_monitor
                WHERE strategy=%s AND status='exited' AND actual_filled=0
                ORDER BY exit_date ASC
            """, (strategy,))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


async def mark_notified(position_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE position_monitor SET notified=1 WHERE id=%s", (position_id,))


# ── 读取 ─────────────────────────────────────────────
async def list_open(strategy: str, is_real: Optional[int] = None) -> list[dict]:
    pool = await get_pool()
    sql = "SELECT * FROM position_monitor WHERE strategy=%s AND status='open'"
    args = [strategy]
    if is_real is not None:
        sql += " AND is_real=%s"
        args.append(is_real)
    sql += " ORDER BY entry_date DESC, code ASC"
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


async def list_exited(strategy: str, limit: int = 100, is_real: Optional[int] = None) -> list[dict]:
    pool = await get_pool()
    sql = "SELECT * FROM position_monitor WHERE strategy=%s AND status='exited'"
    args = [strategy]
    if is_real is not None:
        sql += " AND is_real=%s"
        args.append(is_real)
    sql += " ORDER BY exit_date DESC, code ASC LIMIT %s"
    args.append(limit)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


async def list_pending_push(strategy: str) -> list[dict]:
    """已离场但还没推送过的真实持仓（用于离场推送循环）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT * FROM position_monitor
                WHERE strategy=%s AND status='exited' AND is_real=1 AND notified=0
                ORDER BY exit_date ASC
            """, (strategy,))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


async def delete_position(position_id: int) -> bool:
    """删除（用于误录的真实持仓）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM position_monitor WHERE id=%s", (position_id,))
            return cur.rowcount > 0
