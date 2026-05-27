"""
pattern_outcome 表 DAO
=======================
形态命中事件 + 后续 5/10/30/60 日表现追踪
"""
import json
from datetime import date
from typing import Optional, Iterable
from db.mysql_pool import get_pool


# ── 写入：upsert 一条命中事件（首次创建时 outcome 字段全 NULL） ──
async def upsert_event(
    strategy: str,
    code: str,
    name: str,
    signal_date: str,
    signal_type: str,
    signal_reason: str = '',
    confidence: float = 0.0,
    strategy_version: str = '',
    parameter_snapshot: Optional[str] = None,
    signal_meta: Optional[dict] = None,
    scan_time: Optional[str] = None,
) -> None:
    """
    新增/更新一条命中事件。
    UNIQUE KEY (strategy, code, signal_date) — 同一股票同一日的同一信号合并。
    outcome 字段不在此函数管理（由 update_outcomes 填充）。
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO pattern_outcome
                  (strategy, code, name, signal_date, signal_type, signal_reason, confidence,
                   strategy_version, parameter_snapshot, signal_meta, scan_time, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ON DUPLICATE KEY UPDATE
                  name=VALUES(name),
                  signal_type=VALUES(signal_type),
                  signal_reason=VALUES(signal_reason),
                  confidence=VALUES(confidence),
                  strategy_version=VALUES(strategy_version),
                  parameter_snapshot=VALUES(parameter_snapshot),
                  signal_meta=VALUES(signal_meta),
                  scan_time=VALUES(scan_time)
            """, (strategy, code, name, signal_date, signal_type,
                  signal_reason[:255] if signal_reason else None, confidence,
                  strategy_version or None,
                  parameter_snapshot,
                  json.dumps(signal_meta or {}, ensure_ascii=False, sort_keys=True),
                  scan_time))


# ── 写入：更新某条事件的 outcome 字段 ──
async def update_outcome(
    strategy: str,
    code: str,
    signal_date: str,
    *,
    buy_price: Optional[float] = None,
    buy_date: Optional[str] = None,
    ret_5d: Optional[float] = None,
    ret_10d: Optional[float] = None,
    ret_30d: Optional[float] = None,
    ret_60d: Optional[float] = None,
    peak_ret: Optional[float] = None,
    trough_ret: Optional[float] = None,
    peak_date: Optional[str] = None,
    trough_date: Optional[str] = None,
    bars_seen: int = 0,
    status: str = 'pending',
) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE pattern_outcome SET
                  buy_price=%s, buy_date=%s,
                  ret_5d=%s, ret_10d=%s, ret_30d=%s, ret_60d=%s,
                  peak_ret=%s, trough_ret=%s,
                  peak_date=%s, trough_date=%s,
                  bars_seen=%s, status=%s
                WHERE strategy=%s AND code=%s AND signal_date=%s
            """, (buy_price, buy_date,
                  ret_5d, ret_10d, ret_30d, ret_60d,
                  peak_ret, trough_ret, peak_date, trough_date,
                  bars_seen, status,
                  strategy, code, signal_date))


# ── 读取：列出某策略下的事件（含 outcome） ──
async def list_events(
    strategy: str,
    status_in: Optional[Iterable[str]] = None,
    signal_type: Optional[str] = None,
    since_date: Optional[str] = None,
    limit: int = 10000,
) -> list[dict]:
    pool = await get_pool()
    sql = "SELECT * FROM pattern_outcome WHERE strategy=%s"
    args: list = [strategy]
    if status_in:
        placeholders = ','.join(['%s'] * len(list(status_in)))
        sql += f" AND status IN ({placeholders})"
        args.extend(status_in)
    if signal_type:
        sql += " AND signal_type=%s"
        args.append(signal_type)
    if since_date:
        sql += " AND signal_date >= %s"
        args.append(since_date)
    sql += " ORDER BY signal_date DESC, code ASC LIMIT %s"
    args.append(limit)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


# ── 读取：聚合统计（前端图表用） ──
async def aggregate_stats(strategy: str, since_date: Optional[str] = None) -> dict:
    pool = await get_pool()
    where = "strategy=%s AND status IN ('partial', 'completed')"
    args: list = [strategy]
    if since_date:
        where += " AND signal_date >= %s"
        args.append(since_date)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 总命中数、各窗口胜率、平均收益
            await cur.execute(f"""
                SELECT
                  COUNT(*) AS total,
                  SUM(signal_type='BUY')   AS n_buy,
                  SUM(signal_type='WATCH') AS n_watch,
                  AVG(ret_5d)  AS avg_5d,
                  AVG(ret_10d) AS avg_10d,
                  AVG(ret_30d) AS avg_30d,
                  AVG(ret_60d) AS avg_60d,
                  SUM(ret_5d  > 0) / SUM(ret_5d  IS NOT NULL) AS win_5d,
                  SUM(ret_10d > 0) / SUM(ret_10d IS NOT NULL) AS win_10d,
                  SUM(ret_30d > 0) / SUM(ret_30d IS NOT NULL) AS win_30d,
                  SUM(ret_60d > 0) / SUM(ret_60d IS NOT NULL) AS win_60d,
                  AVG(peak_ret)   AS avg_peak,
                  AVG(trough_ret) AS avg_trough,
                  MAX(peak_ret)   AS max_peak,
                  MIN(trough_ret) AS min_trough
                FROM pattern_outcome
                WHERE {where}
            """, tuple(args))
            cols = [d[0] for d in cur.description]
            row = await cur.fetchone()
    if not row:
        return {}
    out = {c: (float(v) if isinstance(v, (int, float)) and v is not None and c != 'total' and c.startswith(('n_', 'win_'))
               else v) for c, v in zip(cols, row)}
    # 数字字段统一转 float（避免 Decimal）
    for k, v in list(out.items()):
        if v is not None and not isinstance(v, (int, str, bool)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                pass
    return out


# ── 读取：拿待更新的事件（buy_date 后还没满 60 bar 的） ──
async def list_pending(strategy: str, limit: int = 10000) -> list[dict]:
    """返回 status='pending' 或 'partial' 的事件，按 signal_date 升序（先处理老的）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT * FROM pattern_outcome
                WHERE strategy=%s AND status IN ('pending', 'partial')
                ORDER BY signal_date ASC, code ASC
                LIMIT %s
            """, (strategy, limit))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]
