"""
股票本地数据仓库 DAO
─────────────────────────────────────
提供三张表的读写操作：
  - stock_basic     股票基础信息
  - stock_snapshot  每日行情快照（选股主数据源）
  - stock_daily     日线K线数据（技术分析用）
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from db.mysql_pool import get_pool
from utils.logger import setup_logger

logger = setup_logger("stock_dao")


# ══════════════════════════════════════════════════════════
# stock_basic  股票基础信息
# ══════════════════════════════════════════════════════════

async def upsert_basic_batch(rows: list[dict]) -> int:
    """
    批量写入 stock_basic（不存在则插入，已存在则更新名称/行业/上市日期）。
    rows 每行需包含: code, name, market, industry(可选), list_date(可选), is_st(可选)
    返回受影响行数。
    """
    if not rows:
        return 0
    pool = await get_pool()
    sql = """
        INSERT INTO stock_basic (code, name, market, industry, list_date, is_st)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name      = VALUES(name),
            market    = VALUES(market),
            industry  = COALESCE(VALUES(industry), industry),
            list_date = COALESCE(VALUES(list_date), list_date),
            is_st     = VALUES(is_st)
    """
    params = [
        (
            r["code"],
            r.get("name", ""),
            r.get("market", "SZ"),
            r.get("industry"),
            r.get("list_date"),
            1 if r.get("is_st") else 0,
        )
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, params)
            return cur.rowcount


async def get_basic_missing_info(limit: int = 200) -> list[str]:
    """返回 industry 或 list_date 为空的股票代码列表（优先补全）"""
    pool = await get_pool()
    sql = """
        SELECT code FROM stock_basic
        WHERE industry IS NULL OR list_date IS NULL
        ORDER BY code LIMIT %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (limit,))
            rows = await cur.fetchall()
            return [r[0] for r in rows]


# ══════════════════════════════════════════════════════════
# stock_snapshot  每日行情快照
# ══════════════════════════════════════════════════════════

async def upsert_snapshot_batch(rows: list[dict]) -> int:
    """
    批量写入 stock_snapshot（INSERT ON DUPLICATE KEY UPDATE）。
    rows 每行字段对应 stock_snapshot 列。
    返回受影响行数。
    """
    if not rows:
        return 0
    pool = await get_pool()
    sql = """
        INSERT INTO stock_snapshot (
            code, name, market,
            price, pct_change, volume, amount,
            market_cap, float_cap, pe_ttm, pb,
            turnover_rate, amplitude, high, low,
            open_price, prev_close, vol_ratio,
            pct_60d, pct_ytd,
            industry, list_date, is_st, trade_date
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            name          = VALUES(name),
            market        = VALUES(market),
            price         = VALUES(price),
            pct_change    = VALUES(pct_change),
            volume        = VALUES(volume),
            amount        = VALUES(amount),
            market_cap    = VALUES(market_cap),
            float_cap     = VALUES(float_cap),
            pe_ttm        = VALUES(pe_ttm),
            pb            = VALUES(pb),
            turnover_rate = VALUES(turnover_rate),
            amplitude     = VALUES(amplitude),
            high          = VALUES(high),
            low           = VALUES(low),
            open_price    = VALUES(open_price),
            prev_close    = VALUES(prev_close),
            vol_ratio     = VALUES(vol_ratio),
            pct_60d       = VALUES(pct_60d),
            pct_ytd       = VALUES(pct_ytd),
            industry      = COALESCE(VALUES(industry), industry),
            list_date     = COALESCE(VALUES(list_date), list_date),
            is_st         = VALUES(is_st),
            trade_date    = VALUES(trade_date)
    """

    def _v(r: dict, key: str, dp: int = -1):
        """读取数值字段；dp>=0 时四舍五入到指定小数位（避免 DECIMAL 截断警告）"""
        v = r.get(key)
        if v is None:
            return None
        try:
            import math
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return round(f, dp) if dp >= 0 else f
        except (TypeError, ValueError):
            return v  # 字符串字段（code/name/market/industry）

    params = [
        (
            r["code"], r.get("name", ""), r.get("market", "SZ"),
            _v(r, "price", 3), _v(r, "pct_change", 4), _v(r, "volume"), _v(r, "amount", 2),
            _v(r, "market_cap", 2), _v(r, "float_cap", 2), _v(r, "pe_ttm", 3), _v(r, "pb", 3),
            _v(r, "turnover_rate", 4), _v(r, "amplitude", 4), _v(r, "high", 3), _v(r, "low", 3),
            _v(r, "open_price", 3), _v(r, "prev_close", 3), _v(r, "vol_ratio", 4),
            _v(r, "pct_60d", 4), _v(r, "pct_ytd", 4),
            r.get("industry"), r.get("list_date"), 1 if r.get("is_st") else 0,
            r.get("trade_date"),
        )
        for r in rows
    ]

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, params)
            return cur.rowcount


async def get_snapshot_status() -> dict:
    """返回快照表状态：股票总数、最新数据日期"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*), MAX(trade_date) FROM stock_snapshot")
            row = await cur.fetchone()
            return {
                "total": row[0] or 0,
                "last_trade_date": str(row[1]) if row[1] else None,
            }


async def query_snapshot(
    min_cap_yi: float = 0,
    max_cap_yi: float = 0,
    min_amount_wan: float = 0,
    min_price: float = 0,
    max_price: float = 0,
    exclude_st: bool = True,
    min_list_days: int = 0,
    industry: Optional[str] = None,
    min_pe: Optional[float] = None,
    max_pe: Optional[float] = None,
    min_pb: Optional[float] = None,
    max_pb: Optional[float] = None,
    min_turnover: Optional[float] = None,
    order_by: str = "amount DESC",
    top_n: int = 500,
) -> list[dict]:
    """
    从 stock_snapshot 做 SQL 筛选，返回符合条件的股票列表。
    全程在 MySQL 完成过滤，无需 Python 遍历 5000+ 行。
    """
    pool = await get_pool()
    conditions = ["trade_date IS NOT NULL", "price > 0"]
    args: list = []

    if exclude_st:
        conditions.append("is_st = 0")

    if min_cap_yi > 0:
        conditions.append("market_cap >= %s")
        args.append(min_cap_yi * 1e8)
    if max_cap_yi > 0:
        conditions.append("market_cap <= %s")
        args.append(max_cap_yi * 1e8)

    if min_amount_wan > 0:
        conditions.append("amount >= %s")
        args.append(min_amount_wan * 1e4)

    if min_price > 0:
        conditions.append("price >= %s")
        args.append(min_price)
    if max_price > 0:
        conditions.append("price <= %s")
        args.append(max_price)

    if min_list_days > 0:
        conditions.append("list_date IS NOT NULL AND list_date <= DATE_SUB(CURDATE(), INTERVAL %s DAY)")
        args.append(min_list_days)

    if industry:
        conditions.append("industry = %s")
        args.append(industry)

    if min_pe is not None:
        conditions.append("pe_ttm >= %s")
        args.append(min_pe)
    if max_pe is not None:
        conditions.append("pe_ttm <= %s")
        args.append(max_pe)

    if min_pb is not None:
        conditions.append("pb >= %s")
        args.append(min_pb)

    if min_turnover is not None:
        conditions.append("turnover_rate >= %s")
        args.append(min_turnover)

    # 安全白名单限制 order_by（防注入）
    allowed_orders = {
        "amount DESC", "amount ASC",
        "market_cap DESC", "market_cap ASC",
        "pct_change DESC", "pct_change ASC",
        "turnover_rate DESC", "turnover_rate ASC",
        "pe_ttm ASC", "pe_ttm DESC",
    }
    safe_order = order_by if order_by in allowed_orders else "amount DESC"

    where = " AND ".join(conditions)
    sql = f"""
        SELECT code, name, market, price, pct_change, volume, amount,
               market_cap, float_cap, pe_ttm, pb, turnover_rate,
               industry, list_date, is_st, trade_date
        FROM stock_snapshot
        WHERE {where}
        ORDER BY {safe_order}
        LIMIT %s
    """
    args.append(top_n)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()

    result = []
    for row in rows:
        r = dict(zip(cols, row))
        # 转为选股器标准格式
        mc = float(r["market_cap"] or 0)
        amt = float(r["amount"] or 0)
        result.append({
            "code":         r["code"],
            "name":         r["name"],
            "market":       r["market"],
            "price":        float(r["price"] or 0),
            "pct_change":   float(r["pct_change"] or 0) if r["pct_change"] is not None else None,
            "cap_yi":       round(mc / 1e8, 1),
            "amount_wan":   round(amt / 1e4, 0),
            "pe":           float(r["pe_ttm"]) if r["pe_ttm"] is not None else None,
            "pb":           float(r["pb"]) if r["pb"] is not None else None,
            "turnover_rate": float(r["turnover_rate"]) if r["turnover_rate"] is not None else None,
            "industry":     r["industry"],
            "list_date":    str(r["list_date"]) if r["list_date"] else None,
            "trade_date":   str(r["trade_date"]) if r["trade_date"] else None,
            "source":       "local_db",
        })
    return result


# ══════════════════════════════════════════════════════════
# stock_daily  日线K线数据
# ══════════════════════════════════════════════════════════

async def upsert_daily_batch(rows: list[dict]) -> int:
    """
    批量写入 stock_daily（INSERT IGNORE，已有则跳过）。
    rows 每行包含: code, trade_date, open_price, high, low, close,
                  volume, amount, pct_change, turnover_rate
    """
    if not rows:
        return 0
    pool = await get_pool()
    sql = """
        INSERT IGNORE INTO stock_daily
            (code, trade_date, open_price, high, low, close,
             volume, amount, pct_change, turnover_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    def _f(v):
        if v is None:
            return None
        try:
            import math
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except (TypeError, ValueError):
            return None

    params = [
        (
            r["code"], r["trade_date"],
            _f(r.get("open_price")), _f(r.get("high")), _f(r.get("low")), _f(r.get("close")),
            r.get("volume"), _f(r.get("amount")), _f(r.get("pct_change")), _f(r.get("turnover_rate")),
        )
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, params)
            return cur.rowcount


async def get_last_daily_dates(codes: list[str]) -> dict[str, date | None]:
    """
    批量查询各股票在 stock_daily 中的最新日期。
    返回 {code: last_date or None}
    """
    if not codes:
        return {}
    pool = await get_pool()
    placeholders = ",".join(["%s"] * len(codes))
    sql = f"""
        SELECT code, MAX(trade_date) AS last_date
        FROM stock_daily
        WHERE code IN ({placeholders})
        GROUP BY code
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, codes)
            rows = await cur.fetchall()
    result = {code: None for code in codes}
    for code, last_date in rows:
        result[code] = last_date
    return result


async def get_daily_history(code: str, start_date: str, end_date: str) -> list[dict]:
    """读取某只股票的日线历史（供策略计算技术指标用）"""
    pool = await get_pool()
    sql = """
        SELECT trade_date, open_price, high, low, close, volume, amount, pct_change, turnover_rate
        FROM stock_daily
        WHERE code = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (code, start_date, end_date))
            cols = [d[0] for d in cur.description]
            rows = await cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


async def get_daily_status() -> dict:
    """返回日线表状态：记录总数、覆盖股票数、最新日期"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT code), MAX(trade_date) FROM stock_daily"
            )
            row = await cur.fetchone()
            return {
                "total_records": row[0] or 0,
                "covered_stocks": row[1] or 0,
                "last_trade_date": str(row[2]) if row[2] else None,
            }
