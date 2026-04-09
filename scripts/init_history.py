"""
A 股历史数据一次性初始化脚本（Baostock）
──────────────────────────────────────────────────────────
只需运行一次。拉取全市场 5000+ 只 A 股历史日线数据，
写入 MySQL stock_basic + stock_daily 表。

数据内容：
  stock_basic  ── 代码、名称、上市日期、类型
  stock_daily  ── 日期、开盘、最高、最低、收盘、成交量、成交额、换手率、涨跌幅

运行时间：约 20~40 分钟（5000 只 × 0.3s/只）

用法：
  venv/bin/python3 -m scripts.init_history              # 默认拉取近 2 年
  venv/bin/python3 -m scripts.init_history --years 3   # 拉取近 3 年
  venv/bin/python3 -m scripts.init_history --resume    # 跳过已有数据的股票（断点续传）
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# 确保项目根目录在 sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import setup_logger

logger = setup_logger("init_history")


def _bs_code_to_db(bs_code: str) -> tuple[str, str]:
    """'sh.600000' → (code='600000', market='SH')"""
    parts = bs_code.split(".")
    market = parts[0].upper()
    code   = parts[1].zfill(6) if len(parts) > 1 else ""
    return code, market


def _safe(val: str, dp: int = -1):
    """字符串 → float，空字符串返回 None"""
    if not val or val.strip() == "":
        return None
    try:
        f = float(val)
        import math
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, dp) if dp >= 0 else f
    except ValueError:
        return None


def _safe_int(val: str):
    try:
        return int(float(val)) if val and val.strip() else None
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════
# Phase 1（同步）：Baostock 拉取
# ══════════════════════════════════════════════════════════

def fetch_stock_list() -> list[dict]:
    """获取全部上市中的 A 股（type=1, status=1）"""
    import baostock as bs
    rs = bs.query_stock_basic()
    stocks = []
    while rs.next():
        row = rs.get_row_data()
        code_bs, name, ipo_date, out_date, typ, status = row
        if typ != "1" or status != "1":
            continue
        code, market = _bs_code_to_db(code_bs)
        if not code:
            continue
        ipo = None
        if ipo_date and ipo_date.strip():
            try:
                ipo = date.fromisoformat(ipo_date.strip())
            except ValueError:
                pass
        stocks.append({
            "code":      code,
            "name":      name.strip(),
            "market":    market,
            "list_date": ipo,
            "is_st":     1 if "ST" in name.upper() else 0,
        })
    logger.info(f"[股票列表] 共 {len(stocks)} 只上市 A 股")
    return stocks


# ── 多进程工作函数（必须定义在模块顶层才能被 pickle）────────────

def _worker_batch_fetch(batch: list) -> list[tuple[str, list[dict]]]:
    """
    子进程工作函数：每个 worker 只登录一次，批量处理分配给它的全部股票。
    batch = [(bs_code, code, start_date, end_date), ...]
    返回 [(code, rows), ...] 列表。
    """
    import baostock as bs
    if not batch:
        return []
    lg = bs.login()
    if lg.error_code != "0":
        return [(task[1], []) for task in batch]
    results = []
    try:
        for bs_code, code, start_date, end_date in batch:
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount,turn,pctChg",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="3",
                )
                if rs.error_code != "0":
                    results.append((code, []))
                    continue
                rows = []
                while rs.next():
                    r = rs.get_row_data()
                    if not r[0]:
                        continue
                    rows.append({
                        "code":          code,
                        "trade_date":    r[0],
                        "open_price":    _safe(r[1], 3),
                        "high":          _safe(r[2], 3),
                        "low":           _safe(r[3], 3),
                        "close":         _safe(r[4], 3),
                        "volume":        _safe_int(r[5]),
                        "amount":        _safe(r[6], 2),
                        "turnover_rate": _safe(r[7], 4),
                        "pct_change":    _safe(r[8], 4),
                    })
                results.append((code, rows))
            except Exception:
                results.append((code, []))
    finally:
        bs.logout()
    return results


def run_fetch_all_parallel(
    stocks: list[dict],
    start_date: str,
    end_date: str,
    resume_set: set[str],
    workers: int = 4,
    db_cfg: dict = None,
) -> tuple[list[dict], list[dict]]:
    """
    多进程并发拉取所有股票历史数据。
    将任务均分给 workers 个子进程，每个子进程只登录一次，避免频繁登录被限速。
    """
    import multiprocessing as mp

    tasks = []
    basic_rows: list[dict] = []

    for stock in stocks:
        code    = stock["code"]
        market  = stock["market"]
        if market == "BJ":
            continue   # Baostock 不支持北交所（BJ）股票
        bs_code = f"{market.lower()}.{code}"
        if code in resume_set:
            continue
        tasks.append((bs_code, code, start_date, end_date))
        basic_rows.append(stock)

    if not tasks:
        logger.info("[拉取] 所有股票已有数据，无需重新拉取")
        return [], []

    # 将任务均分为 workers 个批次
    batch_size = max(1, (len(tasks) + workers - 1) // workers)
    batches = [tasks[i:i + batch_size] for i in range(0, len(tasks), batch_size)]
    actual_workers = len(batches)

    logger.info(f"[拉取] 共 {len(tasks)} 只股票，{actual_workers} 进程并发（每进程约 {batch_size} 只）...")
    t0 = time.time()

    total_written = 0
    done = 0
    FLUSH_EVERY = 10000

    with mp.Pool(processes=actual_workers) as pool:
        for batch_results in pool.imap_unordered(_worker_batch_fetch, batches):
            daily_buffer: list[dict] = []
            for code, rows in batch_results:
                daily_buffer.extend(rows)
                done += 1

            if daily_buffer and db_cfg:
                # 分块写入防单次过大
                for i in range(0, len(daily_buffer), FLUSH_EVERY):
                    chunk = daily_buffer[i:i + FLUSH_EVERY]
                    total_written += _flush_daily_sync(chunk, db_cfg)

            elapsed = time.time() - t0
            pct = done / len(tasks) * 100
            logger.info(
                f"[进度] {done}/{len(tasks)} ({pct:.0f}%) | "
                f"已写 {total_written} 行 | "
                f"耗时 {elapsed/60:.1f}min"
            )

    logger.info(f"[拉取完成] 共写入 {total_written} 行日线数据")
    return basic_rows, []   # daily 已实时写入，返回空避免重复


# ══════════════════════════════════════════════════════════
# Phase 2（异步）：批量写入 MySQL
# ══════════════════════════════════════════════════════════

def _flush_daily_sync(rows: list[dict], db_cfg: dict) -> int:
    """
    同步写入 daily 数据（在多进程回调中使用，避免 asyncio 嵌套问题）。
    直接使用 pymysql 同步驱动。
    """
    import pymysql
    if not rows:
        return 0
    sql = """
        INSERT IGNORE INTO stock_daily
            (code, trade_date, open_price, high, low, close,
             volume, amount, pct_change, turnover_rate)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
    conn = pymysql.connect(**db_cfg)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, params)
        conn.commit()
        return len(rows)
    finally:
        conn.close()

async def write_batch(basic_rows: list[dict], daily_rows: list[dict]):
    from db.stock_dao import upsert_basic_batch, upsert_daily_batch
    if basic_rows:
        await upsert_basic_batch(basic_rows)
    if daily_rows:
        n = await upsert_daily_batch(daily_rows)
        logger.info(f"[写入] {len(basic_rows)} 只基础信息 + {n} 行日线数据")


async def get_resume_set(min_rows: int = 10) -> set[str]:
    """
    返回 stock_daily 中已有足够历史数据的股票代码集合（用于断点续传）。
    min_rows: 至少有这么多行才算"已有数据"（避免只有今日快照的情况）
    """
    from db.mysql_pool import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT code FROM stock_daily GROUP BY code HAVING COUNT(*) >= %s",
                (min_rows,)
            )
            rows = await cur.fetchall()
    return {r[0] for r in rows}


# ══════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════

async def run(years: int = 2, resume: bool = False):
    """
    完整初始化流程：
      1. 连接 Baostock，获取股票列表
      2. 分批拉取历史数据（每 500 只写一次 DB，节省内存）
      3. 写入 stock_basic + stock_daily
    """
    from db.mysql_pool import get_pool, close_pool
    await get_pool()   # 建连接池 + 建表

    end_date   = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    logger.info(f"{'='*60}")
    logger.info(f"[初始化] 历史数据范围: {start_date} ~ {end_date}（{years}年）")
    logger.info(f"{'='*60}")

    import baostock as bs
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"Baostock 登录失败: {lg.error_msg}")
    logger.info("[Baostock] 登录成功")

    try:
        stocks = fetch_stock_list()

        resume_set: set[str] = set()
        if resume:
            resume_set = await get_resume_set()
            logger.info(f"[断点续传] 已跳过 {len(resume_set)} 只有历史数据的股票")

        t0 = time.time()

        # 构建同步写入用的 pymysql 配置
        from db.mysql_pool import DB_CONFIG
        _db_sync = {
            "host":    DB_CONFIG["host"],
            "port":    DB_CONFIG["port"],
            "user":    DB_CONFIG["user"],
            "password": DB_CONFIG["password"],
            "database": DB_CONFIG["db"],
            "charset":  DB_CONFIG["charset"],
        }

        basic_rows, daily_rows = run_fetch_all_parallel(
            stocks, start_date, end_date, resume_set, workers=4, db_cfg=_db_sync
        )

        # daily_rows 已在 run_fetch_all_parallel 中实时写入，这里只写 basic
        await write_batch(basic_rows, [])

    finally:
        bs.logout()
        logger.info("[Baostock] 已退出登录")

    total_elapsed = time.time() - t0
    logger.info(f"[完成] 历史数据初始化完毕，总耗时 {total_elapsed/60:.1f} 分钟")

    from db.stock_dao import get_daily_status
    status = await get_daily_status()
    logger.info(f"[结果] stock_daily: {status['total_records']} 行 | "
                f"覆盖 {status['covered_stocks']} 只 | "
                f"最新日期 {status['last_trade_date']}")

    await close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A 股历史数据一次性初始化（Baostock）")
    parser.add_argument("--years",  type=int,  default=2,     help="拉取最近 N 年数据（默认2年）")
    parser.add_argument("--resume", action="store_true",      help="断点续传：跳过已有数据的股票")
    args = parser.parse_args()

    asyncio.run(run(years=args.years, resume=args.resume))
