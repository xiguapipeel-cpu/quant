"""
个股资金流向采集器 — AKShare → MySQL stock_fund_flow

核心特点：
  - 接口 ak.stock_individual_fund_flow 只返回最近约 120 个交易日
  - 必须每日增量采集才能积累历史；首次启用建议 cron 每日 17:00 跑一次
  - 用 INSERT ... ON DUPLICATE KEY UPDATE 实现幂等 upsert

用法：
  # 采集指定股票
  python -m data.fund_flow_fetcher --codes 600600,000001 --workers 4

  # 从 stock_basic 取全部代码（建议先 limit 测试）
  python -m data.fund_flow_fetcher --all --limit 50 --workers 8

  # 只补齐已有但近 N 日缺失的（增量模式）
  python -m data.fund_flow_fetcher --all --since-days 7
"""

import argparse
import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool
from utils.logger import setup_logger

logger = setup_logger("fund_flow_fetcher")


# ── akshare 列名映射（中文 → 英文 / DB 列名） ──────────────────
_COL_MAP = {
    "日期":            "trade_date",
    "收盘价":          "close_price",
    "涨跌幅":          "pct_change",
    "主力净流入-净额":   "main_net_amount",
    "主力净流入-净占比": "main_net_pct",
    "超大单净流入-净额": "super_large_net",
    "超大单净流入-净占比": "super_large_pct",
    "大单净流入-净额":   "large_net",
    "大单净流入-净占比": "large_pct",
    "中单净流入-净额":   "medium_net",
    "中单净流入-净占比": "medium_pct",
    "小单净流入-净额":   "small_net",
    "小单净流入-净占比": "small_pct",
}

_DB_COLS = [
    "code", "trade_date", "close_price", "pct_change",
    "main_net_amount", "main_net_pct",
    "super_large_net", "super_large_pct",
    "large_net", "large_pct",
    "medium_net", "medium_pct",
    "small_net", "small_pct",
]


# ──────────────────────────────────────────────────────────────
# 采集层（同步，akshare 不支持 async）
# ──────────────────────────────────────────────────────────────

def _market_for(code: str) -> str:
    """600/601/603/605/688 → sh，其他 → sz"""
    return "sh" if code.startswith(("6", "9")) else "sz"


def _bypass_proxy() -> dict:
    """
    临时移除系统代理 — akshare/请求库会读取这些环境变量。
    返回保存的环境变量以便恢复。
    """
    keys = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
            "all_proxy", "ALL_PROXY"]
    saved = {k: os.environ.pop(k) for k in keys if k in os.environ}
    return saved


def _restore_proxy(saved: dict) -> None:
    os.environ.update(saved)


def fetch_one_sync(code: str) -> Optional[pd.DataFrame]:
    """
    同步拉取单只股票的资金流（最近 ~120 个交易日）。
    返回标准化后的 DataFrame；失败返回 None。
    """
    try:
        import akshare as ak
    except ImportError:
        logger.error("akshare 未安装")
        return None

    saved = _bypass_proxy()
    try:
        market = _market_for(code)
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is None or df.empty:
            return None

        # 标准化列名
        df = df.rename(columns=_COL_MAP)
        # 校验关键列
        if "trade_date" not in df.columns or "main_net_amount" not in df.columns:
            logger.warning(f"{code} 返回 schema 异常: {list(df.columns)}")
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["code"] = code

        # 保留 DB 列；缺失的列填 NaN
        for col in _DB_COLS:
            if col not in df.columns:
                df[col] = None
        df = df[_DB_COLS]

        # 数值列强制 float（akshare 偶尔返回字符串）
        num_cols = [c for c in _DB_COLS if c not in ("code", "trade_date")]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        return df
    except Exception as e:
        logger.warning(f"{code} 资金流拉取失败: {e}")
        return None
    finally:
        _restore_proxy(saved)


# ──────────────────────────────────────────────────────────────
# 入库层
# ──────────────────────────────────────────────────────────────

_UPSERT_SQL = """
INSERT INTO stock_fund_flow
  (code, trade_date, close_price, pct_change,
   main_net_amount, main_net_pct,
   super_large_net, super_large_pct,
   large_net, large_pct,
   medium_net, medium_pct,
   small_net, small_pct)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  close_price=VALUES(close_price),
  pct_change=VALUES(pct_change),
  main_net_amount=VALUES(main_net_amount),
  main_net_pct=VALUES(main_net_pct),
  super_large_net=VALUES(super_large_net),
  super_large_pct=VALUES(super_large_pct),
  large_net=VALUES(large_net),
  large_pct=VALUES(large_pct),
  medium_net=VALUES(medium_net),
  medium_pct=VALUES(medium_pct),
  small_net=VALUES(small_net),
  small_pct=VALUES(small_pct)
"""


async def upsert_df(df: pd.DataFrame) -> int:
    """把 DataFrame 批量 upsert 到 stock_fund_flow，返回行数。"""
    if df is None or df.empty:
        return 0
    # 转 None（aiomysql 不接受 NaN）
    df = df.where(pd.notnull(df), None)
    rows = [tuple(r) for r in df[_DB_COLS].itertuples(index=False, name=None)]

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(_UPSERT_SQL, rows)
    return len(rows)


# ──────────────────────────────────────────────────────────────
# 编排层（并发 + 进度）
# ──────────────────────────────────────────────────────────────

async def update_codes(
    codes: Iterable[str],
    workers: int = 4,
    log_every: int = 20,
) -> dict:
    """
    并发采集 + 入库。返回 {success, fail, total_rows, fail_codes}。
    采集用 ThreadPoolExecutor（akshare 是 IO 绑定的同步库），入库走 asyncio。
    """
    codes = list(codes)
    n = len(codes)
    success = fail = total_rows = 0
    fail_codes = []

    loop = asyncio.get_event_loop()
    pool_exec = ThreadPoolExecutor(max_workers=workers)

    sem = asyncio.Semaphore(workers)

    async def _one(code: str, idx: int):
        nonlocal success, fail, total_rows
        async with sem:
            df = await loop.run_in_executor(pool_exec, fetch_one_sync, code)
            if df is None or df.empty:
                fail += 1
                fail_codes.append(code)
            else:
                rows = await upsert_df(df)
                total_rows += rows
                success += 1
            if (idx + 1) % log_every == 0 or (idx + 1) == n:
                logger.info(f"进度 {idx+1}/{n}  成功={success}  失败={fail}  累计入库={total_rows}行")

    tasks = [_one(c, i) for i, c in enumerate(codes)]
    await asyncio.gather(*tasks)
    pool_exec.shutdown(wait=False)

    return {
        "success":     success,
        "fail":        fail,
        "total_rows":  total_rows,
        "fail_codes":  fail_codes,
    }


# ──────────────────────────────────────────────────────────────
# 辅助：从 stock_basic 取全部代码
# ──────────────────────────────────────────────────────────────

async def all_basic_codes(limit: Optional[int] = None) -> list[str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = "SELECT code FROM stock_basic"
            if limit:
                sql += f" LIMIT {int(limit)}"
            await cur.execute(sql)
            rows = await cur.fetchall()
    return [r[0] for r in rows]


async def stale_codes(since_days: int) -> list[str]:
    """
    返回 stock_fund_flow 中『最新 trade_date 距今 ≥ since_days』的代码 +
    完全没有记录的 stock_basic 代码（即首次采集）。
    """
    pool = await get_pool()
    cutoff = (datetime.now().date() - pd.Timedelta(days=since_days)).date() \
        if hasattr(pd.Timedelta(days=since_days), "date") \
        else datetime.now().date()
    # 简化：用 SQL 计算
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT b.code
                FROM stock_basic b
                LEFT JOIN (
                    SELECT code, MAX(trade_date) AS last_dt
                    FROM stock_fund_flow GROUP BY code
                ) f ON f.code = b.code
                WHERE f.last_dt IS NULL
                   OR f.last_dt < DATE_SUB(CURDATE(), INTERVAL %s DAY)
            """, (since_days,))
            rows = await cur.fetchall()
    return [r[0] for r in rows]


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

async def _main_async(args):
    from db.mysql_pool import close_pool

    try:
        if args.codes:
            codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        elif args.since_days:
            codes = await stale_codes(args.since_days)
            logger.info(f"增量模式: 发现 {len(codes)} 只过期/缺失代码")
        elif args.all:
            codes = await all_basic_codes(args.limit)
            logger.info(f"全量模式: stock_basic 共 {len(codes)} 只")
        else:
            logger.error("必须指定 --codes / --all / --since-days 之一")
            return 1

        if not codes:
            logger.info("无需要采集的代码")
            return 0

        t0 = datetime.now()
        res = await update_codes(codes, workers=args.workers)
        elapsed = (datetime.now() - t0).total_seconds()

        logger.info(f"完成 | 总耗时 {elapsed:.1f}s")
        logger.info(f"  成功: {res['success']}/{len(codes)}")
        logger.info(f"  失败: {res['fail']}")
        logger.info(f"  入库: {res['total_rows']} 行")
        if res["fail_codes"][:10]:
            logger.warning(f"  失败示例: {res['fail_codes'][:10]}")
        return 0
    finally:
        await close_pool()


def main() -> int:
    ap = argparse.ArgumentParser(description="个股资金流采集器")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--codes", type=str, help="逗号分隔的代码列表")
    g.add_argument("--all", action="store_true", help="从 stock_basic 取全部")
    g.add_argument("--since-days", type=int, help="只采集最新 trade_date 距今 ≥ N 天的代码")
    ap.add_argument("--limit", type=int, default=None, help="--all 模式下限制条数（测试用）")
    ap.add_argument("--workers", type=int, default=4, help="并发数（默认 4）")
    args = ap.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
