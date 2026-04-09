"""
每日行情增量更新脚本
──────────────────────────────────────────────────────────
运行时机：每个交易日收盘后 15:30

数据源：
  主源  新浪财经 Market_Center.getHQNodeData
        全量 5500+ 只 A 股，含价格/PE/PB/市值/换手率
        10 线程并发约 3 秒拉完，无代理封锁问题

更新内容：
  stock_snapshot ── 全量 upsert（每日行情快照）
  stock_daily    ── 当日 OHLCV 追加（INSERT IGNORE 防重复）
  stock_basic    ── 名称/ST 标记同步更新

用法：
  venv/bin/python3 -m scripts.daily_data_update

  # crontab（每天 15:30，仅工作日）
  30 15 * * 1-5  cd /Users/zhuzhu/Documents/quant_system && \\
      venv/bin/python3 -m scripts.daily_data_update >> logs/update.log 2>&1
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import setup_logger

logger = setup_logger("daily_update")

_SINA_URL  = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
_SINA_HDR  = {
    "Referer":    "https://finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}
_PAGE_SIZE = 100
_WORKERS   = 10


# ── 工具函数 ────────────────────────────────────────────────

def _safe(val, dp: int = -1):
    if val is None:
        return None
    try:
        import math
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, dp) if dp >= 0 else f
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    f = _safe(val)
    return int(f) if f is not None else None


def _infer_market(symbol: str) -> str:
    """'sh600000' / 'sz000001' / 'bj920000' → 'SH' / 'SZ' / 'BJ'"""
    s = symbol.lower()
    if s.startswith("sh"):
        return "SH"
    if s.startswith("bj"):
        return "BJ"
    return "SZ"


# ── 新浪全市场行情拉取 ──────────────────────────────────────

def _fetch_one_page(page: int, session: requests.Session) -> list[dict]:
    """拉取单页行情数据，失败返回空列表"""
    try:
        r = session.get(
            _SINA_URL,
            params={
                "page": page,
                "num":  _PAGE_SIZE,
                "sort": "symbol",
                "asc":  1,
                "node": "hs_a",
                "_s_rep_type": 1,
            },
            timeout=15,
        )
        r.raise_for_status()
        return json.loads(r.text) or []
    except Exception as e:
        logger.warning(f"[快照] 第{page}页拉取失败: {e}")
        return []


def _fetch_all_sina() -> list[dict]:
    """
    用 ThreadPoolExecutor 并发拉取全量新浪行情。
    先用串行探测总页数，再并发拉取所有页。
    """
    session = requests.Session()
    session.headers.update(_SINA_HDR)

    # 探测总页数（最多 200 页保护）
    total_pages = 1
    for p in range(1, 200):
        data = _fetch_one_page(p, session)
        if not data:
            total_pages = p - 1
            break
        if len(data) < _PAGE_SIZE:
            total_pages = p
            break

    if total_pages < 1:
        return []

    logger.info(f"[快照] 共 {total_pages} 页，开始 {_WORKERS} 线程并发拉取...")

    all_rows: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        futures = {ex.submit(_fetch_one_page, p, session): p
                   for p in range(1, total_pages + 1)}
        for fut in concurrent.futures.as_completed(futures):
            all_rows.extend(fut.result())

    return all_rows


# ── 解析 + 写入 ─────────────────────────────────────────────

async def update_snapshot() -> dict:
    """
    拉取新浪全市场行情，写入 stock_snapshot / stock_basic / stock_daily。
    """
    logger.info("[快照] 开始拉取全市场行情（新浪财经）...")
    t0 = time.time()

    loop = asyncio.get_event_loop()
    raw = await loop.run_in_executor(None, _fetch_all_sina)

    if not raw:
        raise RuntimeError("[快照] 新浪行情返回空数据，请检查网络")

    logger.info(f"[快照] 获取 {len(raw)} 条，耗时 {time.time()-t0:.1f}s，开始解析写入...")

    trade_date     = date.today()
    snapshot_rows: list[dict] = []
    daily_rows:    list[dict] = []
    basic_rows:    list[dict] = []

    for row in raw:
        symbol = str(row.get("symbol", "")).strip()   # e.g. "sh600000"
        code   = str(row.get("code",   "")).strip().zfill(6)
        name   = str(row.get("name",   "")).strip()

        if not code or not code.isdigit():
            continue

        market = _infer_market(symbol)
        is_st  = 1 if "ST" in name.upper() else 0

        price      = _safe(row.get("trade"),        3)
        pct_change = _safe(row.get("changepercent"), 4)
        open_p     = _safe(row.get("open"),          3)
        high       = _safe(row.get("high"),          3)
        low        = _safe(row.get("low"),           3)
        prev_close = _safe(row.get("settlement"),    3)   # 昨收
        volume     = _safe_int(row.get("volume"))         # 手
        amount     = _safe(row.get("amount"),        2)   # 元

        # 市值单位：万元 → 元
        mktcap_wan = _safe(row.get("mktcap"))
        nmc_wan    = _safe(row.get("nmc"))
        market_cap = round(mktcap_wan * 1e4, 2) if mktcap_wan else None
        float_cap  = round(nmc_wan    * 1e4, 2) if nmc_wan    else None

        pe_ttm     = _safe(row.get("per"),           3)
        pb         = _safe(row.get("pb"),            3)
        turnover   = _safe(row.get("turnoverratio"), 4)

        snapshot_rows.append({
            "code":         code,
            "name":         name,
            "market":       market,
            "price":        price,
            "pct_change":   pct_change,
            "volume":       volume,
            "amount":       amount,
            "market_cap":   market_cap,
            "float_cap":    float_cap,
            "pe_ttm":       pe_ttm,
            "pb":           pb,
            "turnover_rate": turnover,
            "high":         high,
            "low":          low,
            "open_price":   open_p,
            "prev_close":   prev_close,
            "is_st":        is_st,
            "trade_date":   trade_date,
        })

        if price and price > 0:
            daily_rows.append({
                "code":         code,
                "trade_date":   trade_date,
                "open_price":   open_p,
                "high":         high,
                "low":          low,
                "close":        price,
                "volume":       volume,
                "amount":       amount,
                "pct_change":   pct_change,
                "turnover_rate": turnover,
            })

        basic_rows.append({
            "code": code, "name": name, "market": market, "is_st": is_st,
        })

    from db.stock_dao import upsert_snapshot_batch, upsert_daily_batch, upsert_basic_batch

    await upsert_basic_batch(basic_rows)
    inserted_snap  = await upsert_snapshot_batch(snapshot_rows)
    inserted_daily = await upsert_daily_batch(daily_rows)

    elapsed = time.time() - t0
    logger.info(
        f"[快照] 完成：{len(snapshot_rows)} 只股票 | "
        f"快照 {inserted_snap} 行 | 日线 {inserted_daily} 行 | "
        f"总耗时 {elapsed:.1f}s"
    )
    return {
        "total":          len(snapshot_rows),
        "inserted_snap":  inserted_snap,
        "inserted_daily": inserted_daily,
        "trade_date":     str(trade_date),
    }


# ── 主入口 ─────────────────────────────────────────────────

async def run_daily_update() -> dict:
    from db.mysql_pool import get_pool, close_pool
    import datetime

    start = time.time()
    logger.info(f"{'='*60}")
    logger.info(f"[更新] 开始每日数据更新 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}")

    await get_pool()

    result = {}
    try:
        result["snapshot"] = await update_snapshot()
    except Exception as e:
        logger.error(f"[更新] 快照更新失败: {e}")
        result["snapshot"] = {"error": str(e)}

    elapsed = time.time() - start
    logger.info(f"[更新] 完成，总耗时 {elapsed:.0f}s | 结果: {result}")

    await close_pool()
    return result


if __name__ == "__main__":
    asyncio.run(run_daily_update())
