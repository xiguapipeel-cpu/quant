"""
update_idx_sh.py — 每日更新上证综指(idx_sh)到 stock_daily
========================================================
大盘 regime 判断（evaluate_market_regime）依赖 stock_daily 中 code='idx_sh' 的
指数日线。该数据需每日同步，否则会用陈旧数据得出错误的"趋势走弱"减仓提示。

用法：
  python -m scripts.update_idx_sh                # 更新最近 ~120 天
  python -m scripts.update_idx_sh --days 400     # 更大回补窗口

被调用：
  from scripts.update_idx_sh import update_idx_sh
  n, latest = await update_idx_sh()
"""
import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.stock_dao import upsert_daily_batch
from db.mysql_pool import close_pool
from utils.logger import setup_logger

logger = setup_logger("update_idx_sh")

INDEX_SYMBOL = "sh000001"   # 上证综指（新浪接口）
DB_CODE = "idx_sh"


async def update_idx_sh(lookback_days: int = 120) -> tuple[int, str]:
    """拉取上证综指最近 lookback_days 天日线，upsert 到 stock_daily(code='idx_sh')。
    返回 (写入行数, 最新交易日字符串)。INSERT IGNORE：已存在的日期跳过。"""
    # akshare 走直连，移除代理
    for k in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY'):
        os.environ.pop(k, None)

    import akshare as ak
    import pandas as pd

    df = ak.stock_zh_index_daily(symbol=INDEX_SYMBOL)
    df['date'] = pd.to_datetime(df['date'])
    cutoff = pd.Timestamp(date.today() - timedelta(days=lookback_days))
    df = df[df['date'] >= cutoff].copy()

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'code':          DB_CODE,
            'trade_date':    r['date'].date(),
            'open_price':    float(r['open']),
            'high':          float(r['high']),
            'low':           float(r['low']),
            'close':         float(r['close']),
            'volume':        float(r['volume']) if r.get('volume') is not None else None,
            'amount':        None,
            'pct_change':    None,
            'turnover_rate': None,
        })
    n = await upsert_daily_batch(rows) if rows else 0
    latest = str(df['date'].max().date()) if len(df) else "—"
    logger.info(f"[update_idx_sh] 拉取 {len(rows)} 行（近{lookback_days}天），新增 {n} 行，最新 {latest}")
    return n, latest


async def _main(args):
    await update_idx_sh(lookback_days=args.days)
    await close_pool()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=120, help='回补窗口天数（默认120）')
    asyncio.run(_main(ap.parse_args()))
