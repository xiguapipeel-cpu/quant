"""
historical_signal_scan.py — 历史信号批量扫描
==============================================

跑历史区间的 MajorCapitalAccumulationStrategy.generate_signals，把所有 WATCH/BUY
事件批量写到 pattern_outcome（不实际下单，纯信号统计）。

用法：
  # 扫 2024-09 ~ 2026-04 全部活跃股
  python -m scripts.historical_signal_scan --start 2024-09-01 --end 2026-04-30

  # 只扫指定股票（调试）
  python -m scripts.historical_signal_scan --codes 000001,600519,300750

  # 限制范围（先小规模试）
  python -m scripts.historical_signal_scan --start 2024-09-01 --end 2026-04-30 --limit 500
"""
import argparse
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.stock_dao import get_daily_history
from db.pattern_dao import upsert_event
from backtest.strategies import MajorCapitalAccumulationStrategy
from utils.logger import setup_logger

logger = setup_logger("historical_signal_scan")

STRATEGY_ID = "major_capital_accumulation"
# 拉到比 start 更早 90 天作为 warmup（generate_signals 需要 60+ bars 历史）
WARMUP_DAYS = 90


async def _list_universe(limit: int = None, only_codes: list[str] = None) -> list[dict]:
    """从 stock_basic 取股票列表（含 name）"""
    pool = await get_pool()
    sql = "SELECT code, name, market FROM stock_basic"
    args = []
    if only_codes:
        placeholders = ','.join(['%s'] * len(only_codes))
        sql += f" WHERE code IN ({placeholders})"
        args.extend(only_codes)
    sql += " ORDER BY code"
    if limit:
        sql += f" LIMIT {int(limit)}"
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(args))
            rows = await cur.fetchall()
    return [{'code': r[0], 'name': r[1] or r[0], 'market': r[2] or 'SZ'} for r in rows]


async def scan_one_stock(stock: dict, start: str, end: str, sem: asyncio.Semaphore) -> tuple[str, int]:
    """单股扫描：拉 bars → generate_signals → 过滤区间 → upsert 到 pattern_outcome"""
    async with sem:
        code = stock['code']
        name = stock['name']
        # 拉 warmup_start ~ end 全部 bars
        warmup_start = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=WARMUP_DAYS)).strftime('%Y-%m-%d')
        bars = await get_daily_history(code, warmup_start, end)
        if not bars or len(bars) < 60:
            return code, 0
        # 标准化（确保策略期望的 key 都在）
        norm_bars = []
        for b in bars:
            norm_bars.append({
                'date':     str(b['trade_date']),
                'open':     float(b['open_price']),
                'high':     float(b['high']),
                'low':      float(b['low']),
                'close':    float(b['close']),
                'volume':   float(b['volume'] or 0),
                'amount':   float(b['amount'] or 0),
                'pct_change': float(b.get('pct_change') or 0),
            })
        # 用 P0 之前的纯 Python 策略生成信号
        strategy = MajorCapitalAccumulationStrategy()
        try:
            signals = strategy.generate_signals(code, norm_bars, {'integrity_pass': True})
        except Exception as e:
            logger.debug(f"  {code} 信号生成失败: {e}")
            return code, 0

        # 预计算每交易日 20 日均成交额(万元)，作为 signal_meta.amount_wan（rank 因子之一）
        _amt_wan_by_date = {}
        for _i, b in enumerate(norm_bars):
            _w = [x['amount'] for x in norm_bars[max(0, _i - 19):_i + 1]]
            _amt_wan_by_date[b['date']] = round(sum(_w) / len(_w) / 1e4, 0) if _w else 0

        # 过滤到 [start, end] 内
        n_inserted = 0
        for s in signals:
            if s.action not in ('WATCH', 'BUY'):
                continue
            if not (start <= s.date <= end):
                continue
            try:
                await upsert_event(
                    strategy=STRATEGY_ID,
                    code=code,
                    name=name,
                    signal_date=s.date,
                    signal_type=s.action,
                    signal_reason=(s.reason or '')[:255],
                    confidence=float(getattr(s, 'confidence', 0) or 0),
                    # 逐事件 buy_meta（rsi/yy_ratio/bb_narrow/watch_days/breakout_strength…）
                    # + amount_wan，供 rank_score 选股质量审计。WATCH 事件 meta 为空 dict。
                    signal_meta={
                        **(getattr(s, 'meta', {}) or {}),
                        "amount_wan": _amt_wan_by_date.get(s.date),
                        "source": "historical_signal_scan",
                    },
                )
                n_inserted += 1
            except Exception as e:
                logger.debug(f"  {code} {s.date} {s.action} upsert 失败: {e}")
        return code, n_inserted


async def main(args):
    logger.info(f"[scan] 区间 {args.start} ~ {args.end}")

    if args.codes:
        codes = [c.strip() for c in args.codes.split(',') if c.strip()]
        universe = await _list_universe(only_codes=codes)
    else:
        universe = await _list_universe(limit=args.limit)
    logger.info(f"[scan] 股票池: {len(universe)} 只")

    # 并发扫描（asyncio + DB 查询是 IO-bound，可高并发）
    sem = asyncio.Semaphore(args.workers)
    total_signals = 0
    ok_count = 0
    failed = 0

    async def _wrap(stock, idx):
        nonlocal total_signals, ok_count, failed
        try:
            _, n = await scan_one_stock(stock, args.start, args.end, sem)
            total_signals += n
            ok_count += 1
            if n > 0 and (idx + 1) % 50 == 0:
                logger.info(f"  进度 {idx+1}/{len(universe)} (有信号 {ok_count}, 累计 {total_signals} 个事件)")
            elif (idx + 1) % 200 == 0:
                logger.info(f"  进度 {idx+1}/{len(universe)} (累计 {total_signals} 个事件)")
        except Exception as e:
            failed += 1
            logger.debug(f"  {stock['code']} 失败: {e}")

    tasks = [_wrap(s, i) for i, s in enumerate(universe)]
    await asyncio.gather(*tasks)

    logger.info(f"[scan] 完成: {ok_count}/{len(universe)} 只成功, 失败={failed}, 累计写入 {total_signals} 个事件")
    await close_pool()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2024-09-01')
    ap.add_argument('--end',   default='2026-04-30')
    ap.add_argument('--codes', type=str, default=None, help='逗号分隔的 code 列表（调试用）')
    ap.add_argument('--limit', type=int, default=None, help='限制扫多少只股票（调试用）')
    ap.add_argument('--workers', type=int, default=16)
    args = ap.parse_args()
    asyncio.run(main(args))
