"""
pattern_tracker.py — 形态命中事件 + 后续走势追踪
====================================================

每个命中事件记录于 pattern_outcome 表，跟踪信号日次开盘价后 5/10/30/60 交易日
的收益、60d 内峰值/谷值。daily cron 增量更新所有 pending/partial 状态的事件。

用法：
  # 每日：扫描新事件 + 刷新所有未完成的 outcome
  python -m scripts.pattern_tracker --update

  # 一次性回填：从 scan_results 的 signal_dates JSON 提取历史事件
  python -m scripts.pattern_tracker --bootstrap

  # 重新跑历史：删除全部记录后从头开始
  python -m scripts.pattern_tracker --reset --bootstrap --update

  # 限定策略（默认 major_capital_accumulation）
  python -m scripts.pattern_tracker --update --strategy major_capital_accumulation
"""
import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.pattern_dao import upsert_event, update_outcome, list_pending
from db.stock_dao import get_daily_history
from config.strategy_versions import (
    MAJOR_CAPITAL_FROZEN_VERSION,
    major_capital_param_snapshot,
)
from utils.logger import setup_logger

logger = setup_logger("pattern_tracker")


# ──────────────────────────────────────────────────────────
# 1. Bootstrap：从 scan_results.signal_dates JSON 提取历史事件
# ──────────────────────────────────────────────────────────

async def bootstrap_from_scan_results(strategy: str) -> int:
    """
    遍历 scan_results 表，把每条记录的 signal_dates JSON 数组每一项写为一条 pattern_outcome 事件。
    返回新增/更新的事件数。
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT code, name, signal_dates, signal_type, signal_date,
                       signal_reason, confidence
                FROM scan_results
                WHERE strategy=%s
            """, (strategy,))
            rows = await cur.fetchall()

    n_events = 0
    for code, name, signal_dates_json, latest_type, latest_date, latest_reason, conf in rows:
        # 1) 解析 signal_dates JSON 数组
        try:
            sd_list = json.loads(signal_dates_json) if signal_dates_json else []
        except Exception:
            sd_list = []
        # 2) 加上当前最新一条（signal_date 字段）
        seen_dates = {x.get('date') for x in sd_list if x.get('date')}
        if latest_date and latest_date not in seen_dates:
            sd_list.append({
                'date': latest_date,
                'type': latest_type,
                'reason': latest_reason or '',
            })
        # 3) 写入每条事件
        for ev in sd_list:
            d = ev.get('date')
            t = ev.get('type') or latest_type or 'WATCH'
            r = ev.get('reason') or ''
            if not d:
                continue
            try:
                # signal_dates JSON 里的日期可能是 'YYYY-MM-DD' 字符串
                date.fromisoformat(d)
            except Exception:
                continue
            await upsert_event(
                strategy=strategy,
                code=code,
                name=name or code,
                signal_date=d,
                signal_type=t,
                signal_reason=r,
                confidence=float(conf or 0.0),
                strategy_version=MAJOR_CAPITAL_FROZEN_VERSION,
                parameter_snapshot=major_capital_param_snapshot(),
                signal_meta={"source": "scan_results.bootstrap"},
            )
            n_events += 1

    return n_events


# ──────────────────────────────────────────────────────────
# 2. Update：刷新所有 pending/partial 事件的 outcome
# ──────────────────────────────────────────────────────────

async def _next_trading_day_close(code: str, after_date: str, days_offset: int = 1):
    """
    在 stock_daily 中查找 after_date 之后 第 days_offset 个交易日的 open + close 价格。
    返回 (date_iso, open_price, close_price) 或 None。
    """
    # 拉 90 天裕度，足以覆盖 60 个交易日 + 节假日
    end = (date.fromisoformat(after_date) + timedelta(days=days_offset + 30)).isoformat()
    rows = await get_daily_history(code, after_date, end)
    if not rows or len(rows) <= days_offset:
        return None
    target = rows[days_offset]   # 0=after_date 本身, 1=次日, ...
    return (str(target['trade_date']), float(target['open_price']), float(target['close']))


async def refresh_outcome_for_event(event: dict) -> None:
    """
    为一条事件填充 buy_price、ret_5d/10d/30d/60d、peak_ret/trough_ret。
    status 进展：
      pending  : 还没拿到 buy_price
      partial  : 部分窗口已到期
      completed: 60d 已到期
      no_data  : 信号日后股票没有数据（停牌/退市等）
    """
    strategy = event['strategy']
    code = event['code']
    signal_date = str(event['signal_date'])

    # 1) 入场基线：signal_date 的次交易日 open 价
    buy_info = await _next_trading_day_close(code, signal_date, days_offset=1)
    if buy_info is None:
        await update_outcome(strategy, code, signal_date, status='no_data')
        return
    buy_date, buy_open, _ = buy_info

    # 2) 拉到 buy_date 之后的全部数据（取 90 自然天 ≈ 60 交易日）
    end = (date.fromisoformat(buy_date) + timedelta(days=120)).isoformat()
    rows = await get_daily_history(code, buy_date, end)
    if not rows:
        await update_outcome(strategy, code, signal_date,
                             buy_price=buy_open, buy_date=buy_date,
                             bars_seen=0, status='no_data')
        return

    # 3) 计算 ret_Nd（N 之后的 close 相对 buy_open 的变化）
    def _ret_at(n):
        if len(rows) > n:
            close_n = float(rows[n]['close'])
            return (close_n / buy_open - 1.0)
        return None

    ret_5d  = _ret_at(5)
    ret_10d = _ret_at(10)
    ret_30d = _ret_at(30)
    ret_60d = _ret_at(60)

    # 4) 60 交易日内极值（用 high/low 各扫一次）
    window = rows[: min(60, len(rows))]
    if window:
        # 注意：buy_date 当日的 high/low 也参与
        peaks = [(float(r['high']) / buy_open - 1.0, str(r['trade_date'])) for r in window]
        troughs = [(float(r['low']) / buy_open - 1.0, str(r['trade_date'])) for r in window]
        peak_ret, peak_date = max(peaks, key=lambda x: x[0])
        trough_ret, trough_date = min(troughs, key=lambda x: x[0])
    else:
        peak_ret = trough_ret = None
        peak_date = trough_date = None

    bars_seen = len(rows)
    if bars_seen >= 60:
        status = 'completed'
    elif bars_seen >= 5:
        status = 'partial'
    else:
        status = 'pending'

    await update_outcome(strategy, code, signal_date,
                         buy_price=buy_open, buy_date=buy_date,
                         ret_5d=ret_5d, ret_10d=ret_10d, ret_30d=ret_30d, ret_60d=ret_60d,
                         peak_ret=peak_ret, trough_ret=trough_ret,
                         peak_date=peak_date, trough_date=trough_date,
                         bars_seen=bars_seen, status=status)


async def refresh_all_pending(strategy: str, batch_size: int = 10000) -> tuple[int, int]:
    """循环处理直到所有 pending/partial 事件都被处理过一次。
    用 seen 集合避免同一事件因状态保持 pending/partial 而被重复处理。"""
    seen: set = set()
    total_done = 0
    round_no = 0
    while True:
        pending = await list_pending(strategy, limit=batch_size)
        new_events = [ev for ev in pending
                       if (ev['code'], str(ev['signal_date'])) not in seen]
        if not new_events:
            break
        round_no += 1
        logger.info(f"  批 {round_no}: 拿到 {len(new_events)} 条新事件（剩余 {len(pending) - len(new_events)} 已处理过）")
        for i, ev in enumerate(new_events, 1):
            try:
                await refresh_outcome_for_event(ev)
                total_done += 1
            except Exception as e:
                logger.debug(f"  [{ev['code']} {ev['signal_date']}] 失败: {e}")
            seen.add((ev['code'], str(ev['signal_date'])))
            if i % 500 == 0:
                logger.info(f"    批内 {i}/{len(new_events)}（累计 {total_done}）")
    return total_done, total_done


# ──────────────────────────────────────────────────────────
# 3. 工具：清空（仅在 --reset 时调用）
# ──────────────────────────────────────────────────────────

async def reset_all(strategy: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM pattern_outcome WHERE strategy=%s", (strategy,))
            return cur.rowcount


# ──────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────

async def main(args):
    if args.reset:
        n = await reset_all(args.strategy)
        logger.info(f"[reset] 已清空 {n} 条 strategy={args.strategy} 事件")

    if args.bootstrap:
        logger.info(f"[bootstrap] 从 scan_results 提取 {args.strategy} 历史事件...")
        n_ev = await bootstrap_from_scan_results(args.strategy)
        logger.info(f"[bootstrap] 写入/更新 {n_ev} 条事件")

    if args.update:
        logger.info(f"[update] 刷新所有 pending/partial 事件的 outcome...")
        n_done, n_total = await refresh_all_pending(args.strategy)
        logger.info(f"[update] 完成 {n_done}/{n_total} 条")

    # 简要统计
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT status, COUNT(*) FROM pattern_outcome
                WHERE strategy=%s GROUP BY status
            """, (args.strategy,))
            stats = dict(await cur.fetchall())
    logger.info(f"[stats] {args.strategy}: " + ' '.join(f'{k}={v}' for k, v in stats.items()))

    await close_pool()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Pattern outcome tracker')
    ap.add_argument('--strategy', default='major_capital_accumulation')
    ap.add_argument('--bootstrap', action='store_true',
                    help='从 scan_results.signal_dates 提取历史事件')
    ap.add_argument('--update', action='store_true',
                    help='刷新 pending/partial 事件的 5/10/30/60 日收益')
    ap.add_argument('--reset', action='store_true',
                    help='删除该策略所有事件（危险）')
    args = ap.parse_args()
    if not (args.bootstrap or args.update or args.reset):
        ap.error("至少指定一个动作：--bootstrap / --update / --reset")
    asyncio.run(main(args))
