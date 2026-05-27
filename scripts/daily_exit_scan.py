"""
daily_exit_scan.py — 持仓离场扫描 + 真实持仓推送
=====================================================

对 position_monitor 中所有 status='open' 的持仓，跑 tight_trail 离场逻辑：
  - ATR 自适应 trail（越赚越紧）：stage1 k=2.0 / stage2 k=1.5 / stage3 k=1.0
  - ATR 硬止损：-2 × ATR（仅在峰值 ≤ +2% 时生效，防 gap-down）
  - 单笔最大亏损硬限：收盘亏损 ≥ 10% 强制离场
  - MA20 反转：连续 N 日跌破 MA20

命中后：写 exit_*；若 is_real=1 → 推送企业微信。

用法：
  # 增量扫描（每天一次）
  python -m scripts.daily_exit_scan

  # 一次性扫所有历史已开仓位（包括很久前的）
  python -m scripts.daily_exit_scan --backfill
"""
import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.stock_dao import get_daily_history
from db.position_dao import (
    list_open, update_trail_state, mark_exited,
    list_pending_push, mark_notified,
    list_pending_actual_fill, fill_actual_exit,
)
from config.execution_rules import MAX_SINGLE_LOSS_PCT
from utils.logger import setup_logger

logger = setup_logger("exit_scan")

STRATEGY_ID = "major_capital_accumulation"

# tight_trail 配方（与 bt_major_capital.py 默认一致）
ATR_STOP_K        = 2.0
ATR_TRAIL_K       = 2.0   # stage1
TRAIL_STAGE2_GAIN = 0.05
TRAIL_STAGE2_K    = 1.5
TRAIL_STAGE3_GAIN = 0.15
TRAIL_STAGE3_K    = 1.0
ATR_PERIOD        = 20
MA20_PERIOD       = 20
MA_EXIT_DAYS      = 5    # 连续 N 日收盘跌破 MA20
MA_EXIT_GRACE     = 10   # 入场后宽限期内 MA20 反转不算


def calc_atr(bars: list[dict], period: int = 14) -> Optional[float]:
    """TR 简化版（高-低）的 SMA。Wilder 平滑可后续升级。"""
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(-period, 0):
        h = float(bars[i]['high'])
        l = float(bars[i]['low'])
        pc = float(bars[i - 1]['close']) if i - 1 >= -len(bars) else float(bars[i]['close'])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return sum(trs) / period


def calc_ma(bars: list[dict], period: int = 20) -> Optional[float]:
    if len(bars) < period:
        return None
    return sum(float(b['close']) for b in bars[-period:]) / period


def check_exit(
    entry_price: float,
    highest_price: float,
    bars_since_entry: list[dict],   # 入场日起的 bars（含入场日 bar）
    full_bars: list[dict],           # 完整 bars（含入场前历史，给 ATR/MA 计算）
) -> tuple[bool, str, float, int]:
    """
    返回 (exited, reason, exit_price, days_below_ma)。
    exit_price = 触发当日 close。
    """
    if not bars_since_entry:
        return False, "", 0.0, 0

    today = bars_since_entry[-1]
    close = float(today['close'])
    days_held = len(bars_since_entry) - 1   # 不含入场当日（次开盘 = 第一根 bar）

    # 1. ATR：用入场之前的历史 bars 估计当下 ATR
    atr = calc_atr(full_bars, period=ATR_PERIOD)
    if not atr or atr <= 0:
        return False, "", 0.0, 0

    # 2. 当前浮盈
    cur_gain = (close - entry_price) / entry_price

    # 2.5 单笔最大亏损硬限：执行层风控，不改变入场形态
    if cur_gain <= -MAX_SINGLE_LOSS_PCT:
        return True, f"单笔硬止损({MAX_SINGLE_LOSS_PCT:.0%}): {cur_gain:+.1%}", close, 0

    # 3. 动态 trail k（越赚越紧）
    if cur_gain >= TRAIL_STAGE3_GAIN:
        dyn_k = TRAIL_STAGE3_K
        stage = 3
    elif cur_gain >= TRAIL_STAGE2_GAIN:
        dyn_k = TRAIL_STAGE2_K
        stage = 2
    else:
        dyn_k = ATR_TRAIL_K
        stage = 1

    # 4. 三道止损线
    trail_line = highest_price - dyn_k * atr
    hard_line  = entry_price - ATR_STOP_K * atr
    stop_line  = max(trail_line, hard_line)

    if close <= stop_line:
        # 区分硬止损 vs trail
        if close <= hard_line and highest_price <= entry_price * 1.02:
            return True, f"ATR硬止损(k={ATR_STOP_K}): {cur_gain:+.1%}", close, 0
        else:
            dd = (highest_price - close) / highest_price if highest_price > 0 else 0
            return True, f"ATR追踪止损 stage{stage}(k={dyn_k}) 回撤{dd:.1%} 收益{cur_gain:+.1%}", close, 0

    # 5. MA20 反转（持仓 > grace 天，连续 N 日跌破）
    days_below_ma = 0
    if days_held > MA_EXIT_GRACE:
        # 计算最近 N 日是否都跌破 MA20
        ma20 = calc_ma(full_bars, period=MA20_PERIOD)
        if ma20:
            # 检查最近 MA_EXIT_DAYS 天每一日的 close 是否都低于当日的 MA20
            # 简化：只看当日；连续判断从外部状态读
            if close < ma20:
                # 重建 days_below_ma：往回查 bars_since_entry 末尾连续多少日 close < ma_at_that_day
                for i in range(len(bars_since_entry) - 1, -1, -1):
                    bar = bars_since_entry[i]
                    # 用 full_bars 算 bar 当时的 MA20
                    bar_idx_full = len(full_bars) - (len(bars_since_entry) - 1 - i) - 1
                    if bar_idx_full < MA20_PERIOD - 1:
                        break
                    ma_at_bar = sum(float(full_bars[j]['close']) for j in range(bar_idx_full - MA20_PERIOD + 1, bar_idx_full + 1)) / MA20_PERIOD
                    if float(bar['close']) < ma_at_bar:
                        days_below_ma += 1
                    else:
                        break
                if days_below_ma >= MA_EXIT_DAYS:
                    return True, f"破MA20连续{days_below_ma}日 收益{cur_gain:+.1%}", close, days_below_ma

    return False, "", 0.0, 0


async def scan_one_position(pos: dict) -> Optional[dict]:
    """对一笔持仓跑离场检查；返回 None 表示无触发，否则返回 {exit_*} dict"""
    code = pos['code']
    entry_date = str(pos['entry_date'])
    entry_price = float(pos['entry_price'])

    # 拉数据：入场前 60 自然日 ~ 今日
    start = (date.fromisoformat(entry_date) - timedelta(days=60)).isoformat()
    end = date.today().isoformat()
    rows = await get_daily_history(code, start, end)
    if not rows:
        return None

    # 分割：入场前 + 入场后
    bars_since_entry = [b for b in rows if str(b['trade_date']) >= entry_date]
    if not bars_since_entry:
        return None

    # 跑过去每一天检查离场（用于回填；增量时只看今日）
    highest = float(bars_since_entry[0]['high'])
    highest_date = str(bars_since_entry[0]['trade_date'])
    lowest = float(bars_since_entry[0]['low'])
    lowest_date = str(bars_since_entry[0]['trade_date'])

    last_check = pos.get('last_check_date')
    last_check_iso = str(last_check) if last_check else entry_date

    for i, bar in enumerate(bars_since_entry):
        bd = str(bar['trade_date'])
        # 滚动更新 highest / lowest
        if float(bar['high']) > highest:
            highest = float(bar['high'])
            highest_date = bd
        if float(bar['low']) < lowest:
            lowest = float(bar['low'])
            lowest_date = bd

        # 只在 last_check_date 之后做离场判断（避免重复跑）
        if bd <= last_check_iso:
            continue

        # 截断：到当前 bar 的全部历史
        full_bars_so_far = rows[: rows.index(bar) + 1]
        bars_since_entry_so_far = bars_since_entry[: i + 1]

        exited, reason, exit_price, dbm = check_exit(
            entry_price=entry_price,
            highest_price=highest,
            bars_since_entry=bars_since_entry_so_far,
            full_bars=full_bars_so_far,
        )
        if exited:
            pnl = (exit_price - entry_price) / entry_price
            return {
                'highest_price': highest, 'highest_date': highest_date,
                'lowest_price':  lowest,  'lowest_date':  lowest_date,
                'days_held': i,
                'exit_date': bd,
                'exit_price': exit_price,
                'exit_reason': reason,
                'exit_pnl_pct': pnl,
                'last_check_date': bd,
            }

    # 未触发离场 — 只更新 trail 状态
    return {
        'highest_price': highest, 'highest_date': highest_date,
        'lowest_price':  lowest,  'lowest_date':  lowest_date,
        'days_held': len(bars_since_entry) - 1,
        'last_check_date': str(bars_since_entry[-1]['trade_date']),
        'still_open': True,
    }


async def main(args):
    logger.info(f"[exit-scan] 开始扫描 status='open' 持仓")
    open_positions = await list_open(STRATEGY_ID)
    logger.info(f"[exit-scan] 开仓中: {len(open_positions)} 笔（{sum(1 for p in open_positions if p['is_real']==1)} 真实）")

    exited_count = 0
    still_open_count = 0

    for pos in open_positions:
        result = await scan_one_position(pos)
        if result is None:
            continue
        if result.get('still_open'):
            await update_trail_state(
                position_id=pos['id'],
                highest_price=result['highest_price'], highest_date=result['highest_date'],
                lowest_price=result['lowest_price'], lowest_date=result['lowest_date'],
                days_held=result['days_held'],
                last_check_date=result['last_check_date'],
            )
            still_open_count += 1
        else:
            # 先更新 trail 状态
            await update_trail_state(
                position_id=pos['id'],
                highest_price=result['highest_price'], highest_date=result['highest_date'],
                lowest_price=result['lowest_price'], lowest_date=result['lowest_date'],
                days_held=result['days_held'],
                last_check_date=result['last_check_date'],
            )
            # 标记离场
            await mark_exited(
                position_id=pos['id'],
                exit_date=result['exit_date'],
                exit_price=result['exit_price'],
                exit_reason=result['exit_reason'],
                exit_pnl_pct=result['exit_pnl_pct'],
            )
            exited_count += 1
            if pos['is_real']:
                logger.info(f"  🔴 真实持仓离场: {pos['code']} {pos['name']} {result['exit_reason']} pnl={result['exit_pnl_pct']:+.2%}")
            else:
                logger.debug(f"  ⚪ 模拟持仓离场: {pos['code']} pnl={result['exit_pnl_pct']:+.2%}")

    logger.info(f"[exit-scan] 完成: 离场 {exited_count} 笔, 仍开仓 {still_open_count} 笔")

    # ── 用次日 open 填充实际成交价（方案 C 核心逻辑） ──
    pending_actual = await list_pending_actual_fill(STRATEGY_ID)
    n_actual_filled = 0
    if pending_actual:
        logger.info(f"[exit-scan] 处理 {len(pending_actual)} 笔待填充实际成交价")
        for pos in pending_actual:
            sig_exit_date = str(pos['exit_date'])
            entry_price = float(pos['entry_price'])
            # 找信号日次日 open
            try:
                # 拉信号日次日开始 ~ 15 天范围
                start = (date.fromisoformat(sig_exit_date) + timedelta(days=1)).isoformat()
                end = (date.fromisoformat(sig_exit_date) + timedelta(days=15)).isoformat()
                rows = await get_daily_history(pos['code'], start, end)
                next_bar = rows[0] if rows else None
                if next_bar:
                    actual_date = str(next_bar['trade_date'])
                    actual_price = float(next_bar['open_price'])
                    actual_pnl = (actual_price - entry_price) / entry_price
                    await fill_actual_exit(pos['id'], actual_date, actual_price, actual_pnl)
                    n_actual_filled += 1
            except Exception as e:
                logger.debug(f"  fill_actual {pos['code']} 失败: {e}")
        logger.info(f"[exit-scan] 已填充 {n_actual_filled}/{len(pending_actual)} 笔实际成交价")

    # ── 推送：真实持仓离场，未推送过 ──
    pending = await list_pending_push(STRATEGY_ID)
    if pending:
        logger.info(f"[exit-scan] 推送 {len(pending)} 笔真实持仓离场")
        try:
            from notifications.push import pusher
            for pos in pending:
                ok = await _push_exit(pusher, pos)
                if ok:
                    await mark_notified(pos['id'])
        except Exception as e:
            logger.error(f"[exit-scan] 推送异常: {e}")

    await close_pool()


async def _push_exit(pusher, pos: dict) -> bool:
    """推送单笔真实持仓离场（pusher.send_exit_signal 处理具体渠道）"""
    try:
        res = await pusher.send_exit_signal(pos)
        ok = any(v.get('ok') for v in res.values() if isinstance(v, dict))
        logger.info(f"[push] {pos['code']} {pos['name']} 离场推送: {res}")
        return ok
    except Exception as e:
        logger.warning(f"[push] {pos['code']} 失败: {e}")
        return False


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--backfill', action='store_true',
                    help='跑所有历史已开仓位（清零 last_check_date，强制重新计算）')
    args = ap.parse_args()

    if args.backfill:
        async def _reset():
            pool = await get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE position_monitor SET last_check_date=NULL WHERE strategy=%s AND status='open'",
                        (STRATEGY_ID,),
                    )
                    logger.info(f"[backfill] 已清零 {cur.rowcount} 笔的 last_check_date")
            await close_pool()
        asyncio.run(_reset())

    asyncio.run(main(args))
