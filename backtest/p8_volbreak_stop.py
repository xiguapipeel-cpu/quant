"""
p8_volbreak_stop.py — 「放量跌破建仓平台下沿」候选离场规则 vs 现有止损 对比验证
================================================================================
专业建议3：放量跌破均线粘合区下沿（生命线）= 变盘向下，应止损。
现有离场体系：max(ATR trail, ATR 硬止损, MA20 连续跌破) + 单笔 -10% 硬止损。

本脚本对一批已离场样本（position_monitor）重放持仓期，对比：
  • 现有规则：已记录的 exit_date / exit_pnl_pct
  • 候选规则：持仓期内首次「收盘跌破 MA(PLATFORM_MA) 平台下沿 且 当日放量」

⚠️ 核心验证（避免重蹈 CLAUDE.md P0#2 lock_floor 被 trail 包络、4 折 OOS 全 0 触发的覆辙）：
  1. 候选规则到底触发多少笔？（触发率）
  2. 其中**真正早于**现有离场日的占比？（更早=有增量价值）
  3. 被现有止损包络（不早于现有离场日）的占比？（=无效，加了等于没加）
  4. 在「更早触发」的样本上，候选规则离场 PnL 是否优于现有？（更早离场是否避开了后续下跌）

用法：
  python -m backtest.p8_volbreak_stop                 # 默认采样 3000 笔
  python -m backtest.p8_volbreak_stop --limit 11502   # 全样本
  python -m backtest.p8_volbreak_stop --ma 30 --vol 1.5
"""
import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.stock_dao import get_daily_history
from utils.logger import setup_logger

logger = setup_logger("p8_volbreak")

PLATFORM_MA = 60     # 建仓平台下沿 = 核心均线（生命线），区别于现有 MA20 离场
VOL_MULT = 1.5       # 放量：当日量 > 近 20 日均量 × 该值
VOL_AVG_WIN = 20


def _sma(arr, n, i):
    if i < n - 1 or any(v is None for v in arr[i - n + 1:i + 1]):
        return None
    return sum(arr[i - n + 1:i + 1]) / n


def find_volbreak_exit(bars, entry_date, platform_ma, vol_mult):
    """持仓期内首次「收盘跌破 MA(platform_ma) 且 当日放量」。
    返回 (trigger_date, trigger_close) 或 None。bars 含 entry 前历史用于算 MA。"""
    closes = [float(b["close"]) for b in bars]
    vols = [float(b.get("volume") or 0) for b in bars]
    dates = [str(b["trade_date"]) for b in bars]
    for i in range(len(bars)):
        if dates[i] < entry_date:   # 只看入场日及之后
            continue
        ma = _sma(closes, platform_ma, i)
        avgv = _sma(vols, VOL_AVG_WIN, i)
        if ma is None or avgv is None or avgv <= 0:
            continue
        if closes[i] < ma and vols[i] > avgv * vol_mult:
            return dates[i], closes[i]
    return None


async def main(args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT code, entry_date, entry_price, exit_date, exit_pnl_pct
                FROM position_monitor
                WHERE strategy='major_capital_accumulation' AND status='exited'
                  AND entry_date IS NOT NULL AND exit_date IS NOT NULL
                ORDER BY exit_date DESC LIMIT %s
            """, (args.limit,))
            rows = await cur.fetchall()

    n = len(rows)
    logger.info(f"[p8] 对比样本 {n} 笔 | 平台下沿=MA{args.ma} 放量={args.vol}x")

    triggered = 0          # 候选规则触发
    earlier = 0            # 候选早于现有离场日
    enveloped = 0          # 候选不早于现有（被包络）
    no_trigger = 0         # 候选未触发
    earlier_new_pnls, earlier_old_pnls = [], []   # 更早触发样本：新 vs 旧 PnL
    no_trigger_old_pnls = []                       # 候选漏掉的样本：现有 PnL

    done = 0
    for code, ed, ep, xd, old_pnl in rows:
        done += 1
        if done % 1000 == 0:
            logger.info(f"  进度 {done}/{n}")
        entry_date = str(ed); exit_date = str(xd)
        entry_price = float(ep)
        old_pnl = float(old_pnl) if old_pnl is not None else None
        start = (date.fromisoformat(entry_date) - timedelta(days=120)).isoformat()
        end = (date.fromisoformat(exit_date) + timedelta(days=5)).isoformat()
        bars = await get_daily_history(code, start, end)
        if not bars:
            continue
        res = find_volbreak_exit(bars, entry_date, args.ma, args.vol)
        if res is None:
            no_trigger += 1
            if old_pnl is not None:
                no_trigger_old_pnls.append(old_pnl)
            continue
        trig_date, trig_close = res
        triggered += 1
        if trig_date < exit_date:
            earlier += 1
            new_pnl = (trig_close - entry_price) / entry_price if entry_price else None
            if new_pnl is not None and old_pnl is not None:
                earlier_new_pnls.append(new_pnl)
                earlier_old_pnls.append(old_pnl)
        else:
            enveloped += 1

    def pct(v): return "—" if v is None else f"{v*100:+.2f}%"
    def avg(xs): return mean(xs) if xs else None

    print("\n" + "=" * 64)
    print(f"候选规则：放量({args.vol}x)跌破 MA{args.ma} 平台下沿  |  样本 {n} 笔")
    print("=" * 64)
    print(f"候选规则触发:        {triggered:5d} ({triggered/n*100:.1f}%)")
    print(f"  ├─ 早于现有离场:   {earlier:5d} ({earlier/n*100:.1f}%)  ← 有增量价值")
    print(f"  └─ 被现有止损包络: {enveloped:5d} ({enveloped/n*100:.1f}%)  ← 加了等于没加")
    print(f"候选规则未触发:      {no_trigger:5d} ({no_trigger/n*100:.1f}%)")
    print("-" * 64)
    print("【更早触发的样本】候选离场 vs 现有离场 PnL 对比：")
    if earlier_new_pnls:
        an, ao = avg(earlier_new_pnls), avg(earlier_old_pnls)
        better = sum(1 for a, b in zip(earlier_new_pnls, earlier_old_pnls) if a > b)
        print(f"  候选规则均 PnL: {pct(an)}   现有规则均 PnL: {pct(ao)}   差: {pct(an-ao)}")
        print(f"  候选更优笔数: {better}/{len(earlier_new_pnls)} ({better/len(earlier_new_pnls)*100:.0f}%)")
        verdict = "✅ 更早离场避开了后续下跌，有增量价值" if an > ao else "⚠️ 更早离场反而离在低点（砍在地板），损害收益"
        print(f"  结论: {verdict}")
    else:
        print("  无更早触发样本。")
    print("-" * 64)
    if no_trigger_old_pnls:
        print(f"【候选未触发的样本】现有规则均 PnL: {pct(avg(no_trigger_old_pnls))}（这些靠现有止损处理，候选无意见）")
    print("=" * 64)
    # 总评
    if triggered == 0:
        print("总评：候选规则 0 触发 —— 被现有止损完全包络，重蹈 P0#2 覆辙，不采纳。")
    elif earlier / max(triggered, 1) < 0.2:
        print(f"总评：候选触发中仅 {earlier/triggered*100:.0f}% 早于现有止损，绝大多数被包络，增量极小，不建议采纳。")
    elif earlier_new_pnls and avg(earlier_new_pnls) <= avg(earlier_old_pnls):
        print("总评：候选虽更早触发，但更早离场反而损害收益（砍在地板），不建议采纳。")
    else:
        print("总评：候选规则有实质增量（更早触发且改善收益），值得进一步 walk-forward 验证。")
    print("=" * 64)
    await close_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=3000, help="对比样本数（默认3000，最多11502）")
    ap.add_argument("--ma", type=int, default=PLATFORM_MA, help="平台下沿核心均线（默认60）")
    ap.add_argument("--vol", type=float, default=VOL_MULT, help="放量倍数（默认1.5）")
    args = ap.parse_args()
    asyncio.run(main(args))
