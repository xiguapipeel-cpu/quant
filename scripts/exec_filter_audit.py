"""执行过滤审计（只读）—— 量化 evaluate_next_open 是否砍掉右尾。

背景：position_monitor 执行轨从 2026-04 起稀疏，且 buy_meta 未落库 → 无法忠实重建
top-N 排序。但 evaluate_next_open（次日高开>5% 或 跌破信号价 → skip）是确定性的，
只用「信号日收盘 vs 次日开盘」即可复算。本脚本对 pattern_outcome 全部 BUY 信号复算
该过滤的 allow/skip，join 已到期的 peak_ret / ret_30d / trough_ret，回答：

  这个「不追高/不接跌破」的执行过滤，是净帮你（躲掉烂单）还是净害你（砍掉右尾大赢家）？

用法: ./venv/bin/python -m scripts.exec_filter_audit [--start 2024-09-01] [--end 2026-06-30]
只读，不写任何表。
"""
import argparse
import asyncio
import statistics as st

from db.mysql_pool import get_pool
from config.execution_rules import evaluate_next_open


async def main(start: str, end: str):
    pool = await get_pool()
    rows_eval = []   # (code, sd, gap, allowed, peak, ret30, trough)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await cur.execute(
                "SELECT code, name, signal_date, peak_ret, ret_30d, trough_ret "
                "FROM pattern_outcome WHERE signal_type='BUY' "
                "AND signal_date BETWEEN %s AND %s ORDER BY signal_date", (start, end))
            buys = await cur.fetchall()

            for code, name, sd, peak, ret30, trough in buys:
                sd = str(sd)
                # 信号日收盘 + 次日开盘
                await cur.execute(
                    "SELECT trade_date, open_price, close FROM stock_daily "
                    "WHERE code=%s AND trade_date>=%s ORDER BY trade_date LIMIT 2",
                    (code, sd))
                pr = await cur.fetchall()
                if len(pr) < 2:
                    continue
                sig_close = float(pr[0][2])
                nxt_open = float(pr[1][1])
                if sig_close <= 0 or nxt_open <= 0:
                    continue
                allowed, reason, gap = evaluate_next_open(sig_close, nxt_open)
                # peak_ret/ret_30d/trough_ret 库内存小数(0.2674=26.74%) → ×100 转百分
                rows_eval.append((
                    code, sd, gap, allowed,
                    float(peak) * 100 if peak is not None else None,
                    float(ret30) * 100 if ret30 is not None else None,
                    float(trough) * 100 if trough is not None else None,
                ))

    def bucket(rs, key):
        vals = [r[key] for r in rs if r[key] is not None]
        return vals

    allow = [r for r in rows_eval if r[3]]
    skip = [r for r in rows_eval if not r[3]]

    def summ(rs, label):
        ret30 = bucket(rs, 5)
        peak = bucket(rs, 4)
        trough = bucket(rs, 6)
        n = len(rs)
        mat = len(ret30)
        win = sum(1 for v in ret30 if v > 0)
        big = sum(1 for v in peak if v >= 15)     # 后续峰值≥15% = 大赢家潜质
        dis = sum(1 for v in trough if v <= -10)  # 后续谷值≤-10% = 烂单
        print(f"  【{label}】 n={n}  (30日到期 {mat})")
        if mat:
            print(f"    30日胜率 {win/mat*100:.1f}%  平均30日收益 {st.mean(ret30):+.2f}%  "
                  f"中位 {st.median(ret30):+.2f}%")
        if peak:
            print(f"    平均最大浮盈 {st.mean(peak):+.2f}%  峰值≥15%(大赢家) {big}/{len(peak)} "
                  f"= {big/len(peak)*100:.1f}%")
        if trough:
            print(f"    平均最大浮亏 {st.mean(trough):+.2f}%  谷值≤-10%(烂单) {dis}/{len(trough)} "
                  f"= {dis/len(trough)*100:.1f}%")
        return ret30, peak

    print("=" * 70)
    print(f"执行过滤审计 evaluate_next_open | BUY 信号 {start} ~ {end}")
    print(f"可复算 {len(rows_eval)} 笔 (放行 {len(allow)} / skip {len(skip)})")
    print("=" * 70)
    a30, ap = summ(allow, "放行 (allowed)")
    print()
    s30, sp = summ(skip, "被过滤 (skipped)")
    print()
    print("─" * 70)
    print("判读：")
    if a30 and s30:
        d = st.mean(s30) - st.mean(a30)
        print(f"  skip vs 放行 平均30日收益差: {d:+.2f}pp")
        if d > 0:
            print("  ⚠️ 被 skip 的信号 30 日收益反而更高 → 过滤在砍右尾（净害）")
        else:
            print("  ✅ 被 skip 的信号 30 日收益更低 → 过滤躲掉了烂单（净帮）")
    if sp:
        big_skipped = sum(1 for v in sp if v >= 15)
        print(f"  被 skip 但后续峰值≥15% 的「错过大赢家」: {big_skipped} 笔")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-09-01")
    ap.add_argument("--end", default="2026-06-30")
    args = ap.parse_args()
    asyncio.run(main(args.start, args.end))
