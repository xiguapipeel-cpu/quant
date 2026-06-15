"""rank_score 选股质量审计（只读）—— top-3 选中的有没有比落选的更好？

依赖 buy_meta 落库（2026-06-15 起 signal_meta 含 rsi/yy_ratio/bb_narrow/watch_days/
breakout_strength/amount_wan）。对每个交易日的全部 BUY 信号用生产 `rank_score` 打分排序，
取 top-MAX_NEW_ENTRIES_PER_DAY 为「选中」、其余为「落选」，对比后续 peak_ret/ret_30d。

  rank_score 有效  ⇔  选中组后续收益显著 > 落选组。
  rank_score 无效  ⇔  两组无差异（= 每天盲选 top-3，排序权重需重写）。

用法: ./venv/bin/python -m scripts.rank_quality_audit [--start 2024-09-01] [--end 2026-06-30]
只读。
"""
import argparse
import asyncio
import json
import statistics as st
from collections import defaultdict

from db.mysql_pool import get_pool
from config.execution_rules import rank_score, MAX_NEW_ENTRIES_PER_DAY


def build_stock(confidence, meta):
    return {"confidence": confidence or 0,
            "buy_meta": meta or {},
            "amount_wan": (meta or {}).get("amount_wan")}


async def main(start, end):
    pool = await get_pool()
    by_day = defaultdict(list)   # signal_date -> [(score, peak, ret30, code)]
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await cur.execute(
                "SELECT code, signal_date, confidence, signal_meta, peak_ret, ret_30d "
                "FROM pattern_outcome WHERE signal_type='BUY' "
                "AND signal_date BETWEEN %s AND %s", (start, end))
            for code, sd, conf, meta_s, peak, ret30 in await cur.fetchall():
                meta = json.loads(meta_s) if meta_s else {}
                if meta.get("rsi") is None:
                    continue   # 无 rank 因子，跳过
                sc = rank_score(build_stock(float(conf or 0), meta))
                by_day[str(sd)].append((
                    sc,
                    float(peak) * 100 if peak is not None else None,
                    float(ret30) * 100 if ret30 is not None else None,
                    code,
                ))

    N = MAX_NEW_ENTRIES_PER_DAY
    sel_peak, sel_ret, rej_peak, rej_ret = [], [], [], []
    contested_days = 0   # 当日信号数 > N，排序才真正起作用
    for sd, rows in by_day.items():
        if len(rows) <= N:
            continue
        contested_days += 1
        rows.sort(key=lambda x: -x[0])
        sel, rej = rows[:N], rows[N:]
        for _, pk, r30, _c in sel:
            if pk is not None: sel_peak.append(pk)
            if r30 is not None: sel_ret.append(r30)
        for _, pk, r30, _c in rej:
            if pk is not None: rej_peak.append(pk)
            if r30 is not None: rej_ret.append(r30)

    def m(v): return st.mean(v) if v else float("nan")
    def wr(v): return (sum(1 for x in v if x > 0) / len(v) * 100) if v else float("nan")
    def big(v): return (sum(1 for x in v if x >= 15) / len(v) * 100) if v else float("nan")

    print("=" * 68)
    print(f"rank_score 选股质量审计 | BUY {start} ~ {end}")
    print(f"有 rank 因子的交易日 {len(by_day)} | 信号>{N} 的「有竞争」日 {contested_days}")
    print(f"top-{N} 选中样本 {len(sel_ret)} | 落选样本 {len(rej_ret)}")
    print("=" * 68)
    print(f"  {'组':<14}{'30d胜率':>9}{'30d均收益':>11}{'峰值均值':>10}{'峰值≥15%':>10}")
    print(f"  {'选中 top-'+str(N):<14}{wr(sel_ret):>8.1f}%{m(sel_ret):>+10.2f}%"
          f"{m(sel_peak):>+9.2f}%{big(sel_peak):>9.1f}%")
    print(f"  {'落选 rank≥'+str(N+1):<14}{wr(rej_ret):>8.1f}%{m(rej_ret):>+10.2f}%"
          f"{m(rej_peak):>+9.2f}%{big(rej_peak):>9.1f}%")
    print("─" * 68)
    if sel_ret and rej_ret:
        d_ret = m(sel_ret) - m(rej_ret)
        d_peak = m(sel_peak) - m(rej_peak)
        print(f"  选中−落选: 30d均收益 {d_ret:+.2f}pp | 峰值 {d_peak:+.2f}pp")
        if d_ret > 1.0:
            print("  ✅ rank_score 有效：选中组后续显著更好")
        elif d_ret > 0:
            print("  ◐ rank_score 弱有效：方向对但边际小")
        else:
            print("  ⚠️ rank_score 无效/反向：top-3 没挑出更好的，权重需重写")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-09-01")
    ap.add_argument("--end", default="2026-06-30")
    args = ap.parse_args()
    asyncio.run(main(args.start, args.end))
