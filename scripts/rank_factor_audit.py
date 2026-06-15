"""逐因子诊断（只读）—— rank_score 的每个因子与后续收益是正相关还是反相关？

承接 rank_quality_audit 的「rank_score 反向」结论，拆到单因子：对每个 rank 因子，
按因子值排序分三档，比较高档(top tercile) vs 低档(bottom tercile)的后续 ret_30d。
  delta = top − bottom > 0 → 该因子有效（值越高收益越高，权重应保留/加大）
  delta < 0 → 该因子反向（追这个特征反而更差，权重应砍/反号）

用法: ./venv/bin/python -m scripts.rank_factor_audit [--start ...] [--end ...]
只读。
"""
import argparse
import asyncio
import json
import statistics as st

from db.mysql_pool import get_pool

FACTORS = ["rsi", "watch_days", "yy_ratio", "bb_narrow",
           "breakout_strength", "amount_wan", "confidence"]


async def main(start, end):
    pool = await get_pool()
    rows = []   # dict per BUY: {factor: val, ..., ret30}
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await cur.execute(
                "SELECT confidence, signal_meta, ret_30d FROM pattern_outcome "
                "WHERE signal_type='BUY' AND ret_30d IS NOT NULL "
                "AND signal_date BETWEEN %s AND %s", (start, end))
            for conf, meta_s, ret30 in await cur.fetchall():
                meta = json.loads(meta_s) if meta_s else {}
                if meta.get("rsi") is None:
                    continue
                d = {"ret30": float(ret30) * 100, "confidence": float(conf or 0)}
                d["rsi"] = meta.get("rsi")
                d["watch_days"] = meta.get("watch_days") or meta.get("accumulation_days")
                d["yy_ratio"] = meta.get("yy_ratio")
                d["bb_narrow"] = 1.0 if meta.get("bb_narrow") else 0.0
                d["breakout_strength"] = meta.get("breakout_strength")
                d["amount_wan"] = meta.get("amount_wan")
                rows.append(d)

    print("=" * 60)
    print(f"逐因子诊断 | BUY 30d 已到期 {len(rows)} 笔 | {start}~{end}")
    print("=" * 60)
    print(f"  {'因子':<18}{'低档收益':>10}{'高档收益':>10}{'高−低':>9}  判定")
    for f in FACTORS:
        vals = [(r[f], r["ret30"]) for r in rows if r.get(f) is not None]
        if len(vals) < 30:
            continue
        vals.sort(key=lambda x: x[0])
        k = len(vals) // 3
        low = [v[1] for v in vals[:k]]
        high = [v[1] for v in vals[-k:]]
        lo_m, hi_m = st.mean(low), st.mean(high)
        d = hi_m - lo_m
        tag = "✅有效" if d > 1 else ("⚠️反向" if d < -1 else "◐弱/无")
        print(f"  {f:<18}{lo_m:>+9.2f}%{hi_m:>+9.2f}%{d:>+8.2f}pp  {tag}")
    print("─" * 60)
    print("  ✅有效=值越高收益越高(权重保留) | ⚠️反向=追此特征更差(应砍/反号)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-09-01")
    ap.add_argument("--end", default="2026-06-30")
    args = ap.parse_args()
    asyncio.run(main(args.start, args.end))
