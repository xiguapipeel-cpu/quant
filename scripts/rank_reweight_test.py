"""权重重排验证（只读）—— 新 RANK_WEIGHTS 能否把 top-3 从「反向」翻成「正向」？

承接 rank_factor_audit：watch_days/amount_wan 反向、yy_ratio/breakout_strength 有效、
confidence/rsi/bb_narrow 中性。本脚本用旧权重 vs 候选新权重各自对每个交易日的 BUY
信号排序，比较 top-3 选中组 vs 落选组的后续 ret_30d，看新权重是否使选中组反超。

用法: ./venv/bin/python -m scripts.rank_reweight_test
只读，不改 config。验证通过后再决定是否写入 RANK_WEIGHTS。
"""
import asyncio
import json
import statistics as st
from collections import defaultdict

from db.mysql_pool import get_pool
from config.execution_rules import RANK_WEIGHTS, _norm, MAX_NEW_ENTRIES_PER_DAY

# 候选新权重：砍掉反向(watch_days/amount_wan)，集中到有效因子(yy_ratio/breakout_strength)，
# confidence/rsi 中性故大幅下调，bb_narrow 弱正保留小权重。amount_wan 流动性已由
# screener.min_amount_wan 硬过滤兜底，不再进排序。
NEW_WEIGHTS = {
    "confidence":        0.10,
    "rsi":               0.00,
    "watch_days":        0.00,
    "yy_ratio":          0.45,
    "bb_narrow":         0.05,
    "breakout_strength": 0.40,
    "amount_wan":        0.00,
}


def score_with(meta, confidence, w):
    rsi = meta.get("rsi")
    watch_days = meta.get("watch_days") or meta.get("accumulation_days")
    return (
        w["confidence"] * _norm(confidence, 0.3, 1.0)
        + w["rsi"] * _norm(rsi, 55.0, 75.0)
        + w["watch_days"] * _norm(watch_days, 15.0, 40.0)
        + w["yy_ratio"] * _norm(meta.get("yy_ratio"), 1.0, 1.5)
        + w["bb_narrow"] * (1.0 if meta.get("bb_narrow") else 0.0)
        + w["breakout_strength"] * _norm(meta.get("breakout_strength"), 3.0, 8.0)
        + w["amount_wan"] * _norm(meta.get("amount_wan"), 300.0, 5000.0)
    )


async def main():
    pool = await get_pool()
    by_day = defaultdict(list)   # sd -> [(meta, conf, ret30)]
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await cur.execute(
                "SELECT signal_date, confidence, signal_meta, ret_30d FROM pattern_outcome "
                "WHERE signal_type='BUY' AND ret_30d IS NOT NULL "
                "AND signal_date BETWEEN '2024-09-01' AND '2026-06-30'")
            for sd, conf, meta_s, ret30 in await cur.fetchall():
                meta = json.loads(meta_s) if meta_s else {}
                if meta.get("rsi") is None:
                    continue
                by_day[str(sd)].append((meta, float(conf or 0), float(ret30) * 100))

    N = MAX_NEW_ENTRIES_PER_DAY

    def evaluate(w):
        sel, rej = [], []
        for sd, rows in by_day.items():
            if len(rows) <= N:
                continue
            ranked = sorted(rows, key=lambda r: -score_with(r[0], r[1], w))
            sel += [r[2] for r in ranked[:N]]
            rej += [r[2] for r in ranked[N:]]
        return sel, rej

    def line(label, sel, rej):
        sm, rm = st.mean(sel), st.mean(rej)
        sw = sum(1 for x in sel if x > 0) / len(sel) * 100
        rw = sum(1 for x in rej if x > 0) / len(rej) * 100
        print(f"  {label:<10} 选中 {sm:+.2f}% (胜率{sw:.1f}%) | 落选 {rm:+.2f}% (胜率{rw:.1f}%) "
              f"| 选中−落选 {sm-rm:+.2f}pp")
        return sm - rm

    # 反号权重：watch_days/amount_wan 按实测反向给负权重（奖励新鲜+小票）
    INV_WEIGHTS = {
        "confidence": 0.05, "rsi": 0.0,
        "watch_days": -0.25, "yy_ratio": 0.35,
        "bb_narrow": 0.05, "breakout_strength": 0.30, "amount_wan": -0.25,
    }

    print("=" * 72)
    print("权重重排验证 | top-3 选中 vs 落选 后续 30d 收益 | 2024-09~2026-06")
    print("=" * 72)
    do = line("旧权重", *evaluate(RANK_WEIGHTS))
    dn = line("新权重", *evaluate(NEW_WEIGHTS))
    di = line("反号权重", *evaluate(INV_WEIGHTS))
    print("─" * 72)
    print(f"  反号方案: {INV_WEIGHTS}")
    print(f"  选中−落选差: 旧 {do:+.2f}pp → 新 {dn:+.2f}pp  (改善 {dn-do:+.2f}pp)")
    if dn > 0 and dn > do:
        print("  ✅ 新权重使 top-3 反超落选 → 可写入 RANK_WEIGHTS")
    elif dn > do:
        print("  ◐ 新权重改善但 top-3 仍未反超 → 继续调或考虑反号 watch_days/amount_wan")
    else:
        print("  ⚠️ 新权重未改善")
    print(f"\n  新权重方案: {NEW_WEIGHTS}")


if __name__ == "__main__":
    asyncio.run(main())
