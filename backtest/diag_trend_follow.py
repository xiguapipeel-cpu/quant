"""趋势跟踪策略现状诊断 — 不改参数，只剖析当前默认行为。

验证 4 个假设：
  H1 死叉出场是否被 6% trail 包络（触发率≈0 = 死代码）
  H2 6% 固定 trail 是否在砍赢家（看 trail 出场单的 pnl 分布 + 赢家右尾）
  H3 RSI≥50 入场过滤是否冗余（入场 RSI 分布，是否都远高于 50）
  H4 收益结构是否典型趋势跟踪（低胜率高赔率 / 期望值 / profit factor）

用法: ./venv/bin/python -m backtest.diag_trend_follow
"""
import asyncio
import json
import re
import statistics as st
from collections import Counter, defaultdict

from backtest.bt_runner import run_for_web

START = "2024-01-01"
END = "2026-03-31"
CASH = 100_000.0


def classify_exit(reason: str) -> str:
    if "硬止损" in reason:
        return "硬止损-5%"
    if "追踪止损" in reason:
        return "追踪止损-6%"
    if "下穿" in reason or "死叉" in reason:
        return "EMA死叉"
    if "持仓中" in reason:
        return "持仓中"
    return f"其他:{reason[:20]}"


def pct_stats(vals):
    if not vals:
        return {}
    vals = sorted(vals)
    n = len(vals)
    return {
        "n": n,
        "min": round(min(vals), 2),
        "p25": round(vals[int(n * 0.25)], 2),
        "median": round(st.median(vals), 2),
        "mean": round(st.mean(vals), 2),
        "p75": round(vals[int(n * 0.75)], 2),
        "p90": round(vals[int(n * 0.90)], 2),
        "max": round(max(vals), 2),
    }


async def main():
    logs = []
    res = await run_for_web(
        "trend_follow", START, END, CASH,
        log_fn=lambda m: logs.append(m),
        data_source="local_db",
    )
    if "error" in res:
        print("ERROR:", res["error"])
        for l in logs[-10:]:
            print("  ", l)
        return

    m = res["metrics"]
    trades = res["trades_paired"]
    closed = [t for t in trades if t["sell_date"] != "（持仓中）"]
    open_t = [t for t in trades if t["sell_date"] == "（持仓中）"]

    print("=" * 70)
    print(f"趋势跟踪现状诊断 | {START} ~ {END} | 初始 {CASH:,.0f}")
    print("=" * 70)
    print(f"股票池: {m['stock_count']}  | 总收益 {m['total_return']}  年化 {m['annualized_return']}")
    print(f"最大回撤 {m['max_drawdown']}  夏普 {m['sharpe_ratio']}")
    print(f"总交易 {m['total_trades']}  (已平仓 {len(closed)} / 持仓中 {len(open_t)})")
    print(f"胜率 {m['win_rate']}  profit_factor {m['profit_factor']}")
    print()

    # ── 期望值 / 赔率（趋势跟踪核心指标） ──
    wins = [t["pnl_pct"] for t in closed if t["pnl"] > 0]
    losses = [t["pnl_pct"] for t in closed if t["pnl"] < 0]
    if wins and losses:
        avg_w, avg_l = st.mean(wins), st.mean(losses)
        wr = len(wins) / len(closed)
        expectancy = wr * avg_w + (1 - wr) * avg_l
        payoff = avg_w / abs(avg_l)
        print("【H4 收益结构 — 趋势跟踪应为低胜率高赔率】")
        print(f"  平均盈 +{avg_w:.2f}%  平均亏 {avg_l:.2f}%  盈亏比(payoff) {payoff:.2f}")
        print(f"  单笔期望值 {expectancy:+.2f}% / 笔")
        print(f"  最大赢家 +{max(wins):.1f}%  最大输家 {min(losses):.1f}%")
        # 右尾贡献：top3 赢家占总盈利金额比
        win_pnls = sorted([t["pnl"] for t in closed if t["pnl"] > 0], reverse=True)
        top3 = sum(win_pnls[:3])
        total_win = sum(win_pnls)
        print(f"  Top3 赢家贡献 {top3/total_win*100:.0f}% 的总盈利金额"
              f" (共 {len(win_pnls)} 笔盈利)")
    print()

    # ── 出场原因剖析 ──
    print("【H1+H2 出场原因剖析】")
    by_reason = defaultdict(list)
    for t in closed:
        by_reason[classify_exit(t["sell_reason"])].append(t)
    print(f"  {'出场原因':<14}{'笔数':>5}{'占比':>7}{'胜率':>7}{'均收益':>9}{'中位':>8}{'最大赢':>8}")
    for r, ts in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
        pnls = [t["pnl_pct"] for t in ts]
        w = sum(1 for t in ts if t["pnl"] > 0)
        print(f"  {r:<14}{len(ts):>5}{len(ts)/len(closed)*100:>6.0f}%"
              f"{w/len(ts)*100:>6.0f}%{st.mean(pnls):>+8.2f}%"
              f"{st.median(pnls):>+7.2f}%{max(pnls):>+7.1f}%")
    print()
    print("  ↑ H1 验证: 若 EMA死叉 触发率≈0 → 被 6% trail 包络的死代码")
    print("  ↑ H2 验证: 看 追踪止损-6% 出场单的'最大赢'，若高 → trail 在高位砍赢家")
    print()

    # ── 追踪止损出场单的盈利分布（H2 核心） ──
    trail_ts = by_reason.get("追踪止损-6%", [])
    if trail_ts:
        trail_wins = [t["pnl_pct"] for t in trail_ts if t["pnl"] > 0]
        print("【H2 追踪止损出场单 — 是否砍在浮盈高位】")
        print(f"  trail 出场盈利单 pnl% 分布: {pct_stats(trail_wins)}")
        print(f"  其中 ≥10% 离场: {sum(1 for x in trail_wins if x>=10)} 笔  "
              f"≥20%: {sum(1 for x in trail_wins if x>=20)} 笔")
        print()

    # ── 入场 RSI 分布（H3） ──
    rsis = []
    for t in closed + open_t:
        mt = re.search(r"RSI=([\d.]+)", t.get("buy_reason", ""))
        if mt:
            rsis.append(float(mt.group(1)))
    if rsis:
        print("【H3 入场 RSI 分布 — RSI≥50 过滤是否冗余】")
        print(f"  {pct_stats(rsis)}")
        near = sum(1 for r in rsis if r < 55)
        print(f"  入场 RSI ∈ [50,55) 的边际单: {near}/{len(rsis)} "
              f"({near/len(rsis)*100:.0f}%)  ← 越少说明 RSI≥50 越冗余")
        print()

    # ── 持仓时长 ──
    print("【交易频率】")
    print(f"  已平仓 {len(closed)} 笔 / {m['stock_count']} 股池 / 27 个月")
    print(f"  持仓中未平仓 {len(open_t)} 笔")

    # 存档
    out = {
        "params": {"start": START, "end": END, "cash": CASH},
        "metrics": m,
        "exit_breakdown": {
            r: {
                "n": len(ts),
                "win_rate": round(sum(1 for t in ts if t["pnl"] > 0) / len(ts), 3),
                "mean_pnl_pct": round(st.mean([t["pnl_pct"] for t in ts]), 2),
                "max_pnl_pct": round(max(t["pnl_pct"] for t in ts), 2),
            }
            for r, ts in by_reason.items()
        },
        "entry_rsi": pct_stats(rsis) if rsis else {},
        "trades": closed,
    }
    with open("logs/diag_trend_follow.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n→ 明细已存 logs/diag_trend_follow.json")


if __name__ == "__main__":
    asyncio.run(main())
