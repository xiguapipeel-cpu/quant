"""趋势跟踪 — 递进式 ablation：股票池 + 止损执行修复。

config 1 baseline          : 全市场池 + 次开市价硬止损（现状）
config 2 +股票池            : 20日均成交额≥5000万 + 现状止损
config 3 +股票池+止损修复    : 干净池 + intrabar 挂单止损(-5%)

逐层隔离每个杠杆的边际贡献。其余参数全冻结。
用法: ./venv/bin/python -m backtest.ablation_market_filter
"""
import asyncio
import json
import statistics as st
from collections import defaultdict

from backtest.bt_runner import run_for_web

START = "2024-01-01"
END = "2026-03-31"
CASH = 100_000.0


def classify_exit(reason: str) -> str:
    if "硬止损" in reason:
        return "硬止损"
    if "追踪止损" in reason:
        return "追踪止损"
    if "下穿" in reason or "死叉" in reason:
        return "EMA死叉"
    return "其他"


def summarize(res):
    m = res["metrics"]
    trades = res["trades_paired"]
    closed = [t for t in trades if t["sell_date"] != "（持仓中）"]
    wins = [t["pnl_pct"] for t in closed if t["pnl"] > 0]
    loss = [t["pnl_pct"] for t in closed if t["pnl"] < 0]
    by_reason = defaultdict(list)
    for t in closed:
        by_reason[classify_exit(t["sell_reason"])].append(t["pnl_pct"])
    n = len(closed)
    wr = len(wins) / n if n else 0
    aw = st.mean(wins) if wins else 0
    al = st.mean(loss) if loss else 0
    hard = by_reason["硬止损"]
    return {
        "total_return": m["total_return"],
        "max_dd": m["max_drawdown"],
        "sharpe": m["sharpe_ratio"],
        "pf": m["profit_factor"],
        "n_closed": n,
        "win_rate": round(wr * 100, 1),
        "avg_win": round(aw, 2),
        "avg_loss": round(al, 2),
        "payoff": round(aw / abs(al), 2) if al else 0,
        "expectancy": round(wr * aw + (1 - wr) * al, 2),
        "breakeven_wr": round(abs(al) / (aw + abs(al)) * 100, 1) if (aw + abs(al)) else 0,
        "max_win": round(max((t["pnl_pct"] for t in closed), default=0), 1),
        "hard_stop_n": len(hard),
        "hard_stop_pct": round(len(hard) / n * 100, 0) if n else 0,
        "hard_stop_avg_pnl": round(st.mean(hard), 2) if hard else 0,  # sanity: intrabar 应≈-5%
        "trail_n": len(by_reason["追踪止损"]),
        "deathcross_n": len(by_reason["EMA死叉"]),
    }


async def main():
    configs = {
        "baseline":      {},
        "+股票池":        {"min_turnover_wan": 5000},
        "+池+止损修复":   {"min_turnover_wan": 5000, "intrabar_stop": True},
    }
    out = {}
    for label, params in configs.items():
        res = await run_for_web(
            "trend_follow", START, END, CASH,
            data_source="local_db", extra_params=params,
        )
        if "error" in res:
            print(f"[{label}] ERROR:", res["error"])
            return
        out[label] = summarize(res)
        print(f"  [{label}] done: {out[label]['total_return']}")

    print("\n" + "=" * 76)
    print(f"趋势跟踪 · 递进 ablation | {START} ~ {END}")
    print("=" * 76)
    rows = [
        ("总收益", "total_return"), ("最大回撤", "max_dd"), ("夏普", "sharpe"),
        ("ProfitFactor", "pf"), ("平仓笔数", "n_closed"), ("胜率%", "win_rate"),
        ("盈亏平衡胜率%", "breakeven_wr"),
        ("平均盈%", "avg_win"), ("平均亏%", "avg_loss"), ("盈亏比", "payoff"),
        ("单笔期望%", "expectancy"), ("最大赢%", "max_win"),
        ("硬止损笔数", "hard_stop_n"), ("硬止损占比%", "hard_stop_pct"),
        ("硬止损均亏%", "hard_stop_avg_pnl"),
        ("追踪止损笔数", "trail_n"), ("死叉笔数", "deathcross_n"),
    ]
    labels = list(configs.keys())
    hdr = f"  {'指标':<14}" + "".join(f"{l:>14}" for l in labels)
    print(hdr)
    for disp, key in rows:
        line = f"  {disp:<14}" + "".join(f"{str(out[l][key]):>14}" for l in labels)
        print(line)

    with open("logs/ablation_trend_follow.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n→ 已存 logs/ablation_trend_follow.json")
    print("【sanity】+池+止损修复 的'硬止损均亏%'应≈-5%（vs baseline 的 -8.68%），"
          "否则挂单止损实现有问题")


if __name__ == "__main__":
    asyncio.run(main())
