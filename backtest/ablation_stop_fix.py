"""趋势跟踪 — Donchian55 + 正确挂单止损 对照。

四次实验锁定：突破失败率 ~51% 不可降，盈利只能靠(a)压低失败单亏损成本(b)做大右尾。
Donchian55 已把右尾做大(PF 0.81)，本实验修(a)：把次开市价硬止损(实际亏 -7.96%)
换成 broker 端 OCO 挂单(Stop+StopTrail，当根触及即成交)，看 avg_loss 能否压回 -5.x%
并把 Donchian55 推到正期望。

config 1 Donchian55            : 次开市价止损（现状最佳）
config 2 Donchian55+挂单止损    : intrabar OCO 止损

用法: ./venv/bin/python -m backtest.ablation_stop_fix
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
    closed = [t for t in res["trades_paired"] if t["sell_date"] != "（持仓中）"]
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
    trail = by_reason["追踪止损"]
    return {
        "total_return": m["total_return"],
        "max_dd": m["max_drawdown"],
        "sharpe": m.get("sharpe_ratio", m.get("sharpe")),
        "pf": m["profit_factor"],
        "n_closed": n,
        "win_rate": round(wr * 100, 1),
        "breakeven_wr": round(abs(al) / (aw + abs(al)) * 100, 1) if (aw + abs(al)) else 0,
        "avg_win": round(aw, 2),
        "avg_loss": round(al, 2),
        "payoff": round(aw / abs(al), 2) if al else 0,
        "expectancy": round(wr * aw + (1 - wr) * al, 2),
        "max_win": round(max((t["pnl_pct"] for t in closed), default=0), 1),
        "hard_stop_n": len(hard),
        "hard_stop_avg_pnl": round(st.mean(hard), 2) if hard else 0,   # sanity: 挂单应≈-5%
        "trail_n": len(trail),
        "trail_avg_pnl": round(st.mean(trail), 2) if trail else 0,
    }


async def main():
    configs = {
        "D55_次开止损":   {"entry_mode": "donchian", "donchian_n": 55},
        "D55_挂单止损":   {"entry_mode": "donchian", "donchian_n": 55,
                          "intrabar_stop": True},
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
        print(f"  [{label}] done: {out[label]['total_return']} "
              f"PF{out[label]['pf']} 期望{out[label]['expectancy']}%")

    print("\n" + "=" * 60)
    print(f"趋势跟踪 · Donchian55 止损执行修复 | {START} ~ {END}")
    print("=" * 60)
    rows = [
        ("总收益", "total_return"), ("最大回撤", "max_dd"), ("夏普", "sharpe"),
        ("ProfitFactor", "pf"), ("平仓笔数", "n_closed"),
        ("胜率%", "win_rate"), ("盈亏平衡胜率%", "breakeven_wr"),
        ("平均盈%", "avg_win"), ("平均亏%", "avg_loss"), ("盈亏比", "payoff"),
        ("单笔期望%", "expectancy"), ("最大赢%", "max_win"),
        ("硬止损笔数", "hard_stop_n"), ("硬止损均亏%", "hard_stop_avg_pnl"),
        ("追踪止损笔数", "trail_n"), ("追踪止损均%", "trail_avg_pnl"),
    ]
    labels = list(configs.keys())
    print(f"  {'指标':<14}" + "".join(f"{l:>14}" for l in labels))
    for disp, key in rows:
        print(f"  {disp:<14}" + "".join(f"{str(out[l][key]):>14}" for l in labels))

    with open("logs/ablation_stop_fix.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n→ 已存 logs/ablation_stop_fix.json")
    print("【sanity】D55_挂单止损 的'硬止损均亏%'应≈-5~-6%（vs 次开的更深），"
          "否则挂单实现仍有问题")


if __name__ == "__main__":
    asyncio.run(main())
