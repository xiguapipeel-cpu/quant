"""趋势跟踪 — 入场信号对照：EMA金叉 vs Donchian 突破。

过滤类杠杆已三次证伪，问题在入场信号。EMA12/26 金叉 = coin-flip 入场。
换成海龟法则的 N 日新高突破，保留现有 trail/硬止损/冷静期出场。

config 1 EMA金叉(baseline) : ef 上穿 es + close>EMA60 + RSI≥50 + 放量
config 2 Donchian20        : close>前20日高 + close>EMA60 + RSI≥50 + 放量
config 3 Donchian55        : close>前55日高 + ...（海龟 System 2 长周期）

用法: ./venv/bin/python -m backtest.ablation_entry_mode
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
    return {
        "total_return": m["total_return"],
        "max_dd": m["max_drawdown"],
        "sharpe": m["sharpe"] if "sharpe" in m else m["sharpe_ratio"],
        "pf": m["profit_factor"],
        "n_closed": n,
        "win_rate": round(wr * 100, 1),
        "breakeven_wr": round(abs(al) / (aw + abs(al)) * 100, 1) if (aw + abs(al)) else 0,
        "avg_win": round(aw, 2),
        "avg_loss": round(al, 2),
        "payoff": round(aw / abs(al), 2) if al else 0,
        "expectancy": round(wr * aw + (1 - wr) * al, 2),
        "max_win": round(max((t["pnl_pct"] for t in closed), default=0), 1),
        "hard_stop_pct": round(len(hard) / n * 100, 0) if n else 0,
        "trail_n": len(by_reason["追踪止损"]),
    }


async def main():
    configs = {
        "EMA金叉":     {},
        "Donchian20":  {"entry_mode": "donchian", "donchian_n": 20},
        "Donchian55":  {"entry_mode": "donchian", "donchian_n": 55},
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
              f"胜率{out[label]['win_rate']}% PF{out[label]['pf']}")

    print("\n" + "=" * 70)
    print(f"趋势跟踪 · 入场信号对照 | {START} ~ {END}")
    print("=" * 70)
    rows = [
        ("总收益", "total_return"), ("最大回撤", "max_dd"), ("夏普", "sharpe"),
        ("ProfitFactor", "pf"), ("平仓笔数", "n_closed"),
        ("胜率%", "win_rate"), ("盈亏平衡胜率%", "breakeven_wr"),
        ("平均盈%", "avg_win"), ("平均亏%", "avg_loss"), ("盈亏比", "payoff"),
        ("单笔期望%", "expectancy"), ("最大赢%", "max_win"),
        ("硬止损占比%", "hard_stop_pct"), ("追踪止损笔数", "trail_n"),
    ]
    labels = list(configs.keys())
    print(f"  {'指标':<14}" + "".join(f"{l:>12}" for l in labels))
    for disp, key in rows:
        print(f"  {disp:<14}" + "".join(f"{str(out[l][key]):>12}" for l in labels))

    with open("logs/ablation_entry_mode.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n→ 已存 logs/ablation_entry_mode.json")


if __name__ == "__main__":
    asyncio.run(main())
