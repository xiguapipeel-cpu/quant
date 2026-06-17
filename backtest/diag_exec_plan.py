"""10 万组合级回测：中性选股 vs 现状(confidence) —— 决定是否采用 exec_plan。

M=8 等权，资金 10 万，同期同股池。对比 select_mode='confidence'(baseline) 与
'neutral'(不按动量、稳定伪随机填槽)。回答：换中性选股，组合级收益/回撤实际差多少。

用法: ./venv/bin/python -m backtest.diag_exec_plan
"""
import asyncio
import statistics as st
from backtest.bt_runner import run_for_web

START, END, CASH = "2024-09-01", "2026-06-30", 100_000.0
M = 8


def summarize(res):
    m = res["metrics"]
    closed = [t for t in res["trades_paired"] if t["sell_date"] != "（持仓中）"]
    wins = [t["pnl_pct"] for t in closed if t["pnl"] > 0]
    losses = [t["pnl_pct"] for t in closed if t["pnl"] < 0]
    out = {
        "total_return": m["total_return"], "max_drawdown": m["max_drawdown"],
        "sharpe": m["sharpe_ratio"], "win_rate": m["win_rate"],
        "pf": m["profit_factor"], "n": len(closed),
    }
    if wins and losses:
        out["avg_win"] = round(st.mean(wins), 2)
        out["avg_loss"] = round(st.mean(losses), 2)
        out["max_winner"] = round(max(wins), 1)
    return out


async def main():
    rows = {}
    for mode, label in [("confidence", "现状(confidence选股)"), ("neutral", "中性选股(exec_plan)")]:
        logs = []
        res = await run_for_web(
            "major_capital_accumulation", START, END, CASH,
            log_fn=lambda x: logs.append(x), data_source="local_db",
            extra_params={"max_positions": M, "position_pct": 0.20,
                          "select_mode": mode},
        )
        if "error" in res:
            print(f"ERROR [{mode}]:", res["error"]); [print(" ", l) for l in logs[-6:]]
            continue
        rows[mode] = summarize(res)
        print(f"\n【{label}】 M={M} 资金{CASH:,.0f} | {START}~{END}")
        for k, v in rows[mode].items():
            print(f"  {k:<14}{v}")

    if len(rows) == 2:
        a, b = rows["confidence"], rows["neutral"]
        print("\n" + "=" * 56)
        print(f"  {'指标':<14}{'现状':>16}{'中性选股':>16}")
        print("=" * 56)
        for k, lbl in [("total_return", "总收益"), ("max_drawdown", "最大回撤"),
                       ("sharpe", "夏普"), ("win_rate", "胜率"),
                       ("pf", "盈亏因子"), ("avg_loss", "平均亏"),
                       ("max_winner", "最大赢家"), ("n", "笔数")]:
            print(f"  {lbl:<14}{str(a.get(k,'—')):>16}{str(b.get(k,'—')):>16}")


if __name__ == "__main__":
    asyncio.run(main())
