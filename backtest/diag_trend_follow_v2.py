"""趋势跟踪2 (trend_follow_v2) vs 旧 trend_follow 同口径对照回测。

验证设计目标（见 CLAUDE 讨论 / 设计方案第八节）：
  胜率 24-30% → 40-50%；均亏 -6.85% → -4% 以内；PF >1.2；单笔期望转正；
  最大回撤 74% → <40%；最大赢家 ≥ +100%（右尾不能掉）；硬止损占比是否被回踩入场压下。

用法: ./venv/bin/python -m backtest.diag_trend_follow_v2
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
        return "硬止损(挂单)"
    if "保本前止损" in reason:
        return "保本前止损"
    if "吊灯追踪" in reason or "追踪止损" in reason:
        return "吊灯追踪止损"
    if "下穿" in reason or "死叉" in reason:
        return "EMA死叉"
    return f"其他:{reason[:16]}"


def analyze(res, label):
    m = res["metrics"]
    trades = res["trades_paired"]
    closed = [t for t in trades if t["sell_date"] != "（持仓中）"]
    open_t = [t for t in trades if t["sell_date"] == "（持仓中）"]

    wins = [t["pnl_pct"] for t in closed if t["pnl"] > 0]
    losses = [t["pnl_pct"] for t in closed if t["pnl"] < 0]
    out = {
        "label": label,
        "stock_count": m["stock_count"],
        "total_return": m["total_return"],
        "max_drawdown": m["max_drawdown"],
        "sharpe": m["sharpe_ratio"],
        "n_closed": len(closed),
        "n_open": len(open_t),
        "win_rate": m["win_rate"],
        "profit_factor": m["profit_factor"],
    }
    if wins and losses:
        avg_w, avg_l = st.mean(wins), st.mean(losses)
        wr = len(wins) / len(closed)
        out.update({
            "avg_win": round(avg_w, 2),
            "avg_loss": round(avg_l, 2),
            "payoff": round(avg_w / abs(avg_l), 2),
            "expectancy_per_trade": round(wr * avg_w + (1 - wr) * avg_l, 2),
            "max_winner": round(max(wins), 1),
            "max_loser": round(min(losses), 1),
        })
        win_pnls = sorted([t["pnl"] for t in closed if t["pnl"] > 0], reverse=True)
        out["top3_win_share"] = round(sum(win_pnls[:3]) / sum(win_pnls) * 100, 0) if win_pnls else 0

    by_reason = defaultdict(list)
    for t in closed:
        by_reason[classify_exit(t["sell_reason"])].append(t)
    out["exit_breakdown"] = {
        r: {
            "n": len(ts),
            "pct": round(len(ts) / len(closed) * 100, 0) if closed else 0,
            "win_rate": round(sum(1 for t in ts if t["pnl"] > 0) / len(ts) * 100, 0),
            "mean_pnl": round(st.mean([t["pnl_pct"] for t in ts]), 2),
            "max_pnl": round(max(t["pnl_pct"] for t in ts), 1),
        }
        for r, ts in sorted(by_reason.items(), key=lambda kv: -len(kv[1]))
    }
    out["_closed"] = closed
    return out


def show(out):
    print("=" * 72)
    print(f"【{out['label']}】  股池 {out['stock_count']}  | {START} ~ {END}")
    print("=" * 72)
    print(f"  总收益 {out['total_return']}   最大回撤 {out['max_drawdown']}   夏普 {out['sharpe']}")
    print(f"  已平仓 {out['n_closed']}  持仓中 {out['n_open']}   胜率 {out['win_rate']}  PF {out['profit_factor']}")
    if "avg_win" in out:
        print(f"  平均盈 +{out['avg_win']}%  平均亏 {out['avg_loss']}%  盈亏比 {out['payoff']}")
        print(f"  单笔期望 {out['expectancy_per_trade']:+}% / 笔")
        print(f"  最大赢家 +{out['max_winner']}%  最大输家 {out['max_loser']}%  Top3赢家占 {out.get('top3_win_share')}%")
    print(f"  {'出场原因':<14}{'笔数':>5}{'占比':>6}{'胜率':>6}{'均收益':>9}{'最大赢':>8}")
    for r, s in out["exit_breakdown"].items():
        print(f"  {r:<14}{s['n']:>5}{s['pct']:>5.0f}%{s['win_rate']:>5.0f}%"
              f"{s['mean_pnl']:>+8.2f}%{s['max_pnl']:>+7.1f}%")
    print()


async def main():
    results = {}
    for name, label in [("trend_follow", "旧 trend_follow (EMA金叉 baseline)"),
                        ("trend_follow_v2", "趋势跟踪2 (回踩MA20转强)")]:
        logs = []
        res = await run_for_web(name, START, END, CASH,
                                log_fn=lambda m: logs.append(m),
                                data_source="local_db")
        if "error" in res:
            print(f"ERROR [{name}]:", res["error"])
            for l in logs[-8:]:
                print("  ", l)
            continue
        results[name] = analyze(res, label)
        show(results[name])

    # ── 并排对照表 ──
    if "trend_follow" in results and "trend_follow_v2" in results:
        a, b = results["trend_follow"], results["trend_follow_v2"]
        print("=" * 72)
        print("对照 (旧 → 新)")
        print("=" * 72)
        def row(k, label, fmt="{}"):
            va, vb = a.get(k, "—"), b.get(k, "—")
            print(f"  {label:<18} {fmt.format(va):>14}  →  {fmt.format(vb):>14}")
        row("total_return", "总收益")
        row("max_drawdown", "最大回撤")
        row("win_rate", "胜率")
        row("profit_factor", "盈亏因子PF")
        row("avg_loss", "平均亏%")
        row("payoff", "盈亏比")
        row("expectancy_per_trade", "单笔期望%")
        row("max_winner", "最大赢家%")
        row("n_closed", "已平仓笔数")

    # 存档
    archive = {k: {kk: vv for kk, vv in v.items() if kk != "_closed"}
               for k, v in results.items()}
    archive["trades_v2"] = results.get("trend_follow_v2", {}).get("_closed", [])
    with open("logs/diag_trend_follow_v2.json", "w") as f:
        json.dump(archive, f, ensure_ascii=False, indent=2)
    print("→ 明细已存 logs/diag_trend_follow_v2.json")


if __name__ == "__main__":
    asyncio.run(main())
