"""
dual_track_review.py — 实盘/模拟双轨验证复盘报告
====================================================

把 position_monitor（执行轨：模拟 is_real=0 / 真实 is_real=1 / 执行过滤 skipped）
与 pattern_outcome（信号轨：命中后 5/10/30/60 日后续走势）两张表串联，
回答 4-8 周复盘的核心问题：

  1. 信号是否有效？           —— pattern_outcome 命中后胜率 / 平均收益 / 峰值
  2. 执行规则是否减少亏损？   —— 进场单 PnL、硬止损触发、skipped 拦截、实盘 vs 回测退出滑点
  3. 是否错过太多大赢家？     —— skipped / 放弃补仓(stage=3) 的票后续是否大涨
  4. 是否需要调整执行规则？   —— 基于以上指标给出启发式建议（调执行，不调形态）

并对每笔进场交易输出 item-4 要求的逐笔明细：
  信号日收盘价 / 次日开盘价 / 实际成交价 / 最高浮盈 / 最大浮亏 / 回测退出价 / 实盘退出价

用法：
  python -m scripts.dual_track_review                 # 默认近 6 周
  python -m scripts.dual_track_review --weeks 8
  python -m scripts.dual_track_review --since 2026-04-01
  python -m scripts.dual_track_review --all           # 全部历史
  python -m scripts.dual_track_review --weeks 6 --save # 额外写 logs/dual_track_review_<date>.md
"""
import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.pattern_dao import aggregate_stats
from utils.logger import setup_logger

logger = setup_logger("dual_review")

STRATEGY_ID = "major_capital_accumulation"

# 大赢家 / 大输家阈值（基于 pattern_outcome.peak_ret / trough_ret）
BIG_WINNER_PEAK = 0.15      # 峰值浮盈 ≥ 15% 视为"大赢家"
AVOIDED_TROUGH = -0.08      # 谷值浮亏 ≤ -8% 视为"成功避开的输家"


def _pct(v, digits=2):
    return "—" if v is None else f"{float(v)*100:+.{digits}f}%"


def _num(v, digits=2):
    return "—" if v is None else f"{float(v):.{digits}f}"


def _f(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


async def _fetch(cur, sql, args=()):
    await cur.execute(sql, args)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in await cur.fetchall()]


# ════════════════════════════════════════════════════════════
# 数据采集
# ════════════════════════════════════════════════════════════
async def collect(since: Optional[str]) -> dict:
    pool = await get_pool()
    where_pos = "strategy=%s"
    where_pm = "pm.strategy=%s"            # 同条件，列加 pm. 前缀（用于 JOIN 查询）
    args_pos: list = [STRATEGY_ID]
    if since:
        where_pos += " AND signal_date >= %s"
        where_pm += " AND pm.signal_date >= %s"
        args_pos.append(since)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            entered = await _fetch(cur, f"""
                SELECT * FROM position_monitor
                WHERE {where_pos} AND status='exited'
                ORDER BY signal_date ASC, code ASC
            """, tuple(args_pos))
            open_pos = await _fetch(cur, f"""
                SELECT * FROM position_monitor
                WHERE {where_pos} AND status='open'
                ORDER BY signal_date ASC, code ASC
            """, tuple(args_pos))
            skipped = await _fetch(cur, f"""
                SELECT * FROM position_monitor
                WHERE {where_pos} AND status='skipped'
                ORDER BY signal_date ASC, code ASC
            """, tuple(args_pos))

            # skipped + 放弃补仓(stage=3) 的票后续走势（join pattern_outcome）
            missed = await _fetch(cur, f"""
                SELECT pm.code, pm.name, pm.signal_date, pm.status, pm.entry_stage,
                       pm.execution_reason, pm.position_pct,
                       po.peak_ret, po.trough_ret, po.ret_30d
                FROM position_monitor pm
                LEFT JOIN pattern_outcome po
                  ON po.strategy=pm.strategy AND po.code=pm.code AND po.signal_date=pm.signal_date
                WHERE {where_pm}
                  AND (pm.status='skipped' OR pm.entry_stage=3)
                ORDER BY pm.signal_date ASC
            """, tuple(args_pos))

    signal_stats = await aggregate_stats(STRATEGY_ID, since_date=since)
    return {
        "entered": entered, "open": open_pos, "skipped": skipped,
        "missed": missed, "signal_stats": signal_stats,
    }


# ════════════════════════════════════════════════════════════
# 分析
# ════════════════════════════════════════════════════════════
def analyze_execution(entered: list[dict]) -> dict:
    n = len(entered)
    if n == 0:
        return {"n": 0}
    pnls = [_f(p.get("exit_pnl_pct")) for p in entered if _f(p.get("exit_pnl_pct")) is not None]
    wins = [p for p in pnls if p > 0]
    hard_stops = [p for p in entered if "硬止损" in (p.get("exit_reason") or "")]
    trail_exits = [p for p in entered if "trail" in (p.get("exit_reason") or "").lower()
                   or "追踪" in (p.get("exit_reason") or "")]

    # 实盘 vs 回测退出滑点：actual_exit_price vs exit_price（信号日收盘）
    slips = []
    for p in entered:
        ep, ap = _f(p.get("exit_price")), _f(p.get("actual_exit_price"))
        if ep and ap and ep > 0:
            slips.append(ap / ep - 1.0)

    real = [p for p in entered if p.get("is_real") == 1]
    sim = [p for p in entered if p.get("is_real") != 1]

    def _avg(xs):
        return sum(xs) / len(xs) if xs else None

    return {
        "n": n,
        "avg_pnl": _avg(pnls),
        "win_rate": len(wins) / len(pnls) if pnls else None,
        "best": max(pnls) if pnls else None,
        "worst": min(pnls) if pnls else None,
        "n_hard_stop": len(hard_stops),
        "n_trail_exit": len(trail_exits),
        "avg_exit_slippage": _avg(slips),
        "n_real": len(real), "avg_pnl_real": _avg([_f(p.get("exit_pnl_pct")) for p in real if _f(p.get("exit_pnl_pct")) is not None]),
        "n_sim": len(sim), "avg_pnl_sim": _avg([_f(p.get("exit_pnl_pct")) for p in sim if _f(p.get("exit_pnl_pct")) is not None]),
    }


def analyze_missed(missed: list[dict]) -> dict:
    skipped = [m for m in missed if m.get("status") == "skipped"]
    half = [m for m in missed if m.get("entry_stage") == 3]

    def _split(rows):
        winners = [r for r in rows if _f(r.get("peak_ret")) is not None and _f(r["peak_ret"]) >= BIG_WINNER_PEAK]
        avoided = [r for r in rows if _f(r.get("trough_ret")) is not None and _f(r["trough_ret"]) <= AVOIDED_TROUGH]
        tracked = [r for r in rows if _f(r.get("peak_ret")) is not None]
        return winners, avoided, tracked

    sk_win, sk_avoid, sk_tracked = _split(skipped)
    hf_win, hf_avoid, hf_tracked = _split(half)
    return {
        "n_skipped": len(skipped), "skipped_tracked": len(sk_tracked),
        "skipped_winners": sk_win, "skipped_avoided": len(sk_avoid),
        "n_half": len(half), "half_tracked": len(hf_tracked),
        "half_winners": hf_win,
    }


def build_recommendations(execu: dict, missed: dict, signal: dict) -> list[str]:
    recs = []
    # 1. 信号有效性
    win30 = _f(signal.get("win_30d"))
    if win30 is not None:
        if win30 < 0.5:
            recs.append(f"⚠️ 信号 30 日胜率 {win30:.0%} < 50% —— 信号层质量偏弱；但按 CLAUDE.md 原则"
                        "**优先怀疑样本量/市场 regime，而非立即改形态参数**。")
        else:
            recs.append(f"✅ 信号 30 日胜率 {win30:.0%} ≥ 50%，形态本身有效，问题集中在执行层即可。")

    # 2. 错过大赢家
    if missed["skipped_tracked"] > 0:
        miss_ratio = len(missed["skipped_winners"]) / missed["skipped_tracked"]
        if miss_ratio >= 0.3:
            codes = ", ".join(f"{m['code']}({_pct(m.get('peak_ret'))})" for m in missed["skipped_winners"][:5])
            recs.append(f"⚠️ 被执行过滤拦截的票中 {miss_ratio:.0%} 后续峰值 ≥ {BIG_WINNER_PEAK:.0%}（{codes}）"
                        "—— 可能过滤过严，考虑放宽次日高开阈值或提高 MAX_NEW_ENTRIES_PER_DAY。")
        else:
            recs.append(f"✅ 被拦截票中仅 {miss_ratio:.0%} 是大赢家，{missed['skipped_avoided']} 笔成功避开输家，执行过滤净正贡献。")

    # 3. 分批进场漏接
    if missed["half_tracked"] > 0 and missed["half_winners"]:
        codes = ", ".join(f"{m['code']}({_pct(m.get('peak_ret'))})" for m in missed["half_winners"][:5])
        recs.append(f"⚠️ {len(missed['half_winners'])} 笔放弃补仓(维持半仓)的票后续仍大涨（{codes}）"
                    "—— 补仓窗口 ADD_WINDOW_DAYS 可能过短或站稳判定过严，考虑放宽。")

    # 4. 硬止损 / 滑点
    if execu.get("n"):
        if execu["n_hard_stop"] and execu["n_hard_stop"] / execu["n"] >= 0.3:
            recs.append(f"⚠️ {execu['n_hard_stop']}/{execu['n']} 笔触发 -10% 硬止损 —— 入场时点偏弱或硬止损过松，"
                        "可结合次日高开过滤/分批进场进一步保护，而非放松止损。")
        slip = execu.get("avg_exit_slippage")
        if slip is not None and abs(slip) >= 0.01:
            recs.append(f"ℹ️ 实盘退出价相对回测退出价平均滑点 {_pct(slip)} —— "
                        f"{'实盘成交更优' if slip > 0 else '实盘成交更差，注意离场执行时滞'}。")

    if not recs:
        recs.append("样本不足，建议继续累计至 4-8 周后再评估。")
    return recs


# ════════════════════════════════════════════════════════════
# 报告渲染
# ════════════════════════════════════════════════════════════
def render(data: dict, since: Optional[str], weeks_label: str, detail_limit: int = 60) -> str:
    execu = analyze_execution(data["entered"])
    missed = analyze_missed(data["missed"])
    sig = data["signal_stats"] or {}
    recs = build_recommendations(execu, missed, sig)

    L = []
    L.append(f"# 实盘/模拟双轨验证复盘报告")
    L.append("")
    L.append(f"- 策略：`{STRATEGY_ID}`")
    L.append(f"- 复盘窗口：{weeks_label}（信号日 ≥ {since or '不限'}）")
    L.append(f"- 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    L.append(f"- 进场已退出 {execu.get('n',0)} 笔（真实 {execu.get('n_real',0)} / 模拟 {execu.get('n_sim',0)}）｜"
             f"持仓中 {len(data['open'])} 笔｜执行过滤 {len(data['skipped'])} 笔")
    L.append("")

    # ── Q1 信号是否有效 ──
    L.append("## 1. 信号是否有效？（信号轨 pattern_outcome）")
    L.append("")
    if sig.get("total"):
        L.append(f"- 命中事件 {int(sig['total'])} 条（BUY {int(sig.get('n_buy') or 0)} / WATCH {int(sig.get('n_watch') or 0)}）")
        L.append("")
        L.append("| 持有窗口 | 胜率 | 平均收益 |")
        L.append("|---------|------|---------|")
        for d in ("5d", "10d", "30d", "60d"):
            L.append(f"| {d} | {_pct(sig.get('win_'+d), 1)} | {_pct(sig.get('avg_'+d))} |")
        L.append("")
        L.append(f"- 平均峰值浮盈 {_pct(sig.get('avg_peak'))}｜平均谷值浮亏 {_pct(sig.get('avg_trough'))}"
                 f"｜最大峰值 {_pct(sig.get('max_peak'))}｜最深谷值 {_pct(sig.get('min_trough'))}")
    else:
        L.append("- 窗口内无已完成跟踪的命中事件（pattern_outcome）。")
    L.append("")

    # ── Q2 执行规则是否减少亏损 ──
    L.append("## 2. 执行规则是否减少亏损？（执行轨 position_monitor）")
    L.append("")
    if execu.get("n"):
        L.append(f"- 进场单平均 PnL {_pct(execu['avg_pnl'])}｜胜率 {_pct(execu['win_rate'],1)}"
                 f"｜最佳 {_pct(execu['best'])}｜最差 {_pct(execu['worst'])}")
        L.append(f"- 离场结构：硬止损(-10%) {execu['n_hard_stop']} 笔｜追踪止损 {execu['n_trail_exit']} 笔")
        if execu.get("avg_exit_slippage") is not None:
            L.append(f"- 实盘退出价 vs 回测退出价平均滑点：{_pct(execu['avg_exit_slippage'])}")
        if execu.get("n_real"):
            L.append(f"- 双轨对比：真实 {execu['n_real']} 笔均 {_pct(execu['avg_pnl_real'])}"
                     f"｜模拟 {execu['n_sim']} 笔均 {_pct(execu['avg_pnl_sim'])}")
    else:
        L.append("- 窗口内无已退出的进场单。")
    L.append("")
    # 执行过滤拦截结构
    if data["skipped"]:
        from collections import Counter
        cats = Counter()
        for s in data["skipped"]:
            r = s.get("execution_reason") or ""
            if "高开" in r: cats["次日高开过大"] += 1
            elif "跌回" in r: cats["跌回信号价下方"] += 1
            elif "排序" in r or "满" in r: cats["排序超额(top-N)"] += 1
            else: cats["其他"] += 1
        L.append(f"- 执行过滤拦截 {len(data['skipped'])} 笔：" + "，".join(f"{k} {v}" for k, v in cats.items()))
        L.append("")

    # ── Q3 是否错过大赢家 ──
    L.append("## 3. 是否错过太多大赢家？")
    L.append("")
    L.append(f"- 被执行过滤拦截且有后续跟踪 {missed['skipped_tracked']} 笔："
             f"其中峰值 ≥ {BIG_WINNER_PEAK:.0%} 的大赢家 {len(missed['skipped_winners'])} 笔，"
             f"成功避开(谷值 ≤ {AVOIDED_TROUGH:.0%})的输家 {missed['skipped_avoided']} 笔")
    if missed["skipped_winners"]:
        L.append("  - 错过的大赢家：" + "，".join(
            f"{m['code']} {m.get('name','')}（峰值{_pct(m.get('peak_ret'))}）" for m in missed["skipped_winners"][:8]))
    L.append(f"- 放弃补仓(维持半仓)的票 {missed['half_tracked']} 笔有跟踪，其中后续大涨 {len(missed['half_winners'])} 笔"
             f"（这些票只吃到半仓收益）")
    if missed["half_winners"]:
        L.append("  - 半仓漏接：" + "，".join(
            f"{m['code']} {m.get('name','')}（峰值{_pct(m.get('peak_ret'))}）" for m in missed["half_winners"][:8]))
    L.append("")

    # ── Q4 是否需要调整执行规则 ──
    L.append("## 4. 是否需要调整执行规则？（调执行，不调形态）")
    L.append("")
    for r in recs:
        L.append(f"- {r}")
    L.append("")
    L.append("> 提醒（CLAUDE.md 原则）：18 笔级别样本已用尽形态参数的统计验证空间，"
             "本复盘的调整建议**只针对执行层规则**（高开阈值 / top-N / 补仓窗口 / 止损），"
             "不应回到形态参数（WATCH/BUY 定义）上继续过拟合。")
    L.append("")

    # ── item-4 逐笔明细（最近优先，最多 detail_limit 笔）──
    all_trades = data["entered"] + data["open"]
    all_trades.sort(key=lambda p: (str(p.get("signal_date", "")), p.get("code", "")), reverse=True)
    shown = all_trades[:detail_limit]
    L.append(f"## 附：逐笔交易明细（item-4 要求字段，最近 {len(shown)}/{len(all_trades)} 笔）")
    L.append("")
    L.append("| 代码 | 名称 | 轨道 | 信号日 | 信号日收盘 | 次日开盘 | 实际成交(退出) | 最高浮盈 | 最大浮亏 | 回测退出价 | 实盘退出价 | PnL |")
    L.append("|------|------|------|--------|-----------|---------|--------------|---------|---------|-----------|-----------|-----|")
    for p in shown:
        track = "真实" if p.get("is_real") == 1 else "模拟"
        ep = _f(p.get("entry_price"))
        hi, lo = _f(p.get("highest_price")), _f(p.get("lowest_price"))
        hi_pct = (hi / ep - 1.0) if (hi and ep) else None
        lo_pct = (lo / ep - 1.0) if (lo and ep) else None
        status_tag = "" if p.get("status") == "exited" else "〔持仓中〕"
        L.append("| {code} | {name} | {track}{st} | {sd} | {sp} | {op} | {ap} | {hi} | {lo} | {bx} | {rx} | {pnl} |".format(
            code=p.get("code", ""), name=(p.get("name") or "")[:6], track=track, st=status_tag,
            sd=str(p.get("signal_date", "")), sp=_num(p.get("signal_price")), op=_num(ep),
            ap=_num(p.get("actual_exit_price")), hi=_pct(hi_pct), lo=_pct(lo_pct),
            bx=_num(p.get("exit_price")), rx=_num(p.get("actual_exit_price")),
            pnl=_pct(p.get("exit_pnl_pct")),
        ))
    L.append("")
    return "\n".join(L)


async def main(args):
    if args.all:
        since = None
        weeks_label = "全部历史"
    elif args.since:
        since = args.since
        weeks_label = f"自 {args.since}"
    else:
        since = (date.today() - timedelta(weeks=args.weeks)).isoformat()
        weeks_label = f"近 {args.weeks} 周"

    data = await collect(since)
    report = render(data, since, weeks_label, detail_limit=args.detail_limit)
    print("\n" + report)

    if args.save:
        out_dir = ROOT / "logs"
        out_dir.mkdir(exist_ok=True)
        fn = out_dir / f"dual_track_review_{date.today().isoformat()}.md"
        fn.write_text(report, encoding="utf-8")
        logger.info(f"[复盘] 已保存 {fn}")

    await close_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="实盘/模拟双轨验证复盘报告")
    ap.add_argument("--weeks", type=int, default=6, help="复盘窗口周数（默认 6）")
    ap.add_argument("--since", type=str, default=None, help="起始信号日 YYYY-MM-DD（覆盖 --weeks）")
    ap.add_argument("--all", action="store_true", help="全部历史")
    ap.add_argument("--save", action="store_true", help="额外写入 logs/dual_track_review_<date>.md")
    ap.add_argument("--detail-limit", type=int, default=60, help="逐笔明细最多显示笔数（默认 60，最近优先）")
    asyncio.run(main(ap.parse_args()))
