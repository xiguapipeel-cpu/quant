"""
monthly_outcome_report.py — 月度/周度 outcome 报告（样本外信号追踪）
====================================================================

依托 pattern_outcome（每个 WATCH/BUY 命中事件 + 信号日次开盘价后 5/10/30/60 日收益、
60 日内峰值/谷值），对**全部信号（即使没交易）**做样本外效力统计，回答：

  - BUY 数量
  - 5/10/30 日胜率
  - 平均收益（5/10/30/60 日）
  - 平均最大浮盈（peak_ret）
  - 平均最大浮亏（trough_ret）
  - 跳空亏损案例（次日跳空低开 + 后续亏损）
  - 未买入但后续大涨/大跌案例（执行层未建仓的信号，对照其真实后续走势）

与 dual_track_review 的分工：
  - dual_track_review  → 执行轨（position_monitor）：已建仓单的执行规则效果
  - monthly_outcome_report → 信号轨（pattern_outcome）：全信号样本外效力（含没买的）

前置（每日 cron）：
  python -m scripts.daily_major_capital_scan     # 选股 + WATCH/BUY 写入 pattern_outcome
  python -m scripts.pattern_tracker --update     # 刷新 5/10/30/60 日收益 + 峰值/谷值

用法：
  python -m scripts.monthly_outcome_report                  # 当月
  python -m scripts.monthly_outcome_report --month 2025-09
  python -m scripts.monthly_outcome_report --weeks 4
  python -m scripts.monthly_outcome_report --since 2025-01-01 --until 2025-12-31
  python -m scripts.monthly_outcome_report --all --save
"""
import argparse
import asyncio
import calendar
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool
from db.stock_dao import get_daily_history
from utils.logger import setup_logger

logger = setup_logger("monthly_outcome")

STRATEGY_ID = "major_capital_accumulation"

# 案例阈值
GAP_DOWN_PCT = -0.02       # 次日开盘相对信号日收盘跳空 ≤ -2% 视为"跳空低开"
BIG_UP_PEAK = 0.15         # 后续峰值 ≥ 15% = 大涨
BIG_DOWN_TROUGH = -0.10    # 后续谷值 ≤ -10% = 大跌


def _pct(v, d=2):
    return "—" if v is None else f"{float(v)*100:+.{d}f}%"


def _f(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


async def _fetch(cur, sql, args=()):
    await cur.execute(sql, args)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in await cur.fetchall()]


# ════════════════════════════════════════════════════════════
# 统计：BUY 信号样本外效力（headline）
# ════════════════════════════════════════════════════════════
async def headline_stats(cur, since, until) -> dict:
    where = "strategy=%s AND signal_type='BUY' AND status IN ('partial','completed')"
    args = [STRATEGY_ID]
    if since:
        where += " AND signal_date >= %s"; args.append(since)
    if until:
        where += " AND signal_date <= %s"; args.append(until)
    await cur.execute(f"""
        SELECT
          COUNT(*) AS n_buy,
          AVG(ret_5d)  AS avg_5d,  AVG(ret_10d) AS avg_10d,
          AVG(ret_30d) AS avg_30d, AVG(ret_60d) AS avg_60d,
          SUM(ret_5d  > 0) / SUM(ret_5d  IS NOT NULL) AS win_5d,
          SUM(ret_10d > 0) / SUM(ret_10d IS NOT NULL) AS win_10d,
          SUM(ret_30d > 0) / SUM(ret_30d IS NOT NULL) AS win_30d,
          SUM(ret_60d > 0) / SUM(ret_60d IS NOT NULL) AS win_60d,
          AVG(peak_ret)   AS avg_peak,
          AVG(trough_ret) AS avg_trough,
          MAX(peak_ret)   AS max_peak,
          MIN(trough_ret) AS min_trough,
          SUM(ret_30d IS NOT NULL) AS n_30d_ready
        FROM pattern_outcome WHERE {where}
    """, tuple(args))
    cols = [d[0] for d in cur.description]
    row = await cur.fetchone()
    out = dict(zip(cols, row)) if row else {}
    for k, v in list(out.items()):
        if v is not None and not isinstance(v, (int, str)):
            out[k] = _f(v)
    return out


# ════════════════════════════════════════════════════════════
# 案例：跳空亏损（次日跳空低开 + 后续亏损）
# ════════════════════════════════════════════════════════════
async def gap_down_losses(cur, since, until, limit=15) -> list[dict]:
    # 实际亏损：最近可得窗口收益（30→10→5）为负，而非仅盘中短暂回调
    where = ("strategy=%s AND signal_type='BUY' AND status IN ('partial','completed') "
             "AND buy_price IS NOT NULL AND COALESCE(ret_30d, ret_10d, ret_5d) < 0")
    args = [STRATEGY_ID]
    if since:
        where += " AND signal_date >= %s"; args.append(since)
    if until:
        where += " AND signal_date <= %s"; args.append(until)
    rows = await _fetch(cur, f"""
        SELECT code, name, signal_date, buy_date, buy_price,
               ret_5d, ret_30d, peak_ret, trough_ret,
               COALESCE(ret_30d, ret_10d, ret_5d) AS realized_ret
        FROM pattern_outcome WHERE {where}
        ORDER BY realized_ret ASC
    """, tuple(args))

    cases = []
    for r in rows:
        # 信号日收盘价：从 stock_daily 取（次日 open = buy_price 已有）
        sd = str(r["signal_date"])
        sig_rows = await get_daily_history(r["code"], sd, sd)
        if not sig_rows:
            continue
        sig_close = _f(sig_rows[0]["close"])
        buy_open = _f(r["buy_price"])
        if not sig_close or not buy_open:
            continue
        gap = buy_open / sig_close - 1.0
        if gap <= GAP_DOWN_PCT:        # 确认跳空低开
            r["signal_close"] = sig_close
            r["gap_pct"] = gap
            cases.append(r)
        if len(cases) >= limit:
            break
    return cases


# ════════════════════════════════════════════════════════════
# 案例：未买入（执行层未建仓）但后续大涨/大跌
# ════════════════════════════════════════════════════════════
async def not_bought_movers(cur, since, until, limit=15) -> dict:
    """pattern_outcome BUY 事件中，position_monitor 没有以 open/exited 建仓的
    （= 执行层未实际买入：skipped 或从未登记），看其后续峰值/谷值。"""
    where = ("po.strategy=%s AND po.signal_type='BUY' AND po.status IN ('partial','completed')")
    args = [STRATEGY_ID]
    if since:
        where += " AND po.signal_date >= %s"; args.append(since)
    if until:
        where += " AND po.signal_date <= %s"; args.append(until)
    rows = await _fetch(cur, f"""
        SELECT po.code, po.name, po.signal_date, po.peak_ret, po.trough_ret,
               po.ret_30d,
               pm.status AS pm_status, pm.execution_reason
        FROM pattern_outcome po
        LEFT JOIN position_monitor pm
          ON pm.strategy=po.strategy AND pm.code=po.code
         AND pm.signal_date=po.signal_date AND pm.status IN ('open','exited')
        WHERE {where} AND pm.id IS NULL
        ORDER BY po.signal_date ASC
    """, tuple(args))

    big_up = sorted([r for r in rows if _f(r.get("peak_ret")) is not None
                     and _f(r["peak_ret"]) >= BIG_UP_PEAK],
                    key=lambda r: _f(r["peak_ret"]), reverse=True)[:limit]
    big_down = sorted([r for r in rows if _f(r.get("trough_ret")) is not None
                       and _f(r["trough_ret"]) <= BIG_DOWN_TROUGH],
                      key=lambda r: _f(r["trough_ret"]))[:limit]
    return {"total_not_bought": len(rows), "big_up": big_up, "big_down": big_down}


# ════════════════════════════════════════════════════════════
# 渲染
# ════════════════════════════════════════════════════════════
def render(window_label, since, until, hl, gaps, movers) -> str:
    L = []
    L.append("# 月度 Outcome 报告（样本外信号追踪）")
    L.append("")
    L.append(f"- 策略：`{STRATEGY_ID}`")
    L.append(f"- 统计窗口：{window_label}（信号日 {since or '不限'} ~ {until or '不限'}）")
    L.append(f"- 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    L.append("")

    # ── 1. headline ──
    n_buy = int(hl.get("n_buy") or 0)
    L.append("## 1. BUY 信号样本外效力")
    L.append("")
    if n_buy == 0:
        L.append("- 窗口内无已跟踪的 BUY 信号（pattern_outcome）。")
        L.append("")
        return "\n".join(L)
    L.append(f"- **BUY 数量**：{n_buy} 笔（其中 30 日窗口已到期 {int(hl.get('n_30d_ready') or 0)} 笔）")
    L.append("")
    L.append("| 持有窗口 | 胜率 | 平均收益 |")
    L.append("|---------|------|---------|")
    for d in ("5d", "10d", "30d", "60d"):
        L.append(f"| {d} | {_pct(hl.get('win_'+d),1)} | {_pct(hl.get('avg_'+d))} |")
    L.append("")
    L.append(f"- **平均最大浮盈**（peak_ret）：{_pct(hl.get('avg_peak'))}"
             f"｜最高 {_pct(hl.get('max_peak'))}")
    L.append(f"- **平均最大浮亏**（trough_ret）：{_pct(hl.get('avg_trough'))}"
             f"｜最深 {_pct(hl.get('min_trough'))}")
    L.append("")

    # ── 2. 跳空亏损案例 ──
    L.append("## 2. 跳空亏损案例（次日跳空低开 ≤ {:.0%} + 后续亏损）".format(GAP_DOWN_PCT))
    L.append("")
    if gaps:
        L.append("| 代码 | 名称 | 信号日 | 信号日收盘 | 次日开盘 | 跳空 | 5日收益 | 30日收益 | 最大浮亏 |")
        L.append("|------|------|--------|-----------|---------|------|--------|---------|---------|")
        for g in gaps:
            L.append("| {c} | {n} | {sd} | {sc:.2f} | {bo:.2f} | {gp} | {r5} | {r30} | {tr} |".format(
                c=g["code"], n=(g.get("name") or "")[:6], sd=str(g["signal_date"]),
                sc=g["signal_close"], bo=_f(g["buy_price"]), gp=_pct(g["gap_pct"]),
                r5=_pct(g.get("ret_5d")), r30=_pct(g.get("ret_30d")), tr=_pct(g.get("trough_ret"))))
    else:
        L.append("- 窗口内无跳空低开导致亏损的 BUY 案例。")
    L.append("")

    # ── 3. 未买入但后续大涨/大跌 ──
    L.append("## 3. 未买入但后续大涨/大跌案例")
    L.append("")
    L.append(f"- 执行层未实际建仓（skipped 或未登记）且有后续跟踪的 BUY 信号：{movers['total_not_bought']} 笔")
    L.append("")
    L.append(f"### 3.1 错过的大涨（后续峰值 ≥ {BIG_UP_PEAK:.0%}）：{len(movers['big_up'])} 笔")
    if movers["big_up"]:
        L.append("")
        L.append("| 代码 | 名称 | 信号日 | 后续峰值 | 30日收益 | 未买入原因 |")
        L.append("|------|------|--------|---------|---------|-----------|")
        for m in movers["big_up"]:
            L.append("| {c} | {n} | {sd} | {pk} | {r30} | {rs} |".format(
                c=m["code"], n=(m.get("name") or "")[:6], sd=str(m["signal_date"]),
                pk=_pct(m.get("peak_ret")), r30=_pct(m.get("ret_30d")),
                rs=(m.get("execution_reason") or "未登记")[:24]))
    L.append("")
    L.append(f"### 3.2 成功避开的大跌（后续谷值 ≤ {BIG_DOWN_TROUGH:.0%}）：{len(movers['big_down'])} 笔")
    if movers["big_down"]:
        L.append("")
        L.append("| 代码 | 名称 | 信号日 | 后续谷值 | 30日收益 | 未买入原因 |")
        L.append("|------|------|--------|---------|---------|-----------|")
        for m in movers["big_down"]:
            L.append("| {c} | {n} | {sd} | {tr} | {r30} | {rs} |".format(
                c=m["code"], n=(m.get("name") or "")[:6], sd=str(m["signal_date"]),
                tr=_pct(m.get("trough_ret")), r30=_pct(m.get("ret_30d")),
                rs=(m.get("execution_reason") or "未登记")[:24]))
    L.append("")

    # ── 小结 ──
    win30 = _f(hl.get("win_30d"))
    L.append("## 小结")
    L.append("")
    if win30 is not None:
        verdict = "信号有效" if win30 >= 0.5 else "信号偏弱（优先怀疑样本量/regime，而非改形态参数）"
        L.append(f"- 30 日胜率 {win30:.0%} → {verdict}")
    if movers["big_up"]:
        L.append(f"- 错过 {len(movers['big_up'])} 笔大涨 —— 若集中在某类执行过滤原因，考虑**只放宽该执行规则**。")
    if movers["big_down"]:
        L.append(f"- 避开 {len(movers['big_down'])} 笔大跌 —— 执行过滤的正贡献。")
    if gaps:
        L.append(f"- {len(gaps)} 笔跳空低开亏损 —— 次日高开过滤无法防低开；-10% 硬止损是主要防线，"
                 "可评估是否对跳空低开追加快速离场规则（执行层）。")
    L.append("")
    L.append("> 原则（CLAUDE.md）：本报告是样本外信号效力跟踪。若要调整，**只动执行层规则**"
             "（高开/低开过滤、top-N、补仓窗口、止损），不要回到形态参数（WATCH/BUY 定义）上过拟合。")
    L.append("")
    return "\n".join(L)


# ════════════════════════════════════════════════════════════
async def main(args):
    if args.all:
        since = until = None
        label = "全部历史"
    elif args.month:
        y, m = map(int, args.month.split("-"))
        since = date(y, m, 1).isoformat()
        until = date(y, m, calendar.monthrange(y, m)[1]).isoformat()
        label = f"{args.month} 月度"
    elif args.since or args.until:
        since, until = args.since, args.until
        label = f"自定义区间"
    elif args.weeks:
        since = (date.today() - timedelta(weeks=args.weeks)).isoformat()
        until = None
        label = f"近 {args.weeks} 周"
    else:
        today = date.today()
        since = today.replace(day=1).isoformat()
        until = None
        label = f"{today.strftime('%Y-%m')} 当月（至今）"

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            hl = await headline_stats(cur, since, until)
            gaps = await gap_down_losses(cur, since, until, limit=args.case_limit)
            movers = await not_bought_movers(cur, since, until, limit=args.case_limit)

    report = render(label, since, until, hl, gaps, movers)
    print("\n" + report)

    if args.save:
        out_dir = ROOT / "logs"
        out_dir.mkdir(exist_ok=True)
        tag = args.month if args.month else date.today().isoformat()
        fn = out_dir / f"monthly_outcome_{tag}.md"
        fn.write_text(report, encoding="utf-8")
        logger.info(f"[月报] 已保存 {fn}")

    await close_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="月度 outcome 报告（样本外信号追踪）")
    ap.add_argument("--month", type=str, default=None, help="指定月份 YYYY-MM")
    ap.add_argument("--weeks", type=int, default=None, help="近 N 周")
    ap.add_argument("--since", type=str, default=None, help="起始信号日 YYYY-MM-DD")
    ap.add_argument("--until", type=str, default=None, help="结束信号日 YYYY-MM-DD")
    ap.add_argument("--all", action="store_true", help="全部历史")
    ap.add_argument("--save", action="store_true", help="额外写入 logs/monthly_outcome_<tag>.md")
    ap.add_argument("--case-limit", type=int, default=15, help="各类案例最多列出条数")
    asyncio.run(main(ap.parse_args()))
