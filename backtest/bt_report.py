"""
回测报告生成器
输出：文本报告 + JSON数据 + 净值曲线CSV
"""

import json
import csv
from pathlib import Path
from datetime import datetime
from backtest.engine import BacktestResult
from utils.logger import setup_logger

logger = setup_logger("bt_report")

BT_REPORT_DIR = Path("backtest_reports")
BT_REPORT_DIR.mkdir(exist_ok=True)


class BacktestReporter:

    def generate(self, result: BacktestResult) -> dict[str, str]:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        slug = f"{result.strategy_name}_{result.start_date}_{result.end_date}_{ts}"
        slug = slug.replace(" ", "_").replace("/", "-")

        txt_path  = BT_REPORT_DIR / f"{slug}.txt"
        json_path = BT_REPORT_DIR / f"{slug}.json"
        csv_path  = BT_REPORT_DIR / f"{slug}_equity.csv"

        self._write_text(result, txt_path)
        self._write_json(result, json_path)
        self._write_csv(result, csv_path)

        logger.info(f"回测报告已写入: {txt_path}")
        return {"txt": str(txt_path), "json": str(json_path), "csv": str(csv_path)}

    # ── 文本报告 ────────────────────────────────────────────
    def _write_text(self, r: BacktestResult, path: Path):
        lines = []
        W = 65

        def sep(c="─"): lines.append(c * W)
        def row(label, value, unit=""):
            lines.append(f"  {label:<22} {value}{unit}")

        sep("═")
        lines.append(f"  回测报告 — {r.strategy_name}")
        lines.append(f"  区间：{r.start_date} ~ {r.end_date}")
        sep("═")

        lines.append("")
        lines.append("  【策略配置】")
        sep()
        cfg = r.config
        row("初始资金",        f"{cfg.initial_cash:>14,.0f}", " 元")
        row("手续费率",        f"{cfg.commission_rate*100:.2f}%（双向）")
        row("印花税",          f"{cfg.stamp_duty*100:.1f}%（仅卖出）")
        row("滑点",            f"{cfg.slippage*100:.1f}%")
        row("单股最大仓位",    f"{cfg.max_position_pct*100:.0f}%")

        lines.append("")
        lines.append("  【绩效指标】")
        sep()
        row("最终净值",        f"{r.final_value:>14,.0f}", " 元")
        row("总收益率",        f"{r.total_return*100:>+.2f}%")
        row("年化收益率",      f"{r.annualized_return*100:>+.2f}%")
        row("最大回撤",        f"{r.max_drawdown*100:.2f}%")
        row("夏普比率",        f"{r.sharpe_ratio:.2f}")
        row("胜率",            f"{r.win_rate*100:.1f}%")
        row("盈亏比",          f"{r.profit_factor:.2f}")
        row("总交易次数",      f"{r.total_trades}")

        lines.append("")
        lines.append("  【逐股盈亏归因】")
        sep()
        for code, pnl in sorted(r.per_stock.items(), key=lambda x: -x[1]["pnl"]):
            mark = "▲" if pnl["pnl"] >= 0 else "▼"
            lines.append(f"  {mark} {code}  交易{pnl['trades']}次  "
                         f"盈亏={pnl['pnl']:+,.0f}元  ({pnl['pnl_pct']:+.2f}%)")

        lines.append("")
        lines.append("  【交易记录（最近20条）】")
        sep()
        for t in r.trades[-20:]:
            lines.append(
                f"  {t.date}  {t.action:<4}  {t.code}  "
                f"价={t.price:.2f}  股={t.shares:>6}  "
                f"额={t.amount:>12,.0f}  {t.reason[:30]}"
            )

        sep("═")
        lines.append(f"  原则声明：每条交易均有信号来源，无模糊判断")
        sep("═")

        path.write_text("\n".join(lines), encoding="utf-8")

    # ── JSON报告 ─────────────────────────────────────────────
    def _write_json(self, r: BacktestResult, path: Path):
        data = {
            "strategy":         r.strategy_name,
            "start_date":       r.start_date,
            "end_date":         r.end_date,
            "initial_cash":     r.initial_cash,
            "final_value":      r.final_value,
            "total_return":     r.total_return,
            "annualized_return":r.annualized_return,
            "max_drawdown":     r.max_drawdown,
            "sharpe_ratio":     r.sharpe_ratio,
            "win_rate":         r.win_rate,
            "profit_factor":    r.profit_factor,
            "total_trades":     r.total_trades,
            "per_stock":        r.per_stock,
            "trades": [
                {"date": t.date, "code": t.code, "action": t.action,
                 "price": t.price, "shares": t.shares, "amount": t.amount,
                 "commission": t.commission, "reason": t.reason}
                for t in r.trades
            ],
            "daily_equity": [
                {"date": s.date, "total_value": s.total_value,
                 "cumulative_return": s.cumulative_return}
                for s in r.daily_snapshots
            ],
        }
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── 净值曲线CSV ──────────────────────────────────────────
    def _write_csv(self, r: BacktestResult, path: Path):
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["日期", "总资产(元)", "累计收益率", "每日收益率"])
            for s in r.daily_snapshots:
                writer.writerow([
                    s.date,
                    f"{s.total_value:.2f}",
                    f"{s.cumulative_return*100:.4f}%",
                    f"{s.daily_return*100:.4f}%",
                ])
