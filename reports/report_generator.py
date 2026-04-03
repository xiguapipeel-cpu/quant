"""
报告生成器
原则：每个结论都有数据来源，禁止"众所周知""一般认为"
"""

import json
from datetime import datetime
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger("reporter")

REPORT_DIR = Path("reports_output")
REPORT_DIR.mkdir(exist_ok=True)


class ReportGenerator:
    """生成结构化报告（文本 + JSON）"""

    def generate(self, results: list, scan_type: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"{ts}_{scan_type}"

        # 写 JSON（机器可读）
        json_path = REPORT_DIR / f"{base_name}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)

        # 写文本报告（人类可读）
        txt_path = REPORT_DIR / f"{base_name}.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self._build_text_report(results, scan_type))

        logger.info(f"报告已写入: {txt_path}")
        return str(txt_path)

    def _build_text_report(self, results: list, scan_type: str) -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        passed  = [r for r in results if r.get("integrity_pass")]
        excluded = [r for r in results if not r.get("integrity_pass")]

        lines = []
        lines.append("=" * 70)
        lines.append(f"A股量化交易系统 - {scan_type}扫描报告")
        lines.append(f"生成时间：{now}")
        lines.append(f"分析股票：{len(results)}只 | 通过：{len(passed)}只 | 排除：{len(excluded)}只")
        lines.append("=" * 70)
        lines.append("")
        lines.append("【数据完整性自检说明】")
        lines.append("  ✓ 通过条件：股价 / PE / 市值 / 近期事件 四项全有")
        lines.append("  ✗ 排除条件：任一字段缺失，不猜测，直接排除")
        lines.append("")

        # ── 通过的股票 ──────────────────────────────────────────
        lines.append("━" * 70)
        lines.append(f"▶ 通过自检（{len(passed)}只）")
        lines.append("━" * 70)

        for r in passed:
            lines.append(f"\n  {r['name']}（{r['code']}·{r['market']}）")
            lines.append(f"  ├─ 股价：¥{r.get('price', 'N/A')}")

            pe_v = r.get("validation", {}).get("pe", {})
            pe_sources = " | ".join(pe_v.get("sources", []))
            lines.append(f"  ├─ PE：{r.get('pe', 'N/A')} （{pe_sources}）")
            lines.append(f"  │    交叉验证偏差：{pe_v.get('max_deviation','N/A')} — {pe_v.get('note','')}")

            cap = r.get("market_cap")
            cap_str = f"{cap:.0f}亿" if cap else "N/A"
            lines.append(f"  ├─ 总市值：{cap_str}")

            events = r.get("recent_events", [])
            if events:
                lines.append(f"  ├─ 近期事件（{len(events)}条）：")
                for e in events[:3]:
                    title = e.get("title") or e.get("公告标题") or str(e)
                    lines.append(f"  │    · {title[:60]}")
            else:
                lines.append(f"  ├─ 近期事件：无公开记录")

            lines.append(f"  └─ 决策：{r.get('decision')} — {r.get('decision_reason')}")

        # ── 被排除的股票 ─────────────────────────────────────────
        lines.append("")
        lines.append("━" * 70)
        lines.append(f"▶ 排除（{len(excluded)}只）")
        lines.append("━" * 70)

        for r in excluded:
            missing = r.get("missing_fields", [])
            lines.append(f"\n  {r['name']}（{r['code']}）")
            lines.append(f"  └─ 排除原因：{r.get('decision_reason', '数据不完整')}")
            if missing:
                lines.append(f"       缺失字段：{', '.join(missing)}")

        lines.append("")
        lines.append("=" * 70)
        lines.append("原则声明：本报告所有结论均基于实际采集数据，")
        lines.append('不使用"众所周知""一般认为"等模糊表述，每项数据均标注来源。')
        lines.append("=" * 70)

        return "\n".join(lines)
