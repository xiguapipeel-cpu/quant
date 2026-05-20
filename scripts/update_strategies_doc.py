"""
策略文档自动生成器
==================
从 backtest/strategies.py 和 backtest/screener.py 中提取策略逻辑，
生成 STRATEGIES.md 文档。

使用方式：
  python scripts/update_strategies_doc.py          # 手动生成
  （由 web/app.py 启动事件自动调用，策略文件变更时自动刷新）
"""

import ast
import hashlib
import inspect
import sys
from datetime import datetime
from pathlib import Path

# ── 项目根目录 ─────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
OUTPUT = ROOT / "STRATEGIES.md"

# ── 被监控的源文件 ─────────────────────────────────────────
WATCH_FILES = [
    ROOT / "backtest" / "strategies.py",
    ROOT / "backtest" / "screener.py",
    ROOT / "backtest" / "bt_major_capital.py",
    ROOT / "backtest" / "bt_strategies.py",
]

# ── 缓存文件（记录上次生成时各文件的哈希，避免重复生成）──────
HASH_CACHE = ROOT / "backtest_cache" / ".doc_hash"


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.md5(path.read_bytes()).hexdigest()


def _combined_hash() -> str:
    return "|".join(_file_hash(p) for p in WATCH_FILES)


def needs_update() -> bool:
    """检查策略文件是否有变更"""
    current = _combined_hash()
    if HASH_CACHE.exists():
        return HASH_CACHE.read_text().strip() != current
    return True


def _save_hash():
    HASH_CACHE.parent.mkdir(exist_ok=True)
    HASH_CACHE.write_text(_combined_hash())


# ══════════════════════════════════════════════════════════════
# 从源码中提取策略参数（AST解析，不 import 模块）
# ══════════════════════════════════════════════════════════════

def _const_val(node):
    """安全提取常量节点的 Python 值（含 -N 形式）"""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        if isinstance(node.operand, ast.Constant):
            return -node.operand.value
    return None


def _extract_init_params(source: str, class_name: str) -> list[dict]:
    """通过 AST 提取类的参数列表，支持：
       1. `def __init__(self, foo=1, bar='x'):` 形式
       2. backtrader `params = dict(foo=1, bar='x')` 形式
       3. backtrader `params = (('foo', 1), ('bar', 'x'))` 形式
    """
    tree = ast.parse(source)
    params = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == class_name):
            continue
        for item in node.body:
            # ── 1. __init__ kwargs ──
            if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                args = item.args
                defaults = args.defaults
                pad = len(args.args) - len(defaults)
                for i, arg in enumerate(args.args):
                    if arg.arg in ("self",):
                        continue
                    idx = i - pad
                    default = _const_val(defaults[idx]) if 0 <= idx < len(defaults) else None
                    params.append({"name": arg.arg, "default": default})

            # ── 2/3. params = ... 赋值 ──
            if isinstance(item, ast.Assign):
                for tgt in item.targets:
                    if isinstance(tgt, ast.Name) and tgt.id == "params":
                        val = item.value
                        # dict(foo=1, bar=2) 调用
                        if isinstance(val, ast.Call) and isinstance(val.func, ast.Name) and val.func.id == "dict":
                            for kw in val.keywords:
                                if kw.arg:
                                    params.append({"name": kw.arg, "default": _const_val(kw.value)})
                        # (('foo', 1), ('bar', 2)) 元组
                        elif isinstance(val, ast.Tuple):
                            for elt in val.elts:
                                if isinstance(elt, ast.Tuple) and len(elt.elts) >= 2:
                                    name_node, val_node = elt.elts[0], elt.elts[1]
                                    if isinstance(name_node, ast.Constant):
                                        params.append({"name": name_node.value, "default": _const_val(val_node)})
    return params


def _extract_docblock(source: str, class_name: str) -> str:
    """提取类上方的多行注释块（# ═══ ... ═══ 格式）"""
    lines = source.splitlines()
    # 找到 class 定义的行号
    class_line = None
    for i, line in enumerate(lines):
        if line.strip().startswith(f"class {class_name}"):
            class_line = i
            break
    if class_line is None:
        return ""

    # 向上搜索注释块
    block_lines = []
    i = class_line - 1
    while i >= 0:
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("#"):
            block_lines.insert(0, stripped.lstrip("# ").strip())
            i -= 1
        else:
            break
    # 过滤掉纯分隔线
    result = [l for l in block_lines if not set(l).issubset({"═", "─", "=", "-", " ", ""})]
    return "\n".join(result)


# ══════════════════════════════════════════════════════════════
# 策略文档内容构建
# ══════════════════════════════════════════════════════════════

STRATEGY_META = {
    "MajorCapitalBT": {
        "id":    "major_capital_accumulation",
        "emoji": "🏗️",
        "title": "主力建仓形态扫描器（Major Capital Accumulation）",
        "source": "bt_major_capital.py",
        "param_desc": {
            # WATCH 阶段（吸筹形态识别）
            "low_lookback":         "近 N 日低点回望窗口",
            "max_above_low_pct":    "距 N 日低点涨幅上限（超过则不算低位）",
            "ma_converge_pct":      "MA5/MA10/MA20 收敛离散度上限",
            "min_watch_days":       "滚动 30 日窗口内最少累计天数（满足 7 条件）",
            "watch_rolling_window": "WATCH 滚动窗口（默认 30 日）",
            # 信号开关
            "enable_signal_a":      "信号 A：放量大阳线突破（默认开）",
            "enable_signal_f":      "信号 F：量先萎缩后扩量（P1-B 验证后默认关）",
            "signal_a_min_rsi":     "P2-A 入场过滤：A 信号要求 RSI ≥ 该值（默认 55）",
            "signal_a_break_high_lookback": "P4 突破真实性：close 必须 > 前 N 日最高价（默认 30）",
            # ATR 自适应止损（trail 反转哲学）
            "atr_stop_k":           "硬止损：buy_price - k × ATR（仅未盈利时生效）",
            "atr_trail_k":          "Stage1 trail：浮盈 0~stage2_gain 时 k × ATR",
            "trail_stage2_gain":    "Stage2 启动浮盈门槛（默认 5%）",
            "trail_stage2_k":       "Stage2 trail k（更紧）",
            "trail_stage3_gain":    "Stage3 启动浮盈门槛（默认 15%）",
            "trail_stage3_k":       "Stage3 trail k（最紧，锁利）",
            # 加仓
            "pyramid_enabled":      "金字塔加仓开关",
            "pyramid_trigger_gain": "加仓浮盈门槛（默认 8%）",
            "pyramid_max_adds":     "最大加仓次数",
            # 自适应阈值（信号 A 的涨幅 / 量比）
            "adaptive_lookback":    "历史分位窗口（默认 60 日）",
            "vol_ratio_percentile": "放量阈值：60 日 5 日量比的 N 分位",
            "breakout_pct_percentile": "突破阈值：60 日涨幅的 N 分位",
            # 大盘过滤
            "market_filter":        "大盘过滤：idx_sh MA20<MA60 时禁止买入",
            "market_code":          "大盘指数 data feed 名称（默认 idx_sh = 上证综指）",
            "market_ma_fast":       "大盘快线周期",
            "market_ma_slow":       "大盘慢线周期",
            # 个股趋势
            "trend_filter":         "个股趋势过滤（要求 MA20 > MA60）",
            # 仓位
            "max_positions":        "最大同时持仓数",
            "position_pct":         "单仓资金占比",
        },
    },
    "MajorCapitalPumpBT": {
        "id":    "major_capital_pump",
        "emoji": "🚀",
        "title": "主力拉升策略（Major Capital Pump）",
        "source": "bt_strategies.py",
        "param_desc": {
            "pct_entry":          "入场涨幅门槛（%）",
            "vol_ratio_entry":    "入场量比门槛（倍）",
            "vol_ratio_exit":     "出场放量阴线量比门槛（倍）",
            "rsi_min":            "入场RSI下限",
            "rsi_max":            "入场RSI上限（避免追高）",
            "rsi_exit":           "RSI超买出场阈值",
            "ma_fast":            "快线MA周期",
            "ma_slow":            "慢线MA周期",
            "upper_shadow_exit":  "上影线出场阈值（长上影=主力出货）",
            "trailing_pct":       "追踪止损比例（从持仓最高点）",
            "max_positions":      "最大持仓数",
            "position_pct":       "单仓资金占比",
        },
    },
    "TrendFollowStrategy": {
        "id":    "trend_follow",
        "emoji": "📈",
        "title": "趋势跟踪策略（Trend Follow）",
        "param_desc": {
            "fast":          "快线 EMA 周期",
            "slow":          "慢线 EMA 周期",
            "trend":         "大趋势 EMA 周期",
            "trailing_pct":  "追踪止损回撤比例",
        },
    },
    "RSIReversalStrategy": {
        "id":    "rsi_reversal",
        "emoji": "🔁",
        "title": "RSI 超卖反转策略（RSI Reversal）",
        "param_desc": {
            "period":        "RSI 计算周期",
            "entry_low":     "超卖阈值（RSI 曾跌破此值）",
            "entry_cross":   "入场阈值（RSI 回升过此值时买入）",
            "take_profit":   "止盈阈值（RSI 达到此值卖出）",
            "stop_loss_pct": "固定止损比例",
        },
    },
    "BollingerRevertStrategy": {
        "id":    "bollinger_revert",
        "emoji": "〽️",
        "title": "布林带均值回归策略（Bollinger Revert）",
        "param_desc": {
            "period":        "布林带计算周期（移动均线窗口）",
            "num_std":       "布林带标准差倍数（控制通道宽度）",
            "stop_loss_pct": "固定止损比例",
            "take_profit":   "止盈目标：mid=中轨，upper=上轨",
        },
    },
}

SCREENER_META = {
    "param_desc": {
        "min_cap_yi":     "最小市值（亿元）",
        "max_cap_yi":     "最大市值（亿元），排除超大盘",
        "min_amount_wan": "最小日均成交额（万元）",
        "min_price":      "最低股价（排除低价垃圾股）",
        "max_price":      "最高股价上限",
        "exclude_st":     "是否排除 ST/*ST 股票",
        "min_list_days":  "上市最少天数（过滤次新股）",
        "top_n":          "最终保留股票数量",
        "trend_filter":   "是否启用趋势过滤（价格 > MA60）",
    }
}


def build_markdown() -> str:
    strat_src = (ROOT / "backtest" / "strategies.py").read_text(encoding="utf-8")
    screen_src = (ROOT / "backtest" / "screener.py").read_text(encoding="utf-8")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []

    # ── 标题 ──────────────────────────────────────────────
    lines += [
        "# 量化系统 · 策略说明文档",
        "",
        f"> 📅 **最后更新**: {now}  ",
        f"> 🤖 本文档由 `scripts/update_strategies_doc.py` 自动生成，策略文件变更时自动刷新，请勿手动编辑",
        "",
        "---",
        "",
        "## 目录",
        "",
        "1. [选股策略（动态筛选）](#一选股策略动态筛选)",
        "2. [交易策略总览](#二交易策略总览)",
        "3. [主力建仓形态扫描器](#三主力建仓形态扫描器major-capital-accumulation)",
        "4. [主力拉升策略](#四主力拉升策略major-capital-pump)",
        "5. [趋势跟踪策略](#五趋势跟踪策略trend-follow)",
        "6. [RSI 超卖反转策略](#六rsi-超卖反转策略rsi-reversal)",
        "7. [布林带均值回归策略](#七布林带均值回归策略bollinger-revert)",
        "8. [策略通用机制](#八策略通用机制)",
        "",
        "---",
        "",
    ]

    # ══════════════════════════════════════════════════════
    # 一、选股策略
    # ══════════════════════════════════════════════════════
    screen_params = _extract_init_params(screen_src, "DynamicScreener")

    lines += [
        "## 一、选股策略（动态筛选）",
        "",
        "所有交易策略的**股票池均来自动态筛选**，而非固定列表。",
        "系统在每次回测或扫描前自动从 AKShare 拉取全 A 股实时行情，按以下条件过滤。",
        "",
        "### 数据源",
        "",
        "| 来源 | 接口 | 说明 |",
        "| --- | --- | --- |",
        "| AKShare | `stock_zh_a_spot_em()` | 东方财富全 A 股实时行情 |",
        "| 本地缓存 | `backtest_cache/screen_result.json` | 4 小时有效，API 失败时降级使用 |",
        "| 固定兜底 | `config/settings.py WATCHLIST` | 缓存也失效时的最终降级 |",
        "",
        "### 筛选条件",
        "",
        "| 参数 | 默认值 | 说明 |",
        "| --- | --- | --- |",
    ]

    param_desc = SCREENER_META["param_desc"]
    for p in screen_params:
        desc = param_desc.get(p["name"], p["name"])
        val = p["default"]
        if isinstance(val, bool):
            val_str = "✅ 是" if val else "❌ 否"
        elif val is None:
            val_str = "—"
        else:
            val_str = f"`{val}`"
        lines.append(f"| `{p['name']}` | {val_str} | {desc} |")

    lines += [
        "",
        "### 筛选流程",
        "",
        "```",
        "全A股实时行情（5000+ 只）",
        "    ↓ 排除 ST/*ST",
        "    ↓ 市值过滤（100亿 ~ 5万亿）",
        "    ↓ 成交额过滤（日均 > 5000万）",
        "    ↓ 价格过滤（5元 ~ 500元）",
        "    ↓ PE过滤（0 < PE < 200）",
        "    ↓ 按成交额降序，取前 50 只",
        "最终股票池（≤ 50 只）→ 进入各交易策略回测",
        "```",
        "",
        "### 预设方案",
        "",
        "| 方案 | 说明 | 市值门槛 | 成交额门槛 | 数量 |",
        "| --- | --- | --- | --- | --- |",
        "| 大盘蓝筹 | 稳健型，流动性强 | > 500亿 | > 1亿/日 | 30 |",
        "| 中盘成长 | 成长型，兼顾弹性 | 100~1000亿 | > 5000万/日 | 50 |",
        "| 活跃热门 | 追踪市场热点 | > 50亿 | > 1亿/日 | 50 |",
        "| 默认筛选 | 均衡配置（推荐） | > 100亿 | > 5000万/日 | 50 |",
        "",
        "---",
        "",
    ]

    # ══════════════════════════════════════════════════════
    # 二、交易策略总览
    # ══════════════════════════════════════════════════════
    lines += [
        "## 二、交易策略总览",
        "",
        "| 策略 ID | 策略名称 | 风格 | 适合行情 | 持仓周期 |",
        "| --- | --- | --- | --- | --- |",
        "| `major_capital_accumulation` | 主力建仓形态扫描器 | 形态扫描型 | 主力静默吸筹后突破 | 中线（数周~数月）|",
        "| `major_capital_pump` | 主力拉升 | 动量型 | 牛市拉升阶段 | 短中线（数日~3周）|",
        "| `trend_follow` | 趋势跟踪 | 趋势型 | 单边牛市、结构性行情 | 中长线（数周~数月）|",
        "| `rsi_reversal` | RSI 超卖反转 | 反转型 | 震荡市、短期超跌 | 短线（数日~2周）|",
        "| `bollinger_revert` | 布林带均值回归 | 均值回归型 | 震荡市、箱体整理 | 短中线（1~3周）|",
        "",
        "> **多源验证为强制前置条件**：所有策略在生成信号前，必须通过数据完整性自检（`integrity_pass=True`），",
        "> 验证来源记录于信号日志中。验证不通过的股票直接跳过，不产生任何买卖信号。",
        "",
        "---",
        "",
    ]

    # ══════════════════════════════════════════════════════
    # 三/四/五/六/七 各策略详情
    # ══════════════════════════════════════════════════════
    bt_major_src = (ROOT / "backtest" / "bt_major_capital.py").read_text(encoding="utf-8")
    bt_strat_src = (ROOT / "backtest" / "bt_strategies.py").read_text(encoding="utf-8")

    section_num = 3
    for class_name, meta in STRATEGY_META.items():
        # 选择正确的源文件
        src_file = meta.get("source", "strategies.py")
        if src_file == "bt_major_capital.py":
            src = bt_major_src
        elif src_file == "bt_strategies.py":
            src = bt_strat_src
        else:
            src = strat_src
        params = _extract_init_params(src, class_name)
        doc_block = _extract_docblock(src, class_name)
        param_desc = meta["param_desc"]
        emoji = meta["emoji"]
        title = meta["title"]
        strat_id = meta["id"]
        anchor = title.lower().replace(" ", "-").replace("（", "").replace("）", "").replace("(", "").replace(")", "")

        lines += [
            f"## {'一二三四五六七八九十'[section_num - 1]}、{emoji} {title}",
            "",
            f"**策略 ID**: `{strat_id}`",
            "",
        ]

        # 从注释块提取核心逻辑说明
        if doc_block:
            doc_lines = [l for l in doc_block.splitlines() if l.strip()]
            # 分组：核心逻辑 / 为什么有效 / 修复
            in_entry = False
            entry_lines = []
            exit_lines = []
            why_lines = []
            fix_lines = []
            cur_section = None
            for dl in doc_lines:
                if "入场" in dl or "entry" in dl.lower():
                    cur_section = "entry"
                elif "出场" in dl or "exit" in dl.lower():
                    cur_section = "exit"
                elif "为什么有效" in dl or "修复" in dl:
                    cur_section = "why"
                elif cur_section == "entry":
                    entry_lines.append(dl)
                elif cur_section == "exit":
                    exit_lines.append(dl)
                elif cur_section == "why":
                    why_lines.append(dl)

            lines += ["### 核心逻辑", ""]
            # 提取所有关键说明行
            all_doc = [l for l in doc_block.splitlines() if l.strip() and
                       not set(l.strip()).issubset({"═", "─", "=", "-"})]
            for dl in all_doc:
                if dl.strip():
                    lines.append(f"> {dl.strip()}")
            lines.append("")

        # 参数表 — 只展示在 param_desc 中显式记录的参数（避免遗留杂项混入）
        documented = [p for p in params if p["name"] in param_desc]
        lines += [
            "### 参数说明（已记录的核心参数）",
            "",
            f"_完整 {len(params)} 个参数见源码 `params = dict(...)`；下表仅列出 {len(documented)} 个文档化参数_",
            "",
            "| 参数 | 默认值 | 含义 |",
            "| --- | --- | --- |",
        ]
        # 保持 param_desc 中声明的顺序（更可读）
        ordered_names = list(param_desc.keys())
        params_by_name = {p["name"]: p for p in documented}
        for name in ordered_names:
            p = params_by_name.get(name)
            if not p:
                continue
            desc = param_desc[name]
            val = p["default"]
            if isinstance(val, bool):
                val_str = "`True`" if val else "`False`"
            elif isinstance(val, str):
                val_str = f'`"{val}"`'
            elif val is None:
                val_str = "—"
            else:
                val_str = f"`{val}`"
            lines.append(f"| `{p['name']}` | {val_str} | {desc} |")

        lines.append("")

        # 策略专属详情
        if class_name == "MajorCapitalBT":
            lines += [
                "### 策略定位（P9/P9b 验证后修正）",
                "",
                "本策略是 **「主力建仓形态扫描器」**，不是连续盈利策略。",
                "",
                "- **形态识别**：捕捉「主力静默吸筹（横盘地量+均线收敛+RSI 中性）→ 放量大阳突破前 30 日高」",
                "- **罕见但真实**：2018-2026 共 8 年验证，该形态仅在 2024-09~2025-12 等少数时段密集出现",
                "- **预期 silent 长**：30-60% 时间无信号是设计特性（形态本身罕见）",
                "- **评估口径**：形态命中精确度 + 命中后 5/10/30 日收益，不看年化",
                "",
                "### 两阶段信号逻辑",
                "",
                "```",
                "阶段 1 — WATCH（吸筹形态识别，7 条件 + 滚动累计）",
                "  ① near_low ≤ 15%       ：股价距 60 日低点 ≤ 15%（低位）",
                "  ② below_high ≥ 25%     ：股价距 120 日高点 ≥ 25%（远离前期高位）",
                "  ③ ma_convergence ≤ 1%  ：MA5/MA10/MA20 三线纠缠",
                "  ④ |MA20 斜率| ≤ 0.5%   ：均线趋势平缓",
                "  ⑤ RSI(14) ∈ [35, 65]  ：非超买非超卖",
                "  ⑥ 阳阴量比 ≥ 0.9       ：上涨日量能不弱于下跌日（暗示主力护盘）",
                "  ⑦ vol_compression ≤ 60% ：地量特征",
                "  ★ 30 日滚动窗口内 ≥ 15 天同时满足 → 确认 WATCH",
                "",
                "阶段 2 — BUY（放量真突破，仅信号 A）",
                "  • 涨幅 ≥ 自适应阈值（60 日涨幅 75 分位 + 3% 下限）",
                "  • 量比 ≥ 自适应阈值（60 日 5日量比 85 分位 + 2x 下限）",
                "  • 收盘在当日上半段（≥ 50% 振幅位置）",
                "  • RSI ≥ 55（P2-A：低 RSI 入场组 final 均亏 -1.83%）",
                "  • close > max(high[-1..-30])（P4：必须破前 30 日真高点，否则随机大阳被砍）",
                "",
                "  ✅ 上层入场过滤",
                "  • RSI ≤ 70（不追超买）",
                "  • 单日涨幅 ≤ 8%（不追涨停）",
                "  • 个股 MA20 > MA60（趋势确认）",
                "  • 大盘 idx_sh MA20 > MA60（market_filter）",
                "",
                "  ❌ 信号 F（量萎缩后扩量）已默认关闭",
                "    P1-A 诊断：5 日胜率 78.6% 但 final 均亏 1.78%（假突破回吐）",
                "    P1-B 验证：关闭后 4 折 OOS 累计 -7.83% → +12.29%（Δ +20.12pp）",
                "",
                "持仓管理：",
                "  • 金字塔加仓：浮盈 ≥ 8% 且创新高 → 加 0.5x 初始仓位（最多 2 次）",
                "",
                "出场（ATR 反转 trail 哲学：越赚越紧）：",
                "  浮盈区间        trail k    含义",
                "  ────────────  ─────────  ──────────────",
                "  0 ~ 5%        2.0 × ATR   stage1（容差较宽）",
                "  5% ~ 15%      1.5 × ATR   stage2（开始锁利）",
                "  ≥ 15%         1.0 × ATR   stage3（最紧，保大单）",
                "",
                "  + 硬止损：buy_price - 2 × ATR（仅未盈利时生效，防 gap-down）",
                "  + MA20 反转：连续 N 日跌破 MA20",
                "```",
                "",
                "### 适用场景与限制",
                "",
                "- ✅ **主力静默吸筹后突破期**：核心目标形态",
                "- ✅ **趋势 + 突破共振**：大盘 trending + 个股突破长期平台",
                "- ❌ **熊市 / 震荡市 / 急涨急跌**：形态本身不存在，策略 silent（设计特性）",
                "- ❌ **不要追求连续盈利或年化** — 评估应基于形态命中后的真实走势",
                "",
                "### 当前默认值的验证追溯",
                "",
                "见仓库 `CLAUDE.md` 决策追溯链（P0/P1-A/P1-B/P2-A/P4/P5/P6/P7/P9）—",
                "所有默认参数都附 4-10 折 OOS 数据 + bootstrap CI 论证。",
                "",
            ]
        elif class_name == "MajorCapitalPumpBT":
            lines += [
                "### 信号逻辑",
                "",
                "```",
                "买入条件（同时满足）：",
                "  ① 当日阳线 + 涨幅 ≥ 3%",
                "  ② 量比 ≥ 1.5x（成交放量）",
                "  ③ 收盘价 > MA20",
                "  ④ RSI(14) 在 50~70 范围内（避免追顶）",
                "  ⑤ MACD DIF ≥ 0（多头动量确认）",
                "",
                "卖出条件（满足其一）：",
                "  ① 主力出货：RSI>85 + 长上影线（上影线占全范围≥30%）",
                "  ② 追踪止损：从持仓最高点回撤≥10%",
                "  ③ 趋势转弱：收盘跌破MA5且收阴线",
                "  ④ 放量阴线：量比≥2x 的下跌阴线",
                "```",
                "",
                "### 适用场景",
                "",
                "- ✅ **牛市拉升阶段**：捕捉主力拉升中继信号，快速跟进",
                "- ✅ **强势股追涨**：量价配合 + MACD 确认避免假突破",
                "- ❌ **震荡市/熊市**：RSI 和量比门槛无法有效过滤震荡假信号",
                "",
            ]
        elif class_name == "TrendFollowStrategy":
            lines += [
                "### 信号逻辑",
                "",
                "```",
                "买入条件（同时满足）：",
                "  ① EMA(fast) 上穿 EMA(slow)    ← 短期动量转强",
                "  ② 当日收盘价 > EMA(trend)      ← 确认大趋势向上",
                "",
                "卖出条件（满足其一）：",
                "  ① 追踪止损：从持仓最高点回撤 ≥ trailing_pct",
                "  ② EMA(fast) 下穿 EMA(slow)    ← 趋势转弱",
                "```",
                "",
                "### 适用场景",
                "",
                "- ✅ **牛市、单边上涨行情**：大趋势过滤（EMA60）避免在熊市频繁买入",
                "- ✅ **中长线持仓**：追踪止损自动锁定利润，不会过早卖出",
                "- ❌ **震荡市**：频繁金叉死叉导致多次小额亏损（需配合布林带/RSI策略）",
                "",
            ]
        elif class_name == "RSIReversalStrategy":
            lines += [
                "### 信号逻辑",
                "",
                "```",
                "买入条件（同时满足）：",
                "  ① RSI 曾跌破 entry_low（确认超卖）",
                "  ② RSI 从 < entry_cross 回升到 ≥ entry_cross（确认反弹动量）",
                "  ③ 当日收阳线（close > open）",
                "",
                "卖出条件（满足其一）：",
                "  ① 止盈：RSI ≥ take_profit",
                "  ② 止损：持仓亏损 ≥ stop_loss_pct",
                "```",
                "",
                "### 适用场景",
                "",
                "- ✅ **震荡市、短期超跌**：捕捉技术性反弹，快进快出",
                "- ✅ **熊市中的反弹波段**：RSI 过滤保证入场时动量已回升",
                "- ❌ **强趋势下跌中**：超卖后继续超卖，止损频繁触发",
                "",
            ]
        elif class_name == "BollingerRevertStrategy":
            lines += [
                "### 信号逻辑",
                "",
                "```",
                "布林带计算：",
                "  中轨(MID)  = 20日移动均线",
                "  上轨(UPPER) = MID + 2σ",
                "  下轨(LOWER) = MID - 2σ",
                "",
                "买入条件（同时满足）：",
                "  ① 收盘价 ≤ 下轨（统计超卖，价格偏离度过大）",
                "  ② 当日收阳线（close > open）← 确认反弹启动，非接飞刀",
                "",
                "卖出条件（满足其一）：",
                "  ① 止盈：收盘价 ≥ 中轨（均值回归完成）",
                "  ② 止损：持仓亏损 ≥ stop_loss_pct",
                "```",
                "",
                "### 适用场景",
                "",
                "- ✅ **震荡行情、箱体整理**：均值回归特性在横盘市场表现最佳",
                "- ✅ **与 RSI 互补**：RSI 看动量维度，布林带看价格偏离度维度",
                "- ❌ **单边趋势行情**：下轨被持续跌穿，止损频繁；上涨趋势中信号很少",
                "",
            ]

        section_num += 1

    # ══════════════════════════════════════════════════════
    # 六、通用机制
    # ══════════════════════════════════════════════════════
    lines += [
        "---",
        "",
        "## 八、策略通用机制",
        "",
        "### 多源验证门控（强制前置）",
        "",
        "```python",
        "# 所有策略入口 generate_signals() 均执行以下检查",
        "if extra.get('integrity_pass') is not True:",
        "    return []  # 数据不完整，跳过此股票",
        "```",
        "",
        "每只股票在进入策略前必须通过数据完整性验证：",
        "- `integrity_pass = True`：来自 AKShare 的完整 OHLCV 数据",
        "- 缺少关键字段（开高低收量）→ 直接跳过，不产生信号",
        "- 验证来源记录在信号的 `reason` 字段中",
        "",
        "### 仓位管理",
        "",
        "- 每只股票分配等额资金（总资金 ÷ 最大持仓数）",
        "- 同一时刻最多持有 N 只股票（由回测引擎的 `max_positions` 控制）",
        "- 已持仓的股票不重复买入",
        "",
        "### 胜率计算（FIFO 配对）",
        "",
        "```",
        "每笔卖出与对应买入按先进先出（FIFO）配对：",
        "  买入队列: [BUY@10, BUY@12, BUY@11]",
        "  卖出     SELL@13 → 配对 BUY@10 → 盈利 +30%  ✅ 胜",
        "  卖出     SELL@9  → 配对 BUY@12 → 亏损 -25%  ❌ 负",
        "  胜率 = 盈利笔数 / 总配对笔数",
        "```",
        "",
        "### 资产快照与收益计算",
        "",
        "- 每个交易日记录总资产快照（现金 + 持仓市值）",
        "- **区间收益**：`(期末总资产 - 初始资金) / 初始资金`",
        "- **年化收益**：`(1 + 区间收益率)^(365/持仓天数) - 1`",
        "- **精确金额**：存储 `abs_values`（实际元数），避免比例→金额转换的精度损失",
        "",
        "---",
        "",
        f"*本文档由系统自动维护，最后更新于 {now}*",
    ]

    return "\n".join(lines) + "\n"


# ══════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════

def run(force: bool = False) -> bool:
    """
    生成文档。
    force=True：强制重新生成；False：仅在策略文件变更时生成。
    返回 True 表示文档已更新，False 表示无需更新。
    """
    if not force and not needs_update():
        return False

    try:
        content = build_markdown()
        OUTPUT.write_text(content, encoding="utf-8")
        _save_hash()
        print(f"[策略文档] ✅ STRATEGIES.md 已更新 ({datetime.now().strftime('%H:%M:%S')})")
        return True
    except Exception as e:
        print(f"[策略文档] ❌ 生成失败: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    force = "--force" in sys.argv
    updated = run(force=force)
    if not updated:
        print("[策略文档] ℹ️  策略文件未变更，跳过生成（使用 --force 强制刷新）")
