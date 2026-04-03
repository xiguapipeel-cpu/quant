"""
回测运行入口 v3
改进点：
  1. 使用动态选股器替代固定WATCHLIST（从全A股筛选）
  2. 验证门控数据从AKShare实时拉取
  3. 回测结果持久化到 backtest_results.json
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from backtest.data_loader import BacktestDataLoader
from backtest.engine import BacktestEngine, BacktestConfig
from backtest.strategies import BUILTIN_STRATEGIES
from backtest.screener import DynamicScreener, SCREEN_PRESETS
from backtest.bt_report import BacktestReporter
from utils.logger import setup_logger

logger = setup_logger("bt_runner")

# 已迁移至 MySQL，保留路径仅用于兼容
RESULTS_DB = Path("backtest_results.json")


# ══════════════════════════════════════════════════════════
# 真实 extra_data 构建
# ══════════════════════════════════════════════════════════

async def build_extra_data(
    watchlist:      list[dict],
    start:          str,
    end:            str,
    scan_results:   list[dict] | None = None,
    all_bars:       dict[str, list] | None = None,
    strategy_name:  str = "",
) -> dict[str, dict]:
    """
    为回测构建 extra_data（验证门控的输入）
    优先级：
    1. scan_results → 用扫描结果
    2. AKShare 实时数据
    3. 缓存日线降级

    strategy_name 用于判断策略是否需要 PE/市值，
    不需要的策略（如主力建仓）只要有日线数据即可通过验证。
    """
    # 判断策略是否需要 PE/市值
    strategy_cls = BUILTIN_STRATEGIES.get(strategy_name)
    need_pe  = strategy_cls.requires_pe if strategy_cls else False
    need_cap = strategy_cls.requires_market_cap if strategy_cls else False
    if scan_results:
        logger.info("[回测] 使用扫描结果作为验证门控数据")
        extra = {}
        for r in scan_results:
            extra[r["code"]] = {
                "integrity_pass":  r.get("integrity_pass", False),
                "missing_fields":  r.get("missing_fields", []),
                "pe":              r.get("pe"),
                "market_cap":      r.get("market_cap"),
                "verified_sources": (r.get("validation") or {}).get("pe", {}).get("sources", []),
            }
        return extra

    # AKShare 实时数据
    akshare_available = False
    ak_df = None
    try:
        import akshare as ak
        loop = asyncio.get_event_loop()
        ak_df = await loop.run_in_executor(None, ak.stock_zh_a_spot_em)
        akshare_available = ak_df is not None and not ak_df.empty
        if akshare_available:
            logger.info("[回测] AKShare实时数据拉取成功")
    except ImportError:
        logger.warning("[回测] akshare未安装")
    except Exception as e:
        logger.warning(f"[回测] AKShare拉取失败: {e}")

    all_bars = all_bars or {}
    extra = {}

    for stock in watchlist:
        code = stock["code"]

        if akshare_available and ak_df is not None:
            row = ak_df[ak_df["代码"] == code]
            if row is not None and not row.empty:
                r   = row.iloc[0]
                pe  = _safe_float(r.get("市盈率-动态"))
                cap = _safe_float(r.get("总市值"))
                cap_yi = cap / 1e8 if cap else None

                missing = []
                if need_pe and not (pe and pe > 0):
                    missing.append("PE")
                if need_cap and not (cap_yi and cap_yi > 0):
                    missing.append("市值")

                extra[code] = {
                    "integrity_pass":  len(missing) == 0,
                    "missing_fields":  missing,
                    "pe":              pe if (pe and pe > 0) else None,
                    "market_cap":      cap_yi if (cap_yi and cap_yi > 0) else None,
                    "verified_sources": ["AKShare/东方财富"],
                }
                continue

        # 降级：缓存日线
        bars = all_bars.get(code)
        if bars and len(bars) > 0:
            extra[code] = {
                "integrity_pass": True,
                "missing_fields": [],
                "pe":             None,
                "market_cap":     None,
                "verified_sources": ["本地缓存日线"],
            }
        else:
            extra[code] = {
                "integrity_pass": False,
                "missing_fields": ["无任何数据"],
                "pe": None, "market_cap": None,
                "verified_sources": [],
            }

    passed  = sum(1 for v in extra.values() if v.get("integrity_pass") is True)
    excluded= sum(1 for v in extra.values() if v.get("integrity_pass") is False)
    logger.info(f"[回测验证] 通过: {passed} | 排除: {excluded}")
    return extra


def _safe_float(val) -> float | None:
    try:
        v = float(val)
        return None if v != v else v
    except (TypeError, ValueError):
        return None


# ══════════════════════════════════════════════════════════
# 主回测函数
# ══════════════════════════════════════════════════════════

async def run_backtest(
    strategy_name:  str   = "trend_follow",
    start:          str   = "2022-01-01",
    end:            str   = "2024-12-31",
    initial_cash:   float = 1_000_000.0,
    watchlist:      list  = None,
    scan_results:   list  = None,
    screen_preset:  str   = "default",    # 选股预设
    log_fn                = None,
) -> dict:
    """
    运行单个策略回测。
    如果 watchlist 为空，自动调用动态选股器筛选股票。
    """

    def log(msg):
        logger.info(msg)
        if log_fn:
            log_fn(msg)

    # ── Step0: 动态选股（如果没有提供watchlist）───────────
    if not watchlist:
        log(f"启动动态选股 | 预设: {screen_preset}")
        preset_params = SCREEN_PRESETS.get(screen_preset, SCREEN_PRESETS["default"])["params"]
        screener = DynamicScreener(**preset_params)
        screened = await screener.screen(use_cache_hours=4)
        if screened:
            watchlist = screened
            log(f"动态选股完成 | 筛选出 {len(watchlist)} 只股票")
        else:
            from config.settings import WATCHLIST as FALLBACK
            watchlist = FALLBACK
            log(f"选股失败，降级使用固定股票池 ({len(watchlist)}只)")

    log(f"回测启动 | 策略={strategy_name} | {start}~{end} | 初始资金¥{initial_cash:,.0f} | 股票={len(watchlist)}只")

    # ── Step1: 加载历史日线 ────────────────────────────────
    loader   = BacktestDataLoader()
    all_bars = await loader.load_all_stocks(watchlist, start, end)
    if not all_bars:
        msg = "历史日线数据加载失败"
        logger.error(msg)
        return {"error": msg}

    loaded = len(all_bars)
    log(f"日线数据加载完成 | 成功: {loaded}/{len(watchlist)} 只")

    # ── Step2: 构建验证门控数据 ────────────────────────────
    log("构建验证门控数据...")
    extra_data = await build_extra_data(watchlist, start, end, scan_results, all_bars, strategy_name)

    # ── Step3: 加载PE序列（PE策略需要）────────────────────
    for stock in watchlist:
        code = stock["code"]
        if code not in all_bars:
            continue
        pe_series = await loader.load_pe_series(code, start, end)
        if pe_series:
            extra_data.setdefault(code, {})["pe_series"] = pe_series

    # ── Step4: 初始化策略 ──────────────────────────────────
    strategy_cls = BUILTIN_STRATEGIES.get(strategy_name)
    if not strategy_cls:
        return {"error": f"未知策略: {strategy_name}，可选: {list(BUILTIN_STRATEGIES)}"}
    strategy = strategy_cls()

    v_pass = [code for code, v in extra_data.items() if v.get("integrity_pass") is True]
    v_excl = [code for code, v in extra_data.items() if v.get("integrity_pass") is False]
    log(f"验证门控 | 通过: {len(v_pass)}只 | 排除: {len(v_excl)}只")

    # ── Step5: 运行回测引擎 ────────────────────────────────
    # 根据股票池规模 + 资金量综合决定最大持仓
    n_valid = len(v_pass)
    if n_valid >= 100:
        max_pos = 20
        pos_pct = 0.10
    elif n_valid >= 60:
        max_pos = 15
        pos_pct = 0.12
    elif n_valid >= 30:
        max_pos = 10
        pos_pct = 0.15
    else:
        max_pos = 5
        pos_pct = 0.30

    # 资金规模修正：小资金减少仓位数，提高单仓比例，确保每仓不低于 ¥20,000
    min_per_slot = 20000
    if initial_cash / max_pos < min_per_slot:
        max_pos = max(2, int(initial_cash / min_per_slot))
        pos_pct = round(1.0 / max_pos, 2)
    cfg    = BacktestConfig(initial_cash=initial_cash, max_positions=max_pos, max_position_pct=pos_pct)
    log(f"仓位配置 | 最大持仓: {max_pos}只 | 单股上限: {pos_pct:.0%}")
    engine = BacktestEngine(cfg)
    log("开始逐日撮合模拟...")
    result = engine.run(strategy, all_bars, extra_data)

    # ── Step6: 生成报告 ────────────────────────────────────
    reporter = BacktestReporter()
    paths    = reporter.generate(result)

    metrics = {
        "strategy":          result.strategy_name,
        "start":             result.start_date,
        "end":               result.end_date,
        "initial_cash":      result.initial_cash,
        "final_value":       result.final_value,
        "total_return":      f"{result.total_return*100:+.2f}%",
        "annualized_return": f"{result.annualized_return*100:+.2f}%",
        "max_drawdown":      f"{result.max_drawdown*100:.2f}%",
        "sharpe_ratio":      round(result.sharpe_ratio, 2),
        "win_rate":          f"{result.win_rate*100:.1f}%",
        "profit_factor":     round(result.profit_factor, 2),
        "total_trades":      result.total_trades,
        "period_profit":     round(result.final_value - result.initial_cash, 2),
        "period_profit_fmt": f"{result.final_value - result.initial_cash:+,.2f} 元",
        "final_value_fmt":   f"{result.final_value:,.2f} 元",
        "verified_pass":     len(v_pass),
        "verified_excl":     len(v_excl),
        "per_stock":         result.per_stock,
        "stock_count":       len(watchlist),
    }

    log(f"回测完成 | 年化={metrics['annualized_return']} "
        f"夏普={metrics['sharpe_ratio']} 胜率={metrics['win_rate']} "
        f"交易={metrics['total_trades']}次")

    # ── Step7: 持久化 ────────────────────────────────────
    equity_data = {
        "dates":      [s.date for s in result.daily_snapshots],
        "values":     [round(s.total_value / initial_cash, 4) for s in result.daily_snapshots],
        "abs_values": [round(s.total_value, 2) for s in result.daily_snapshots],
    }

    # 构建交易详情（配对买卖）
    code_name = {s["code"]: s.get("name", s["code"]) for s in watchlist}
    from collections import defaultdict
    buy_queues = defaultdict(list)
    trades_paired = []
    for t in result.trades:
        if t.action == "BUY":
            buy_queues[t.code].append(t)
        elif t.action == "SELL":
            queue = buy_queues.get(t.code, [])
            bt = queue.pop(0) if queue else None
            pnl = round((t.price - bt.price) * t.shares - t.commission - bt.commission, 2) if bt else 0
            pnl_pct = round((t.price / bt.price - 1) * 100, 2) if bt else 0
            trades_paired.append({
                "code":       t.code,
                "name":       code_name.get(t.code, t.code),
                "buy_date":   bt.date if bt else "",
                "buy_price":  round(bt.price, 2) if bt else 0,
                "sell_date":  t.date,
                "sell_price": round(t.price, 2),
                "shares":     t.shares,
                "pnl":        pnl,
                "pnl_pct":    pnl_pct,
                "buy_reason": bt.reason if bt else "",
                "sell_reason": t.reason,
            })
    # 还没平仓的持仓
    for code, buys in buy_queues.items():
        for bt in buys:
            trades_paired.append({
                "code":       bt.code,
                "name":       code_name.get(bt.code, bt.code),
                "buy_date":   bt.date,
                "buy_price":  round(bt.price, 2),
                "sell_date":  "（持仓中）",
                "sell_price": 0,
                "shares":     bt.shares,
                "pnl":        0,
                "pnl_pct":    0,
                "buy_reason": bt.reason,
                "sell_reason": "",
            })

    # 交易记录按时间倒序排列（最新的交易在前）
    def _trade_sort_key(t):
        # 持仓中的排最前面，其余按卖出日期倒序
        sd = t.get("sell_date", "")
        if sd == "（持仓中）":
            return "9999-99-99"  # 持仓中排最前
        return sd if sd else t.get("buy_date", "")

    trades_paired.sort(key=_trade_sort_key, reverse=True)

    # 持久化到 MySQL
    try:
        from db.backtest_dao import save_backtest
        await save_backtest(
            strategy=strategy_name,
            start=start,
            end=end,
            initial_cash=initial_cash,
            metrics=metrics,
            equity_data=equity_data,
            trades_data=trades_paired,
            is_real=True,
        )
    except Exception as e:
        logger.warning(f"[回测] MySQL持久化失败，降级写JSON: {e}")
        _persist_result(strategy_name, start, end, initial_cash, metrics, equity_data, trades_paired)

    return {"metrics": metrics, "report_paths": paths, "result": result}


def _sanitize_for_json(obj):
    import math
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isinf(obj) or math.isnan(obj):
            return 0.0
    return obj


def _persist_result(strategy_name, start, end, cash, metrics, equity_data=None, trades_data=None):
    records = _load_results()
    new_id  = max((r["id"] for r in records), default=0) + 1
    record = {
        "id":       new_id,
        "strategy": strategy_name,
        "start":    start,
        "end":      end,
        "cash":     cash,
        "metrics":  _sanitize_for_json(metrics),
        "time":     datetime.now().strftime("%Y-%m-%d %H:%M"),
        "is_real":  True,
    }
    if equity_data:
        record["equity"] = equity_data
    if trades_data:
        record["trades"] = trades_data
    records.insert(0, record)
    records = _sanitize_for_json(records)
    RESULTS_DB.write_text(
        json.dumps(records[:50], ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    logger.info(f"[回测] 结果已持久化 id={new_id}")


def _load_results() -> list:
    if RESULTS_DB.exists():
        try:
            return json.loads(RESULTS_DB.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def load_backtest_results() -> list:
    """兼容旧同步调用，优先读 JSON 降级"""
    return _load_results()


async def load_backtest_results_async() -> list:
    """异步版本，优先从 MySQL 读取"""
    try:
        from db.backtest_dao import load_backtest_results as _load_from_db
        return await _load_from_db()
    except Exception as e:
        logger.warning(f"[回测] MySQL读取失败，降级读JSON: {e}")
        return _load_results()


# ══════════════════════════════════════════════════════════
# 多策略对比
# ══════════════════════════════════════════════════════════

async def run_strategy_comparison(
    start:        str   = "2022-01-01",
    end:          str   = "2024-12-31",
    initial_cash: float = 1_000_000.0,
    watchlist:    list  = None,
    scan_results: list  = None,
    screen_preset: str  = "default",
) -> list[dict]:
    """并行运行所有内置策略"""
    logger.info(f"策略对比回测：{len(BUILTIN_STRATEGIES)}个策略")

    tasks = [
        run_backtest(name, start, end, initial_cash, watchlist, scan_results, screen_preset)
        for name in BUILTIN_STRATEGIES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    comparison = []
    for name, r in zip(BUILTIN_STRATEGIES, results):
        if isinstance(r, Exception) or not r or "error" in r:
            comparison.append({"strategy": name, "error": str(r)})
        else:
            comparison.append(r["metrics"])

    comparison.sort(
        key=lambda x: float(str(x.get("annualized_return", "0%")).replace("%", "").replace("+", "")),
        reverse=True,
    )
    return comparison


# ══════════════════════════════════════════════════════════
# 参数扫描
# ══════════════════════════════════════════════════════════

async def run_parameter_scan(
    strategy_name: str  = "trend_follow",
    param_grid:    dict = None,
    start:         str  = "2022-01-01",
    end:           str  = "2024-12-31",
    watchlist:     list = None,
    scan_results:  list = None,
) -> list[dict]:
    import itertools
    param_grid = param_grid or {}
    if not watchlist:
        screener = DynamicScreener()
        watchlist = await screener.screen()

    keys   = list(param_grid.keys())
    combos = list(itertools.product(*param_grid.values()))
    logger.info(f"参数扫描: {len(combos)}组参数 | 策略={strategy_name}")

    loader   = BacktestDataLoader()
    all_bars = await loader.load_all_stocks(watchlist, start, end)
    extra    = await build_extra_data(watchlist, start, end, scan_results, all_bars)

    scan_results_out = []
    for combo in combos:
        params   = dict(zip(keys, combo))
        strategy = BUILTIN_STRATEGIES[strategy_name](**params)
        result   = BacktestEngine(BacktestConfig()).run(strategy, all_bars, extra)
        scan_results_out.append({
            "params":            params,
            "total_return":      round(result.total_return, 4),
            "annualized_return": round(result.annualized_return, 4),
            "max_drawdown":      round(result.max_drawdown, 4),
            "sharpe_ratio":      round(result.sharpe_ratio, 2),
            "win_rate":          round(result.win_rate, 4),
        })

    scan_results_out.sort(key=lambda x: x["sharpe_ratio"], reverse=True)
    return scan_results_out


# ── CLI ──────────────────────────────────────────────────
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "single"
    if mode == "compare":
        asyncio.run(run_strategy_comparison())
    elif mode == "scan":
        asyncio.run(run_parameter_scan(
            "trend_follow", param_grid={"fast": [5, 10, 15], "slow": [20, 30, 60]}
        ))
    else:
        asyncio.run(run_backtest("trend_follow"))
