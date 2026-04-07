"""
统一 Backtrader 回测运行器 — 供 Web API 调用

支持所有 5 个策略，返回与原引擎完全兼容的 metrics / equity_data / trades_paired 格式。

用法:
    from backtest.bt_runner import run_for_web
    result = await run_for_web("trend_follow", "2025-01-01", "2026-03-31", 100000)
"""

import asyncio
from collections import defaultdict
from datetime import datetime as _dt, timedelta

import backtrader as bt

from backtest.bt_major_capital import (
    MajorCapitalBT,
    load_cache_data,
    get_all_cached_stocks,
)
from backtest.bt_strategies import (
    BT_STRATEGY_MAP,
    TrendFollowBT,
    RSIReversalBT,
    BollingerRevertBT,
    MajorCapitalPumpBT,
)


# 完整策略注册表
ALL_BT_STRATEGIES = {
    **BT_STRATEGY_MAP,
    "major_capital_accumulation": MajorCapitalBT,
}


def _calc_positions(n_stocks: int, cash: float):
    """根据股票数量和资金量计算最大持仓数和单仓比例"""
    if n_stocks >= 100:
        max_pos = 20
    elif n_stocks >= 60:
        max_pos = 15
    elif n_stocks >= 30:
        max_pos = 10
    else:
        max_pos = 5

    min_per_slot = 20000
    if cash / max_pos < min_per_slot:
        max_pos = max(2, int(cash / min_per_slot))

    pos_pct = round(1.0 / max_pos, 2)
    return max_pos, pos_pct


async def run_for_web(
    strategy_name: str,
    start: str,
    end: str,
    cash: float,
    log_fn=None,
    screen_preset: str = "default",
) -> dict:
    """
    运行 Backtrader 回测，返回与 runner.run_backtest() 兼容的字典。

    设计原则（滑动窗口 / 无未来函数）：
      - 数据层：全量加载缓存中所有股票，不做预筛选
      - 策略层：由 MajorCapitalBT._screen_stock() 在每根 bar 内实时过滤
      - 交易层：按 confidence 排序，仓位管理规则控制下执行买入
      - 仓位上限：加载完成后基于 loaded 数量计算，而非预估数量

    返回:
        {"metrics": {...}, "equity_data": {...}, "trades_paired": [...]}
        或 {"error": "..."} 失败时
    """

    def log(msg, level="info"):
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    # ── 策略查找 ──
    strat_cls = ALL_BT_STRATEGIES.get(strategy_name)
    if strat_cls is None:
        return {"error": f"未知策略: {strategy_name}"}

    log(f"Backtrader 引擎启动 | 策略: {strategy_name}")

    # ── 全量加载缓存股票（不预筛选，由策略内部动态过滤） ──
    all_stocks = get_all_cached_stocks(start, end)
    if not all_stocks:
        return {"error": "无可用股票缓存数据，请先运行数据采集"}

    n_stocks = len(all_stocks)
    log(f"数据源: 缓存中发现 {n_stocks} 只股票，正在加载...")

    # ── 构建 Cerebro ──
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.broker.set_slippage_perc(0.002)

    # 数据预热：从 start 前1年加载（让策略在 start 时已有足够历史 bar）
    warmup_start = (_dt.strptime(start, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    loaded = 0
    code_name_map = {}
    loaded_codes = []
    for stock in all_stocks:
        code = stock['code']
        df = load_cache_data(code, warmup_start, end)
        if df is None:
            df = load_cache_data(code, start, end)
        if df is None or len(df) < 30:
            continue
        data = bt.feeds.PandasData(
            dataname=df, name=code, datetime=None,
            open='open', high='high', low='low', close='close',
            volume='volume', openinterest=-1,
        )
        cerebro.adddata(data)
        code_name_map[code] = stock.get('name', code)
        loaded_codes.append(code)
        loaded += 1

    if loaded == 0:
        return {"error": "无有效历史数据可加载"}

    # ── 仓位上限：加载完成后基于实际数量计算（滑动窗口原则） ──
    max_pos, pos_pct = _calc_positions(loaded, cash)
    log(f"实际加载 {loaded}/{n_stocks} 只股票 | 最大持仓={max_pos} 只, 单仓={pos_pct:.0%}")

    # ── 策略参数 ──
    strat_kwargs = {
        'max_positions': max_pos,
        'position_pct':  pos_pct,
    }
    # 主力建仓策略需要额外的选股参数
    if strategy_name == "major_capital_accumulation":
        strat_kwargs['screen_enabled'] = True

    cerebro.addstrategy(strat_cls, **strat_kwargs)

    # ── 分析器 ──
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.025)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='time_return')

    log("正在运行 Backtrader 回测...")

    # cerebro.run() 是同步阻塞的，放到线程池
    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, cerebro.run)
    strat = results[0]

    # ══════════════════════════════════════════════════════════
    # 提取结果
    # ══════════════════════════════════════════════════════════

    final_val = cerebro.broker.getvalue()
    total_return = (final_val - cash) / cash

    # 分析器
    sharpe_a = strat.analyzers.sharpe.get_analysis()
    sharpe = sharpe_a.get('sharperatio') or 0

    dd_a = strat.analyzers.drawdown.get_analysis()
    max_dd = dd_a.get('max', {}).get('drawdown', 0) / 100

    # 年化收益
    time_returns = strat.analyzers.time_return.get_analysis()
    n_days = len(time_returns) if time_returns else 1
    annualized = (1 + total_return) ** (252 / max(n_days, 1)) - 1

    period_profit = round(final_val - cash, 2)

    # 选股统计（主力建仓策略有内部选股）
    screened_pass = 0
    screened_fail = 0
    if hasattr(strat, '_screened'):
        screened_pass = sum(1 for v in strat._screened.values() if v)
        screened_fail = sum(1 for v in strat._screened.values() if not v)
    else:
        screened_pass = loaded
        screened_fail = 0

    # ── metrics 占位（trades_paired 统计后再填充） ──
    metrics = {
        "strategy":          strategy_name,
        "start":             start,
        "end":               end,
        "initial_cash":      cash,
        "final_value":       round(final_val, 2),
        "total_return":      f"{total_return*100:+.2f}%",
        "annualized_return": f"{annualized*100:+.2f}%",
        "max_drawdown":      f"{max_dd*100:.2f}%",
        "sharpe_ratio":      round(sharpe, 2),
        "win_rate":          "0.0%",           # 下方从 trades_paired 重新计算
        "profit_factor":     0.0,              # 下方从 trades_paired 重新计算
        "total_trades":      0,                # 下方从 trades_paired 重新计算
        "period_profit":     period_profit,
        "period_profit_fmt": f"{period_profit:+,.2f} 元",
        "final_value_fmt":   f"{final_val:,.2f} 元",
        "verified_pass":     screened_pass,
        "verified_excl":     screened_fail,
        "per_stock":         {},
        "stock_count":       loaded,
        "stock_pool":        loaded_codes,
    }

    # ── equity_data（净值曲线） ──
    dates_list = []
    values_list = []
    abs_values_list = []
    cum_val = cash
    for dt_key, ret in sorted(time_returns.items()):
        dt_str = dt_key.strftime('%Y-%m-%d') if hasattr(dt_key, 'strftime') else str(dt_key)
        if dt_str < start:
            cum_val *= (1 + ret)
            continue
        cum_val *= (1 + ret)
        dates_list.append(dt_str)
        values_list.append(round(cum_val / cash, 4))
        abs_values_list.append(round(cum_val, 2))

    equity_data = {
        "dates":      dates_list,
        "values":     values_list,
        "abs_values": abs_values_list,
    }

    # ── trades_paired（配对交易记录） ──
    buy_queues = defaultdict(list)
    trades_paired = []
    per_stock = {}

    for t in strat.trade_log:
        if t['action'] == 'BUY':
            buy_queues[t['code']].append(t)
        elif t['action'] == 'SELL':
            code = t['code']
            queue = buy_queues.get(code, [])
            bt_trade = queue.pop(0) if queue else None
            if bt_trade:
                pnl = round((t['price'] - bt_trade['price']) * t['size'], 2)
                pnl_pct = round((t['price'] / bt_trade['price'] - 1) * 100, 2)
            else:
                pnl, pnl_pct = 0, 0
            trades_paired.append({
                "code":        code,
                "name":        code_name_map.get(code, code),
                "buy_date":    bt_trade['date'] if bt_trade else "",
                "buy_price":   round(bt_trade['price'], 2) if bt_trade else 0,
                "sell_date":   t['date'],
                "sell_price":  round(t['price'], 2),
                "shares":      t['size'],
                "pnl":         pnl,
                "pnl_pct":     pnl_pct,
                "buy_reason":  bt_trade.get('reason', '') if bt_trade else '',
                "sell_reason": t.get('reason', ''),
                "confidence":  bt_trade.get('confidence', 0) if bt_trade else 0,
                "buy_meta":    bt_trade.get('buy_meta', {}) if bt_trade else {},
            })
            ps = per_stock.setdefault(code, {"trades": 0, "pnl": 0.0})
            ps["trades"] += 1
            ps["pnl"] = round(ps["pnl"] + pnl, 2)

    # 未平仓持仓
    for code, buys in buy_queues.items():
        for bt_trade in buys:
            trades_paired.append({
                "code":        code,
                "name":        code_name_map.get(code, code),
                "buy_date":    bt_trade['date'],
                "buy_price":   round(bt_trade['price'], 2),
                "sell_date":   "（持仓中）",
                "sell_price":  0,
                "shares":      bt_trade['size'],
                "pnl":         0,
                "pnl_pct":     0,
                "buy_reason":  bt_trade.get('reason', ''),
                "sell_reason": "持仓中",
                "confidence":  bt_trade.get('confidence', 0),
                "buy_meta":    bt_trade.get('buy_meta', {}),
            })

    # 按卖出日期降序
    def _sort_key(t):
        sd = t.get("sell_date", "")
        return "9999-99-99" if sd == "（持仓中）" else (sd or t.get("buy_date", ""))
    trades_paired.sort(key=_sort_key, reverse=True)

    # 逐股盈亏百分比
    for code, ps in per_stock.items():
        total_buy = sum(
            t["buy_price"] * t["shares"]
            for t in trades_paired
            if t["code"] == code and t["buy_price"] > 0
        )
        ps["pnl_pct"] = round(ps["pnl"] / total_buy * 100, 2) if total_buy > 0 else 0

    metrics["per_stock"] = per_stock

    # ── 从 trades_paired 统一计算交易统计（确保列表/详情数字一致） ──
    closed_trades = [t for t in trades_paired if t.get("sell_date") != "（持仓中）"]
    total_trades = len(trades_paired)
    won  = sum(1 for t in closed_trades if t["pnl"] > 0)
    lost = sum(1 for t in closed_trades if t["pnl"] < 0)
    win_rate = won / len(closed_trades) if closed_trades else 0
    won_pnl  = sum(t["pnl"] for t in closed_trades if t["pnl"] > 0)
    lost_pnl = abs(sum(t["pnl"] for t in closed_trades if t["pnl"] < 0))
    profit_factor = round(won_pnl / lost_pnl, 2) if lost_pnl > 0 else (0.0 if won_pnl == 0 else 99.99)

    metrics["total_trades"]  = total_trades
    metrics["win_rate"]      = f"{win_rate*100:.1f}%"
    metrics["profit_factor"] = profit_factor

    log(f"回测完成: 收益={total_return*100:+.2f}% 夏普={sharpe:.2f} "
        f"最大回撤={max_dd*100:.2f}% 交易={total_trades}笔 胜率={win_rate*100:.1f}%")

    return {
        "metrics":       metrics,
        "equity_data":   equity_data,
        "trades_paired": trades_paired,
    }
