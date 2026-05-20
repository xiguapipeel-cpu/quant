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
    get_all_db_stocks,
    load_all_db_data,
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
    data_source: str = "cache",   # "cache"=本地JSON缓存  "local_db"=MySQL stock_daily
    extra_params: dict | None = None,   # 策略自定义参数（param sweep 时使用）
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

    log(f"Backtrader 引擎启动 | 策略: {strategy_name} | 数据源: {data_source}")

    # 数据预热：从 start 前1年加载（让策略在 start 时已有足够历史 bar）
    warmup_start = (_dt.strptime(start, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    # ── 构建 Cerebro ──
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.broker.set_slippage_perc(0.002)

    loaded = 0
    code_name_map = {}
    loaded_codes = []

    if data_source == "local_db":
        # ── 先加载大盘指数（idx_sh = 上证指数）作为市场过滤基准 ──
        idx_data = await load_all_db_data(['idx_sh'], warmup_start, end)
        idx_df = idx_data.get('idx_sh')
        if idx_df is not None and len(idx_df) >= 60:
            idx_feed = bt.feeds.PandasData(
                dataname=idx_df, name='idx_sh', datetime=None,
                open='open', high='high', low='low', close='close',
                volume='volume', openinterest=-1,
            )
            cerebro.adddata(idx_feed)
            log("已加载上证指数 (idx_sh) 作为大盘过滤基准")
        else:
            log("警告: 未找到上证指数数据，大盘过滤将不生效")

        # ── 本地数据库模式：从 MySQL stock_daily 批量读取 ──
        log("数据源: 本地MySQL数据库，正在查询股票列表...")
        all_stocks = await get_all_db_stocks(warmup_start, end)
        if not all_stocks:
            return {"error": "本地数据库中无可用股票数据，请先同步行情数据"}
        n_stocks = len(all_stocks)
        log(f"数据源: 本地DB发现 {n_stocks} 只股票，批量加载中...")
        codes = [s['code'] for s in all_stocks]
        db_data = await load_all_db_data(codes, warmup_start, end)
        # 最小 bar 数须 ≥ 策略最长指标周期(MA60=60) + 安全余量
        # 避免 Backtrader oncestart 时 dst 数组越界
        _min_bars_db = 80
        # ★ 关键过滤：Backtrader 的 next() 在所有 data feed 的 MA60 都 ready
        # 之后才开始调用。若某只股票首日在 trade_start_date 之后（如2025年新股），
        # 其 MA60 要到 trade_start + 60交易日才 ready，会导致整个策略的 next()
        # 延迟到2026年4月，造成回测期内几乎没有交易。
        # 解决方案：只添加在 trade_start_date 前已有 ≥60 根 bar 的股票。
        #
        # ★ 自适应预热期：若 DB 数据起点晚于请求 start（如 OOS 从历史起点开始），
        # 则自动把 trade_start_date 推迟到"大多数股票已有 60 根 bar"的日期，
        # 而非直接报错"无有效历史数据"。
        import pandas as pd
        _WARMUP = 60
        _trade_start_ts = pd.Timestamp(start)

        # ── 第一遍：收集所有总 bar 数 ≥ _min_bars_db 的股票 ──
        _candidate: list[tuple[dict, object]] = []  # (stock_meta, df)
        for stock in all_stocks:
            code = stock['code']
            df = db_data.get(code)
            if df is None or len(df) < _min_bars_db:
                continue
            _candidate.append((stock, df))

        # ── 判断是否需要自适应推迟 trade_start ──
        _bars_before = [int((df.index < _trade_start_ts).sum()) for _, df in _candidate]
        _median_before = sorted(_bars_before)[len(_bars_before) // 2] if _bars_before else 0

        _effective_start = _dt.strptime(start, '%Y-%m-%d').date()
        if _median_before < _WARMUP and _candidate:
            # 计算每只股票第 60 根 bar 的日期，取 75 分位数作为有效起点
            _bar60_dates = sorted(
                df.index[_WARMUP - 1].date()
                for _, df in _candidate
                if len(df) >= _WARMUP
            )
            if _bar60_dates:
                _p75 = _bar60_dates[int(len(_bar60_dates) * 0.75)]
                _effective_start = max(_effective_start, _p75)
                log(
                    f"⚠️ 预热数据不足（DB 最早 {_candidate[0][1].index[0].date()}，"
                    f"请求起点 {start}），实际交易开始日期自动调整为 {_effective_start}"
                )

        _effective_start_ts = pd.Timestamp(_effective_start)
        trade_start = _effective_start   # 覆盖下方 strat_kwargs 使用的 trade_start

        # ── 第二遍：按调整后的 effective_start 过滤并加载 ──
        # 用 <= 包含 effective_start 当天，确保 MA60 恰好在交易起点 ready
        for stock, df in _candidate:
            code = stock['code']
            bars_up_to_effective = int((df.index <= _effective_start_ts).sum())
            if bars_up_to_effective < _WARMUP:
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
    else:
        # ── 缓存模式（默认）：从 JSON 文件加载 ──
        trade_start = _dt.strptime(start, '%Y-%m-%d').date()  # 缓存模式固定使用请求 start
        all_stocks = get_all_cached_stocks(start, end)
        if not all_stocks:
            return {"error": "无可用股票缓存数据，请先运行数据采集"}
        n_stocks = len(all_stocks)
        log(f"数据源: 缓存中发现 {n_stocks} 只股票，正在加载...")
        for stock in all_stocks:
            code = stock['code']
            df = load_cache_data(code, warmup_start, end)
            if df is None:
                df = load_cache_data(code, start, end)
            if df is None or len(df) < 80:
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
    # data_source=local_db 时 trade_start 可能已被自适应预热逻辑推迟（见上方），
    # 缓存模式下仍使用原始 start 日期。
    if data_source != 'local_db':
        trade_start = _dt.strptime(start, '%Y-%m-%d').date()
    # else: trade_start 已在 local_db 分支中赋值（自适应 or 原始 start）
    strat_kwargs = {
        'max_positions':    max_pos,
        'position_pct':     pos_pct,
        'trade_start_date': trade_start,
    }
    if strategy_name == "major_capital_accumulation":
        strat_kwargs['screen_enabled'] = True
    if extra_params:
        strat_kwargs.update(extra_params)

    # ── 资金流注入（仅 local_db + major_capital_accumulation + extra_params 启用时） ──
    # 从 stock_fund_flow 表读取已加载股票的资金流数据，注入策略 fund_flow_data 参数。
    # 缓存模式不支持（资金流仅入库 MySQL）；其他策略不需要此过滤。
    if (data_source == "local_db"
            and strategy_name == "major_capital_accumulation"
            and strat_kwargs.get("fund_flow_enabled")
            and not strat_kwargs.get("fund_flow_data")):
        try:
            from db.fund_flow_dao import load_fund_flow_range
            ff_data = await load_fund_flow_range(loaded_codes, warmup_start, end)
            strat_kwargs["fund_flow_data"] = ff_data
            log(f"[资金流] 已加载 {len(ff_data)}/{len(loaded_codes)} 只股票的资金流数据")
        except Exception as e:
            log(f"[资金流] 加载失败，自动降级（fund_flow_data=空）: {e}")
            strat_kwargs["fund_flow_data"] = {}

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

    # ── 分年度收益 ──
    annual_returns = {}
    for i, (dt_str, val) in enumerate(zip(dates_list, abs_values_list)):
        year = dt_str[:4]
        if year not in annual_returns:
            annual_returns[year] = {"start_val": abs_values_list[i - 1] if i > 0 else cash}
        annual_returns[year]["end_val"] = val
    for yr, d in annual_returns.items():
        d["return_pct"] = round((d["end_val"] / d["start_val"] - 1) * 100, 2)

    # ── 最大回撤期分析（Top 3，不重叠） ──
    drawdown_periods = []
    if len(abs_values_list) > 1:
        vals = abs_values_list
        n = len(vals)
        peak = vals[0]
        peaks, peak_idxs = [peak], [0]
        for i in range(1, n):
            if vals[i] > peak:
                peak, pk_i = vals[i], i
            else:
                pk_i = peak_idxs[-1]
            peaks.append(peak)
            peak_idxs.append(pk_i)
        dd_series = [(peaks[i] - vals[i]) / peaks[i] if peaks[i] > 0 else 0 for i in range(n)]
        used = [False] * n
        for _ in range(3):
            max_dd, max_dd_idx = 0.0, -1
            for i in range(n):
                if not used[i] and dd_series[i] > max_dd:
                    max_dd, max_dd_idx = dd_series[i], i
            if max_dd_idx < 0 or max_dd < 0.005:
                break
            pk_idx = peak_idxs[max_dd_idx]
            target_val = peaks[max_dd_idx]
            rec_idx = None
            for i in range(max_dd_idx, n):
                if vals[i] >= target_val:
                    rec_idx = i
                    break
            end_mark = rec_idx if rec_idx is not None else n - 1
            for i in range(pk_idx, min(end_mark + 1, n)):
                used[i] = True
            drawdown_periods.append({
                "peak_date":     dates_list[pk_idx],
                "trough_date":   dates_list[max_dd_idx],
                "recovery_date": dates_list[rec_idx] if rec_idx is not None else None,
                "peak_val":      round(vals[pk_idx], 2),
                "trough_val":    round(vals[max_dd_idx], 2),
                "drawdown_pct":  round(max_dd * 100, 2),
                "down_days":     max_dd_idx - pk_idx,
                "recovery_days": (rec_idx - max_dd_idx) if rec_idx is not None else None,
            })

    equity_data["annual_returns"]    = annual_returns
    equity_data["drawdown_periods"]  = drawdown_periods

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
