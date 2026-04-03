"""
回测引擎
逐日撮合 · 仓位管理 · 手续费/滑点 · 收益归因
"""

from dataclasses import dataclass, field
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger("bt_engine")


# ─────────────────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    initial_cash:     float = 1_000_000.0   # 初始资金（元）
    commission_rate:  float = 0.0003        # 手续费率（双向）
    stamp_duty:       float = 0.001         # 印花税（仅卖出，A股0.1%）
    slippage:         float = 0.002         # 滑点（0.2%）
    max_position_pct: float = 0.3           # 单股最大仓位比例（占总资产）
    max_positions:    int   = 5             # 最大同时持仓股票数
    min_trade_amount: float = 100.0         # 最小交易金额（元）
    benchmark:        str   = "000300"      # 对标指数（沪深300）


@dataclass
class Trade:
    date:       str
    code:       str
    action:     str    # BUY / SELL
    price:      float
    shares:     int
    amount:     float  # 含手续费的实际金额
    commission: float
    reason:     str


@dataclass
class DailySnapshot:
    date:       str
    cash:       float
    holdings:   dict   # code → {shares, avg_cost, market_value}
    total_value: float
    daily_return: float
    cumulative_return: float


@dataclass
class BacktestResult:
    config:           BacktestConfig
    strategy_name:    str
    start_date:       str
    end_date:         str
    initial_cash:     float
    final_value:      float
    total_return:     float
    annualized_return: float
    max_drawdown:     float
    sharpe_ratio:     float
    win_rate:         float
    profit_factor:    float
    total_trades:     int
    trades:           list[Trade]   = field(default_factory=list)
    daily_snapshots:  list[DailySnapshot] = field(default_factory=list)
    per_stock:        dict          = field(default_factory=dict)


# ─────────────────────────────────────────────────────────
# 回测引擎
# ─────────────────────────────────────────────────────────

class BacktestEngine:
    """
    事件驱动回测引擎
    - 逐日撮合：信号日次日开盘价成交（避免未来数据）
    - 仓位管理：单股不超过 max_position_pct
    - 成本：手续费 + 印花税 + 滑点
    - 输出：完整交易记录 + 每日净值 + 绩效指标
    """

    def __init__(self, config: BacktestConfig = None):
        self.cfg = config or BacktestConfig()

    def run(
        self,
        strategy,
        all_bars: dict[str, list[dict]],   # code → bars
        extra_data: dict[str, dict] = None, # code → {pe_series, integrity_pass, ...}
    ) -> BacktestResult:
        """
        执行回测
        all_bars: 所有股票历史日线 {code: [bar, ...]}
        extra_data: 额外数据（PE序列、完整性验证结果等）
        """
        extra_data = extra_data or {}
        cfg = self.cfg

        # ── 收集所有信号 ──────────────────────────────────
        all_signals: dict[str, list] = {}
        for code, bars in all_bars.items():
            extra = extra_data.get(code, {})
            sigs  = strategy.generate_signals(code, bars, extra)
            all_signals[code] = sigs
            logger.info(f"[{strategy.name}] {code} 生成{len(sigs)}条信号")

        # ── 构建全局日期序列 ──────────────────────────────
        all_dates = sorted({b["date"] for bars in all_bars.values() for b in bars})

        # ── 建立日期→价格映射 ─────────────────────────────
        price_map: dict[str, dict[str, dict]] = {}  # date→code→bar
        for code, bars in all_bars.items():
            for b in bars:
                price_map.setdefault(b["date"], {})[code] = b

        # 信号映射：next_date→pending_signals（信号日+1执行）
        pending: dict[str, list] = {}
        for code, sigs in all_signals.items():
            dates_in_code = [b["date"] for b in all_bars[code]]
            for sig in sigs:
                idx = dates_in_code.index(sig.date) if sig.date in dates_in_code else -1
                if idx >= 0 and idx + 1 < len(dates_in_code):
                    exec_date = dates_in_code[idx + 1]   # 次日执行
                    pending.setdefault(exec_date, []).append(sig)

        # ── 逐日模拟 ──────────────────────────────────────
        cash       = cfg.initial_cash
        holdings   = {}   # code → {shares, avg_cost}
        trades     = []
        snapshots  = []
        prev_value = cfg.initial_cash

        for date in all_dates:
            day_prices = price_map.get(date, {})

            # ★ 在执行信号前，计算当前总资产（用于等权仓位分配）
            holdings_mv = sum(
                h["shares"] * (day_prices.get(code, {}).get("open", h["avg_cost"]))
                for code, h in holdings.items() if h["shares"] > 0
            )
            total_portfolio = cash + holdings_mv
            n_held = sum(1 for h in holdings.values() if h["shares"] > 0)

            # 执行待成交信号（SELL优先释放仓位，BUY按信心排序）
            day_sigs = pending.get(date, [])
            day_sigs.sort(key=lambda s: (0 if s.action == "SELL" else 1, -getattr(s, 'confidence', 0)))
            for sig in day_sigs:
                bar = day_prices.get(sig.code)
                if bar is None:
                    continue
                exec_price = bar["open"] * (1 + cfg.slippage if sig.action == "BUY" else 1 - cfg.slippage)
                trade = self._execute(sig, exec_price, cash, holdings, cfg,
                                      total_portfolio, n_held)
                if trade:
                    trades.append(trade)
                    if sig.action == "BUY":
                        cash -= trade.amount
                        h = holdings.setdefault(sig.code, {"shares": 0, "avg_cost": 0.0})
                        was_new = h["shares"] == 0
                        total_shares = h["shares"] + trade.shares
                        total_cost   = h["shares"] * h["avg_cost"] + trade.shares * exec_price
                        h["shares"]  = total_shares
                        h["avg_cost"] = total_cost / total_shares if total_shares > 0 else 0
                        if was_new:
                            n_held += 1     # 新增持仓
                    else:
                        cash += trade.amount
                        h = holdings.get(sig.code, {"shares": 0, "avg_cost": 0.0})
                        h["shares"] -= trade.shares
                        if h["shares"] <= 0:
                            holdings.pop(sig.code, None)
                            n_held = max(0, n_held - 1)

            # 计算当日总资产
            holdings_val = {
                code: {
                    "shares":       h["shares"],
                    "avg_cost":     h["avg_cost"],
                    "current_price": day_prices[code]["close"] if code in day_prices else h["avg_cost"],
                    "market_value": h["shares"] * (day_prices[code]["close"] if code in day_prices else h["avg_cost"]),
                }
                for code, h in holdings.items() if h["shares"] > 0
            }
            mv         = sum(v["market_value"] for v in holdings_val.values())
            total_val  = cash + mv
            daily_ret  = (total_val - prev_value) / prev_value if prev_value > 0 else 0
            cum_ret    = (total_val - cfg.initial_cash) / cfg.initial_cash

            snapshots.append(DailySnapshot(
                date=date,
                cash=round(cash, 2),
                holdings=holdings_val,
                total_value=round(total_val, 2),
                daily_return=round(daily_ret, 6),
                cumulative_return=round(cum_ret, 6),
            ))
            prev_value = total_val

        # ── 计算绩效指标 ──────────────────────────────────
        metrics = self._calc_metrics(snapshots, trades, cfg)

        return BacktestResult(
            config=cfg,
            strategy_name=strategy.name,
            start_date=all_dates[0] if all_dates else "",
            end_date=all_dates[-1] if all_dates else "",
            initial_cash=cfg.initial_cash,
            final_value=snapshots[-1].total_value if snapshots else cfg.initial_cash,
            total_return=metrics["total_return"],
            annualized_return=metrics["annualized_return"],
            max_drawdown=metrics["max_drawdown"],
            sharpe_ratio=metrics["sharpe_ratio"],
            win_rate=metrics["win_rate"],
            profit_factor=metrics["profit_factor"],
            total_trades=len(trades),
            trades=trades,
            daily_snapshots=snapshots,
            per_stock=self._per_stock_pnl(trades),
        )

    def _execute(self, sig, exec_price: float, cash: float, holdings: dict,
                 cfg: BacktestConfig, total_portfolio: float = 0, n_held: int = 0) -> Optional[Trade]:
        if sig.action == "BUY":
            # ★ 仓位限制：已达最大持仓数则拒绝新买入
            if n_held >= cfg.max_positions:
                return None
            # 已持有该股则跳过（不加仓）
            if sig.code in holdings and holdings[sig.code]["shares"] > 0:
                return None

            # ★ 等权分配：每只股票目标仓位 = 总资产 / 最大持仓数
            #   比旧逻辑（剩余现金×30%）更均衡，避免后买的股票只买一手
            target_amount = total_portfolio / cfg.max_positions if total_portfolio > 0 else cash * cfg.max_position_pct
            # 但不能超过可用现金，也不超过单股仓位上限
            max_amount = min(target_amount, cash, total_portfolio * cfg.max_position_pct)

            shares = int(max_amount / exec_price / 100) * 100   # 整手（A股100股/手）
            if shares < 100:
                return None
            raw_amount  = shares * exec_price
            commission  = max(5.0, raw_amount * cfg.commission_rate)
            total_cost  = raw_amount + commission
            # 超过现金则用全部现金重算
            if total_cost > cash:
                shares     = int((cash / (exec_price * (1 + cfg.commission_rate))) / 100) * 100
                if shares < 100:
                    return None
                raw_amount = shares * exec_price
                commission = max(5.0, raw_amount * cfg.commission_rate)
                total_cost = raw_amount + commission
            return Trade(sig.date, sig.code, "BUY", exec_price, shares,
                         round(total_cost, 2), round(commission, 2), sig.reason)

        else:  # SELL
            h = holdings.get(sig.code)
            if not h or h["shares"] <= 0:
                return None
            shares      = h["shares"]
            raw_amount  = shares * exec_price
            commission  = max(5.0, raw_amount * cfg.commission_rate)
            stamp       = raw_amount * cfg.stamp_duty
            net_amount  = raw_amount - commission - stamp
            return Trade(sig.date, sig.code, "SELL", exec_price, shares,
                         round(net_amount, 2), round(commission + stamp, 2), sig.reason)

    def _calc_metrics(self, snapshots: list, trades: list, cfg: BacktestConfig) -> dict:
        if not snapshots:
            return {"total_return": 0, "annualized_return": 0, "max_drawdown": 0,
                    "sharpe_ratio": 0, "win_rate": 0, "profit_factor": 0}

        values     = [s.total_value for s in snapshots]
        daily_rets = [s.daily_return for s in snapshots]

        total_return = (values[-1] - cfg.initial_cash) / cfg.initial_cash

        # 年化收益（按252个交易日）
        n_days = len(snapshots)
        annualized = (1 + total_return) ** (252 / n_days) - 1 if n_days > 0 else 0

        # 最大回撤
        peak = values[0]
        max_dd = 0.0
        for v in values:
            peak = max(peak, v)
            dd   = (peak - v) / peak
            max_dd = max(max_dd, dd)

        # 夏普比率（无风险利率 2.5%）
        rf_daily = 0.025 / 252
        excess   = [r - rf_daily for r in daily_rets]
        mean_ex  = sum(excess) / len(excess) if excess else 0
        std_ex   = (sum((r - mean_ex) ** 2 for r in excess) / len(excess)) ** 0.5 if excess else 1
        # 标准差过小（无交易时daily_rets全为0）→ 夏普设为0
        sharpe   = (mean_ex / std_ex * (252 ** 0.5)) if std_ex > 1e-9 else 0

        # 胜率 & 盈亏比（正确配对：每个BUY与下一个同股SELL配对）
        # 按股票分组，按时间顺序配对
        from collections import defaultdict
        buy_queues = defaultdict(list)  # code → [buy_trade, ...]
        wins, total_profit, total_loss, n_closed = 0, 0.0, 0.0, 0
        for t in trades:
            if t.action == "BUY":
                buy_queues[t.code].append(t)
            elif t.action == "SELL":
                queue = buy_queues.get(t.code, [])
                if queue:
                    bt = queue.pop(0)  # FIFO配对
                    pnl = (t.price - bt.price) * t.shares - t.commission - bt.commission
                    n_closed += 1
                    if pnl > 0:
                        wins += 1
                        total_profit += pnl
                    else:
                        total_loss += abs(pnl)
        win_rate   = wins / n_closed if n_closed > 0 else 0
        profit_fac = total_profit / total_loss if total_loss > 0 else (0.0 if total_profit == 0 else 99.99)

        return {
            "total_return":      round(total_return, 4),
            "annualized_return": round(annualized, 4),
            "max_drawdown":      round(max_dd, 4),
            "sharpe_ratio":      round(sharpe, 2),
            "win_rate":          round(win_rate, 4),
            "profit_factor":     round(profit_fac, 2),
        }

    def _per_stock_pnl(self, trades: list) -> dict:
        """逐股盈亏归因"""
        stock_trades: dict[str, list] = {}
        for t in trades:
            stock_trades.setdefault(t.code, []).append(t)

        result = {}
        for code, ts in stock_trades.items():
            buys   = [t for t in ts if t.action == "BUY"]
            sells  = [t for t in ts if t.action == "SELL"]
            cost   = sum(t.amount for t in buys)
            revenue= sum(t.amount for t in sells)
            pnl    = revenue - cost
            result[code] = {
                "trades":   len(ts),
                "pnl":      round(pnl, 2),
                "pnl_pct":  round(pnl / cost * 100, 2) if cost > 0 else 0,
            }
        return result
