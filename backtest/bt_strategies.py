"""
Backtrader 策略集合 — 趋势跟踪 / RSI反转 / 布林带回归 / 主力拉升

与 strategies.py 中的原策略算法保持一致，移植到 Backtrader 框架。
主力建仓策略在 bt_major_capital.py 中（已有）。

所有策略共享统一的 notify_order / 下单 / trade_log 模式。
"""

import backtrader as bt
from backtest.bt_major_capital import WilderRSI


# ══════════════════════════════════════════════════════════════
# 基础 Mixin：统一的 notify_order、持仓计数、下单逻辑
# ══════════════════════════════════════════════════════════════

class _BTStrategyBase(bt.Strategy):
    """所有 BT 策略的公共基类"""

    def _base_init(self):
        self.order_dict = {}
        self.trade_log = []

    def notify_order(self, order):
        if order.status == order.Completed:
            name = order.data._name
            state = self.stock_state[name]
            if order.isbuy():
                state['in_position'] = True
                state['buy_price'] = order.executed.price
                state['highest_since_buy'] = order.executed.price
                self.trade_log.append({
                    'date': self.datetime.date(0).isoformat(),
                    'code': name, 'action': 'BUY',
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'reason': getattr(order, '_reason', ''),
                })
            else:
                state['in_position'] = False
                self.trade_log.append({
                    'date': self.datetime.date(0).isoformat(),
                    'code': name, 'action': 'SELL',
                    'price': order.executed.price,
                    'size': abs(order.executed.size),
                    'reason': getattr(order, '_reason', ''),
                })
            self.order_dict.pop(name, None)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order_dict.pop(order.data._name, None)

    def _n_positions(self):
        return sum(1 for d in self.datas if self.getposition(d).size > 0)

    def _execute_signals(self, sell_signals, buy_signals):
        """
        统一执行信号：SELL 优先，BUY 按 confidence 降序、受仓位限制。
        sell_signals: [(data, reason), ...]
        buy_signals:  [(data, reason, confidence), ...]
        """
        for d, reason in sell_signals:
            o = self.close(data=d)
            o._reason = reason
            self.order_dict[d._name] = o

        buy_signals.sort(key=lambda x: -x[2])
        n_held = self._n_positions()
        available = self.p.max_positions - n_held + len(sell_signals)

        for d, reason, conf in buy_signals:
            if available <= 0:
                break
            name = d._name
            if name in self.order_dict:
                continue
            cash = self.broker.getcash()
            total_val = self.broker.getvalue()
            target = total_val / self.p.max_positions
            max_amount = min(target, cash, total_val * self.p.position_pct)
            price = float(d.close[0])
            shares = int(max_amount / price / 100) * 100
            if shares < 100:
                continue
            o = self.buy(data=d, size=shares)
            o._reason = reason
            self.order_dict[name] = o
            available -= 1


# ══════════════════════════════════════════════════════════════
# 策略1：趋势跟踪 (EMA交叉 + 大趋势过滤 + 追踪止损)
# ══════════════════════════════════════════════════════════════

class TrendFollowBT(_BTStrategyBase):
    """
    入场：EMA_fast 上穿 EMA_slow + 价格 > EMA_trend
    出场：追踪止损 trailing_pct 或 EMA_fast 下穿 EMA_slow
    """
    params = dict(
        fast=10, slow=30, trend=60,
        trailing_pct=0.08,
        max_positions=5,
        position_pct=0.20,
    )

    def __init__(self):
        self._base_init()
        self.indicators = {}
        self.stock_state = {}

        for d in self.datas:
            self.indicators[d._name] = {
                'ema_fast':  bt.indicators.EMA(d.close, period=self.p.fast),
                'ema_slow':  bt.indicators.EMA(d.close, period=self.p.slow),
                'ema_trend': bt.indicators.EMA(d.close, period=self.p.trend),
            }
            self.stock_state[d._name] = {
                'in_position': False,
                'buy_price': 0.0,
                'highest_since_buy': 0.0,
            }

    def next(self):
        sell_signals = []
        buy_signals = []

        for d in self.datas:
            name = d._name
            if name in self.order_dict:
                continue
            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < self.p.trend + 2:
                continue

            ef0 = float(ind['ema_fast'][-1])
            ef1 = float(ind['ema_fast'][0])
            es0 = float(ind['ema_slow'][-1])
            es1 = float(ind['ema_slow'][0])
            et  = float(ind['ema_trend'][0])
            close = float(d.close[0])

            if any(v != v for v in [ef0, ef1, es0, es1, et]):
                continue

            if state['in_position']:
                state['highest_since_buy'] = max(state['highest_since_buy'], close)
                drawdown = (state['highest_since_buy'] - close) / state['highest_since_buy']
                if drawdown >= self.p.trailing_pct:
                    sell_signals.append((d,
                        f"追踪止损 最高{state['highest_since_buy']:.2f}→{close:.2f} "
                        f"回撤{drawdown:.1%}"))
                    continue
                if ef0 > es0 and ef1 <= es1:
                    sell_signals.append((d,
                        f"EMA{self.p.fast}下穿EMA{self.p.slow} 趋势转弱"))
            else:
                if ef0 <= es0 and ef1 > es1 and close > et:
                    buy_signals.append((d,
                        f"EMA{self.p.fast}上穿EMA{self.p.slow} "
                        f"价格>{self.p.trend}日趋势线",
                        0.5))

        self._execute_signals(sell_signals, buy_signals)


# ══════════════════════════════════════════════════════════════
# 策略2：RSI反转 (超卖反弹 + 动量确认)
# ══════════════════════════════════════════════════════════════

class RSIReversalBT(_BTStrategyBase):
    """
    入场：RSI 从 <entry_low 回升到 >entry_cross + 收阳线
    出场：RSI >= take_profit 止盈 或 固定止损
    """
    params = dict(
        period=14,
        entry_low=25.0,
        entry_cross=30.0,
        take_profit=60.0,
        stop_loss_pct=0.06,
        max_positions=5,
        position_pct=0.20,
    )

    def __init__(self):
        self._base_init()
        self.indicators = {}
        self.stock_state = {}

        for d in self.datas:
            self.indicators[d._name] = {
                'rsi': WilderRSI(d.close, period=self.p.period),
            }
            self.stock_state[d._name] = {
                'in_position': False,
                'buy_price': 0.0,
                'highest_since_buy': 0.0,
                'was_oversold': False,
            }

    def next(self):
        sell_signals = []
        buy_signals = []

        for d in self.datas:
            name = d._name
            if name in self.order_dict:
                continue
            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < self.p.period + 3:
                continue

            rsi_now  = float(ind['rsi'][0])
            rsi_prev = float(ind['rsi'][-1])
            close = float(d.close[0])
            open_ = float(d.open[0])

            if rsi_now != rsi_now or rsi_prev != rsi_prev:
                continue

            if state['in_position']:
                # 止损
                loss = (close - state['buy_price']) / state['buy_price']
                if loss <= -self.p.stop_loss_pct:
                    sell_signals.append((d, f"止损 跌幅{loss:.1%}"))
                    continue
                # 止盈
                if rsi_now >= self.p.take_profit:
                    gain = (close - state['buy_price']) / state['buy_price']
                    sell_signals.append((d,
                        f"RSI={rsi_now:.0f}止盈 收益{gain:.1%}"))
            else:
                if rsi_now <= self.p.entry_low:
                    state['was_oversold'] = True
                if (state['was_oversold']
                        and rsi_prev < self.p.entry_cross
                        and rsi_now >= self.p.entry_cross
                        and close > open_):
                    buy_signals.append((d,
                        f"RSI超卖反弹 {rsi_prev:.0f}→{rsi_now:.0f} 收阳确认",
                        0.5))
                    state['was_oversold'] = False

        self._execute_signals(sell_signals, buy_signals)


# ══════════════════════════════════════════════════════════════
# 策略3：布林带回归 (触碰下轨买入 + 中轨止盈)
# ══════════════════════════════════════════════════════════════

class BollingerRevertBT(_BTStrategyBase):
    """
    入场：价格 <= 布林下轨 + 收阳线
    出场：价格 >= 中轨（止盈）或 固定止损
    """
    params = dict(
        period=20,
        num_std=2.0,
        stop_loss_pct=0.05,
        take_profit_target='mid',  # 'mid' or 'upper'
        max_positions=5,
        position_pct=0.20,
    )

    def __init__(self):
        self._base_init()
        self.indicators = {}
        self.stock_state = {}

        for d in self.datas:
            self.indicators[d._name] = {
                'bb': bt.indicators.BollingerBands(
                    d.close, period=self.p.period, devfactor=self.p.num_std),
            }
            self.stock_state[d._name] = {
                'in_position': False,
                'buy_price': 0.0,
                'highest_since_buy': 0.0,
            }

    def next(self):
        sell_signals = []
        buy_signals = []

        for d in self.datas:
            name = d._name
            if name in self.order_dict:
                continue
            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < self.p.period + 2:
                continue

            close = float(d.close[0])
            open_ = float(d.open[0])
            bb_top = float(ind['bb'].top[0])
            bb_mid = float(ind['bb'].mid[0])
            bb_bot = float(ind['bb'].bot[0])

            if any(v != v for v in [bb_top, bb_mid, bb_bot]):
                continue

            if state['in_position']:
                # 止损
                loss = (close - state['buy_price']) / state['buy_price']
                if loss <= -self.p.stop_loss_pct:
                    sell_signals.append((d, f"止损 跌幅{loss:.1%}"))
                    continue
                # 止盈
                target = bb_mid if self.p.take_profit_target == 'mid' else bb_top
                label  = '中轨' if self.p.take_profit_target == 'mid' else '上轨'
                if close >= target:
                    gain = (close - state['buy_price']) / state['buy_price']
                    sell_signals.append((d,
                        f"回归{label} 收益{gain:.1%}"))
            else:
                if close <= bb_bot and close > open_:
                    buy_signals.append((d,
                        f"触碰布林下轨{bb_bot:.2f} 收阳确认",
                        0.5))

        self._execute_signals(sell_signals, buy_signals)


# ══════════════════════════════════════════════════════════════
# 策略4：主力拉升 (量价共振 + MACD + RSI)
# ══════════════════════════════════════════════════════════════

class MajorCapitalPumpBT(_BTStrategyBase):
    """
    入场：阳线 + 涨幅>3% + 量比>1.5x + 价格>MA20 + RSI 50-70 + MACD DIF>=0
    出场：
      - RSI>85 + 长上影线（主力出货）
      - 追踪止损 10%
      - 跌破MA5收阴
      - 放量阴线（量比>2x）
    """
    params = dict(
        pct_entry=3.0,
        vol_ratio_entry=1.5,
        vol_ratio_exit=2.0,
        rsi_min=50.0,
        rsi_max=70.0,
        rsi_exit=85.0,
        ma_fast=5,
        ma_slow=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        upper_shadow_exit=0.30,
        trailing_pct=0.10,
        max_positions=5,
        position_pct=0.20,
    )

    def __init__(self):
        self._base_init()
        self.indicators = {}
        self.stock_state = {}

        for d in self.datas:
            self.indicators[d._name] = {
                'ma_fast': bt.indicators.SMA(d.close, period=self.p.ma_fast),
                'ma_slow': bt.indicators.SMA(d.close, period=self.p.ma_slow),
                'rsi':     WilderRSI(d.close, period=14),
                'macd':    bt.indicators.MACD(
                    d.close,
                    period_me1=self.p.macd_fast,
                    period_me2=self.p.macd_slow,
                    period_signal=self.p.macd_signal,
                ),
            }
            self.stock_state[d._name] = {
                'in_position': False,
                'buy_price': 0.0,
                'highest_since_buy': 0.0,
            }

    def _vol_ratio(self, d, period=5):
        """量比：当日量 / 过去 N 日均量"""
        if len(d) < period + 1:
            return None
        avg = sum(float(d.volume[-j]) for j in range(1, period + 1)) / period
        return float(d.volume[0]) / avg if avg > 1e-9 else None

    def _upper_shadow_ratio(self, d):
        """上影线占全范围比例"""
        high = float(d.high[0])
        low = float(d.low[0])
        full = high - low
        if full < 1e-9:
            return 0.0
        top = max(float(d.close[0]), float(d.open[0]))
        return (high - top) / full

    def next(self):
        min_len = self.p.macd_slow + self.p.macd_signal + 5
        sell_signals = []
        buy_signals = []

        for d in self.datas:
            name = d._name
            if name in self.order_dict:
                continue
            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < min_len:
                continue

            close = float(d.close[0])
            open_ = float(d.open[0])
            maf   = float(ind['ma_fast'][0])
            mas   = float(ind['ma_slow'][0])
            rsi   = float(ind['rsi'][0])
            dif   = float(ind['macd'].macd[0])

            if any(v != v for v in [maf, mas, rsi, dif]):
                continue

            is_bull = close > open_

            if state['in_position']:
                state['highest_since_buy'] = max(state['highest_since_buy'], close)

                # 止盈1：RSI过热 + 长上影线
                upper_shadow = self._upper_shadow_ratio(d)
                if rsi >= self.p.rsi_exit and upper_shadow >= self.p.upper_shadow_exit:
                    gain = (close - state['buy_price']) / state['buy_price']
                    sell_signals.append((d,
                        f"主力出货: RSI={rsi:.0f} 上影线{upper_shadow:.0%} "
                        f"收益{gain:+.1%}"))
                    continue

                # 止盈2：追踪止损
                drawdown = (state['highest_since_buy'] - close) / state['highest_since_buy']
                if drawdown >= self.p.trailing_pct:
                    gain = (close - state['buy_price']) / state['buy_price']
                    sell_signals.append((d,
                        f"追踪止损: 峰值{state['highest_since_buy']:.2f} "
                        f"回撤{drawdown:.1%} 收益{gain:+.1%}"))
                    continue

                # 止损1：跌破MA_fast 且收阴
                if not is_bull and close < maf:
                    loss = (close - state['buy_price']) / state['buy_price']
                    sell_signals.append((d,
                        f"跌破MA{self.p.ma_fast}收阴 收益{loss:+.1%}"))
                    continue

                # 止损2：放量阴线
                vr = self._vol_ratio(d)
                if vr is not None and vr >= self.p.vol_ratio_exit and not is_bull:
                    loss = (close - state['buy_price']) / state['buy_price']
                    sell_signals.append((d,
                        f"放量收阴(量比{vr:.1f}x) 收益{loss:+.1%}"))

            else:
                if len(d) < 2:
                    continue
                prev_close = float(d.close[-1])
                pct_chg = (close / prev_close - 1) * 100 if prev_close > 0 else 0
                vr = self._vol_ratio(d)
                if vr is None:
                    continue

                if (is_bull
                        and pct_chg >= self.p.pct_entry
                        and vr >= self.p.vol_ratio_entry
                        and close > mas
                        and self.p.rsi_min <= rsi <= self.p.rsi_max
                        and dif >= 0):
                    buy_signals.append((d,
                        f"主力拉升: +{pct_chg:.1f}% 量比{vr:.1f}x "
                        f"RSI={rsi:.0f} MACD多头",
                        min(1.0, vr / 3.0)))

        self._execute_signals(sell_signals, buy_signals)


# ══════════════════════════════════════════════════════════════
# 策略注册表（Backtrader 版本）
# ══════════════════════════════════════════════════════════════

BT_STRATEGY_MAP = {
    "trend_follow":       TrendFollowBT,
    "rsi_reversal":       RSIReversalBT,
    "bollinger_revert":   BollingerRevertBT,
    "major_capital_pump": MajorCapitalPumpBT,
    # major_capital_accumulation 在 bt_major_capital.py 中
}
