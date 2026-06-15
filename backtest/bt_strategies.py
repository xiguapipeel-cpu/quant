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

    # 子类 params 中可包含 trade_start_date=None，预热期内不交易
    def _base_init(self):
        self.order_dict = {}
        self.trade_log = []
        self._trade_start = getattr(self.p, 'trade_start_date', None)

    def _before_trade_start(self):
        """当前 bar 是否在交易起始日之前（预热期）"""
        if self._trade_start is None:
            return False
        return self.datetime.date(0) < self._trade_start

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
    v2: EMA12/26 金叉 + EMA60大趋势 + RSI>50 + 成交量放大
        硬止损5% + 追踪止损6% + 5日冷静期
    """
    params = dict(
        fast=12, slow=26, trend=60,
        trailing_pct=0.06,
        hard_stop_pct=0.05,
        cool_days=5,
        vol_ratio=1.2,
        rsi_min=50.0,
        max_positions=5,
        position_pct=0.20,
        trade_start_date=None,
        # ── 大盘 regime 过滤（默认关，ablation 时 extra_params 打开）──
        market_filter=False,
        market_code='idx_sh',
        market_ma_fast=20,
        market_ma_slow=60,
        # ── 流动性股票池过滤：20日均成交额下限(万元)，0=关 ──
        min_turnover_wan=0.0,
        # ── 入场信号模式：'ema_cross'(EMA金叉,默认) | 'donchian'(N日新高突破,海龟) ──
        entry_mode='ema_cross',
        donchian_n=20,
        # ── intrabar 挂单止损：True 时硬止损+追踪止损改用 broker OCO 挂单
        #    (Stop + StopTrail，当根触及即成交，跳空按开盘价)，杜绝次开市价的滑点击穿 ──
        intrabar_stop=False,
    )

    def __init__(self):
        self._base_init()
        self.indicators = {}
        self.stock_state = {}
        self.hard_stops = {}   # name -> 挂单硬止损 order（intrabar_stop 模式）

        # ── 大盘指数 data feed（不参与个股交易，仅作 regime 基准）──
        self._market_ma_fast = None
        self._market_ma_slow = None
        for d in self.datas:
            if d._name == self.p.market_code:
                self._market_ma_fast = bt.indicators.SMA(
                    d.close, period=self.p.market_ma_fast)
                self._market_ma_slow = bt.indicators.SMA(
                    d.close, period=self.p.market_ma_slow)
                break

        for d in self.datas:
            if d._name == self.p.market_code:
                continue   # 指数不参与个股逻辑
            self.indicators[d._name] = {
                'ema_fast':  bt.indicators.EMA(d.close, period=self.p.fast),
                'ema_slow':  bt.indicators.EMA(d.close, period=self.p.slow),
                'ema_trend': bt.indicators.EMA(d.close, period=self.p.trend),
                'rsi':       WilderRSI(d.close, period=14),
                'vol_ma':    bt.indicators.SMA(d.volume, period=20),
                # 20日均成交额(元) = 均(close×volume)，/1e4 得万元
                'turnover_ma': bt.indicators.SMA(d.close * d.volume, period=20),
                # Donchian 上轨：N 日最高价（取 [-1] 得"前 N 日"高点，排除当日）
                'dc_high': bt.indicators.Highest(d.high, period=self.p.donchian_n),
            }
            self.stock_state[d._name] = {
                'in_position': False,
                'buy_price': 0.0,
                'highest_since_buy': 0.0,
                'last_sell_bar': -999,
            }

    def next(self):
        if self._before_trade_start():
            return

        sell_signals = []
        buy_signals = []
        cur_bar = len(self)

        # ── 大盘 regime：快线 > 慢线才允许新开仓（不影响已持仓卖出）──
        market_ok = True
        if self.p.market_filter and self._market_ma_fast is not None:
            mf = float(self._market_ma_fast[0])
            ms = float(self._market_ma_slow[0])
            if mf == mf and ms == ms:   # 非 NaN
                market_ok = mf > ms

        for d in self.datas:
            name = d._name
            if name == self.p.market_code:
                continue   # 指数不参与个股交易
            if name in self.order_dict:
                continue
            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < self.p.trend + 2:
                continue

            ef0   = float(ind['ema_fast'][-1])
            ef1   = float(ind['ema_fast'][0])
            es0   = float(ind['ema_slow'][-1])
            es1   = float(ind['ema_slow'][0])
            et    = float(ind['ema_trend'][0])
            rsi   = float(ind['rsi'][0])
            vma   = float(ind['vol_ma'][0])
            vol   = float(d.volume[0])
            close = float(d.close[0])

            if any(v != v for v in [ef0, ef1, es0, es1, et, rsi]):
                continue

            if state['in_position']:
                state['highest_since_buy'] = max(state['highest_since_buy'], close)

                # 硬止损：intrabar 模式由 buy 成交时挂出的 Stop 单(broker端)负责，
                # 当根触及 -hard_stop_pct 即成交、跳空按开盘价，避免次开市价滑点击穿。
                # close-based 模式仍用次开市价。
                if not self.p.intrabar_stop:
                    hard_loss = (state['buy_price'] - close) / state['buy_price']
                    if hard_loss >= self.p.hard_stop_pct:
                        sell_signals.append((d,
                            f"硬止损 买入{state['buy_price']:.2f}→{close:.2f} "
                            f"亏损{hard_loss:.1%}"))
                        state['last_sell_bar'] = cur_bar
                        continue

                # 追踪止损：始终 close-based（盘中插针不洗盘，让赢家奔跑到收盘）。
                # intrabar 模式下触发时先撤掉挂单硬止损，再下次开市价卖单，防双卖做空。
                drawdown = (state['highest_since_buy'] - close) / state['highest_since_buy']
                if drawdown >= self.p.trailing_pct:
                    self._cancel_hard_stop(name)
                    sell_signals.append((d,
                        f"追踪止损 最高{state['highest_since_buy']:.2f}→{close:.2f} "
                        f"回撤{drawdown:.1%}"))
                    state['last_sell_bar'] = cur_bar
                    continue

                # 死叉卖出
                if ef0 > es0 and ef1 <= es1:
                    self._cancel_hard_stop(name)
                    sell_signals.append((d,
                        f"EMA{self.p.fast}下穿EMA{self.p.slow} 趋势转弱"))
                    state['last_sell_bar'] = cur_bar
            else:
                # 冷静期
                if cur_bar - state['last_sell_bar'] < self.p.cool_days:
                    continue

                # 成交量过滤
                vol_ok = (vma != vma or vma < 1e-9) or (vol >= vma * self.p.vol_ratio)

                # 流动性股票池过滤：20日均成交额(万元) ≥ 阈值
                turnover_ok = True
                if self.p.min_turnover_wan > 0:
                    tov = float(ind['turnover_ma'][0])
                    turnover_ok = (tov == tov) and (tov / 1e4 >= self.p.min_turnover_wan)

                # ── 入场触发：EMA金叉 or Donchian N日新高突破 ──
                if self.p.entry_mode == 'donchian':
                    dc = float(ind['dc_high'][-1])   # 前 N 日最高价（排除当日）
                    trigger = (dc == dc) and (close > dc)
                    entry_desc = f"突破{self.p.donchian_n}日新高 {dc:.2f}→{close:.2f}"
                else:
                    trigger = (ef0 <= es0 and ef1 > es1)
                    entry_desc = f"EMA{self.p.fast}上穿EMA{self.p.slow}"

                if (market_ok
                        and trigger
                        and close > et
                        and rsi >= self.p.rsi_min
                        and vol_ok
                        and turnover_ok):
                    buy_signals.append((d,
                        f"{entry_desc} RSI={rsi:.1f} 量比{vol/(vma or 1):.1f}x",
                        0.5 + min(rsi - 50, 20) / 100))

        self._execute_signals(sell_signals, buy_signals)

    def _cancel_hard_stop(self, name):
        """撤销某股残留的挂单硬止损（在 close-based 出场触发前调用，防双卖做空）。"""
        so = self.hard_stops.pop(name, None)
        if so is not None and so.alive():
            self.cancel(so)

    def notify_order(self, order):
        """intrabar_stop 模式：买入成交后挂出单个 Stop 硬止损单（broker 端撮合）。

        只挂硬止损一单（追踪止损仍走 next() 的 close-based 逻辑）。防做空靠两点：
        ① close-based 出场触发时先 _cancel_hard_stop 撤掉挂单（撤单在下一 bar 撮合前生效）；
        ② self.close() 是 size-aware 的，持仓已被挂单平掉后再 close 不会反向开空。
        挂单填充后于此清理 hard_stops 记录。
        """
        if self.p.intrabar_stop and order.status == order.Completed:
            name = order.data._name
            if order.isbuy():
                super().notify_order(order)   # 记 BUY、置 in_position
                sz = order.executed.size
                px = order.executed.price
                stop_px = px * (1 - self.p.hard_stop_pct)
                so = self.sell(data=order.data, size=sz,
                               exectype=bt.Order.Stop, price=stop_px)
                so._reason = (f"硬止损(挂单@{stop_px:.2f}) 买入{px:.2f}")
                self.hard_stops[name] = so
                return
            if order.issell():
                # 出场成交（挂单硬止损 or close-based 卖单）：清理记录、记冷静期
                so = self.hard_stops.pop(name, None)
                if so is not None and so is not order and so.alive():
                    self.cancel(so)
                self.stock_state[name]['last_sell_bar'] = len(self)
                super().notify_order(order)
                return
        super().notify_order(order)


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
        trade_start_date=None,
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
        if self._before_trade_start():
            return

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
        trade_start_date=None,
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
        if self._before_trade_start():
            return

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
        trade_start_date=None,
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
        if self._before_trade_start():
            return

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
# 策略5：趋势跟踪2 (确认上升趋势中的缩量回踩转强)
# ══════════════════════════════════════════════════════════════

class TrendFollowV2BT(_BTStrategyBase):
    """
    趋势跟踪2 — 推翻 EMA 金叉/突破入场，改买「确认上升趋势中的缩量回踩转强」。

    设计依据（全部来自旧 trend_follow 的 5 轮 ablation 证伪结论）：
      1. A 股日线突破失败率恒为 ~51%（信号层无解）→ 不在信号层硬刚，
         改从结构层降低失败【成本】：买在 MA20 支撑而非阻力突破，止损天然近。
      2. 右尾是唯一 alpha，砍右尾必亏（铁律）→ 出场端宽松吊灯、不锁利、
         不固定止盈、不死叉砍单、close-based 不插针。
      3. 指数 MA 门控是毒药（砍早期反转右尾）→ 绝不用大盘过滤；
         但个股自身趋势确认（MA20>MA60）非元凶，保留并作为入场前提。

    入场（4 条全满足）:
      A 趋势确认: MA20>MA60 且 MA60 上升 且 close>MA60
      B 前期动量: 近 prior_high_recent 日内创过 prior_high_n 日新高（确认有一条腿）
      C 缩量回踩: 近 pullback_lookback 日内 low 触及 MA20 且 close 仍站上 MA20，
                  且回踩期缩量（vol_ma5 < vol_ma20）
      D 转强确认: close>MA10 且 close>昨高（右侧确认，不接飞刀）

    出场:
      - 初始硬止损 = min(近 stop_struct_lookback 日最低, MA60) - stop_atr_buf×ATR，
        用 broker Stop 挂单（intrabar，跳空按开盘成交，抓住已验证对的那一半修复）
      - 保本: 浮盈达 +1R 后，close-based 止损上移到买入价
      - 吊灯追踪: trail = highest_since_buy - chandelier_k×ATR，close-based（不插针），
        k 保持宽松不收紧（让右尾奔跑）

    仓位: 按固定风险 R 下注 shares=(equity×risk_pct)/(entry-stop)，
          每笔最大亏损恒定，止损近的多买、远的少买，压低回撤。单票上限 max_pos_pct。
    """
    params = dict(
        ma_fast=10, ma_mid=20, ma_trend=60,
        atr_period=14,
        prior_high_n=60,          # B: 前高回看窗口
        prior_high_recent=15,     # B: 前高须发生在近 N 日内
        pullback_lookback=4,      # C: 回踩触及 MA20 的回看窗口
        pullback_touch_buf=0.01,  # C: low ≤ MA20×(1+buf) 算触及
        require_vol_contract=True,# C: 回踩须缩量
        stop_struct_lookback=5,   # 止损: 结构低点回看
        stop_atr_buf=0.5,         # 止损: 结构低点再下 buf×ATR
        max_entry_risk_pct=0.06,  # 止损: 止损距离 > 此值则放弃（清晰回踩本应贴近支撑）
        risk_pct=0.0075,          # 仓位: 单笔风险敞口（账户净值的 0.75%）
        max_pos_pct=0.30,         # 仓位: 单票市值上限（止损极近时防过度下注）
        breakeven_R=1.0,          # 出场: 浮盈达 N×R 后止损移到保本
        chandelier_k=3.0,         # 出场: 吊灯追踪 ATR 倍数（宽松，不收紧）
        cool_days=5,
        max_positions=5,
        position_pct=0.20,        # 兼容 runner 注入，本策略用 risk_pct 定量
        trade_start_date=None,
    )

    def __init__(self):
        self._base_init()
        self.indicators = {}
        self.stock_state = {}
        self.hard_stops = {}   # name -> broker Stop 挂单

        for d in self.datas:
            if d._name == 'idx_sh':
                continue   # 大盘指数不参与（本策略不做大盘过滤）
            self.indicators[d._name] = {
                'ma_fast':  bt.indicators.SMA(d.close, period=self.p.ma_fast),
                'ma_mid':   bt.indicators.SMA(d.close, period=self.p.ma_mid),
                'ma_trend': bt.indicators.SMA(d.close, period=self.p.ma_trend),
                'atr':      bt.indicators.ATR(d, period=self.p.atr_period),
                'hi_n':     bt.indicators.Highest(d.high, period=self.p.prior_high_n),
                'hi_recent':bt.indicators.Highest(d.high, period=self.p.prior_high_recent),
                'lo_pull':  bt.indicators.Lowest(d.low, period=self.p.pullback_lookback),
                'lo_stop':  bt.indicators.Lowest(d.low, period=self.p.stop_struct_lookback),
                'vol_ma5':  bt.indicators.SMA(d.volume, period=5),
                'vol_ma20': bt.indicators.SMA(d.volume, period=20),
            }
            self.stock_state[d._name] = {
                'in_position': False,
                'buy_price': 0.0,
                'stop_price': 0.0,        # 初始结构止损线
                'risk_per_share': 0.0,    # 1R
                'highest_since_buy': 0.0,
                'breakeven_armed': False, # 浮盈达 +1R 后置 True
                'last_sell_bar': -999,
            }

    def next(self):
        if self._before_trade_start():
            return

        sell_signals = []
        buy_plans = []   # (data, reason, conf, stop_price)
        cur_bar = len(self)

        for d in self.datas:
            name = d._name
            if name == 'idx_sh':
                continue
            if name in self.order_dict:
                continue
            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < self.p.ma_trend + 2:
                continue

            ma_fast = float(ind['ma_fast'][0])
            ma_mid  = float(ind['ma_mid'][0])
            ma_trend0 = float(ind['ma_trend'][0])
            ma_trend_prev = float(ind['ma_trend'][-self.p.ma_mid])
            atr = float(ind['atr'][0])
            close = float(d.close[0])

            if any(v != v for v in [ma_fast, ma_mid, ma_trend0, ma_trend_prev, atr]):
                continue

            # ───────────── 持仓中：出场判断 ─────────────
            if state['in_position']:
                state['highest_since_buy'] = max(state['highest_since_buy'], close)
                bp = state['buy_price']
                r = state['risk_per_share']

                # 保本闸：浮盈达 +breakeven_R×R 后武装
                if not state['breakeven_armed'] and r > 0 \
                        and close >= bp + self.p.breakeven_R * r:
                    state['breakeven_armed'] = True

                # close-based 止损线（初始硬止损由 broker 挂单负责 intrabar）
                trail_line = state['highest_since_buy'] - self.p.chandelier_k * atr
                floor_line = bp if state['breakeven_armed'] else state['stop_price']
                stop_line = max(trail_line, floor_line)

                if close <= stop_line:
                    self._cancel_hard_stop(name)
                    gain = (close - bp) / bp if bp > 0 else 0
                    if state['breakeven_armed']:
                        dd = (state['highest_since_buy'] - close) / state['highest_since_buy']
                        tag = f"吊灯追踪止损(k={self.p.chandelier_k}) 回撤{dd:.1%} 收益{gain:+.1%}"
                    else:
                        tag = f"保本前止损 买入{bp:.2f}→{close:.2f} {gain:+.1%}"
                    sell_signals.append((d, tag))
                    state['last_sell_bar'] = cur_bar
                continue

            # ───────────── 空仓：入场判断 ─────────────
            if cur_bar - state['last_sell_bar'] < self.p.cool_days:
                continue

            hi_n     = float(ind['hi_n'][0])
            hi_recent= float(ind['hi_recent'][0])
            lo_pull  = float(ind['lo_pull'][0])
            lo_stop  = float(ind['lo_stop'][0])
            vol_ma5  = float(ind['vol_ma5'][0])
            vol_ma20 = float(ind['vol_ma20'][0])
            prev_high= float(d.high[-1])
            if any(v != v for v in [hi_n, hi_recent, lo_pull, lo_stop, vol_ma5, vol_ma20]):
                continue

            # A 趋势确认
            cond_trend = (ma_mid > ma_trend0) and (ma_trend0 > ma_trend_prev) \
                and (close > ma_trend0)
            # B 前期动量：60 日高点发生在近 prior_high_recent 日内
            cond_momentum = hi_recent >= hi_n * 0.999
            # C 缩量回踩到 MA20：近 N 日 low 触及 MA20，今日 close 站上 MA20
            cond_touch = (lo_pull <= ma_mid * (1 + self.p.pullback_touch_buf)) \
                and (close > ma_mid)
            cond_vol = (not self.p.require_vol_contract) or (vol_ma5 < vol_ma20)
            # D 转强确认：今 close 站上 MA10 且 收在昨高之上
            cond_strength = (close > ma_fast) and (close > prev_high)

            if not (cond_trend and cond_momentum and cond_touch and cond_vol
                    and cond_strength):
                continue

            # 初始结构止损线 = 回踩 swing low 下方（贴近支撑，止损极紧）。
            # 不用 MA60：上升趋势中 MA60 常在下方 8~15%，会把止损放太远。
            stop_price = lo_stop - self.p.stop_atr_buf * atr
            if stop_price >= close:
                continue   # 止损无效（结构低点已在现价之上）
            risk_ps = close - stop_price
            if risk_ps / close > self.p.max_entry_risk_pct:
                continue   # 止损太远 = 不是贴支撑的干净回踩，放弃
            conf = min(1.0, (close - ma_mid) / (close * 0.05 + 1e-9))  # 越接近 MA20 越优
            buy_plans.append((d,
                f"回踩MA20转强 close{close:.2f}>MA10 止损{stop_price:.2f} "
                f"R={risk_ps/close:.1%} 量缩{vol_ma5/(vol_ma20 or 1):.2f}",
                conf, stop_price))

        self._execute_signals_risk(sell_signals, buy_plans)

    # ── 按风险 R 定量下单（取代基类固定金额 _execute_signals）──
    def _execute_signals_risk(self, sell_signals, buy_plans):
        for d, reason in sell_signals:
            o = self.close(data=d)
            o._reason = reason
            self.order_dict[d._name] = o

        buy_plans.sort(key=lambda x: -x[2])
        n_held = self._n_positions()
        available = self.p.max_positions - n_held + len(sell_signals)

        for d, reason, conf, stop_price in buy_plans:
            if available <= 0:
                break
            name = d._name
            if name in self.order_dict:
                continue
            price = float(d.close[0])
            risk_ps = price - stop_price
            if risk_ps <= 0:
                continue
            equity = self.broker.getvalue()
            cash = self.broker.getcash()
            # 按风险定股数
            risk_shares = (equity * self.p.risk_pct) / risk_ps
            # 单票市值上限 + 可用现金双重封顶
            cap_amount = min(equity * self.p.max_pos_pct, cash)
            cap_shares = cap_amount / price
            shares = int(min(risk_shares, cap_shares) / 100) * 100
            if shares < 100:
                continue
            # 记录待入场的止损线，notify_order 成交后用
            self.stock_state[name]['stop_price'] = stop_price
            self.stock_state[name]['risk_per_share'] = risk_ps
            o = self.buy(data=d, size=shares)
            o._reason = reason
            self.order_dict[name] = o
            available -= 1

    def _cancel_hard_stop(self, name):
        so = self.hard_stops.pop(name, None)
        if so is not None and so.alive():
            self.cancel(so)

    def notify_order(self, order):
        """成交回调：买入后挂出 broker Stop 硬止损（intrabar 抓跳空）；
        close-based 出场成交时清理挂单。防反向开空靠 _cancel_hard_stop +
        size-aware self.close()（同 TrendFollowBT intrabar 模式的已验证实现）。"""
        if order.status == order.Completed:
            name = order.data._name
            state = self.stock_state[name]
            if order.isbuy():
                state['in_position'] = True
                state['buy_price'] = order.executed.price
                state['highest_since_buy'] = order.executed.price
                state['breakeven_armed'] = False
                # 用实际成交价重算止损保持 R 一致性（成交价偏离信号价时）
                # stop_price/risk_per_share 已在下单前写入，这里保留信号价口径止损线
                self.trade_log.append({
                    'date': self.datetime.date(0).isoformat(),
                    'code': name, 'action': 'BUY',
                    'price': order.executed.price,
                    'size': order.executed.size,
                    'reason': getattr(order, '_reason', ''),
                })
                sz = order.executed.size
                stop_px = state['stop_price']
                so = self.sell(data=order.data, size=sz,
                               exectype=bt.Order.Stop, price=stop_px)
                so._reason = f"硬止损(挂单@{stop_px:.2f})"
                self.hard_stops[name] = so
                self.order_dict.pop(name, None)
                return
            else:
                state['in_position'] = False
                so = self.hard_stops.pop(name, None)
                if so is not None and so is not order and so.alive():
                    self.cancel(so)
                state['last_sell_bar'] = len(self)
                self.trade_log.append({
                    'date': self.datetime.date(0).isoformat(),
                    'code': name, 'action': 'SELL',
                    'price': order.executed.price,
                    'size': abs(order.executed.size),
                    'reason': getattr(order, '_reason', ''),
                })
                self.order_dict.pop(name, None)
                return
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.order_dict.pop(order.data._name, None)


# ══════════════════════════════════════════════════════════════
# 策略注册表（Backtrader 版本）
# ══════════════════════════════════════════════════════════════

BT_STRATEGY_MAP = {
    "trend_follow":       TrendFollowBT,
    "trend_follow_v2":    TrendFollowV2BT,
    "rsi_reversal":       RSIReversalBT,
    "bollinger_revert":   BollingerRevertBT,
    "major_capital_pump": MajorCapitalPumpBT,
    # major_capital_accumulation 在 bt_major_capital.py 中
}
