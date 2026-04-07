"""
主力低位建仓策略 — Backtrader 版本 v2
重构：Backtrader 仅作为执行器，策略内部封装选股/算法/风控

核心改进（vs v1）：
  1. 策略内部动态选股 — 基于价格/成交额/数据长度实时过滤 data feeds
  2. 自定义 RSI 指标   — 与原策略 _rsi() Wilder 平滑算法完全一致
  3. 信号优先级排序   — 每日收集全部 SELL/BUY 信号，SELL 先行、BUY 按信心排序

用法：
  python -m backtest.bt_major_capital --start 2025-01-01 --end 2026-03-31 --cash 100000
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import backtrader as bt

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CACHE_DIR = ROOT / "backtest_cache"
_NAME_CACHE_FILE = CACHE_DIR / "stock_names.json"
_NAME_CACHE_TTL  = 86400  # 24 小时


def _load_stock_name_cache() -> dict:
    """从本地缓存或 akshare 获取「代码→名称」映射（{code: name}）"""
    import time
    # 命中有效缓存
    if _NAME_CACHE_FILE.exists():
        age = time.time() - _NAME_CACHE_FILE.stat().st_mtime
        if age < _NAME_CACHE_TTL:
            try:
                with open(_NAME_CACHE_FILE, encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
    # 重新拉取（绕过系统代理，与 screener._bypass_proxy 逻辑相同）
    proxy_keys = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
                  "all_proxy", "ALL_PROXY"]
    saved_env = {k: os.environ.pop(k) for k in proxy_keys if k in os.environ}
    try:
        import akshare as ak
        df = ak.stock_info_a_code_name()  # 返回 code / name 两列，数据稳定
        if 'code' in df.columns and 'name' in df.columns:
            name_map = {str(row['code']).zfill(6): str(row['name']).strip()
                        for _, row in df.iterrows()}
            CACHE_DIR.mkdir(exist_ok=True)
            with open(_NAME_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(name_map, f, ensure_ascii=False)
            return name_map
    except Exception:
        pass
    finally:
        os.environ.update(saved_env)
    return {}


# ══════════════════════════════════════════════════════════════
# 自定义指标：与原策略 _rsi() 完全一致的 RSI
# ══════════════════════════════════════════════════════════════

class WilderRSI(bt.Indicator):
    """
    与 strategies.py BaseStrategy._rsi() 算法逐字节一致的 RSI。

    差异点（vs bt.indicators.RSI）：
      - 种子期：取 [bar1-bar0, bar2-bar1, ..., bar_period-bar_{period-1}] 的简单平均
      - 首个 RSI 值在第 period 根 bar（0-based）输出
      - Wilder 平滑从第 period 根 bar 开始（含种子期末尾 bar 的变化量）
      - 结果 round(2) 与原策略一致
    """
    lines = ('rsi',)
    params = (('period', 14),)

    def __init__(self):
        self.addminperiod(self.p.period + 1)

    def nextstart(self):
        p = self.p.period
        gains, losses = 0.0, 0.0
        for i in range(p):
            diff = self.data[-p + i + 1] - self.data[-p + i]
            if diff > 0:
                gains += diff
            else:
                losses -= diff
        self._avg_gain = gains / p
        self._avg_loss = losses / p
        rs = self._avg_gain / self._avg_loss if self._avg_loss > 1e-9 else 1e9
        self.lines.rsi[0] = round(100 - 100 / (1 + rs), 2)

    def next(self):
        p = self.p.period
        diff = self.data[0] - self.data[-1]
        self._avg_gain = (self._avg_gain * (p - 1) + max(diff, 0)) / p
        self._avg_loss = (self._avg_loss * (p - 1) + max(-diff, 0)) / p
        rs = self._avg_gain / self._avg_loss if self._avg_loss > 1e-9 else 1e9
        self.lines.rsi[0] = round(100 - 100 / (1 + rs), 2)


# ══════════════════════════════════════════════════════════════
# 策略主体
# ══════════════════════════════════════════════════════════════

class MajorCapitalBT(bt.Strategy):
    """
    主力低位建仓策略 (Backtrader v2)

    Backtrader 仅充当 "执行器"（数据分发、撮合、滑点/手续费）。
    选股、信号、风控全部在策略内部完成。
    """

    params = dict(
        # ── 阶段1 WATCH ──
        low_lookback=60,
        max_above_low_pct=20.0,
        ma_converge_pct=5.0,
        ma_slope_max=0.05,
        bb_period=20,
        bb_narrow_ratio=0.85,
        vol_yang_yin_min=1.03,
        vol_lookback=30,
        rsi_watch_min=25.0,
        rsi_watch_max=62.0,
        # ── 阶段2 BUY ──
        min_watch_days=20,              # ★ 优化：15→20 要求更长建仓确认
        breakout_pct=4.0,
        breakout_vol_ratio=2.0,         # ★ 优化：1.8→2.0 更强量能确认
        breakout_max_pct=8.0,           # ★ 新增：单日涨幅上限，超过视为追高
        rsi_buy_max=70.0,               # ★ 新增：买入时RSI上限，拒绝超买入场
        ma_slope_up_min=0.002,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        trend_filter=True,              # ★ 新增：趋势过滤 MA20>MA60
        # ── 出场 ──
        rsi_exit=80.0,
        rsi_exit_drop=8.0,
        trailing_pct=0.12,              # ★ 优化：0.15→0.12 收紧追踪止损
        ma_exit_days=5,
        ma_exit_grace=10,
        stop_loss_pct=0.08,             # ★ 优化：0.12→0.08 收紧硬止损
        breakout_fail_days=3,           # ★ 新增：3日突破失败止损
        time_stop_days=10,              # ★ 新增：10日不盈利则止损
        dist_vol_ratio=1.5,
        dist_shadow_pct=0.35,
        dist_min_gain=0.10,
        dist_confirm_ma=10,
        # ── 仓位 ──
        max_positions=5,
        position_pct=0.20,
        # ── 内部选股参数（主力建仓预设） ──
        screen_enabled=True,           # 是否启用策略内选股
        screen_min_price=2.0,
        screen_max_price=100.0,
        screen_min_amount_wan=300,     # 日均成交额下限（万元）建仓期成交低是特征
        screen_min_bars=80,            # 最少历史 bar 数（需覆盖 low_lookback=60 + buffer）
        screen_vol_window=20,          # 成交额计算窗口
    )

    def __init__(self):
        self.indicators = {}
        self.active_datas = []   # 通过选股过滤的 data feeds

        for d in self.datas:
            ind = {}
            ind['ma5'] = bt.indicators.SMA(d.close, period=5)
            ind['ma10'] = bt.indicators.SMA(d.close, period=10)
            ind['ma20'] = bt.indicators.SMA(d.close, period=20)
            ind['ma60'] = bt.indicators.SMA(d.close, period=60)
            # ★ 使用自定义 RSI，与原策略算法完全一致
            ind['rsi'] = WilderRSI(d.close, period=14)
            ind['macd'] = bt.indicators.MACD(
                d.close,
                period_me1=self.p.macd_fast,
                period_me2=self.p.macd_slow,
                period_signal=self.p.macd_signal,
            )
            ind['bb'] = bt.indicators.BollingerBands(d.close, period=self.p.bb_period)
            ind['ma_dist'] = bt.indicators.SMA(d.close, period=self.p.dist_confirm_ma)
            self.indicators[d._name] = ind

        # 每只股票的状态
        self.stock_state = {}
        for d in self.datas:
            self.stock_state[d._name] = self._init_state()

        self.order_dict = {}
        self.trade_log = []
        self._screened = {}   # name → bool, 选股结果缓存

    @staticmethod
    def _init_state():
        return {
            'watch_start': None,
            'accumulation_days': 0,
            'watch_signal_dates': [],   # 每日满足建仓条件的日期列表（最多保留60条）
            'in_position': False,
            'buy_price': 0.0,
            'highest_since_buy': 0.0,
            'days_below_ma': 0,
            'days_since_buy': 0,
            'rsi_peaked': False,
            'dist_warned': False,
            'dist_warn_info': '',
            'dist_warn_high': 0.0,
            'bb_bw_history': [],
            'breakout_day_low': 0.0,   # ★ 新增：突破日最低价（用于3日失败止损）
        }

    # ══════════════════════════════════════════════════════════
    # 1. 策略内部动态选股
    # ══════════════════════════════════════════════════════════

    def _screen_stock(self, d):
        """
        策略内部动态选股 — 对每个 data feed 进行实时过滤。
        等效于原引擎的 DynamicScreener(major_capital_accumulation 预设)。

        过滤维度：
          - 数据长度：>= screen_min_bars（排除新股/数据不足）
          - 价格区间：screen_min_price ~ screen_max_price
          - 成交额：近 N 日日均成交额 > screen_min_amount_wan 万

        注意：数据长度不足时不缓存结果（等数据积累够再判断）。
        """
        name = d._name

        # 数据不够长时不做最终判断，每次重新评估
        if len(d) < self.p.screen_min_bars:
            return False

        # 已有缓存结果 → 直接用
        if name in self._screened:
            return self._screened[name]

        passed = True

        price = float(d.close[0])
        if price < self.p.screen_min_price or price > self.p.screen_max_price:
            passed = False

        if passed:
            window = min(self.p.screen_vol_window, len(d))
            total_amount = 0.0
            for j in range(window):
                total_amount += float(d.close[-j]) * float(d.volume[-j])
            avg_amount_wan = (total_amount / window) / 1e4
            if avg_amount_wan < self.p.screen_min_amount_wan:
                passed = False

        self._screened[name] = passed
        return passed

    def _refresh_screening(self):
        """每 20 个交易日重新筛选（股价/成交额会变化）"""
        self._screened.clear()

    # ══════════════════════════════════════════════════════════
    # 2. 辅助指标（与原策略一致）
    # ══════════════════════════════════════════════════════════

    def _vol_ratio(self, d, period=5):
        if len(d) < period + 1:
            return None
        avg = sum(float(d.volume[-j]) for j in range(1, period + 1)) / period
        return float(d.volume[0]) / avg if avg > 1e-9 else None

    def _yang_yin_vol_ratio(self, d, lookback=30):
        n = min(lookback, len(d))
        yang_vols, yin_vols = [], []
        for j in range(n):
            if float(d.close[-j]) >= float(d.open[-j]):
                yang_vols.append(float(d.volume[-j]))
            else:
                yin_vols.append(float(d.volume[-j]))
        if not yang_vols or not yin_vols:
            return None
        return (sum(yang_vols) / len(yang_vols)) / (sum(yin_vols) / len(yin_vols))

    def _ma_convergence(self, ind):
        vals = [float(ind['ma5'][0]), float(ind['ma10'][0]), float(ind['ma20'][0])]
        if any(v != v for v in vals):
            return None
        mid_val = sorted(vals)[1]
        if mid_val <= 0:
            return None
        return (max(vals) - min(vals)) / mid_val * 100

    def _ma_slope(self, ma_line, days=5):
        if len(ma_line) < days + 1:
            return None
        base = float(ma_line[-days])
        cur = float(ma_line[0])
        if base != base or cur != cur or base <= 0:
            return None
        return (cur - base) / base / days

    def _near_low(self, d, lookback=60):
        n = min(lookback, len(d))
        low = min(float(d.low[-j]) for j in range(n))
        if low <= 0:
            return None
        return (float(d.close[0]) - low) / low * 100

    def _bb_bandwidth(self, ind):
        top = float(ind['bb'].top[0])
        bot = float(ind['bb'].bot[0])
        mid = float(ind['bb'].mid[0])
        if mid <= 0 or mid != mid:
            return None
        return (top - bot) / mid * 100

    def _is_bb_narrow(self, state, current_bw):
        if current_bw is None:
            return False
        history = state['bb_bw_history']
        recent = [x for x in history[-30:] if x is not None]
        if not recent:
            return False
        return current_bw < (sum(recent) / len(recent)) * self.p.bb_narrow_ratio

    def _n_positions(self):
        return sum(1 for d in self.datas if self.getposition(d).size > 0)

    # ══════════════════════════════════════════════════════════
    # 3. 信号生成（单只股票的当日判断）
    # ══════════════════════════════════════════════════════════

    def _check_sell(self, d, state, ind):
        """
        检查出场条件，返回 (sell_reason, None) 或 (None, None)。
        仅判断、不下单。
        """
        close = float(d.close[0])
        open_ = float(d.open[0])
        high = float(d.high[0])
        low = float(d.low[0])
        rsi = float(ind['rsi'][0])
        ma20_val = float(ind['ma20'][0])
        buy_price = state['buy_price']

        state['highest_since_buy'] = max(state['highest_since_buy'], close)
        state['days_since_buy'] += 1

        prev_close = float(d.close[-1]) if len(d) > 1 else close
        daily_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
        is_limit_up = daily_pct >= 9.5

        # 止盈1：RSI 超买回落
        if rsi >= self.p.rsi_exit and not is_limit_up:
            state['rsi_peaked'] = True
        if state['rsi_peaked'] and rsi < self.p.rsi_exit - self.p.rsi_exit_drop and not is_limit_up:
            gain = (close - buy_price) / buy_price
            return f"超买回落: RSI→{rsi:.0f} 收益{gain:+.1%}"

        # 止盈2：放量冲高回落 + 跌破短期均线（两步确认）
        cur_gain = (close - buy_price) / buy_price
        if cur_gain >= self.p.dist_min_gain:
            k_range = high - low
            if k_range > 0 and high >= state['highest_since_buy']:
                body_top = max(open_, close)
                upper_shadow = (high - body_top) / k_range
                vr_i = self._vol_ratio(d, 5)
                if (upper_shadow >= self.p.dist_shadow_pct
                        and vr_i is not None
                        and vr_i >= self.p.dist_vol_ratio):
                    state['dist_warned'] = True
                    state['dist_warn_high'] = state['highest_since_buy']
                    state['dist_warn_info'] = (
                        f"冲高回落: 影{upper_shadow:.0%} 量比{vr_i:.1f}x"
                    )

        if state['dist_warned']:
            if close > state['dist_warn_high']:
                state['dist_warned'] = False
                state['dist_warn_info'] = ''
                state['dist_warn_high'] = 0.0
            else:
                ma_dist_val = float(ind['ma_dist'][0])
                if ma_dist_val == ma_dist_val and close < ma_dist_val:
                    gain = (close - buy_price) / buy_price
                    return (
                        f"{state['dist_warn_info']}→破MA{self.p.dist_confirm_ma} "
                        f"收益{gain:+.1%}"
                    )

        # 止损3a：3日突破失败止损（突破后3日内跌破突破日低点）
        if (self.p.breakout_fail_days > 0
                and state['days_since_buy'] <= self.p.breakout_fail_days
                and state['breakout_day_low'] > 0
                and close < state['breakout_day_low']):
            loss = (close - buy_price) / buy_price
            return f"突破失败({state['days_since_buy']}日): 破突破日低点 {loss:.1%}"

        # 止损3b：10日不盈利则止损（死钱退出）
        if (self.p.time_stop_days > 0
                and state['days_since_buy'] >= self.p.time_stop_days
                and close <= buy_price):
            loss = (close - buy_price) / buy_price
            return f"时间止损({state['days_since_buy']}日未盈利): {loss:.1%}"

        # 止损3：硬止损
        loss = (close - buy_price) / buy_price
        if loss <= -self.p.stop_loss_pct:
            return f"硬止损: {loss:.1%}"

        # 止盈3：追踪止损
        highest = state['highest_since_buy']
        drawdown = (highest - close) / highest if highest > 0 else 0
        if drawdown >= self.p.trailing_pct and highest > buy_price:
            gain = (close - buy_price) / buy_price
            return f"追踪止损: 回撤{drawdown:.1%} 收益{gain:+.1%}"

        # 止损4：跌破 MA20 连续 N 日
        if state['days_since_buy'] > self.p.ma_exit_grace:
            if close < ma20_val:
                state['days_below_ma'] += 1
                if state['days_below_ma'] >= self.p.ma_exit_days:
                    gain = (close - buy_price) / buy_price
                    return f"破MA20连续{state['days_below_ma']}日 收益{gain:+.1%}"
            else:
                state['days_below_ma'] = 0
        else:
            if close >= ma20_val:
                state['days_below_ma'] = 0

        return None

    def _check_buy(self, d, state, ind):
        """
        检查两阶段买入信号，返回 (trigger, confidence) 或 (None, 0)。
        仅判断、不下单。
        """
        close = float(d.close[0])
        open_ = float(d.open[0])
        rsi = float(ind['rsi'][0])
        dif = float(ind['macd'].macd[0])
        dea = float(ind['macd'].signal[0])
        ma20_val = float(ind['ma20'][0])

        if any(v != v for v in [rsi, dif, dea, ma20_val]):
            return None, 0

        is_bull = close >= open_

        # ── 检测建仓中条件 ──
        near_low = self._near_low(d, self.p.low_lookback)
        conv = self._ma_convergence(ind)
        slope = self._ma_slope(ind['ma20'], 5)
        yy_ratio = self._yang_yin_vol_ratio(d, self.p.vol_lookback)

        is_accumulating = (
            near_low is not None and near_low <= self.p.max_above_low_pct
            and conv is not None and conv <= self.p.ma_converge_pct
            and (slope is None or abs(slope) <= self.p.ma_slope_max)
            and self.p.rsi_watch_min <= rsi <= self.p.rsi_watch_max
            and yy_ratio is not None and yy_ratio >= self.p.vol_yang_yin_min
        )

        # 阶段1：WATCH
        if is_accumulating:
            state['accumulation_days'] += 1
            if state['watch_start'] is None:
                state['watch_start'] = len(d)
            # 记录满足建仓条件的日期（去重，最多保留60条）
            today_iso = self.datetime.date(0).isoformat()
            if not state['watch_signal_dates'] or state['watch_signal_dates'][-1] != today_iso:
                state['watch_signal_dates'].append(today_iso)
                if len(state['watch_signal_dates']) > 60:
                    state['watch_signal_dates'] = state['watch_signal_dates'][-60:]
        else:
            if state['accumulation_days'] < self.p.min_watch_days:
                state['watch_start'] = None
                state['accumulation_days'] = 0
                state['watch_signal_dates'] = []

        # 阶段2：BUY 临界信号
        if state['watch_start'] is None or state['accumulation_days'] < self.p.min_watch_days:
            return None, 0, {}

        trigger = ''
        trigger_strength = 0

        # 信号A：放量大阳线突破
        prev_close = float(d.close[-1]) if len(d) > 1 else close
        pct_chg = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
        vr = self._vol_ratio(d, 5)
        if (is_bull and pct_chg >= self.p.breakout_pct
                and vr is not None and vr >= self.p.breakout_vol_ratio):
            trigger = f"放量突破+{pct_chg:.1f}% 量比{vr:.1f}x"
            trigger_strength = 3

        # 信号B：突破布林上轨（从收窄状态）
        if not trigger:
            bb_top = float(ind['bb'].top[0])
            if bb_top == bb_top and close > bb_top:
                was_narrow = False
                for j in range(1, min(11, len(state['bb_bw_history']))):
                    bw_j = state['bb_bw_history'][-j] if j <= len(state['bb_bw_history']) else None
                    if bw_j is not None:
                        recent_bws = [x for x in state['bb_bw_history'][-30 - j:-j] if x is not None]
                        if recent_bws and bw_j < (sum(recent_bws) / len(recent_bws)) * self.p.bb_narrow_ratio:
                            was_narrow = True
                            break
                if was_narrow:
                    trigger = f"突破布林上轨{bb_top:.2f}"
                    trigger_strength = 3

        # 信号C：均线多头发散 + MA20 斜率转正
        if not trigger:
            ma5_val = float(ind['ma5'][0])
            ma10_val = float(ind['ma10'][0])
            ma20_slope = self._ma_slope(ind['ma20'], 5)
            if (ma5_val > ma10_val > ma20_val
                    and ma20_slope is not None
                    and ma20_slope >= self.p.ma_slope_up_min):
                prev_slope = None
                if len(ind['ma20']) > 10:
                    try:
                        base_val = float(ind['ma20'][-10])
                        mid_val = float(ind['ma20'][-5])
                        if base_val == base_val and mid_val == mid_val and base_val > 0:
                            prev_slope = (mid_val - base_val) / base_val / 5
                    except (IndexError, ValueError):
                        pass
                if prev_slope is not None and prev_slope < self.p.ma_slope_up_min:
                    trigger = f"均线多头发散 MA20↑{ma20_slope:.4f}"
                    trigger_strength = 2

        # 信号D：MACD零轴上方金叉
        if not trigger:
            if len(ind['macd'].macd) > 1:
                prev_dif = float(ind['macd'].macd[-1])
                prev_dea = float(ind['macd'].signal[-1])
                if (prev_dif == prev_dif and prev_dea == prev_dea
                        and prev_dif <= prev_dea and dif > dea
                        and dif >= 0):
                    trigger = "MACD零轴上方金叉"
                    trigger_strength = 2

        if not trigger:
            return None, 0, {}

        # ★ 优化：RSI 上限过滤（拒绝超买入场）
        if rsi > self.p.rsi_buy_max:
            return None, 0, {}

        # ★ 优化：单日涨幅上限（超过 breakout_max_pct 视为追高）
        prev_close = float(d.close[-1]) if len(d) > 1 else close
        day_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
        if day_pct > self.p.breakout_max_pct:
            return None, 0, {}

        # ★ 优化：趋势过滤（要求 MA20 > MA60，中期趋势向上）
        if self.p.trend_filter:
            ma60_val = float(ind['ma60'][0])
            if ma60_val == ma60_val and ma20_val <= ma60_val:
                return None, 0, {}

        # ── 计算信心 ──
        conf = 0.3 + trigger_strength * 0.1
        if state['accumulation_days'] >= 30:
            conf += 0.15
        elif state['accumulation_days'] >= 20:
            conf += 0.10
        if yy_ratio and yy_ratio >= 1.10:
            conf += 0.10

        bw = self._bb_bandwidth(ind)
        bb_narrow = self._is_bb_narrow(state, bw) or any(
            self._is_bb_narrow(state, state['bb_bw_history'][-j])
            for j in range(1, min(11, len(state['bb_bw_history'])))
            if state['bb_bw_history'][-j] is not None
        ) if state['bb_bw_history'] else False
        if bb_narrow:
            conf += 0.10
        conf = min(conf, 1.0)

        reason = (
            f"建仓完毕: {trigger} | "
            f"累计{state['accumulation_days']}天 "
            f"阳阴量比{yy_ratio:.2f} RSI={rsi:.0f}"
        )
        meta = {
            'trigger':            trigger,
            'accumulation_days':  state['accumulation_days'],
            'watch_signal_dates': list(state['watch_signal_dates']),
            'confidence':         round(conf, 3),
            'rsi':                round(rsi, 1),
            'near_low_pct':       round(near_low, 1) if near_low is not None else None,
            'ma_converge_pct':    round(conv, 2) if conv is not None else None,
            'yy_ratio':           round(yy_ratio, 2) if yy_ratio is not None else None,
            'bb_narrow':          bb_narrow,
        }
        return reason, conf, meta

    # ══════════════════════════════════════════════════════════
    # 4. 核心 next — 信号收集 + 优先级排序 + 批量执行
    # ══════════════════════════════════════════════════════════

    def notify_order(self, order):
        if order.status in [order.Completed]:
            name = order.data._name
            state = self.stock_state[name]
            if order.isbuy():
                state['in_position'] = True
                state['buy_price'] = order.executed.price
                state['highest_since_buy'] = order.executed.price
                state['days_below_ma'] = 0
                state['days_since_buy'] = 0
                state['rsi_peaked'] = False
                state['dist_warned'] = False
                state['dist_warn_info'] = ''
                state['dist_warn_high'] = 0.0
                # ★ 记录突破日最低价（用于3日突破失败止损）
                try:
                    state['breakout_day_low'] = float(order.data.low[0])
                except Exception:
                    state['breakout_day_low'] = order.executed.price * 0.95
                self.trade_log.append({
                    'date':       self.datetime.date(0).isoformat(),
                    'code':       name,
                    'action':     'BUY',
                    'price':      order.executed.price,
                    'size':       order.executed.size,
                    'reason':     getattr(order, '_reason', ''),
                    'confidence': getattr(order, '_conf', 0),
                    'buy_meta':   getattr(order, '_buy_meta', {}),
                })
            else:
                state['in_position'] = False
                state['watch_start'] = None
                state['accumulation_days'] = 0
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

    def next(self):
        # 每 20 日刷新选股（价格/成交额会变化）
        bar_count = len(self.datas[0]) if self.datas else 0
        if bar_count > 0 and bar_count % 20 == 0:
            self._refresh_screening()

        min_len = max(self.p.low_lookback, self.p.macd_slow + self.p.macd_signal + 5, 60)

        # ── Phase 1：收集所有信号 ──────────────────────────
        sell_signals = []   # [(data, reason)]
        buy_signals = []    # [(data, reason, confidence)]

        for d in self.datas:
            name = d._name
            if name in self.order_dict:
                continue

            ind = self.indicators[name]
            state = self.stock_state[name]

            if len(d) < min_len:
                continue

            # 更新布林带宽度历史
            bw = self._bb_bandwidth(ind)
            state['bb_bw_history'].append(bw)

            # NaN 检查
            rsi = float(ind['rsi'][0])
            dif = float(ind['macd'].macd[0])
            dea = float(ind['macd'].signal[0])
            ma20_val = float(ind['ma20'][0])
            if any(v != v for v in [rsi, dif, dea, ma20_val]):
                continue

            # ── 已持仓 → 检查出场 ──
            if state['in_position']:
                sell_reason = self._check_sell(d, state, ind)
                if sell_reason:
                    pos = self.getposition(d)
                    if pos.size > 0:
                        sell_signals.append((d, sell_reason))
                continue

            # ── 动态选股过滤 ──
            if self.p.screen_enabled and not self._screen_stock(d):
                continue

            # ── 未持仓 → 检查买入 ──
            reason, conf, meta = self._check_buy(d, state, ind)
            if reason:
                buy_signals.append((d, reason, conf, meta))

        # ── Phase 2：执行 SELL（优先释放仓位和资金） ──────
        for d, reason in sell_signals:
            o = self.close(data=d)
            o._reason = reason
            self.order_dict[d._name] = o

        # ── Phase 3：执行 BUY（按信心降序，受仓位限制） ──
        buy_signals.sort(key=lambda x: -x[2])

        n_held = self._n_positions()
        # 加上即将释放的仓位
        n_freeing = len(sell_signals)
        available_slots = self.p.max_positions - n_held + n_freeing

        for d, reason, conf, meta in buy_signals:
            if available_slots <= 0:
                break
            name = d._name
            if name in self.order_dict:
                continue

            cash = self.broker.getcash()
            total_val = self.broker.getvalue()
            target_amount = total_val / self.p.max_positions
            max_amount = min(target_amount, cash, total_val * self.p.position_pct)

            price = float(d.close[0])
            shares = int(max_amount / price / 100) * 100
            if shares < 100:
                continue

            o = self.buy(data=d, size=shares)
            o._reason = reason
            o._conf = conf
            o._buy_meta = meta
            self.order_dict[name] = o
            available_slots -= 1

            # 重置观察状态
            state = self.stock_state[name]
            state['watch_start'] = None
            state['accumulation_days'] = 0
            state['watch_signal_dates'] = []


# ══════════════════════════════════════════════════════════════
# 数据加载
# ══════════════════════════════════════════════════════════════

def load_cache_data(code, start, end):
    import pandas as pd

    exact = CACHE_DIR / f"{code}_{start}_{end}_qfq.json"
    if exact.exists():
        with open(exact, encoding='utf-8') as f:
            return _bars_to_df(json.load(f), start, end)

    # 模糊匹配
    candidates = list(CACHE_DIR.glob(f"{code}_*_qfq.json"))
    best, best_end = None, ''
    for p in candidates:
        parts = p.stem.split('_')
        if len(parts) < 4:
            continue
        c_start, c_end = parts[1], parts[2]
        if c_start <= start and c_end >= end and c_end > best_end:
            best, best_end = p, c_end
    if best is None:
        for p in candidates:
            parts = p.stem.split('_')
            if len(parts) < 4:
                continue
            c_start, c_end = parts[1], parts[2]
            if c_start <= start and c_end > best_end:
                best, best_end = p, c_end

    if best is None:
        return None
    with open(best, encoding='utf-8') as f:
        return _bars_to_df(json.load(f), start, end)


def _bars_to_df(bars, start, end):
    import pandas as pd
    df = pd.DataFrame(bars)
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= start) & (df['date'] <= end)]
    if df.empty:
        return None
    df = df.set_index('date').sort_index()
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col not in df.columns:
            return None
        df[col] = df[col].astype(float)
    return df


def fetch_tencent(code, market, start, end):
    import pandas as pd
    import requests

    prefix = "sh" if market == "SH" else "sz"
    symbol = f"{prefix}{code}"
    url = (
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        f"?param={symbol},day,{start},{end},800,qfq"
    )
    session = requests.Session()
    session.trust_env = False
    session.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json().get('data', {}).get(symbol, {})
        klines = data.get('qfqday') or data.get('day') or []
        if not klines:
            return None
        bars = []
        for k in klines:
            if len(k) < 6:
                continue
            d = k[0]
            if d < start or d > end:
                continue
            bars.append({
                'date': d, 'open': float(k[1]), 'high': float(k[3]),
                'low': float(k[4]), 'close': float(k[2]),
                'volume': int(float(k[5])),
            })
        return _bars_to_df(bars, start, end) if bars else None
    except Exception as e:
        print(f"  [腾讯] {code} 失败: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# 股票池加载（尽量多加载，由策略内部过滤）
# ══════════════════════════════════════════════════════════════

def get_all_cached_stocks(start, end):
    """
    加载缓存中所有有数据的股票（不做预筛选，由策略内部选股）。
    尽量选覆盖范围最广的缓存文件（含预热期数据）。
    """
    # 收集每只股票最优缓存
    best_files = {}  # code → (path, c_start, c_end)
    for p in CACHE_DIR.glob("*_qfq.json"):
        parts = p.stem.split('_')
        if len(parts) < 4:
            continue
        code, c_start, c_end = parts[0], parts[1], parts[2]
        # 缓存必须覆盖到 start 以前（有数据），且 end 尽可能晚
        if c_start > start:
            continue
        prev = best_files.get(code)
        if prev is None or c_end > prev[2] or (c_end == prev[2] and c_start < prev[1]):
            best_files[code] = (p, c_start, c_end)

    stocks = []
    for code, (path, c_start, c_end) in best_files.items():
        market = 'SH' if code.startswith('6') else 'SZ'
        stocks.append({'code': code, 'name': code, 'market': market})
    print(f"[数据源] 缓存中发现 {len(stocks)} 只股票")
    return stocks


# ══════════════════════════════════════════════════════════════
# 主运行入口
# ══════════════════════════════════════════════════════════════

def run(start='2025-01-01', end='2026-03-31', cash=100000.0,
        use_network=False, screen_enabled=True):
    import pandas as pd

    screen_label = "启用" if screen_enabled else "关闭"
    print(f"\n{'='*60}")
    print(f"  主力建仓策略 Backtrader 回测 v2")
    print(f"  区间: {start} ~ {end}  初始资金: ¥{cash:,.0f}")
    print(f"  自定义RSI + 信号优先级 + 策略内选股({screen_label})")
    print(f"{'='*60}\n")

    # 加载所有缓存股票（不预筛选，策略内部动态选股）
    all_stocks = get_all_cached_stocks(start, end)
    if not all_stocks:
        print("无可用股票数据")
        return

    # 仓位配置
    n_stocks = len(all_stocks)
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

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.broker.set_slippage_perc(0.002)

    # 数据预热期：从 start 前 1 年开始加载，让指标有充分的 warmup
    from datetime import datetime as _dt, timedelta
    warmup_start = (_dt.strptime(start, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    loaded = 0
    for stock in all_stocks:
        code = stock['code']
        market = stock.get('market', 'SZ')
        # 先尝试带预热期的更长范围，降级到原始范围
        df = load_cache_data(code, warmup_start, end)
        if df is None:
            df = load_cache_data(code, start, end)
        if df is None and use_network:
            df = fetch_tencent(code, market, warmup_start, end)
        if df is None or len(df) < 60:
            continue

        data = bt.feeds.PandasData(
            dataname=df, name=code, datetime=None,
            open='open', high='high', low='low', close='close',
            volume='volume', openinterest=-1,
        )
        cerebro.adddata(data)
        loaded += 1

    print(f"[数据] 加载 {loaded}/{len(all_stocks)} 只股票 (策略内部再动态选股)\n")
    if loaded == 0:
        print("无有效数据，无法回测")
        return

    cerebro.addstrategy(
        MajorCapitalBT,
        max_positions=max_pos,
        position_pct=pos_pct,
        screen_enabled=screen_enabled,
    )
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.025)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

    print("正在运行回测...\n")
    results = cerebro.run()
    strat = results[0]

    # ── 输出结果 ──
    final_val = cerebro.broker.getvalue()
    total_return = (final_val - cash) / cash

    sharpe_analysis = strat.analyzers.sharpe.get_analysis()
    sharpe = sharpe_analysis.get('sharperatio') or 0

    dd_analysis = strat.analyzers.drawdown.get_analysis()
    max_dd = dd_analysis.get('max', {}).get('drawdown', 0) / 100

    trade_analysis = strat.analyzers.trades.get_analysis()
    total_trades = trade_analysis.get('total', {}).get('total', 0)
    won = trade_analysis.get('won', {}).get('total', 0)
    lost = trade_analysis.get('lost', {}).get('total', 0)
    win_rate = won / (won + lost) if (won + lost) > 0 else 0

    # 策略选中了多少只
    if screen_enabled:
        screened_count = sum(1 for v in strat._screened.values() if v)
        screened_fail = sum(1 for v in strat._screened.values() if not v)
    else:
        screened_count = loaded
        screened_fail = 0

    print(f"{'='*60}")
    print(f"  回测结果")
    print(f"{'='*60}")
    print(f"  初始资金:   ¥{cash:>12,.2f}")
    print(f"  最终净值:   ¥{final_val:>12,.2f}")
    print(f"  总收益:     {total_return:>+11.2%}")
    print(f"  期间盈亏:   ¥{final_val - cash:>+12,.2f}")
    print(f"  夏普比率:   {sharpe:>11.2f}")
    print(f"  最大回撤:   {max_dd:>11.2%}")
    print(f"  总交易次数: {total_trades:>11d}")
    print(f"  胜率:       {win_rate:>11.1%}")
    print(f"  持仓配置:   最多{max_pos}只 单仓{pos_pct:.0%}")
    print(f"  策略选股:   通过{screened_count}只 / 过滤{screened_fail}只")

    if strat.trade_log:
        print(f"\n{'─'*60}")
        print(f"  交易明细 (共{len(strat.trade_log)}笔)")
        print(f"{'─'*60}")
        for t in strat.trade_log[-40:]:
            tag = 'BUY ' if t['action'] == 'BUY' else 'SELL'
            print(f"  [{tag}] {t['date']} {t['code']} "
                  f"x{t['size']} @{t['price']:.2f} | {t['reason']}")
        if len(strat.trade_log) > 40:
            print(f"  ... 共{len(strat.trade_log)}笔 (只显示最近40笔)")

    print(f"\n{'='*60}\n")

    return {
        'final_value': final_val,
        'total_return': total_return,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'total_trades': total_trades,
        'win_rate': win_rate,
        'screened_pass': screened_count,
        'screened_fail': screened_fail,
        'trade_log': strat.trade_log,
    }


# ══════════════════════════════════════════════════════════════
# Web API 入口 — 供 web/app.py _do_backtest() 调用
# ══════════════════════════════════════════════════════════════

async def run_for_web(strategy_name: str, start: str, end: str, cash: float,
                      log_fn=None, screen_preset: str = "default"):
    """
    运行 Backtrader 回测，返回与 runner.run_backtest() 完全兼容的字典格式。

    返回值:
        {"metrics": {...}, "equity_data": {...}, "trades_paired": [...]}
        或 {"error": "..."} 失败时
    """
    import asyncio
    import pandas as pd
    from collections import defaultdict
    from datetime import datetime as _dt, timedelta

    def log(msg, level="info"):
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    log("Backtrader v2 引擎启动...")

    # ── 加载股票数据 ──
    all_stocks = get_all_cached_stocks(start, end)
    if not all_stocks:
        return {"error": "无可用股票缓存数据，请先运行数据采集"}

    n_stocks = len(all_stocks)
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

    log(f"数据源: {n_stocks}只股票, 最大持仓={max_pos}, 单仓={pos_pct:.0%}")

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.broker.set_slippage_perc(0.002)

    warmup_start = (_dt.strptime(start, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')

    loaded = 0
    code_name_map = {}
    for stock in all_stocks:
        code = stock['code']
        market = stock.get('market', 'SZ')
        df = load_cache_data(code, warmup_start, end)
        if df is None:
            df = load_cache_data(code, start, end)
        if df is None or len(df) < 60:
            continue
        data = bt.feeds.PandasData(
            dataname=df, name=code, datetime=None,
            open='open', high='high', low='low', close='close',
            volume='volume', openinterest=-1,
        )
        cerebro.adddata(data)
        code_name_map[code] = stock.get('name', code)
        loaded += 1

    if loaded == 0:
        return {"error": "无有效历史数据可加载"}

    # 用本地缓存（或 akshare）补全股票名称
    try:
        name_cache = _load_stock_name_cache()
        for code in list(code_name_map.keys()):
            if code in name_cache:
                code_name_map[code] = name_cache[code]
    except Exception:
        pass

    log(f"加载 {loaded}/{n_stocks} 只股票数据")

    cerebro.addstrategy(
        MajorCapitalBT,
        max_positions=max_pos,
        position_pct=pos_pct,
        screen_enabled=True,
    )
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.025)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='time_return')

    log("正在运行 Backtrader 回测...")

    # 在线程池中运行 (cerebro.run 是同步阻塞的)
    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, cerebro.run)
    strat = results[0]

    final_val = cerebro.broker.getvalue()
    total_return = (final_val - cash) / cash

    # ── 提取分析器结果 ──
    sharpe_a = strat.analyzers.sharpe.get_analysis()
    sharpe = sharpe_a.get('sharperatio') or 0

    dd_a = strat.analyzers.drawdown.get_analysis()
    max_dd = dd_a.get('max', {}).get('drawdown', 0) / 100

    trade_a = strat.analyzers.trades.get_analysis()
    total_trades = trade_a.get('total', {}).get('total', 0)
    won = trade_a.get('won', {}).get('total', 0)
    lost = trade_a.get('lost', {}).get('total', 0)
    win_rate = won / (won + lost) if (won + lost) > 0 else 0

    # 盈亏比
    won_pnl = trade_a.get('won', {}).get('pnl', {}).get('total', 0)
    lost_pnl = abs(trade_a.get('lost', {}).get('pnl', {}).get('total', 0))
    profit_factor = round(won_pnl / lost_pnl, 2) if lost_pnl > 0 else (0.0 if won_pnl == 0 else 99.99)

    # 年化收益
    time_returns = strat.analyzers.time_return.get_analysis()
    n_days = len(time_returns) if time_returns else 1
    annualized = (1 + total_return) ** (252 / max(n_days, 1)) - 1

    period_profit = round(final_val - cash, 2)

    # 选股统计
    screened_pass = sum(1 for v in strat._screened.values() if v)
    screened_fail = sum(1 for v in strat._screened.values() if not v)

    # ── 构建 metrics（与 runner.py 格式一致） ──
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
        "win_rate":          f"{win_rate*100:.1f}%",
        "profit_factor":     profit_factor,
        "total_trades":      total_trades,
        "period_profit":     period_profit,
        "period_profit_fmt": f"{period_profit:+,.2f} 元",
        "final_value_fmt":   f"{final_val:,.2f} 元",
        "verified_pass":     screened_pass,
        "verified_excl":     screened_fail,
        "per_stock":         {},
        "stock_count":       loaded,
    }

    # ── 构建 equity_data（净值曲线） ──
    # 从 TimeReturn 分析器构建日净值序列
    dates_list = []
    values_list = []
    abs_values_list = []
    cum_val = cash
    for dt_key, ret in sorted(time_returns.items()):
        dt_str = dt_key.strftime('%Y-%m-%d') if hasattr(dt_key, 'strftime') else str(dt_key)
        # 只取回测区间内的数据
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

    # ── 构建 trades_paired（配对交易记录） ──
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
            # 逐股盈亏
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

    # 按卖出日期降序排列（持仓中排最前）
    def _sort_key(t):
        sd = t.get("sell_date", "")
        if sd == "（持仓中）":
            return "9999-99-99"
        return sd if sd else t.get("buy_date", "")
    trades_paired.sort(key=_sort_key, reverse=True)

    # 逐股盈亏百分比
    for code, ps in per_stock.items():
        # 计算该股总买入成本
        total_buy = sum(
            t["buy_price"] * t["shares"]
            for t in trades_paired
            if t["code"] == code and t["buy_price"] > 0
        )
        ps["pnl_pct"] = round(ps["pnl"] / total_buy * 100, 2) if total_buy > 0 else 0

    metrics["per_stock"] = per_stock
    # 用 trades_paired 长度覆盖 total_trades，与交易详情"总交易"保持一致
    # （Backtrader TradeAnalyzer.total.total 可能将分批买入计为多笔，导致数值偏大）
    metrics["total_trades"] = len(trades_paired)

    log(f"回测完成: 收益={total_return*100:+.2f}% 夏普={sharpe:.2f} "
        f"最大回撤={max_dd*100:.2f}% 交易={total_trades}笔 胜率={win_rate*100:.1f}%")

    return {
        "metrics":       metrics,
        "equity_data":   equity_data,
        "trades_paired": trades_paired,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='主力建仓策略 Backtrader v2')
    parser.add_argument('--start', default='2025-01-01')
    parser.add_argument('--end', default='2026-03-31')
    parser.add_argument('--cash', type=float, default=100000)
    parser.add_argument('--network', action='store_true', help='允许联网拉取数据')
    parser.add_argument('--no-screen', action='store_true', help='关闭策略内选股')
    args = parser.parse_args()
    run(start=args.start, end=args.end, cash=args.cash,
        use_network=args.network, screen_enabled=not args.no_screen)
