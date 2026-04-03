"""
策略模块 v5 — 动态选股 + 优化信号逻辑

核心改进：
  ★ 所有策略内置趋势过滤，熊市中不盲目抄底
  ★ 统一使用追踪止损保护利润
  ★ 多源验证仍为强制前置条件（integrity_pass）

内置策略：
  1. 趋势跟踪 (EMA)  — 顺大势做多，EMA金叉入场，追踪止损出场
  2. RSI反转          — 超卖反弹 + 动量确认，快进快出
  3. 布林带回归       — 触碰下轨买入，回归中轨止盈
  4. 主力拉升         — 量价放大+MACD+RSI共振，追踪主力资金拉升行情
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from utils.logger import setup_logger

logger = setup_logger("strategies")


@dataclass
class Signal:
    """交易信号"""
    date:       str
    code:       str
    action:     str          # BUY / SELL
    price:      float
    reason:     str
    confidence: float = 1.0


@dataclass
class VerifyResult:
    """验证门控结果"""
    passed:  bool
    reason:  str


# ══════════════════════════════════════════════════════════
# 基类：统一验证门控
# ══════════════════════════════════════════════════════════

class BaseStrategy(ABC):
    name: str = "base"
    requires_pe:         bool = False
    requires_market_cap: bool = False

    def generate_signals(
        self,
        code:  str,
        bars:  list[dict],
        extra: dict = None,
    ) -> list[Signal]:
        extra = extra or {}
        vr = self._check_verified(code, extra)

        if not vr.passed:
            logger.info(f"[{self.name}] {code} 未通过验证门控 | {vr.reason}")
            return []

        logger.debug(f"[{self.name}] {code} 验证通过 | {vr.reason}")
        return self._signals(code, bars, extra)

    def _check_verified(self, code: str, extra: dict) -> VerifyResult:
        if extra.get("integrity_pass") is not True:
            missing = extra.get("missing_fields", [])
            reason = f"完整性自检失败: {', '.join(missing)}" if missing else "验证未通过"
            return VerifyResult(passed=False, reason=reason)

        if self.requires_pe:
            pe = extra.get("pe")
            if pe is None or (isinstance(pe, float) and pe <= 0):
                return VerifyResult(passed=False, reason="PE数据缺失")

        if self.requires_market_cap:
            cap = extra.get("market_cap")
            if cap is None or (isinstance(cap, float) and cap <= 0):
                return VerifyResult(passed=False, reason="市值数据缺失")

        sources = extra.get("verified_sources", [])
        src_note = f"来源: {', '.join(sources)}" if sources else "来源: AKShare"
        return VerifyResult(passed=True, reason=f"验证通过 | {src_note}")

    @abstractmethod
    def _signals(self, code: str, bars: list[dict], extra: dict) -> list[Signal]:
        ...

    # ── 技术指标工具 ──────────────────────────────────────

    def _sma(self, closes: list[float], n: int) -> list[Optional[float]]:
        result = [None] * len(closes)
        for i in range(n - 1, len(closes)):
            result[i] = sum(closes[i - n + 1: i + 1]) / n
        return result

    def _ema(self, closes: list[float], n: int) -> list[Optional[float]]:
        result = [None] * len(closes)
        k = 2 / (n + 1)
        for i, c in enumerate(closes):
            if i == 0:
                result[i] = c
            elif result[i - 1] is not None:
                result[i] = c * k + result[i - 1] * (1 - k)
        return result

    def _rsi(self, closes: list[float], period: int = 14) -> list[Optional[float]]:
        result = [None] * len(closes)
        if len(closes) < period + 1:
            return result
        gains, losses = [], []
        for i in range(1, period + 1):
            diff = closes[i] - closes[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        for i in range(period, len(closes)):
            diff = closes[i] - closes[i - 1]
            avg_gain = (avg_gain * (period - 1) + max(diff, 0)) / period
            avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
            rs = avg_gain / avg_loss if avg_loss > 1e-9 else 1e9
            result[i] = round(100 - 100 / (1 + rs), 2)
        return result

    def _atr(self, bars: list[dict], period: int = 14) -> list[Optional[float]]:
        """Average True Range"""
        result = [None] * len(bars)
        if len(bars) < period + 1:
            return result
        trs = []
        for i in range(1, len(bars)):
            h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)
        # 第一个ATR = SMA
        if len(trs) >= period:
            atr_val = sum(trs[:period]) / period
            result[period] = atr_val
            for i in range(period + 1, len(bars)):
                atr_val = (atr_val * (period - 1) + trs[i - 1]) / period
                result[i] = atr_val
        return result

    def _highest(self, values: list[float], n: int, idx: int) -> float:
        """过去n个值的最高值"""
        start = max(0, idx - n + 1)
        return max(values[start:idx + 1])

    def _lowest(self, values: list[float], n: int, idx: int) -> float:
        """过去n个值的最低值"""
        start = max(0, idx - n + 1)
        return min(values[start:idx + 1])


# ══════════════════════════════════════════════════════════
# 策略1：趋势跟踪（EMA交叉 + 大趋势过滤 + 追踪止损）
#
# 核心逻辑：
#   入场：EMA10 上穿 EMA30 且 价格 > EMA60（确认大趋势向上）
#   出场：追踪止损（从最高价回撤 trailing_pct）或 EMA10 下穿 EMA30
#
# 为什么有效：
#   - EMA60 过滤掉下跌趋势，避免熊市频繁买入
#   - 追踪止损保护已有利润，不会等到死叉才卖
#   - EMA 比 SMA 对近期价格更敏感，信号更及时
# ══════════════════════════════════════════════════════════
class TrendFollowStrategy(BaseStrategy):
    name = "趋势跟踪"
    requires_pe = False
    requires_market_cap = False

    def __init__(
        self,
        fast:          int   = 10,
        slow:          int   = 30,
        trend:         int   = 60,     # 大趋势EMA周期
        trailing_pct:  float = 0.08,   # 追踪止损比例
    ):
        self.fast = fast
        self.slow = slow
        self.trend = trend
        self.trailing_pct = trailing_pct

    def _signals(self, code: str, bars: list[dict], extra: dict) -> list[Signal]:
        closes = [b["close"] for b in bars]
        dates  = [b["date"] for b in bars]

        ema_fast  = self._ema(closes, self.fast)
        ema_slow  = self._ema(closes, self.slow)
        ema_trend = self._ema(closes, self.trend)

        signals = []
        in_position = False
        highest_since_buy = 0.0

        for i in range(1, len(bars)):
            ef0, ef1 = ema_fast[i - 1], ema_fast[i]
            es0, es1 = ema_slow[i - 1], ema_slow[i]
            et = ema_trend[i]

            if None in (ef0, ef1, es0, es1, et):
                continue

            close = closes[i]

            if in_position:
                highest_since_buy = max(highest_since_buy, close)
                # 追踪止损
                drawdown = (highest_since_buy - close) / highest_since_buy
                if drawdown >= self.trailing_pct:
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"追踪止损 最高{highest_since_buy:.2f}→{close:.2f} 回撤{drawdown:.1%}",
                    ))
                    in_position = False
                    continue
                # 死叉卖出
                if ef0 > es0 and ef1 <= es1:
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"EMA{self.fast}下穿EMA{self.slow} 趋势转弱",
                    ))
                    in_position = False
            else:
                # 入场：快线上穿慢线 + 价格在趋势线上方
                if ef0 <= es0 and ef1 > es1 and close > et:
                    signals.append(Signal(
                        dates[i], code, "BUY", close,
                        f"EMA{self.fast}上穿EMA{self.slow} 价格>{self.trend}日趋势线",
                    ))
                    in_position = True
                    highest_since_buy = close

        return signals


# ══════════════════════════════════════════════════════════
# 策略2：RSI反转（超卖反弹 + 动量确认，快进快出）
#
# 核心逻辑：
#   入场：RSI从<25回升到>30（确认反弹动量）+ 当日收阳线
#   出场：RSI>60止盈 或 固定止损 6%
#
# 修复了旧版的问题：
#   - 去掉了"价格>MA20"的矛盾条件（RSI<30时价格基本在MA20下方）
#   - 改为确认反弹动量（RSI从25以下升到30以上 = 已经开始反转）
#   - 收阳线确认（确保当日是向上走的，不是继续跌）
#   - 止盈目标降到RSI>60（不贪心，快进快出）
# ══════════════════════════════════════════════════════════
class RSIReversalStrategy(BaseStrategy):
    name = "RSI反转"
    requires_pe = False
    requires_market_cap = False

    def __init__(
        self,
        period:        int   = 14,
        entry_low:     float = 25.0,   # RSI曾跌到这个以下
        entry_cross:   float = 30.0,   # RSI回升到这个以上时入场
        take_profit:   float = 60.0,   # RSI达到这个止盈
        stop_loss_pct: float = 0.06,   # 固定止损比例
    ):
        self.period = period
        self.entry_low = entry_low
        self.entry_cross = entry_cross
        self.take_profit = take_profit
        self.stop_loss_pct = stop_loss_pct

    def _signals(self, code: str, bars: list[dict], extra: dict) -> list[Signal]:
        closes = [b["close"] for b in bars]
        opens  = [b["open"] for b in bars]
        dates  = [b["date"] for b in bars]
        rsi    = self._rsi(closes, self.period)

        signals = []
        in_position = False
        buy_price = 0.0
        was_oversold = False  # RSI曾跌到entry_low以下

        for i in range(1, len(bars)):
            r0, r1 = rsi[i - 1], rsi[i]
            if r0 is None or r1 is None:
                continue

            close = closes[i]

            if in_position:
                # 止损
                loss_pct = (close - buy_price) / buy_price
                if loss_pct <= -self.stop_loss_pct:
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"止损 跌幅{loss_pct:.1%}",
                    ))
                    in_position = False
                    buy_price = 0.0
                    was_oversold = False
                    continue

                # 止盈
                if r1 >= self.take_profit:
                    gain = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"RSI={r1:.0f}止盈 收益{gain:.1%}",
                    ))
                    in_position = False
                    buy_price = 0.0
                    was_oversold = False

            else:
                # 标记是否曾超卖
                if r1 <= self.entry_low:
                    was_oversold = True

                # 入场：RSI从超卖区回升 + 收阳线确认
                if was_oversold and r0 < self.entry_cross and r1 >= self.entry_cross:
                    is_bullish = close > opens[i]  # 收阳
                    if is_bullish:
                        signals.append(Signal(
                            dates[i], code, "BUY", close,
                            f"RSI超卖反弹 {r0:.0f}→{r1:.0f} 收阳确认",
                        ))
                        in_position = True
                        buy_price = close
                        was_oversold = False

        return signals


# ══════════════════════════════════════════════════════════
# 策略3：布林带回归（触碰下轨买入 + 中轨/上轨止盈 + 止损）
#
# 核心逻辑：
#   入场：价格触碰布林带下轨 + 收阳线（确认反弹）
#   出场：价格回到中轨止盈 或 固定止损
#
# 为什么有效：
#   - 布林带下轨代表统计学意义上的超卖（2σ以外）
#   - 收阳线确认反弹已开始，不是接飞刀
#   - 目标是中轨（均值回归），胜率高，适合震荡市
#   - 与RSI策略互补：RSI看动量，布林带看价格偏离度
# ══════════════════════════════════════════════════════════
class BollingerRevertStrategy(BaseStrategy):
    name = "布林带回归"
    requires_pe = False
    requires_market_cap = False

    def __init__(
        self,
        period:        int   = 20,     # 布林带周期
        num_std:       float = 2.0,    # 标准差倍数
        stop_loss_pct: float = 0.05,   # 止损比例
        take_profit:   str   = "mid",  # "mid"=回到中轨止盈, "upper"=到上轨
    ):
        self.period = period
        self.num_std = num_std
        self.stop_loss_pct = stop_loss_pct
        self.take_profit = take_profit

    def _bollinger(self, closes: list[float]) -> tuple[list, list, list]:
        """返回 (upper, mid, lower)"""
        n = len(closes)
        upper = [None] * n
        mid = [None] * n
        lower = [None] * n
        for i in range(self.period - 1, n):
            window = closes[i - self.period + 1:i + 1]
            m = sum(window) / self.period
            std = (sum((x - m) ** 2 for x in window) / self.period) ** 0.5
            mid[i] = m
            upper[i] = m + self.num_std * std
            lower[i] = m - self.num_std * std
        return upper, mid, lower

    def _signals(self, code: str, bars: list[dict], extra: dict) -> list[Signal]:
        if len(bars) < self.period + 1:
            return []

        closes = [b["close"] for b in bars]
        opens  = [b["open"] for b in bars]
        dates  = [b["date"] for b in bars]

        upper, mid, lower = self._bollinger(closes)

        signals = []
        in_position = False
        buy_price = 0.0

        for i in range(self.period, len(bars)):
            close = closes[i]
            lo = lower[i]
            mi = mid[i]
            up = upper[i]

            if None in (lo, mi, up):
                continue

            if in_position:
                # 止损
                loss_pct = (close - buy_price) / buy_price
                if loss_pct <= -self.stop_loss_pct:
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"止损 跌幅{loss_pct:.1%}",
                    ))
                    in_position = False
                    buy_price = 0.0
                    continue

                # 止盈：回到中轨或上轨
                target = mi if self.take_profit == "mid" else up
                if close >= target:
                    gain = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"回归{'中轨' if self.take_profit == 'mid' else '上轨'} 收益{gain:.1%}",
                    ))
                    in_position = False
                    buy_price = 0.0

            else:
                # 入场：触碰下轨 + 收阳线
                if close <= lo:
                    is_bullish = close > opens[i]
                    if is_bullish:
                        signals.append(Signal(
                            dates[i], code, "BUY", close,
                            f"触碰布林下轨{lo:.2f} 收阳确认",
                        ))
                        in_position = True
                        buy_price = close

        return signals


# ══════════════════════════════════════════════════════════
# 策略4：主力拉升（量价共振 + MACD + RSI，捕捉主力资金驱动行情）
#
# 核心逻辑（来源：神剑股份/赛微电子/广西能源三股实证分析）：
#   入场：当日阳线 + 涨幅>3% + 量比>1.5x + 价格>MA20
#         + RSI在50~70强势区（未超买）+ MACD DIF>0（多头格局）
#   出场：
#     止盈1 — RSI>85 且上影线占比>30%（主力出货信号）
#     止盈2 — 追踪止损（从最高价回撤 trailing_pct）
#     止损1 — 跌破MA5 且收阴线（趋势转弱）
#     止损2 — 量比>2x 但收阴线（放量滞涨/主力出货）
#
# 为什么有效：
#   - 三只实证股票区间涨幅均值146%，核心信号为量价放大+MACD金叉
#   - RSI维持50~70是主力控盘拉升的典型特征（不让RSI过热以维持吸引力）
#   - RSI>85+长上影线是主力开始出货的量化信号
#   - 跌破MA5收阴 = 主力撤退，及时止损保护利润
# ══════════════════════════════════════════════════════════
class MajorCapitalPumpStrategy(BaseStrategy):
    name = "主力拉升"
    requires_pe = False
    requires_market_cap = False

    def __init__(
        self,
        pct_entry:         float = 3.0,    # 入场最低涨幅(%)
        vol_ratio_entry:   float = 1.5,    # 入场量比阈值
        vol_ratio_exit:    float = 2.0,    # 放量阴线止损的量比阈值
        rsi_min:           float = 50.0,   # 入场RSI下限（强势区起点）
        rsi_max:           float = 70.0,   # 入场RSI上限（避免追高）
        rsi_exit:          float = 85.0,   # 止盈RSI阈值（主力出货区）
        ma_fast:           int   = 5,      # 短期均线（止损参考）
        ma_slow:           int   = 20,     # 中期均线（主升浪过滤）
        macd_fast:         int   = 12,
        macd_slow:         int   = 26,
        macd_signal:       int   = 9,
        upper_shadow_exit: float = 0.30,   # 上影线占比止盈阈值
        trailing_pct:      float = 0.10,   # 追踪止损回撤比例
    ):
        self.pct_entry = pct_entry
        self.vol_ratio_entry = vol_ratio_entry
        self.vol_ratio_exit = vol_ratio_exit
        self.rsi_min = rsi_min
        self.rsi_max = rsi_max
        self.rsi_exit = rsi_exit
        self.ma_fast = ma_fast
        self.ma_slow = ma_slow
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.upper_shadow_exit = upper_shadow_exit
        self.trailing_pct = trailing_pct

    # ── 指标计算 ──────────────────────────────────────────

    def _vol_ratio(self, volumes: list[float], i: int, period: int = 5) -> Optional[float]:
        """量比：当日量 / 过去N日均量"""
        if i < period:
            return None
        avg = sum(volumes[i - period:i]) / period
        return volumes[i] / avg if avg > 1e-9 else None

    def _macd(
        self, closes: list[float]
    ) -> tuple[list[Optional[float]], list[Optional[float]]]:
        """返回 (DIF, DEA)"""
        ema_fast = self._ema(closes, self.macd_fast)
        ema_slow = self._ema(closes, self.macd_slow)
        n = len(closes)
        dif = [None] * n
        for i in range(n):
            if ema_fast[i] is not None and ema_slow[i] is not None:
                dif[i] = ema_fast[i] - ema_slow[i]

        dea = [None] * n
        valid_start = next((i for i, v in enumerate(dif) if v is not None), None)
        if valid_start is None:
            return dif, dea

        k = 2 / (self.macd_signal + 1)
        dea[valid_start] = dif[valid_start]
        for i in range(valid_start + 1, n):
            if dif[i] is not None and dea[i - 1] is not None:
                dea[i] = dif[i] * k + dea[i - 1] * (1 - k)
        return dif, dea

    def _upper_shadow_ratio(self, bar: dict) -> float:
        """上影线占全范围比例"""
        full = bar["high"] - bar["low"]
        if full < 1e-9:
            return 0.0
        top = max(bar["close"], bar["open"])
        return (bar["high"] - top) / full

    # ── 信号生成 ──────────────────────────────────────────

    def _signals(self, code: str, bars: list[dict], extra: dict) -> list[Signal]:
        if len(bars) < self.macd_slow + self.macd_signal + 5:
            return []

        closes  = [b["close"] for b in bars]
        opens   = [b["open"]  for b in bars]
        volumes = [b.get("volume", 0) for b in bars]
        dates   = [b["date"]  for b in bars]

        ma_fast_arr = self._sma(closes, self.ma_fast)
        ma_slow_arr = self._sma(closes, self.ma_slow)
        rsi_arr     = self._rsi(closes, 14)
        dif_arr, dea_arr = self._macd(closes)

        signals = []
        in_position = False
        buy_price = 0.0
        highest_since_buy = 0.0

        for i in range(self.macd_slow + self.macd_signal, len(bars)):
            close = closes[i]
            open_ = opens[i]
            maf   = ma_fast_arr[i]
            mas   = ma_slow_arr[i]
            rsi   = rsi_arr[i]
            dif   = dif_arr[i]

            if None in (maf, mas, rsi, dif):
                continue

            is_bull = close > open_

            if in_position:
                highest_since_buy = max(highest_since_buy, close)

                # ── 止盈1：RSI过热 + 长上影线（主力出货信号）
                upper_shadow = self._upper_shadow_ratio(bars[i])
                if rsi >= self.rsi_exit and upper_shadow >= self.upper_shadow_exit:
                    gain = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"主力出货信号: RSI={rsi:.0f}≥{self.rsi_exit} "
                        f"上影线{upper_shadow:.0%}≥{self.upper_shadow_exit:.0%} "
                        f"收益{gain:+.1%}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0
                    continue

                # ── 止盈2：追踪止损
                drawdown = (highest_since_buy - close) / highest_since_buy
                if drawdown >= self.trailing_pct:
                    gain = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"追踪止损: 峰值{highest_since_buy:.2f}→{close:.2f} "
                        f"回撤{drawdown:.1%} 收益{gain:+.1%}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0
                    continue

                # ── 止损1：跌破MA5 且收阴线
                if not is_bull and close < maf:
                    loss = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"跌破MA{self.ma_fast}收阴: {close:.2f}<{maf:.2f} "
                        f"收益{loss:+.1%}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0
                    continue

                # ── 止损2：放量阴线（主力出货）
                vr = self._vol_ratio(volumes, i)
                if vr is not None and vr >= self.vol_ratio_exit and not is_bull:
                    loss = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"放量收阴(量比{vr:.1f}x)主力出货嫌疑 收益{loss:+.1%}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0

            else:
                # ── 入场条件（全部满足）
                if i < 1:
                    continue
                pct_chg = (close / closes[i - 1] - 1) * 100
                vr = self._vol_ratio(volumes, i)
                if vr is None:
                    continue

                cond_bull    = is_bull                          # 收阳线
                cond_pct     = pct_chg >= self.pct_entry        # 涨幅≥3%
                cond_vol     = vr >= self.vol_ratio_entry       # 量比≥1.5x
                cond_ma      = close > mas                      # 价格>MA20（主升浪）
                cond_rsi     = self.rsi_min <= rsi <= self.rsi_max  # RSI在强势区
                cond_macd    = dif >= 0                         # MACD多头格局

                if cond_bull and cond_pct and cond_vol and cond_ma and cond_rsi and cond_macd:
                    signals.append(Signal(
                        dates[i], code, "BUY", close,
                        f"主力拉升入场: +{pct_chg:.1f}% 量比{vr:.1f}x "
                        f"RSI={rsi:.0f} 价格>{self.ma_slow}MA MACD多头",
                        confidence=min(1.0, vr / 3.0),
                    ))
                    in_position = True
                    buy_price = close
                    highest_since_buy = close

        return signals


# ══════════════════════════════════════════════════════════
# 策略5：主力建仓（两阶段：发现建仓 → 建仓完毕即将拉升）
# ══════════════════════════════════════════════════════════

class MajorCapitalAccumulationStrategy(BaseStrategy):
    """
    主力低位建仓策略 — 两阶段信号
    ──────────────────────────────────────────────────────────
    基于神剑股份/赛微电子/广西能源等实证研究。

    ★ 阶段1 — WATCH（发现建仓迹象，加入观察）
      检测条件：底部区域 + 均线粘合 + 阳量>阴量 + RSI低位
      推送："发现建仓迹象，加入观察名单"
      不入场，只是标记为正在被主力吸筹。

    ★ 阶段2 — BUY（建仓完毕临界点，可以入场）
      前提：必须已经有过 WATCH 信号（确认经历过充分吸筹）
      临界信号（建仓完毕的标志）：
        A. 长期横盘后放量突破：涨幅≥4% + 量比≥1.8（突破平台）
        B. 价格突破布林上轨：从收窄状态爆发突破
        C. 均线多头发散：MA5>MA10>MA20 且 MA20 斜率转正向上
      推送："建仓完毕即将拉升，可以入场！"

    出场条件：
      - RSI > 80（超买）
      - 追踪止损 15%（拉升期给更大空间）
      - 跌破 MA20 连续 5 日
    """
    name = "主力建仓"
    requires_pe = False
    requires_market_cap = False

    def __init__(
        self,
        # ── 阶段1（WATCH）参数 ──
        low_lookback:       int   = 60,
        max_above_low_pct:  float = 20.0,
        ma_converge_pct:    float = 5.0,
        ma_slope_max:       float = 0.05,
        bb_period:          int   = 20,
        bb_narrow_ratio:    float = 0.85,
        vol_yang_yin_min:   float = 1.03,
        vol_lookback:       int   = 30,
        rsi_watch_min:      float = 25.0,
        rsi_watch_max:      float = 62.0,
        # ── 阶段2（BUY 临界信号）参数 ──
        min_watch_days:     int   = 15,     # WATCH 后至少观察N天才能触发 BUY
        breakout_pct:       float = 4.0,    # 突破涨幅阈值（比建仓期更高）
        breakout_vol_ratio: float = 1.8,    # 突破量比（明显放量）
        ma_slope_up_min:    float = 0.002,  # MA20 斜率 > 此值 = 开始向上
        # ── MACD ──
        macd_fast:          int   = 12,
        macd_slow:          int   = 26,
        macd_signal:        int   = 9,
        # ── 出场 ──
        rsi_exit:           float = 80.0,   # RSI 超买阈值
        rsi_exit_drop:      float = 8.0,    # RSI 需从超买下落此值才触发出场（避免涨停板误触）
        trailing_pct:       float = 0.15,   # 追踪止损回撤比例（拉升期给更大空间）
        ma_exit_days:       int   = 5,      # 连续5日低于MA20才止损
        ma_exit_grace:      int   = 10,     # BUY后N日内不启用MA20止损（突破回踩缓冲）
        stop_loss_pct:      float = 0.12,   # 硬止损比例（从买入价计算）
        # ── 放量冲高回落出货检测（两步确认） ──
        dist_vol_ratio:     float = 1.5,    # 出货量比阈值（当日量/5日均量）
        dist_shadow_pct:    float = 0.35,   # 上影线占K线总幅度的比例阈值
        dist_min_gain:      float = 0.10,   # 只有已盈利≥此比例时才启用出货检测
        dist_confirm_ma:    int   = 10,     # 冲高回落后跌破此均线才确认出场（MA10）
    ):
        self.low_lookback = low_lookback
        self.max_above_low_pct = max_above_low_pct
        self.ma_converge_pct = ma_converge_pct
        self.ma_slope_max = ma_slope_max
        self.bb_period = bb_period
        self.bb_narrow_ratio = bb_narrow_ratio
        self.vol_yang_yin_min = vol_yang_yin_min
        self.vol_lookback = vol_lookback
        self.rsi_watch_min = rsi_watch_min
        self.rsi_watch_max = rsi_watch_max
        self.min_watch_days = min_watch_days
        self.breakout_pct = breakout_pct
        self.breakout_vol_ratio = breakout_vol_ratio
        self.ma_slope_up_min = ma_slope_up_min
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.rsi_exit = rsi_exit
        self.rsi_exit_drop = rsi_exit_drop
        self.trailing_pct = trailing_pct
        self.ma_exit_days = ma_exit_days
        self.ma_exit_grace = ma_exit_grace
        self.stop_loss_pct = stop_loss_pct
        self.dist_vol_ratio = dist_vol_ratio
        self.dist_shadow_pct = dist_shadow_pct
        self.dist_min_gain = dist_min_gain
        self.dist_confirm_ma = dist_confirm_ma

    # ── 辅助指标 ──────────────────────────────────────────

    def _macd(self, closes):
        ema_fast = self._ema(closes, self.macd_fast)
        ema_slow = self._ema(closes, self.macd_slow)
        n = len(closes)
        dif = [None] * n
        for i in range(n):
            if ema_fast[i] is not None and ema_slow[i] is not None:
                dif[i] = ema_fast[i] - ema_slow[i]
        dea = [None] * n
        start = next((i for i, v in enumerate(dif) if v is not None), None)
        if start is None:
            return dif, dea
        k = 2 / (self.macd_signal + 1)
        dea[start] = dif[start]
        for i in range(start + 1, n):
            if dif[i] is not None and dea[i - 1] is not None:
                dea[i] = dif[i] * k + dea[i - 1] * (1 - k)
        return dif, dea

    def _bollinger(self, closes, period=20):
        n = len(closes)
        upper = [None] * n
        mid = [None] * n
        lower = [None] * n
        bw = [None] * n
        for i in range(period - 1, n):
            seg = closes[i - period + 1: i + 1]
            m = sum(seg) / period
            std = (sum((x - m) ** 2 for x in seg) / period) ** 0.5
            mid[i] = m
            upper[i] = m + 2 * std
            lower[i] = m - 2 * std
            bw[i] = (4 * std / m * 100) if m > 0 else 0.0
        return upper, mid, lower, bw

    def _vol_ratio(self, volumes, i, period=5):
        if i < period:
            return None
        avg = sum(volumes[i - period:i]) / period
        return volumes[i] / avg if avg > 1e-9 else None

    def _yang_yin_vol_ratio(self, closes, opens, volumes, i, lookback=30):
        start = max(0, i - lookback + 1)
        yang_vols = [volumes[j] for j in range(start, i + 1) if closes[j] >= opens[j]]
        yin_vols  = [volumes[j] for j in range(start, i + 1) if closes[j] < opens[j]]
        if not yang_vols or not yin_vols:
            return None
        return (sum(yang_vols) / len(yang_vols)) / (sum(yin_vols) / len(yin_vols))

    def _ma_convergence(self, ma5, ma10, ma20, i):
        vals = [v for v in (ma5[i], ma10[i], ma20[i]) if v is not None]
        if len(vals) < 3:
            return None
        mid_val = sorted(vals)[1]
        if mid_val <= 0:
            return None
        return (max(vals) - min(vals)) / mid_val * 100

    def _ma_slope(self, ma_arr, i, days=5):
        if i < days or ma_arr[i] is None or ma_arr[i - days] is None:
            return None
        base = ma_arr[i - days]
        if base <= 0:
            return None
        return (ma_arr[i] - base) / base / days

    def _near_low(self, closes, i, lookback=60):
        start = max(0, i - lookback + 1)
        low = min(closes[start:i + 1])
        if low <= 0:
            return None
        return (closes[i] - low) / low * 100

    def _is_bb_narrow(self, bb_bw, i):
        """判断布林带是否处于收窄状态"""
        if bb_bw[i] is None:
            return False
        recent = [bb_bw[j] for j in range(max(0, i - 30), i) if bb_bw[j] is not None]
        if not recent:
            return False
        return bb_bw[i] < (sum(recent) / len(recent)) * self.bb_narrow_ratio

    # ── 核心信号（两阶段） ────────────────────────────────

    def _signals(self, code: str, bars: list[dict], extra: dict) -> list[Signal]:
        min_len = max(self.low_lookback, self.macd_slow + self.macd_signal + 5)
        if len(bars) < min_len:
            return []

        closes  = [b["close"]  for b in bars]
        opens   = [b["open"]   for b in bars]
        highs   = [b["high"]   for b in bars]
        lows    = [b["low"]    for b in bars]
        volumes = [b.get("volume", 0) for b in bars]
        dates   = [b["date"]   for b in bars]

        ma5  = self._sma(closes, 5)
        ma10 = self._sma(closes, 10)
        ma20 = self._sma(closes, 20)
        ma60 = self._sma(closes, 60)
        rsi_arr = self._rsi(closes, 14)
        dif_arr, dea_arr = self._macd(closes)
        bb_upper, bb_mid, bb_lower, bb_bw = self._bollinger(closes, self.bb_period)

        signals = []
        in_position = False
        buy_price = 0.0
        highest_since_buy = 0.0
        days_below_ma = 0
        days_since_buy = 0           # 持仓天数（用于缓冲期判断）
        rsi_peaked = False           # 是否已进入超买区（等待回落出场）
        dist_warned = False          # 放量冲高回落预警标记
        dist_warn_info = ""          # 预警时的描述信息
        dist_warn_high = 0.0         # 预警时的最高收盘价（用于判断复位）

        # 根据 dist_confirm_ma 选择对应均线
        dist_ma = self._sma(closes, self.dist_confirm_ma)

        # ★ 两阶段核心状态
        watch_start_idx: Optional[int] = None   # WATCH 阶段起始 bar 索引
        accumulation_days = 0                     # 满足建仓条件的累计天数

        for i in range(min_len, len(bars)):
            close = closes[i]
            open_ = opens[i]
            rsi   = rsi_arr[i]
            dif   = dif_arr[i]
            dea   = dea_arr[i]

            if None in (rsi, dif, dea, ma20[i], bb_mid[i]):
                continue

            is_bull = close >= open_

            # ════════════════════════════════════════════
            # 已持仓 → 检查出场
            # ════════════════════════════════════════════
            if in_position:
                highest_since_buy = max(highest_since_buy, close)
                days_since_buy += 1

                # 判断当日是否涨停（≥9.5%），涨停日不触发任何主观止盈
                daily_pct = (close - closes[i - 1]) / closes[i - 1] * 100 if i > 0 else 0
                is_limit_up = daily_pct >= 9.5

                # 止盈1：RSI 超买回落出场
                # ★ 改为"峰后回落"机制：RSI 首次达到超买时进入观察，
                #   等 RSI 从超买高点回落 rsi_exit_drop 点才真正出场。
                #   涨停板当日 RSI 飙升属于动量最强信号，不触发出场。
                if rsi >= self.rsi_exit and not is_limit_up:
                    rsi_peaked = True      # 标记已进入超买区，等待回落
                if rsi_peaked and rsi < self.rsi_exit - self.rsi_exit_drop and not is_limit_up:
                    gain = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"超买回落止盈: RSI峰后回落至{rsi:.0f} 收益{gain:+.1%}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0
                    days_below_ma = days_since_buy = 0
                    rsi_peaked = False
                    dist_warned = False; dist_warn_info = ""; dist_warn_high = 0.0
                    continue

                # 止盈2：放量冲高回落 + 跌破短期均线（两步确认）
                # ★ 第一步：检测冲高回落 → 设预警（不立即卖出）
                # ★ 第二步：预警后收盘跌破 MA10 → 确认出货，卖出
                # ★ 复位：预警后如果收盘再创新高 → 取消预警（洗盘非出货）
                cur_gain = (close - buy_price) / buy_price
                if cur_gain >= self.dist_min_gain:
                    h_i, l_i = highs[i], lows[i]
                    k_range = h_i - l_i
                    if k_range > 0 and h_i >= highest_since_buy:
                        body_top = max(open_, close)
                        upper_shadow_ratio = (h_i - body_top) / k_range
                        vr_i = self._vol_ratio(volumes, i, 5)
                        if (upper_shadow_ratio >= self.dist_shadow_pct
                                and vr_i is not None
                                and vr_i >= self.dist_vol_ratio):
                            # 标记/更新预警（每次检测到都刷新信息）
                            dist_warned = True
                            dist_warn_high = highest_since_buy  # 记录预警时最高点
                            dist_warn_info = (
                                f"放量冲高回落: 上影{upper_shadow_ratio:.0%} "
                                f"量比{vr_i:.1f}x "
                                f"高{h_i:.2f}→收{close:.2f}"
                            )

                # ★ 预警后的确认/复位逻辑
                if dist_warned:
                    # 复位：收盘突破预警时高点 → 洗盘结束，取消预警
                    if close > dist_warn_high:
                        dist_warned = False
                        dist_warn_info = ""
                        dist_warn_high = 0.0
                    # 确认出场：跌破短期均线（MA10）
                    elif dist_ma[i] is not None and close < dist_ma[i]:
                        gain = (close - buy_price) / buy_price
                        signals.append(Signal(
                            dates[i], code, "SELL", close,
                            f"{dist_warn_info} → 跌破MA{self.dist_confirm_ma} "
                            f"确认出货 收益{gain:+.1%}",
                        ))
                        in_position = False
                        buy_price = highest_since_buy = 0.0
                        days_below_ma = days_since_buy = 0
                        rsi_peaked = False
                        dist_warned = False
                        dist_warn_info = ""
                        dist_warn_high = 0.0
                        continue

                # 止损3：硬止损（无缓冲期，任何时候生效）
                loss = (close - buy_price) / buy_price
                if loss <= -self.stop_loss_pct:
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"硬止损: 亏损{loss:.1%} 买入价{buy_price:.2f}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0
                    days_below_ma = days_since_buy = 0
                    rsi_peaked = False
                    dist_warned = False; dist_warn_info = ""; dist_warn_high = 0.0
                    continue

                # 止盈3：追踪止损（需要先盈利过才有意义）
                drawdown = (highest_since_buy - close) / highest_since_buy
                if drawdown >= self.trailing_pct and highest_since_buy > buy_price:
                    gain = (close - buy_price) / buy_price
                    signals.append(Signal(
                        dates[i], code, "SELL", close,
                        f"追踪止损: 峰{highest_since_buy:.2f}→{close:.2f} "
                        f"回撤{drawdown:.1%} 收益{gain:+.1%}",
                    ))
                    in_position = False
                    buy_price = highest_since_buy = 0.0
                    days_below_ma = days_since_buy = 0
                    rsi_peaked = False
                    dist_warned = False; dist_warn_info = ""; dist_warn_high = 0.0
                    continue

                # 止损4：跌破 MA20 连续 N 日（缓冲期内不触发）
                if days_since_buy > self.ma_exit_grace:
                    if close < ma20[i]:
                        days_below_ma += 1
                        if days_below_ma >= self.ma_exit_days:
                            gain = (close - buy_price) / buy_price
                            signals.append(Signal(
                                dates[i], code, "SELL", close,
                                f"破位止损: 连续{days_below_ma}日<MA20 收益{gain:+.1%}",
                            ))
                            in_position = False
                            buy_price = highest_since_buy = 0.0
                            days_below_ma = days_since_buy = 0
                            rsi_peaked = False
                            dist_warned = False; dist_warn_info = ""; dist_warn_high = 0.0
                            continue
                    else:
                        days_below_ma = 0
                else:
                    # 缓冲期内只重置计数，不触发
                    if close >= ma20[i]:
                        days_below_ma = 0
                continue

            # ════════════════════════════════════════════
            # 未持仓 → 两阶段检测
            # ════════════════════════════════════════════

            # ── 检测当前是否满足"建仓中"条件 ──
            near_low = self._near_low(closes, i, self.low_lookback)
            conv = self._ma_convergence(ma5, ma10, ma20, i)
            slope = self._ma_slope(ma20, i, 5)
            yy_ratio = self._yang_yin_vol_ratio(closes, opens, volumes, i, self.vol_lookback)

            is_accumulating = (
                near_low is not None and near_low <= self.max_above_low_pct
                and conv is not None and conv <= self.ma_converge_pct
                and (slope is None or abs(slope) <= self.ma_slope_max)
                and self.rsi_watch_min <= rsi <= self.rsi_watch_max
                and yy_ratio is not None and yy_ratio >= self.vol_yang_yin_min
            )

            # ── 阶段1：发现建仓迹象 → WATCH ──
            if is_accumulating:
                accumulation_days += 1

                if watch_start_idx is None:
                    # 首次发现建仓迹象
                    watch_start_idx = i
                    bb_narrow = self._is_bb_narrow(bb_bw, i)
                    signals.append(Signal(
                        dates[i], code, "WATCH", close,
                        f"发现建仓迹象: 距底{near_low:.0f}% 均线粘合{conv:.1f}% "
                        f"阳阴量比{yy_ratio:.2f} RSI={rsi:.0f} "
                        f"{'布林收窄' if bb_narrow else ''}",
                        confidence=0.3,
                    ))
            else:
                # 不再满足建仓条件：如果已经累计了足够天数，保留状态等待突破
                # 如果累计天数不够，重置
                if accumulation_days < self.min_watch_days:
                    watch_start_idx = None
                    accumulation_days = 0

            # ── 阶段2：建仓完毕临界信号 → BUY ──
            # 前提：已观察到足够长的建仓期
            if watch_start_idx is None or accumulation_days < self.min_watch_days:
                continue

            watched_days = i - watch_start_idx
            trigger = ""
            trigger_strength = 0

            # 信号A：长期横盘后放量大阳线突破
            pct_chg = (close - closes[i - 1]) / closes[i - 1] * 100 if i > 0 else 0
            vr = self._vol_ratio(volumes, i, 5)
            if (is_bull and pct_chg >= self.breakout_pct
                    and vr is not None and vr >= self.breakout_vol_ratio):
                trigger = f"放量突破+{pct_chg:.1f}% 量比{vr:.1f}x"
                trigger_strength = 3

            # 信号B：价格突破布林上轨（从收窄状态爆发）
            if not trigger and bb_upper[i] is not None:
                was_narrow = any(self._is_bb_narrow(bb_bw, j)
                                 for j in range(max(min_len, i - 10), i))
                if close > bb_upper[i] and was_narrow:
                    trigger = f"突破布林上轨{bb_upper[i]:.2f}"
                    trigger_strength = 3

            # 信号C：均线多头发散 + MA20 斜率转正
            if not trigger:
                ma20_slope = self._ma_slope(ma20, i, 5)
                if (ma5[i] is not None and ma10[i] is not None and ma20[i] is not None
                        and ma5[i] > ma10[i] > ma20[i]
                        and ma20_slope is not None
                        and ma20_slope >= self.ma_slope_up_min):
                    # 确认 MA20 斜率从平走转为向上
                    prev_slope = self._ma_slope(ma20, i - 5, 5) if i >= 10 else None
                    if prev_slope is not None and prev_slope < self.ma_slope_up_min:
                        trigger = f"均线多头发散 MA20↑{ma20_slope:.4f}"
                        trigger_strength = 2

            # 信号D：MACD零轴上方金叉（建仓完毕后的金叉更有意义）
            if not trigger:
                prev_dif = dif_arr[i - 1] if i > 0 else None
                prev_dea = dea_arr[i - 1] if i > 0 else None
                if (prev_dif is not None and prev_dea is not None
                        and prev_dif <= prev_dea and dif > dea
                        and dif >= 0):  # 零轴上方金叉
                    trigger = "MACD零轴上方金叉"
                    trigger_strength = 2

            if not trigger:
                continue

            # ── 计算信心 ─────────────────────────────
            conf = 0.3 + trigger_strength * 0.1
            if accumulation_days >= 30:
                conf += 0.15   # 建仓期越长，信号越可靠
            elif accumulation_days >= 20:
                conf += 0.10
            if yy_ratio and yy_ratio >= 1.10:
                conf += 0.10
            bb_narrow = self._is_bb_narrow(bb_bw, i) or any(
                self._is_bb_narrow(bb_bw, j) for j in range(max(min_len, i - 10), i))
            if bb_narrow:
                conf += 0.10
            conf = min(conf, 1.0)

            signals.append(Signal(
                dates[i], code, "BUY", close,
                f"建仓完毕即将拉升: {trigger} | "
                f"已建仓{accumulation_days}天 观察{watched_days}天 "
                f"阳阴量比{yy_ratio:.2f} RSI={rsi:.0f}",
                confidence=conf,
            ))
            in_position = True
            buy_price = close
            highest_since_buy = close
            days_below_ma = days_since_buy = 0
            rsi_peaked = False
            # 重置观察状态
            watch_start_idx = None
            accumulation_days = 0

        return signals


# 注册表
BUILTIN_STRATEGIES = {
    "trend_follow":                TrendFollowStrategy,
    "rsi_reversal":                RSIReversalStrategy,
    "bollinger_revert":            BollingerRevertStrategy,
    "major_capital_pump":          MajorCapitalPumpStrategy,
    "major_capital_accumulation":  MajorCapitalAccumulationStrategy,
}
