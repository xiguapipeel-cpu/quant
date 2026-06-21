"""
主力建仓策略 — 公共信号核(单一信号源)
═══════════════════════════════════════════════════════════════════════
回测引擎 `bt_major_capital.py` 与实时扫描 `strategies.py` 共用此模块的
WATCH→BUY 判定逻辑，杜绝两套实现漂移(2026-06 曾因脱节导致线上跑的是
P0 之前的老策略，所有 P 系列调参从未部署)。

设计：数组化纯函数。`evaluate_bar` 接收开高低收量 + 已算好的指标值
(rsi/atr/ma/bb/macd) + 参数 + 可变状态，返回该 bar 的 WATCH/BUY 判定。

调用方各自负责(不进公共核，因两引擎差异大)：
  - 大盘过滤 / trade_start 预热 / 资金流二次确认(需 DB/index feed)
  - bb_bw_history 每根 bar 的 append(在调用 evaluate_bar 前)
  - 持仓退出时对 watch_history/accumulation_days/watch_signal_dates 的重置

索引约定：数组为完整历史，i 为当前 bar 下标(=最后一根)。
back(arr,i,j)=arr[i-j]，对应 backtrader 的 d.x[-j]；nb=i+1 对应 len(d)。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


# ════════════════════════════════════════════════════════════════════
# 参数(P 系列固化默认值，与 bt_major_capital.py params 对齐 — 单一来源)
# ════════════════════════════════════════════════════════════════════
@dataclass
class MajorCapitalParams:
    # ── 阶段1 WATCH ──
    low_lookback: int = 60
    max_above_low_pct: float = 15.0
    high_lookback: int = 120
    min_below_high_pct: float = 30.0
    ma_converge_pct: float = 3.0
    ma_slope_max: float = 0.01
    vol_yang_yin_min: float = 1.03
    vol_lookback: int = 30
    rsi_watch_min: float = 25.0
    rsi_watch_max: float = 62.0
    vol_compression_max: float = 0.70
    vol_compression_short: int = 20    # bt 默认 20/120(非 5/20)
    vol_compression_long: int = 120
    watch_rolling_window: int = 30
    min_watch_days: int = 15
    # ── 阶段2 BUY ──
    enable_signal_a: bool = True
    enable_signal_f: bool = False          # P1-B：关闭(累计 -20pp)
    breakout_max_pct: float = 8.0
    rsi_buy_max: float = 70.0
    trend_filter: bool = True              # MA20 > MA60
    signal_a_min_rsi: float = 55.0         # P2-A
    signal_a_break_high_lookback: int = 30  # P4：close > max(high[-1..-N])
    # 自适应阈值(旧 percentile 默认在用；P5 ATR/原始量分位 >0 时启用)
    breakout_atr_k: float = 0.0
    vol_raw_percentile: float = 0.0
    atr_period: int = 20
    adaptive_lookback: int = 60
    vol_ratio_percentile: float = 0.85
    vol_ratio_min: float = 2.0
    breakout_pct_percentile: float = 0.75
    breakout_pct_min: float = 3.0
    # ── 量缩放大(信号F) / BB narrow ──
    vol_shrink_days: int = 15
    vol_expand_days: int = 5
    vol_shrink_max_ratio: float = 0.80
    vol_expand_min_ratio: float = 1.2
    vol_expand_max_ratio: float = 2.5
    bb_narrow_ratio: float = 0.85
    ma_diverge_lookback: int = 5


def new_state() -> dict:
    """WATCH 状态机 + BB 历史(公共核读写的字段子集)。"""
    return {
        'watch_start': None,
        'accumulation_days': 0,
        'watch_history': [],        # 滚动窗口布尔队列
        'watch_signal_dates': [],   # 满足建仓条件的日期(最多60)
        'bb_bw_history': [],        # 由调用方每根 bar append
    }


# ════════════════════════════════════════════════════════════════════
# 小工具
# ════════════════════════════════════════════════════════════════════
def _isnan(x) -> bool:
    return x is None or x != x


def _b(arr, i, j):
    """back: arr[i-j]，对应 backtrader d.x[-j]。越界返回 nan。"""
    k = i - j
    if k < 0 or k >= len(arr):
        return float('nan')
    return float(arr[k])


# ── helper：全部复刻 bt_major_capital 的同名方法(数组化) ──
def _vol_ratio(vol, i, period=5):
    nb = i + 1
    if nb < period + 1:
        return None
    avg = sum(_b(vol, i, j) for j in range(1, period + 1)) / period
    return _b(vol, i, 0) / avg if avg > 1e-9 else None


def _volume_compression(vol, i, short_n, long_n):
    nb = i + 1
    if nb < short_n + 1:
        return None
    if nb < long_n + 1:
        long_n = min(nb - 1, short_n * 2)
        if long_n <= short_n:
            return None
    short_avg = sum(_b(vol, i, j) for j in range(1, short_n + 1)) / short_n
    long_avg = sum(_b(vol, i, j) for j in range(1, long_n + 1)) / long_n
    if long_avg <= 1e-9:
        return None
    return short_avg / long_avg


def _adaptive_vol_threshold(vol, i, p):
    lb = p.adaptive_lookback
    if (i + 1) < lb + 6:
        return p.vol_ratio_min
    samples = []
    for j in range(1, lb + 1):
        avg5 = sum(_b(vol, i, j + k) for k in range(1, 6)) / 5.0
        if avg5 > 0:
            samples.append(_b(vol, i, j) / avg5)
    if len(samples) < 20:
        return p.vol_ratio_min
    samples.sort()
    idx = min(int(p.vol_ratio_percentile * len(samples)), len(samples) - 1)
    return max(samples[idx], p.vol_ratio_min)


def _adaptive_breakout_pct(close, i, p):
    lb = p.adaptive_lookback
    if (i + 1) < lb + 2:
        return p.breakout_pct_min
    samples = []
    for j in range(1, lb + 1):
        prev = _b(close, i, j + 1)
        cur = _b(close, i, j)
        if prev > 0:
            samples.append((cur - prev) / prev * 100)
    if len(samples) < 20:
        return p.breakout_pct_min
    samples.sort()
    idx = min(int(p.breakout_pct_percentile * len(samples)), len(samples) - 1)
    return max(samples[idx], p.breakout_pct_min)


def _vol_raw_threshold(vol, i, p):
    if p.vol_raw_percentile <= 0:
        return None
    lb = p.adaptive_lookback
    if (i + 1) < lb + 1:
        return None
    vols = []
    for j in range(1, lb + 1):
        v = _b(vol, i, j)
        if v > 0 and v == v:
            vols.append(v)
    if len(vols) < 20:
        return None
    vols.sort()
    idx = min(int(p.vol_raw_percentile * len(vols)), len(vols) - 1)
    return vols[idx]


def _yang_yin_vol_ratio(close, open_, vol, i, lookback=30):
    n = min(lookback, i + 1)
    yang, yin = [], []
    for j in range(n):
        if _b(close, i, j) >= _b(open_, i, j):
            yang.append(_b(vol, i, j))
        else:
            yin.append(_b(vol, i, j))
    if not yang or not yin:
        return None
    return (sum(yang) / len(yang)) / (sum(yin) / len(yin))


def _ma_convergence(ma5, ma10, ma20, ma60, i):
    vals = [_b(ma5, i, 0), _b(ma10, i, 0), _b(ma20, i, 0)]
    ma60_v = _b(ma60, i, 0)
    if ma60_v == ma60_v:
        vals.append(ma60_v)
    if any(v != v for v in vals):
        return None
    vs = sorted(vals)
    mid = vs[len(vs) // 2]
    if mid <= 0:
        return None
    return (max(vals) - min(vals)) / mid * 100


def _ma_slope(ma, i, days=5):
    if (i + 1) < days + 1:
        return None
    base = _b(ma, i, days)
    cur = _b(ma, i, 0)
    if base != base or cur != cur or base <= 0:
        return None
    return (cur - base) / base / days


def _near_low(low, close, i, lookback=60):
    n = min(lookback, i + 1)
    lo = min(_b(low, i, j) for j in range(n))
    if lo <= 0:
        return None
    return (_b(close, i, 0) - lo) / lo * 100


def _below_high(high, close, i, lookback=120):
    n = min(lookback, i + 1)
    hi = max(_b(high, i, j) for j in range(n))
    if hi <= 0:
        return None
    return (hi - _b(close, i, 0)) / hi * 100


def _bb_bandwidth(bb_top, bb_bot, bb_mid, i):
    top = _b(bb_top, i, 0)
    bot = _b(bb_bot, i, 0)
    mid = _b(bb_mid, i, 0)
    if mid <= 0 or mid != mid:
        return None
    return (top - bot) / mid * 100


def _check_vol_shrink_expand(vol, i, p):
    baseline_days = 10
    ed, sd = p.vol_expand_days, p.vol_shrink_days
    need = baseline_days + sd + ed
    if (i + 1) < need + 1:
        return False, None
    expand = [_b(vol, i, j) for j in range(0, ed)]
    shrink = [_b(vol, i, j) for j in range(ed, ed + sd)]
    base = [_b(vol, i, j) for j in range(ed + sd, ed + sd + baseline_days)]
    avg_base = sum(base) / baseline_days
    avg_shrink = sum(shrink) / sd
    avg_expand = sum(expand) / ed
    if avg_base <= 0 or avg_shrink <= 0:
        return False, None
    sr = avg_shrink / avg_base
    er = avg_expand / avg_shrink
    if (sr <= p.vol_shrink_max_ratio
            and p.vol_expand_min_ratio <= er <= p.vol_expand_max_ratio):
        return True, f"量先萎缩后温和放大 缩量比{sr:.2f} 扩量比{er:.2f}"
    return False, None


def _is_bb_narrow(state, current_bw, p):
    if current_bw is None:
        return False
    history = state['bb_bw_history']
    recent = [x for x in history[-30:] if x is not None]
    if not recent:
        return False
    return current_bw < (sum(recent) / len(recent)) * p.bb_narrow_ratio


# ════════════════════════════════════════════════════════════════════
# 主判定：复刻 bt_major_capital._check_buy 的 888-1096 行
# ════════════════════════════════════════════════════════════════════
@dataclass
class BarDecision:
    is_accumulating: bool = False           # 本 bar 是否满足建仓条件(WATCH)
    accumulation_days: int = 0              # 滚动窗口内命中天数
    watch_confirmed: bool = False           # window_hits ≥ min_watch_days
    buy: Optional[tuple] = None             # None 或 (reason, conf, meta)
    watch_reason: Optional[str] = None      # WATCH 描述(供实时轨推送)


def evaluate_bar(o, h, l, c, vol, ind, i, p: MajorCapitalParams,
                 state: dict, date_iso: str) -> BarDecision:
    """
    对第 i 根 bar 做 WATCH→BUY 判定。会读写 state 的 watch_* 字段。
    ind: dict/对象，含数组 rsi/atr/ma5/ma10/ma20/ma60/bb_top/bb_bot/bb_mid/macd_dif/macd_dea。
    调用前：① 已通过大盘过滤/trade_start；② 已把当前 bb_bandwidth append 到 state['bb_bw_history']。
    调用后：BUY 候选仍需调用方做资金流二次确认(若启用)。
    """
    g = lambda key: ind[key]
    close = _b(c, i, 0)
    open_ = _b(o, i, 0)
    rsi = _b(g('rsi'), i, 0)
    dif = _b(g('macd_dif'), i, 0)
    dea = _b(g('macd_dea'), i, 0)
    ma20_val = _b(g('ma20'), i, 0)

    dec = BarDecision(accumulation_days=state['accumulation_days'])

    # NaN 守卫(复刻 895：在 WATCH 累计之前)
    if any(v != v for v in [rsi, dif, dea, ma20_val]):
        return dec

    is_bull = close >= open_

    # ── 建仓中条件 ──
    near_low = _near_low(l, c, i, p.low_lookback)
    below_high = _below_high(h, c, i, p.high_lookback)
    conv = _ma_convergence(g('ma5'), g('ma10'), g('ma20'), g('ma60'), i)
    slope = _ma_slope(g('ma20'), i, 5)
    yy_ratio = _yang_yin_vol_ratio(c, o, vol, i, p.vol_lookback)
    vol_comp = _volume_compression(vol, i, p.vol_compression_short, p.vol_compression_long)

    is_accumulating = (
        near_low is not None and near_low <= p.max_above_low_pct
        and below_high is not None and below_high >= p.min_below_high_pct
        and conv is not None and conv <= p.ma_converge_pct
        and (slope is None or abs(slope) <= p.ma_slope_max)
        and p.rsi_watch_min <= rsi <= p.rsi_watch_max
        and yy_ratio is not None and yy_ratio >= p.vol_yang_yin_min
        and vol_comp is not None and vol_comp <= p.vol_compression_max
    )
    dec.is_accumulating = is_accumulating

    # 阶段1：WATCH 滚动窗口计数
    state['watch_history'].append(bool(is_accumulating))
    if len(state['watch_history']) > p.watch_rolling_window:
        state['watch_history'] = state['watch_history'][-p.watch_rolling_window:]
    window_hits = sum(1 for x in state['watch_history'] if x)
    state['accumulation_days'] = window_hits
    dec.accumulation_days = window_hits

    if is_accumulating:
        if state['watch_start'] is None:
            state['watch_start'] = i + 1
        if not state['watch_signal_dates'] or state['watch_signal_dates'][-1] != date_iso:
            state['watch_signal_dates'].append(date_iso)
            if len(state['watch_signal_dates']) > 60:
                state['watch_signal_dates'] = state['watch_signal_dates'][-60:]
        dec.watch_reason = (
            f"发现建仓迹象: 距底{near_low:.0f}% 均线粘合{conv:.1f}% "
            f"阳阴量比{yy_ratio:.2f} RSI={rsi:.0f} 窗口累计{window_hits}天"
        )

    # 阶段2：BUY — 窗口命中不足不触发
    if window_hits < p.min_watch_days:
        return dec
    dec.watch_confirmed = True
    if state['watch_start'] is None:
        state['watch_start'] = i + 1

    trigger = ''
    trigger_strength = 0
    adaptive_vol_thr = _adaptive_vol_threshold(vol, i, p)
    adaptive_pct_thr = _adaptive_breakout_pct(c, i, p)
    atr_val = _b(g('atr'), i, 0)

    # 信号A：放量大阳线突破
    if p.enable_signal_a:
        prev_close = _b(c, i, 1) if (i + 1) > 1 else close
        pct_chg = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
        vr = _vol_ratio(vol, i, 5)
        _high = _b(h, i, 0)
        _low = _b(l, i, 0)
        _cpr = ((close - _low) / (_high - _low)) if _high > _low else 1.0

        if p.breakout_atr_k > 0:
            _pct_pass = (atr_val == atr_val and atr_val > 0
                         and (close - prev_close) >= p.breakout_atr_k * atr_val)
            _pct_desc = f"涨{(close - prev_close):.2f}≥{p.breakout_atr_k}×ATR({atr_val:.2f})"
        else:
            _pct_pass = pct_chg >= adaptive_pct_thr
            _pct_desc = f"涨+{pct_chg:.1f}%(阈{adaptive_pct_thr:.1f}%)"

        if p.vol_raw_percentile > 0:
            _vol_q = _vol_raw_threshold(vol, i, p)
            _today_vol = _b(vol, i, 0)
            _vol_pass = _vol_q is not None and _today_vol > _vol_q
            _vol_desc = (f"量{_today_vol:.0f}>Q{int(p.vol_raw_percentile*100)}({_vol_q:.0f})"
                         if _vol_q else "量分位数据不足")
        else:
            _vol_pass = vr is not None and vr >= adaptive_vol_thr
            _vol_desc = f"量比{vr:.1f}x(阈{adaptive_vol_thr:.1f}x)" if vr else "量比数据不足"

        if is_bull and _pct_pass and _vol_pass and _cpr >= 0.5:
            # P2-A：rsi ≥ signal_a_min_rsi
            _qa_ok = p.signal_a_min_rsi <= 0 or rsi >= p.signal_a_min_rsi
            # P4：close > 前 N 日最高价
            _break_high = None
            if _qa_ok and p.signal_a_break_high_lookback > 0:
                lb = p.signal_a_break_high_lookback
                if (i + 1) >= lb + 1:
                    try:
                        _break_high = max(_b(h, i, j) for j in range(1, lb + 1))
                        if close <= _break_high:
                            _qa_ok = False
                    except Exception:
                        pass
            if _qa_ok:
                _bh = f" 破{p.signal_a_break_high_lookback}日高{_break_high:.2f}" if _break_high is not None else ""
                trigger = f"突破 {_pct_desc} {_vol_desc}{_bh}"
                trigger_strength = 3

    # 信号F：量先萎缩后温和放大
    if p.enable_signal_f and not trigger:
        if near_low is not None and near_low <= p.max_above_low_pct * 1.5:
            vsb_ok, vsb_desc = _check_vol_shrink_expand(vol, i, p)
            if vsb_ok:
                trigger = vsb_desc
                trigger_strength = 3

    if not trigger:
        return dec

    # ── 统一入场过滤 ──
    if rsi > p.rsi_buy_max:
        return dec
    prev_close_x = _b(c, i, 1) if (i + 1) > 1 else close
    day_pct = (close - prev_close_x) / prev_close_x * 100 if prev_close_x > 0 else 0
    if day_pct > p.breakout_max_pct:
        return dec
    if p.trend_filter:
        ma60_val = _b(g('ma60'), i, 0)
        if ma60_val == ma60_val and ma20_val <= ma60_val:
            return dec

    # ── 信心 ──
    conf = 0.3 + trigger_strength * 0.1
    if state['accumulation_days'] >= 30:
        conf += 0.15
    elif state['accumulation_days'] >= 20:
        conf += 0.10
    if yy_ratio and yy_ratio >= 1.10:
        conf += 0.10
    bw = _bb_bandwidth(g('bb_top'), g('bb_bot'), g('bb_mid'), i)
    bb_narrow = _is_bb_narrow(state, bw, p) or (any(
        _is_bb_narrow(state, state['bb_bw_history'][-j], p)
        for j in range(1, min(11, len(state['bb_bw_history'])))
        if state['bb_bw_history'][-j] is not None
    ) if state['bb_bw_history'] else False)
    if bb_narrow:
        conf += 0.10
    conf = min(conf, 1.0)

    reason = (
        f"建仓完毕: {trigger} | "
        f"累计{state['accumulation_days']}天 "
        f"阳阴量比{yy_ratio:.2f} RSI={rsi:.0f}"
    )
    _ma5_v = _b(g('ma5'), i, 0)
    _ma20_v = _b(g('ma20'), i, 0)
    _lb = p.ma_diverge_lookback
    try:
        _ma5_prev = _b(g('ma5'), i, _lb)
        _ma20_prev = _b(g('ma20'), i, _lb)
        _ma_div = round((_ma5_v - _ma20_v) / _ma20_v * 100, 2) if _ma20_v > 0 else None
        _ma_div_prev = round((_ma5_prev - _ma20_prev) / _ma20_prev * 100, 2) if _ma20_prev > 0 else None
    except Exception:
        _ma_div = _ma_div_prev = None

    meta = {
        'trigger': trigger,
        'accumulation_days': state['accumulation_days'],
        'watch_signal_dates': list(state['watch_signal_dates']),
        'confidence': round(conf, 3),
        'rsi': round(rsi, 1),
        'near_low_pct': round(near_low, 1) if near_low is not None else None,
        'ma_converge_pct': round(conv, 2) if conv is not None else None,
        'yy_ratio': round(yy_ratio, 2) if yy_ratio is not None else None,
        'bb_narrow': bb_narrow,
        'ma_diverge_pct': _ma_div,
        'ma_diverge_prev_pct': _ma_div_prev,
    }
    dec.buy = (reason, conf, meta)
    return dec
