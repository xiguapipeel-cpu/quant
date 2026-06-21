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

from backtest import major_capital_rules as mc_rules  # 公共信号核(与实时扫描共用)

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
        max_above_low_pct=15.0,          # ★ 20→15 收紧，剔除下跌中继平台
        # ── 真正低位确认：距 120 日最高 ≥ 30% ──
        high_lookback=120,
        min_below_high_pct=30.0,
        ma_converge_pct=3.0,             # ★ 5→3 真正的粘合极差在 2-3%，纳入 MA60 四线收敛
        ma_slope_max=0.01,               # ★ 日斜率 ≤ 1%，允许缓慢上升/下降
        bb_period=20,
        bb_narrow_ratio=0.85,
        vol_yang_yin_min=1.03,
        vol_lookback=30,
        rsi_watch_min=25.0,
        rsi_watch_max=62.0,
        # ── 阶段2 BUY ──
        min_watch_days=15,              # ★ 滚动 30 天窗口内满足 ≥ 15 天即确认
        breakout_pct=4.0,
        breakout_vol_ratio=4.0,         # ★ 优化：2.0→4.0 要求超强量能确认
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
        # 选股填槽模式：'confidence'(按置信度降序,默认/baseline) |
        #   'neutral'(中性稳定伪随机,不按动量——审计证 rank_score 反向有害后的落地选股)
        select_mode='confidence',
        # ── 内部选股参数（主力建仓预设） ──
        screen_enabled=True,           # 是否启用策略内选股
        screen_min_price=2.0,
        screen_max_price=100.0,
        screen_min_amount_wan=300,     # 日均成交额下限（万元）建仓期成交低是特征
        screen_min_bars=80,            # 最少历史 bar 数（需覆盖 low_lookback=60 + buffer）
        screen_vol_window=20,          # 成交额计算窗口
        # ── 信号E：三线多头排列持续向上发散 ──
        ma_diverge_lookback=5,
        # ── 信号F：横盘整理尾端量先萎缩后温和放大 ──
        vol_shrink_days=15,
        vol_expand_days=5,
        vol_shrink_max_ratio=0.80,
        vol_expand_min_ratio=1.2,
        vol_expand_max_ratio=2.5,
        # ── 信号G：持续均匀大买单 ──
        orderflow_days=10,
        orderflow_bull_min=0.6,
        orderflow_vol_cv_max=0.5,
        orderflow_max_daily_pct=4.0,
        # ── 交易起始日（预热期内只计算指标，不下单） ──
        # 用于"本地数据库"模式：数据从更早加载，但实际交易从 start 开始
        trade_start_date=None,          # datetime.date 对象，None=不限制
        # ── 大盘过滤 ──
        market_filter=True,             # 大盘下跌趋势时禁止买入
        market_code='idx_sh',           # 指数 data feed 的 name
        market_ma_fast=20,              # 大盘快线
        market_ma_slow=60,              # 大盘慢线
        # 扩展过滤（P3，默认 off；通过 extra_params 启用并对照测试）
        market_rsi_period=14,           # 指数 RSI 周期
        market_rsi_min=0.0,             # 指数 RSI 下限（0 = 关闭；建议 50）
        market_breadth_min=0.0,         # 涨跌家数比下限（0 = 关闭；建议 1.0 即上涨家数 ≥ 下跌家数）
        # ══════════════════════════════════════════════════════════
        # 方案 A：自适应阈值（替代 breakout_vol_ratio / breakout_pct /
        #            stop_loss_pct / trailing_pct 等硬编码值，降低过拟合）
        # ══════════════════════════════════════════════════════════
        adaptive_lookback=60,           # 历史分位数窗口
        vol_ratio_percentile=0.85,      # 放量要求：近60日5日量比序列 85 分位
        breakout_pct_percentile=0.75,   # 突破涨幅：近60日涨幅序列 75 分位
        vol_ratio_min=2.0,              # 放量下限（保底，避免分位数太低）
        breakout_pct_min=3.0,           # 涨幅下限（同上）
        # P5：新阈值定义（>0 启用，取代上述 percentile 系列）
        # 涨幅用 ATR 倍数自适应不同股票波动率；量用原始 60 日量分位
        breakout_atr_k=0.0,             # >0：要求 (close - prev_close) ≥ k × ATR
        vol_raw_percentile=0.0,         # >0：要求 today_vol > Q(N%) of prev 60d volume
        atr_period=20,                  # ATR 周期
        atr_stop_k=2.0,                 # 硬止损 = k × ATR（自适应）
        atr_trail_k=2.0,                # 追踪止损 = k × ATR — P0 #1 验证：3.0→2.0 stage1 收紧
        # ══════════════════════════════════════════════════════════
        # 持仓管理：金字塔加仓 + 动态追踪止损（放大盈利单）
        # ══════════════════════════════════════════════════════════
        pyramid_enabled=True,
        pyramid_trigger_gain=0.08,       # 浮盈 ≥ 8% 且创持仓新高触发加仓
        pyramid_size_ratio=0.5,          # 每次加仓 = 0.5 × 初始仓位
        pyramid_max_adds=2,              # 单只股票最多加仓次数
        pyramid_min_gap_days=5,          # 两次加仓之间至少间隔 N 日
        # 动态追踪止损：反转哲学（P0 #1）— 盈利越大，trail k 越紧，锁住浮盈
        # 旧值（baseline，越大越宽）: stage2_gain=0.15 k=5.0; stage3_gain=0.30 k=7.0
        # 新值（tight，4 折 OOS 累计 -15% → -8%）: 越赚越紧
        trail_stage2_gain=0.05,          # 盈利 ≥ 5% 后切换 stage2（早启动保护）
        trail_stage2_k=1.5,              # stage2 trail = 1.5 × ATR
        trail_stage3_gain=0.15,          # 盈利 ≥ 15% 后切换 stage3
        trail_stage3_k=1.0,              # stage3 trail = 1.0 × ATR（最紧锁利）
        # 注：旧版本曾有 lock_floor_t1/t2/t3 + pyramid_require_highest_gain 共 7 个参数
        # 4 折 OOS 全部 0 触发（trail 包络其上 → max(trail, hard, floor) = trail），
        # 已删除以减少自由度避免过拟合心理依赖（参考 CLAUDE.md 决策追溯）
        # ══════════════════════════════════════════════════════════
        # 方案 B：信号精简开关
        # ══════════════════════════════════════════════════════════
        enable_signal_a=True,           # 放量突破（核心）
        enable_signal_f=False,          # 量萎缩后放大 — P1-B walk-forward 验证：4 折累计 -20pp，关闭。
                                        #   P1-A 诊断：5日胜率 78.6% 但 final 均亏 1.78%（典型假突破后回吐）
                                        #   关闭后 4 折累计由 -7.83% 改善至 +12.29%（Δ +20.12pp，4/4 折改善或持平）
        # 信号 A 质量过滤 — P2-A walk-forward 验证：4 折累计 +22.60% (vs +12.31% 未过滤，Δ +10.29pp)
        # P1-A 诊断：rsi 低分位 final 均亏 -1.83%；rsi 与 ma_diverge 强相关
        # 砍掉的入场是 Fold 1 的 4 月关税黑天鹅灾难单（如 600611 -17.77%）+ Fold 2 噪音区
        # Fold 2 闭仓胜率从 58% → 83.3%；Fold 3 几乎不影响
        # 注：旧版本曾有 signal_a_min_ma_diverge_pct（mdiv≥0）配合使用，4 折逐笔等价
        # 已删除（P2-A dissect 证明 rsi/mdiv 测同一件事，砍同一批 6 笔；保留 rsi 即可）
        signal_a_min_rsi=55.0,                   # A_breakout 要求 rsi ≥ 55（P2-A）
        # P4：突破需"close > max(high[-1..-N])" 才算真突破（解决随机放量大阳线假信号）
        # 4 折 OOS 累计 +22.60% → +30.52%（Δ +7.92pp，4/4 折改善或持平）
        # Fold 2/3 闭仓胜率均从 83% 升至 100%；BH_10/20 等价（同砍 2 笔），BH_30 在 Fold 3 多 +3.35pp
        # 30 日窗口与策略 watch 期（30 日滚动窗口 ≥15 天累计）哲学一致
        signal_a_break_high_lookback=30,         # 0=关闭；>0 时 close 必须 > max(high[-1..-N])
        # B/C/D/E/G 已删除：过拟合痕迹明显，经统计显著性不足
        # ══════════════════════════════════════════════════════════
        # 滚动窗口建仓计数（替代"中途断档清零"刚性逻辑）
        # 过去 watch_rolling_window 天内满足 ≥ min_watch_days 天即确认
        # 允许主力洗盘期间条件短暂失效
        # ══════════════════════════════════════════════════════════
        watch_rolling_window=30,        # 滚动窗口天数
        # ══════════════════════════════════════════════════════════
        # 量能维度：地量特征 或 换手温和（主力低位吸筹特征）
        # ══════════════════════════════════════════════════════════
        vol_compression_short=20,       # 短期均量窗口
        vol_compression_long=120,       # 长期均量窗口
        vol_compression_max=0.70,       # 短均量 / 长均量 ≤ 0.70 → 地量
        # ══════════════════════════════════════════════════════════
        # 资金流二次确认（依赖 stock_fund_flow 表）
        # 数据来源：akshare.stock_individual_fund_flow（单股最近 ~120 个交易日）
        # 默认 disabled — 仅当外部把 fund_flow_data 注入并启用后生效。
        # 数据不足时（lookback 不够）按 strict_mode 决定通过/拒绝。
        # ══════════════════════════════════════════════════════════
        fund_flow_enabled=False,
        fund_flow_data=None,                # dict: {code: pd.DataFrame indexed by date}
        fund_flow_lookback=20,
        fund_flow_min_avg_pct=0.0,          # 主力净占比近 N 日均值 ≥ 此值（%）
        fund_flow_pos_days_ratio=0.45,      # 主力流入天数占比 ≥ 此值
        fund_flow_require_pos_breakout=True, # 突破日主力净流入必须为正
        fund_flow_strict_mode=False,        # 数据不足时：False=放行(降级) True=拒绝
    )

    def __init__(self):
        self.indicators = {}
        self.active_datas = []   # 通过选股过滤的 data feeds

        # ── 大盘指数指标 ──
        self._market_data = None
        self._market_ma_fast = None
        self._market_ma_slow = None
        self._market_rsi = None
        if self.p.market_filter:
            for d in self.datas:
                if d._name == self.p.market_code:
                    self._market_data = d
                    self._market_ma_fast = bt.indicators.SMA(
                        d.close, period=self.p.market_ma_fast)
                    self._market_ma_slow = bt.indicators.SMA(
                        d.close, period=self.p.market_ma_slow)
                    self._market_rsi = bt.indicators.RSI(
                        d.close, period=self.p.market_rsi_period)
                    break
        # 涨跌家数缓存（按交易日缓存，避免每只股票重复遍历）
        self._breadth_cache_date = None
        self._breadth_cache_value = None

        for d in self.datas:
            if d._name == self.p.market_code:
                continue   # 指数不参与个股逻辑
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
            # ★ 方案A：ATR 用于自适应止损/追踪
            ind['atr'] = bt.indicators.ATR(d, period=self.p.atr_period)
            self.indicators[d._name] = ind

        # 每只股票的状态（跳过指数）
        self.stock_state = {}
        for d in self.datas:
            if d._name == self.p.market_code:
                continue
            self.stock_state[d._name] = self._init_state()

        self.order_dict = {}
        self.trade_log = []
        self._screened = {}   # name → bool, 选股结果缓存

    @staticmethod
    def _init_state():
        return {
            'watch_start': None,
            'accumulation_days': 0,
            'watch_history': [],        # 滚动窗口布尔队列：最近 N 日每天是否满足建仓条件
            'watch_signal_dates': [],   # 每日满足建仓条件的日期列表（最多保留60条）
            'in_position': False,
            'buy_price': 0.0,            # 加权均价
            'total_cost': 0.0,           # 累计成本（含所有加仓）
            'total_shares': 0,           # 累计股数
            'init_shares': 0,            # 初次买入股数（用于加仓基数）
            'add_count': 0,              # 已加仓次数
            'last_add_day': 0,           # 上次加仓时 days_since_buy
            'highest_since_buy': 0.0,
            'days_below_ma': 0,
            'days_since_buy': 0,
            'rsi_peaked': False,
            'dist_warned': False,
            'dist_warn_info': '',
            'dist_warn_high': 0.0,
            'bb_bw_history': [],
            'breakout_day_low': 0.0,   # ★ 新增：突破日最低价（用于3日突破失败止损）
            'pending_breakout': None,  # ★ 新增：A/B信号挂起，等待次日收盘确认
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

    def _market_breadth_ratio(self):
        """当日涨跌家数比 = up_count / down_count（缓存到日，避免重复遍历）。
        下跌家数为 0 时返回 inf。len(d) < 2 的 data 跳过。"""
        today = self.datetime.date(0)
        if self._breadth_cache_date == today:
            return self._breadth_cache_value
        up = down = 0
        for d in self.datas:
            if d is self._market_data:
                continue
            if len(d) < 2:
                continue
            try:
                c0 = float(d.close[0])
                c1 = float(d.close[-1])
                if c0 != c0 or c1 != c1 or c1 <= 0:
                    continue
                if c0 > c1:
                    up += 1
                elif c0 < c1:
                    down += 1
            except Exception:
                pass
        ratio = (up / down) if down > 0 else float('inf')
        self._breadth_cache_date = today
        self._breadth_cache_value = ratio
        return ratio

    def _vol_ratio(self, d, period=5):
        if len(d) < period + 1:
            return None
        avg = sum(float(d.volume[-j]) for j in range(1, period + 1)) / period
        return float(d.volume[0]) / avg if avg > 1e-9 else None

    def _volume_compression(self, d, short_n=None, long_n=None):
        """
        量能萎缩比 = 近 short_n 日均量 / 近 long_n 日均量
        值 ≤ vol_compression_max（默认 0.70）视为"地量特征"（主力低位吸筹标志）
        数据不足时退化：若至少有 short_n*2 日，用 short_n*2 作长窗口；仍不足则返回 None
        """
        short_n = short_n or self.p.vol_compression_short
        long_n = long_n or self.p.vol_compression_long
        if len(d) < short_n + 1:
            return None
        if len(d) < long_n + 1:
            long_n = min(len(d) - 1, short_n * 2)
            if long_n <= short_n:
                return None
        short_avg = sum(float(d.volume[-j]) for j in range(1, short_n + 1)) / short_n
        long_avg = sum(float(d.volume[-j]) for j in range(1, long_n + 1)) / long_n
        if long_avg <= 1e-9:
            return None
        return short_avg / long_avg

    # ── 方案A 自适应阈值辅助 ────────────────────────────────────
    def _adaptive_vol_threshold(self, d):
        """
        返回"放量"的自适应门槛：
          取过去 adaptive_lookback 日中每日的 5日量比分布，返回 percentile 分位数
          与 vol_ratio_min 取大（下限保底）
        """
        lb = self.p.adaptive_lookback
        need = lb + 6
        if len(d) < need:
            return self.p.vol_ratio_min
        samples = []
        for j in range(1, lb + 1):   # j=1..lb，跳过今日本身
            avg5 = sum(float(d.volume[-j - k]) for k in range(1, 6)) / 5.0
            if avg5 > 0:
                samples.append(float(d.volume[-j]) / avg5)
        if len(samples) < 20:
            return self.p.vol_ratio_min
        samples.sort()
        idx = min(int(self.p.vol_ratio_percentile * len(samples)), len(samples) - 1)
        return max(samples[idx], self.p.vol_ratio_min)

    def _adaptive_breakout_pct(self, d):
        """
        返回"大涨"的自适应门槛（百分比）：
          过去 adaptive_lookback 日单日涨幅的 percentile 分位数，与 breakout_pct_min 取大
        """
        lb = self.p.adaptive_lookback
        if len(d) < lb + 2:
            return self.p.breakout_pct_min
        samples = []
        for j in range(1, lb + 1):
            prev = float(d.close[-j - 1])
            cur = float(d.close[-j])
            if prev > 0:
                samples.append((cur - prev) / prev * 100)
        if len(samples) < 20:
            return self.p.breakout_pct_min
        samples.sort()
        idx = min(int(self.p.breakout_pct_percentile * len(samples)), len(samples) - 1)
        return max(samples[idx], self.p.breakout_pct_min)

    def _vol_raw_threshold(self, d):
        """
        返回"今日成交量需超越"的绝对值门槛（P5 新逻辑）：
          过去 adaptive_lookback 日成交量的 percentile 分位数（不做 5 日平滑）
        返回 None 表示数据不足或未启用。
        """
        if self.p.vol_raw_percentile <= 0:
            return None
        lb = self.p.adaptive_lookback
        if len(d) < lb + 1:
            return None
        vols = []
        for j in range(1, lb + 1):
            v = float(d.volume[-j])
            if v > 0 and v == v:   # 排除 0 和 NaN
                vols.append(v)
        if len(vols) < 20:
            return None
        vols.sort()
        idx = min(int(self.p.vol_raw_percentile * len(vols)), len(vols) - 1)
        return vols[idx]

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
        """
        MA5/10/20/60 四线极差率（%）。
        真正底部区域四条均线同时收敛 — 仅用 MA5/10/20 会把缓慢下跌的状态误判为粘合。
        数据不足时（ma60 NaN）退化为三线计算，保持兼容。
        """
        vals = [float(ind['ma5'][0]), float(ind['ma10'][0]), float(ind['ma20'][0])]
        ma60_v = float(ind['ma60'][0])
        if ma60_v == ma60_v:   # 非 NaN 则纳入
            vals.append(ma60_v)
        if any(v != v for v in vals):
            return None
        vals_sorted = sorted(vals)
        mid_val = vals_sorted[len(vals_sorted) // 2]
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

    def _below_high(self, d, lookback=120):
        """当前收盘距近 lookback 日最高点的跌幅百分比（正值越大代表越低）"""
        n = min(lookback, len(d))
        high = max(float(d.high[-j]) for j in range(n))
        if high <= 0:
            return None
        return (high - float(d.close[0])) / high * 100

    def _bb_bandwidth(self, ind):
        top = float(ind['bb'].top[0])
        bot = float(ind['bb'].bot[0])
        mid = float(ind['bb'].mid[0])
        if mid <= 0 or mid != mid:
            return None
        return (top - bot) / mid * 100

    # ── 信号E helper：三线多头排列持续向上发散 ──────────────
    def _check_ma_diverge(self, ind):
        """
        三条均线同时满足：
          1. 多头排列：MA5 > MA10 > MA20
          2. 三线斜率均为正且依次递减（短期 > 中期 > 长期）
          3. MA5-MA20 间距在过去 lookback 天内扩大（发散加速）
        返回 (passed, description_str)
        """
        lb = self.p.ma_diverge_lookback
        ma5_v  = float(ind['ma5'][0])
        ma10_v = float(ind['ma10'][0])
        ma20_v = float(ind['ma20'][0])
        if any(v != v for v in [ma5_v, ma10_v, ma20_v]):
            return False, None
        if not (ma5_v > ma10_v > ma20_v):
            return False, None

        s5  = self._ma_slope(ind['ma5'],  lb)
        s10 = self._ma_slope(ind['ma10'], lb)
        s20 = self._ma_slope(ind['ma20'], lb)
        if None in (s5, s10, s20):
            return False, None
        if not (s5 > s10 > s20 > 0):
            return False, None

        try:
            ma5_prev  = float(ind['ma5'][-lb])
            ma20_prev = float(ind['ma20'][-lb])
        except Exception:
            return False, None
        if ma5_prev != ma5_prev or ma20_prev != ma20_prev or ma20_prev <= 0:
            return False, None

        prev_gap = (ma5_prev - ma20_prev) / ma20_prev
        cur_gap  = (ma5_v   - ma20_v)    / ma20_v
        if cur_gap <= prev_gap:
            return False, None
        return True, f"三线多头发散加速 斜率{s5:.4f}>{s10:.4f}>{s20:.4f}"

    # ── 信号F helper：横盘整理尾端量先萎缩后温和放大 ─────────
    def _check_vol_shrink_expand(self, d):
        """
        将当前时点往前分三段：
          基准期（10日）→ 萎缩期（vol_shrink_days）→ 扩张期（vol_expand_days，最近）
        """
        baseline_days = 10
        ed = self.p.vol_expand_days
        sd = self.p.vol_shrink_days
        need = baseline_days + sd + ed
        if len(d) < need + 1:
            return False, None

        expand_vols  = [float(d.volume[-j]) for j in range(0, ed)]
        shrink_vols  = [float(d.volume[-j]) for j in range(ed, ed + sd)]
        base_vols    = [float(d.volume[-j]) for j in range(ed + sd, ed + sd + baseline_days)]

        avg_base   = sum(base_vols)   / baseline_days
        avg_shrink = sum(shrink_vols) / sd
        avg_expand = sum(expand_vols) / ed

        if avg_base <= 0 or avg_shrink <= 0:
            return False, None

        shrink_ratio = avg_shrink / avg_base
        expand_ratio = avg_expand / avg_shrink
        if (shrink_ratio <= self.p.vol_shrink_max_ratio
                and self.p.vol_expand_min_ratio <= expand_ratio <= self.p.vol_expand_max_ratio):
            return True, f"量先萎缩后温和放大 缩量比{shrink_ratio:.2f} 扩量比{expand_ratio:.2f}"
        return False, None

    # ── 信号G helper：持续均匀大买单（OHLCV 近似） ───────────
    def _check_orderflow(self, d):
        """
        近 orderflow_days 天满足：
          阳线占比 ≥ orderflow_bull_min
          量的变异系数（CV）≤ orderflow_vol_cv_max
          单日最大涨跌幅 ≤ orderflow_max_daily_pct
        """
        n = self.p.orderflow_days
        if len(d) < n + 1:
            return False, None

        bull_count = sum(
            1 for j in range(0, n)
            if float(d.close[-j]) >= float(d.open[-j])
        )
        bull_ratio = bull_count / n

        recent_vols = [float(d.volume[-j]) for j in range(0, n)]
        avg_vol = sum(recent_vols) / n
        if avg_vol <= 0:
            return False, None
        vol_std = (sum((v - avg_vol) ** 2 for v in recent_vols) / n) ** 0.5
        vol_cv  = vol_std / avg_vol

        max_daily_pct = max(
            abs((float(d.close[-j]) - float(d.close[-j - 1])) / float(d.close[-j - 1]) * 100)
            for j in range(0, n)
            if float(d.close[-j - 1]) > 0
        )

        if (bull_ratio >= self.p.orderflow_bull_min
                and vol_cv  <= self.p.orderflow_vol_cv_max
                and max_daily_pct <= self.p.orderflow_max_daily_pct):
            return True, f"持续均匀买单 阳线{bull_count}/{n}日 量均匀CV={vol_cv:.2f}"
        return False, None

    def _is_bb_narrow(self, state, current_bw):
        if current_bw is None:
            return False
        history = state['bb_bw_history']
        recent = [x for x in history[-30:] if x is not None]
        if not recent:
            return False
        return current_bw < (sum(recent) / len(recent)) * self.p.bb_narrow_ratio

    # ── 资金流二次确认 ──────────────────────────────────────
    def _check_fund_flow(self, code: str):
        """
        基于 stock_fund_flow 表的二次确认。

        通过条件（同时满足）：
          1. 近 lookback 日 main_net_pct 均值 ≥ fund_flow_min_avg_pct
          2. 近 lookback 日 main_net_amount > 0 的天数占比 ≥ fund_flow_pos_days_ratio
          3. （可选）突破日 main_net_amount > 0

        数据不足或缺失：strict_mode=False 时降级放行；True 时拒绝。

        返回 (passed: bool, reason_str: str, meta: dict)
        """
        ff_data = self.p.fund_flow_data
        if not ff_data:
            return self._ff_fallback("fund_flow_data 未注入")

        df = ff_data.get(code)
        if df is None or len(df) == 0:
            return self._ff_fallback(f"{code} 无资金流数据")

        # 当前 bar 日期
        try:
            cur_dt = self.datetime.date(0)
        except Exception:
            return self._ff_fallback("无法获取当前 bar 日期")

        # 取 ≤ 当日的所有行（DataFrame index 是 Timestamp，比较时转 date）
        try:
            cur_ts = __import__('pandas').Timestamp(cur_dt)
            prior = df[df.index <= cur_ts]
        except Exception as e:
            return self._ff_fallback(f"切片失败: {e}")

        lookback = self.p.fund_flow_lookback
        if len(prior) < lookback:
            return self._ff_fallback(
                f"近 {lookback} 日数据不足（实际 {len(prior)}）"
            )

        recent = prior.tail(lookback)
        # 容错：列存在但全为 NaN
        main_pct_series = recent['main_net_pct'].dropna()
        main_amt_series = recent['main_net_amount'].dropna()
        if len(main_pct_series) < lookback // 2:
            return self._ff_fallback("main_net_pct 有效样本不足")

        avg_pct = float(main_pct_series.mean())
        pos_days = int((main_amt_series > 0).sum())
        valid_days = int(len(main_amt_series))
        pos_ratio = pos_days / valid_days if valid_days > 0 else 0.0

        meta = {
            'ff_avg_main_pct':       round(avg_pct, 3),
            'ff_pos_days_ratio':     round(pos_ratio, 3),
            'ff_lookback_used':      valid_days,
        }

        # 累计期均值检查
        if avg_pct < self.p.fund_flow_min_avg_pct:
            return False, f"主力净占比均值{avg_pct:+.2f}% <阈{self.p.fund_flow_min_avg_pct}", meta

        # 主力流入天数占比
        if pos_ratio < self.p.fund_flow_pos_days_ratio:
            return False, f"主力流入天数{pos_days}/{valid_days}={pos_ratio:.0%}<阈{self.p.fund_flow_pos_days_ratio:.0%}", meta

        # 突破日主力净流入必须为正
        if self.p.fund_flow_require_pos_breakout:
            today_amt = recent['main_net_amount'].iloc[-1]
            today_amt = float(today_amt) if today_amt == today_amt else None  # NaN check
            meta['ff_today_main_amt'] = round(today_amt, 0) if today_amt is not None else None
            if today_amt is None or today_amt <= 0:
                return False, f"突破日主力净流入={today_amt} (≤0)", meta

        return True, f"资金流确认 均值{avg_pct:+.2f}% 正流入{pos_ratio:.0%}", meta

    def _ff_fallback(self, reason: str):
        """资金流数据缺失/不足时的降级行为，根据 strict_mode 决定。"""
        meta = {'ff_skip_reason': reason}
        if self.p.fund_flow_strict_mode:
            return False, f"资金流严格模式拒绝: {reason}", meta
        return True, f"资金流降级放行: {reason}", meta

    def _n_positions(self):
        return sum(1 for d in self.datas if self.getposition(d).size > 0)

    # ══════════════════════════════════════════════════════════
    # 3. 信号生成（单只股票的当日判断）
    # ══════════════════════════════════════════════════════════

    def _check_sell(self, d, state, ind):
        """
        方案B：出场逻辑简化为 2 个条件（删除所有过拟合痕迹）
          1. ATR 自适应止损（兼顾硬止损 + 追踪止损：用 max(初始止损线, 追踪止损线)）
          2. 跌破 MA20 连续 N 日
        删除：RSI超买回落 / 冲高回落 / 3日突破失败 / 时间止损 / 固定百分比止损
        """
        close = float(d.close[0])
        ma20_val = float(ind['ma20'][0])
        buy_price = state['buy_price']

        state['highest_since_buy'] = max(state['highest_since_buy'], close)
        state['days_since_buy'] += 1

        # ── 止损/止盈：ATR 自适应（含动态 trailing） ──
        atr_val = float(ind['atr'][0])
        if atr_val == atr_val and atr_val > 0:
            # 动态 trail_k：盈利越大，止损越宽（让大赢单跑）
            cur_gain = (close - buy_price) / buy_price if buy_price > 0 else 0
            if cur_gain >= self.p.trail_stage3_gain:
                dyn_trail_k = self.p.trail_stage3_k
            elif cur_gain >= self.p.trail_stage2_gain:
                dyn_trail_k = self.p.trail_stage2_k
            else:
                dyn_trail_k = self.p.atr_trail_k
            # 追踪止损线 = 最高价 - k × ATR
            trail_line = state['highest_since_buy'] - dyn_trail_k * atr_val
            # 初始硬止损线 = 买入价 - k × ATR（仅在未盈利时有效）
            hard_line = buy_price - self.p.atr_stop_k * atr_val
            # 有效止损线 = max(追踪线, 硬止损线)
            # 盈利期：trail_line 高于 hard_line，用 trail
            # 亏损期：trail_line 接近 hard_line，两者等效
            stop_line = max(trail_line, hard_line)
            if close <= stop_line:
                gain = (close - buy_price) / buy_price
                if close <= hard_line and state['highest_since_buy'] <= buy_price * 1.02:
                    return f"ATR硬止损(k={self.p.atr_stop_k}): {gain:+.1%}"
                else:
                    dd = (state['highest_since_buy'] - close) / state['highest_since_buy'] if state['highest_since_buy'] > 0 else 0
                    return f"ATR追踪止损(k={dyn_trail_k}) 回撤{dd:.1%} 收益{gain:+.1%}"

        # ── 趋势反转：跌破 MA20 连续 N 日 ──
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

    def _build_mc_params(self) -> "mc_rules.MajorCapitalParams":
        """从 backtrader params 映射到公共信号核参数(单一来源)。"""
        p = self.p
        return mc_rules.MajorCapitalParams(
            low_lookback=p.low_lookback, max_above_low_pct=p.max_above_low_pct,
            high_lookback=p.high_lookback, min_below_high_pct=p.min_below_high_pct,
            ma_converge_pct=p.ma_converge_pct, ma_slope_max=p.ma_slope_max,
            vol_yang_yin_min=p.vol_yang_yin_min, vol_lookback=p.vol_lookback,
            rsi_watch_min=p.rsi_watch_min, rsi_watch_max=p.rsi_watch_max,
            vol_compression_max=p.vol_compression_max,
            vol_compression_short=p.vol_compression_short,
            vol_compression_long=p.vol_compression_long,
            watch_rolling_window=p.watch_rolling_window, min_watch_days=p.min_watch_days,
            enable_signal_a=p.enable_signal_a, enable_signal_f=p.enable_signal_f,
            breakout_max_pct=p.breakout_max_pct, rsi_buy_max=p.rsi_buy_max,
            trend_filter=p.trend_filter, signal_a_min_rsi=p.signal_a_min_rsi,
            signal_a_break_high_lookback=p.signal_a_break_high_lookback,
            breakout_atr_k=p.breakout_atr_k, vol_raw_percentile=p.vol_raw_percentile,
            atr_period=p.atr_period, adaptive_lookback=p.adaptive_lookback,
            vol_ratio_percentile=p.vol_ratio_percentile, vol_ratio_min=p.vol_ratio_min,
            breakout_pct_percentile=p.breakout_pct_percentile,
            breakout_pct_min=p.breakout_pct_min,
            vol_shrink_days=p.vol_shrink_days, vol_expand_days=p.vol_expand_days,
            vol_shrink_max_ratio=p.vol_shrink_max_ratio,
            vol_expand_min_ratio=p.vol_expand_min_ratio,
            vol_expand_max_ratio=p.vol_expand_max_ratio,
            bb_narrow_ratio=p.bb_narrow_ratio, ma_diverge_lookback=p.ma_diverge_lookback,
        )

    def _check_buy(self, d, state, ind):
        """
        检查两阶段买入信号，返回 (trigger, confidence) 或 (None, 0)。
        仅判断、不下单。
        """
        # DB模式：trade_start_date 之前完全跳过，不更新建仓状态
        # 2024年数据仅供指标预热，建仓计数从 trade_start_date 起算（避免924行情扰乱）
        if self.p.trade_start_date is not None:
            current_date = self.datetime.date(0)
            if current_date < self.p.trade_start_date:
                return None, 0, {}

        # ── 大盘过滤：MA20 < MA60 + 可选 RSI/breadth ──
        if self.p.market_filter and self._market_ma_fast is not None:
            try:
                mf = float(self._market_ma_fast[0])
                ms = float(self._market_ma_slow[0])
                if mf == mf and ms == ms and mf < ms:
                    return None, 0, {}   # 大盘下跌趋势，拒绝入场
                # 指数 RSI 下限（默认 off）
                if self.p.market_rsi_min > 0 and self._market_rsi is not None:
                    mr = float(self._market_rsi[0])
                    if mr == mr and mr < self.p.market_rsi_min:
                        return None, 0, {}
                # 涨跌家数比下限（默认 off）
                if self.p.market_breadth_min > 0:
                    br = self._market_breadth_ratio()
                    if br < self.p.market_breadth_min:
                        return None, 0, {}
            except Exception:
                pass

        # ── 公共信号核：构造窗口数组 + 指标值，调 evaluate_bar ──
        # (WATCH 状态机 + 信号A/F + P2-A/P4 过滤 + 信心/meta 全在 mc_rules)
        if not hasattr(self, '_mc_params'):
            self._mc_params = self._build_mc_params()
        W = min(len(d), 200)
        o   = list(d.open.get(size=W))
        h   = list(d.high.get(size=W))
        l   = list(d.low.get(size=W))
        c   = list(d.close.get(size=W))
        vol = list(d.volume.get(size=W))
        if not c:
            return None, 0, {}
        ind_arr = {
            'rsi':      list(ind['rsi'].get(size=W)),
            'atr':      list(ind['atr'].get(size=W)),
            'ma5':      list(ind['ma5'].get(size=W)),
            'ma10':     list(ind['ma10'].get(size=W)),
            'ma20':     list(ind['ma20'].get(size=W)),
            'ma60':     list(ind['ma60'].get(size=W)),
            'bb_top':   list(ind['bb'].top.get(size=W)),
            'bb_bot':   list(ind['bb'].bot.get(size=W)),
            'bb_mid':   list(ind['bb'].mid.get(size=W)),
            'macd_dif': list(ind['macd'].macd.get(size=W)),
            'macd_dea': list(ind['macd'].signal.get(size=W)),
        }
        i = len(c) - 1
        date_iso = self.datetime.date(0).isoformat()
        dec = mc_rules.evaluate_bar(o, h, l, c, vol, ind_arr, i,
                                    self._mc_params, state, date_iso)
        if dec.buy is None:
            return None, 0, {}
        reason, conf, meta = dec.buy

        # ── 资金流二次确认(后置门，需 DB；不进公共核) ──
        if self.p.fund_flow_enabled:
            ff_pass, ff_reason, ff_meta = self._check_fund_flow(d._name)
            if not ff_pass:
                return None, 0, {}
            if ff_meta:
                meta.update(ff_meta)
        return reason, conf, meta

    # ══════════════════════════════════════════════════════════
    # 4. 核心 next — 信号收集 + 优先级排序 + 批量执行
    # ══════════════════════════════════════════════════════════

    def notify_order(self, order):
        if order.status in [order.Completed]:
            name = order.data._name
            state = self.stock_state[name]
            if order.isbuy():
                exec_price = order.executed.price
                exec_size = order.executed.size
                is_add = state.get('in_position', False) and getattr(order, '_is_pyramid', False)
                if is_add:
                    # 加仓：更新加权均价
                    new_total_cost = state['total_cost'] + exec_price * exec_size
                    new_total_shares = state['total_shares'] + exec_size
                    state['total_cost'] = new_total_cost
                    state['total_shares'] = new_total_shares
                    state['buy_price'] = new_total_cost / new_total_shares if new_total_shares > 0 else exec_price
                    state['add_count'] += 1
                    state['last_add_day'] = state['days_since_buy']
                else:
                    # 初次买入
                    state['in_position'] = True
                    state['buy_price'] = exec_price
                    state['total_cost'] = exec_price * exec_size
                    state['total_shares'] = exec_size
                    state['init_shares'] = exec_size
                    state['add_count'] = 0
                    state['last_add_day'] = 0
                    state['highest_since_buy'] = exec_price
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
                state['watch_history'] = []
                state['total_cost'] = 0.0
                state['total_shares'] = 0
                state['init_shares'] = 0
                state['add_count'] = 0
                state['last_add_day'] = 0
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
        add_signals = []    # [(data, reason, shares)]  金字塔加仓

        for d in self.datas:
            name = d._name
            if name == self.p.market_code:
                continue   # 指数 feed 不参与个股逻辑
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
                # 金字塔加仓检查（不出场才考虑加仓）
                if self.p.pyramid_enabled and state.get('add_count', 0) < self.p.pyramid_max_adds:
                    close = float(d.close[0])
                    buy_price = state['buy_price']
                    if buy_price > 0:
                        gain = (close - buy_price) / buy_price
                        gap_ok = (state['days_since_buy'] - state.get('last_add_day', 0)) >= self.p.pyramid_min_gap_days
                        new_high = close >= state.get('highest_since_buy', close)
                        if gain >= self.p.pyramid_trigger_gain and gap_ok and new_high:
                            init_shares = state.get('init_shares', 0)
                            add_shares = int(init_shares * self.p.pyramid_size_ratio / 100) * 100
                            if add_shares >= 100:
                                add_signals.append((d, f"金字塔加仓#{state['add_count']+1} 浮盈{gain:+.1%}", add_shares))
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

        # 预热期内只计算指标、不下单（DB 模式：数据从更早加载但交易从 start 开始）
        if self.p.trade_start_date is not None:
            current_date = self.datetime.date(0)
            if current_date < self.p.trade_start_date:
                return

        # ── Phase 2.5：金字塔加仓（只用可用现金，不占 slot） ──
        for d, reason, add_shares in add_signals:
            name = d._name
            if name in self.order_dict:
                continue
            price = float(d.close[0])
            cash = self.broker.getcash()
            if cash < price * add_shares * 1.01:
                continue
            o = self.buy(data=d, size=add_shares)
            o._reason = reason
            o._is_pyramid = True
            self.order_dict[name] = o

        # ── Phase 3：执行 BUY（受仓位限制） ──
        # confidence: 按置信度降序（baseline）；neutral: 稳定伪随机填槽（不按动量，
        # 审计证 rank_score 动量排序在当日内反向有害 -3.68pp）。
        if self.p.select_mode == 'neutral':
            import zlib
            _d = self.datetime.date(0).isoformat()
            buy_signals.sort(key=lambda x: zlib.crc32(f"{x[0]._name}{_d}".encode()))
        else:
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
            state['watch_history'] = []
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
# 本地数据库（MySQL stock_daily）加载
# ══════════════════════════════════════════════════════════════

async def get_all_db_stocks(start: str, end: str) -> list[dict]:
    """
    从 MySQL stock_daily 表获取在回测区间内有足够数据的股票列表。
    同时尝试从 stock_snapshot 获取股票名称。

    Survivorship 缓解：JOIN stock_basic 拿 list_date，剔除"区间起点时还没上市/上市未满 60 交易日"的股票。
    （注：仍无法修复"区间内已退市"的偏差——需要付费数据源历史快照。详见 CLAUDE.md）
    """
    from db.mysql_pool import get_pool
    from datetime import datetime, timedelta
    # ≥60 交易日 ≈ 90 自然日，给个安全裕量
    listing_cutoff = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=90)).date().isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 取有≥30条记录的股票，LEFT JOIN 拿名称和上市日
            await cur.execute("""
                SELECT sd.code,
                       COALESCE(ss.name, sb.name, sd.code) AS name,
                       COALESCE(ss.market, sb.market, IF(sd.code LIKE '6%%', 'SH', 'SZ')) AS market,
                       sb.list_date
                FROM stock_daily sd
                LEFT JOIN stock_snapshot ss ON ss.code = sd.code
                LEFT JOIN stock_basic    sb ON sb.code = sd.code
                WHERE sd.trade_date >= %s AND sd.trade_date <= %s
                  AND sd.code NOT LIKE 'idx!_%%' ESCAPE '!'
                  -- list_date 缺失则放行（保留向后兼容）；存在则要求早于 cutoff
                  AND (sb.list_date IS NULL OR sb.list_date <= %s)
                GROUP BY sd.code, ss.name, sb.name, ss.market, sb.market, sb.list_date
                HAVING COUNT(sd.code) >= 30
            """, (start, end, listing_cutoff))
            rows = await cur.fetchall()
    stocks = [{'code': r[0], 'name': r[1] or r[0],
                'market': r[2] or ('SH' if r[0].startswith('6') else 'SZ'),
                'list_date': r[3]}
               for r in rows]
    print(f"[数据源-本地DB] 发现 {len(stocks)} 只股票 ({start}~{end}, list_date ≤ {listing_cutoff})")
    return stocks


async def load_all_db_data(codes: list, start: str, end: str) -> dict:
    """
    批量从 MySQL stock_daily 加载多只股票的日线数据。
    一次 SQL 查询取全量，按 code 分组后转为 DataFrame。
    返回 {code: pd.DataFrame} 字典（已设置 date 为索引）。
    """
    import pandas as pd
    from collections import defaultdict
    from db.mysql_pool import get_pool

    if not codes:
        return {}

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            placeholders = ','.join(['%s'] * len(codes))
            await cur.execute(f"""
                SELECT code, trade_date, open_price, high, low, close, volume, amount
                FROM stock_daily
                WHERE code IN ({placeholders})
                  AND trade_date >= %s
                  AND trade_date <= %s
                ORDER BY code, trade_date
            """, (*codes, start, end))
            rows = await cur.fetchall()

    # 把 trade_date 一并存入 bar，避免后续重建索引时出现长度不一致
    by_code = defaultdict(list)
    for code, trade_date, open_p, high, low, close_, volume, amount in rows:
        if None in (open_p, high, low, close_):
            continue
        by_code[code].append({
            'date':   trade_date,
            'open':   float(open_p),
            'high':   float(high),
            'low':    float(low),
            'close':  float(close_),
            'volume': int(volume or 0),
        })

    result = {}
    for code, bars in by_code.items():
        if len(bars) < 60:   # MA60 需要至少60条，低于此数的数据加载进 Backtrader 会 IndexError
            continue
        dates = pd.to_datetime([b['date'] for b in bars])
        df = pd.DataFrame(
            [{k: v for k, v in b.items() if k != 'date'} for b in bars],
            index=dates,
        )
        df.index.name = 'date'
        df = df.sort_index()
        for col in ('open', 'high', 'low', 'close', 'volume'):
            df[col] = df[col].astype(float)
        result[code] = df

    print(f"[数据源-本地DB] 成功加载 {len(result)}/{len(codes)} 只股票日线")
    return result


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
