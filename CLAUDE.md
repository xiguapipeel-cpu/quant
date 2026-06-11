# CLAUDE.md — 主力建仓策略决策追溯

> **重要**：本文件记录 `backtest/bt_major_capital.py` 当前默认参数的来源验证，避免后人在不读数据的情况下回滚改动。
> 修改任何"已写入默认"的参数前，请先看本文档对应的 walk-forward / ablation 数据。

## 当前策略默认值（2026-04 已固化）

| 参数 | 当前默认 | 旧默认 | 验证阶段 | 净增益 |
|------|---------|--------|---------|--------|
| `enable_signal_f` | `False` | `True` | P1-B (VAR_A) | **+20.12pp** 累计 |
| `atr_trail_k` | `2.0` | `3.0` | P0 #1 | tight_trail 系列共 +7.47pp |
| `trail_stage2_gain` | `0.05` | `0.15` | P0 #1 | 越赚越紧 |
| `trail_stage2_k` | `1.5` | `5.0` | P0 #1 | 越赚越紧 |
| `trail_stage3_gain` | `0.15` | `0.30` | P0 #1 | 越赚越紧 |
| `trail_stage3_k` | `1.0` | `7.0` | P0 #1 | 越赚越紧 |
| `signal_a_min_rsi` | `55.0` | `0.0` (off) | P2-A | +10.29pp 累计（rsi 单独足够） |
| ~~`signal_a_min_ma_diverge_pct`~~ | _已删除_ | `-999.0` (off) | P2-A → P2-A dissect | rsi 包络冗余，4 折逐笔等价 |
| `signal_a_break_high_lookback` | `30` | `0` (off) | P4 | +7.92pp 累计（Fold 2/3 胜率→100%） |

**累计 4 折 OOS 改善：-15.30% → +30.52%（Δ +45.82pp）**

## 决策链（按时间顺序）

### P0 #1 — tight_trail（追踪止损反转）
**问题**：原 trail 哲学"越赚越宽"（k=3→5→7）。Fold 3 OOS 13 笔中 9 笔被 k=3.0 trail 触发，平均回撤 10.6%——浮盈被吃光，明星单 300790 峰值 +43% 仅留 +11.7%。

**实验**：反转哲学，stage1 k=2.0、stage2 (gain≥5%) k=1.5、stage3 (gain≥15%) k=1.0。

**单 fold 结果**（Fold 3 OOS）：-3.81% → +5.78%；MDD 10.36% → 4.25%；明星单留 +31.2%。

**4 折 walk-forward**：累计 -15.30% → -7.83%（Δ +7.47pp）。但 Fold 2 反而变差（+1.4% → -3.2%）——指向更深问题。

**数据**：`logs/p0_trail_compare.json`、`backtest_cache/walk_forward_report/20260427_213437/`

### P0 #2 — lock_floor + pyramid 加固（已删除）
曾尝试分层锁利下限（峰值≥10%锁+3%、≥20%锁+10%、≥35%锁+22%）+ 加仓需先达 stage2_gain。
**4 折 OOS 全部 0 触发**——`max(trail, hard, floor) = trail`，floor 被 trail 包络。
**已删除**（共 7 个参数：`lock_floor_t1/t2/t3_gain/_pct`、`pyramid_require_highest_gain`），降低自由度避免过拟合心理依赖。

### P1-A — 入场信号质量诊断
对 4 折 OOS 全部 31 个 entry 提取 `buy_meta` 特征 + 5 日后收益。

**关键发现**：
- A_breakout (n=17)：5 日胜率 47.1%、final 均赚 +0.48%（低胜率高赔率）
- F_vol_pattern (n=14)：5 日胜率 78.6%、final **均亏 -1.78%**（典型假突破回吐）
- rsi 高分位 final +3.33% vs 低分位 -1.83%
- ma_diverge 高分位 final +3.03% vs 低分位 -4.44%

**数据**：`logs/p1_signal_analysis.json`

### P1-B — Ablation 验证（VAR_A vs VAR_B）
- **VAR_A**：`enable_signal_f=False` — 累计 -7.83% → **+12.31%**（Δ +20.12pp，4/4 折改善或持平）
- VAR_B：rsi≥53 + mdiv≥0 过滤 — 仅 +3.33pp（边际）

**采纳 VAR_A**，写入默认。

**数据**：`logs/p1_ablation.json`

### P1-C — Walk-forward 验证（rsi 过滤是否值得叠加）
单 grid `signal_a_min_rsi: [0, 53]`，4 折 IS 选择：3/4 折选 0、1 折 0:0 平局取 53。
4 折 OOS 累计 +10.48% vs VAR_A +12.31%（Δ -1.83pp，rsi=53 略劣）。
**结论**：rsi=53 阈值无显著边际增益，未采纳。

**数据**：`backtest_cache/walk_forward_report/20260428_122722/`

### P2 — Fold 1 失败模式诊断
Fold 1 OOS（2025-01~06）-5.61%、6 笔 0 胜。最差单 #6 600611：3-27 买入、4-08 卖出（关税黑天鹅当日），亏 17.77%（gap-down 跨过 ATR 硬止损）。

**4 折特征对比**：
- 赢家折（3）：rsi 均 59.6、mdiv 均 +1.09、硬止损率 10%
- 输家折（1, 4）：rsi 均 52-54、mdiv 均 -1.82~-0.03、硬止损率 50%

### P2-A — 入场质量过滤（rsi≥55 AND mdiv>0）
直接对应 P1-A 的"低质量入场"诊断。

**4 折 OOS 结果**：
| Fold | VAR_A baseline | P2-A | Δ |
|------|---------------|------|---|
| 1 | -5.61% n=6 | **-0.53% n=1** | +5.08pp |
| 2 | +8.32% n=17（胜率58%）| **+13.64% n=10（胜率83%）** | +5.32pp |
| 3 | +9.60% n=14 | +9.49% n=9 | -0.11 |
| 4 | 0% n=0 | 0% n=0 | 0 |
| **累计** | **+12.31%** | **+22.60%** | **+10.29pp** |

Fold 1 砍掉了 4-7 关税单，Fold 2 闭仓胜率从 58% 飙至 83.3%，Fold 3 几乎不影响。

**数据**：`logs/p2a_filter.json`

### P2-A dissect — rsi 与 mdiv 哪个真正起作用？
分别测试 RSI_ONLY（rsi=55, mdiv off）和 MDIV_ONLY（rsi off, mdiv=0）。

**4 折 OOS 结果（逐位相同）**：
| Fold | BOTH_P2A | RSI_ONLY | MDIV_ONLY |
|------|----------|----------|-----------|
| 1 | -0.53% n=1 | -0.53% n=1 | -0.53% n=1 |
| 2 | +13.64% n=10 | +13.64% n=10 | +13.64% n=10 |
| 3 | +9.49% n=9 | +9.49% n=9 | +9.49% n=9 |
| 4 | 0% n=0 | 0% n=0 | 0% n=0 |
| **累计** | **+22.60%** | **+22.60%** | **+22.60%** |

**结论**：rsi 与 ma_diverge 在该样本上**测同一件事**（短期趋势确认），强相关，砍同一批 6 笔入场。
**采纳 RSI_ONLY**：删除 `signal_a_min_ma_diverge_pct` 参数 + 过滤逻辑，再降一个自由度。

**数据**：`logs/p2a_dissect.json`

### P4 — 突破质量过滤（close > max(high[-1..-N])）
**问题**：原"放量大阳线"突破定义只看今日 K 线（涨幅、量比、收盘位置），**没指明突破了什么**——随机放量大阳线（如下跌中继、横盘内大阳）也满足。

**实验**：要求 `close > max(high[-1..-N])` 才算真突破。

**4 折 OOS 结果**：
| Fold | baseline | BH_10 | BH_20 | BH_30 |
|------|----------|-------|-------|-------|
| 1 | -0.53% n=1 | -0.53% n=1 | -0.53% n=1 | -0.53% n=1 |
| 2 | +13.64% n=10 (83%) | +14.16% n=9 (**100%**) | +14.16% n=9 (**100%**) | +14.16% n=9 (**100%**) |
| 3 | +9.49% n=9 (83%) | +13.54% n=8 (**100%**) | +13.54% n=8 (**100%**) | **+16.89% n=8 (100%)** |
| 4 | 0% n=0 | 0% n=0 | 0% n=0 | 0% n=0 |
| **累计** | **+22.60%** | +27.17% | +27.17% | **+30.52%** |

- BH_10/20 等价（砍同一批 2 笔输家）；BH_30 在 Fold 3 多 +3.35pp（slot 释放给更扎实的后续突破）
- Fold 2/3 闭仓胜率全部从 83% 升至 100%
- BH_30 与策略 watch 期（30 日滚动窗口）哲学一致

**采纳 BH_30**：`signal_a_break_high_lookback=30` 写入默认。

**数据**：`logs/p4_break_high.json`

### P5 — 自适应阈值定义重构（保留，未采纳）
**问题**：原 percentile-based 阈值定义（涨幅 75 分位 + 5日量比 85 分位）混合了百分比与比率两个尺子，**没有按股票波动率/流动性自适应**。
- 涨幅用固定 % 不能区分高低波动股票（小盘 5% ≈ 大盘 1%）
- 量比用 5 日均值平滑，掩盖了股票真实的相对成交活跃度

**实验**：
- 涨幅: `today_change ≥ K × ATR`（波动率自适应）
- 量比: `today_vol > Q(N%) of past 60d raw volume`（流动性自适应）

**4 折 OOS 结果（V80 固定，扫 ATR_K）**：
| Fold | baseline | ATR1.0_V80 | **ATR1.5_V80** | ATR2.0_V80 |
|------|----------|-----------|---------------|-----------|
| 1 | -0.53% n=1 | -1.97% n=2 | -1.97% n=2 | -0.53% n=1 |
| 2 | +14.16% n=9 (100%) | +15.22% n=10 (86%) | +18.70% n=8 (80%) | +14.20% n=5 (100%) |
| 3 | +16.89% n=8 (100%) | +11.79% n=10 (83%) | +17.32% n=6 (75%) | +12.82% n=3 (100%) |
| 4 | 0% n=0 | 0% n=0 | 0% n=0 | 0% n=0 |
| **累计** | **+30.52%** | +25.04% (-5.48pp) | **+34.05% (+3.53pp)** | +26.49% (-4.03pp) |
| 单笔均 | +1.70% | +1.14% | +2.13% | +2.94% |

**结论**：ATR1.5_V80 净增益 +3.53pp（用胜率换大额收益：F2/F3 胜率 100%→80%/75%，但单笔均 +0.43pp）。
**未采纳**：增益不如 P4 大，且 Fold 1 多放 1 笔输家、Fold 2/3 胜率显著回落，需要更大范围验证（如更长历史 + walk-forward）确认非样本效应后再决定。

**保留**：`breakout_atr_k` 和 `vol_raw_percentile` 参数已加入策略代码，默认 0（关闭，沿用旧 percentile 逻辑）。通过 `extra_params={'breakout_atr_k': 1.5, 'vol_raw_percentile': 0.80}` 启用。

**数据**：`logs/p5_adaptive_threshold.json`

### P6 — 扩展 10 折 walk-forward 验证（2023-07 ~ 2026-03）
**问题**：所有调参都基于 2025 年 4 折 OOS（~18 笔样本）；策略在更长历史上是否稳定？

**结果**：
| Fold | Period | baseline | P5_ATR15 |
|------|--------|----------|----------|
| 1 | 2023-07~12 | 0 笔 | 0 笔 |
| 2 | 2023-10~24-03 | 0 笔 | 0 笔 |
| 3 | 2024-01~06 | 0 笔 | 0 笔 |
| 4 | 2024-04~09 | 0 笔 | 0 笔 |
| 5 | 2024-07~12 | 0 笔 | 0 笔 |
| 6 | 2024-10~25-03 | 0 笔 | -1.46% n=1 |
| 7-10 | 2025-01~26-03 | (同 P4) | (同 P5 单 fold) |
| **累计** | | **+30.52%** | **+32.59%** |

**核心发现**：**Fold 1-6（21 个月）baseline 完全不交易**——策略在 2023 熊市 + 2024 震荡市自动息火。

**数据**：`logs/p6_extended_validation.json`

### P7 — Dead Zone 诊断（找元凶）
对 4 个过滤层在 Dead Zone（Fold 1-6）和 Active Zone（Fold 7-10）做独立通过率统计：

| 过滤层 | Dead Zone | Active Zone | 差异 |
|--------|-----------|-------------|------|
| L1 idx_sh MA20>MA60 | **39.7%** 交易日 | **74.9%** 交易日 | **1.9×** ⚠️ |
| L2 stock MA20>MA60 | 43.5% | 55.2% | 1.3× |
| L3 RSI≥55 | 36.3% | 41.4% | 1.1× |
| L4 close>30d 高 | 3.4% | 4.3% | 1.3× |
| 链式 L1∩L2∩L3∩L4 | 1.00% | 2.73% | 2.7× |

**元凶 = L1 大盘过滤**：Dead Zone 期间 60% 交易日大盘 MA20≤MA60，直接禁止入场。次要因素：L1 通过日**碎片化**，无法形成 WATCH 阶段需要的"30 日滚动窗口 ≥15 天累计"。

**决策：D 不改，接受策略性质**
- Dead Zone silent 是设计而非缺陷——L1 在数学上明确"只在 trending 市进场"
- 放宽 L1 会让策略在 60% 弱市/反弹失败时段强行进场，**提取负 alpha**
- 18 笔总样本已不足以再支持任何参数调整（每多一个参数 = 进一步过拟合 18 笔）
- 0% return（保本）≠ 亏损；不要把"机会成本"和"实际损失"混淆

**数据**：`logs/p7_*` 诊断脚本 `backtest/p7_dead_zone_diag.py`

### P8 — 「放量跌破建仓平台下沿」候选离场规则（验证后否决）
**来源**：外部专业投资者建议——放量跌破均线粘合区下沿（生命线）= 变盘向下，应止损。

**验证**：对 position_monitor 已离场样本重放持仓期，对比候选规则「收盘放量跌破 MA(平台下沿) 」与现有 `max(ATR trail, ATR 硬止损, MA20) + -10% 硬止损` 的触发时机与收益。脚本 `backtest/p8_volbreak_stop.py`。

**3 参数变体结果（一致）**：
| 变体 | 触发率 | 早于现有 | 候选 vs 现有均 PnL |
|------|--------|---------|-------------------|
| MA60 + 放量1.5x | 37.7% | 26.9% | +0.46% vs +2.46% = **-2.00pp** |
| MA60 + 放量2.5x | 5.1% | 3.0% | +1.59% vs +1.88% = **-0.29pp** |
| MA30 + 放量1.5x | 26.8% | 13.0% | -0.44% vs -0.23% = **-0.21pp** |

**关键发现**：
- **未被包络**（不同于 P0#2 lock_floor 的 0 触发）——候选确实会触发且 27% 真早于现有离场。所以"包络"不是问题。
- 但「更早触发」在该形态上**恰是坏事**：放量跌破均线后主力建仓股多数回踩再续涨，过早离场**砍掉少数大赢家**（更优笔数占 51~67%，但金额上一致更差），躲掉的下跌幅度有限——"砍了赢家、只躲了小亏"。

**决策：不采纳。** 现有离场体系（P0#1 tight_trail 已调校到位）给了趋势必要的容忍空间，再紧（放量跌破生命线）即过度。脚本保留，样本变化后可重跑。

### 回踩确认买点（建议2）— 影子信号前向跟踪中（未纳入）
**来源**：外部专业建议——突破后缩量回踩不破核心均线，是成本更优的二次买点。现有策略缺此买点（仅突破当日右侧单一买点）。

**做法**：作为「影子信号」只跟踪不交易（`scripts/shadow_pullback_scan.py`，独立命名空间 `major_capital_pullback_shadow`，写 pattern_outcome 跟踪后续走势，**完全不影响现有交易**），已接入每日调度前向跟踪。

**带二次确认定义**（突破→缩量回踩不破 MA30→重新放量阳线转强）历史回填基线（180 天、样本 34）：5 日 62.5%/+0.75%，30 日 31%/-1.65%，60 日 22%/-7.48%。短期有效、中长期偏弱（疑似短线买点，与中长线定位不符）。**待攒 1-2 个月前向真实样本后再判断是否纳入。** `python -m scripts.shadow_pullback_scan --report` 查最新。

### 策略定位（固化 — P9/P9b 后修正）
**本策略是「主力建仓形态扫描器」**，不是连续交易策略。

**深层验证（P9b 2026-05-20）**：
- 2020-2023 期间 7 个新 fold（COVID V、复苏牛、新能源高峰、抱团瓦解、熊市等）baseline + pre_P0_legacy **全部 0 trades**
- 强制关闭 `market_filter` + 降 `min_watch_days=5` 才能进场，但 20 笔全亏（-3.15% 胜率 23.5%）
- 结论：WATCH 阶段 7 条件（near_low + ma_convergence + slope + RSI 35-65 + yy_ratio ≥ 0.9 + 地量 + 30 日窗口 ≥15 天累计）**就是"主力建仓"形态本身的特征**——这种形态在 5 年里只在特定时段（如 2024-09~2025）真实出现过

**部署模式**：
- ✅ 形态扫描器：每日扫描全市场，发现匹配形态时推送告警
- ✅ 推送命中后**人工或自动**进场，跟随 trail 退出
- ✅ 接受长期 silent（30-60% 时间无信号是该形态本身罕见）
- ❌ **不要**把它当成"连续盈利策略"看回报率
- ❌ **不要**对它做"年化收益"评估

**评估口径修正**：
- 旧（错）：年化 X%、夏普 Y、累计 N 折 OOS
- 新（对）：**形态命中精确度**（命中后多少天内突破并盈利？）、**单次形态收益**（每次形态发生时入场到出场的回报）、**形态频率**（每年触发多少次）

**互补策略思路（如有需要）**：
- 当前策略：捕捉「主力静默吸筹+放量突破」形态（罕见、高质量）
- 不需要"全天候" — 不是这个策略的设计目标
- 如要全天候投资，另起其他策略（不要继续放宽本策略过滤）

## 复现命令

```bash
# 完整 4 折 OOS 对照（验证当前默认参数）
./venv/bin/python -m backtest.p1_dump_oos_trades   # 4 折 dump（~40min）
./venv/bin/python -m backtest.p1_analyze_signals    # 信号质量分析

# Walk-forward 完整网格（参数搜索 + 稳定性检验）
./venv/bin/python -m backtest.walk_forward \
  --start 2024-01-01 --end 2026-03-31 \
  --train-months 12 --test-months 6 --step-months 3 \
  --data-source local_db --grid <grid.json>
```

## 仍然待验证 / 已知风险

1. **Sharpe 仍接近 0**（10 折 OOS 累计 +30.52% / 33 个月 → 年化 ~11% 但 sharpe ~0.2-0.7 in active folds）。表明波动率仍偏高。
2. **样本量小**：10 折 × 总 18 笔（baseline）= 每参数 ~2 笔。所有参数都基于 OOS 反复迭代选出（OOS 已退化为 IS）。
3. **Regime-conditional**：6/10 折 0 trades（已接受为设计特性，见 P6/P7）。
4. **未做手续费 / 滑点压力测试**。
5. **过拟合风险**：~9 个有效参数 × 18 笔总样本 = 严重过拟合区域。任何新调整都应该被怀疑。
6. **退市股 Survivorship Bias 不可修复**：`get_all_db_stocks` 已加 `list_date` 过滤剔除 IPO 未足 60 日的股票，但**stock_daily 本身只含当前上市股**（akshare 不返回退市股历史）。期望真实表现降级 1-3pp 收益。需付费 Tushare Pro / Wind 才能修复。

## 数据层修复历史

- **2026-04-29**: `get_all_db_stocks` 加 `JOIN stock_basic` + `list_date <= start - 90d` 过滤——剔除 trade_date 时还未 IPO/上市未满 60 日的股票。验证：Fold 3 OOS 期股票池 4619 → 4590（剔除 ~29 只 2025 Q2 IPO）。
- **未做（建议）**: 月度归档 `stock_basic` 至 `stock_basic_history`（仅对未来回测有效，不补救历史）。
- **未做（理想）**: 接入 Tushare Pro 拉历史退市/ST 状态，真正修复 survivorship。

## 实盘前必做

- 小资金（1-2 万）跑 4-8 周，对比纸面回测
- ✅ 加固单笔最大亏损硬限（-10%）防 gap-down — 已实现（`config/execution_rules.py` `MAX_SINGLE_LOSS_PCT`，`daily_exit_scan.check_exit`）
- ✅ 加 idx_sh 5 日跳水检测（P2-B）— 已实现（`evaluate_market_regime`：5 日跌幅 ≤ -5% 或 MA20<MA60 → 减仓清仓推送，`daily_exit_scan.main`）

## 执行层优化（阶段 3，2026-06 实现）

> 均在 `config/execution_rules.py` 集中配置，不改变 WATCH/BUY 形态定义。

| 子项 | 实现 | 配置 |
|------|------|------|
| 次日高开过大不追 | `evaluate_next_open` gap>5% → status='skipped' | `NEXT_OPEN_MAX_GAP_UP_PCT=0.05` |
| 跌回信号价下方不买 | `evaluate_next_open` | `NEXT_OPEN_REQUIRE_ABOVE_SIGNAL_PRICE` |
| 排序买入 + top-N | `rank_buy_signals`/`rank_score` 多因子综合分（confidence/RSI/watch_days/yy_ratio/bb_narrow/突破强度/成交额），同日只入排名前 N | `MAX_NEW_ENTRIES_PER_DAY=3`、`RANK_WEIGHTS` |
| 分批进场 | 次日半仓首批（entry_stage=1）→ N 日内收盘站稳突破位/继续放量补满（stage=2，写 avg_entry_price）；窗口内未站稳放弃（stage=3 维持半仓） | `STAGED_ENTRY_ENABLED`、`FIRST_TRANCHE_PCT=0.5`、`ADD_WINDOW_DAYS=5` |
| 单笔 -10% 硬止损 | `check_exit` 收盘亏损 ≥ 10% 强制离场 | `MAX_SINGLE_LOSS_PCT=0.10` |
| 大盘急跌/趋势走弱减仓提示 | `evaluate_market_regime` + `pusher.send_market_alert` | `MARKET_5D_DROP_PCT=-0.05`、`MARKET_MA_FAST/SLOW=20/60` |

## 实盘/模拟双轨验证（2026-06 实现）

> 目标：用真实成交对照模拟信号，验证「信号是否有效 / 执行规则是否减少亏损 / 是否错过大赢家」，并据此**只调执行规则、不调形态参数**。

| 环节 | 实现 |
|------|------|
| BUY 自动登记模拟持仓 | `daily_major_capital_scan` Step 3.5 → `position_monitor`（`is_real=0`，附 signal_price/次开/执行过滤/分批 stage） |
| 人工确认买入标记真实 | Web `/api/position/mark_real`（或 `/api/position/add` 直接录真实持仓 `is_real=1`） |
| 每日统一离场扫描 | `scripts.daily_exit_scan` 扫所有 `status='open'`（模拟+真实），跑 trail/硬止损/MA20/分批补仓/大盘 regime；**已接入调度器**：每日 BUY 选股后以子进程自动连跑（`web/app.py _scheduler_loop`） |
| 逐笔记录字段 | `position_monitor`：信号日收盘价(`signal_price`)、次日开盘价(`entry_price`)、实际成交价(`actual_exit_price`)、最高浮盈(`highest_price`)、最大浮亏(`lowest_price`)、回测退出价(`exit_price`)、实盘退出价(`actual_exit_price`)；信号后续走势在 `pattern_outcome`(5/10/30/60 日 + peak/trough) |
| 4-8 周复盘 | `python -m scripts.dual_track_review [--weeks N \| --since DATE \| --all] [--save]` —— 串联两表回答 4 问，输出逐笔明细 + 执行规则调整建议 |

**复盘脚本 4 问对应数据源**：
1. 信号是否有效？→ `pattern_outcome` 命中后 5/10/30/60 日胜率 + 平均收益 + 峰值
2. 执行规则是否减少亏损？→ 进场单 PnL/硬止损次数/skipped 拦截结构/实盘 vs 回测退出滑点/真实 vs 模拟对比
3. 是否错过大赢家？→ skipped 与 放弃补仓(stage=3) 的票 join `pattern_outcome.peak_ret`（峰值≥15% = 错过；谷值≤-8% = 成功避开）
4. 是否需调整执行规则？→ 基于以上指标启发式生成建议（**强调调执行层，不回到形态参数过拟合**）

## 月度 Outcome 报告（样本外信号追踪，2026-06 实现）

> 目标：对**全部信号（即使没交易也记录）**做样本外效力跟踪，与执行轨复盘互补。

**每日 cron 链**（已接入调度器 `web/app.py _scheduler_loop`，选股触发后自动依次跑）：
1. `scripts.daily_major_capital_scan` —— 选股 + 所有 WATCH/BUY 写入 `pattern_outcome`（Step 2.5，不交易也记录）
2. `scripts.daily_exit_scan` —— 统一扫描模拟+真实持仓退出
3. `scripts.pattern_tracker --update` —— 刷新每个事件的 5/10/30/60 日收益、峰值(peak_ret)、谷值(trough_ret)

**周/月报告**（按需运行）：
```bash
python -m scripts.monthly_outcome_report --month 2025-09   # 指定月
python -m scripts.monthly_outcome_report --weeks 4         # 近 4 周
python -m scripts.monthly_outcome_report --all --save      # 全历史并存档
```
报告内容：BUY 数量、5/10/30/60 日胜率、平均收益、平均最大浮盈(peak)、平均最大浮亏(trough)、
**跳空亏损案例**（次日跳空低开 ≤ -2% 且 30 日实亏）、**未买入但后续大涨/大跌案例**
（执行层未建仓的 BUY 信号 join `pattern_outcome`：峰值≥15% 错过 / 谷值≤-10% 避开）。

**两份报告分工**：
| 报告 | 数据源 | 回答 |
|------|--------|------|
| `dual_track_review` | position_monitor（执行轨） | 已建仓单的执行规则效果、真实 vs 模拟、退出滑点 |
| `monthly_outcome_report` | pattern_outcome（信号轨） | 全信号样本外效力、含没买的、跳空/错过案例 |

## 下一步建议（按"形态扫描器"定位重组）

### 1. 部署为日常扫描器（核心交付）
- 每个交易日盘后跑 `major_capital_accumulation` 策略的 **buy signal** 部分（不做完整回测，只扫描）
- 命中股票推送到飞书/微信，附 `buy_meta` 关键指标（RSI、ma_diverge、yy_ratio、bb_narrow、watch 累计天数等）
- 人工二次审查后下单
- **存储所有形态命中事件**（即使没下单），追踪后续 5/10/30 日表现，做事后形态质量评估

### 2. 实盘对接（命中即买）
- 每次推送可选择"自动下单"模式，使用当前默认参数 + 仓位 10-20%
- 实时跟踪 trail 状态，命中卖出条件后自动平仓
- 接受 0 命中的月份，不强求成交

### 3. 形态质量持续监测
- 每月统计：命中数量、5/10/30 日后正收益占比、平均收益
- 如果命中后 30 日胜率长期 < 50%，反思形态定义；高于 60% → 形态有效
- **不是基于回测年化收益**判断策略好坏，而是基于"形态命中后的真实后续走势"

### 4. 拒绝继续调参
- 18 笔总样本已用尽统计验证空间
- 任何新过滤都是在同一形态内进一步收紧 → 过拟合无疑
- **冻结当前默认值**，让形态自然在市场中浮现
