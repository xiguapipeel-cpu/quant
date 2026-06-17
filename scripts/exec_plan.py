"""可执行策略计划生成器 —— 输入资金，输出主力建仓扫描器的落地参数。

依据本仓库审计结论（2026-06，logs + memory）：
  - rank_score 动量排序选股【反向有害】(-3.68pp)，故计划【不按动量排序选】，等权中性填槽。
  - 全池等权 30d +6.51%/胜率58%/单名摸-10%概率41%(信号轨平均，非组合收益)。
  - A 股一手=100 股 + 分批首批半仓 → 每仓太小不可执行，太大则单名爆雷打穿组合。

用法: ./venv/bin/python -m scripts.exec_plan --capital 100000
      ./venv/bin/python -m scripts.exec_plan --capital 100000 --max-positions 8
"""
import argparse

BOARD_LOT = 100              # A 股最小一手
TARGET_PER_POSITION = 12500  # 每仓目标金额(元)，决定持仓数
M_MIN, M_MAX = 4, 12         # 持仓数实用区间（retail 管理 + 一手粒度）
FIRST_TRANCHE_PCT = 0.50     # 分批进场首批半仓（沿用 STAGED_ENTRY）
TYPICAL_PRICE = 20.0         # A 股中位价档位(元/股)，估一手成本/粒度

# 审计得到的单名信号轨期望（全池等权 E 政策，30 日窗口）
NAME_RET_30D = 6.51          # %
NAME_WIN = 57.7              # %
NAME_DISASTER = 40.9         # % 中途摸到 -10%


def build_plan(capital: float, max_positions: int | None = None) -> dict:
    if max_positions:
        M = max(1, int(max_positions))
    else:
        M = round(capital / TARGET_PER_POSITION)
        M = max(M_MIN, min(M_MAX, M))
    per_pos = capital / M
    first_tranche = per_pos * FIRST_TRANCHE_PCT
    # 每日新入上限：把一波行情的入场摊到 ~3 天，避免一天打满
    daily_cap = max(2, round(M / 3))
    # 一手成本 & 单仓可买手数（按典型价估粒度是否够细）
    lot_cost = BOARD_LOT * TYPICAL_PRICE
    lots_per_pos = first_tranche / lot_cost
    # 组合层风险：单名摸 -10% 在等权下对总账户的冲击
    single_hit = 10.0 / M
    return {
        "capital": capital, "M": M, "per_pos": per_pos,
        "first_tranche": first_tranche, "daily_cap": daily_cap,
        "lots_per_pos_first": lots_per_pos, "single_hit": single_hit,
    }


def main(capital, max_positions):
    p = build_plan(capital, max_positions)
    print("=" * 60)
    print(f"  可执行策略计划 | 主力建仓扫描器 | 资金 {capital:,.0f} 元")
    print("=" * 60)
    print(f"  最大并发持仓 M           : {p['M']} 只（等权）")
    print(f"  每仓目标金额             : {p['per_pos']:,.0f} 元")
    print(f"  分批首批(半仓)           : {p['first_tranche']:,.0f} 元  "
          f"≈ {p['lots_per_pos_first']:.1f} 手(@{TYPICAL_PRICE:.0f}元股)")
    print(f"  每日最多新建仓           : {p['daily_cap']} 只")
    print("  ── 选股规则（关键，区别于现状）──")
    print("  · 不按 rank_score 动量排序选（实测反向 -3.68pp）")
    print("  · 当日合格信号 > 空槽数时：等权 + 中性填槽")
    print("    （随机 / 或只按流动性地板过滤，不按 RSI/突破强度/成交额排）")
    print("  · 退出沿用现有 trail/硬止损/MA20（已验证，不动）")
    print("  ── 预期与风险（信号轨平均，非保证）──")
    print(f"  · 单名 30 日：均 +{NAME_RET_30D}% / 胜率 {NAME_WIN}% / 摸-10% 概率 {NAME_DISASTER}%")
    print(f"  · 单名摸 -10% 对总账户冲击 ≈ -{p['single_hit']:.1f}%（M={p['M']} 等权摊薄后）")
    print("  ── 诚实限制 ──")
    if p['M'] <= 8:
        print(f"  ⚠️ {capital:,.0f} 资金只够 ~{p['M']} 个并发仓 → 处于【集中区】，")
        print("     无法收割全池 +6.51% 的【广度】edge（那需几百个仓位分散）。")
        print("     单名 40% 摸-10% 的高方差靠这点分散压不住，回撤会比信号轨平均更颠。")
    print(f"  ⚠️ 信号稀疏：60% 时间可能 0-1 信号（regime-conditional），接受空仓。")
    print("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, required=True, help="可用资金(元)")
    ap.add_argument("--max-positions", type=int, default=None, help="手动指定并发持仓数")
    args = ap.parse_args()
    main(args.capital, args.max_positions)
