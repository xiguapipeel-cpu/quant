"""
P10：追踪止损哲学 A/B —— "越赚越紧"(现默认) vs "越赚越放"(海龟式)

背景：P0#1 曾把旧的"越赚越放"(k=3→5→7)反转为"越赚越紧"(k=2→1.5→1)，
      4 折 walk-forward 累计 -15.30% → -7.83% (Δ +7.47pp)。
      但那次 A/B 跑在【旧的脏入场体系】上(signal_f 开、无 rsi≥55、无 break_high30)。

盲点：现在入场已被层层过滤到 Fold2/3 闭仓胜率 100%——进来的票质量完全不同。
      对真正能走趋势的高质量突破，"让利润奔跑"是否反而更优？P0#1 未单独重测。

本实验：固定当前所有过滤/形态参数不动，仅切 trail 哲学，在【今天的干净入场】上重测。
        TIGHT_CURRENT 与 LOOSE_* 在同一次运行、同一份代码下对比(不信旧文档数字)。

重点看：累计收益、MDD、明星单留存率(peak_ret vs 实际落袋)，而非只看累计。

结论无论如何【都不写入默认】——只追加 CLAUDE.md P10 记录。
"""
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.bt_runner import run_for_web
from db.mysql_pool import close_pool


VARIANTS = {
    # 当前默认(越赚越紧)——空 params，跑当前代码默认值做真·基准
    'TIGHT_CURRENT': {},
    # 海龟式越赚越放(P0 旧 baseline 原值)
    'LOOSE_OLD': {
        'atr_trail_k': 3.0,
        'trail_stage2_gain': 0.15, 'trail_stage2_k': 5.0,
        'trail_stage3_gain': 0.30, 'trail_stage3_k': 7.0,
    },
    # 温和放宽(介于两者之间，测单调性)
    'LOOSE_MID': {
        'atr_trail_k': 2.5,
        'trail_stage2_gain': 0.10, 'trail_stage2_k': 3.0,
        'trail_stage3_gain': 0.20, 'trail_stage3_k': 4.0,
    },
}

FOLDS = [
    (1, '2025-01-01', '2025-06-30'),
    (2, '2025-04-01', '2025-09-30'),
    (3, '2025-07-01', '2025-12-31'),
    (4, '2025-10-01', '2026-03-31'),
]


async def run_one(label, fold, start, end, params):
    print(f"\n--- {label} | Fold {fold} {start}~{end} ---")
    result = await run_for_web(
        strategy_name='major_capital_accumulation',
        start=start, end=end,
        cash=100000.0, log_fn=None,
        screen_preset='default',
        data_source='local_db',
        extra_params=params,
    )
    m = result.get('metrics', {})
    print(f"  ret={m.get('total_return')} sharpe={m.get('sharpe_ratio')} "
          f"win={m.get('win_rate')} mdd={m.get('max_drawdown')} "
          f"trades={m.get('total_trades')}")
    return {'label': label, 'fold': fold, 'period': f'{start}~{end}', 'metrics': m,
            'trades_paired': result.get('trades_paired', [])}


async def main():
    results = []
    for label, params in VARIANTS.items():
        print(f'\n{"="*60}\n{label}: {params}\n{"="*60}')
        for idx, s, e in FOLDS:
            r = await run_one(label, idx, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p10_trail_philosophy.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'note': 'TIGHT_CURRENT 为同次运行的真基准(当前代码默认)，勿与旧文档数字比',
            'variants': VARIANTS,
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
