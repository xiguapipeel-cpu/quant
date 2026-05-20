"""
P5：自适应阈值定义重构 ablation
=================================

旧定义（baseline，已知 4 折 OOS +30.52%）：
  - 涨幅: percentile_75 of past 60d daily pct_chg, floor 3%
  - 量比: percentile_85 of past 60d (today_vol / avg_prev_5d_vol)

新定义（P5）：
  - 涨幅: today_change ≥ K × ATR  （volatility-normalized）
  - 量比: today_vol > Q(N%) of past 60d raw volume

测试 ATR 倍数（1.0/1.5/2.0）× 量分位（固定 0.80）共 3 个变体。
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
    'ATR1.0_V80': {'breakout_atr_k': 1.0, 'vol_raw_percentile': 0.80},
    'ATR1.5_V80': {'breakout_atr_k': 1.5, 'vol_raw_percentile': 0.80},
    'ATR2.0_V80': {'breakout_atr_k': 2.0, 'vol_raw_percentile': 0.80},
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
          f"win={m.get('win_rate')} trades={m.get('total_trades')}")
    return {'label': label, 'fold': fold, 'period': f'{start}~{end}', 'metrics': m,
            'trades_paired': result.get('trades_paired', [])}


async def main():
    results = []
    for label, params in VARIANTS.items():
        print(f'\n{"="*60}\n{label}: {params}\n{"="*60}')
        for idx, s, e in FOLDS:
            r = await run_one(label, idx, s, e, params)
            results.append(r)
    out = ROOT / 'logs' / 'p5_adaptive_threshold.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'baseline_known': {
                'fold1': {'ret': '-0.53%', 'trades': 1},
                'fold2': {'ret': '+14.16%', 'trades': 9},
                'fold3': {'ret': '+16.89%', 'trades': 8},
                'fold4': {'ret': '+0.00%',  'trades': 0},
                'cum': '+30.52%',
            },
            'variants': VARIANTS,
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
