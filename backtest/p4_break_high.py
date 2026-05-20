"""
P4：突破质量 ablation — close 必须 > 前 N 日最高价才算"真突破"

Baseline = 当前默认（无 break-high 要求）→ 已知 4 折 OOS +22.60%

变体：
  BH_10 : signal_a_break_high_lookback = 10  （宽松）
  BH_20 : signal_a_break_high_lookback = 20  （标准平台高点）
  BH_30 : signal_a_break_high_lookback = 30  （严格箱体高点）

预期：砍掉"放量大阳但还在平台内"的假突破，提升胜率。
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
    'BH_10': {'signal_a_break_high_lookback': 10},
    'BH_20': {'signal_a_break_high_lookback': 20},
    'BH_30': {'signal_a_break_high_lookback': 30},
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
    out = ROOT / 'logs' / 'p4_break_high.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({
            'baseline_known': {
                'fold1': {'ret': '-0.53%', 'trades': 1},
                'fold2': {'ret': '+13.64%', 'trades': 10},
                'fold3': {'ret': '+9.49%',  'trades': 9},
                'fold4': {'ret': '+0.00%',  'trades': 0},
                'cum': '+22.60%',
            },
            'variants': VARIANTS,
            'results': results,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[保存] {out}")
    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
